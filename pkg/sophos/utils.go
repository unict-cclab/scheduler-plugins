package sophos

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	appLabel          = "app"
	groupLabel        = "group"
	chainLabelPrefix  = "chain-"
	cpuUsageKey       = "cpu-usage"
	memoryUsageKey    = "memory-usage"
	networkLatencyKey = "network-latency."
)

func GetOwnerDeployment(ctx context.Context, handle framework.Handle, pod *v1.Pod) (*appsv1.Deployment, error) {
	replicaSetOwner := metav1.GetControllerOf(pod)
	if replicaSetOwner == nil || replicaSetOwner.Kind != "ReplicaSet" {
		return nil, fmt.Errorf("pod %s/%s is not controlled by a ReplicaSet", pod.Namespace, pod.Name)
	}

	replicaSet, err := handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, replicaSetOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting ReplicaSet %s/%s for pod %s/%s: %w", pod.Namespace, replicaSetOwner.Name, pod.Namespace, pod.Name, err)
	}

	deploymentOwner := metav1.GetControllerOf(replicaSet)
	if deploymentOwner == nil || deploymentOwner.Kind != "Deployment" {
		return nil, fmt.Errorf("replicaSet %s/%s is not controlled by a Deployment", replicaSet.Namespace, replicaSet.Name)
	}

	deployment, err := handle.ClientSet().AppsV1().Deployments(replicaSet.Namespace).Get(ctx, deploymentOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting Deployment %s/%s for ReplicaSet %s/%s: %w", replicaSet.Namespace, deploymentOwner.Name, replicaSet.Namespace, replicaSet.Name, err)
	}

	return deployment, nil
}

func sameGroup(pod *v1.Pod, peerPod *v1.Pod) bool {
	group, ok := pod.GetLabels()[groupLabel]
	if !ok {
		klog.Infof("error getting group label for pod %s", pod.Name)
		return false
	}

	peerGroup, ok := peerPod.GetLabels()[groupLabel]
	if !ok {
		klog.Infof("error getting group label for pod %s", peerPod.Name)
		return false
	}

	if group != peerGroup {
		klog.Infof("pods %s and %s do not belong to the same group", pod.Name, peerPod.Name)
		return false
	}

	return true
}

func parseAnnotationFloat(annotations map[string]string, key, objectKind, objectName string) float64 {
	value, ok := annotations[key]
	if !ok {
		klog.Infof("%q annotation not found on %s %s", key, objectKind, objectName)
		return 0.0
	}

	parsedValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		klog.Infof("error parsing %q annotation of %s %s", key, objectKind, objectName)
		return 0.0
	}

	return parsedValue
}

func AreLesserOrderPodsScheduled(ctx context.Context, handle framework.Handle, pod *v1.Pod) bool {
	namespace := pod.GetNamespace()

	group, ok := pod.GetLabels()[groupLabel]
	if !ok {
		klog.Infof("error getting group label for pod %s", pod.Name)
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, chainLabelPrefix) {
			index, err := strconv.Atoi(value)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return false
			}

			if index > 0 {
				listOptions := metav1.ListOptions{
					LabelSelector: labels.Set{
						groupLabel: group,
						key:        strconv.Itoa(index - 1),
					}.String(),
				}
				lesserOrderPods, err := handle.ClientSet().CoreV1().Pods(namespace).List(ctx, listOptions)
				if err != nil {
					klog.Infof("error getting lesser order pods for pod %s", pod.Name)
					return false
				}

				if len(lesserOrderPods.Items) == 0 {
					return false
				}

				for _, lesserOrderPod := range lesserOrderPods.Items {
					if lesserOrderPod.Spec.NodeName == "" {
						return false
					}
				}
			}
		}
	}

	return true
}

func ArePodsNeighbors(pod *v1.Pod, peerPod *v1.Pod) bool {
	if !sameGroup(pod, peerPod) {
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, chainLabelPrefix) {
			index, err := strconv.Atoi(value)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return false
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.Atoi(peerValue)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
					return false
				}
				if index-peerIndex == 1 || peerIndex-index == 1 {
					klog.Infof("Pods %s and %s are neighbors", pod.Name, peerPod.Name)
					return true
				}
			}
		}
	}

	return false
}

func GetSharedChainsSlos(pod *v1.Pod, peerPod *v1.Pod) []float64 {
	var chainsSlos []float64

	if !sameGroup(pod, peerPod) {
		return chainsSlos
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, chainLabelPrefix) {
			index, err := strconv.Atoi(value)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return chainsSlos
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.Atoi(peerValue)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
					return chainsSlos
				}
				if index-peerIndex == 1 || peerIndex-index == 1 {
					klog.Infof("pods %s and %s are neighbors", pod.Name, peerPod.Name)

					chainSlo := parseAnnotationFloat(pod.GetAnnotations(), key+"-slo", "pod", pod.Name)
					if chainSlo == 0.0 {
						return chainsSlos
					}

					chainsSlos = append(chainsSlos, chainSlo)
				}
			}
		}
	}

	return chainsSlos
}

func GetAppCpuUsage(ctx context.Context, handle framework.Handle, pod *v1.Pod) float64 {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("error getting owner deployment for Pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	return parseAnnotationFloat(deployment.Annotations, cpuUsageKey, "deployment", deployment.Name)
}

func GetAppMemoryUsage(ctx context.Context, handle framework.Handle, pod *v1.Pod) float64 {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("error getting owner deployment for Pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	return parseAnnotationFloat(deployment.Annotations, memoryUsageKey, "deployment", deployment.Name)
}

func GetAppRequestsPerSecond(_ context.Context, _ framework.Handle, pod *v1.Pod, peerPod *v1.Pod) float64 {
	if !sameGroup(pod, peerPod) {
		return 0.0
	}

	peerApp, ok := peerPod.GetLabels()[appLabel]
	if !ok {
		klog.Infof("error getting app label for pod %s", peerPod.Name)
		return 0.0
	}

	return parseAnnotationFloat(pod.GetAnnotations(), "rps."+peerApp, "pod", pod.Name)
}

func GetAppTraffic(ctx context.Context, handle framework.Handle, pod *v1.Pod, peerPod *v1.Pod) float64 {
	if !sameGroup(pod, peerPod) {
		return 0.0
	}

	peerApp, ok := peerPod.GetLabels()[appLabel]
	if !ok {
		klog.Infof("error getting app label for pod %s", peerPod.Name)
		return 0.0
	}

	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	return parseAnnotationFloat(deployment.Annotations, "traffic."+peerApp, "deployment", deployment.Name)
}

func GetNodeCpuUsage(node *v1.Node) float64 {
	return parseAnnotationFloat(node.Annotations, cpuUsageKey, "node", node.Name)
}

func GetNodeMemoryUsage(node *v1.Node) float64 {
	return parseAnnotationFloat(node.Annotations, memoryUsageKey, "node", node.Name)
}

func GetNodeLatency(node *v1.Node, peerNode *v1.Node) float64 {
	return parseAnnotationFloat(node.Annotations, networkLatencyKey+peerNode.Name, "node", node.Name)
}
