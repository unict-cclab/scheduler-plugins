package sophos

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	appLabel            = "app"
	groupLabel          = "group"
	indexLabel          = "index"
	networkLatencyKey   = "network-latency."
	networkBandwidthKey = "network-bandwidth."
	packetLossKey       = "packet-loss."
)

func GetOwnerDeployment(ctx context.Context, handle fwk.Handle, pod *v1.Pod) (*appsv1.Deployment, error) {
	replicaSetOwner := metav1.GetControllerOf(pod)
	if replicaSetOwner == nil || replicaSetOwner.Kind != "ReplicaSet" {
		return nil, fmt.Errorf("pod %s/%s is not controlled by a ReplicaSet", pod.Namespace, pod.Name)
	}

	replicaSet, err := handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, replicaSetOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get ReplicaSet %s/%s: %w", pod.Namespace, replicaSetOwner.Name, err)
	}

	deploymentOwner := metav1.GetControllerOf(replicaSet)
	if deploymentOwner == nil || deploymentOwner.Kind != "Deployment" {
		return nil, fmt.Errorf("ReplicaSet %s/%s is not controlled by a Deployment", replicaSet.Namespace, replicaSet.Name)
	}

	deployment, err := handle.ClientSet().AppsV1().Deployments(replicaSet.Namespace).Get(ctx, deploymentOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get Deployment %s/%s: %w", replicaSet.Namespace, deploymentOwner.Name, err)
	}
	return deployment, nil
}

func SameGroup(pod, peerPod *v1.Pod) bool {
	group, ok := pod.Labels[groupLabel]
	if !ok {
		return false
	}
	peerGroup, ok := peerPod.Labels[groupLabel]
	return ok && group == peerGroup
}

func GetPodIndex(pod *v1.Pod) (int, bool) {
	value, ok := pod.Labels[indexLabel]
	if !ok {
		return 0, false
	}
	index, err := strconv.Atoi(value)
	return index, err == nil
}

func HasLowerOrEqualIndex(pod, peerPod *v1.Pod) bool {
	index, ok := GetPodIndex(pod)
	if !ok {
		return true
	}
	peerIndex, ok := GetPodIndex(peerPod)
	return ok && peerIndex <= index
}

func ParseAnnotationFloat(annotations map[string]string, key string) float64 {
	value, ok := annotations[key]
	if !ok {
		return 0
	}
	parsedValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsedValue
}

func GetAppTrafficFromDeployment(deployment *appsv1.Deployment, peerPod *v1.Pod) float64 {
	if deployment == nil {
		return 0
	}
	peerApp, ok := peerPod.Labels[appLabel]
	if !ok {
		return 0
	}
	return ParseAnnotationFloat(deployment.Annotations, "traffic."+peerApp)
}

func GetNodeLatency(node, peerNode *v1.Node) float64 {
	return ParseAnnotationFloat(node.Annotations, networkLatencyKey+peerNode.Name)
}

func GetNodeBandwidth(node, peerNode *v1.Node) float64 {
	return ParseAnnotationFloat(node.Annotations, networkBandwidthKey+peerNode.Name)
}

func GetNodePacketLoss(node, peerNode *v1.Node) float64 {
	return ParseAnnotationFloat(node.Annotations, packetLossKey+peerNode.Name)
}
