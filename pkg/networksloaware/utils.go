package networksloaware

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func GetOwnerDeployment(ctx context.Context, handle framework.Handle, pod *v1.Pod) (*appsv1.Deployment, error) {
	if len(pod.OwnerReferences) == 0 || pod.OwnerReferences[0].Kind != "ReplicaSet" {
		klog.Infof("no owner replicaSet for pod %s", pod.Name)
		return nil, fmt.Errorf("no owner replicaSet for pod %s", pod.Name)
	}

	replicaSet, err := handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, pod.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		klog.Infof("error in getting pod %s replicaSet", pod.Name)
		return nil, fmt.Errorf("error in getting pod %s replicaSet", pod.Name)
	}

	if len(replicaSet.OwnerReferences) == 0 {
		klog.Infof("no owner deployment for replicaSet %s", pod.Name)
		return nil, fmt.Errorf("no owner deployment for replicaSet %s", pod.Name)
	}

	deployment, err := handle.ClientSet().AppsV1().Deployments(replicaSet.Namespace).Get(ctx, replicaSet.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		klog.Infof("error in getting replicaSet %s deployment", pod.Name)
		return nil, fmt.Errorf("error in getting replicaSet %s deployment", pod.Name)
	}

	return deployment, nil
}

func ArePodsNeighbors(ctx context.Context, handle framework.Handle, pod *v1.Pod, peerPod *v1.Pod) bool {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
		return false
	}

	peerDeployment, err := GetOwnerDeployment(ctx, handle, peerPod)
	if err != nil {
		klog.Infof("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
		return false
	}

	appGroup, ok := deployment.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for deployment %s", deployment.Name)
		return false
	}

	peerAppGroup, ok := peerDeployment.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for deployment %s", peerDeployment.Name)
		return false
	}

	if appGroup != peerAppGroup {
		return false
	}

	for key, value := range deployment.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for deployment %s", deployment.Name)
			}

			peerValue, ok := peerDeployment.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for deployment %s", peerDeployment.Name)
				}
				if int64(math.Abs(index-peerIndex)) == 1 {
					klog.Infof("Pods %s and %s are neighbors", pod.Name, peerPod.Name)
					return true
				}
			}
		}
	}

	return false
}

func GetChainSloSum(ctx context.Context, handle framework.Handle, pod *v1.Pod, peerPod *v1.Pod) (float64, error) {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
		return 0.0, fmt.Errorf("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
	}

	peerDeployment, err := GetOwnerDeployment(ctx, handle, peerPod)
	if err != nil {
		klog.Infof("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
		return 0.0, fmt.Errorf("error getting owner deployment for pod %s: %s", pod.Name, err.Error())
	}

	appGroup, ok := deployment.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for deployment %s", deployment.Name)
		return 0.0, fmt.Errorf("error getting app-group label for deployment %s", deployment.Name)
	}

	peerAppGroup, ok := peerDeployment.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for deployment %s", peerDeployment.Name)
		return 0.0, fmt.Errorf("error getting app-group label for deployment %s", peerDeployment.Name)
	}

	if appGroup != peerAppGroup {
		klog.Infof("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
		return 0.0, fmt.Errorf("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
	}

	var chainSloSum float64

	for key, value := range deployment.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for deployment %s", deployment.Name)
				return 0.0, fmt.Errorf("error parsing chain label value for deployment %s", deployment.Name)
			}

			peerValue, ok := peerDeployment.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for deployment %s", peerDeployment.Name)
				}
				if int64(math.Abs(index-peerIndex)) == 1 {
					klog.Infof("Pods %s and %s are neighbors", pod.Name, peerPod.Name)

					chainSloAnnotation, ok := deployment.GetAnnotations()[key+"-slo"]
					if !ok {
						klog.Infof("error getting %s annotation for deployment %s", key+"-slo", deployment.Name)
						return 0.0, fmt.Errorf("error getting %s annotation for deployment %s", key+"-slo", deployment.Name)
					}

					chainSlo, err := strconv.ParseFloat(chainSloAnnotation, 64)
					if err != nil {
						klog.Infof("error parsing %s annotation for deployment %s", key+"-slo", deployment.Name)
						return 0.0, fmt.Errorf("error parsing %s annotation for deployment %s", key+"-slo", deployment.Name)
					}

					chainSloSum += chainSlo
				}
			}
		}
	}

	return chainSloSum, nil
}

func GetNodeLatency(node *v1.Node, peerNode *v1.Node) (float64, error) {
	latencyAnnotation, ok := node.Annotations["network-latency."+peerNode.Name]
	if !ok {
		klog.Infof("\"network-latency.%s\" annotation not found on node %s", peerNode.Name, node.Name)
		return 0.0, fmt.Errorf("\"network-latency.%s\" annotation not found on node %s", peerNode.Name, node.Name)
	}

	latency, err := strconv.ParseFloat(latencyAnnotation, 64)
	if err != nil {
		klog.Infof("error parsing \"network-latency.%s\" annotation of node %s", peerNode.Name, node.Name)
		return 0.0, fmt.Errorf("error parsing \"network-latency.%s\" annotation of node %s", peerNode.Name, node.Name)
	}

	return latency, nil
}
