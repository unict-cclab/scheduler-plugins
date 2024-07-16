package networksloaware

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func AreLesserOrderPodsScheduled(ctx context.Context, handle framework.Handle, pod *v1.Pod) bool {
	namespace := pod.GetNamespace()

	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.Atoi(value)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return false
			}

			if index > 0 {
				labelSelector := metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app-group": appGroup,
						key:         strconv.Itoa(index - 1),
					},
				}
				listOptions := metav1.ListOptions{
					LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
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

func ArePodsNeighbors(ctx context.Context, handle framework.Handle, pod *v1.Pod, peerPod *v1.Pod) bool {
	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return false
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return false
	}

	if appGroup != peerAppGroup {
		return false
	}

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
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
	appGroup, ok := pod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", pod.Name)
		return 0.0, fmt.Errorf("error getting app-group label for pod %s", pod.Name)
	}

	peerAppGroup, ok := peerPod.GetLabels()["app-group"]
	if !ok {
		klog.Infof("error getting app-group label for pod %s", peerPod.Name)
		return 0.0, fmt.Errorf("error getting app-group label for pod %s", peerPod.Name)
	}

	if appGroup != peerAppGroup {
		klog.Infof("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
		return 0.0, fmt.Errorf("pods %s and %s do not belong to the same app group", pod.Name, peerPod.Name)
	}

	var chainSloSum float64

	for key, value := range pod.GetLabels() {
		if strings.HasPrefix(key, "chain-") {
			index, err := strconv.ParseFloat(value, 64)
			if err != nil {
				klog.Infof("error parsing chain label value for pod %s", pod.Name)
				return 0.0, fmt.Errorf("error parsing chain label value for pod %s", pod.Name)
			}

			peerValue, ok := peerPod.GetLabels()[key]
			if ok {
				peerIndex, err := strconv.ParseFloat(peerValue, 64)
				if err != nil {
					klog.Infof("error parsing chain label value for pod %s", peerPod.Name)
				}
				if int64(math.Abs(index-peerIndex)) == 1 {
					klog.Infof("pods %s and %s are neighbors", pod.Name, peerPod.Name)

					chainSloAnnotation, ok := pod.GetAnnotations()[key+"-slo"]
					if !ok {
						klog.Infof("error getting %s annotation for pod %s", key+"-slo", pod.Name)
						return 0.0, fmt.Errorf("error getting %s annotation for pod %s", key+"-slo", pod.Name)
					}

					chainSlo, err := strconv.ParseFloat(chainSloAnnotation, 64)
					if err != nil {
						klog.Infof("error parsing %s annotation for pod %s", key+"-slo", pod.Name)
						return 0.0, fmt.Errorf("error parsing %s annotation for pod %s", key+"-slo", pod.Name)
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
