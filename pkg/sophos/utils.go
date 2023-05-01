package sophos

import (
	"context"
	"errors"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"strconv"
)

func GetOwnerDeployment(ctx context.Context, handle framework.Handle, pod *v1.Pod) (*appsv1.Deployment, error) {
	if len(pod.OwnerReferences) == 0 || pod.OwnerReferences[0].Kind != "ReplicaSet" {
		return nil, errors.New(fmt.Sprintf("No owner ReplicaSet for Pod %s \n", pod.Name))
	}

	replicaSet, err := handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, pod.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error in getting Pod %s ReplicaSet \n", pod.Name))
	}

	if len(replicaSet.OwnerReferences) == 0 {
		return nil, errors.New(fmt.Sprintf("No owner Deployment for ReplicaSet %s \n", pod.Name))
	}

	deployment, err := handle.ClientSet().AppsV1().Deployments(replicaSet.Namespace).Get(ctx, replicaSet.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error in getting ReplicaSet %s Deployment \n", pod.Name))
	}

	return deployment, nil
}

func GetAppCpuUsage(ctx context.Context, handle framework.Handle, pod *v1.Pod) float64 {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("Error getting owner Deployment for Pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	cpuUsageAnnotation, ok := deployment.Annotations["cpu-usage"]
	if !ok {
		klog.Infof("\"cpu-usage\" annotation not found on Deployment %s", deployment.Name)
		return 0.0
	}

	cpuUsage, err := strconv.ParseFloat(cpuUsageAnnotation, 64)
	if err != nil {
		klog.Infof("Error parsing \"cpu-usage\" annotation of Deployment %s", deployment.Name)
		return 0.0
	}

	return cpuUsage
}

func GetAppMemoryUsage(ctx context.Context, handle framework.Handle, pod *v1.Pod) float64 {
	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("Error getting owner Deployment for Pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	memoryUsageAnnotation, ok := deployment.Annotations["memory-usage"]
	if !ok {
		klog.Infof("\"memory-usage\" annotation not found on Deployment %s", deployment.Name)
		return 0.0
	}

	memoryUsage, err := strconv.ParseFloat(memoryUsageAnnotation, 64)
	if err != nil {
		klog.Infof("Error parsing \"memory-usage\" annotation of Deployment %s", deployment.Name)
		return 0.0
	}

	return memoryUsage
}

func GetAppTraffic(ctx context.Context, handle framework.Handle, pod *v1.Pod, peerPod *v1.Pod) float64 {
	peerAppGroupName, ok := peerPod.Labels["app-group"]
	if !ok || peerAppGroupName != pod.Labels["app-group"] {
		return 0.0
	}

	peerAppName, ok := peerPod.Labels["app"]
	if !ok {
		klog.Infof("\"app\" annotation not found on Pod %s", peerPod.Name)
		return 0.0
	}

	deployment, err := GetOwnerDeployment(ctx, handle, pod)
	if err != nil {
		klog.Infof("Error getting owner Deployment for Pod %s: %s", pod.Name, err.Error())
		return 0.0
	}

	trafficAnnotation, ok := deployment.Annotations["traffic."+peerAppName]
	if !ok {
		klog.Infof("\"traffic.%s\" annotation not found on Deployment %s", deployment.Name)
		return 0.0
	}

	traffic, err := strconv.ParseFloat(trafficAnnotation, 64)
	if err != nil {
		klog.Infof("Error parsing \"traffic.%s\" annotation of Deployment %s", deployment.Name)
		return 0.0
	}

	return traffic
}

func GetLatency(node *v1.Node, peerNode *v1.Node) float64 {
	latencyAnnotation, ok := node.Annotations["network-latency."+peerNode.Name]
	if !ok {
		klog.Infof("\"network-latency.%s\" annotation not found on node %s", peerNode.Name, node.Name)
		return 0.0
	}

	latency, err := strconv.ParseFloat(latencyAnnotation, 64)
	if err != nil {
		klog.Infof("Error parsing \"network-latency.%s\" annotation of node %s", peerNode.Name, node.Name)
		return 0.0
	}

	return latency
}
