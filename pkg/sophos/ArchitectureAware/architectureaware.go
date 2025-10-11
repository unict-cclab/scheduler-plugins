package architectureaware

import (
	"context"
	"fmt"
	"encoding/json"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/apimachinery/pkg/types"
)

const (
	Name = "ArchitectureAware"
)

type ArchitectureAware struct {
	handle framework.Handle
}

var _ = framework.PreBindPlugin(&ArchitectureAware{})


func (pl *ArchitectureAware) Name() string {
	return Name
}
func (pl *ArchitectureAware) PreBind(ctx context.Context,  _ *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status{
    klog.Infof("[ArchitectureAware] PreBind: Pod %s/%s to node %s", pod.Namespace, pod.Name, nodeName)

	client := pl.handle.ClientSet()

	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		msg := fmt.Sprintf("Failed to find node %s: %v", nodeName, err)
		klog.Error(msg)
		return framework.NewStatus(framework.Error, msg)
	}
    //label to choose
	deviceType := node.Labels["nvidia.com/device-plugin.config"]
	klog.Infof("[ArchitectureAware] Node %s have label nvidia.com/device-plugin.config=%s", nodeName, deviceType)

	var newImage string
	switch deviceType {
	case "orin":
		newImage = "192.168.1.252:480/jetson/multicomponent_service:r36"
	case "nano":
		newImage = "192.168.1.252:480/jetson/multicomponent_service:latest"
	default:
		klog.Warningf("[ArchitectureAware] Node %s without valid label (%s), Image not modified", nodeName, deviceType)
		return framework.NewStatus(framework.Success, "")
	}

	podCopy := pod.DeepCopy()
	updated := false
	for i := range podCopy.Spec.Containers {
		if podCopy.Spec.Containers[i].Name == "nn" {
			oldImage := podCopy.Spec.Containers[i].Image
			podCopy.Spec.Containers[i].Image = newImage
			klog.Infof("[ArchitectureAware] Change container 'nn' image from %s → %s", oldImage, newImage)
			updated = true
			break
		}
	}


	if !updated {
		klog.Warningf("[ArchitectureAware] No container 'nn' find in the Pod %s/%s", pod.Namespace, pod.Name)
		return framework.NewStatus(framework.Success, "")
	}

	var containerIndex int = -1
	for i, c := range pod.Spec.Containers {
		if c.Name == "nn" {
			containerIndex = i
			break
		}
	}
	if containerIndex == -1 {
		klog.Warningf("[ArchitectureAware] No container 'nn' found in Pod %s/%s", pod.Namespace, pod.Name)
		return framework.NewStatus(framework.Success, "")
	}

	patchOps := []map[string]string{
		{
			"op":    "replace",
			"path":  fmt.Sprintf("/spec/containers/%d/image", containerIndex),
			"value": newImage,
		},
	}

	// Serializza la patch
	patchBytes, _ := json.Marshal(patchOps)

	_, err = client.CoreV1().Pods(pod.Namespace).Patch(
		ctx,
		pod.Name,
		types.JSONPatchType, // 👈 JSON patch, non StrategicMerge
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		msg := fmt.Sprintf("Failed to patch image on Pod %s/%s: %v", pod.Namespace, pod.Name, err)
		klog.Error(msg)
		return framework.NewStatus(framework.Error, msg)
	}

	klog.Infof("[ArchitectureAware] Patched Pod %s/%s with image %s on container 'nn'", pod.Namespace, pod.Name, newImage)
	return framework.NewStatus(framework.Success, "")

}

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pl := &ArchitectureAware{
		handle: handle,
	}
	return pl, nil
}
