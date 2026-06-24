package architectureaware

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/config"
)

const (
	Name                      = "ArchitectureAware"
	LabelKey                  = "nvidia.com/device-plugin.config"
	backendsPathAnnotationKey = "localai-backends-path"
	workerArgsAnnotationKey   = "localai-worker-args"
)

type ArchMapping struct {
	LabelValue   string   `json:"labelValue"`
	Tag          string   `json:"tag"`
	BackendsPath string   `json:"backendsPath,omitempty"`
	Args         []string `json:"args,omitempty"`
}

type ArchitectureAware struct {
	handle   framework.Handle
	mappings map[string]ArchMapping
}

var _ = framework.PreBindPlugin(&ArchitectureAware{})

func (pl *ArchitectureAware) Name() string {
	return Name
}

func replaceImageTag(image, newTag string) string {
	if newTag == "" {
		return image
	}
	slashIdx := strings.LastIndex(image, "/")
	tagSearchStart := slashIdx + 1
	colonIdx := strings.LastIndex(image[tagSearchStart:], ":")
	if colonIdx == -1 {
		return image + ":" + newTag
	}
	return image[:tagSearchStart+colonIdx] + ":" + newTag
}

func (pl *ArchitectureAware) PreBind(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	klog.Infof("[ArchitectureAware] PreBind: Pod %s/%s to node %s", pod.Namespace, pod.Name, nodeName)

	client := pl.handle.ClientSet()

	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		msg := fmt.Sprintf("Failed to find node %s: %v", nodeName, err)
		klog.Error(msg)
		return framework.NewStatus(framework.Error, msg)
	}

	deviceType := node.Labels[LabelKey]
	klog.Infof("[ArchitectureAware] Node %s has label %s=%s", nodeName, LabelKey, deviceType)

	mapping, ok := pl.mappings[deviceType]
	if !ok || mapping.Tag == "" {
		klog.Warningf("[ArchitectureAware] No mapping for device type %q", deviceType)
		return framework.NewStatus(framework.Success, "")
	}

	patchable := map[string]bool{"nn": true, "local-ai": true, "worker": true}
	patchOps := []map[string]interface{}{}

	// Patch immagine
	for i, c := range pod.Spec.Containers {
		if !patchable[c.Name] {
			continue
		}
		newImage := replaceImageTag(c.Image, mapping.Tag)
		if newImage != c.Image {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "replace",
				"path":  fmt.Sprintf("/spec/containers/%d/image", i),
				"value": newImage,
			})
			klog.Infof("[ArchitectureAware] container[%d]=%s image: %s → %s", i, c.Name, c.Image, newImage)
		}
	}

	// Patch backends path annotation
	if mapping.BackendsPath != "" && pod.Annotations[backendsPathAnnotationKey] != mapping.BackendsPath {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "replace",
			"path":  "/metadata/annotations/" + backendsPathAnnotationKey,
			"value": mapping.BackendsPath,
		})
		klog.Infof("[ArchitectureAware] annotation %s: %q → %q",
			backendsPathAnnotationKey, pod.Annotations[backendsPathAnnotationKey], mapping.BackendsPath)
	}

	// Patch worker-args annotation — solo per pod con role=worker
	role := pod.Labels["role"]
	if role == "worker" && len(mapping.Args) > 0 {
		argsStr := strings.Join(mapping.Args, " ")
		if pod.Annotations[workerArgsAnnotationKey] != argsStr {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "replace",
				"path":  "/metadata/annotations/" + workerArgsAnnotationKey,
				"value": argsStr,
			})
			klog.Infof("[ArchitectureAware] annotation %s: %q → %q",
				workerArgsAnnotationKey, pod.Annotations[workerArgsAnnotationKey], argsStr)
		}
	}

	if len(patchOps) == 0 {
		klog.Infof("[ArchitectureAware] No changes needed for Pod %s/%s", pod.Namespace, pod.Name)
		return framework.NewStatus(framework.Success, "")
	}

	patchBytes, _ := json.Marshal(patchOps)
	_, err = client.CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{},
	)
	if err != nil {
		msg := fmt.Sprintf("Failed to patch Pod %s/%s: %v", pod.Namespace, pod.Name, err)
		klog.Error(msg)
		return framework.NewStatus(framework.Error, msg)
	}

	klog.Infof("[ArchitectureAware] Applied %d patch op(s) to Pod %s/%s", len(patchOps), pod.Namespace, pod.Name)
	return framework.NewStatus(framework.Success, "")
}

func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.ArchitectureAwareArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type ArchitectureAwareArgs, got %T", obj)
	}

	mappings := make(map[string]ArchMapping)
	for _, m := range args.Mappings {
		if m.LabelValue != "" && m.Tag != "" {
			mappings[m.LabelValue] = ArchMapping{
				LabelValue:   m.LabelValue,
				Tag:          m.Tag,
				BackendsPath: m.BackendsPath,
				Args:         m.Args,
			}
		}
	}

	pl := &ArchitectureAware{
		handle:   handle,
		mappings: mappings,
	}

	klog.Infof("[ArchitectureAware] Loaded mappings: %+v", mappings)
	return pl, nil
}