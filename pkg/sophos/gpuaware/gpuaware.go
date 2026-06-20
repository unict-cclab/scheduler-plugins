package gpuaware

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"math"
)

const (
	Name      = "GpuAware"
	logPrefix = "[sophos][GpuAware]"
)

type GpuAware struct {
	handle framework.Handle
}

var _ = framework.ScorePlugin(&GpuAware{})

func (pl *GpuAware) Name() string {
	return Name
}

func (pl *GpuAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("%s Scoring node %q for pod %q", logPrefix, nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	}
	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting pods scheduled on node %q: %v", nodeName, err))
	}

	var podGpuRequest int64 = 0
	for _, container := range pod.Spec.Containers {
		if gpuQuantity, ok := container.Resources.Requests["nvidia.com/gpu.shared"]; ok {
			podGpuRequest += gpuQuantity.Value()
		}
	}
	var totalGpuRequested int64 = 0
	for _, p := range pods.Items {
		for _, container := range p.Spec.Containers {
			if gpuQuantity, ok := container.Resources.Requests["nvidia.com/gpu.shared"]; ok {
				totalGpuRequested += gpuQuantity.Value()
			}
		}
	}

	RequestGpu := totalGpuRequested + podGpuRequest

	gpuCapacity, ok := node.Node().Status.Capacity["nvidia.com/gpu.shared"]
	if !ok || gpuCapacity.Value() == 0 {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %q has no GPU capacity", nodeName))
	}
	if RequestGpu > gpuCapacity.Value() {
		return 0, nil
	}
	if podGpuRequest == 0 && gpuCapacity.Value() == 0 {
		return int64(framework.MaxNodeScore), nil
	}
	NormalizedRequestGpu := float64(RequestGpu) / float64(gpuCapacity.Value())
	score := int64((1 - NormalizedRequestGpu) * float64(framework.MaxNodeScore))

	return score, nil
}

func (pl *GpuAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *GpuAware) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to the lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
		klog.Infof("%s Original score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, nodeScore.Score)
		klog.Infof("%s Normalized score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, scores[i].Score)
	}

	return nil
}

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pl := &GpuAware{
		handle: handle,
	}
	return pl, nil
}
