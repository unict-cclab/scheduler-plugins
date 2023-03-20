package resourceaware

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"math"
	"os"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
	"strconv"
)

const (
	Name          = "ResourceAware"
	DefaultWeight = 0.5
)

type ResourceAware struct {
	handle framework.Handle
	weight float64
}

var _ = framework.ScorePlugin(&ResourceAware{})

func (pl *ResourceAware) Name() string {
	return Name
}

func (pl *ResourceAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("Scoring node %q for pod %q", nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	}

	totalCpuUsage := sophos.GetAppCpuUsage(ctx, pl.handle, pod)
	totalMemoryUsage := sophos.GetAppMemoryUsage(ctx, pl.handle, pod)

	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + node.Node().Name,
	})
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting pods scheduled on node %q", node.Node().Name))
	}

	for _, p := range pods.Items {
		totalCpuUsage += sophos.GetAppCpuUsage(ctx, pl.handle, &p)
		totalMemoryUsage += sophos.GetAppMemoryUsage(ctx, pl.handle, &p)
	}

	cpuScore := -totalCpuUsage * 100 / float64(node.Allocatable.MilliCPU)
	memoryScore := -totalMemoryUsage * 100 / float64(node.Allocatable.Memory)

	score := pl.weight*cpuScore + (1-pl.weight)*memoryScore

	return int64(score), nil
}

func (pl *ResourceAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *ResourceAware) NormalizeScore(_ context.Context, _ *framework.CycleState, _ *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
	}

	return nil
}

func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	var weight = DefaultWeight

	weight, err := strconv.ParseFloat(os.Getenv("RA_WEIGHT"), 64)
	if err != nil {
		klog.Infof("Defaulting weight parameter to 0.5")
	}

	pl := &ResourceAware{
		handle: h,
		weight: weight,
	}
	return pl, nil
}
