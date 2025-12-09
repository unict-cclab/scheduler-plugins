package sortinggpuaware

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	Name = "SortingGpuAware"
)
// SortingGpuAware implements QueueSortPlugin for GPU-first scheduling.
type SortingGpuAware struct{}

var _ framework.QueueSortPlugin = &SortingGpuAware{}

func (pl *SortingGpuAware) Name() string {
	return Name
}

// Less determines the order of pods in the scheduling queue.
// 1) Higher GPU slice request → first
// 2) Higher PriorityClass factor → first
// 3) FIFO fallback (older pod wins)
func (pl *SortingGpuAware) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	p1 := pInfo1.Pod
	p2 := pInfo2.Pod

	//
	// 1) Compare GPU usage (nvidia.com/gpu.shared)
	//
	g1 := gpuSlicesRequested(p1)
	g2 := gpuSlicesRequested(p2)

	// if g1 != g2 {
	// 	return g1 > g2
	// }
	if g1 != g2 {
		result := g1 > g2
		klog.Infof("[SortingGpuAware] Compare GPU: %s(gpu=%d) vs %s(gpu=%d) → %v",
			p1.Name, g1, p2.Name, g2, result)
		return result
	}
	//
	// 2) Compare PriorityClass factor
	//
	f1 := priorityFactorFromClass(p1)
	f2 := priorityFactorFromClass(p2)

	// if f1 != f2 {
	// 	return f1 > f2
	// }
	if f1 != f2 {
		result := f1 > f2
		klog.Infof("[SortingGpuAware] Compare Priority: %s(prio=%.1f) vs %s(prio=%.1f) → %v",
			p1.Name, f1, p2.Name, f2, result)
		return result
	}

	// 3) FIFO fallback
	if !p1.CreationTimestamp.Equal(&p2.CreationTimestamp) {
		result := p1.CreationTimestamp.Before(&p2.CreationTimestamp)
		klog.Infof("[SortingGpuAware] FIFO: %s(ts=%s) vs %s(ts=%s) → %v",
			p1.Name, p1.CreationTimestamp.String(),
			p2.Name, p2.CreationTimestamp.String(),
			result)
		return result
	}

	// 4) FINAL TIE-BREAKER → per garantire ordine deterministico
	result := string(p1.UID) < string(p2.UID)
	klog.Infof("[SortingGpuAware] TIE BREAK (UID): %s vs %s → %v",
		p1.Name, p2.Name, result)

	return result
}

//
// ----------------- HELPERS -----------------
//

// gpuSlicesRequested sums nvidia.com/gpu.shared across all containers.
func gpuSlicesRequested(pod *v1.Pod) int64 {
	var total int64
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Requests["nvidia.com/gpu.shared"]; ok {
			total += q.Value()
		}
	}
	return total
}

// priorityFactorFromClass assigns a custom QoS weight from PriorityClassName.
func priorityFactorFromClass(pod *v1.Pod) float64 {
	priorityFactor := map[string]float64{
		"low-qos":    1.0,
		"normal-qos": 1.5,
		"high-qos":   2.0,
	}

	if f, ok := priorityFactor[pod.Spec.PriorityClassName]; ok {
		return f
	}

	return 1.0 // default neutral priority
}

//
// -------------- PLUGIN REGISTRATION --------------
//

func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.Infof("[SortingGpuAware] Plugin loaded")
	return &SortingGpuAware{}, nil
}