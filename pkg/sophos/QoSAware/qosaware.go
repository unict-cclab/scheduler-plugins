package qosaware

import (
	"context"
	"fmt"
	"math"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name = "QoSAware"
	LabelKey = "nvidia.com/device-plugin.config" // chiave fissa della label sui nodi
)

type QoSAware struct {
	handle framework.Handle
	mappings map[string]string // labelValue → factor

}

var _ = framework.ScorePlugin(&QoSAware{})
var _ = framework.ScoreExtensions(&QoSAware{})

func (pl *QoSAware) Name() string {
	return Name
}

// Score calcola il punteggio del nodo per il pod considerando:
// - slice GPU libere
// - performance del nodo
// - PriorityClass del pod
func (pl *QoSAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("Scoring node %q for pod %q", nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting node info: %v", err))
	}

	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error listing pods on node: %v", err))
	}

	// GPU richieste dal pod da schedulare
	var podGpuRequest int64 = 0
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Requests["nvidia.com/gpu.shared"]; ok {
			podGpuRequest += q.Value()
		}
	}

	// GPU già richieste dai pod sul nodo
	var totalGpuRequested int64 = 0
	for _, p := range pods.Items {
		for _, c := range p.Spec.Containers {
			if q, ok := c.Resources.Requests["nvidia.com/gpu.shared"]; ok {
				totalGpuRequested += q.Value()
			}
		}
	}

	// GPU totali richieste se scheduliamo il pod
	requestGpu := totalGpuRequested + podGpuRequest

	// Capacità GPU del nodo
	gpuCapacity, ok := node.Node().Status.Capacity["nvidia.com/gpu.shared"]
	if !ok || gpuCapacity.Value() == 0 {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %q has no GPU capacity", nodeName))
	}

	// Se il pod non può entrare, score = 0
	if requestGpu > gpuCapacity.Value() {
		return 0, nil
	}

	freeSlices := gpuCapacity.Value() - totalGpuRequested

	// Fattore di priorità del pod
	priorityFactor := map[string]float64{
		"low-qos":    1.0,
		"normal-qos": 1.5,
		"high-qos":   2.0,
	}
	factor := 1.0
	if f, ok := priorityFactor[pod.Spec.PriorityClassName]; ok {
		factor = f
	}

	// Fattore di performance del nodo
	//label to choose
	deviceType := node.Labels[LabelKey]
	perf := 1.0
	if pf, ok := pl.mappings[deviceType]; ok {
		perf = pf
	}

	// Score base
	score := int64(factor * perf * float64(freeSlices) / (1 + float64(len(pods.Items))))

	// --- Optional: controllo dei pod sottoutilizzati ---
	for _, p := range pods.Items {
		throughputPerPod := getThroughputMetric(p) // funzione stub, da implementare con Prometheus
		nodeMaxThroughput := estimateNodeMaxThroughput(nodeName)
		maxThroughputPerPod := nodeMaxThroughput / int64(len(pods.Items))
		relocationScore := (maxThroughputPerPod - throughputPerPod) * int64(perf)
		if relocationScore > 0 {
			klog.Infof("Pod %q on node %q is a candidate for rescheduling, relocationScore=%d", p.Name, nodeName, relocationScore)
			// segnala per rescheduling (controller esterno o annotazioni)
		}
	}

	return score, nil
}

func (pl *QoSAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *QoSAware) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
		klog.Infof("Normalized score of node %q for pod %q: %d", scores[i].Name, pod.Name, scores[i].Score)
	}
	return nil
}

// func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
// 	return &QoSAware{
// 		handle: handle,
// 	}, nil
// }
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
    args, ok := obj.(*config.QoSAwareArgs)
    if !ok {
        return nil, fmt.Errorf("want args to be of type QoSAwareArgs, got %T", obj)
    }


	// Trasforma la lista in una mappa
	mappings := make(map[string]string)
	for _, m := range args.Mappings {
		if m.LabelValue != "" && m. != "" {
			mappings[m.LabelValue] = m.Factor
		}
	}

	pl := &QoSAware{
		handle:   handle,
		mappings: mappings,
	}

	klog.Infof("[QoSAware] Loaded mappings: %+v", mappings)
	return pl, nil
}


// --- Stub functions ---
// TO_DO Per metriche prometheus
func getThroughputMetric(p v1.Pod) int64 {
	// TODO: leggi metriche RPS da Prometheus o altro sistema
	return 0
}

func estimateNodeMaxThroughput(nodeName string) int64 {
	// TODO: valori empirici o basati sul tipo di nodo
	if nodeName == "jetsonorigin" {
		return 800 // esempio
	}
	return 200 // esempio Nano
}
