package qosaware


import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config"
)

const (
	Name = "QoSAware"
	LabelKey = "nvidia.com/device-plugin.config" // chiave fissa della label sui nodi
	prometheusURL = "http://prometheus-stack-kube-prom-prometheus.observability.svc.cluster.local:9090/api/v1/query"
)

type QoSAware struct {
	handle framework.Handle
	mappings map[string]float64// labelValue → factor

}

var _ = framework.ScorePlugin(&QoSAware{})
var _ = framework.ScoreExtensions(&QoSAware{})

func (pl *QoSAware) Name() string {
	return Name
}

// Score calcola il punteggio del nodo per il pod considerando:
// - shared-GPU libere
// - performance del nodo
// - PriorityClass del pod
func (pl *QoSAware) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("Scoring node %q for pod %q", nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting node info: %v", err))
	}
	nodeObj := node.Node()
	if nodeObj == nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node object is nil for %q", nodeName))
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
	gpuCapacity, ok := nodeObj.Status.Capacity["nvidia.com/gpu.shared"]
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
		// Fattore di performance del nodo (from mappings)
	deviceType := ""
	if v, ok := nodeObj.Labels[LabelKey]; ok {
		deviceType = v
	}
	perf := 1.0
	if pf, ok := pl.mappings[deviceType]; ok {
		perf = pf
	}

	// Score base
	score := int64(factor * perf * float64(freeSlices) / (1 + float64(len(pods.Items))))

	// --- Optional: controllo dei pod sottoutilizzati ---
	for _, p := range pods.Items {
		throughputPerPod := getThroughputMetric(p) // funzione che interroga Prometheus
		nodeMaxThroughput := estimateNodeMaxThroughput(nodeName, deviceType)
		if len(pods.Items) == 0 {
			continue
		}
		maxThroughputPerPod := nodeMaxThroughput / int64(len(pods.Items))
		// relocationScore as int64 derived from float perf
		relocationScore := int64(float64(maxThroughputPerPod-throughputPerPod) * perf)
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
	mappings := make(map[string]float64)
	for _, m := range args.Mappings {
		if m.LabelValue != "" && m.Factor != "" {
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


func getThroughputMetric(p v1.Pod) int64 {
    
    // label : nodo, pipeline_id, step_id, pod_name
    query := fmt.Sprintf(
        `rate(http_requests_total{pod="%s"}[1m])`,
        p.Name,
    )

    resp, err := http.Get(prometheusURL + "?query=" + url.QueryEscape(query))
    if err != nil {
        klog.Infof("Errore Prometheus: %v", err)
        return 0
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
	if err != nil {
		klog.Infof("Errore lettura body Prometheus: %v", err)
		return 0
	}

    // Estraggo il valore del primo risultato
    var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		klog.Infof("Errore parsing JSON: %v", err)
		return 0
	}


    // Safely extract value
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return 0
	}
	results, ok := data["result"].([]interface{})
	if !ok || len(results) == 0 {
		return 0
	}
	first, ok := results[0].(map[string]interface{})
	if !ok {
		return 0
	}
	valArr, ok := first["value"].([]interface{})
	if !ok || len(valArr) < 2 {
		return 0
	}
	valueStr, ok := valArr[1].(string)
	if !ok {
		return 0
	}
	throughputF, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0
	}
	return int64(throughputF)
}

func estimateNodeMaxThroughput(nodeName, deviceType string) int64 {
	// fallback statico
	staticMax := map[string]int64{
		"origin": 800,
		"nano":   200,
	}

	val, ok := queryPrometheusForThroughput(nodeName)
	if ok {
		return val
	}
	if v, ok := staticMax[deviceType]; ok {
		return v
	}
	// default fallback
	return 100
}

func queryPrometheusForThroughput(nodeName string) (int64, bool) {
	q := fmt.Sprintf("avg_over_time(http_requests_total{node=\"%s\"}[1m])", nodeName)
	req, _ := http.NewRequest("GET", prometheusURL+"?query="+url.QueryEscape(q), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false
	}
	var result struct {
		Data struct {
			Result []struct {
				Value [2]interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, false
	}

	if len(result.Data.Result) == 0 {
		return 0, false
	}

	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false
	}
	valFloat, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, false
	}
	return int64(valFloat), true
}