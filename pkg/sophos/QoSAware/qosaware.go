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
	"sigs.k8s.io/scheduler-plugins/apis/config"
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
	totalSlices := gpuCapacity.Value()
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
	var nodeIP string
	for _, addr := range nodeObj.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			nodeIP = addr.Address
			break
		}
	}
	fit := float64(freeSlices) / float64(podGpuRequest)
	if fit < 0.1 {
		fit = 0.1
	}

	// Fit adjustment leggero (range 0.8 – 1.3)
	fitAdj := 1 + 0.2*(fit-1)
	if fitAdj < 0.8 {
		fitAdj = 0.8
	}
	if fitAdj > 1.3 {
		fitAdj = 1.3
	}
	nodeGpuUtil, _ := getNodeGPUUtil(nodeIP)
	if nodeGpuUtil < 0 || math.IsNaN(nodeGpuUtil) {
		nodeGpuUtil = 0
	}

	// Score base
	podPenalty := math.Exp(0.25 * float64(len(pods.Items))) 
	sliceUtil := float64(requestGpu) / float64(totalSlices)
	if sliceUtil > 1 {
		sliceUtil = 1 // evita overflow se la richiesta eccede
	}
	fragmentPenalty := float64(freeSlices) / float64(freeSlices+int64(len(pods.Items))) //indica quanto è frammentata la GPU
	if fragmentPenalty < 0.3 {
		fragmentPenalty = 0.3
	}

	sliceFactor := 1 - sliceUtil
	if sliceFactor < 0.05 {
		sliceFactor = 0.05 // clamp minimo per evitare score = 0
	}
	gpuBias := 1 + math.Log2(float64(podGpuRequest)) * (perf - 1)

	score := int64((perf * gpuBias) * sliceFactor * fitAdj * fragmentPenalty * (1 - nodeGpuUtil/100)/ (1 + podPenalty))

	// ---  controllo dei pod sottoutilizzati --- da usare per possibile rescheduling
	// gamma := 0.5
	// for _, p := range pods.Items {
	// 	throughputPerPod := getThroughputMetric(p) // es. RPS medio negli ultimi 60s
	// 	nodeMaxThroughput := estimateNodeMaxThroughput(nodeName, deviceType)
	// 	estGpuUtil, _ := estimatePodGPUUtil(p, nodeName, nodeIP)
	// 	utilFactor := 1.0 + gamma * (estGpuUtil / 100.0)

	// 	// ignora pod appena creati (<60s)
	// 	age := time.Since(p.CreationTimestamp.Time)
	// 	if age < 60*time.Second {
	// 		continue
	// 	}

	// 	// evita divisione per zero e considera il nuovo pod in ingresso
	// 	totalPods := int64(len(pods.Items) + 1)
	// 	maxThroughputPerPod := nodeMaxThroughput / totalPods

	// 	// considera anche un margine per variazioni normali di traffico
	// 	if throughputPerPod < (maxThroughputPerPod * 80 / 100) {
	// 		relocationScore := int64(float64(maxThroughputPerPod-throughputPerPod) * perf * utilFactor)
	// 		if relocationScore > 0 {
	// 			klog.Infof("Pod %q on node %q is a candidate for rescheduling (throughput=%d < expected=%d, relocationScore=%d)",
	// 				p.Name, nodeName, throughputPerPod, maxThroughputPerPod, relocationScore)
	// 			// TODO: segnalazione per rescheduling
	// 		}
	// 	}
	// }

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
		if m.LabelValue != "" {
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
		"orin": 800,
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
func getNodeGPUUtil(nodeIP string) (float64, error) {
    query := fmt.Sprintf(`avg_over_time(gpu_usage_percentage{job="jetson-exporter", instance=~"%s.*"}[60s])`, nodeIP)
    return queryPrometheus(query)
}

func getPodRPS(pod v1.Pod) (float64, error) {
    query := fmt.Sprintf(`sum(rate(http_requests_total{pod="%s"}[60s]))`, pod.Name)
    return queryPrometheus(query)
}

func getNodeTotalRPS(nodeName string) (float64, error) {
    query := fmt.Sprintf(`sum(rate(http_requests_total{node="%s"}[60s]))`, nodeName)
    return queryPrometheus(query)
}

func estimatePodGPUUtil(pod v1.Pod, nodeName, nodeIP string) (float64, error) {
    podRPS, err := getPodRPS(pod)
    if err != nil { return 0, err }

    totalRPS, err := getNodeTotalRPS(nodeName)
    if err != nil || totalRPS == 0 { return 0, nil }

    nodeGPU, err := getNodeGPUUtil(nodeIP)
    if err != nil { return 0, err }

    return nodeGPU * (podRPS / totalRPS), nil
}
func queryPrometheus(query string) (float64, error) {
    req, _ := http.NewRequest("GET", prometheusURL+"?query="+url.QueryEscape(query), nil)
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    req = req.WithContext(ctx)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return 0, err
    }

    var result struct {
        Data struct {
            Result []struct {
                Value [2]interface{} `json:"value"`
            } `json:"result"`
        } `json:"data"`
    }

    if err := json.Unmarshal(body, &result); err != nil {
        return 0, err
    }

    if len(result.Data.Result) == 0 {
        return 0, fmt.Errorf("no results for query %s", query)
    }

    valStr, ok := result.Data.Result[0].Value[1].(string)
    if !ok {
        return 0, fmt.Errorf("invalid value format for query %s", query)
    }

    valFloat, err := strconv.ParseFloat(valStr, 64)
    if err != nil {
        return 0, err
    }

    return valFloat, nil
}
