package networkawarelocalai

import (
	"context"
	"fmt"
	"math"
	"os"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	Name      = "NetworkAwareLocalAi"
	logPrefix = "[sophos][NetworkAwareLocalAi]"
	defaultGatewayTrafficKey = "gateway-traffic"
	minTrafficWeight = 10.0
	preScoreStateKey         = "PreScore" + Name
)

type NetworkAwareLocalAi struct {
	handle framework.Handle
	gatewayTrafficKey  string
}

type preScoreState struct {
	role           string
	gatewayTraffic float64
	maxBandwidth float64

	// Master scoring: avgLatency per candidate node
	nodeAvgLatencyMs map[string]float64
	nodeAvgBandwidth map[string]float64

	// Worker scoring: master node name + latency per candidate + traffic to master
	masterNodeName   string
	nodeLatencyToMasterMs map[string]float64
	nodeBandwidthToMaster map[string]float64
	trafficToMaster  float64
}

var _ = framework.PreScorePlugin(&NetworkAwareLocalAi{})
var _ = framework.ScorePlugin(&NetworkAwareLocalAi{})

func (pl *NetworkAwareLocalAi) Name() string {
	return Name
}

func (s *preScoreState) Clone() framework.StateData {
	clone := &preScoreState{
		role:            s.role,
		gatewayTraffic:  s.gatewayTraffic,
		masterNodeName:  s.masterNodeName,
		trafficToMaster: s.trafficToMaster,
		nodeAvgLatencyMs:      make(map[string]float64, len(s.nodeAvgLatencyMs)),
		nodeAvgBandwidth:      make(map[string]float64, len(s.nodeAvgBandwidth)),
		nodeLatencyToMasterMs: make(map[string]float64, len(s.nodeLatencyToMasterMs)),
		nodeBandwidthToMaster: make(map[string]float64, len(s.nodeBandwidthToMaster)),
	}
	for k, v := range s.nodeAvgLatencyMs {
		clone.nodeAvgLatencyMs[k] = v
	}
	for k, v := range s.nodeAvgBandwidth {
		clone.nodeAvgBandwidth[k] = v
	}
	for k, v := range s.nodeLatencyToMasterMs {
		clone.nodeLatencyToMasterMs[k] = v
	}
	for k, v := range s.nodeBandwidthToMaster {
		clone.nodeBandwidthToMaster[k] = v
	}
	return clone
}

func (pl *NetworkAwareLocalAi) PreScore(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	role := pod.GetLabels()["role"]
	if role == "" {
		klog.Infof("%s pod %s has no role label, skipping PreScore", logPrefix, pod.Name)
		cycleState.Write(preScoreStateKey, &preScoreState{})
		return nil
	}

	group := pod.GetLabels()["group"]
	gatewayTraffic := sophos.GetGatewayTraffic(ctx, pl.handle, pod, pl.gatewayTrafficKey)

	s := &preScoreState{
		role:           role,
		gatewayTraffic: gatewayTraffic,
	}

	switch role {
		case "master":
			s.nodeAvgLatencyMs, s.nodeAvgBandwidth, s.maxBandwidth = pl.preScoreMaster(nodes)
		case "worker":
			s.nodeLatencyToMasterMs, s.nodeBandwidthToMaster, s.masterNodeName, s.trafficToMaster, s.maxBandwidth = pl.preScoreWorker(ctx, pod, group, nodes)
	}

	klog.Infof("%s PreScore done for %s (role=%s, group=%s, gwTraffic=%.2f)",
		logPrefix, pod.Name, role, group, gatewayTraffic)

	cycleState.Write(preScoreStateKey, s)
	return nil
}
func (pl *NetworkAwareLocalAi) preScoreMaster(nodes []*framework.NodeInfo) (map[string]float64, map[string]float64, float64) {	
	latencies := make(map[string]float64, len(nodes))
	bandwidths := make(map[string]float64, len(nodes))

	for _, candidateInfo := range nodes {
		candidate := candidateInfo.Node()
		if candidate == nil {
			continue
		}

		var totalLatency, totalBandwidth float64
		var latCount, bwCount float64
		for _, otherInfo := range nodes {
			other := otherInfo.Node()
			if other == nil || other.Name == candidate.Name {
				continue
			}

			lat := sophos.GetNodeLatency(candidate, other)
			if lat > 0 {
				totalLatency += lat
				latCount++
			}

			bw := sophos.GetNodeBandwidth(candidate, other)
			if bw > 0 {
				totalBandwidth += bw
				bwCount++
			}
		}

		avgLatMs := 0.0
		if latCount > 0 {
			avgLatMs = (totalLatency / latCount) * 1000
		}
		avgBw := 0.0
		if bwCount > 0 {
			avgBw = totalBandwidth / bwCount
		}
		latencies[candidate.Name] = avgLatMs
		bandwidths[candidate.Name] = avgBw

		klog.Infof("%s PreScore master: %s avgLatency=%.4fms avgBandwidth=%.0f B/s",
			logPrefix, candidate.Name, avgLatMs, avgBw)
	}
	maxBw := 0.0
	for _, bw := range bandwidths {
		if bw > maxBw {
			maxBw = bw
		}
	}
	return latencies, bandwidths, maxBw
}
func (pl *NetworkAwareLocalAi) preScoreWorker(ctx context.Context, pod *v1.Pod, group string, nodes []*framework.NodeInfo) (map[string]float64, map[string]float64, string, float64, float64) {
	latencies := make(map[string]float64, len(nodes))
	bandwidths := make(map[string]float64, len(nodes))
	
	// Find master node
	masterPods, err := pl.handle.ClientSet().CoreV1().Pods(pod.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			"group": group,
			"role":  "master",
		}.String(),
	})
	if err != nil || len(masterPods.Items) == 0 {
		klog.Infof("%s PreScore worker: no master found for group %s", logPrefix, group)
		return latencies, bandwidths, "", 0, 0
	}

	masterPod := &masterPods.Items[0]
	masterNodeName := masterPod.Spec.NodeName
	if masterNodeName == "" {
		klog.Infof("%s PreScore worker: master pod %s not yet scheduled", logPrefix, masterPod.Name)
		return latencies, bandwidths, "", 0, 0
	}

	// Find master node info for latency calculation
	var masterNode *v1.Node
	for _, ni := range nodes {
		if ni.Node() != nil && ni.Node().Name == masterNodeName {
			masterNode = ni.Node()
			break
		}
	}
	// Master might not be in candidate list, fetch separately
	if masterNode == nil {
		masterNodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(masterNodeName)
		if err == nil {
			masterNode = masterNodeInfo.Node()
		}
	}
	if masterNode == nil {
		klog.Infof("%s PreScore worker: master node %s not found", logPrefix, masterNodeName)
		return latencies, bandwidths, masterNodeName, 0, 0
	}

	// Compute latency from each candidate to master
	for _, candidateInfo := range nodes {
		candidate := candidateInfo.Node()
		if candidate == nil {
			continue
		}
		latency := sophos.GetNodeLatency(candidate, masterNode)
		latencyMs := latency * 1000
		latencies[candidate.Name] = latencyMs

		bandwidth := sophos.GetNodeBandwidth(candidate, masterNode)
		bandwidths[candidate.Name] = bandwidth

		klog.Infof("%s PreScore worker: %s → master(%s) latency=%.4fms bandwidth=%.4fms",
			logPrefix, candidate.Name, masterNodeName, latencyMs,bandwidth)
	}

	// Get traffic from worker deployment to master
	traffic := sophos.GetGroupTraffic(ctx, pl.handle, pod, masterPod)

	klog.Infof("%s PreScore worker: masterNode=%s traffic=%.0f",
		logPrefix, masterNodeName, traffic)
	maxBw := 0.0
	for _, bw := range bandwidths {
		if bw > maxBw {
			maxBw = bw
		}
	}
	return latencies, bandwidths, masterNodeName, traffic, maxBw
}

func (pl *NetworkAwareLocalAi) Score(_ context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	rawState, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error reading PreScore state: %v", err))
	}
	s, ok := rawState.(*preScoreState)
	if !ok {
		return 0, framework.NewStatus(framework.Error, "invalid PreScore state type")
	}
	
	// klog.Infof("%s scoring node %q for pod %q", logPrefix, nodeName, pod.Name)

	// node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	// if err != nil {
	// 	return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	// }

	var score int64
	// role := pod.GetLabels()["role"]

	switch s.role {
		case "master":
			score = pl.scoreMaster(s, nodeName)
		case "worker":
			score = pl.scoreWorker(s, nodeName)
	}

	return score, nil
}

func (pl *NetworkAwareLocalAi) scoreMaster(s *preScoreState, nodeName string) int64 {
	avgLatencyMs, ok := s.nodeAvgLatencyMs[nodeName]
	avgBandwidth := s.nodeAvgBandwidth[nodeName]
	if !ok || avgLatencyMs == 0 && avgBandwidth == 0  {
		return 0
	}

	weight := s.gatewayTraffic
	if weight < minTrafficWeight {
		weight = minTrafficWeight
	}

	score := -int64(avgLatencyMs * weight)
	if avgBandwidth > 0 && s.maxBandwidth > 0 {
		bwRatio := 1 - avgBandwidth/s.maxBandwidth
		score -= int64(bwRatio * weight)
	}
	klog.Infof("%s Score master %s: latency=%.4fms bw=%.0f B/s weight=%.0f score=%d",
		logPrefix, nodeName, avgLatencyMs, avgBandwidth, weight, score)

	return score
}

func (pl *NetworkAwareLocalAi) scoreWorker(s *preScoreState, nodeName string) int64 {
	// latency from node worker to master network-latency.<peerNode>
	latencyMs, ok := s.nodeLatencyToMasterMs[nodeName]
	bandwidth := s.nodeBandwidthToMaster[nodeName]
	if !ok || latencyMs == 0 && bandwidth == 0 {
		return 0
	}

	var score int64

	// traffic to master , calcolated with node_network_transmit_bytes_total, traffic of the worker from an interface
	if s.trafficToMaster > 0 {
		score -= int64(latencyMs * s.trafficToMaster)
	}

	// gateway traffic 
	weight := s.gatewayTraffic
	if weight < minTrafficWeight {
		weight = minTrafficWeight
	}
	score -= int64(latencyMs * weight)
	if bandwidth > 0 && s.maxBandwidth > 0 {
    	bwRatio := 1 - bandwidth/s.maxBandwidth  // 0 = best, 1 = worst
    	score -= int64(bwRatio * weight)
	}
	klog.Infof("%s Score worker %s: latency=%.4fms bw=%.0f B/s traffic=%.0f weight=%.0f score=%d",
		logPrefix, nodeName, latencyMs, bandwidth, s.trafficToMaster, weight, score)

	return score
}

// GetGroupPodNodes returns nodes where pod of a given role are running in the same group as the pod in input 
func (pl *NetworkAwareLocalAi) getGroupPodNodes(ctx context.Context, pod *v1.Pod, role string) []string {
	group, ok := pod.GetLabels()["group"]
	if !ok {
		return nil
	}

	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			"group": group,
			"role":  role,
		}.String(),
	})
	if err != nil || len(pods.Items) == 0 {
		return nil
	}

	seen := map[string]bool{}
	var nodes []string
	for _, p := range pods.Items {
		if p.Spec.NodeName != "" && !seen[p.Spec.NodeName] {
			seen[p.Spec.NodeName] = true
			nodes = append(nodes, p.Spec.NodeName)
		}
	}
	return nodes
}


func (pl *NetworkAwareLocalAi) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *NetworkAwareLocalAi) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
		klog.Infof("%s Normalized score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, scores[i].Score)
	}
	return nil
}


func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	key := os.Getenv("GATEWAY_TRAFFIC_KEY")
	if key == "" {
		key = defaultGatewayTrafficKey
	}
	klog.Infof("%s using gateway traffic annotation key: %s", logPrefix, key)
	return &NetworkAwareLocalAi{
		handle:            handle,
		gatewayTrafficKey: key,
	}, nil
}