// Package nas implements the scoring phase of the Network-Aware Scheduler
// baseline. It minimizes network cost to communicating peer pods without
// weighting that cost by the amount of traffic exchanged.
package nas

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name      = "Nas"
	logPrefix = "[sophos][Nas]"

	preScoreStateKey = "PreScore" + Name
)

type Nas struct {
	handle framework.Handle
}

type preScoreState struct {
	peers         []peerPlacement
	metricsByNode map[string][]nodeNetworkMetrics
	maxMetrics    nodeNetworkMetrics
}

type nodeNetworkMetrics struct {
	latency    float64
	bandwidth  float64
	packetLoss float64
	zeroCost   bool
}

type peerPlacement struct {
	node *v1.Node
}

func (s *preScoreState) Clone() framework.StateData {
	clone := &preScoreState{
		peers:         make([]peerPlacement, len(s.peers)),
		metricsByNode: make(map[string][]nodeNetworkMetrics, len(s.metricsByNode)),
		maxMetrics:    s.maxMetrics,
	}
	copy(clone.peers, s.peers)
	for nodeName, metrics := range s.metricsByNode {
		clone.metricsByNode[nodeName] = append([]nodeNetworkMetrics(nil), metrics...)
	}
	return clone
}

var _ = framework.PreScorePlugin(&Nas{})
var _ = framework.ScorePlugin(&Nas{})

func (pl *Nas) Name() string { return Name }

func (pl *Nas) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("error listing pods in namespace %q: %v", pod.Namespace, err))
	}

	deployment, err := sophos.GetOwnerDeployment(ctx, pl.handle, pod)
	if err != nil {
		klog.Infof("%s cannot get traffic annotations for pod %s/%s: %v", logPrefix, pod.Namespace, pod.Name, err)
	}

	clusterNodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("error getting cluster nodes info: %v", err))
	}
	nodeByName := make(map[string]*v1.Node, len(clusterNodes))
	for _, nodeInfo := range clusterNodes {
		if nodeInfo.Node() != nil {
			nodeByName[nodeInfo.Node().Name] = nodeInfo.Node()
		}
	}

	peers := make([]peerPlacement, 0, len(pods.Items))
	for i := range pods.Items {
		peerPod := &pods.Items[i]
		if peerPod.Spec.NodeName == "" || !sophos.SameGroup(pod, peerPod) {
			continue
		}
		peerNode, ok := nodeByName[peerPod.Spec.NodeName]
		if !ok || sophos.GetAppTrafficFromDeployment(deployment, peerPod) <= 0 {
			continue
		}
		peers = append(peers, peerPlacement{node: peerNode})
	}

	metricsByNode := make(map[string][]nodeNetworkMetrics, len(nodes))
	maxMetrics := nodeNetworkMetrics{}
	for _, nodeInfo := range nodes {
		candidate := nodeInfo.Node()
		if candidate == nil {
			continue
		}
		metricsForNode := make([]nodeNetworkMetrics, 0, len(peers))
		for _, peer := range peers {
			metrics := nodeNetworkMetrics{}
			// Communication within one node does not traverse the network.
			if candidate.Name == peer.node.Name {
				metrics.zeroCost = true
			} else {
				metrics.latency = sophos.GetNodeLatency(candidate, peer.node)
				metrics.bandwidth = sophos.GetNodeBandwidth(candidate, peer.node)
				metrics.packetLoss = sophos.GetNodePacketLoss(candidate, peer.node)
			}
			maxMetrics.latency = math.Max(maxMetrics.latency, metrics.latency)
			maxMetrics.bandwidth = math.Max(maxMetrics.bandwidth, metrics.bandwidth)
			maxMetrics.packetLoss = math.Max(maxMetrics.packetLoss, metrics.packetLoss)
			metricsForNode = append(metricsForNode, metrics)
		}
		metricsByNode[candidate.Name] = metricsForNode
	}

	state.Write(preScoreStateKey, &preScoreState{
		peers:         peers,
		metricsByNode: metricsByNode,
		maxMetrics:    maxMetrics,
	})
	return nil
}

func (pl *Nas) Score(_ context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	rawState, err := state.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error reading %s state: %v", Name, err))
	}
	preScore, ok := rawState.(*preScoreState)
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("invalid %s state type %T", Name, rawState))
	}
	metrics, ok := preScore.metricsByNode[nodeName]
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("network metrics for node %q not found in %s state", nodeName, Name))
	}

	cost := 0.0
	for i := range preScore.peers {
		if i >= len(metrics) {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("network metrics for node %q are incomplete in %s state", nodeName, Name))
		}
		cost += communicationCost(metrics[i], preScore.maxMetrics)
	}

	score := -int64(math.Round(cost))
	klog.Infof("%s raw score of node %q for pod %q: %d", logPrefix, nodeName, pod.Name, score)
	return score, nil
}

func (pl *Nas) ScoreExtensions() framework.ScoreExtensions { return pl }

func (pl *Nas) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		highest = max(highest, nodeScore.Score)
		lowest = min(lowest, nodeScore.Score)
	}

	oldRange := highest - lowest
	for i := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = (scores[i].Score - lowest) * (framework.MaxNodeScore - framework.MinNodeScore) / oldRange
		}
		klog.Infof("%s normalized score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, scores[i].Score)
	}
	return nil
}

func communicationCost(metrics, maxMetrics nodeNetworkMetrics) float64 {
	if metrics.zeroCost {
		return 0
	}

	latencyRatio := ratio(metrics.latency, maxMetrics.latency)
	bandwidthRatio := 1.0
	if maxMetrics.bandwidth > 0 {
		bandwidthRatio = ratio(metrics.bandwidth, maxMetrics.bandwidth)
	}
	packetLossRatio := ratio(metrics.packetLoss, maxMetrics.packetLoss)
	return latencyRatio + 1 - bandwidthRatio + packetLossRatio
}

func ratio(value, maximum float64) float64 {
	if value <= 0 || maximum <= 0 {
		return 0
	}
	return math.Min(value/maximum, 1)
}

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &Nas{handle: handle}, nil
}
