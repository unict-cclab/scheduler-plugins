package networkaware

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name      = "NetworkAware"
	logPrefix = "[sophos][NetworkAware]"

	preScoreStateKey = "PreScore" + Name
)

type NetworkAware struct {
	handle                    framework.Handle
	ignoreSameZoneNetworkCost bool
}

type preScoreState struct {
	peers         []peerPlacement
	metricsByNode map[string][]nodeNetworkMetrics
	maxTraffic    float64
	maxMetrics    nodeNetworkMetrics
}

type nodeNetworkMetrics struct {
	latency    float64
	bandwidth  float64
	packetLoss float64
	zeroCost   bool
}

type peerPlacement struct {
	node    *v1.Node
	traffic float64
}

var _ = framework.QueueSortPlugin(&NetworkAware{})
var _ = framework.PreScorePlugin(&NetworkAware{})
var _ = framework.ScorePlugin(&NetworkAware{})

func (pl *NetworkAware) Name() string {
	return Name
}

func (pl *NetworkAware) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	p1 := pInfo1.Pod
	p2 := pInfo2.Pod

	index1, ok1 := sophos.GetPodIndex(p1)
	index2, ok2 := sophos.GetPodIndex(p2)
	if ok1 && ok2 && index1 != index2 {
		return index1 < index2
	}

	return (&queuesort.PrioritySort{}).Less(pInfo1, pInfo2)
}

func (s *preScoreState) Clone() framework.StateData {
	clone := &preScoreState{
		peers:         make([]peerPlacement, len(s.peers)),
		metricsByNode: make(map[string][]nodeNetworkMetrics, len(s.metricsByNode)),
		maxTraffic:    s.maxTraffic,
		maxMetrics:    s.maxMetrics,
	}
	copy(clone.peers, s.peers)
	for nodeName, metrics := range s.metricsByNode {
		clone.metricsByNode[nodeName] = append([]nodeNetworkMetrics(nil), metrics...)
	}
	return clone
}

func (pl *NetworkAware) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	klog.Infof("%s prescoring pod %q", logPrefix, pod.Name)

	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("error listing pods in namespace %q: %v", pod.Namespace, err))
	}

	deployment, err := sophos.GetOwnerDeployment(ctx, pl.handle, pod)
	if err != nil {
		klog.Infof("%s error getting owner deployment for pod %s: %s", logPrefix, pod.Name, err.Error())
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
	maxTraffic := 0.0
	for i := range pods.Items {
		peerPod := &pods.Items[i]
		if peerPod.Spec.NodeName == "" || !sophos.SameGroup(pod, peerPod) || !sophos.HasLowerOrEqualIndex(pod, peerPod) {
			continue
		}
		if _, ok := nodeByName[peerPod.Spec.NodeName]; !ok {
			continue
		}
		traffic := sophos.GetAppTrafficFromDeployment(deployment, peerPod)
		if traffic <= 0 {
			continue
		}
		if traffic > maxTraffic {
			maxTraffic = traffic
		}
		peers = append(peers, peerPlacement{node: nodeByName[peerPod.Spec.NodeName], traffic: traffic})
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
			metrics := nodeNetworkMetrics{
				latency:    sophos.GetNodeLatency(candidate, peer.node),
				bandwidth:  sophos.GetNodeBandwidth(candidate, peer.node),
				packetLoss: sophos.GetNodePacketLoss(candidate, peer.node),
			}
			if pl.ignoreSameZoneNetworkCost && sameZone(candidate, peer.node) {
				metrics.zeroCost = true
			}
			if metrics.latency > maxMetrics.latency {
				maxMetrics.latency = metrics.latency
			}
			if metrics.bandwidth > maxMetrics.bandwidth {
				maxMetrics.bandwidth = metrics.bandwidth
			}
			if metrics.packetLoss > maxMetrics.packetLoss {
				maxMetrics.packetLoss = metrics.packetLoss
			}
			metricsForNode = append(metricsForNode, metrics)
		}
		metricsByNode[candidate.Name] = metricsForNode
	}

	state.Write(preScoreStateKey, &preScoreState{
		peers:         peers,
		metricsByNode: metricsByNode,
		maxTraffic:    maxTraffic,
		maxMetrics:    maxMetrics,
	})
	return nil
}

func (pl *NetworkAware) Score(_ context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("%s scoring node %q for pod %q", logPrefix, nodeName, pod.Name)

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
	for i, peer := range preScore.peers {
		if i >= len(metrics) {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("network metrics for node %q are incomplete in %s state", nodeName, Name))
		}
		cost += communicationCost(metrics[i], preScore.maxMetrics, peer.traffic, preScore.maxTraffic)
	}

	return -int64(math.Round(cost)), nil
}

func (pl *NetworkAware) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

func (pl *NetworkAware) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
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
		//klog.Infof("%s Original score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, nodeScore.Score)
		klog.Infof("%s Normalized score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, scores[i].Score)
	}

	return nil
}

func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.NetworkAwareArgs)
	if !ok && obj != nil {
		return nil, fmt.Errorf("want args to be of type NetworkAwareArgs, got %T", obj)
	}

	pl := &NetworkAware{
		handle: handle,
	}
	if args != nil {
		pl.ignoreSameZoneNetworkCost = args.IgnoreSameZoneNetworkCost
	}
	return pl, nil
}

func sameZone(a, b *v1.Node) bool {
	if a == nil || b == nil {
		return false
	}
	zone := a.Labels[v1.LabelTopologyZone]
	return zone != "" && zone == b.Labels[v1.LabelTopologyZone]
}

func communicationCost(metrics, maxMetrics nodeNetworkMetrics, traffic, maxTraffic float64) float64 {
	if metrics.zeroCost {
		return 0
	}
	if traffic <= 0 || maxTraffic <= 0 {
		return 0
	}

	trafficRatio := traffic / maxTraffic
	if trafficRatio > 1 {
		trafficRatio = 1
	}

	latencyRatio := 0.0
	if metrics.latency > 0 && maxMetrics.latency > 0 {
		latencyRatio = metrics.latency / maxMetrics.latency
	}
	if latencyRatio > 1 {
		latencyRatio = 1
	}

	bandwidthRatio := 1.0
	if maxMetrics.bandwidth > 0 {
		if metrics.bandwidth <= 0 {
			bandwidthRatio = 0
		} else {
			bandwidthRatio = metrics.bandwidth / maxMetrics.bandwidth
		}
	}
	if bandwidthRatio > 1 {
		bandwidthRatio = 1
	}

	packetLossRatio := 0.0
	if metrics.packetLoss > 0 && maxMetrics.packetLoss > 0 {
		packetLossRatio = metrics.packetLoss / maxMetrics.packetLoss
	}
	if packetLossRatio > 1 {
		packetLossRatio = 1
	}

	return trafficRatio * (latencyRatio + 1 - bandwidthRatio + packetLossRatio)
}
