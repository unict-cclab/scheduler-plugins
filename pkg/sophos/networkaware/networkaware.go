package networkaware

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"

	"sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name = "NetworkAware"

	preScoreStateKey fwk.StateKey = "PreScore" + Name
)

type NetworkAware struct {
	handle                    fwk.Handle
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

var (
	_ fwk.QueueSortPlugin = &NetworkAware{}
	_ fwk.PreScorePlugin  = &NetworkAware{}
	_ fwk.ScorePlugin     = &NetworkAware{}
)

func (pl *NetworkAware) Name() string {
	return Name
}

func (pl *NetworkAware) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	p1 := pInfo1.GetPodInfo().GetPod()
	p2 := pInfo2.GetPodInfo().GetPod()

	index1, ok1 := sophos.GetPodIndex(p1)
	index2, ok2 := sophos.GetPodIndex(p2)
	if ok1 && ok2 && index1 != index2 {
		return index1 < index2
	}

	return (&queuesort.PrioritySort{}).Less(pInfo1, pInfo2)
}

func (s *preScoreState) Clone() fwk.StateData {
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

func (pl *NetworkAware) PreScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx).WithValues("plugin", Name, "pod", klog.KObj(pod))
	logger.V(4).Info("Pre-scoring pod")

	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("list pods in namespace %q: %w", pod.Namespace, err))
	}

	deployment, err := sophos.GetOwnerDeployment(ctx, pl.handle, pod)
	if err != nil {
		logger.V(4).Info("Cannot get owner Deployment; traffic costs will be zero", "err", err)
	}

	clusterNodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("list cluster node information: %w", err))
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
		peerNode, ok := nodeByName[peerPod.Spec.NodeName]
		if !ok {
			continue
		}
		traffic := sophos.GetAppTrafficFromDeployment(deployment, peerPod)
		if traffic <= 0 {
			continue
		}
		if traffic > maxTraffic {
			maxTraffic = traffic
		}
		peers = append(peers, peerPlacement{node: peerNode, traffic: traffic})
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
				zeroCost:   pl.ignoreSameZoneNetworkCost && sameZone(candidate, peer.node),
			}
			maxMetrics.latency = max(maxMetrics.latency, metrics.latency)
			maxMetrics.bandwidth = max(maxMetrics.bandwidth, metrics.bandwidth)
			maxMetrics.packetLoss = max(maxMetrics.packetLoss, metrics.packetLoss)
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

func (pl *NetworkAware) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, fwk.AsStatus(fmt.Errorf("node information is missing"))
	}
	nodeName := nodeInfo.Node().Name

	rawState, err := state.Read(preScoreStateKey)
	if err != nil {
		return 0, fwk.AsStatus(fmt.Errorf("read %s state: %w", Name, err))
	}
	preScore, ok := rawState.(*preScoreState)
	if !ok {
		return 0, fwk.AsStatus(fmt.Errorf("invalid %s state type %T", Name, rawState))
	}

	metrics, ok := preScore.metricsByNode[nodeName]
	if !ok {
		return 0, fwk.AsStatus(fmt.Errorf("network metrics for node %q not found in %s state", nodeName, Name))
	}

	cost := 0.0
	for i, peer := range preScore.peers {
		if i >= len(metrics) {
			return 0, fwk.AsStatus(fmt.Errorf("network metrics for node %q are incomplete in %s state", nodeName, Name))
		}
		cost += communicationCost(metrics[i], preScore.maxMetrics, peer.traffic, preScore.maxTraffic)
	}

	klog.FromContext(ctx).V(4).Info("Calculated network-aware score", "plugin", Name, "pod", klog.KObj(pod), "node", nodeName, "cost", cost)
	return -int64(math.Round(cost)), nil
}

func (pl *NetworkAware) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

func (pl *NetworkAware) NormalizeScore(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	if len(scores) == 0 {
		return nil
	}

	highest := int64(-math.MaxInt64)
	lowest := int64(math.MaxInt64)
	for _, nodeScore := range scores {
		highest = max(highest, nodeScore.Score)
		lowest = min(lowest, nodeScore.Score)
	}

	oldRange := highest - lowest
	newRange := fwk.MaxNodeScore - fwk.MinNodeScore
	logger := klog.FromContext(ctx).WithValues("plugin", Name, "pod", klog.KObj(pod))
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = fwk.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + fwk.MinNodeScore
		}
		logger.V(4).Info("Normalized node score", "node", scores[i].Name, "score", scores[i].Score)
	}

	return nil
}

func New(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	args, ok := obj.(*config.NetworkAwareArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NetworkAwareArgs, got %T", obj)
	}

	klog.FromContext(ctx).V(4).Info("Creating plugin", "plugin", Name)
	return &NetworkAware{
		handle:                    handle,
		ignoreSameZoneNetworkCost: args.IgnoreSameZoneNetworkCost,
	}, nil
}

func sameZone(a, b *v1.Node) bool {
	if a == nil || b == nil {
		return false
	}
	zone := a.Labels[v1.LabelTopologyZone]
	return zone != "" && zone == b.Labels[v1.LabelTopologyZone]
}

func communicationCost(metrics, maxMetrics nodeNetworkMetrics, traffic, maxTraffic float64) float64 {
	if metrics.zeroCost || traffic <= 0 || maxTraffic <= 0 {
		return 0
	}

	trafficRatio := min(traffic/maxTraffic, 1)
	latencyRatio := 0.0
	if metrics.latency > 0 && maxMetrics.latency > 0 {
		latencyRatio = min(metrics.latency/maxMetrics.latency, 1)
	}

	bandwidthRatio := 1.0
	if maxMetrics.bandwidth > 0 {
		if metrics.bandwidth <= 0 {
			bandwidthRatio = 0
		} else {
			bandwidthRatio = min(metrics.bandwidth/maxMetrics.bandwidth, 1)
		}
	}

	packetLossRatio := 0.0
	if metrics.packetLoss > 0 && maxMetrics.packetLoss > 0 {
		packetLossRatio = min(metrics.packetLoss/maxMetrics.packetLoss, 1)
	}

	return trafficRatio * (latencyRatio + 1 - bandwidthRatio + packetLossRatio)
}
