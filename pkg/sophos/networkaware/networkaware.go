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
	"sigs.k8s.io/scheduler-plugins/pkg/sophos"
)

const (
	Name      = "NetworkAware"
	logPrefix = "[sophos][NetworkAware]"

	preScoreStateKey = "PreScore" + Name
	scoreScale       = 1000000
)

type NetworkAware struct {
	handle framework.Handle
}

type preScoreState struct {
	peers         []peerPlacement
	latencyByNode map[string][]float64
	maxLatency    float64
	maxTraffic    float64
}

func (s *preScoreState) Clone() framework.StateData {
	clone := &preScoreState{
		peers:         make([]peerPlacement, len(s.peers)),
		latencyByNode: make(map[string][]float64, len(s.latencyByNode)),
		maxLatency:    s.maxLatency,
		maxTraffic:    s.maxTraffic,
	}
	copy(clone.peers, s.peers)
	for nodeName, latencies := range s.latencyByNode {
		clone.latencyByNode[nodeName] = append([]float64(nil), latencies...)
	}
	return clone
}

type peerPlacement struct {
	node    *v1.Node
	traffic float64
}

var _ = framework.ScorePlugin(&NetworkAware{})
var _ = framework.PreScorePlugin(&NetworkAware{})

func (pl *NetworkAware) Name() string {
	return Name
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

	candidateByName := make(map[string]*v1.Node, len(nodes))
	for _, nodeInfo := range nodes {
		if nodeInfo.Node() != nil {
			candidateByName[nodeInfo.Node().Name] = nodeInfo.Node()
		}
	}

	peers := make([]peerPlacement, 0, len(pods.Items))
	maxTraffic := 0.0
	for i := range pods.Items {
		peerPod := &pods.Items[i]
		if peerPod.Spec.NodeName == "" || !sophos.SameGroup(pod, peerPod) {
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

	maxLatency := 0.0
	latencyByNode := make(map[string][]float64, len(candidateByName))
	for nodeName, candidate := range candidateByName {
		latencies := make([]float64, 0, len(peers))
		for _, peer := range peers {
			latency := sophos.GetNodeLatency(candidate, peer.node)
			if latency > maxLatency {
				maxLatency = latency
			}
			latencies = append(latencies, latency)
		}
		latencyByNode[nodeName] = latencies
	}

	state.Write(preScoreStateKey, &preScoreState{
		peers:         peers,
		latencyByNode: latencyByNode,
		maxLatency:    maxLatency,
		maxTraffic:    maxTraffic,
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

	latencies, ok := preScore.latencyByNode[nodeName]
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("latencies for node %q not found in %s state", nodeName, Name))
	}

	cost := 0.0
	for i, peer := range preScore.peers {
		cost += normalizedProduct(latencies[i], preScore.maxLatency, peer.traffic, preScore.maxTraffic)
	}

	return -int64(math.Round(cost * scoreScale)), nil
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

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	pl := &NetworkAware{
		handle: handle,
	}
	return pl, nil
}

func normalizedProduct(latency, maxLatency, traffic, maxTraffic float64) float64 {
	if latency <= 0 || maxLatency <= 0 || traffic <= 0 || maxTraffic <= 0 {
		return 0
	}
	return (latency / maxLatency) * (traffic / maxTraffic)
}
