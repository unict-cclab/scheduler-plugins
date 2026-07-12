// Package netmarks implements the traffic-aware placement policy proposed by
// NetMARKS. Candidate nodes are preferred in proportion to the traffic between
// the pod being scheduled and its already scheduled peer pods on that node.
package netmarks

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
	Name      = "NetMarks"
	logPrefix = "[sophos][NetMarks]"

	preScoreStateKey = "PreScore" + Name
)

type NetMarks struct {
	handle framework.Handle
}

type preScoreState struct {
	trafficByNode map[string]float64
}

func (s *preScoreState) Clone() framework.StateData {
	clone := &preScoreState{trafficByNode: make(map[string]float64, len(s.trafficByNode))}
	for nodeName, traffic := range s.trafficByNode {
		clone.trafficByNode[nodeName] = traffic
	}
	return clone
}

var _ = framework.PreScorePlugin(&NetMarks{})
var _ = framework.ScorePlugin(&NetMarks{})

func (pl *NetMarks) Name() string { return Name }

// PreScore obtains the traffic graph once per scheduling cycle and aggregates
// the traffic towards peer pods by their current node.
func (pl *NetMarks) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, _ []*framework.NodeInfo) *framework.Status {
	pods, err := pl.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("error listing pods in namespace %q: %v", pod.Namespace, err))
	}

	deployment, err := sophos.GetOwnerDeployment(ctx, pl.handle, pod)
	if err != nil {
		klog.Infof("%s cannot get traffic annotations for pod %s/%s: %v", logPrefix, pod.Namespace, pod.Name, err)
	}

	trafficByNode := make(map[string]float64)
	for i := range pods.Items {
		peer := &pods.Items[i]
		if peer.Spec.NodeName == "" || !sophos.SameGroup(pod, peer) {
			continue
		}

		traffic := sophos.GetAppTrafficFromDeployment(deployment, peer)
		if traffic > 0 {
			trafficByNode[peer.Spec.NodeName] += traffic
		}
	}

	state.Write(preScoreStateKey, &preScoreState{trafficByNode: trafficByNode})
	return nil
}

// Score returns the sum of traffic between the pod and peers already placed on
// the candidate node. Higher traffic therefore favors co-location.
func (pl *NetMarks) Score(_ context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	rawState, err := state.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error reading %s state: %v", Name, err))
	}

	preScore, ok := rawState.(*preScoreState)
	if !ok {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("invalid %s state type %T", Name, rawState))
	}

	score := int64(math.Round(preScore.trafficByNode[nodeName]))
	klog.Infof("%s raw score of node %q for pod %q: %d", logPrefix, nodeName, pod.Name, score)
	return score, nil
}

func (pl *NetMarks) ScoreExtensions() framework.ScoreExtensions { return pl }

// NormalizeScore only converts the raw traffic totals to the score range that
// kube-scheduler requires. Individual traffic values are never normalized
// before they are summed, so this does not change the NetMARKS traffic model.
func (pl *NetMarks) NormalizeScore(_ context.Context, _ *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var maxScore int64
	for _, nodeScore := range scores {
		if nodeScore.Score > maxScore {
			maxScore = nodeScore.Score
		}
	}

	for i := range scores {
		if maxScore == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = scores[i].Score * framework.MaxNodeScore / maxScore
		}
		klog.Infof("%s normalized score of node %q for pod %q: %d", logPrefix, scores[i].Name, pod.Name, scores[i].Score)
	}
	return nil
}

func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &NetMarks{handle: handle}, nil
}
