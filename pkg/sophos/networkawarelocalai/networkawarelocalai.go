package networkawarelocalai

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
	"k8s.io/apimachinery/pkg/labels"
)

const (
	Name      = "NetworkAwareLocalAi"
	logPrefix = "[sophos][NetworkAwareLocalAi]"
	gatewayTrafficEnv        = "GATEWAY_TRAFFIC_KEY"
	defaultGatewayTrafficKey = "gateway-traffic"
)

type NetworkAwareLocalAi struct {
	handle framework.Handle
}

var _ = framework.ScorePlugin(&NetworkAwareLocalAi{})

func (pl *NetworkAwareLocalAi) Name() string {
	return Name
}
func getGatewayTrafficKey(pod *v1.Pod) string {
	for _, c := range pod.Spec.Containers {
		for _, env := range c.Env {
			if env.Name == gatewayTrafficEnv {
				return env.Value
			}
		}
	}
	return defaultGatewayTrafficKey
}
func (pl *NetworkAwareLocalAi) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.Infof("%s scoring node %q for pod %q", logPrefix, nodeName, pod.Name)

	node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("error getting info for node %q: %v", nodeName, err))
	}

	var score int64
	role := pod.GetLabels()["role"]

	switch role {
	case "master":
		score = pl.scoreMaster(ctx, pod, node)
	case "worker":
		score = pl.scoreWorker(ctx, pod, node)
	}

	return score, nil
}

func (pl *NetworkAwareLocalAi) scoreMaster(ctx context.Context, pod *v1.Pod, candidateNode *framework.NodeInfo) int64 {
	var score int64



	allNodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0
	}

	var totalLatency float64
	var count float64
	for _, otherNode := range allNodes {
		if otherNode.Node().Name == candidateNode.Node().Name {
			continue
		}
		totalLatency += sophos.GetNodeLatency(candidateNode.Node(), otherNode.Node())
		count++
	}

	if count > 0 {
		avgLatency := totalLatency / count
		gatewayTraffic := sophos.GetGatewayTraffic(ctx, pl.handle, pod, getGatewayTrafficKey(pod))
		if gatewayTraffic > 0 {
			score -= int64(avgLatency * gatewayTraffic)
		} else {
			// First deploy without warmup(only to evict problem): use constant value
			score -= int64(avgLatency * 1000)
		}
	}

	return score
}

func (pl *NetworkAwareLocalAi) scoreWorker(ctx context.Context, pod *v1.Pod, candidateNode *framework.NodeInfo) int64 {
	var score int64

	masterNodeName := pl.getGroupPodNodes(ctx, pod, "master")
	if len(masterNodeName) == 0 {
		return 0
	}

	masterNodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(masterNodeName[0])
	if err != nil {
		return 0
	}

	latencyToMaster := sophos.GetNodeLatency(candidateNode.Node(), masterNodeInfo.Node())

	// traffic between this worker's deployment and master
	masterPods, err := pl.handle.ClientSet().CoreV1().Pods(pod.GetNamespace()).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + masterNodeName[0],
	})
	if err == nil {
		for _, mp := range masterPods.Items {
			traffic := sophos.GetGroupTraffic(ctx, pl.handle, pod, &mp)
			if traffic > 0 {
				score -= int64(latencyToMaster * traffic)
			}
		}
	}

	// gateway traffic × latency to master
	// More incoming requests =  closer to master
	gatewayTraffic := sophos.GetGatewayTraffic(ctx, pl.handle, pod, getGatewayTrafficKey(pod))
	score -= int64(latencyToMaster * gatewayTraffic)

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
	return &NetworkAwareLocalAi{handle: handle}, nil
}