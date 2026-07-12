package nas

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestCommunicationCost(t *testing.T) {
	maxMetrics := nodeNetworkMetrics{latency: 100, bandwidth: 1000, packetLoss: 10}
	tests := []struct {
		name    string
		metrics nodeNetworkMetrics
		want    float64
	}{
		{name: "same node", metrics: nodeNetworkMetrics{zeroCost: true}, want: 0},
		{name: "remote node with missing metrics", metrics: nodeNetworkMetrics{}, want: 1},
		{name: "half of every metric", metrics: nodeNetworkMetrics{latency: 50, bandwidth: 500, packetLoss: 5}, want: 1.5},
		{name: "best network", metrics: nodeNetworkMetrics{bandwidth: 1000}, want: 0},
		{name: "worst network", metrics: nodeNetworkMetrics{latency: 100, packetLoss: 10}, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := communicationCost(tt.metrics, maxMetrics); got != tt.want {
				t.Errorf("communicationCost() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScoreAndNormalize(t *testing.T) {
	pl := &Nas{}
	state := framework.NewCycleState()
	state.Write(preScoreStateKey, &preScoreState{
		peers: []peerPlacement{{node: &v1.Node{}}},
		metricsByNode: map[string][]nodeNetworkMetrics{
			"peer-node": {{zeroCost: true}},
			"near-node": {{latency: 100, bandwidth: 1000}},
			"far-node":  {{latency: 100, packetLoss: 10}},
		},
		maxMetrics: nodeNetworkMetrics{latency: 100, bandwidth: 1000, packetLoss: 10},
	})
	pod := &v1.Pod{}
	scores := framework.NodeScoreList{{Name: "peer-node"}, {Name: "near-node"}, {Name: "far-node"}}
	for i := range scores {
		score, status := pl.Score(context.Background(), state, pod, scores[i].Name)
		if !status.IsSuccess() {
			t.Fatalf("Score(%q) failed: %v", scores[i].Name, status)
		}
		scores[i].Score = score
	}
	if status := pl.NormalizeScore(context.Background(), state, pod, scores); !status.IsSuccess() {
		t.Fatalf("NormalizeScore failed: %v", status)
	}
	want := framework.NodeScoreList{{Name: "peer-node", Score: 100}, {Name: "near-node", Score: 66}, {Name: "far-node", Score: 0}}
	for i := range want {
		if scores[i] != want[i] {
			t.Errorf("score[%d] = %+v, want %+v", i, scores[i], want[i])
		}
	}
}
