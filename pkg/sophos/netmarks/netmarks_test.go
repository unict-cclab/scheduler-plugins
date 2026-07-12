package netmarks

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestScoreAndNormalize(t *testing.T) {
	pl := &NetMarks{}
	state := framework.NewCycleState()
	state.Write(preScoreStateKey, &preScoreState{trafficByNode: map[string]float64{
		"node-a": 750,
		"node-b": 250,
	}})
	pod := &v1.Pod{}

	scores := framework.NodeScoreList{{Name: "node-a"}, {Name: "node-b"}, {Name: "node-c"}}
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
	want := framework.NodeScoreList{{Name: "node-a", Score: 100}, {Name: "node-b", Score: 33}, {Name: "node-c", Score: 0}}
	for i := range want {
		if scores[i] != want[i] {
			t.Errorf("score[%d] = %+v, want %+v", i, scores[i], want[i])
		}
	}
}

func TestNormalizeScoreNoTraffic(t *testing.T) {
	pl := &NetMarks{}
	scores := framework.NodeScoreList{{Name: "node-a"}, {Name: "node-b"}}
	status := pl.NormalizeScore(context.Background(), nil, &v1.Pod{}, scores)
	if !status.IsSuccess() {
		t.Fatalf("NormalizeScore failed: %v", status)
	}
	for _, score := range scores {
		if score.Score != framework.MinNodeScore {
			t.Errorf("score for %q = %d, want %d", score.Name, score.Score, framework.MinNodeScore)
		}
	}
}
