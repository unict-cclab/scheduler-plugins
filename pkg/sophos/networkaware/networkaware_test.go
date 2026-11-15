package networkaware

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	fwk "k8s.io/kube-scheduler/framework"
	frameworkimpl "k8s.io/kubernetes/pkg/scheduler/framework"
)

type testHandle struct {
	fwk.Handle
	client kubernetes.Interface
	lister fwk.SharedLister
}

func (h testHandle) ClientSet() kubernetes.Interface        { return h.client }
func (h testHandle) SnapshotSharedLister() fwk.SharedLister { return h.lister }

type testSharedLister struct {
	fwk.SharedLister
	nodes []fwk.NodeInfo
}

func (l testSharedLister) NodeInfos() fwk.NodeInfoLister { return testNodeInfoLister{nodes: l.nodes} }

type testNodeInfoLister struct {
	fwk.NodeInfoLister
	nodes []fwk.NodeInfo
}

func (l testNodeInfoLister) List() ([]fwk.NodeInfo, error) { return l.nodes, nil }

func TestPreScoreNormalizationExcludesZeroCostLinks(t *testing.T) {
	controller := true
	local := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "local", Labels: map[string]string{v1.LabelTopologyZone: "a"}, Annotations: map[string]string{"network-latency.peer": "1000", "network-bandwidth.peer": "1000", "packet-loss.peer": "10"}}}
	remote := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "remote", Labels: map[string]string{v1.LabelTopologyZone: "b"}, Annotations: map[string]string{"network-latency.peer": "100", "network-bandwidth.peer": "50", "packet-loss.peer": "1"}}}
	other := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other", Labels: map[string]string{v1.LabelTopologyZone: "c"}, Annotations: map[string]string{"network-latency.peer": "200", "network-bandwidth.peer": "25", "packet-loss.peer": "2"}}}
	peerNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "peer", Labels: map[string]string{v1.LabelTopologyZone: "a"}}}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "default", Labels: map[string]string{"group": "shop", "index": "1"}, OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "rs", Controller: &controller}}}}
	objects := []runtime.Object{
		&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "default", Labels: map[string]string{"app": "peer", "group": "shop", "index": "0"}}, Spec: v1.PodSpec{NodeName: "peer"}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "default", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "frontend", Controller: &controller}}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "default", Annotations: map[string]string{"traffic.peer": "10"}}},
	}
	info := func(n *v1.Node) fwk.NodeInfo { i := frameworkimpl.NewNodeInfo(); i.SetNode(n); return i }
	for _, tt := range []struct {
		name   string
		ignore bool
		nodes  []*v1.Node
		want   nodeNetworkMetrics
	}{
		{"exclude same zone", true, []*v1.Node{local, remote, other}, nodeNetworkMetrics{latency: 200, bandwidth: 50, packetLoss: 2}},
		{"include same zone when disabled", false, []*v1.Node{local, remote, other}, nodeNetworkMetrics{latency: 1000, bandwidth: 1000, packetLoss: 10}},
		{"all links free", true, []*v1.Node{local}, nodeNetworkMetrics{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nodes := []fwk.NodeInfo{}
			for _, n := range tt.nodes {
				nodes = append(nodes, info(n))
			}
			all := append(append([]fwk.NodeInfo{}, nodes...), info(peerNode))
			pl := &NetworkAware{handle: testHandle{client: fake.NewClientset(objects...), lister: testSharedLister{nodes: all}}, ignoreSameZoneNetworkCost: tt.ignore}
			state := frameworkimpl.NewCycleState()
			if status := pl.PreScore(context.Background(), state, pod, nodes); !status.IsSuccess() {
				t.Fatal(status)
			}
			raw, err := state.Read(preScoreStateKey)
			if err != nil {
				t.Fatal(err)
			}
			got := raw.(*preScoreState)
			if got.maxMetrics != tt.want {
				t.Fatalf("max metrics = %+v, want %+v", got.maxMetrics, tt.want)
			}
			if len(got.peers) != 1 || got.maxTraffic != 10 {
				t.Fatalf("unexpected peers or traffic: %+v", got)
			}
			localMetrics := got.metricsByNode["local"][0]
			if localMetrics.latency != 1000 || localMetrics.bandwidth != 1000 || localMetrics.packetLoss != 10 || localMetrics.zeroCost != tt.ignore {
				t.Fatalf("local metrics not preserved: %+v", localMetrics)
			}
			if tt.ignore && communicationCost(localMetrics, got.maxMetrics, 10, 10) != 0 {
				t.Fatal("same-zone communication must remain free")
			}
			if tt.ignore && len(tt.nodes) > 1 {
				if cost := communicationCost(got.metricsByNode["remote"][0], got.maxMetrics, 10, 10); cost != 1 {
					t.Fatalf("remote cost = %v, want 1", cost)
				}
				if cost := communicationCost(got.metricsByNode["other"][0], got.maxMetrics, 10, 10); cost != 2.5 {
					t.Fatalf("other cost = %v, want 2.5", cost)
				}
			}
		})
	}
}
