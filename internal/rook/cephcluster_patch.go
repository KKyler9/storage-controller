package rook

import (
	"context"

	rookcephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func UpsertNodeInCephClusterStorage(ctx context.Context, c client.Client, cluster *rookcephv1.CephCluster, desired rookcephv1.Node) error {
	nodes := cluster.Spec.Storage.Nodes
	replaced := false
	for i := range nodes {
		if nodes[i].Name == desired.Name {
			nodes[i] = desired
			replaced = true
			break
		}
	}
	if !replaced {
		nodes = append(nodes, desired)
	}
	cluster.Spec.Storage.Nodes = nodes
	return c.Update(ctx, cluster)
}

func RemoveNodeFromCephClusterStorage(ctx context.Context, c client.Client, cluster *rookcephv1.CephCluster, nodeName string) error {
	nodes := cluster.Spec.Storage.Nodes
	filtered := make([]rookcephv1.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Name != nodeName {
			filtered = append(filtered, n)
		}
	}
	cluster.Spec.Storage.Nodes = filtered
	return c.Update(ctx, cluster)
}
