package ceph

import (
	"fmt"

	rookcephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	cloudadminv1 "swfts-gitlab-dev.external.lmco.com/swfts-paas/cloud-admin/api/v1"
)

// RenderNodeStorageForCluster builds a Rook storage node from NodeConfiguration storage entries
// that target the given Ceph cluster.
func RenderNodeStorageForCluster(nc cloudadminv1.NodeConfiguration, nodeName, cephClusterName string) (rookcephv1.Node, error) {
	n := rookcephv1.Node{Name: nodeName}

	for _, dev := range nc.Spec.Storage {
		if dev.CephClusterName != cephClusterName {
			continue
		}
		if dev.Path == "" {
			return rookcephv1.Node{}, fmt.Errorf("node %q has empty storage path for cluster %q", nodeName, cephClusterName)
		}

		rd := rookcephv1.Device{Name: dev.Path}
		if dev.DeviceClass != "" {
			rd.Config = map[string]string{"deviceClass": dev.DeviceClass}
		}
		n.Devices = append(n.Devices, rd)
	}

	return n, nil
}

// PathsForCluster returns configured device paths for the given Ceph cluster.
func PathsForCluster(nc cloudadminv1.NodeConfiguration, cephClusterName string) []string {
	paths := make([]string, 0)
	for _, dev := range nc.Spec.Storage {
		if dev.CephClusterName == cephClusterName && dev.Path != "" {
			paths = append(paths, dev.Path)
		}
	}
	return paths
}
