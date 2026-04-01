package ceph

import (
	"context"
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rookcephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
)

// FindOSDsForNode uses Rook OSD deployments as a first-pass inventory source.
func FindOSDsForNode(ctx context.Context, c client.Client, nodeName string, cluster *rookcephv1.CephCluster, expectedPaths []string) ([]int, error) {
	depList := &appsv1.DeploymentList{}
	selector := labels.SelectorFromSet(labels.Set{"ceph.rook.io/cluster": cluster.Name})
	if err := c.List(ctx, depList, &client.ListOptions{Namespace: cluster.Namespace, LabelSelector: selector}); err != nil {
		return nil, err
	}

	expected := map[string]struct{}{}
	for _, p := range expectedPaths {
		expected[p] = struct{}{}
	}

	seen := map[int]struct{}{}
	for _, dep := range depList.Items {
		osdIDStr, ok := dep.Labels["ceph-osd-id"]
		if !ok {
			continue
		}
		osdID, err := strconv.Atoi(osdIDStr)
		if err != nil {
			continue
		}

		rookNodeName, blockPath, lvPath := readOSDEnv(dep)
		if rookNodeName != nodeName {
			continue
		}

		if len(expected) > 0 {
			if _, ok := expected[blockPath]; !ok {
				if _, ok := expected[lvPath]; !ok {
					continue
				}
			}
		}

		seen[osdID] = struct{}{}
	}

	ids := make([]int, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, nil
}

func readOSDEnv(dep appsv1.Deployment) (nodeName, blockPath, lvPath string) {
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "osd" && c.Name != "rook-ceph-osd" {
			continue
		}
		for _, env := range c.Env {
			switch env.Name {
			case "ROOK_NODE_NAME":
				nodeName = valueOrFieldRef(env)
			case "ROOK_BLOCK_PATH":
				blockPath = valueOrFieldRef(env)
			case "ROOK_LV_PATH":
				lvPath = valueOrFieldRef(env)
			}
		}
		break
	}
	return
}

func valueOrFieldRef(env corev1.EnvVar) string {
	if env.Value != "" {
		return env.Value
	}
	if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
		return env.ValueFrom.FieldRef.FieldPath
	}
	return ""
}
