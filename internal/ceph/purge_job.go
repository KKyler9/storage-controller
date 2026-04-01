package ceph

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rookcephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
)

const defaultPurgeImage = "rook/ceph:v1.15.0"

func BuildPurgeJob(cluster *rookcephv1.CephCluster, nodeName string, osdIDs []int) *batchv1.Job {
	sort.Ints(osdIDs)
	ids := make([]string, 0, len(osdIDs))
	for _, id := range osdIDs {
		ids = append(ids, strconv.Itoa(id))
	}
	osdList := strings.Join(ids, ",")

	image := os.Getenv("ROOK_CEPH_PURGE_IMAGE")
	if image == "" {
		image = defaultPurgeImage
	}

	name := sanitizeName(fmt.Sprintf("storage-admin-purge-%s-%s", cluster.Name, nodeName))
	labels := map[string]string{
		"app.kubernetes.io/managed-by":              "storage-admin",
		"storage-admin.paas.usw/node":               nodeName,
		"storage-admin.paas.usw/ceph-cluster":       cluster.Name,
		"storage-admin.paas.usw/purge-job-for-node": nodeName,
	}

	backoff := int32(3)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: "rook-ceph-purge-osd",
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "osd-removal",
						Image: image,
						Args:  []string{"ceph", "osd", "remove", "--preserve-pvc", "false", "--force-osd-removal", "false", "--osd-ids", osdList},
						Env: []corev1.EnvVar{
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
							{Name: "ROOK_MON_ENDPOINTS", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "rook-ceph-mon-endpoints"}, Key: "data"}}},
							{Name: "ROOK_CEPH_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "rook-ceph-mon"}, Key: "ceph-username"}}},
							{Name: "ROOK_CONFIG_DIR", Value: "/var/lib/rook"},
							{Name: "ROOK_CEPH_CONFIG_OVERRIDE", Value: "/etc/rook/config/override.conf"},
							{Name: "ROOK_FSID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "rook-ceph-mon"}, Key: "fsid"}}},
							{Name: "ROOK_LOG_LEVEL", Value: "DEBUG"},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "ceph-conf", MountPath: "/etc/ceph"},
							{Name: "rook-data", MountPath: "/var/lib/rook"},
							{Name: "rook-mon", MountPath: "/var/lib/rook-ceph-mon"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "ceph-conf", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "rook-ceph-mon", Items: []corev1.KeyToPath{{Key: "ceph-secret", Path: "secret.keyring"}}}}},
						{Name: "rook-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "rook-mon", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "_", "-")
	if len(name) <= 63 {
		return name
	}
	return name[:63]
}
