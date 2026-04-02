package controllers

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rookcephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	cloudadminv1 "swfts-gitlab-dev.external.lmco.com/swfts-paas/cloud-admin/api/v1"

	"storage-admin/internal/ceph"
	rookpatch "storage-admin/internal/rook"
)

const (
	managedLabelKey      = "storage-admin.paas.usw/managed-by"
	managedLabelValue    = "true"
	purgeAnnotationKey   = "storage-admin.paas.usw/osd-purge"
	recoverAnnotationKey = "storage-admin.paas.usw/osd-recover-reinstalled"
	requeueDelay         = 15 * time.Second
	jobCompletionRequeue = 10 * time.Second
)

type NodeConfigurationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=cloud-admin.paas.usw,resources=nodeconfigurations,verbs=get;list;watch
//+kubebuilder:rbac:groups=ceph.rook.io,resources=cephclusters,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *NodeConfigurationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	nc := &cloudadminv1.NodeConfiguration{}
	if err := r.Get(ctx, req.NamespacedName, nc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	nodeName := nc.Name
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("node not found; waiting for node to return, no destructive action", "node", nodeName)
			return ctrl.Result{RequeueAfter: requeueDelay}, nil
		}
		return ctrl.Result{}, err
	}

	clusters, err := r.listManagedCephClusters(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	purgeRequested := node.Annotations[purgeAnnotationKey] == "true"
	recoverRequested := node.Annotations[recoverAnnotationKey] == "true"

	if purgeRequested || recoverRequested {
		if err := r.handlePurgeOrRecover(ctx, nc, node, clusters, purgeRequested); err != nil {
			if wait.Interrupted(err) {
				return ctrl.Result{RequeueAfter: jobCompletionRequeue}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	for i := range clusters {
		cluster := &clusters[i]
		desired, err := ceph.RenderNodeStorageForCluster(*nc, nodeName, cluster.Name)
		if err != nil {
			return ctrl.Result{}, err
		}

		if len(desired.Devices) == 0 {
			if err := rookpatch.RemoveNodeFromCephClusterStorage(ctx, r.Client, cluster, nodeName); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove node %q from cluster %q: %w", nodeName, cluster.Name, err)
			}
			continue
		}

		if err := rookpatch.UpsertNodeInCephClusterStorage(ctx, r.Client, cluster, desired); err != nil {
			return ctrl.Result{}, fmt.Errorf("upsert node %q in cluster %q: %w", nodeName, cluster.Name, err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *NodeConfigurationReconciler) handlePurgeOrRecover(ctx context.Context, nc *cloudadminv1.NodeConfiguration, node *corev1.Node, clusters []rookcephv1.CephCluster, purge bool) error {
	nodeName := nc.Name

	clusterSet := sets.NewString()
	for _, s := range nc.Spec.Storage {
		if s.CephClusterName != "" {
			clusterSet.Insert(s.CephClusterName)
		}
	}

	for i := range clusters {
		cluster := &clusters[i]
		if !clusterSet.Has(cluster.Name) {
			continue
		}

		expectedPaths := ceph.PathsForCluster(*nc, cluster.Name)
		if len(expectedPaths) == 0 {
			continue
		}

		osdIDs, err := ceph.FindOSDsForNode(ctx, r.Client, nodeName, cluster, expectedPaths)
		if err != nil {
			return err
		}

		if len(osdIDs) > 0 {
			job := ceph.BuildPurgeJob(cluster, nodeName, osdIDs)
			ready, err := r.ensurePurgeJobCompleted(ctx, job)
			if err != nil {
				return err
			}
			if !ready {
				return wait.ErrWaitTimeout
			}
		}

		if purge {
			if err := rookpatch.RemoveNodeFromCephClusterStorage(ctx, r.Client, cluster, nodeName); err != nil {
				return err
			}
			continue
		}

		desired, err := ceph.RenderNodeStorageForCluster(*nc, nodeName, cluster.Name)
		if err != nil {
			return err
		}
		if len(desired.Devices) == 0 {
			continue
		}
		if err := rookpatch.UpsertNodeInCephClusterStorage(ctx, r.Client, cluster, desired); err != nil {
			return err
		}
	}

	return nil
}

func (r *NodeConfigurationReconciler) ensurePurgeJobCompleted(ctx context.Context, desired *batchv1.Job) (bool, error) {
	existing := &batchv1.Job{}
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	if err := r.Get(ctx, key, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, r.Create(ctx, desired)
		}
		return false, err
	}
	for _, c := range existing.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true, nil
		}
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return false, fmt.Errorf("purge job %s/%s failed", existing.Namespace, existing.Name)
		}
	}
	return false, nil
}

func (r *NodeConfigurationReconciler) listManagedCephClusters(ctx context.Context) ([]rookcephv1.CephCluster, error) {
	list := &rookcephv1.CephClusterList{}
	if err := r.List(ctx, list, client.MatchingLabels{managedLabelKey: managedLabelValue}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *NodeConfigurationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cloudadminv1.NodeConfiguration{}).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: node.Name}}}
		})).
		Owns(&batchv1.Job{}).
		Complete(r)
}
