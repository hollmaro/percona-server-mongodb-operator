package framework

import (
	"context"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	sigsyaml "sigs.k8s.io/yaml"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
)

// LoadCluster reads a PerconaServerMongoDB CR from a conf YAML and injects
// the images under test, mirroring cat_config from the bash suite.
func (f *Framework) LoadCluster(path string) (*psmdbv1.PerconaServerMongoDB, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cr := &psmdbv1.PerconaServerMongoDB{}
	if err := sigsyaml.Unmarshal(data, cr); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", path, err)
	}
	cr.Namespace = f.Namespace
	if cr.Spec.Image == "" {
		cr.Spec.Image = f.Images.Mongod
	}
	if cr.Spec.InitImage != "" {
		cr.Spec.InitImage = f.Images.Operator
	}
	if cr.Spec.Backup.Image != "" {
		cr.Spec.Backup.Image = f.Images.Backup
	}
	return cr, nil
}

// ApplyCluster creates the CR, or replaces its spec if it already exists
// (the equivalent of kubectl apply in the bash suite).
func (f *Framework) ApplyCluster(ctx context.Context, cr *psmdbv1.PerconaServerMongoDB) error {
	err := f.Client.Create(ctx, cr)
	if err == nil {
		f.Log("created psmdb/%s", cr.Name)
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	current := &psmdbv1.PerconaServerMongoDB{}
	if err := f.Client.Get(ctx, client.ObjectKeyFromObject(cr), current); err != nil {
		return err
	}
	current.Spec = cr.Spec
	if err := f.Client.Update(ctx, current); err != nil {
		return err
	}
	f.Log("updated psmdb/%s", cr.Name)
	return nil
}

// GetCluster fetches the CR by name.
func (f *Framework) GetCluster(ctx context.Context, name string) (*psmdbv1.PerconaServerMongoDB, error) {
	cr := &psmdbv1.PerconaServerMongoDB{}
	err := f.Client.Get(ctx, client.ObjectKey{Namespace: f.Namespace, Name: name}, cr)
	return cr, err
}

// WaitClusterReady polls the CR state until it is ready, the equivalent of
// wait_for_running + wait_cluster_consistency in the bash suite. The state
// must be observed ready on two consecutive polls to avoid racing a
// reconcile that has not started yet.
func (f *Framework) WaitClusterReady(ctx context.Context, name string, timeout time.Duration) error {
	consecutive := 0
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			cr, err := f.GetCluster(ctx, name)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if cr.Status.State == psmdbv1.AppStateReady {
				consecutive++
			} else {
				consecutive = 0
			}
			return consecutive >= 2, nil
		})
	if err != nil {
		return fmt.Errorf("psmdb/%s did not reach ready in %s: %w", name, timeout, err)
	}
	f.Log("psmdb/%s is ready", name)
	return nil
}

// WaitClusterLeaveReady polls until the CR leaves the ready state after a
// spec change, so a following WaitClusterReady cannot pass on stale status.
// Mirrors wait_for_cluster_leave_ready from the bash suite: on timeout it
// proceeds, it never fails the test.
func (f *Framework) WaitClusterLeaveReady(ctx context.Context, name string, ceiling time.Duration) {
	_ = wait.PollUntilContextTimeout(ctx, 5*time.Second, ceiling, true,
		func(ctx context.Context) (bool, error) {
			cr, err := f.GetCluster(ctx, name)
			if err != nil {
				return false, nil
			}
			return cr.Status.State != psmdbv1.AppStateReady, nil
		})
}
