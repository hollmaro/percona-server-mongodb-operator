package framework

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	sigsyaml "sigs.k8s.io/yaml"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
)

// RunBackup creates a psmdb-backup CR from a conf YAML template.
func (f *Framework) RunBackup(ctx context.Context, tmplPath, name, storage string) (*psmdbv1.PerconaServerMongoDBBackup, error) {
	data, err := os.ReadFile(tmplPath)
	if err != nil {
		return nil, err
	}
	bkp := &psmdbv1.PerconaServerMongoDBBackup{}
	if err := sigsyaml.Unmarshal(data, bkp); err != nil {
		return nil, err
	}
	bkp.Name = name
	bkp.Namespace = f.Namespace
	bkp.Spec.StorageName = storage
	if err := f.Client.Create(ctx, bkp); err != nil {
		return nil, err
	}
	f.Log("running backup %s (storage %s)", name, storage)
	return bkp, nil
}

// WaitBackupReady mirrors wait_backup: poll until ready, fail on error state.
func (f *Framework) WaitBackupReady(ctx context.Context, name string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			bkp := &psmdbv1.PerconaServerMongoDBBackup{}
			err := f.Client.Get(ctx, client.ObjectKey{Namespace: f.Namespace, Name: name}, bkp)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			switch bkp.Status.State {
			case psmdbv1.BackupStateReady:
				return true, nil
			case psmdbv1.BackupStateError:
				return false, fmt.Errorf("backup %s is in error state: %s", name, bkp.Status.Error)
			}
			return false, nil
		})
	if err != nil {
		return fmt.Errorf("backup %s: %w", name, err)
	}
	f.Log("backup %s is ready", name)
	return nil
}

// BackupDestination returns status.destination once set.
func (f *Framework) BackupDestination(ctx context.Context, name string) (string, error) {
	bkp := &psmdbv1.PerconaServerMongoDBBackup{}
	if err := f.Client.Get(ctx, client.ObjectKey{Namespace: f.Namespace, Name: name}, bkp); err != nil {
		return "", err
	}
	return string(bkp.Status.Destination), nil
}

// RunRestore creates a psmdb-restore CR from a conf YAML template.
func (f *Framework) RunRestore(ctx context.Context, tmplPath, backupName, clusterName string) error {
	data, err := os.ReadFile(tmplPath)
	if err != nil {
		return err
	}
	rst := &psmdbv1.PerconaServerMongoDBRestore{}
	if err := sigsyaml.Unmarshal(data, rst); err != nil {
		return err
	}
	rst.Name = "restore-" + backupName
	rst.Namespace = f.Namespace
	rst.Spec.BackupName = backupName
	if rst.Spec.ClusterName == "" {
		rst.Spec.ClusterName = clusterName
	}
	if err := f.Client.Create(ctx, rst); err != nil {
		return err
	}
	f.Log("running restore restore-%s", backupName)
	return nil
}

// WaitRestoreReady mirrors wait_restore: poll until ready, fail on error
// state with the tail of the operator log attached for diagnosis.
func (f *Framework) WaitRestoreReady(ctx context.Context, backupName string, timeout time.Duration) error {
	name := "restore-" + backupName
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			rst := &psmdbv1.PerconaServerMongoDBRestore{}
			err := f.Client.Get(ctx, client.ObjectKey{Namespace: f.Namespace, Name: name}, rst)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			switch rst.Status.State {
			case psmdbv1.RestoreStateReady:
				return true, nil
			case psmdbv1.RestoreStateError:
				return false, fmt.Errorf("restore %s is in error state: %s", name, rst.Status.Error)
			}
			return false, nil
		})
	if err != nil {
		return fmt.Errorf("restore %s: %w", name, err)
	}
	f.Log("restore %s is ready", name)
	return nil
}

// WaitPBMResync is the Go port of wait_for_pbm_resync: wait for a resync to
// start (a running PBM operation, or the agent's "got epoch" log line for
// resyncs too fast to observe) up to a ceiling, proceeding on timeout, then
// wait for all PBM operations to finish.
func (f *Framework) WaitPBMResync(ctx context.Context, cluster string) error {
	pod := cluster + "-rs0-0"
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		logs, err := f.PodLogs(ctx, pod, "backup-agent")
		if err == nil && strings.Contains(logs, "got epoch") {
			f.Log("PBM got epoch, config is in sync")
			return f.WaitNoPBMOperations(ctx, cluster)
		}
		out, err := f.PodExec(ctx, pod, "backup-agent",
			"pbm", "status", "-o", "json", "-s", "running")
		if err == nil && strings.Contains(out, `"opID"`) {
			f.Log("PBM resync operation observed")
			return f.WaitNoPBMOperations(ctx, cluster)
		}
		time.Sleep(5 * time.Second)
	}
	f.Log("resync was not observed within 60s, continuing")
	return f.WaitNoPBMOperations(ctx, cluster)
}

// WaitNoPBMOperations mirrors wait_for_pbm_operations.
func (f *Framework) WaitNoPBMOperations(ctx context.Context, cluster string) error {
	pod := cluster + "-rs0-0"
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 45*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			out, err := f.PodExec(ctx, pod, "backup-agent",
				"pbm", "status", "-o", "json", "-s", "running")
			if err != nil {
				return false, nil
			}
			return !strings.Contains(out, `"opID"`), nil
		})
}
