package tests

// Go port of e2e-tests/demand-backup-physical-minio/run: deploy a replica
// set, take a physical backup to in-cluster minio, drop the data, restore,
// verify on every member; then repeat with an arbiter + non-voting topology.
// It reuses the bash test's conf/ YAMLs so both ports assert the same specs.

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/percona/percona-server-mongodb-operator/e2e-go/framework"
)

var _ = Describe("demand-backup-physical-minio", Ordered, Label("backup", "physical"), func() {
	var (
		f       *framework.Framework
		ctx     context.Context
		confDir string
		testDir string
	)
	const cluster = "some-name"

	BeforeAll(func() {
		ctx = context.Background()
		f = newFramework("go-demand-backup-physical-minio")
		confDir = filepath.Join(repoRoot(), "e2e-tests", "conf")
		testDir = filepath.Join(repoRoot(), "e2e-tests", "demand-backup-physical-minio")

		DeferCleanup(func() {
			if !*keepNamespace {
				_ = f.DeleteNamespace(context.Background())
			}
		})
	})

	It("deploys the operator", func() {
		Expect(f.DeployOperator(ctx)).To(Succeed())
	})

	It("deploys minio and creates the bucket", func() {
		Expect(f.DeployMinio(ctx)).To(Succeed())
		Expect(f.ApplyFile(ctx, filepath.Join(confDir, "minio-secret.yml"))).To(Succeed())
		Expect(f.CreateMinioBucket(ctx, "operator-testing")).To(Succeed())
	})

	It("creates the PSMDB cluster", func() {
		Expect(f.ApplyFile(ctx, filepath.Join(testDir, "conf", "secrets.yml"))).To(Succeed())
		cr, err := f.LoadCluster(filepath.Join(testDir, "conf", cluster+".yml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.ApplyCluster(ctx, cr)).To(Succeed())
		Expect(f.WaitClusterReady(ctx, cluster, 15*time.Minute)).To(Succeed())
	})

	It("waits for the PBM resync", func() {
		Expect(f.WaitPBMResync(ctx, cluster)).To(Succeed())
	})

	It("writes test data and sees it on every member", func() {
		Expect(f.EnsureAppUser(ctx, cluster)).To(Succeed())
		Expect(f.InsertDoc(ctx, cluster, 100500)).To(Succeed())
		for _, pod := range members(cluster) {
			Eventually(func() (int, error) {
				return f.CountDocsOnMember(ctx, pod, 100500)
			}).WithTimeout(2*time.Minute).WithPolling(5*time.Second).
				Should(Equal(1), "document should replicate to "+pod)
		}
	})

	runBackupRestoreCycle := func(backupName string) {
		GinkgoHelper()

		By("running the physical backup " + backupName)
		_, err := f.RunBackup(ctx, filepath.Join(testDir, "conf", "backup-minio.yml"), backupName, "minio")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.WaitBackupReady(ctx, backupName, 10*time.Minute)).To(Succeed())

		By("checking the backup exists in minio")
		dest, err := f.BackupDestination(ctx, backupName)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.CheckBackupInStorage(ctx, dest)).To(Succeed())

		By("dropping the collection")
		Expect(f.DropCollection(ctx, cluster)).To(Succeed())

		By("restoring " + backupName)
		Expect(f.RunRestore(ctx, filepath.Join(testDir, "conf", "restore.yml"), backupName, cluster)).To(Succeed())
		Expect(f.WaitRestoreReady(ctx, backupName, 30*time.Minute)).To(Succeed())
		Expect(f.WaitClusterReady(ctx, cluster, 20*time.Minute)).To(Succeed())

		By("verifying the restored data on every member")
		for _, pod := range members(cluster) {
			Eventually(func() (int, error) {
				return f.CountDocsOnMember(ctx, pod, 100500)
			}).WithTimeout(5*time.Minute).WithPolling(5*time.Second).
				Should(Equal(1), "restored document should be on "+pod)
		}
	}

	It("backs up and restores on the plain replica set", func() {
		runBackupRestoreCycle("backup-minio")
	})

	It("switches to the arbiter and non-voting topology", func() {
		cr, err := f.LoadCluster(filepath.Join(testDir, "conf", cluster+"-arbiter-nv.yml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.ApplyCluster(ctx, cr)).To(Succeed())
		f.WaitClusterLeaveReady(ctx, cluster, 2*time.Minute)
		Expect(f.WaitClusterReady(ctx, cluster, 20*time.Minute)).To(Succeed())
	})

	It("backs up and restores on the arbiter and non-voting topology", func() {
		runBackupRestoreCycle("backup-minio-arbiter-nv")
	})
})

func members(cluster string) []string {
	return []string{cluster + "-rs0-0", cluster + "-rs0-1", cluster + "-rs0-2"}
}
