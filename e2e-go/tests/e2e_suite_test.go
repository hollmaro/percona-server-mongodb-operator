package tests

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/percona/percona-server-mongodb-operator/e2e-go/framework"
)

var (
	operatorImage = flag.String("operator-image",
		envOr("IMAGE", "perconalab/percona-server-mongodb-operator:main"),
		"operator image under test")
	mongodImage = flag.String("mongod-image",
		envOr("IMAGE_MONGOD", "perconalab/percona-server-mongodb-operator:main-mongod8.0"),
		"mongod image under test")
	backupImage = flag.String("backup-image",
		envOr("IMAGE_BACKUP", "perconalab/percona-server-mongodb-operator:main-backup"),
		"pbm image under test")
	keepNamespace = flag.Bool("keep-namespace",
		os.Getenv("SKIP_DELETE") != "", "keep the test namespace after the run")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// repoRoot locates the repository root relative to this package.
func repoRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func newFramework(prefix string) *framework.Framework {
	f, err := framework.New(repoRoot(), framework.Images{
		Operator: *operatorImage,
		Mongod:   *mongodImage,
		Backup:   *backupImage,
	})
	Expect(err).NotTo(HaveOccurred())
	ns := fmt.Sprintf("%s-%05d", prefix, rand.Intn(100000))
	Expect(f.CreateNamespace(context.Background(), ns)).To(Succeed())
	return f
}

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteCfg, reporterCfg := GinkgoConfiguration()
	suiteCfg.Timeout = 2 * time.Hour
	RunSpecs(t, "PSMDB operator e2e (Go port)", suiteCfg, reporterCfg)
}
