package perconaservermongodb

import (
	"context"
	"os"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/yaml"

	api "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
	"github.com/percona/percona-server-mongodb-operator/pkg/naming"
	"github.com/percona/percona-server-mongodb-operator/pkg/version"
)

// TestReconcileStatefulSet_LimitsE2EParity proves that e2e-tests/limits' fixture pair -
// conf/no-limits-rs0.yml applied as a CR, compare/statefulset_no-limits-rs0.yml as the
// expected rendered StatefulSet - renders identically through reconcileStatefulSet with
// a fake client, no GKE cluster involved. Today that same pair is only ever checked by
// e2e-tests/limits/test_limits.py::test_cr_config against a live 3-node cluster.
//
// A handful of fields are normalized before comparing, each documented at its strip
// site below: some mirror e2e's own compare_kubectl normalization (namespace, image,
// resourceVersion, ...), the rest are Kubernetes API-server defaulting (generation,
// dnsPolicy, protocol: TCP, ...) that a freshly rendered, never-applied object can't
// carry and that no cluster is needed to verify anyway.
func TestReconcileStatefulSet_LimitsE2EParity(t *testing.T) {
	ctx := context.Background()

	cr := readE2ECR(t, "../../../e2e-tests/limits/conf/no-limits-rs0.yml")
	cr.Namespace = "limits-e2e-parity"
	// The e2e harness fills this in from $IMAGE at apply time; compare_kubectl then
	// strips it back out of the live object before diffing (it's env/build specific,
	// not something the CR shape determines), so a placeholder here is stripped the
	// same way below, right after rendering.
	cr.Spec.Image = "perconalab/percona-server-mongodb-operator:test-mongod"
	cr.Spec.CRVersion = "1.24.0"
	if err := cr.CheckNSetDefaults(ctx, version.PlatformKubernetes); err != nil {
		t.Fatal(err)
	}

	rs := cr.Spec.Replset("rs0")

	r := buildFakeClient(
		cr,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: cr.Spec.Secrets.SSL, Namespace: cr.Namespace},
			Data: map[string][]byte{
				"ca.crt":  []byte("fake-ca-cert"),
				"tls.crt": []byte("fake-tls-cert"),
				"tls.key": []byte("fake-tls-key"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: api.SSLInternalSecretName(cr), Namespace: cr.Namespace},
		},
		// The real controller writes this ConfigMap from rs.Configuration in an
		// earlier reconcile step; reconcileStatefulSet only checks it exists.
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: naming.MongodCustomConfigName(cr, rs), Namespace: cr.Namespace},
			Data:       map[string]string{"mongod.conf": string(rs.Configuration)},
		},
	)
	ls := naming.MongodLabels(cr, rs)

	sts, err := r.reconcileStatefulSet(ctx, cr, rs, ls)
	if err != nil {
		t.Fatalf("reconcileStatefulSet() error = %v", err)
	}

	gvk, err := apiutil.GVKForObject(sts, scheme.Scheme)
	if err != nil {
		t.Fatal(err)
	}
	sts.Kind = gvk.Kind
	sts.APIVersion = gvk.GroupVersion().String()

	for i := range sts.Spec.VolumeClaimTemplates {
		sts.Spec.VolumeClaimTemplates[i].TypeMeta = metav1.TypeMeta{}
		sts.Spec.VolumeClaimTemplates[i].Namespace = ""
	}

	// Same normalizations e2e's compare_kubectl applies to the live object: image is
	// env/build specific, NAMESPACE is this test's namespace, and the config-hash
	// annotation depends on ConfigMap content this test fakes with a placeholder.
	dropEnv := func(env []corev1.EnvVar) []corev1.EnvVar {
		out := env[:0]
		for _, e := range env {
			if e.Name != "NAMESPACE" {
				out = append(out, e)
			}
		}
		return out
	}
	for i := range sts.Spec.Template.Spec.Containers {
		sts.Spec.Template.Spec.Containers[i].Image = ""
		sts.Spec.Template.Spec.Containers[i].Env = dropEnv(sts.Spec.Template.Spec.Containers[i].Env)
	}
	for i := range sts.Spec.Template.Spec.InitContainers {
		sts.Spec.Template.Spec.InitContainers[i].Image = ""
	}
	delete(sts.Spec.Template.Annotations, "percona.com/configuration-hash")

	// Same three fields e2e's compare_kubectl strips from the live object before
	// diffing (namespace, resourceVersion, ownerReferences[].apiVersion) - they come
	// from the API server / this test's namespace, not from the CR shape.
	sts.Namespace = ""
	sts.ResourceVersion = ""
	for i := range sts.OwnerReferences {
		sts.OwnerReferences[i].APIVersion = ""
	}

	want := readE2EExpectedSts(t, "../../../e2e-tests/limits/compare/statefulset_no-limits-rs0.yml")
	want.Generation = 0
	for i := range want.Spec.VolumeClaimTemplates {
		want.Spec.VolumeClaimTemplates[i].Status = corev1.PersistentVolumeClaimStatus{}
	}
	// Everything below this line is Kubernetes' own PodSpec/StatefulSetSpec
	// defaulting (SetDefaults_PodSpec, SetDefaults_Container, ...), applied by the
	// API server on Create. It is fixed behavior for every StatefulSet the cluster
	// ever creates, not something the operator's code or the CR decides, so a live
	// cluster proves nothing about it that the Kubernetes docs don't already.
	want.Spec.PodManagementPolicy = ""
	want.Spec.RevisionHistoryLimit = nil
	stripPodSpecAPIDefaults(&want.Spec.Template.Spec)

	compareSts(t, sts, want)
}

func stripPodSpecAPIDefaults(spec *corev1.PodSpec) {
	spec.DNSPolicy = ""
	spec.SchedulerName = ""
	spec.DeprecatedServiceAccount = ""

	stripContainer := func(c *corev1.Container) {
		c.TerminationMessagePath = ""
		c.TerminationMessagePolicy = ""
		for i := range c.Ports {
			c.Ports[i].Protocol = ""
		}
		// livenessProbe.successThreshold is fixed at 1 by the API server (it's the
		// only value the field accepts there), so the operator never sets it.
		// readinessProbe.successThreshold is a real, operator-chosen value - left as is.
		if p := c.LivenessProbe; p != nil {
			p.SuccessThreshold = 0
		}
	}
	for i := range spec.Containers {
		stripContainer(&spec.Containers[i])
	}
	for i := range spec.InitContainers {
		stripContainer(&spec.InitContainers[i])
	}
	// Only these two volumes' DefaultMode is left unset by the operator (config via
	// VolumeSourceType.VolumeSource(), users-secret-file via the users-secret volume
	// builder) and so picks up the API server's 420 default. Every other volume mode
	// below (ssl/encryption-key secrets at 288, etc.) is a value the operator's code
	// chooses deliberately and must still match exactly.
	apiDefaultedModeVolumes := map[string]bool{"config": true, "users-secret-file": true}
	for i := range spec.Volumes {
		if !apiDefaultedModeVolumes[spec.Volumes[i].Name] {
			continue
		}
		if cm := spec.Volumes[i].ConfigMap; cm != nil {
			cm.DefaultMode = nil
		}
		if s := spec.Volumes[i].Secret; s != nil {
			s.DefaultMode = nil
		}
	}
}

func readE2ECR(t *testing.T, path string) *api.PerconaServerMongoDB {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cr := new(api.PerconaServerMongoDB)
	if err := yaml.Unmarshal(data, cr); err != nil {
		t.Fatal(err)
	}
	return cr
}

func readE2EExpectedSts(t *testing.T, path string) *appsv1.StatefulSet {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sts := new(appsv1.StatefulSet)
	if err := yaml.Unmarshal(data, sts); err != nil {
		t.Fatal(err)
	}
	return sts
}
