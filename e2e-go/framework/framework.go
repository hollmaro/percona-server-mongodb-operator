// Package framework is a Go e2e test framework for the PSMDB operator.
// It mirrors the semantics of e2e-tests/functions (bash) while using the
// operator's own API types, so CRD changes surface at compile time.
package framework

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	sigsyaml "sigs.k8s.io/yaml"

	psmdbv1 "github.com/percona/percona-server-mongodb-operator/pkg/apis/psmdb/v1"
)

// Images holds the container images under test.
type Images struct {
	Operator string
	Mongod   string
	Backup   string
}

// Framework holds clients and the test namespace.
type Framework struct {
	Client     client.Client
	Clientset  *kubernetes.Clientset
	RestConfig *rest.Config
	Scheme     *runtime.Scheme
	Namespace  string
	RepoRoot   string
	Images     Images
	Log        func(format string, args ...any)
}

// New builds clients from the current kubeconfig context.
func New(repoRoot string, images Images) (*Framework, error) {
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := psmdbv1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, err
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Framework{
		Client:     cl,
		Clientset:  cs,
		RestConfig: cfg,
		Scheme:     scheme,
		RepoRoot:   repoRoot,
		Images:     images,
		Log: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "[%s] "+format+"\n",
				append([]any{time.Now().Format("15:04:05")}, args...)...)
		},
	}, nil
}

// CreateNamespace creates the test namespace and records it on the framework.
func (f *Framework) CreateNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := f.Client.Create(ctx, ns); err != nil {
		return err
	}
	f.Namespace = name
	f.Log("created namespace %s", name)
	return nil
}

// DeleteNamespace removes the test namespace (used unless -keep-namespace).
// The psmdb CR finalizers are stripped first: the operator lives in the
// same namespace and dies with it, so nobody would ever process them and
// the namespace would hang in Terminating forever.
func (f *Framework) DeleteNamespace(ctx context.Context) error {
	if f.Namespace == "" {
		return nil
	}
	crs := &psmdbv1.PerconaServerMongoDBList{}
	if err := f.Client.List(ctx, crs, client.InNamespace(f.Namespace)); err == nil {
		for i := range crs.Items {
			cr := &crs.Items[i]
			if len(cr.GetFinalizers()) == 0 {
				continue
			}
			cr.SetFinalizers(nil)
			_ = f.Client.Update(ctx, cr)
		}
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.Namespace}}
	return client.IgnoreNotFound(f.Client.Delete(ctx, ns))
}

// clusterScopedKinds never get the test namespace injected.
var clusterScopedKinds = map[string]bool{
	"CustomResourceDefinition": true,
	"ClusterRole":              true,
	"ClusterRoleBinding":       true,
	"Namespace":                true,
}

// ApplyFile creates every document of a (multi-doc) YAML manifest.
// Namespaced objects with no namespace are placed into the test namespace.
// Existing cluster-scoped objects (e.g. CRDs from a previous run) are skipped.
func (f *Framework) ApplyFile(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		if !clusterScopedKinds[obj.GetKind()] && obj.GetNamespace() == "" {
			obj.SetNamespace(f.Namespace)
		}
		err := f.Client.Create(ctx, obj)
		switch {
		case err == nil:
		case apierrors.IsAlreadyExists(err):
			f.Log("%s/%s already exists, skipping", obj.GetKind(), obj.GetName())
		default:
			return fmt.Errorf("create %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
}

// DeployOperator installs CRDs, RBAC and the operator deployment into the
// test namespace, mirroring create_infra from the bash suite.
func (f *Framework) DeployOperator(ctx context.Context) error {
	deployDir := filepath.Join(f.RepoRoot, "deploy")
	if err := f.ApplyFile(ctx, filepath.Join(deployDir, "crd.yaml")); err != nil {
		return fmt.Errorf("apply crds: %w", err)
	}
	if err := f.ApplyFile(ctx, filepath.Join(deployDir, "rbac.yaml")); err != nil {
		return fmt.Errorf("apply rbac: %w", err)
	}

	data, err := os.ReadFile(filepath.Join(deployDir, "operator.yaml"))
	if err != nil {
		return err
	}
	dep := &appsv1.Deployment{}
	if err := sigsyaml.Unmarshal(data, dep); err != nil {
		return fmt.Errorf("unmarshal operator.yaml: %w", err)
	}
	dep.Namespace = f.Namespace
	dep.Spec.Template.Spec.Containers[0].Image = f.Images.Operator
	if err := f.Client.Create(ctx, dep); err != nil {
		return fmt.Errorf("create operator deployment: %w", err)
	}
	f.Log("operator %s deploying with image %s", dep.Name, f.Images.Operator)
	return f.WaitDeploymentReady(ctx, dep.Name, 5*time.Minute)
}

// WaitDeploymentReady polls until the deployment has a ready replica.
func (f *Framework) WaitDeploymentReady(ctx context.Context, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			dep := &appsv1.Deployment{}
			err := f.Client.Get(ctx, client.ObjectKey{Namespace: f.Namespace, Name: name}, dep)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return dep.Status.ReadyReplicas >= 1, nil
		})
}

// PodLogs returns the logs of one container of a pod.
func (f *Framework) PodLogs(ctx context.Context, pod, container string) (string, error) {
	req := f.Clientset.CoreV1().Pods(f.Namespace).GetLogs(pod,
		&corev1.PodLogOptions{Container: container})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	var buf strings.Builder
	if _, err := io.Copy(&buf, stream); err != nil {
		return "", err
	}
	return buf.String(), nil
}
