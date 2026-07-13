package framework

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	minioChartVersion = "5.4.0" // MINIO_VER in e2e-tests/functions
	minioService      = "minio-service"
	awsCLIImage       = "docker.io/amazon/aws-cli:2.34.60"
)

// DeployMinio installs the same minio chart with the same values as
// deploy_minio in the bash suite. Helm is the one external tool the
// framework shells out to.
func (f *Framework) DeployMinio(ctx context.Context) error {
	_ = exec.CommandContext(ctx, "helm", "repo", "add", "minio", "https://charts.min.io/").Run()
	args := []string{
		"upgrade", "--install", minioService, "minio/minio",
		"--namespace", f.Namespace,
		"--version", minioChartVersion,
		"--set", "replicas=1",
		"--set", "mode=standalone",
		"--set", "resources.requests.memory=256Mi",
		"--set", "rootUser=rootuser",
		"--set", "rootPassword=rootpass123",
		"--set", "users[0].accessKey=some-access-key",
		"--set", "users[0].secretKey=some-secret-key",
		"--set", "users[0].policy=consoleAdmin",
		"--set", "service.type=ClusterIP",
		"--set", "configPathmc=/tmp/",
		"--set", "securityContext.enabled=false",
		"--set", "persistence.size=2G",
		"--set", "fullnameOverride=" + minioService,
		"--set", "serviceAccount.create=true",
		"--set", "serviceAccount.name=" + minioService + "-sa",
	}
	out, err := exec.CommandContext(ctx, "helm", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("helm install minio: %w\n%s", err, out)
	}
	f.Log("minio chart installed")

	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			pods, err := f.Clientset.CoreV1().Pods(f.Namespace).List(ctx,
				metav1.ListOptions{LabelSelector: "release=" + minioService})
			if err != nil || len(pods.Items) == 0 {
				return false, nil
			}
			for _, c := range pods.Items[0].Status.ContainerStatuses {
				if c.Ready {
					return true, nil
				}
			}
			return false, nil
		})
	if err != nil {
		return fmt.Errorf("minio pod not ready: %w", err)
	}
	f.Log("minio pod is ready")
	return nil
}

// AWSCliMinio runs one aws-cli command against the in-cluster minio in a
// short-lived pod and returns its logs. Unlike the bash suite's
// `kubectl run -i --rm` (which gives up after 1 minute of image pull and
// killed our first smoke run), it creates the pod and waits for completion
// with a generous ceiling, so cold image caches are tolerated by design.
func (f *Framework) AWSCliMinio(ctx context.Context, name string, cmd ...string) (string, error) {
	endpoint := fmt.Sprintf("http://%s:9000", minioService)
	args := append([]string{"--no-verify-ssl", "--endpoint-url", endpoint}, cmd...)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.Namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "aws-cli",
				Image: awsCLIImage,
				Args:  args,
				Env: []corev1.EnvVar{
					{Name: "AWS_ACCESS_KEY_ID", Value: "some-access-key"},
					{Name: "AWS_SECRET_ACCESS_KEY", Value: "some-secret-key"},
					{Name: "AWS_DEFAULT_REGION", Value: "us-east-1"},
				},
			}},
		},
	}
	if err := f.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	defer func() { _ = f.Client.Delete(context.Background(), pod) }()

	var phase corev1.PodPhase
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			p, err := f.Clientset.CoreV1().Pods(f.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			phase = p.Status.Phase
			return phase == corev1.PodSucceeded || phase == corev1.PodFailed, nil
		})
	if err != nil {
		return "", fmt.Errorf("aws-cli pod %s did not finish: %w", name, err)
	}
	logs, _ := f.PodLogs(ctx, name, "aws-cli")
	if phase == corev1.PodFailed {
		return logs, fmt.Errorf("aws-cli %v failed: %s", cmd, strings.TrimSpace(logs))
	}
	return logs, nil
}

// CreateMinioBucket mirrors create_minio_bucket.
func (f *Framework) CreateMinioBucket(ctx context.Context, bucket string) error {
	_, err := f.AWSCliMinio(ctx, "aws-cli-mb", "s3", "mb", "s3://"+bucket)
	if err != nil && strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		return nil
	}
	return err
}

// CheckBackupInStorage mirrors check_backup_in_storage for minio: list the
// backup destination in the bucket and expect the dump files to be there.
func (f *Framework) CheckBackupInStorage(ctx context.Context, destination string) error {
	path := strings.TrimPrefix(destination, "s3://")
	out, err := f.AWSCliMinio(ctx, "aws-cli-ls", "s3", "ls", "s3://"+path+"/")
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("backup destination %s is empty in storage", destination)
	}
	f.Log("backup found in storage: %s", destination)
	return nil
}
