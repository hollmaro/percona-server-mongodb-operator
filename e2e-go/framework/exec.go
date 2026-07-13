package framework

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// PodExec runs a command in a container and returns stdout.
func (f *Framework) PodExec(ctx context.Context, pod, container string, command ...string) (string, error) {
	req := f.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(f.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(f.RestConfig, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		return stdout.String(), fmt.Errorf("exec %v in %s/%s: %w, stderr: %s",
			command, pod, container, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// MongoshEval runs a mongosh --eval script inside the mongod container of a
// pod over the in-pod localhost listener. Like the bash suite's client pod,
// it assembles a combined PEM from the pod's own certificate secret and
// connects with TLS (the test CRs use tls.mode requireTLS, which closes
// plain connections). If the cert files are absent (TLS-less cluster), it
// falls back to a plain connection.
func (f *Framework) MongoshEval(ctx context.Context, pod, user, pass, authDB, script string) (string, error) {
	shellCmd := fmt.Sprintf(
		`if [ -f /etc/mongodb-ssl/tls.crt ]; then
			cat /etc/mongodb-ssl/tls.crt /etc/mongodb-ssl/tls.key > /tmp/e2e-tls.pem
			exec mongosh --quiet --tls \
				--tlsCAFile /etc/mongodb-ssl/ca.crt \
				--tlsCertificateKeyFile /tmp/e2e-tls.pem \
				--tlsAllowInvalidHostnames \
				-u %[1]s -p %[2]s --authenticationDatabase %[3]s --eval '%[4]s'
		else
			exec mongosh --quiet -u %[1]s -p %[2]s --authenticationDatabase %[3]s --eval '%[4]s'
		fi`, user, pass, authDB, script)
	out, err := f.PodExec(ctx, pod, "mongod", "sh", "-c", shellCmd)
	return strings.TrimSpace(out), err
}

// evalOnPrimary runs a script against whichever member is primary by trying
// each member's localhost listener until the write is accepted.
func (f *Framework) evalOnPrimary(ctx context.Context, cluster, user, pass, authDB, script string) (string, error) {
	var lastErr error
	for _, pod := range []string{cluster + "-rs0-0", cluster + "-rs0-1", cluster + "-rs0-2"} {
		out, err := f.MongoshEval(ctx, pod, user, pass, authDB, script)
		if err == nil && !strings.Contains(out, "NotWritablePrimary") {
			return out, nil
		}
		if err != nil && !strings.Contains(err.Error(), "NotWritablePrimary") &&
			!strings.Contains(err.Error(), "not primary") {
			// a real error, not just "wrong member": remember it but keep
			// trying the other members before giving up
			lastErr = err
			continue
		}
		lastErr = fmt.Errorf("not primary: %s", pod)
	}
	return "", fmt.Errorf("no primary accepted the operation: %w", lastErr)
}

// EnsureAppUser creates the myApp user, the equivalent of the first
// run_mongo_tls block in the bash tests.
func (f *Framework) EnsureAppUser(ctx context.Context, cluster string) error {
	script := `try {
		db.getSiblingDB("myApp").createUser({user:"myApp",pwd:"myPass",roles:[{db:"myApp",role:"readWrite"}]})
	} catch (e) { if (!String(e).includes("already exists")) { throw e } }
	print("user-ok")`
	_, err := f.evalOnPrimary(ctx, cluster, "userAdmin", "userAdmin123456", "admin", script)
	return err
}

// InsertDoc writes one document through the primary.
func (f *Framework) InsertDoc(ctx context.Context, cluster string, x int) error {
	script := fmt.Sprintf(`db.getSiblingDB("myApp").test.insertOne({ x: %d }); print("insert-ok")`, x)
	_, err := f.evalOnPrimary(ctx, cluster, "myApp", "myPass", "myApp", script)
	if err == nil {
		f.Log("inserted document x=%d", x)
	}
	return err
}

// DropCollection drops myApp.test through the primary.
func (f *Framework) DropCollection(ctx context.Context, cluster string) error {
	script := `db.getSiblingDB("myApp").test.drop(); print("drop-ok")`
	_, err := f.evalOnPrimary(ctx, cluster, "myApp", "myPass", "myApp", script)
	return err
}

// CountDocsOnMember counts myApp.test documents on one specific member via
// its localhost listener, the equivalent of the per-member
// compare_mongo_cmd checks in the bash test.
func (f *Framework) CountDocsOnMember(ctx context.Context, pod string, x int) (int, error) {
	script := fmt.Sprintf(
		`db.getMongo().setReadPref("secondaryPreferred"); print(db.getSiblingDB("myApp").test.countDocuments({ x: %d }))`, x)
	out, err := f.MongoshEval(ctx, pod, "myApp", "myPass", "myApp", script)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return 0, fmt.Errorf("no count in output %q", out)
	}
	return strconv.Atoi(fields[len(fields)-1])
}
