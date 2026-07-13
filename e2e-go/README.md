# e2e-go: Go port of the PSMDB operator e2e suite (pilot)

Goal: replace the bash e2e suite (`e2e-tests/<name>/run` + `functions`)
with Go tests that use the operator's own API types, so CRD changes break
tests at compile time instead of at runtime, and every wait is a poll.

## Layout

```
e2e-go/
  framework/    reusable test framework (the Go equivalent of e2e-tests/functions)
    framework.go  clients, namespace, manifest apply, operator deploy
    cluster.go    typed PSMDB CR load/apply/wait (uses pkg/apis/psmdb/v1)
    exec.go       pod exec + mongosh helpers (data write/read per member)
    backup.go     psmdb-backup / psmdb-restore CRs, PBM resync wait
    minio.go      in-cluster minio (helm) + aws-cli pod for bucket checks
  tests/
    e2e_suite_test.go                     ginkgo suite, image flags
    demand_backup_physical_minio_test.go  pilot port (1:1 with the bash test)
```

## Running

Requires: a kubeconfig pointing at the target cluster (the suite never
creates clusters), helm, and the images reachable by the cluster.

```
cd e2e-go/tests
go test -count=1 -v -timeout 2h .
# flags / env:
#   -operator-image | IMAGE            (default perconalab/...:main)
#   -mongod-image   | IMAGE_MONGOD     (default ...:main-mongod8.0)
#   -backup-image   | IMAGE_BACKUP     (default ...:main-backup)
#   -keep-namespace | SKIP_DELETE=1    keep the test namespace
```

## Design rules carried over from the bash-suite optimization work

- No blind sleeps: every wait polls a condition with an explicit ceiling
  (`wait.PollUntilContextTimeout` / gomega `Eventually`).
- Waits that are best-effort in bash (PBM resync start) stay best-effort:
  they proceed on timeout rather than inventing a new failure mode.
- Conf YAMLs are reused from the bash test directories, so the two ports
  assert the same cluster specs while both exist.
- The aws-cli helper pod waits for pod completion with a generous ceiling
  instead of `kubectl run -i`'s 1-minute attach window (which fails on
  cold image caches - found the hard way during smoke runs).

## What the pilot does NOT port yet (open design questions)

- Golden-file comparison (`compare_kubectl`): the Go way is typed
  assertions or `cmp.Diff` against expected objects with an
  ignore-fields list. Needs a decision before mass migration.
- TLS-client-certificate mongo access from outside the pods: the pilot
  execs mongosh inside the mongod containers over localhost (validated
  against requireTLS clusters), which covers data assertions but not
  client-certificate test scenarios (tls-issue-cert-manager etc).
- Operator log collection on failure (bash `destroy()` tail) - ginkgo
  artifacts hook, straightforward, not yet wired.
```
