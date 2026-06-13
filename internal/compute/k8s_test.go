package compute_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/insforge/fly-pgsql/internal/compute"
)

func newRuntime() (*compute.K8sRuntime, *fake.Clientset) {
	client := fake.NewClientset()
	rt := compute.NewK8sRuntime(client, "fly-pgsql", "ghcr.io/neondatabase/compute-node-v17:release-compute-9073")
	return rt, client
}

func TestStartCreatesConfigMapAndPod(t *testing.T) {
	rt, client := newRuntime()
	ctx := context.Background()
	cfg := compute.BuildComputeConfig(testParams())

	if err := rt.Start(ctx, "ep-abc", cfg); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cm, err := client.CoreV1().ConfigMaps("fly-pgsql").Get(ctx, "compute-ep-abc-config", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("configmap: %v", err)
	}
	var roundTrip compute.ComputeConfig
	if err := json.Unmarshal([]byte(cm.Data["config.json"]), &roundTrip); err != nil {
		t.Fatalf("config.json invalid: %v", err)
	}
	if roundTrip.Spec.TenantID != cfg.Spec.TenantID {
		t.Errorf("config roundtrip mismatch")
	}

	pod, err := client.CoreV1().Pods("fly-pgsql").Get(ctx, "compute-ep-abc", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod: %v", err)
	}
	c := pod.Spec.Containers[0]
	if c.Image != "ghcr.io/neondatabase/compute-node-v17:release-compute-9073" {
		t.Errorf("image: %s", c.Image)
	}
	cmd := strings.Join(c.Command, " ")
	for _, frag := range []string{
		"/usr/local/bin/compute_ctl",
		"--pgdata /var/db/postgres/compute",
		"-C postgresql://cloud_admin@localhost:55433/postgres",
		"-b /usr/local/bin/postgres",
		"--compute-id compute-ep-abc",
		"--config /config/config.json",
		"--dev",
	} {
		if !strings.Contains(cmd, frag) {
			t.Errorf("command missing %q: %s", frag, cmd)
		}
	}
	if pod.Labels["app"] != "compute" || pod.Labels["endpoint"] != "ep-abc" {
		t.Errorf("labels: %v", pod.Labels)
	}
	var hasConfigMount, hasPgdataMount bool
	for _, vm := range c.VolumeMounts {
		if vm.MountPath == "/config" {
			hasConfigMount = true
		}
		if vm.MountPath == "/var/db/postgres" {
			hasPgdataMount = true
		}
	}
	if !hasConfigMount || !hasPgdataMount {
		t.Errorf("volume mounts: %+v", c.VolumeMounts)
	}
}

func TestStartIdempotent(t *testing.T) {
	rt, _ := newRuntime()
	ctx := context.Background()
	cfg := compute.BuildComputeConfig(testParams())
	if err := rt.Start(ctx, "ep-abc", cfg); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := rt.Start(ctx, "ep-abc", cfg); err != nil {
		t.Fatalf("second Start should tolerate AlreadyExists: %v", err)
	}
}

func TestStopDeletesBoth(t *testing.T) {
	rt, client := newRuntime()
	ctx := context.Background()
	cfg := compute.BuildComputeConfig(testParams())
	_ = rt.Start(ctx, "ep-abc", cfg)

	if err := rt.Stop(ctx, "ep-abc"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := client.CoreV1().Pods("fly-pgsql").Get(ctx, "compute-ep-abc", metav1.GetOptions{}); err == nil {
		t.Error("pod still exists")
	}
	if _, err := client.CoreV1().ConfigMaps("fly-pgsql").Get(ctx, "compute-ep-abc-config", metav1.GetOptions{}); err == nil {
		t.Error("configmap still exists")
	}
	// Stop on a non-existent endpoint must not error
	if err := rt.Stop(ctx, "ep-never-existed"); err != nil {
		t.Fatalf("Stop missing: %v", err)
	}
}

func TestStatusReportsPodIP(t *testing.T) {
	rt, client := newRuntime()
	ctx := context.Background()

	st, err := rt.Status(ctx, "ep-abc")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Exists {
		t.Error("expected not exists")
	}

	cfg := compute.BuildComputeConfig(testParams())
	_ = rt.Start(ctx, "ep-abc", cfg)
	pod, _ := client.CoreV1().Pods("fly-pgsql").Get(ctx, "compute-ep-abc", metav1.GetOptions{})
	pod.Status = corev1.PodStatus{PodIP: "10.0.0.5", Phase: corev1.PodRunning}
	_, _ = client.CoreV1().Pods("fly-pgsql").UpdateStatus(ctx, pod, metav1.UpdateOptions{})

	st, err = rt.Status(ctx, "ep-abc")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !st.Exists || st.PodIP != "10.0.0.5" || st.Phase != "Running" {
		t.Errorf("status: %+v", st)
	}
}
