//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestM2FullServerlessLoop: wake by connection → write → disconnect → idle
// suspension destroys the pod → reconnect wakes a fresh compute → data is
// intact. suspend_after_seconds is set to 15 so one sweep (30s interval)
// catches it.
func TestM2FullServerlessLoop(t *testing.T) {
	cp := newControlPlane(t, 28082)
	portForward(t, "svc/proxy", 25433, 4432)

	// Create with a short suspend threshold.
	var p project
	code, raw := cp.doJSON(http.MethodPost, "/v1/projects",
		`{"name":"m2-loop","suspend_after_seconds":15}`, &p)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, raw)
	}

	ctx := context.Background()
	conn, _ := connectViaProxy(t, p, 25433, 3*time.Minute)
	if _, err := conn.Exec(ctx, "CREATE TABLE loop_test(x int); INSERT INTO loop_test VALUES (7)"); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.Close(ctx)

	// Wait for the suspend sweep to reclaim the idle compute. Threshold 15s +
	// sweep interval 30s + termination time → allow up to 2.5 minutes.
	deadline := time.Now().Add(150 * time.Second)
	suspendedAt := time.Time{}
	for time.Now().Before(deadline) {
		if !computePodExists(t, p.EndpointID) {
			suspendedAt = time.Now()
			break
		}
		time.Sleep(3 * time.Second)
	}
	if suspendedAt.IsZero() {
		out, _ := kubectl(t, "logs", "deploy/controlplane", "--tail", "20")
		t.Fatalf("compute pod was never suspended; controlplane logs:\n%s", out)
	}
	t.Logf("idle compute reclaimed (scale-to-zero)")

	// Reconnect: wakes a fresh compute, data must survive.
	conn2, wakeTime := connectViaProxy(t, p, 25433, 3*time.Minute)
	t.Logf("re-wake after suspend: %s", wakeTime.Round(time.Millisecond))
	var x int
	if err := conn2.QueryRow(ctx, "SELECT x FROM loop_test").Scan(&x); err != nil || x != 7 {
		t.Fatalf("data lost across suspend/wake: %d %v", x, err)
	}

	cp.debugStop(p.EndpointID)
}

// TestM2ActiveConnectionNotSuspended: a session running queries keeps the
// compute alive past the suspend threshold.
func TestM2ActiveConnectionNotSuspended(t *testing.T) {
	cp := newControlPlane(t, 28083)
	portForward(t, "svc/proxy", 25434, 4432)

	var p project
	code, raw := cp.doJSON(http.MethodPost, "/v1/projects",
		`{"name":"m2-active","suspend_after_seconds":15}`, &p)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, raw)
	}

	ctx := context.Background()
	conn, _ := connectViaProxy(t, p, 25434, 3*time.Minute)

	// Keep issuing queries for 75s (well past threshold + sweep interval).
	stop := time.Now().Add(75 * time.Second)
	for time.Now().Before(stop) {
		var one int
		if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("query failed at %s remaining — compute suspended under active load? %v",
				time.Until(stop).Round(time.Second), err)
		}
		time.Sleep(2 * time.Second)
	}
	if !computePodExists(t, p.EndpointID) {
		t.Fatal("compute pod gone despite active queries")
	}
	fmt.Println("compute survived 75s of activity with 15s suspend threshold")

	_ = conn.Close(ctx)
	cp.debugStop(p.EndpointID)
}
