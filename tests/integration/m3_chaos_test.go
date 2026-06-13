//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestM3ConcurrentWake: 10 simultaneous connections to a suspended endpoint
// must all succeed and produce exactly one compute pod.
func TestM3ConcurrentWake(t *testing.T) {
	cp := newControlPlane(t, 28085)
	portForward(t, "svc/proxy", 25436, 4432)

	p := cp.createProject("m3-concurrent")
	uri := proxyURI(t, p, 25436)

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			conn, err := pgx.Connect(ctx, uri)
			if err != nil {
				errs[i] = err
				return
			}
			defer conn.Close(context.Background())
			var one int
			errs[i] = conn.QueryRow(ctx, "SELECT 1").Scan(&one)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("conn %d: %v", i, err)
		}
	}

	out, err := kubectl(t, "get", "pods", "-l", "app=compute,endpoint="+p.EndpointID, "--no-headers")
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if got := len(strings.Split(strings.TrimSpace(out), "\n")); got != 1 {
		t.Errorf("expected exactly 1 compute pod, got %d:\n%s", got, out)
	}

	cp.debugStop(p.EndpointID)
}

// TestM3SuspendRace: reconnecting immediately after each suspension must
// always succeed — exercises the suspending/wake race window repeatedly.
func TestM3SuspendRace(t *testing.T) {
	cp := newControlPlane(t, 28086)
	portForward(t, "svc/proxy", 25437, 4432)
	ctx := context.Background()

	var p project
	code, raw := cp.doJSON("POST", "/v1/projects", `{"name":"m3-race","suspend_after_seconds":10}`, &p)
	if code != 201 {
		t.Fatalf("create: %d %s", code, raw)
	}
	uri := proxyURI(t, p, 25437)

	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatalf("initial connect: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE race(x int); INSERT INTO race VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)

	for round := 1; round <= 5; round++ {
		// Wait for suspension (pod gone), then reconnect immediately.
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) && computePodExists(t, p.EndpointID) {
			time.Sleep(2 * time.Second)
		}
		if computePodExists(t, p.EndpointID) {
			t.Fatalf("round %d: never suspended", round)
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		conn, err := pgx.Connect(cctx, uri)
		if err != nil {
			cancel()
			t.Fatalf("round %d reconnect: %v", round, err)
		}
		var x int
		if err := conn.QueryRow(cctx, "SELECT x FROM race").Scan(&x); err != nil || x != 1 {
			cancel()
			t.Fatalf("round %d data: %d %v", round, x, err)
		}
		_ = conn.Close(ctx)
		cancel()
		fmt.Printf("suspend/re-wake round %d OK\n", round)
	}

	cp.debugStop(p.EndpointID)
}

// TestM3PageserverRestart: data must survive a pageserver restart (local
// layers + MinIO), and suspended endpoints must wake afterwards.
func TestM3PageserverRestart(t *testing.T) {
	cp := newControlPlane(t, 28087)
	portForward(t, "svc/proxy", 25438, 4432)
	ctx := context.Background()

	p := cp.createProject("m3-psrestart")
	conn, _ := connectViaProxy(t, p, 25438, 3*time.Minute)
	if _, err := conn.Exec(ctx, "CREATE TABLE ps(x int); INSERT INTO ps SELECT generate_series(1,42)"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	cp.debugStop(p.EndpointID)

	if out, err := kubectl(t, "rollout", "restart", "statefulset/pageserver"); err != nil {
		t.Fatalf("restart pageserver: %v %s", err, out)
	}
	if out, err := kubectl(t, "rollout", "status", "statefulset/pageserver", "--timeout=180s"); err != nil {
		t.Fatalf("pageserver not back: %v %s", err, out)
	}

	conn2, wakeTime := connectViaProxy(t, p, 25438, 3*time.Minute)
	t.Logf("wake after pageserver restart: %s", wakeTime.Round(time.Millisecond))
	var n int
	if err := conn2.QueryRow(ctx, "SELECT count(*) FROM ps").Scan(&n); err != nil || n != 42 {
		t.Fatalf("data after pageserver restart: %d %v", n, err)
	}

	cp.debugStop(p.EndpointID)
}
