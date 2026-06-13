//go:build integration

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// proxyURI builds a connection string that goes through the Neon proxy
// port-forward. The sslip.io hostname resolves to 127.0.0.1, carries the
// endpoint id in SNI, and verifies against our local CA.
func proxyURI(t *testing.T, p project, localPort int) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	caPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "deploy", "certs", "ca.crt")
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=verify-full&sslrootcert=%s",
		p.Role, p.Password, p.Host, localPort, p.Database, caPath)
}

func connectViaProxy(t *testing.T, p project, localPort int, timeout time.Duration) (*pgx.Conn, time.Duration) {
	t.Helper()
	uri := proxyURI(t, p, localPort)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatalf("connect via proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, time.Since(start)
}

// TestM2ColdWakeThroughProxy proves the serverless wake path: a suspended
// endpoint receives a connection through the Neon proxy, which calls our
// wake_compute, which starts the pod — all within one connection attempt.
func TestM2ColdWakeThroughProxy(t *testing.T) {
	cp := newControlPlane(t, 28081)
	portForward(t, "svc/proxy", 25432, 4432)

	p := cp.createProject("m2-wake")
	if computePodExists(t, p.EndpointID) {
		t.Fatal("compute must not exist before first connection")
	}

	// Cold path: connection triggers wake.
	conn, coldTime := connectViaProxy(t, p, 25432, 3*time.Minute)
	t.Logf("COLD connect (incl. compute start): %s", coldTime.Round(time.Millisecond))

	ctx := context.Background()
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("select 1: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE wake_test(x int); INSERT INTO wake_test VALUES (42)"); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.Close(ctx)

	if !computePodExists(t, p.EndpointID) {
		t.Fatal("compute pod should exist after wake")
	}

	// Warm path: compute already running.
	conn2, warmTime := connectViaProxy(t, p, 25432, 30*time.Second)
	t.Logf("WARM connect: %s", warmTime.Round(time.Millisecond))
	var x int
	if err := conn2.QueryRow(ctx, "SELECT x FROM wake_test").Scan(&x); err != nil || x != 42 {
		t.Fatalf("read after reconnect: %d %v", x, err)
	}

	if warmTime > 5*time.Second {
		t.Errorf("warm connect too slow: %s", warmTime)
	}

	cp.debugStop(p.EndpointID)
}
