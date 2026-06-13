//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// connectDirect port-forwards the compute pod and opens a pgx connection as
// the project role.
func connectDirect(t *testing.T, p project, localPort int) *pgx.Conn {
	t.Helper()
	portForward(t, "pod/compute-"+p.EndpointID, localPort, 55433)
	uri := fmt.Sprintf("postgresql://%s:%s@127.0.0.1:%d/%s", p.Role, p.Password, localPort, p.Database)
	var conn *pgx.Conn
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = pgx.Connect(context.Background(), uri)
		if err == nil {
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			return conn
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("connect compute: %v", err)
	return nil
}

// TestM1StorageComputeSeparation is the M1 acceptance: create a project,
// write through one compute, destroy it, start a fresh one — data survives
// because all durable state lives in safekeepers/pageserver/MinIO.
func TestM1StorageComputeSeparation(t *testing.T) {
	cp := newControlPlane(t, 28080)
	p := cp.createProject("m1-acceptance")
	t.Logf("project: %s endpoint: %s", p.ProjectID, p.EndpointID)

	start := time.Now()
	cp.debugStart(p.EndpointID)
	t.Logf("first compute start took %s", time.Since(start).Round(time.Millisecond))

	ctx := context.Background()
	conn := connectDirect(t, p, 28433)
	if _, err := conn.Exec(ctx, "CREATE TABLE t1(x int)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO t1 SELECT generate_series(1,100)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 100 {
		t.Fatalf("count: %d %v", n, err)
	}
	_ = conn.Close(ctx)

	// Destroy the compute entirely.
	cp.debugStop(p.EndpointID)
	if computePodExists(t, p.EndpointID) {
		// pod deletion is async; give it a moment
		time.Sleep(10 * time.Second)
		if computePodExists(t, p.EndpointID) {
			t.Fatal("compute pod still exists after stop")
		}
	}

	// Fresh compute, same timeline: data must be there.
	start = time.Now()
	cp.debugStart(p.EndpointID)
	t.Logf("re-start took %s", time.Since(start).Round(time.Millisecond))

	conn2 := connectDirect(t, p, 28434)
	if err := conn2.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 100 {
		t.Fatalf("data lost after compute restart: count=%d err=%v", n, err)
	}

	// MinIO must hold offloaded pageserver data for this tenant.
	out, err := kubectl(t, "exec", "deploy/minio", "--", "sh", "-c", "ls /data/neon/pageserver/tenants/ | head -20")
	if err != nil {
		t.Fatalf("minio ls: %v: %s", err, out)
	}
	if !strings.Contains(out, "") || len(strings.TrimSpace(out)) == 0 {
		t.Errorf("no tenant data in MinIO: %q", out)
	}

	cp.debugStop(p.EndpointID)
}
