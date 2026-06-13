//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"
)

type branch struct {
	BranchID      string `json:"branch_id"`
	EndpointID    string `json:"endpoint_id"`
	Host          string `json:"host"`
	Role          string `json:"role"`
	Database      string `json:"database"`
	ConnectionURI string `json:"connection_uri"`
}

// TestM3BranchIsolation: write 100 rows on main, branch, verify the branch
// sees them (copy-on-write), diverge both sides, verify isolation, then
// delete the branch without touching main.
func TestM3BranchIsolation(t *testing.T) {
	cp := newControlPlane(t, 28084)
	portForward(t, "svc/proxy", 25435, 4432)
	ctx := context.Background()

	p := cp.createProject("m3-branch")
	conn, _ := connectViaProxy(t, p, 25435, 3*time.Minute)
	if _, err := conn.Exec(ctx, "CREATE TABLE t1(x int); INSERT INTO t1 SELECT generate_series(1,100)"); err != nil {
		t.Fatalf("seed main: %v", err)
	}
	// Branch creation copies up to the ancestor's last LSN; make sure our
	// rows are flushed (they are — committed transactions are quorum-acked
	// on safekeepers before commit returns).

	var br branch
	start := time.Now()
	code, raw := cp.doJSON(http.MethodPost, "/v1/projects/"+p.ProjectID+"/branches", `{"name":"preview"}`, &br)
	if code != http.StatusCreated {
		t.Fatalf("create branch: %d %s", code, raw)
	}
	t.Logf("branch created in %s", time.Since(start).Round(time.Millisecond))

	// Connect to the branch endpoint: project credentials are inherited via COW.
	bp := project{EndpointID: br.EndpointID, Host: br.Host, Role: p.Role, Password: p.Password, Database: p.Database}
	bconn, wakeTime := connectViaProxy(t, bp, 25435, 3*time.Minute)
	t.Logf("branch endpoint cold start: %s", wakeTime.Round(time.Millisecond))

	var n int
	if err := bconn.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 100 {
		t.Fatalf("branch must inherit 100 rows, got %d (%v)", n, err)
	}

	// Diverge: branch gets +50, main gets +10. Each side must only see its own.
	if _, err := bconn.Exec(ctx, "INSERT INTO t1 SELECT generate_series(101,150)"); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO t1 SELECT generate_series(1001,1010)"); err != nil {
		t.Fatalf("insert main: %v", err)
	}
	if err := bconn.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 150 {
		t.Errorf("branch count: %d (want 150)", n)
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 110 {
		t.Errorf("main count: %d (want 110)", n)
	}

	// Delete the branch; main must be unaffected.
	_ = bconn.Close(ctx)
	code, raw = cp.doJSON(http.MethodDelete, "/v1/projects/"+p.ProjectID+"/branches/"+br.BranchID, "", nil)
	if code != http.StatusOK {
		t.Fatalf("delete branch: %d %s", code, raw)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && computePodExists(t, br.EndpointID) {
		time.Sleep(2 * time.Second)
	}
	if computePodExists(t, br.EndpointID) {
		t.Error("branch compute pod still exists after delete")
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM t1").Scan(&n); err != nil || n != 110 {
		t.Errorf("main affected by branch delete: %d %v", n, err)
	}

	// Usage should report the remaining branch.
	var usage struct {
		Total uint64 `json:"total_logical_size_bytes"`
	}
	code, raw = cp.doJSON(http.MethodGet, "/v1/projects/"+p.ProjectID+"/usage", "", &usage)
	if code != http.StatusOK || usage.Total == 0 {
		t.Errorf("usage: %d %s", code, raw)
	}
	t.Logf("project logical size: %d bytes", usage.Total)

	cp.debugStop(p.EndpointID)
}
