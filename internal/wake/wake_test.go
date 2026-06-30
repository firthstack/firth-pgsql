package wake_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/insforge/firth-pgsql/internal/compute"
	"github.com/insforge/firth-pgsql/internal/state"
	"github.com/insforge/firth-pgsql/internal/wake"
)

// fakeRuntime is a programmable in-memory Runtime.
type fakeRuntime struct {
	mu         sync.Mutex
	pods       map[string]bool
	podIP      string
	startCalls atomic.Int32
	stopCalls  atomic.Int32
	startErr   error
}

func newFakeRuntime(ip string) *fakeRuntime {
	return &fakeRuntime{pods: map[string]bool{}, podIP: ip}
}

func (f *fakeRuntime) Start(_ context.Context, id string, _ compute.ComputeConfig) error {
	f.startCalls.Add(1)
	if f.startErr != nil {
		return f.startErr
	}
	f.mu.Lock()
	f.pods[id] = true
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, id string) error {
	f.stopCalls.Add(1)
	f.mu.Lock()
	delete(f.pods, id)
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Status(_ context.Context, id string) (compute.PodStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.pods[id] {
		return compute.PodStatus{Exists: false}, nil
	}
	return compute.PodStatus{Exists: true, PodIP: f.podIP, Phase: "Running"}, nil
}

func (f *fakeRuntime) setPod(id string, exists bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if exists {
		f.pods[id] = true
	} else {
		delete(f.pods, id)
	}
}

func setup(t *testing.T) (*wake.Waker, *fakeRuntime, *state.Store, string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS endpoints, branches, projects CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := state.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := state.New(pool)
	p := state.Project{ID: "prj1", Name: "demo", TenantID: strings.Repeat("a", 32), PgVersion: 17, RoleName: "insforge", RoleVerifier: "SCRAM-SHA-256$4096:s$a:b"}
	b := state.Branch{ID: "br-1", ProjectID: "prj1", Name: "main", TimelineID: strings.Repeat("b", 32), IsDefault: true}
	ep := state.Endpoint{ID: "ep-1", BranchID: "br-1", State: "suspended"}
	if err := store.CreateProject(ctx, p, b, ep); err != nil {
		t.Fatal(err)
	}

	// fake compute_ctl /status that immediately reports running
	statusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"running"}`))
	}))
	t.Cleanup(statusSrv.Close)

	rt := newFakeRuntime("10.0.0.7")
	w := &wake.Waker{
		Store:   store,
		Runtime: rt,
		SpecBuilder: func(ctx context.Context, endpointID string) (compute.ComputeConfig, error) {
			return compute.BuildComputeConfig(compute.SpecParams{
				TenantID: p.TenantID, TimelineID: b.TimelineID,
				RoleName: p.RoleName, RoleVerifier: p.RoleVerifier,
				DatabaseName: "appdb", PageserverConnstring: "host=ps port=6400",
				Safekeepers: []string{"sk:5454"},
			}), nil
		},
		ReadyTimeout: 10 * time.Second,
		PollInterval: 10 * time.Millisecond,
		StatusURL:    func(podIP string) string { return statusSrv.URL },
	}
	return w, rt, store, "ep-1"
}

func TestWakeFromSuspended(t *testing.T) {
	w, rt, store, ep := setup(t)
	addr, err := w.Wake(context.Background(), ep)
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if addr != "10.0.0.7:55433" {
		t.Errorf("addr: %s", addr)
	}
	if rt.startCalls.Load() != 1 {
		t.Errorf("start calls: %d", rt.startCalls.Load())
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.State != "running" || got.ComputeAddr == nil || *got.ComputeAddr != addr {
		t.Errorf("endpoint: %+v", got)
	}
}

func TestWakeWhenRunningHealthy(t *testing.T) {
	w, rt, _, ep := setup(t)
	if _, err := w.Wake(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	addr, err := w.Wake(context.Background(), ep)
	if err != nil || addr != "10.0.0.7:55433" {
		t.Fatalf("second wake: %s %v", addr, err)
	}
	if rt.startCalls.Load() != 1 {
		t.Errorf("expected no extra start, got %d", rt.startCalls.Load())
	}
}

func TestWakeWhenRunningButPodGone(t *testing.T) {
	w, rt, _, ep := setup(t)
	if _, err := w.Wake(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	rt.setPod(ep, false) // pod vanished behind our back
	addr, err := w.Wake(context.Background(), ep)
	if err != nil || addr != "10.0.0.7:55433" {
		t.Fatalf("re-wake: %s %v", addr, err)
	}
	if rt.startCalls.Load() != 2 {
		t.Errorf("expected restart, start calls=%d", rt.startCalls.Load())
	}
}

// If the pod was recreated with a new IP while state still records the old
// address, Wake must not return the stale address — it reconciles and restarts.
func TestWakeRunningButPodIPChanged(t *testing.T) {
	w, rt, store, ep := setup(t)
	if _, err := w.Wake(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	// Pod now reports a different IP than the stored addr (10.0.0.7:55433).
	rt.mu.Lock()
	rt.podIP = "10.0.0.99"
	rt.mu.Unlock()

	addr, err := w.Wake(context.Background(), ep)
	if err != nil {
		t.Fatalf("re-wake: %v", err)
	}
	if addr != "10.0.0.99:55433" {
		t.Errorf("expected reconciled address with new IP, got %s", addr)
	}
	if rt.startCalls.Load() != 2 {
		t.Errorf("expected restart on stale IP, start calls=%d", rt.startCalls.Load())
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.ComputeAddr == nil || *got.ComputeAddr != "10.0.0.99:55433" {
		t.Errorf("stored addr not updated: %+v", got.ComputeAddr)
	}
}

func TestConcurrentWakeSingleStart(t *testing.T) {
	w, rt, _, ep := setup(t)
	const n = 10
	addrs := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addrs[i], errs[i] = w.Wake(context.Background(), ep)
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("wake %d: %v", i, errs[i])
		}
		if addrs[i] != "10.0.0.7:55433" {
			t.Errorf("wake %d addr: %s", i, addrs[i])
		}
	}
	if rt.startCalls.Load() != 1 {
		t.Errorf("expected exactly 1 start, got %d", rt.startCalls.Load())
	}
}

func TestWakeStaleStarting(t *testing.T) {
	w, _, store, ep := setup(t)
	ctx := context.Background()
	if ok, _ := store.TransitionEndpoint(ctx, ep, "suspended", "starting"); !ok {
		t.Fatal("seed transition failed")
	}
	// Make the starting state look stale (owner crashed 5 minutes ago).
	pool, _ := pgxpool.New(ctx, os.Getenv("TEST_DATABASE_URL"))
	defer pool.Close()
	if _, err := pool.Exec(ctx, `UPDATE endpoints SET updated_at = now() - interval '5 minutes' WHERE id=$1`, ep); err != nil {
		t.Fatal(err)
	}
	addr, err := w.Wake(ctx, ep)
	if err != nil || addr != "10.0.0.7:55433" {
		t.Fatalf("stale takeover: %s %v", addr, err)
	}
}

func TestWakeStartFailure(t *testing.T) {
	w, rt, store, ep := setup(t)
	rt.startErr = errors.New("image pull failed")
	if _, err := w.Wake(context.Background(), ep); err == nil {
		t.Fatal("expected error")
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.State != "failed" {
		t.Errorf("state: %s", got.State)
	}
	// failed endpoints are retryable
	rt.startErr = nil
	addr, err := w.Wake(context.Background(), ep)
	if err != nil || addr != "10.0.0.7:55433" {
		t.Fatalf("retry after failure: %s %v", addr, err)
	}
}
