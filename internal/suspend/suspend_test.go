package suspend_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/firthstack/firth-pgsql/internal/compute"
	"github.com/firthstack/firth-pgsql/internal/state"
	"github.com/firthstack/firth-pgsql/internal/suspend"
)

type fakeRuntime struct {
	mu        sync.Mutex
	pods      map[string]bool
	stopCalls atomic.Int32
}

func (f *fakeRuntime) Start(_ context.Context, id string, _ compute.ComputeConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods[id] = true
	return nil
}
func (f *fakeRuntime) Stop(_ context.Context, id string) error {
	f.stopCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pods, id)
	return nil
}
func (f *fakeRuntime) Status(_ context.Context, id string) (compute.PodStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.pods[id] {
		return compute.PodStatus{Exists: false}, nil
	}
	return compute.PodStatus{Exists: true, PodIP: "10.0.0.7", Phase: "Running"}, nil
}

// fakeComputeCtl serves /status with a programmable last_active and records
// /terminate calls.
type fakeComputeCtl struct {
	srv            *httptest.Server
	lastActive     atomic.Value // string (RFC3339) or "" for null
	terminateCalls atomic.Int32
	terminateMode  atomic.Value // string
}

func newFakeComputeCtl(t *testing.T) *fakeComputeCtl {
	f := &fakeComputeCtl{}
	f.lastActive.Store("")
	f.terminateMode.Store("")
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/status":
			la := f.lastActive.Load().(string)
			if la == "" {
				fmt.Fprint(w, `{"status":"running","last_active":null}`)
			} else {
				fmt.Fprintf(w, `{"status":"running","last_active":%q}`, la)
			}
		case r.URL.Path == "/terminate" && r.Method == http.MethodPost:
			f.terminateCalls.Add(1)
			f.terminateMode.Store(r.URL.Query().Get("mode"))
			fmt.Fprint(w, `{"lsn":"0/2000000"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func setup(t *testing.T) (*suspend.Suspender, *fakeRuntime, *fakeComputeCtl, *state.Store, string) {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dbURL)
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
	if err := store.CreateProject(ctx,
		state.Project{ID: "prj1", Name: "demo", TenantID: strings.Repeat("a", 32), PgVersion: 17, RoleName: "insforge", RoleVerifier: "v"},
		state.Branch{ID: "br-1", ProjectID: "prj1", Name: "main", TimelineID: strings.Repeat("b", 32), IsDefault: true},
		state.Endpoint{ID: "ep-1", BranchID: "br-1", State: "suspended", SuspendAfterSeconds: 300},
	); err != nil {
		t.Fatal(err)
	}

	rt := &fakeRuntime{pods: map[string]bool{}}
	cc := newFakeComputeCtl(t)
	ccURL, _ := url.Parse(cc.srv.URL)
	s := &suspend.Suspender{
		Store:   store,
		Runtime: rt,
		// addr is "ip:55433"; route everything to the fake server instead
		StatusURL:    func(addr string) string { return "http://" + ccURL.Host + "/status" },
		TerminateURL: func(addr string) string { return "http://" + ccURL.Host + "/terminate" },
	}
	return s, rt, cc, store, "ep-1"
}

func makeRunning(t *testing.T, store *state.Store, rt *fakeRuntime, ep string) {
	t.Helper()
	rt.pods[ep] = true
	if ok, err := store.TransitionEndpoint(context.Background(), ep, "suspended", "starting"); err != nil || !ok {
		t.Fatal("seed starting failed")
	}
	if err := store.SetEndpointRunning(context.Background(), ep, "10.0.0.7:55433"); err != nil {
		t.Fatal(err)
	}
}

func TestSuspendIdleEndpoint(t *testing.T) {
	s, rt, cc, store, ep := setup(t)
	makeRunning(t, store, rt, ep)
	cc.lastActive.Store(time.Now().Add(-10 * time.Minute).Format(time.RFC3339))

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if cc.terminateCalls.Load() != 1 || cc.terminateMode.Load() != "fast" {
		t.Errorf("terminate: calls=%d mode=%v", cc.terminateCalls.Load(), cc.terminateMode.Load())
	}
	if rt.stopCalls.Load() != 1 {
		t.Errorf("stop calls: %d", rt.stopCalls.Load())
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.State != "suspended" || got.ComputeAddr != nil {
		t.Errorf("endpoint: %+v", got)
	}
}

func TestKeepActiveEndpoint(t *testing.T) {
	s, rt, cc, store, ep := setup(t)
	makeRunning(t, store, rt, ep)
	cc.lastActive.Store(time.Now().Add(-10 * time.Second).Format(time.RFC3339))

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if cc.terminateCalls.Load() != 0 || rt.stopCalls.Load() != 0 {
		t.Error("active endpoint must not be suspended")
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.State != "running" {
		t.Errorf("state: %s", got.State)
	}
}

func TestLastActiveNullFallback(t *testing.T) {
	s, rt, cc, store, ep := setup(t)
	makeRunning(t, store, rt, ep)
	cc.lastActive.Store("") // null last_active

	// Freshly started (last_started_at=now): grace period applies, no suspend.
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rt.stopCalls.Load() != 0 {
		t.Error("fresh compute with null last_active must not be suspended")
	}

	// Age the start time beyond suspend_after: now it suspends.
	pool, _ := pgxpool.New(context.Background(), os.Getenv("TEST_DATABASE_URL"))
	defer pool.Close()
	if _, err := pool.Exec(context.Background(), `UPDATE endpoints SET last_started_at = now() - interval '20 minutes' WHERE id=$1`, ep); err != nil {
		t.Fatal(err)
	}
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rt.stopCalls.Load() != 1 {
		t.Errorf("aged compute with null last_active should suspend, stops=%d", rt.stopCalls.Load())
	}
}

func TestReconcileRunningButPodGone(t *testing.T) {
	s, rt, _, store, ep := setup(t)
	makeRunning(t, store, rt, ep)
	rt.pods = map[string]bool{} // pod vanished

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetEndpointByID(context.Background(), ep)
	if got.State != "suspended" {
		t.Errorf("expected reconciled to suspended, got %s", got.State)
	}
}

func TestSweepSkipsConcurrentlyChanged(t *testing.T) {
	s, rt, cc, store, ep := setup(t)
	makeRunning(t, store, rt, ep)
	cc.lastActive.Store(time.Now().Add(-10 * time.Minute).Format(time.RFC3339))
	// Simulate a concurrent transition (e.g. waker grabbed it) by moving the
	// endpoint out of running before the sweep CAS.
	if ok, _ := store.TransitionEndpoint(context.Background(), ep, "running", "suspending"); !ok {
		t.Fatal("seed")
	}
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep must tolerate concurrent changes: %v", err)
	}
}
