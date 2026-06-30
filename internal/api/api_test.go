package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/firthstack/firth-pgsql/internal/api"
	"github.com/firthstack/firth-pgsql/internal/compute"
	"github.com/firthstack/firth-pgsql/internal/neonclient"
	"github.com/firthstack/firth-pgsql/internal/state"
)

// fakePageserver records calls and can be told to fail.
type fakePageserver struct {
	mu      sync.Mutex
	calls   []string
	deleted map[string]bool
	failAll bool
	srv     *httptest.Server
}

func newFakePageserver(t *testing.T) *fakePageserver {
	f := &fakePageserver{deleted: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		fail := f.failAll
		f.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"msg":"boom"}`))
			return
		}
		switch {
		case r.Method == http.MethodPut:
			w.Write([]byte(`{"shards":[]}`))
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"timeline_id":"x"}`))
		case r.Method == http.MethodDelete:
			f.mu.Lock()
			f.deleted[r.URL.Path] = true
			f.mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default: // GET timeline detail (delete polling + usage)
			if strings.Contains(r.URL.Path, "/timeline/") {
				f.mu.Lock()
				gone := f.deleted[r.URL.Path]
				f.mu.Unlock()
				if gone {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Write([]byte(`{"timeline_id":"x","last_record_lsn":"0/1","current_logical_size":4096}`))
				return
			}
			w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePageserver) callsMatching(sub string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.Contains(c, sub) {
			n++
		}
	}
	return n
}

type stubWaker struct {
	addr  string
	calls int
}

func (s *stubWaker) Wake(ctx context.Context, endpointID string) (string, error) {
	s.calls++
	return s.addr, nil
}

// stubSafekeeper returns a fixed commit LSN <= the fake pageserver's
// last_record_lsn ("0/1"), so waitAncestorIngested returns immediately.
type stubSafekeeper struct{ commit string }

func (s stubSafekeeper) MaxCommitLSN(_ context.Context, _, _ string) (string, error) {
	if s.commit == "" {
		return "0/1", nil
	}
	return s.commit, nil
}

func testServer(t *testing.T) (*api.Server, *fakePageserver, *stubWaker, *state.Store) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
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
	ps := newFakePageserver(t)
	waker := &stubWaker{addr: "10.0.0.9:55433"}
	srv := &api.Server{
		Store:      store,
		Pageserver: neonclient.NewPageserver(ps.srv.URL),
		Safekeeper: stubSafekeeper{},
		Runtime:    compute.NewK8sRuntime(fake.NewClientset(), "firth-pgsql", "img"),
		Waker:      waker,
		Cfg: api.Config{
			Domain:               "db.127-0-0-1.sslip.io",
			ProxyPort:            5432,
			PageserverConnstring: "host=pageserver port=6400",
			Safekeepers:          []string{"sk0:5454", "sk1:5454", "sk2:5454"},
			EnableDebug:          true,
		},
	}
	return srv, ps, waker, store
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var m map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	return rec, m
}

func TestCreateProject(t *testing.T) {
	srv, ps, _, store := testServer(t)
	h := srv.Routes()

	rec, m := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	for _, k := range []string{"project_id", "branch_id", "endpoint_id", "role", "password", "host", "database", "connection_uri"} {
		if m[k] == nil || m[k] == "" {
			t.Errorf("missing %s in response: %v", k, m)
		}
	}
	if !strings.HasPrefix(m["host"].(string), m["endpoint_id"].(string)+".") {
		t.Errorf("host should start with endpoint id: %v", m["host"])
	}
	if ps.callsMatching("location_config") != 1 {
		t.Error("pageserver location_config not called")
	}
	if ps.callsMatching("POST") != 1 {
		t.Error("pageserver timeline create not called")
	}

	p, err := store.GetProjectByID(context.Background(), m["project_id"].(string))
	if err != nil {
		t.Fatalf("project not persisted: %v", err)
	}
	if !strings.HasPrefix(p.RoleVerifier, "SCRAM-SHA-256$") {
		t.Errorf("role verifier not scram: %s", p.RoleVerifier)
	}
}

func TestCreateProjectRollbackOnPageserverError(t *testing.T) {
	srv, ps, _, store := testServer(t)
	ps.failAll = true
	h := srv.Routes()

	rec, _ := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rec.Code)
	}
	eps, err := store.ListEndpointsByState(context.Background(), "suspended")
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 0 {
		t.Errorf("expected no persisted endpoints, got %d", len(eps))
	}
}

func TestGetProject(t *testing.T) {
	srv, _, _, _ := testServer(t)
	h := srv.Routes()
	_, created := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})

	rec, m := doJSON(t, h, http.MethodGet, "/v1/projects/"+created["project_id"].(string), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	branches, _ := m["branches"].([]any)
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch: %v", m)
	}
	b := branches[0].(map[string]any)
	if b["is_default"] != true || b["endpoint_id"] != created["endpoint_id"] {
		t.Errorf("branch mismatch: %v", b)
	}
}

func TestCreateBranch(t *testing.T) {
	srv, ps, _, store := testServer(t)
	h := srv.Routes()
	_, proj := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	projectID := proj["project_id"].(string)

	rec, m := doJSON(t, h, http.MethodPost, "/v1/projects/"+projectID+"/branches", map[string]string{"name": "preview"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	for _, k := range []string{"branch_id", "endpoint_id", "host", "connection_uri"} {
		if m[k] == nil || m[k] == "" {
			t.Errorf("missing %s: %v", k, m)
		}
	}
	if m["password"] != nil {
		t.Errorf("branch must not return a new password (role is project-wide): %v", m["password"])
	}
	// pageserver must have received a branch creation (2 timeline POSTs total)
	if got := ps.callsMatching("POST"); got != 2 {
		t.Errorf("expected 2 timeline POSTs, got %d", got)
	}

	b, err := store.GetBranchByID(context.Background(), m["branch_id"].(string))
	if err != nil {
		t.Fatalf("branch not persisted: %v", err)
	}
	if b.ParentBranchID == nil || *b.ParentBranchID != proj["branch_id"].(string) {
		t.Errorf("parent branch: %v", b.ParentBranchID)
	}
	if b.IsDefault {
		t.Error("new branch must not be default")
	}
}

func TestCreateBranchFromBranch(t *testing.T) {
	srv, _, _, store := testServer(t)
	h := srv.Routes()
	_, proj := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	projectID := proj["project_id"].(string)
	_, b1 := doJSON(t, h, http.MethodPost, "/v1/projects/"+projectID+"/branches", map[string]string{"name": "b1"})

	rec, b2 := doJSON(t, h, http.MethodPost, "/v1/projects/"+projectID+"/branches",
		map[string]string{"name": "b2", "parent_branch_id": b1["branch_id"].(string)})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got, _ := store.GetBranchByID(context.Background(), b2["branch_id"].(string))
	if got.ParentBranchID == nil || *got.ParentBranchID != b1["branch_id"].(string) {
		t.Errorf("parent: %v", got.ParentBranchID)
	}
}

func TestDeleteBranch(t *testing.T) {
	srv, ps, _, store := testServer(t)
	h := srv.Routes()
	_, proj := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	projectID := proj["project_id"].(string)
	_, br := doJSON(t, h, http.MethodPost, "/v1/projects/"+projectID+"/branches", map[string]string{"name": "doomed"})

	rec, _ := doJSON(t, h, http.MethodDelete, "/v1/projects/"+projectID+"/branches/"+br["branch_id"].(string), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status %d", rec.Code)
	}
	if _, err := store.GetBranchByID(context.Background(), br["branch_id"].(string)); err == nil {
		t.Error("branch row still exists")
	}
	if ps.callsMatching("DELETE") < 1 {
		t.Error("pageserver timeline delete not called")
	}

	// default branch cannot be deleted
	rec, _ = doJSON(t, h, http.MethodDelete, "/v1/projects/"+projectID+"/branches/"+proj["branch_id"].(string), nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("deleting default branch must 400, got %d", rec.Code)
	}
}

// A branch may only be deleted through its own project's path. Using another
// project's id must not delete it.
func TestDeleteBranchWrongProjectIsScoped(t *testing.T) {
	srv, _, _, store := testServer(t)
	h := srv.Routes()
	_, projA := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "a"})
	_, projB := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "b"})
	_, brB := doJSON(t, h, http.MethodPost, "/v1/projects/"+projB["project_id"].(string)+"/branches",
		map[string]string{"name": "victim"})

	// Try to delete project B's branch via project A's path.
	rec, _ := doJSON(t, h, http.MethodDelete,
		"/v1/projects/"+projA["project_id"].(string)+"/branches/"+brB["branch_id"].(string), nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-project delete must 404, got %d", rec.Code)
	}
	if _, err := store.GetBranchByID(context.Background(), brB["branch_id"].(string)); err != nil {
		t.Error("victim branch was deleted via another project's path")
	}
}

func TestAuthTokenGate(t *testing.T) {
	srv, _, _, _ := testServer(t)
	srv.Cfg.AuthToken = "s3cret"
	h := srv.Routes()

	// No header → 401.
	req := httptest.NewRequest(http.MethodPost, "/v1/projects", bytes.NewReader([]byte(`{"name":"x"}`)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token must 401, got %d", rec.Code)
	}

	// Correct header → passes the gate (201).
	req = httptest.NewRequest(http.MethodPost, "/v1/projects", bytes.NewReader([]byte(`{"name":"x"}`)))
	req.Header.Set("Authorization", "Bearer s3cret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("valid token must pass, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDebugEndpointsDisabledByDefault(t *testing.T) {
	srv, _, _, _ := testServer(t)
	srv.Cfg.EnableDebug = false
	h := srv.Routes()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/debug/endpoints/ep-x/start", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("debug routes must be absent when disabled, got %d", rec.Code)
	}
}

func TestDeleteProject(t *testing.T) {
	srv, ps, _, store := testServer(t)
	h := srv.Routes()
	_, proj := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	projectID := proj["project_id"].(string)
	doJSON(t, h, http.MethodPost, "/v1/projects/"+projectID+"/branches", map[string]string{"name": "extra"})

	rec, _ := doJSON(t, h, http.MethodDelete, "/v1/projects/"+projectID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status %d", rec.Code)
	}
	if _, err := store.GetProjectByID(context.Background(), projectID); err == nil {
		t.Error("project row still exists")
	}
	if ps.callsMatching("DELETE /v1/tenant/") < 1 {
		t.Error("pageserver tenant delete not called")
	}
}

func TestUsage(t *testing.T) {
	srv, _, _, _ := testServer(t)
	h := srv.Routes()
	_, proj := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})
	projectID := proj["project_id"].(string)

	rec, m := doJSON(t, h, http.MethodGet, "/v1/projects/"+projectID+"/usage", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("usage status %d: %s", rec.Code, rec.Body.String())
	}
	if _, ok := m["total_logical_size_bytes"]; !ok {
		t.Errorf("missing total: %v", m)
	}
	if _, ok := m["branches"]; !ok {
		t.Errorf("missing branches: %v", m)
	}
}

func TestDebugStartUsesWaker(t *testing.T) {
	srv, _, waker, _ := testServer(t)
	h := srv.Routes()
	_, created := doJSON(t, h, http.MethodPost, "/v1/projects", map[string]string{"name": "demo"})

	rec, m := doJSON(t, h, http.MethodPost, "/v1/debug/endpoints/"+created["endpoint_id"].(string)+"/start", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if m["address"] != "10.0.0.9:55433" || waker.calls != 1 {
		t.Errorf("waker not used: %v calls=%d", m, waker.calls)
	}
}
