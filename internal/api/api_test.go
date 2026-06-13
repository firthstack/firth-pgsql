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

	"github.com/insforge/fly-pgsql/internal/api"
	"github.com/insforge/fly-pgsql/internal/compute"
	"github.com/insforge/fly-pgsql/internal/neonclient"
	"github.com/insforge/fly-pgsql/internal/state"
)

// fakePageserver records calls and can be told to fail.
type fakePageserver struct {
	mu        sync.Mutex
	calls     []string
	failAll   bool
	srv       *httptest.Server
}

func newFakePageserver(t *testing.T) *fakePageserver {
	f := &fakePageserver{}
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
			w.WriteHeader(http.StatusAccepted)
		default: // GET timeline detail (also used by delete polling)
			if strings.Contains(r.URL.Path, "/timeline/") {
				w.WriteHeader(http.StatusNotFound)
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
		Runtime:    compute.NewK8sRuntime(nil, "fly-pgsql", "img"), // unused in these tests
		Waker:      waker,
		Cfg: api.Config{
			Domain:               "db.127-0-0-1.sslip.io",
			ProxyPort:            5432,
			PageserverConnstring: "host=pageserver port=6400",
			Safekeepers:          []string{"sk0:5454", "sk1:5454", "sk2:5454"},
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
