package proxycontract_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/firthstack/firth-pgsql/internal/proxycontract"
	"github.com/firthstack/firth-pgsql/internal/state"
)

type stubWaker struct {
	addr string
	err  error
}

func (s *stubWaker) Wake(_ context.Context, _ string) (string, error) { return s.addr, s.err }

const verifier = "SCRAM-SHA-256$4096:c2FsdA==$c3Q=:c2s="

func setup(t *testing.T, w proxycontract.Waker) http.Handler {
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
	if err := store.CreateProject(ctx,
		state.Project{ID: "prj1", Name: "demo", TenantID: strings.Repeat("a", 32), PgVersion: 17, RoleName: "insforge", RoleVerifier: verifier},
		state.Branch{ID: "br-1", ProjectID: "prj1", Name: "main", TimelineID: strings.Repeat("b", 32), IsDefault: true},
		state.Endpoint{ID: "ep-abc", BranchID: "br-1", State: "suspended"},
	); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	(&proxycontract.Handlers{Store: store, Waker: w}).Register(mux)
	return mux
}

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var m map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &m)
	return rec.Code, m
}

func TestGetEndpointAccessControl(t *testing.T) {
	h := setup(t, &stubWaker{addr: "10.0.0.7:55433"})
	code, m := get(t, h, "/proxy/api/get_endpoint_access_control?session_id=u&application_name=psql&endpointish=ep-abc&role=insforge")
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, m)
	}
	if m["role_secret"] != verifier {
		t.Errorf("role_secret: %v", m["role_secret"])
	}
	if m["project_id"] != "prj1" {
		t.Errorf("project_id: %v", m["project_id"])
	}
	if v, present := m["allowed_ips"]; present && v != nil {
		t.Errorf("allowed_ips should be null/absent: %v", v)
	}
}

func TestAccessControlUnknownEndpoint(t *testing.T) {
	h := setup(t, &stubWaker{})
	code, m := get(t, h, "/proxy/api/get_endpoint_access_control?endpointish=ep-nope&role=insforge")
	if code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
	status, _ := m["status"].(map[string]any)
	if status == nil {
		t.Fatalf("missing status: %v", m)
	}
	details, _ := status["details"].(map[string]any)
	errInfo, _ := details["error_info"].(map[string]any)
	if errInfo["reason"] != "ENDPOINT_NOT_FOUND" {
		t.Errorf("reason: %v", m)
	}
}

func TestAccessControlUnknownRole(t *testing.T) {
	h := setup(t, &stubWaker{})
	code, m := get(t, h, "/proxy/api/get_endpoint_access_control?endpointish=ep-abc&role=stranger")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if m["role_secret"] != "" {
		t.Errorf("unknown role must yield empty role_secret: %v", m)
	}
}

func TestWakeCompute(t *testing.T) {
	h := setup(t, &stubWaker{addr: "10.0.0.7:55433"})
	code, m := get(t, h, "/proxy/api/wake_compute?session_id=u&application_name=psql&endpointish=ep-abc")
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, m)
	}
	if m["address"] != "10.0.0.7:55433" {
		t.Errorf("address: %v", m["address"])
	}
	aux, _ := m["aux"].(map[string]any)
	if aux == nil {
		t.Fatal("missing aux")
	}
	for k, want := range map[string]string{
		"endpoint_id": "ep-abc", "project_id": "prj1", "branch_id": "br-1",
		"compute_id": "compute-ep-abc", "cold_start_info": "unknown",
	} {
		if aux[k] != want {
			t.Errorf("aux.%s: got %v want %s", k, aux[k], want)
		}
	}
}

func TestWakeComputeFailure(t *testing.T) {
	h := setup(t, &stubWaker{err: errors.New("no capacity")})
	code, m := get(t, h, "/proxy/api/wake_compute?endpointish=ep-abc")
	if code != http.StatusInternalServerError {
		t.Fatalf("status %d", code)
	}
	if m["error"] == nil {
		t.Errorf("missing error field: %v", m)
	}
}

func TestWakeComputeUnknownEndpoint(t *testing.T) {
	h := setup(t, &stubWaker{addr: "x"})
	code, m := get(t, h, "/proxy/api/wake_compute?endpointish=ep-nope")
	if code != http.StatusNotFound {
		t.Fatalf("status %d: %v", code, m)
	}
}

func TestLegacyAliases(t *testing.T) {
	h := setup(t, &stubWaker{addr: "10.0.0.7:55433"})
	code, m := get(t, h, "/proxy/api/proxy_get_role_secret?endpointish=ep-abc&role=insforge")
	if code != http.StatusOK || m["role_secret"] != verifier {
		t.Errorf("proxy_get_role_secret alias: %d %v", code, m)
	}
	code, m = get(t, h, "/proxy/api/proxy_wake_compute?endpointish=ep-abc")
	if code != http.StatusOK || m["address"] != "10.0.0.7:55433" {
		t.Errorf("proxy_wake_compute alias: %d %v", code, m)
	}
}

func TestJwks(t *testing.T) {
	h := setup(t, &stubWaker{})
	code, m := get(t, h, "/proxy/api/endpoints/ep-abc/jwks?session_id=u")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if jwks, ok := m["jwks"].([]any); !ok || len(jwks) != 0 {
		t.Errorf("jwks: %v", m)
	}
}
