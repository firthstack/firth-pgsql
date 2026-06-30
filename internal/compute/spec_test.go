package compute_test

import (
	"encoding/json"
	"testing"

	"github.com/firthstack/firth-pgsql/internal/compute"
)

func testParams() compute.SpecParams {
	return compute.SpecParams{
		TenantID:             "aaaabbbbccccddddaaaabbbbccccdddd",
		TimelineID:           "1111222233334444aaaabbbbccccdddd",
		RoleName:             "insforge",
		RoleVerifier:         "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0c2FsdA==$AAAA:BBBB",
		DatabaseName:         "appdb",
		PageserverConnstring: "host=pageserver port=6400",
		Safekeepers: []string{
			"safekeeper-0.safekeeper:5454",
			"safekeeper-1.safekeeper:5454",
			"safekeeper-2.safekeeper:5454",
		},
	}
}

func TestBuildComputeConfig(t *testing.T) {
	cfg := compute.BuildComputeConfig(testParams())

	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	spec, ok := m["spec"].(map[string]any)
	if !ok {
		t.Fatal("missing top-level spec")
	}
	if spec["format_version"] != float64(1.0) {
		t.Errorf("format_version: %v", spec["format_version"])
	}
	if spec["suspend_timeout_seconds"] != float64(-1) {
		t.Errorf("suspend_timeout_seconds: %v", spec["suspend_timeout_seconds"])
	}
	if spec["tenant_id"] != "aaaabbbbccccddddaaaabbbbccccdddd" || spec["timeline_id"] != "1111222233334444aaaabbbbccccdddd" {
		t.Errorf("tenant/timeline: %v %v", spec["tenant_id"], spec["timeline_id"])
	}
	if spec["mode"] != "Primary" {
		t.Errorf("mode: %v", spec["mode"])
	}
	if spec["pageserver_connstring"] != "host=pageserver port=6400" {
		t.Errorf("pageserver_connstring: %v", spec["pageserver_connstring"])
	}
	sks, _ := spec["safekeeper_connstrings"].([]any)
	if len(sks) != 3 || sks[0] != "safekeeper-0.safekeeper:5454" {
		t.Errorf("safekeeper_connstrings: %v", sks)
	}

	cluster, _ := spec["cluster"].(map[string]any)
	if cluster == nil {
		t.Fatal("missing cluster")
	}

	roles, _ := cluster["roles"].([]any)
	if len(roles) != 2 {
		t.Fatalf("expected 2 roles, got %d", len(roles))
	}
	admin := roles[0].(map[string]any)
	if admin["name"] != "cloud_admin" || admin["encrypted_password"] != "b093c0d3b281ba6da1eacc608620abd8" {
		t.Errorf("cloud_admin role: %v", admin)
	}
	app := roles[1].(map[string]any)
	if app["name"] != "insforge" || app["encrypted_password"] != "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0c2FsdA==$AAAA:BBBB" {
		t.Errorf("app role: %v", app)
	}

	dbs, _ := cluster["databases"].([]any)
	if len(dbs) != 1 {
		t.Fatalf("expected 1 database, got %d", len(dbs))
	}
	db := dbs[0].(map[string]any)
	if db["name"] != "appdb" || db["owner"] != "insforge" {
		t.Errorf("database: %v", db)
	}

	settings, _ := cluster["settings"].([]any)
	want := map[string]string{
		"port":                       "55433",
		"listen_addresses":           "0.0.0.0",
		"password_encryption":        "scram-sha-256",
		"synchronous_standby_names":  "walproposer",
		"shared_preload_libraries":   "neon,pg_stat_statements",
		"wal_level":                  "logical",
	}
	found := map[string]string{}
	for _, s := range settings {
		sm := s.(map[string]any)
		found[sm["name"].(string)] = sm["value"].(string)
	}
	for k, v := range want {
		if found[k] != v {
			t.Errorf("setting %s: got %q want %q", k, found[k], v)
		}
	}

	cc, _ := m["compute_ctl_config"].(map[string]any)
	if cc == nil {
		t.Fatal("missing compute_ctl_config")
	}
	jwks, _ := cc["jwks"].(map[string]any)
	if keys, ok := jwks["keys"].([]any); !ok || len(keys) != 0 {
		t.Errorf("jwks.keys should be empty array: %v", jwks)
	}

	// delta_operations must serialize as [] not null (upstream config has it)
	if _, ok := spec["delta_operations"]; !ok {
		t.Error("missing delta_operations")
	}
}
