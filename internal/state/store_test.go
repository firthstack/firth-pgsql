package state_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/insforge/firth-pgsql/internal/state"
)

func testPool(t *testing.T) *pgxpool.Pool {
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
		t.Fatalf("drop: %v", err)
	}
	if err := state.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func seedProject(t *testing.T, s *state.Store) (state.Project, state.Branch, state.Endpoint) {
	t.Helper()
	p := state.Project{ID: "prj0001", Name: "demo", TenantID: "aaaabbbbccccddddaaaabbbbccccdddd", PgVersion: 17, RoleName: "insforge", RoleVerifier: "SCRAM-SHA-256$4096:c2FsdA==$x:y"}
	b := state.Branch{ID: "br-0001", ProjectID: p.ID, Name: "main", TimelineID: "1111222233334444aaaabbbbccccdddd", IsDefault: true}
	ep := state.Endpoint{ID: "ep-0001", BranchID: b.ID, State: "suspended", SuspendAfterSeconds: 300}
	if err := s.CreateProject(context.Background(), p, b, ep); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	return p, b, ep
}

func TestMigrateIdempotent(t *testing.T) {
	pool := testPool(t)
	if err := state.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestCreateAndGetProject(t *testing.T) {
	pool := testPool(t)
	s := state.New(pool)
	p, b, ep := seedProject(t, s)

	got, err := s.GetProjectByID(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("GetProjectByID: %v", err)
	}
	if got.TenantID != p.TenantID || got.RoleName != p.RoleName || got.RoleVerifier != p.RoleVerifier {
		t.Errorf("project mismatch: %+v", got)
	}

	gotEp, err := s.GetEndpointByID(context.Background(), ep.ID)
	if err != nil {
		t.Fatalf("GetEndpointByID: %v", err)
	}
	if gotEp.State != "suspended" || gotEp.BranchID != b.ID {
		t.Errorf("endpoint mismatch: %+v", gotEp)
	}
	if gotEp.ComputeAddr != nil {
		t.Errorf("expected nil compute_addr, got %v", *gotEp.ComputeAddr)
	}
}

func TestEndpointStateTransition(t *testing.T) {
	pool := testPool(t)
	s := state.New(pool)
	_, _, ep := seedProject(t, s)
	ctx := context.Background()

	ok, err := s.TransitionEndpoint(ctx, ep.ID, "suspended", "starting")
	if err != nil || !ok {
		t.Fatalf("first transition: ok=%v err=%v", ok, err)
	}
	ok, err = s.TransitionEndpoint(ctx, ep.ID, "suspended", "starting")
	if err != nil {
		t.Fatalf("second transition err: %v", err)
	}
	if ok {
		t.Error("second transition should fail (state already changed)")
	}

	if err := s.SetEndpointRunning(ctx, ep.ID, "10.0.0.5:55433"); err != nil {
		t.Fatalf("SetEndpointRunning: %v", err)
	}
	gotEp, _ := s.GetEndpointByID(ctx, ep.ID)
	if gotEp.State != "running" || gotEp.ComputeAddr == nil || *gotEp.ComputeAddr != "10.0.0.5:55433" || gotEp.LastStartedAt == nil {
		t.Errorf("running endpoint mismatch: %+v", gotEp)
	}

	if err := s.SetEndpointSuspended(ctx, ep.ID); err != nil {
		t.Fatalf("SetEndpointSuspended: %v", err)
	}
	gotEp, _ = s.GetEndpointByID(ctx, ep.ID)
	if gotEp.State != "suspended" || gotEp.ComputeAddr != nil {
		t.Errorf("suspended endpoint mismatch: %+v", gotEp)
	}
}

func TestLookupByEndpointish(t *testing.T) {
	pool := testPool(t)
	s := state.New(pool)
	p, b, ep := seedProject(t, s)

	ac, err := s.GetAccessControl(context.Background(), ep.ID)
	if err != nil {
		t.Fatalf("GetAccessControl: %v", err)
	}
	if ac.RoleName != p.RoleName || ac.RoleVerifier != p.RoleVerifier || ac.ProjectID != p.ID || ac.BranchID != b.ID {
		t.Errorf("access control mismatch: %+v", ac)
	}

	if _, err := s.GetAccessControl(context.Background(), "ep-missing"); err != state.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListEndpointsByState(t *testing.T) {
	pool := testPool(t)
	s := state.New(pool)
	_, _, ep := seedProject(t, s)
	ctx := context.Background()

	eps, err := s.ListEndpointsByState(ctx, "suspended")
	if err != nil || len(eps) != 1 || eps[0].ID != ep.ID {
		t.Fatalf("ListEndpointsByState: eps=%+v err=%v", eps, err)
	}
	eps, err = s.ListEndpointsByState(ctx, "running")
	if err != nil || len(eps) != 0 {
		t.Fatalf("expected empty running list: eps=%+v err=%v", eps, err)
	}
}

func TestWithEndpointLock(t *testing.T) {
	pool := testPool(t)
	s := state.New(pool)
	_, _, ep := seedProject(t, s)

	var seen string
	err := s.WithEndpointLock(context.Background(), ep.ID, func(e *state.Endpoint, _ state.Tx) error {
		seen = e.State
		return nil
	})
	if err != nil {
		t.Fatalf("WithEndpointLock: %v", err)
	}
	if seen != "suspended" {
		t.Errorf("expected suspended, got %q", seen)
	}
}
