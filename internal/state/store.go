// Package state persists control-plane metadata: projects (Neon tenants),
// branches (timelines) and endpoints (connectable computes with a lifecycle
// state machine: suspended -> starting -> running -> suspending -> suspended,
// plus failed).
package state

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

var ErrNotFound = errors.New("not found")

// Tx is re-exported so callers of WithEndpointLock can run statements inside
// the locking transaction without importing pgx directly.
type Tx = pgx.Tx

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

type Project struct {
	ID           string
	Name         string
	TenantID     string
	PgVersion    int
	RoleName     string
	RoleVerifier string
}

type Branch struct {
	ID             string
	ProjectID      string
	Name           string
	TimelineID     string
	ParentBranchID *string
	IsDefault      bool
}

type Endpoint struct {
	ID                  string
	BranchID            string
	State               string
	ComputeAddr         *string
	SuspendAfterSeconds int
	LastStartedAt       *time.Time
	LastActiveAt        *time.Time
	UpdatedAt           time.Time
}

type AccessControl struct {
	RoleName     string
	RoleVerifier string
	ProjectID    string
	BranchID     string
}

func (s *Store) CreateProject(ctx context.Context, p Project, b Branch, ep Endpoint) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO projects (id, name, tenant_id, pg_version, role_name, role_verifier) VALUES ($1,$2,$3,$4,$5,$6)`,
			p.ID, p.Name, p.TenantID, p.PgVersion, p.RoleName, p.RoleVerifier); err != nil {
			return err
		}
		return insertBranchAndEndpoint(ctx, tx, b, ep)
	})
}

func (s *Store) CreateBranch(ctx context.Context, b Branch, ep Endpoint) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return insertBranchAndEndpoint(ctx, tx, b, ep)
	})
}

func insertBranchAndEndpoint(ctx context.Context, tx pgx.Tx, b Branch, ep Endpoint) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO branches (id, project_id, name, timeline_id, parent_branch_id, is_default) VALUES ($1,$2,$3,$4,$5,$6)`,
		b.ID, b.ProjectID, b.Name, b.TimelineID, b.ParentBranchID, b.IsDefault); err != nil {
		return err
	}
	state := ep.State
	if state == "" {
		state = "suspended"
	}
	suspendAfter := ep.SuspendAfterSeconds
	if suspendAfter == 0 {
		suspendAfter = 300
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO endpoints (id, branch_id, state, suspend_after_seconds) VALUES ($1,$2,$3,$4)`,
		ep.ID, ep.BranchID, state, suspendAfter)
	return err
}

func (s *Store) GetProjectByID(ctx context.Context, id string) (*Project, error) {
	p := &Project{}
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, tenant_id, pg_version, role_name, role_verifier FROM projects WHERE id=$1`, id).
		Scan(&p.ID, &p.Name, &p.TenantID, &p.PgVersion, &p.RoleName, &p.RoleVerifier)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

func (s *Store) GetBranchByID(ctx context.Context, id string) (*Branch, error) {
	b := &Branch{}
	err := s.pool.QueryRow(ctx,
		`SELECT id, project_id, name, timeline_id, parent_branch_id, is_default FROM branches WHERE id=$1`, id).
		Scan(&b.ID, &b.ProjectID, &b.Name, &b.TimelineID, &b.ParentBranchID, &b.IsDefault)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

func (s *Store) ListBranchesByProject(ctx context.Context, projectID string) ([]Branch, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, project_id, name, timeline_id, parent_branch_id, is_default FROM branches WHERE project_id=$1 ORDER BY created_at`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Branch
	for rows.Next() {
		var b Branch
		if err := rows.Scan(&b.ID, &b.ProjectID, &b.Name, &b.TimelineID, &b.ParentBranchID, &b.IsDefault); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

const endpointCols = `id, branch_id, state, compute_addr, suspend_after_seconds, last_started_at, last_active_at, updated_at`

func scanEndpoint(row pgx.Row) (*Endpoint, error) {
	ep := &Endpoint{}
	err := row.Scan(&ep.ID, &ep.BranchID, &ep.State, &ep.ComputeAddr, &ep.SuspendAfterSeconds,
		&ep.LastStartedAt, &ep.LastActiveAt, &ep.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return ep, err
}

func (s *Store) GetEndpointByID(ctx context.Context, id string) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx, `SELECT `+endpointCols+` FROM endpoints WHERE id=$1`, id))
}

func (s *Store) GetEndpointByBranch(ctx context.Context, branchID string) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx, `SELECT `+endpointCols+` FROM endpoints WHERE branch_id=$1`, branchID))
}

func (s *Store) ListEndpointsByState(ctx context.Context, st string) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+endpointCols+` FROM endpoints WHERE state=$1`, st)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		ep, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ep)
	}
	return out, rows.Err()
}

func (s *Store) GetAccessControl(ctx context.Context, endpointID string) (*AccessControl, error) {
	ac := &AccessControl{}
	err := s.pool.QueryRow(ctx, `
		SELECT p.role_name, p.role_verifier, p.id, b.id
		FROM endpoints e
		JOIN branches b ON b.id = e.branch_id
		JOIN projects p ON p.id = b.project_id
		WHERE e.id = $1`, endpointID).
		Scan(&ac.RoleName, &ac.RoleVerifier, &ac.ProjectID, &ac.BranchID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return ac, err
}

// TransitionEndpoint performs a compare-and-swap state change. It returns
// false (no error) when the endpoint was not in the expected `from` state.
func (s *Store) TransitionEndpoint(ctx context.Context, id, from, to string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET state=$3, updated_at=now() WHERE id=$1 AND state=$2`, id, from, to)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) SetEndpointRunning(ctx context.Context, id, addr string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET state='running', compute_addr=$2, last_started_at=now(), last_active_at=now(), updated_at=now() WHERE id=$1`,
		id, addr)
	return err
}

func (s *Store) SetEndpointSuspended(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET state='suspended', compute_addr=NULL, updated_at=now() WHERE id=$1`, id)
	return err
}

func (s *Store) SetEndpointSuspendAfter(ctx context.Context, id string, seconds int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET suspend_after_seconds=$2, updated_at=now() WHERE id=$1`, id, seconds)
	return err
}

func (s *Store) DeleteProject(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, id)
	return err
}

func (s *Store) DeleteBranch(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM branches WHERE id=$1`, id)
	return err
}

// WithEndpointLock runs fn while holding a row-level FOR UPDATE lock on the
// endpoint, serializing concurrent wake/suspend decisions for it.
func (s *Store) WithEndpointLock(ctx context.Context, id string, fn func(ep *Endpoint, tx Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ep, err := scanEndpoint(tx.QueryRow(ctx, `SELECT `+endpointCols+` FROM endpoints WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		return fn(ep, tx)
	})
}
