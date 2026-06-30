// Package api serves the FirthStack-facing (northbound) REST API plus debug
// endpoints. The Neon-proxy-facing contract lives in internal/proxycontract.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/firthstack/firth-pgsql/internal/compute"
	"github.com/firthstack/firth-pgsql/internal/ids"
	"github.com/firthstack/firth-pgsql/internal/lsn"
	"github.com/firthstack/firth-pgsql/internal/neonclient"
	"github.com/firthstack/firth-pgsql/internal/scram"
	"github.com/firthstack/firth-pgsql/internal/state"
)

// Waker is implemented by internal/wake. The debug start endpoint and the
// proxy contract share it.
type Waker interface {
	Wake(ctx context.Context, endpointID string) (addr string, err error)
}

// SafekeeperLSN reports a timeline's quorum-committed LSN. Implemented by
// neonclient.SafekeeperClient; stubbed in tests.
type SafekeeperLSN interface {
	MaxCommitLSN(ctx context.Context, tenantID, timelineID string) (string, error)
}

type Config struct {
	Domain               string // e.g. db.127-0-0-1.sslip.io
	ProxyPort            int    // client-facing port on the proxy (5432 via port-forward)
	PageserverConnstring string
	Safekeepers          []string
	// AuthToken, when non-empty, requires "Authorization: Bearer <token>" on
	// every /v1/* request. Empty disables the check (local dev). This is a
	// coarse service-to-service guard; per-tenant FirthStack JWT auth is future
	// work (jwks integration, M4).
	AuthToken string
	// EnableDebug exposes the destructive /v1/debug/endpoints/* routes. Off by
	// default; only the local-dev deployment turns it on (used by the
	// integration tests). Never enable in a shared/production deployment.
	EnableDebug bool
}

type Server struct {
	Store      *state.Store
	Pageserver *neonclient.PageserverClient
	Safekeeper SafekeeperLSN
	Runtime    compute.Runtime
	Waker      Waker
	Cfg        Config
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	v1 := s.requireAuth
	mux.HandleFunc("POST /v1/projects", v1(s.createProject))
	mux.HandleFunc("GET /v1/projects/{id}", v1(s.getProject))
	mux.HandleFunc("DELETE /v1/projects/{id}", v1(s.deleteProject))
	mux.HandleFunc("POST /v1/projects/{id}/branches", v1(s.createBranch))
	mux.HandleFunc("DELETE /v1/projects/{id}/branches/{bid}", v1(s.deleteBranch))
	mux.HandleFunc("GET /v1/projects/{id}/usage", v1(s.usage))
	if s.Cfg.EnableDebug {
		mux.HandleFunc("POST /v1/debug/endpoints/{id}/start", v1(s.debugStart))
		mux.HandleFunc("POST /v1/debug/endpoints/{id}/stop", v1(s.debugStop))
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// requireAuth gates a handler behind the configured bearer token. When no
// token is configured the handler is returned unchanged (local dev).
func (s *Server) requireAuth(h http.HandlerFunc) http.HandlerFunc {
	if s.Cfg.AuthToken == "" {
		return h
	}
	want := "Bearer " + s.Cfg.AuthToken
	return func(w http.ResponseWriter, r *http.Request) {
		// Constant-time compare to avoid leaking the token via timing.
		got := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) connectionURI(endpointID, role, password, database string) (host, uri string) {
	host = fmt.Sprintf("%s.%s", endpointID, s.Cfg.Domain)
	uri = fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=require", role, password, host, s.Cfg.ProxyPort, database)
	return host, uri
}

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name                string `json:"name"`
		SuspendAfterSeconds int    `json:"suspend_after_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name required")
		return
	}
	ctx := r.Context()

	tenantID := ids.NewHex32()
	timelineID := ids.NewHex32()
	projectID := ids.NewProjectID()
	branchID := ids.NewBranchID()
	endpointID := ids.NewEndpointID()
	const roleName = "firth"
	const dbName = "appdb"

	password, err := scram.RandomPassword()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	verifier, err := scram.BuildVerifier(password)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := s.Pageserver.AttachTenant(ctx, tenantID); err != nil {
		slog.Error("attach tenant", "err", err)
		writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
		return
	}
	if err := s.Pageserver.CreateTimeline(ctx, tenantID, timelineID, 17); err != nil {
		slog.Error("create timeline", "err", err)
		writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
		return
	}

	ep := state.Endpoint{ID: endpointID, BranchID: branchID, State: "suspended"}
	if req.SuspendAfterSeconds > 0 {
		ep.SuspendAfterSeconds = req.SuspendAfterSeconds
	}
	err = s.Store.CreateProject(ctx,
		state.Project{ID: projectID, Name: req.Name, TenantID: tenantID, PgVersion: 17, RoleName: roleName, RoleVerifier: verifier},
		state.Branch{ID: branchID, ProjectID: projectID, Name: "main", TimelineID: timelineID, IsDefault: true},
		ep,
	)
	if err != nil {
		slog.Error("persist project", "err", err)
		// Compensate: drop the just-created tenant so we don't orphan storage
		// resources. Best-effort — log if cleanup also fails.
		if cerr := s.Pageserver.DeleteTenant(ctx, tenantID); cerr != nil {
			slog.Error("cleanup orphaned tenant after persist failure", "tenant", tenantID, "err", cerr)
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	host, uri := s.connectionURI(endpointID, roleName, password, dbName)
	writeJSON(w, http.StatusCreated, map[string]any{
		"project_id":     projectID,
		"branch_id":      branchID,
		"endpoint_id":    endpointID,
		"role":           roleName,
		"password":       password, // returned exactly once; only the verifier is stored
		"host":           host,
		"port":           s.Cfg.ProxyPort,
		"database":       dbName,
		"connection_uri": uri,
	})
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, err := s.Store.GetProjectByID(ctx, r.PathValue("id"))
	if errors.Is(err, state.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	branches, err := s.Store.ListBranchesByProject(ctx, p.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(branches))
	for _, b := range branches {
		ep, err := s.Store.GetEndpointByBranch(ctx, b.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		host := fmt.Sprintf("%s.%s", ep.ID, s.Cfg.Domain)
		out = append(out, map[string]any{
			"branch_id":   b.ID,
			"name":        b.Name,
			"is_default":  b.IsDefault,
			"timeline_id": b.TimelineID,
			"endpoint_id": ep.ID,
			"state":       ep.State,
			"host":        host,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"project_id": p.ID,
		"name":       p.Name,
		"tenant_id":  p.TenantID,
		"branches":   out,
	})
}

func (s *Server) createBranch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req struct {
		Name           string `json:"name"`
		ParentBranchID string `json:"parent_branch_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name required")
		return
	}

	p, err := s.Store.GetProjectByID(ctx, r.PathValue("id"))
	if errors.Is(err, state.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Resolve the ancestor: explicit parent or the default branch.
	var parent *state.Branch
	if req.ParentBranchID != "" {
		parent, err = s.Store.GetBranchByID(ctx, req.ParentBranchID)
		if errors.Is(err, state.ErrNotFound) || (err == nil && parent.ProjectID != p.ID) {
			writeErr(w, http.StatusBadRequest, "parent branch not found in project")
			return
		}
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		branches, err := s.Store.ListBranchesByProject(ctx, p.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		for i := range branches {
			if branches[i].IsDefault {
				parent = &branches[i]
				break
			}
		}
		if parent == nil {
			writeErr(w, http.StatusInternalServerError, "project has no default branch")
			return
		}
	}

	branchID := ids.NewBranchID()
	endpointID := ids.NewEndpointID()
	timelineID := ids.NewHex32()

	// Branch at the parent's committed LSN, after the pageserver has ingested
	// up to it. Without this, a branch created right after writes can miss
	// recently-committed data (the pageserver lags the safekeepers' commit
	// LSN by the streaming delay).
	startLSN, err := s.waitAncestorIngested(ctx, p.TenantID, parent.TimelineID)
	if err != nil {
		slog.Error("wait ancestor ingested", "err", err)
		writeErr(w, http.StatusBadGateway, "branch point not ready: "+err.Error())
		return
	}
	if err := s.Pageserver.CreateBranchAtLSN(ctx, p.TenantID, timelineID, parent.TimelineID, startLSN); err != nil {
		slog.Error("create branch timeline", "err", err)
		writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
		return
	}

	parentID := parent.ID
	err = s.Store.CreateBranch(ctx,
		state.Branch{ID: branchID, ProjectID: p.ID, Name: req.Name, TimelineID: timelineID, ParentBranchID: &parentID},
		state.Endpoint{ID: endpointID, BranchID: branchID, State: "suspended"},
	)
	if err != nil {
		slog.Error("persist branch", "err", err)
		// Compensate: drop the just-created timeline to avoid orphaning it.
		if cerr := s.Pageserver.DeleteTimeline(ctx, p.TenantID, timelineID); cerr != nil {
			slog.Error("cleanup orphaned timeline after persist failure", "timeline", timelineID, "err", cerr)
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	host := fmt.Sprintf("%s.%s", endpointID, s.Cfg.Domain)
	writeJSON(w, http.StatusCreated, map[string]any{
		"branch_id":   branchID,
		"endpoint_id": endpointID,
		"host":        host,
		"port":        s.Cfg.ProxyPort,
		"database":    "appdb",
		"role":        p.RoleName,
		// password is nil by design: the project role (and its password)
		// is inherited by the branch via copy-on-write.
		"password":       nil,
		"connection_uri": fmt.Sprintf("postgresql://%s@%s:%d/appdb?sslmode=require", p.RoleName, host, s.Cfg.ProxyPort),
	})
}

// waitAncestorIngested returns the ancestor's quorum-committed LSN once the
// pageserver has ingested at least up to it, so a branch taken at that LSN
// contains all committed WAL. Polls for up to 30s.
func (s *Server) waitAncestorIngested(ctx context.Context, tenantID, timelineID string) (string, error) {
	commit, err := s.Safekeeper.MaxCommitLSN(ctx, tenantID, timelineID)
	if err != nil {
		return "", fmt.Errorf("commit lsn: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		detail, err := s.Pageserver.GetTimeline(ctx, tenantID, timelineID)
		if err != nil {
			return "", err
		}
		caught, err := lsn.AtLeast(detail.LastRecordLSN, commit)
		if err != nil {
			return "", err
		}
		if caught {
			return commit, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("pageserver did not reach lsn %s (at %s)", commit, detail.LastRecordLSN)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *Server) deleteBranch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b, err := s.Store.GetBranchByID(ctx, r.PathValue("bid"))
	if errors.Is(err, state.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "branch not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	// The branch must belong to the project named in the path; otherwise a
	// request scoped to project A could delete project B's branch by id.
	if b.ProjectID != r.PathValue("id") {
		writeErr(w, http.StatusNotFound, "branch not found")
		return
	}
	if b.IsDefault {
		writeErr(w, http.StatusBadRequest, "cannot delete the default branch; delete the project instead")
		return
	}
	p, err := s.Store.GetProjectByID(ctx, b.ProjectID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	ep, err := s.Store.GetEndpointByBranch(ctx, b.ID)
	if err == nil {
		if err := s.Runtime.Stop(ctx, ep.ID); err != nil {
			slog.Warn("stop compute during branch delete", "endpoint", ep.ID, "err", err)
		}
	}
	if err := s.Pageserver.DeleteTimeline(ctx, p.TenantID, b.TimelineID); err != nil {
		slog.Error("delete timeline", "err", err)
		writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
		return
	}
	if err := s.Store.DeleteBranch(ctx, b.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": b.ID})
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, err := s.Store.GetProjectByID(ctx, r.PathValue("id"))
	if errors.Is(err, state.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	branches, err := s.Store.ListBranchesByProject(ctx, p.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, b := range branches {
		if ep, err := s.Store.GetEndpointByBranch(ctx, b.ID); err == nil {
			if err := s.Runtime.Stop(ctx, ep.ID); err != nil {
				slog.Warn("stop compute during project delete", "endpoint", ep.ID, "err", err)
			}
		}
	}
	if err := s.Pageserver.DeleteTenant(ctx, p.TenantID); err != nil {
		slog.Error("delete tenant", "err", err)
		writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
		return
	}
	if err := s.Store.DeleteProject(ctx, p.ID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": p.ID})
}

func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, err := s.Store.GetProjectByID(ctx, r.PathValue("id"))
	if errors.Is(err, state.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	branches, err := s.Store.ListBranchesByProject(ctx, p.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	var total uint64
	out := make([]map[string]any, 0, len(branches))
	for _, b := range branches {
		detail, err := s.Pageserver.GetTimeline(ctx, p.TenantID, b.TimelineID)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "pageserver: "+err.Error())
			return
		}
		total += detail.CurrentLogicalSize
		out = append(out, map[string]any{
			"branch_id":          b.ID,
			"name":               b.Name,
			"logical_size_bytes": detail.CurrentLogicalSize,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"branches":                 out,
		"total_logical_size_bytes": total,
	})
}

func (s *Server) debugStart(w http.ResponseWriter, r *http.Request) {
	addr, err := s.Waker.Wake(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"address": addr})
}

func (s *Server) debugStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	if err := s.Runtime.Stop(ctx, id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.Store.SetEndpointSuspended(ctx, id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"state": "suspended"})
}
