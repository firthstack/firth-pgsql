// Package api serves the InsForge-facing (northbound) REST API plus debug
// endpoints. The Neon-proxy-facing contract lives in internal/proxycontract.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/insforge/fly-pgsql/internal/compute"
	"github.com/insforge/fly-pgsql/internal/ids"
	"github.com/insforge/fly-pgsql/internal/neonclient"
	"github.com/insforge/fly-pgsql/internal/scram"
	"github.com/insforge/fly-pgsql/internal/state"
)

// Waker is implemented by internal/wake. The debug start endpoint and the
// proxy contract share it.
type Waker interface {
	Wake(ctx context.Context, endpointID string) (addr string, err error)
}

type Config struct {
	Domain               string // e.g. db.127-0-0-1.sslip.io
	ProxyPort            int    // client-facing port on the proxy (5432 via port-forward)
	PageserverConnstring string
	Safekeepers          []string
}

type Server struct {
	Store      *state.Store
	Pageserver *neonclient.PageserverClient
	Runtime    compute.Runtime
	Waker      Waker
	Cfg        Config
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/projects", s.createProject)
	mux.HandleFunc("GET /v1/projects/{id}", s.getProject)
	mux.HandleFunc("POST /v1/debug/endpoints/{id}/start", s.debugStart)
	mux.HandleFunc("POST /v1/debug/endpoints/{id}/stop", s.debugStop)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
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
	const roleName = "insforge"
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
