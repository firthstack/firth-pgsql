// Package proxycontract implements the control-plane HTTP API that Neon's
// open-source proxy calls (auth backend "cplane-v1"): role secret lookup,
// compute wake-up, and per-endpoint JWKS. Legacy path aliases are registered
// too so the pinned proxy release works regardless of which names it uses.
package proxycontract

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/insforge/firth-pgsql/internal/state"
)

type Waker interface {
	Wake(ctx context.Context, endpointID string) (addr string, err error)
}

type Handlers struct {
	Store *state.Store
	Waker Waker
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /proxy/api/get_endpoint_access_control", h.accessControl)
	mux.HandleFunc("GET /proxy/api/proxy_get_role_secret", h.accessControl)
	mux.HandleFunc("GET /proxy/api/wake_compute", h.wakeCompute)
	mux.HandleFunc("GET /proxy/api/proxy_wake_compute", h.wakeCompute)
	mux.HandleFunc("GET /proxy/api/endpoints/{endpoint}/jwks", h.jwks)
}

// controlPlaneError mirrors proxy/src/control_plane/messages.rs
// ControlPlaneErrorMessage.
type controlPlaneError struct {
	Error  string `json:"error"`
	Status struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details struct {
			ErrorInfo *struct {
				Reason string `json:"reason"`
			} `json:"error_info,omitempty"`
		} `json:"details"`
	} `json:"status"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeCPError(w http.ResponseWriter, httpCode int, msg, code, reason string) {
	e := controlPlaneError{Error: msg}
	e.Status.Code = code
	e.Status.Message = msg
	if reason != "" {
		e.Status.Details.ErrorInfo = &struct {
			Reason string `json:"reason"`
		}{Reason: reason}
	}
	writeJSON(w, httpCode, e)
}

func (h *Handlers) accessControl(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpointish")
	role := r.URL.Query().Get("role")

	ac, err := h.Store.GetAccessControl(r.Context(), endpoint)
	if errors.Is(err, state.ErrNotFound) {
		writeCPError(w, http.StatusNotFound, "endpoint not found", "NOT_FOUND", "ENDPOINT_NOT_FOUND")
		return
	}
	if err != nil {
		slog.Error("access control lookup", "endpoint", endpoint, "err", err)
		writeCPError(w, http.StatusInternalServerError, "internal error", "INTERNAL", "")
		return
	}

	secret := ""
	if role == ac.RoleName {
		secret = ac.RoleVerifier
	}
	// Empty role_secret => proxy treats the role as nonexistent and fails
	// authentication in constant time, without leaking role existence.
	writeJSON(w, http.StatusOK, map[string]any{
		"role_secret": secret,
		"project_id":  ac.ProjectID,
		"allowed_ips": nil,
	})
}

func (h *Handlers) wakeCompute(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpointish")

	ac, err := h.Store.GetAccessControl(r.Context(), endpoint)
	if errors.Is(err, state.ErrNotFound) {
		writeCPError(w, http.StatusNotFound, "endpoint not found", "NOT_FOUND", "ENDPOINT_NOT_FOUND")
		return
	}
	if err != nil {
		slog.Error("wake lookup", "endpoint", endpoint, "err", err)
		writeCPError(w, http.StatusInternalServerError, "internal error", "INTERNAL", "")
		return
	}

	addr, err := h.Waker.Wake(r.Context(), endpoint)
	if err != nil {
		slog.Error("wake compute", "endpoint", endpoint, "err", err)
		writeCPError(w, http.StatusInternalServerError, "failed to wake compute: "+err.Error(), "INTERNAL", "")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"address": addr,
		"aux": map[string]string{
			"endpoint_id":     endpoint,
			"project_id":      ac.ProjectID,
			"branch_id":       ac.BranchID,
			"compute_id":      "compute-" + endpoint,
			"cold_start_info": "unknown",
		},
	})
}

func (h *Handlers) jwks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"jwks": []any{}})
}
