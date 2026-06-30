// Package suspend scales idle computes to zero. A periodic sweep inspects
// every running endpoint, asks its compute_ctl for the last activity time and
// gracefully terminates computes idle beyond their per-endpoint threshold.
// It doubles as a reconciler: state that disagrees with the cluster
// (running but no pod) is repaired here.
package suspend

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/firthstack/firth-pgsql/internal/compute"
	"github.com/firthstack/firth-pgsql/internal/state"
)

type Suspender struct {
	Store        *state.Store
	Runtime      compute.Runtime
	StatusURL    func(addr string) string // addr is "ip:port(pg)"; default http://ip:3080/status
	TerminateURL func(addr string) string

	hc http.Client
}

func ipOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func (s *Suspender) statusURL(addr string) string {
	if s.StatusURL != nil {
		return s.StatusURL(addr)
	}
	return fmt.Sprintf("http://%s:3080/status", ipOf(addr))
}

func (s *Suspender) terminateURL(addr string) string {
	if s.TerminateURL != nil {
		return s.TerminateURL(addr)
	}
	return fmt.Sprintf("http://%s:3080/terminate", ipOf(addr))
}

// Sweep runs one pass over all running endpoints. Errors on individual
// endpoints are logged, not fatal: the next sweep retries.
func (s *Suspender) Sweep(ctx context.Context) error {
	eps, err := s.Store.ListEndpointsByState(ctx, "running")
	if err != nil {
		return err
	}
	for _, ep := range eps {
		if err := s.sweepOne(ctx, ep); err != nil {
			slog.Error("suspend sweep endpoint", "endpoint", ep.ID, "err", err)
		}
	}
	return nil
}

func (s *Suspender) sweepOne(ctx context.Context, ep state.Endpoint) error {
	st, err := s.Runtime.Status(ctx, ep.ID)
	if err != nil {
		return err
	}
	if !st.Exists {
		// Reconcile: the pod is gone but state says running.
		slog.Warn("reconciling running endpoint with missing pod", "endpoint", ep.ID)
		return s.Store.SetEndpointSuspended(ctx, ep.ID)
	}
	if ep.ComputeAddr == nil {
		return fmt.Errorf("running endpoint %s has no compute_addr", ep.ID)
	}

	lastActive, err := s.fetchLastActive(ctx, *ep.ComputeAddr)
	if err != nil {
		return fmt.Errorf("fetch last_active: %w", err)
	}
	if lastActive.IsZero() {
		// compute_ctl has not observed activity yet; fall back to the start
		// time so a never-used compute still suspends after the grace period.
		if ep.LastStartedAt == nil {
			return nil
		}
		lastActive = *ep.LastStartedAt
	}

	idleFor := time.Since(lastActive)
	threshold := time.Duration(ep.SuspendAfterSeconds) * time.Second
	if idleFor < threshold {
		return nil
	}

	ok, err := s.Store.TransitionEndpoint(ctx, ep.ID, "running", "suspending")
	if err != nil {
		return err
	}
	if !ok {
		return nil // someone else changed the state; not ours to handle
	}
	slog.Info("suspending idle endpoint", "endpoint", ep.ID, "idle", idleFor.Round(time.Second))

	// Graceful: flush the final LSN to safekeepers. Failure is non-fatal —
	// pod deletion below still gets us to a consistent state, and WAL since
	// the last flush is already quorum-committed on safekeepers anyway.
	if err := s.terminate(ctx, *ep.ComputeAddr); err != nil {
		slog.Warn("compute terminate failed, deleting pod anyway", "endpoint", ep.ID, "err", err)
	}
	if err := s.Runtime.Stop(ctx, ep.ID); err != nil {
		return err
	}
	return s.Store.SetEndpointSuspended(ctx, ep.ID)
}

func (s *Suspender) fetchLastActive(ctx context.Context, addr string) (time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.statusURL(addr), nil)
	if err != nil {
		return time.Time{}, err
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("compute_ctl /status: unexpected status %d", resp.StatusCode)
	}
	var body struct {
		Status     string  `json:"status"`
		LastActive *string `json:"last_active"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return time.Time{}, err
	}
	if body.LastActive == nil || *body.LastActive == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, *body.LastActive)
}

func (s *Suspender) terminate(ctx context.Context, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.terminateURL(addr)+"?mode=fast", nil)
	if err != nil {
		return err
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("terminate: status %d", resp.StatusCode)
	}
	return nil
}
