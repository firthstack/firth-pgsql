// Package wake owns the endpoint lifecycle's hot path: turning a suspended
// endpoint into a running compute exactly once, no matter how many
// connections race to trigger it.
package wake

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/insforge/fly-pgsql/internal/compute"
	"github.com/insforge/fly-pgsql/internal/state"
)

// staleAfter is how long a starting/suspending state may go without updates
// before another waker assumes its owner died and takes over.
const staleAfter = 2 * time.Minute

type Waker struct {
	Store        *state.Store
	Runtime      compute.Runtime
	SpecBuilder  func(ctx context.Context, endpointID string) (compute.ComputeConfig, error)
	ReadyTimeout time.Duration // total budget for pod IP + compute_ctl running
	PollInterval time.Duration
	StatusURL    func(podIP string) string // override in tests; default http://<ip>:3080/status
}

func (w *Waker) pollInterval() time.Duration {
	if w.PollInterval > 0 {
		return w.PollInterval
	}
	return 500 * time.Millisecond
}

func (w *Waker) readyTimeout() time.Duration {
	if w.ReadyTimeout > 0 {
		return w.ReadyTimeout
	}
	return 120 * time.Second
}

func (w *Waker) statusURL(podIP string) string {
	if w.StatusURL != nil {
		return w.StatusURL(podIP)
	}
	return fmt.Sprintf("http://%s:3080/status", podIP)
}

// Wake returns the compute address for the endpoint, starting a compute if
// needed. Concurrent callers converge on a single start: one becomes the
// owner via a CAS to `starting`, the rest wait and re-read.
func (w *Waker) Wake(ctx context.Context, endpointID string) (string, error) {
	deadline := time.Now().Add(w.readyTimeout() + 30*time.Second)
	for time.Now().Before(deadline) {
		var (
			action string
			addr   string
		)
		err := w.Store.WithEndpointLock(ctx, endpointID, func(ep *state.Endpoint, _ state.Tx) error {
			switch ep.State {
			case "running":
				if ep.ComputeAddr != nil {
					addr = *ep.ComputeAddr
				}
				action = "verify-running"
			case "starting", "suspending":
				if time.Since(ep.UpdatedAt) > staleAfter {
					action = "takeover"
				} else {
					action = "wait"
				}
			case "suspended", "failed":
				action = "start"
			default:
				return fmt.Errorf("endpoint %s in unknown state %q", endpointID, ep.State)
			}
			return nil
		})
		if err != nil {
			return "", err
		}

		switch action {
		case "verify-running":
			st, err := w.Runtime.Status(ctx, endpointID)
			if err != nil {
				return "", err
			}
			if st.Exists && addr != "" {
				return addr, nil
			}
			// State says running but the pod is gone: reconcile and retry.
			if _, err := w.Store.TransitionEndpoint(ctx, endpointID, "running", "suspended"); err != nil {
				return "", err
			}
		case "start":
			// CAS from both possible source states; only the winner starts.
			for _, from := range []string{"suspended", "failed"} {
				ok, err := w.Store.TransitionEndpoint(ctx, endpointID, from, "starting")
				if err != nil {
					return "", err
				}
				if ok {
					return w.startCompute(ctx, endpointID)
				}
			}
			// Lost the race; loop and observe the new state.
		case "takeover":
			for _, from := range []string{"starting", "suspending"} {
				ok, err := w.Store.TransitionEndpoint(ctx, endpointID, from, "starting")
				if err != nil {
					return "", err
				}
				if ok {
					slog.Warn("taking over stale endpoint start", "endpoint", endpointID, "from", from)
					_ = w.Runtime.Stop(ctx, endpointID) // clear any half-created pod
					return w.startCompute(ctx, endpointID)
				}
			}
		case "wait":
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(w.pollInterval()):
			}
		}
	}
	return "", fmt.Errorf("wake %s: timed out", endpointID)
}

// startCompute runs with ownership of the `starting` state.
func (w *Waker) startCompute(ctx context.Context, endpointID string) (string, error) {
	fail := func(err error) (string, error) {
		_ = w.Runtime.Stop(ctx, endpointID)
		if _, terr := w.Store.TransitionEndpoint(ctx, endpointID, "starting", "failed"); terr != nil {
			slog.Error("mark failed", "endpoint", endpointID, "err", terr)
		}
		return "", err
	}

	cfg, err := w.SpecBuilder(ctx, endpointID)
	if err != nil {
		return fail(fmt.Errorf("build spec: %w", err))
	}
	if err := w.Runtime.Start(ctx, endpointID, cfg); err != nil {
		return fail(fmt.Errorf("start compute: %w", err))
	}

	podIP, err := w.waitReady(ctx, endpointID)
	if err != nil {
		return fail(err)
	}
	addr := fmt.Sprintf("%s:%d", podIP, compute.ComputePort)
	if err := w.Store.SetEndpointRunning(ctx, endpointID, addr); err != nil {
		return fail(err)
	}
	return addr, nil
}

func (w *Waker) waitReady(ctx context.Context, endpointID string) (string, error) {
	deadline := time.Now().Add(w.readyTimeout())
	hc := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		st, err := w.Runtime.Status(ctx, endpointID)
		if err == nil && st.Exists && st.PodIP != "" {
			resp, err := hc.Get(w.statusURL(st.PodIP))
			if err == nil {
				var body struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&body)
				resp.Body.Close()
				switch body.Status {
				case "running":
					return st.PodIP, nil
				case "failed":
					return "", fmt.Errorf("compute_ctl failed: %s", body.Error)
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(w.pollInterval()):
		}
	}
	return "", fmt.Errorf("compute for %s not ready within %s", endpointID, w.readyTimeout())
}
