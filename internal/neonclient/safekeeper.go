package neonclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/insforge/firth-pgsql/internal/lsn"
)

// SafekeeperClient queries the safekeeper HTTP API (port 7676). The control
// plane uses it to learn a timeline's quorum-committed LSN, which bounds all
// durably-committed WAL.
type SafekeeperClient struct {
	baseURLs []string // one per safekeeper; commit_lsn is taken as the max
	hc       *http.Client
}

func NewSafekeeper(baseURLs []string) *SafekeeperClient {
	return &SafekeeperClient{baseURLs: baseURLs, hc: &http.Client{Timeout: 10 * time.Second}}
}

// MaxCommitLSN returns the highest commit_lsn reported across the safekeepers.
// Individual safekeeper failures are tolerated as long as one responds — the
// commit LSN is a quorum-agreed value, so any healthy member is authoritative.
func (c *SafekeeperClient) MaxCommitLSN(ctx context.Context, tenantID, timelineID string) (string, error) {
	var best string
	var bestVal uint64
	var lastErr error
	seen := false
	for _, base := range c.baseURLs {
		cl, err := c.commitLSN(ctx, base, tenantID, timelineID)
		if err != nil {
			lastErr = err
			continue
		}
		v, err := lsn.Parse(cl)
		if err != nil {
			lastErr = err
			continue
		}
		if !seen || v > bestVal {
			best, bestVal, seen = cl, v, true
		}
	}
	if !seen {
		return "", fmt.Errorf("no safekeeper returned commit_lsn: %v", lastErr)
	}
	return best, nil
}

func (c *SafekeeperClient) commitLSN(ctx context.Context, base, tenantID, timelineID string) (string, error) {
	url := base + "/v1/tenant/" + tenantID + "/timeline/" + timelineID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("safekeeper %s: status %d", base, resp.StatusCode)
	}
	var body struct {
		CommitLSN string `json:"commit_lsn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.CommitLSN == "" {
		return "", fmt.Errorf("safekeeper %s: empty commit_lsn", base)
	}
	return body.CommitLSN, nil
}
