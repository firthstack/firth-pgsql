// Package neonclient wraps the HTTP APIs of Neon storage components.
package neonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PageserverClient struct {
	baseURL string
	hc      *http.Client
}

func NewPageserver(baseURL string) *PageserverClient {
	return &PageserverClient{baseURL: baseURL, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (c *PageserverClient) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody, nil
}

func (c *PageserverClient) expect2xx(ctx context.Context, method, path string, body any) error {
	code, respBody, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	if code < 200 || code > 299 {
		return fmt.Errorf("pageserver %s %s: %d: %s", method, path, code, respBody)
	}
	return nil
}

// AttachTenant creates/attaches a tenant via location_config. With
// control_plane_emergency_mode the pageserver accepts a self-supplied
// generation number.
func (c *PageserverClient) AttachTenant(ctx context.Context, tenantID string) error {
	return c.expect2xx(ctx, http.MethodPut, "/v1/tenant/"+tenantID+"/location_config", map[string]any{
		"mode":        "AttachedSingle",
		"generation":  1,
		"tenant_conf": map[string]any{},
	})
}

func (c *PageserverClient) CreateTimeline(ctx context.Context, tenantID, timelineID string, pgVersion int) error {
	return c.expect2xx(ctx, http.MethodPost, "/v1/tenant/"+tenantID+"/timeline/", map[string]any{
		"new_timeline_id": timelineID,
		"pg_version":      pgVersion,
	})
}

func (c *PageserverClient) CreateBranch(ctx context.Context, tenantID, newTimelineID, ancestorTimelineID string) error {
	return c.expect2xx(ctx, http.MethodPost, "/v1/tenant/"+tenantID+"/timeline/", map[string]any{
		"new_timeline_id":      newTimelineID,
		"ancestor_timeline_id": ancestorTimelineID,
	})
}

type TimelineDetail struct {
	TimelineID         string `json:"timeline_id"`
	LastRecordLSN      string `json:"last_record_lsn"`
	CurrentLogicalSize uint64 `json:"current_logical_size"`
}

func (c *PageserverClient) GetTimeline(ctx context.Context, tenantID, timelineID string) (*TimelineDetail, error) {
	code, body, err := c.do(ctx, http.MethodGet, "/v1/tenant/"+tenantID+"/timeline/"+timelineID, nil)
	if err != nil {
		return nil, err
	}
	if code == http.StatusNotFound {
		return nil, fmt.Errorf("timeline %s not found", timelineID)
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("pageserver GET timeline: %d: %s", code, body)
	}
	d := &TimelineDetail{}
	if err := json.Unmarshal(body, d); err != nil {
		return nil, err
	}
	return d, nil
}

// DeleteTimeline issues the async delete (202) then polls until the timeline
// is gone, for up to 60s.
func (c *PageserverClient) DeleteTimeline(ctx context.Context, tenantID, timelineID string) error {
	path := "/v1/tenant/" + tenantID + "/timeline/" + timelineID
	code, body, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	if code == http.StatusNotFound {
		return nil
	}
	if code != http.StatusAccepted && (code < 200 || code > 299) {
		return fmt.Errorf("pageserver DELETE timeline: %d: %s", code, body)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		code, _, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return err
		}
		if code == http.StatusNotFound {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeline %s still present after delete timeout", timelineID)
}

func (c *PageserverClient) DeleteTenant(ctx context.Context, tenantID string) error {
	return c.expect2xx(ctx, http.MethodDelete, "/v1/tenant/"+tenantID, nil)
}
