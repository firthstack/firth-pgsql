package neonclient_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/insforge/fly-pgsql/internal/neonclient"
)

const (
	tid  = "aaaabbbbccccddddaaaabbbbccccdddd"
	tlid = "1111222233334444aaaabbbbccccdddd"
	anc  = "9999888877776666aaaabbbbccccdddd"
)

func TestAttachTenant(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/tenant/"+tid+"/location_config" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Write([]byte(`{"shards":[{"shard_id":"` + tid + `","node_id":1234}]}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.AttachTenant(context.Background(), tid); err != nil {
		t.Fatalf("AttachTenant: %v", err)
	}
	if gotBody["mode"] != "AttachedSingle" || gotBody["generation"] != float64(1) {
		t.Errorf("body mismatch: %v", gotBody)
	}
	if _, ok := gotBody["tenant_conf"]; !ok {
		t.Error("missing tenant_conf")
	}
}

func TestCreateRootTimeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tenant/"+tid+"/timeline/" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if body["new_timeline_id"] != tlid || body["pg_version"] != float64(17) {
			t.Errorf("body mismatch: %v", body)
		}
		if _, hasAncestor := body["ancestor_timeline_id"]; hasAncestor {
			t.Error("root timeline must not send ancestor_timeline_id")
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"timeline_id":"` + tlid + `","last_record_lsn":"0/169AD58"}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.CreateTimeline(context.Background(), tid, tlid, 17); err != nil {
		t.Fatalf("CreateTimeline: %v", err)
	}
}

func TestCreateBranchTimeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if body["new_timeline_id"] != tlid || body["ancestor_timeline_id"] != anc {
			t.Errorf("body mismatch: %v", body)
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"timeline_id":"` + tlid + `"}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.CreateBranch(context.Background(), tid, tlid, anc); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
}

func TestCreateBranchAtLSN(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if body["ancestor_timeline_id"] != anc || body["ancestor_start_lsn"] != "0/214F810" {
			t.Errorf("body mismatch: %v", body)
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"timeline_id":"` + tlid + `"}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.CreateBranchAtLSN(context.Background(), tid, tlid, anc, "0/214F810"); err != nil {
		t.Fatalf("CreateBranchAtLSN: %v", err)
	}
}

func TestGetTimelineDetail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tenant/"+tid+"/timeline/"+tlid {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Write([]byte(`{"timeline_id":"` + tlid + `","last_record_lsn":"0/2000000","current_logical_size":12345}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	d, err := c.GetTimeline(context.Background(), tid, tlid)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if d.CurrentLogicalSize != 12345 || d.LastRecordLSN != "0/2000000" {
		t.Errorf("detail mismatch: %+v", d)
	}
}

func TestDeleteTimeline(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			// first poll: still deleting; second: gone
			if calls.Add(1) == 1 {
				w.Write([]byte(`{"timeline_id":"` + tlid + `","state":"Stopping"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.DeleteTimeline(context.Background(), tid, tlid); err != nil {
		t.Fatalf("DeleteTimeline: %v", err)
	}
	if calls.Load() < 2 {
		t.Errorf("expected polling after 202, got %d polls", calls.Load())
	}
}

func TestDeleteTenant(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/tenant/"+tid {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	if err := c.DeleteTenant(context.Background(), tid); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
}

func TestErrorIncludesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"msg":"already exists"}`))
	}))
	defer srv.Close()

	c := neonclient.NewPageserver(srv.URL)
	err := c.CreateTimeline(context.Background(), tid, tlid, 17)
	if err == nil {
		t.Fatal("expected error")
	}
	if want := "already exists"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q must contain %q", err, want)
	}
}
