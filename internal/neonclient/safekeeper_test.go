package neonclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/insforge/firth-pgsql/internal/neonclient"
)

func skServer(t *testing.T, commitLSN string, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Write([]byte(`{"commit_lsn":"` + commitLSN + `","flush_lsn":"` + commitLSN + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestMaxCommitLSNTakesHighest(t *testing.T) {
	a := skServer(t, "0/100", 0)
	b := skServer(t, "0/214F810", 0)
	c := skServer(t, "0/2000", 0)

	sk := neonclient.NewSafekeeper([]string{a, b, c})
	got, err := sk.MaxCommitLSN(context.Background(), tid, tlid)
	if err != nil {
		t.Fatalf("MaxCommitLSN: %v", err)
	}
	if got != "0/214F810" {
		t.Errorf("want highest 0/214F810, got %s", got)
	}
}

func TestMaxCommitLSNToleratesPartialFailure(t *testing.T) {
	down := skServer(t, "", http.StatusInternalServerError)
	up := skServer(t, "0/500", 0)

	sk := neonclient.NewSafekeeper([]string{down, up})
	got, err := sk.MaxCommitLSN(context.Background(), tid, tlid)
	if err != nil || got != "0/500" {
		t.Fatalf("expected 0/500 despite one down: got %s err %v", got, err)
	}
}

func TestMaxCommitLSNAllDown(t *testing.T) {
	down := skServer(t, "", http.StatusInternalServerError)
	sk := neonclient.NewSafekeeper([]string{down})
	if _, err := sk.MaxCommitLSN(context.Background(), tid, tlid); err == nil {
		t.Error("expected error when all safekeepers are down")
	}
}
