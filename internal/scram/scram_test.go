package scram_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/insforge/firth-pgsql/internal/scram"
)

var verifierRe = regexp.MustCompile(`^SCRAM-SHA-256\$4096:([A-Za-z0-9+/=]+)\$([A-Za-z0-9+/=]+):([A-Za-z0-9+/=]+)$`)

func TestVerifierFormat(t *testing.T) {
	v, err := scram.BuildVerifier("secret-pw")
	if err != nil {
		t.Fatalf("BuildVerifier: %v", err)
	}
	m := verifierRe.FindStringSubmatch(v)
	if m == nil {
		t.Fatalf("verifier format mismatch: %q", v)
	}
	salt, err := base64.StdEncoding.DecodeString(m[1])
	if err != nil || len(salt) != 16 {
		t.Errorf("salt: len=%d err=%v", len(salt), err)
	}
	stored, err := base64.StdEncoding.DecodeString(m[2])
	if err != nil || len(stored) != 32 {
		t.Errorf("storedKey: len=%d err=%v", len(stored), err)
	}
	server, err := base64.StdEncoding.DecodeString(m[3])
	if err != nil || len(server) != 32 {
		t.Errorf("serverKey: len=%d err=%v", len(server), err)
	}
}

func TestVerifierUnique(t *testing.T) {
	a, _ := scram.BuildVerifier("pw")
	b, _ := scram.BuildVerifier("pw")
	if a == b {
		t.Error("verifiers with random salts must differ")
	}
}

// TestVerifierAcceptedByPostgres is the real correctness proof: create a role
// whose rolpassword is our verifier, then authenticate with the plaintext
// password over SCRAM. Success means the math matches Postgres.
func TestVerifierAcceptedByPostgres(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)

	const password = "s3cret-pw-for-scram-test"
	v, err := scram.BuildVerifier(password)
	if err != nil {
		t.Fatalf("BuildVerifier: %v", err)
	}
	if _, err := admin.Exec(ctx, "DROP ROLE IF EXISTS scramtest"); err != nil {
		t.Fatalf("drop role: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE scramtest LOGIN PASSWORD '%s'", v)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS scramtest") })

	var stored string
	if err := admin.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='scramtest'").Scan(&stored); err != nil {
		t.Fatalf("read rolpassword: %v", err)
	}
	if stored != v {
		t.Fatalf("postgres rewrote the verifier:\n stored=%s\n ours=%s", stored, v)
	}

	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.User = "scramtest"
	cfg.Password = password
	userConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("scram auth with generated verifier failed: %v", err)
	}
	defer userConn.Close(ctx)
	var one int
	if err := userConn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("query as scramtest: %v", err)
	}
}

func TestRandomPassword(t *testing.T) {
	p1, err := scram.RandomPassword()
	if err != nil {
		t.Fatalf("RandomPassword: %v", err)
	}
	p2, _ := scram.RandomPassword()
	if p1 == p2 {
		t.Error("passwords must differ")
	}
	if len(p1) < 24 {
		t.Errorf("too short: %d", len(p1))
	}
	if strings.ContainsAny(p1, "+/=$:@") {
		t.Errorf("password contains url/connstring-unsafe chars: %q", p1)
	}
}
