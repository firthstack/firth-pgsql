// Package scram builds PostgreSQL rolpassword-style SCRAM-SHA-256 verifiers.
// The same verifier string is consumed by three parties: stored in the state
// db, served to Neon proxy as role_secret, and placed in the compute spec as
// the role's encrypted_password.
package scram

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

const Iterations = 4096

// BuildVerifier returns "SCRAM-SHA-256$<iter>:<salt_b64>$<storedkey_b64>:<serverkey_b64>".
func BuildVerifier(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	return buildVerifier(password, salt, Iterations), nil
}

func buildVerifier(password string, salt []byte, iter int) string {
	salted := pbkdf2.Key([]byte(password), salt, iter, 32, sha256.New)
	clientKey := hmacSHA256(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	serverKey := hmacSHA256(salted, "Server Key")
	b64 := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iter, b64(salt), b64(storedKey[:]), b64(serverKey))
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// RandomPassword returns a 32-char password safe to embed in connection URIs.
func RandomPassword() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
