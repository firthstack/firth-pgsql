// Package ids generates identifiers. Endpoint/branch/project ids only use
// [a-z0-9-] because Neon proxy rejects other characters in endpoint names.
package ids

import (
	"crypto/rand"
	"encoding/hex"
)

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}

// NewHex32 returns a 32-hex-char id (Neon tenant/timeline format).
func NewHex32() string { return randHex(16) }

func NewProjectID() string  { return "prj" + randHex(6) }
func NewBranchID() string   { return "br-" + randHex(6) }
func NewEndpointID() string { return "ep-" + randHex(6) }
