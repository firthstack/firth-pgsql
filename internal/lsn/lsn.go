// Package lsn parses and compares PostgreSQL/Neon log sequence numbers, which
// are formatted as two hex halves "X/Y" (high 32 bits / low 32 bits).
package lsn

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse converts an "X/Y" LSN string into a single uint64.
func Parse(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok || hi == "" || lo == "" {
		return 0, fmt.Errorf("invalid lsn %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn %q: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn %q: %w", s, err)
	}
	return (h << 32) | l, nil
}

// AtLeast reports whether a >= b.
func AtLeast(a, b string) (bool, error) {
	av, err := Parse(a)
	if err != nil {
		return false, err
	}
	bv, err := Parse(b)
	if err != nil {
		return false, err
	}
	return av >= bv, nil
}
