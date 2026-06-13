package lsn_test

import (
	"testing"

	"github.com/insforge/fly-pgsql/internal/lsn"
)

func TestParse(t *testing.T) {
	cases := map[string]uint64{
		"0/0":         0,
		"0/1":         1,
		"0/214F810":   0x214F810,
		"1/0":         1 << 32,
		"FF/FFFFFFFF": (0xFF << 32) | 0xFFFFFFFF,
	}
	for s, want := range cases {
		got, err := lsn.Parse(s)
		if err != nil || got != want {
			t.Errorf("Parse(%q) = %d, %v; want %d", s, got, err, want)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	for _, s := range []string{"", "abc", "0/", "/0", "0/0/0", "zz/1"} {
		if _, err := lsn.Parse(s); err == nil {
			t.Errorf("Parse(%q) should error", s)
		}
	}
}

func TestAtLeast(t *testing.T) {
	if ok, _ := lsn.AtLeast("0/214F810", "0/100"); !ok {
		t.Error("0/214F810 should be >= 0/100")
	}
	if ok, _ := lsn.AtLeast("0/100", "0/214F810"); ok {
		t.Error("0/100 should not be >= 0/214F810")
	}
	if ok, _ := lsn.AtLeast("1/0", "0/FFFFFFFF"); !ok {
		t.Error("1/0 should be >= 0/FFFFFFFF")
	}
	if ok, _ := lsn.AtLeast("0/5", "0/5"); !ok {
		t.Error("equal LSNs should satisfy AtLeast")
	}
}
