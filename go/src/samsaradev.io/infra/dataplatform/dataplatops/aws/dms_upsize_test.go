package aws

import "testing"

func TestNormalizeDmsInstanceClass(t *testing.T) {
	t.Run("lowercases and trims", func(t *testing.T) {
		got := normalizeDmsInstanceClass("  DMS.R5.2XLARGE  ")
		if got != "dms.r5.2xlarge" {
			t.Fatalf("expected %q, got %q", "dms.r5.2xlarge", got)
		}
	})

	t.Run("empty stays empty", func(t *testing.T) {
		got := normalizeDmsInstanceClass("   ")
		if got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}
	})

	t.Run("normalization makes equality casing-insensitive", func(t *testing.T) {
		current := normalizeDmsInstanceClass("dms.r5.2xlarge")
		target := normalizeDmsInstanceClass("DMS.R5.2XLARGE")
		if current != target {
			t.Fatalf("expected current == target after normalization; current=%q target=%q", current, target)
		}
	})
}
