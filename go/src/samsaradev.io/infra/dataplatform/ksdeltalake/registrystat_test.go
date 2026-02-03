package ksdeltalake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStatsAll(t *testing.T) {
	// Create a new registry
	registry := NewRegistryStat()

	// Get the stats
	stats := registry.GetStatsAll()

	// Basic assertions
	assert.NotNil(t, stats)
	assert.IsType(t, []*Stat{}, stats)

	// Verify that stats are sorted correctly
	for i := 1; i < len(stats); i++ {
		// If kinds are different, verify they are in ascending order
		if stats[i-1].Kind != stats[i].Kind {
			assert.Less(t, stats[i-1].Kind, stats[i].Kind)
		} else {
			// If kinds are the same, verify StatTypes are in ascending order
			assert.LessOrEqual(t, stats[i-1].StatType, stats[i].StatType)
		}
	}
}

func TestGetStatsCheckForDuplicates(t *testing.T) {
	// Create a new registry
	registry := NewRegistryStat()

	// Create a map to track seen Kind-StatType combinations
	seen := make(map[string]bool)

	// Helper function to make a key for a stat
	makeKey := func(kind, statType interface{}) string {
		return fmt.Sprintf("%v-%v", kind, statType)
	}

	// Load seen map from rawStats()
	for _, rawStat := range rawStats() {
		seen[makeKey(rawStat.Kind, rawStat.StatType)] = true
	}

	// Check for duplicates
	stats := registry.GetStatsAll()
	for _, stat := range stats {
		key := makeKey(stat.Kind, stat.StatType)
		if seen[key] {
			t.Errorf("Duplicate stat found: Kind=%v, StatType=%v", stat.Kind, stat.StatType)
		}
		seen[key] = true
	}
}
