package dataplatformconsts

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestSloDescription(t *testing.T) {
	var tests = []struct {
		name     string
		slo      JobSlo
		expected string
	}{
		{
			name:     "empty",
			slo:      JobSlo{},
			expected: "",
		},
		{
			name: "low urgency",
			slo: JobSlo{
				Urgency:        JobSloLow,
				SloTargetHours: 10,
			},
			expected: "Data Freshness: This table will be at least 10 hours fresh, with breaches posted to #data-platform-status and no guarantee on response.",
		},
		{
			name: "business hours",
			slo: JobSlo{
				Urgency:        JobSloBusinessHoursHigh,
				SloTargetHours: 10,
			},
			expected: "Data Freshness: This table will be at least 10 hours fresh, with breaches posted to #data-platform-status and responded to from 8am-4pm PT on weekdays.",
		},
		{
			name: "high urgency",
			slo: JobSlo{
				Urgency:        JobSloHigh,
				SloTargetHours: 10,
			},
			expected: "Data Freshness: This table will be at least 10 hours fresh, with breaches posted to #data-platform-status and responded to 24/7.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			description := tc.slo.SloDescription()
			assert.Equal(t, tc.expected, description)
		})
	}
}
