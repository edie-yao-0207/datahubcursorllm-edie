package kinesisstatsdeltalakedifftool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/ksdeltalake/difftool"
)

func TestBuildParams(t *testing.T) {
	testCases := []struct {
		description       string
		startDate         string
		endDate           string
		prefix            string
		stats             string
		orgIds            string
		randomSampleCount int
		shouldError       bool
		expected          *difftool.DiffToolParams
	}{
		{
			description: "no arguments",
			expected:    &defaultParams,
		},
		{
			description: "partial arguments (just a date)",
			startDate:   "2021-01-01",
			shouldError: true,
		},
		{
			description: "partial arguments (most of them)",
			startDate:   "2021-01-01",
			endDate:     "2021-01-01",
			prefix:      "prefix",
			shouldError: true,
		},
		{
			description: "validate that one of orgid and randomSampleCount is passed",
			startDate:   "2021-01-01",
			endDate:     "2021-01-01",
			prefix:      "prefix",
			stats:       "osDDerivedGpsDistance,osDCableVoltage",
			shouldError: true,
		},
		{
			description: "validate that one of orgid and randomSampleCount is passed",
			startDate:   "2021-01-01",
			endDate:     "2021-01-01",
			prefix:      "prefix",
			stats:       "osDDerivedGpsDistance,osDCableVoltage",
			shouldError: true,
		},
		{
			description:       "valid arguments should get parsed correctly",
			startDate:         "2021-01-01",
			endDate:           "2021-01-04",
			prefix:            "testprefix",
			stats:             "osDDerivedGpsDistance,osDCableVoltage",
			randomSampleCount: 100,
			expected: &difftool.DiffToolParams{
				DeviceRandomSampleCount: 100,
				StartDate:               time.Date(2021, 01, 01, 0, 0, 0, 0, time.UTC),
				EndDate:                 time.Date(2021, 01, 04, 0, 0, 0, 0, time.UTC),
				Stats:                   []objectstatproto.ObjectStatEnum{objectstatproto.ObjectStatEnum_osDDerivedGpsDistance, objectstatproto.ObjectStatEnum_osDCableVoltage},
				ExportPrefix:            "testprefix",
				Local:                   false,
			},
		},
		{
			description: "invalid stats should fail to parse",
			startDate:   "2021-01-01",
			endDate:     "2021-01-04",
			prefix:      "testprefix",
			stats:       "osdnotastat,osdsmalldata",
			shouldError: true,
		},
		{
			description: "invalid orgids should fail to parse",
			startDate:   "2021-01-01",
			endDate:     "2021-01-04",
			prefix:      "testprefix",
			stats:       "osDDerivedGpsDistance,osDCableVoltage",
			orgIds:      "fakeorg1,fakeorg2,3,4,5",
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			params, err := buildParams(tc.startDate, tc.endDate, tc.prefix, tc.stats, tc.orgIds, tc.randomSampleCount)
			if tc.shouldError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, params)
			}
		})
	}
}

// This test just makes sure that building the default params succeeds, and that a couple fields make sense.
func TestDefaultParams(t *testing.T) {
	params := buildDefaultParams()
	assert.True(t, len(params.Stats) > 0)
	assert.Equal(t, "dataplatcron", params.ExportPrefix)
}
