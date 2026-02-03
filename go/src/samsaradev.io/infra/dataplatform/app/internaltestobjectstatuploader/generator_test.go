package main

import (
	"context"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/testinghelpers"
	"samsaradev.io/stats/kinesisstats/kinesisstatsproto"
)

func TestGenerateObjectStats(t *testing.T) {
	stat := func(timeMs int64) *kinesisstatsproto.WriteChunkRequest {
		series, err := objectStatChunkSeries()
		require.NoError(t, err)
		bytes, err := objectStatChunkValue(timeMs)
		return &kinesisstatsproto.WriteChunkRequest{
			Chunk: &kinesisstatsproto.KVChunk{
				Series: series,
				Values: []*kinesisstatsproto.KVValue{
					{
						Time:  timeMs,
						Value: bytes,
					},
				},
			},
		}
	}
	testCases := []struct {
		description   string
		currentTimeMs int64
		numOfStats    int
		expected      []*kinesisstatsproto.WriteChunkRequest
	}{
		{
			description:   "should generate 10 stats",
			currentTimeMs: testinghelpers.Mar10_12AM_2022PST,
			numOfStats:    10,
			expected: []*kinesisstatsproto.WriteChunkRequest{
				stat(testinghelpers.Mar10_12AM_2022PST),
				stat(testinghelpers.Mar10_12AM_2022PST + 1),
				stat(testinghelpers.Mar10_12AM_2022PST + 2),
				stat(testinghelpers.Mar10_12AM_2022PST + 3),
				stat(testinghelpers.Mar10_12AM_2022PST + 4),
				stat(testinghelpers.Mar10_12AM_2022PST + 5),
				stat(testinghelpers.Mar10_12AM_2022PST + 6),
				stat(testinghelpers.Mar10_12AM_2022PST + 7),
				stat(testinghelpers.Mar10_12AM_2022PST + 8),
				stat(testinghelpers.Mar10_12AM_2022PST + 9),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := generateObjectStats(context.Background(), tc.currentTimeMs, tc.numOfStats)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
