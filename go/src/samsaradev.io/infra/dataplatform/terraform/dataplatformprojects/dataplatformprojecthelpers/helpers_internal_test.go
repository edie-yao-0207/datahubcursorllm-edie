package dataplatformprojecthelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkParamsSlice(t *testing.T) {
	testCases := []struct {
		description      string
		allParameters    []string
		maxBytesPerSlice int
		shouldError      bool
		expected         [][]string
	}{
		{
			description:      "when the max bytes is greater than bytes in all params",
			allParameters:    []string{"foo", "bar"},
			maxBytesPerSlice: 1000,
			expected: [][]string{
				{"foo", "bar"},
			},
		},
		{
			description:      "when the max bytes is less than some of the strings in all params",
			allParameters:    []string{"foobar", "foo", "hi"},
			maxBytesPerSlice: 7,
			expected: [][]string{
				{"foobar"},
				{"foo", "hi"},
			},
		},
		{
			description:      "when the max bytes is exactly equal to all params",
			allParameters:    []string{"foo", "bar"},
			maxBytesPerSlice: 6,
			// These are split into 2 chunks b/c we need the extra 1 byte for the space character b/w params
			expected: [][]string{
				{"foo"},
				{"bar"},
			},
		},
		{
			description:      "when the max bytes is 1 more than the length of all params",
			allParameters:    []string{"foo", "bar"},
			maxBytesPerSlice: 7,
			expected: [][]string{
				{"foo", "bar"},
			},
		},
		{
			description:      "when the max bytes is exactly equal to the sum of the last set of params (+1 b/c of space)",
			allParameters:    []string{"foo", "hi", "aa"},
			maxBytesPerSlice: 5,
			expected: [][]string{
				{"foo"},
				{"hi", "aa"},
			},
		},
		{
			description:      "when there is a string at the beginning that is longer than max bytes per slice",
			allParameters:    []string{"foobar", "foo", "hi"},
			maxBytesPerSlice: 3,
			shouldError:      true,
		},
		{
			description:      "when there is a string in the middle that is longer than max bytes per slice",
			allParameters:    []string{"hi", "foobar", "hi"},
			maxBytesPerSlice: 3,
			shouldError:      true,
		},
		{
			description:      "when there is a string at the end that is longer than max bytes per slice",
			allParameters:    []string{"hi", "aa", "foobar"},
			maxBytesPerSlice: 3,
			shouldError:      true,
		},
		{
			description:      "when the input is empty",
			allParameters:    []string{},
			maxBytesPerSlice: 100,
			expected:         [][]string{{}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result, err := chunkParamsSlice(tc.allParameters, tc.maxBytesPerSlice)
			if tc.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestSplitParamsBySpace(t *testing.T) {
	testCases := []struct {
		description string
		parameters  []string
		expected    []string
	}{
		{
			description: "splits the params appropriately",
			parameters:  []string{"--path foo", "--path bar"},
			expected: []string{
				"--path", "foo", "--path", "bar",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := splitParamsBySpace(tc.parameters)
			assert.Equal(t, tc.expected, result)
		})
	}
}
