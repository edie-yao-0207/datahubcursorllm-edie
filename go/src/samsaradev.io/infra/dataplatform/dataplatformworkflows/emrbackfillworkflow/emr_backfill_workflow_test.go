package emrbackfillworkflow

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/emrreplication"
)

func TestGetPageSizeFromRegistry(t *testing.T) {
	testCases := []struct {
		description string
		entityName  string
		mockSpecs   []emrreplication.EmrReplicationSpec
		expected    int64
	}{
		{
			description: "should return page size when entity is found with page size override",
			entityName:  "TestEntity",
			mockSpecs: []emrreplication.EmrReplicationSpec{
				{
					Name: "TestEntity",
					InternalOverrides: &emrreplication.EmrReplicationInternalOverrides{
						PageSize: int64(250),
					},
				},
			},
			expected: int64(250),
		},
		{
			description: "should return 500 when entity is found without page size override",
			entityName:  "TestEntity",
			mockSpecs: []emrreplication.EmrReplicationSpec{
				{
					Name: "TestEntity",
				},
			},
			expected: int64(500),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Create mock function for this test case
			mockGetAllSpecs := func() []emrreplication.EmrReplicationSpec {
				return tc.mockSpecs
			}

			result := getPageSizeFromRegistry(tc.entityName, mockGetAllSpecs)
			assert.Equal(t, tc.expected, result)
		})
	}
}
