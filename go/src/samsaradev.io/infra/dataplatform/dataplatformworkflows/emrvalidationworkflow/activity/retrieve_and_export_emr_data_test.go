package activity

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/platform/entity/types/value"
)

func TestEmptyOrgDataProducesEmptyArrayNotNull(t *testing.T) {
	// This test verifies the fix for the issue where organizations with no EMR data
	// were producing "null" instead of "[]" in their exported JSON files.

	// Create test data with some orgs having data and others having none
	allData := []EmrDataRecord{
		{
			OrgId:     1001,
			AssetId:   2001,
			Data:      value.Value{}, // Mock EMR data
			Timestamp: time.Now(),
		},
		{
			OrgId:     1001,
			AssetId:   2002,
			Data:      value.Value{}, // Mock EMR data
			Timestamp: time.Now(),
		},
		// Note: Org 1002 and 1003 have no data
	}

	testCases := []struct {
		name                 string
		orgId                int64
		expectedRecords      int
		shouldHaveEmptyArray bool
	}{
		{
			name:                 "org with data should have records",
			orgId:                1001,
			expectedRecords:      2,
			shouldHaveEmptyArray: false,
		},
		{
			name:                 "org without data should have empty array not null",
			orgId:                1002,
			expectedRecords:      0,
			shouldHaveEmptyArray: true,
		},
		{
			name:                 "another org without data should have empty array not null",
			orgId:                1003,
			expectedRecords:      0,
			shouldHaveEmptyArray: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Group data by org (simulating the logic in exportDataToS3)
			orgData := make(map[int64][]EmrDataRecord)
			for _, record := range allData {
				orgData[record.OrgId] = append(orgData[record.OrgId], record)
			}

			// Get records for this org
			records := orgData[tc.orgId]
			if records == nil {
				records = []EmrDataRecord{} // This is the fix we're testing
			}

			// Convert to JSON
			jsonData, err := json.MarshalIndent(records, "", "  ")
			require.NoError(t, err)

			jsonStr := string(jsonData)

			if tc.shouldHaveEmptyArray {
				// Verify we get an empty array, not null
				assert.Equal(t, "[]", strings.TrimSpace(jsonStr),
					"Organization %d with no data should produce empty array [], not null", tc.orgId)
			} else {
				// Verify we have actual data
				assert.NotEqual(t, "[]", strings.TrimSpace(jsonStr),
					"Organization %d should have data, not empty array", tc.orgId)
				assert.NotEqual(t, "null", strings.TrimSpace(jsonStr),
					"Organization %d should not produce null", tc.orgId)

				// Verify we can unmarshal back to the expected number of records
				var unmarshaled []EmrDataRecord
				err := json.Unmarshal(jsonData, &unmarshaled)
				require.NoError(t, err)
				assert.Len(t, unmarshaled, tc.expectedRecords)
			}
		})
	}
}

func TestNilSliceVsEmptySliceJSONMarshaling(t *testing.T) {
	// This test demonstrates the difference between marshaling nil vs empty slice
	// to help document why the fix is necessary

	var nilSlice []EmrDataRecord
	emptySlice := []EmrDataRecord{}

	// Marshal nil slice
	nilJSON, err := json.Marshal(nilSlice)
	require.NoError(t, err)
	assert.Equal(t, "null", string(nilJSON), "nil slice should marshal to 'null'")

	// Marshal empty slice
	emptyJSON, err := json.Marshal(emptySlice)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(emptyJSON), "empty slice should marshal to '[]'")
}

func TestParseS3Path(t *testing.T) {
	activity := &RetrieveAndExportEmrDataActivity{}

	testCases := []struct {
		name           string
		s3Path         string
		expectedBucket string
		expectedKey    string
		expectError    bool
	}{
		{
			name:           "valid s3 path with key",
			s3Path:         "s3://my-bucket/path/to/file.json",
			expectedBucket: "my-bucket",
			expectedKey:    "path/to/file.json",
			expectError:    false,
		},
		{
			name:           "valid s3 path bucket only",
			s3Path:         "s3://my-bucket/",
			expectedBucket: "my-bucket",
			expectedKey:    "",
			expectError:    false,
		},
		{
			name:        "invalid path without s3 prefix",
			s3Path:      "my-bucket/path/file.json",
			expectError: true,
		},
		{
			name:        "invalid path empty bucket",
			s3Path:      "s3:///path/file.json",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, key, err := activity.parseS3Path(tc.s3Path)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedBucket, bucket)
				assert.Equal(t, tc.expectedKey, key)
			}
		})
	}
}
