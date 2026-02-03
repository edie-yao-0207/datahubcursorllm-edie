package activity

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
)

func TestExtractFilterOptionsFromYamlActivity_Execute(t *testing.T) {
	tests := []struct {
		name          string
		yamlContent   string
		filters       helpers.FilterFieldToValueMap
		expectedOpts  []helpers.FilterOptions
		expectedError bool
	}{
		{
			name: "valid yaml with matching comparators",
			yamlContent: `
apiEndpoints:
  - type: ListRecords
    listRecordsSpec:
      filterOptions:
        - field: tripStartTime
          comparator: GreaterThanOrEqual
        - field: tripStartTime
          comparator: LessThan
        - field: assetId
          comparator: In
`,
			filters: nil,
			expectedOpts: []helpers.FilterOptions{
				{
					Field:      "tripStartTime",
					Comparator: "GreaterThanOrEqual",
				},
				{
					Field:      "tripStartTime",
					Comparator: "LessThan",
				},
			},
			expectedError: false,
		},
		{
			name: "valid yaml with range filters on same field",
			yamlContent: `
apiEndpoints:
  - type: ListRecords
    listRecordsSpec:
      filterOptions:
        - field: tripStartTime
          comparator: GreaterThanOrEqual
        - field: tripStartTime
          comparator: LessThan
`,
			filters: helpers.FilterFieldToValueMap{
				"tripStartTimeGreaterThanOrEqual": 1719817200000,
				"tripStartTimeLessThan":           1722495600000,
			},
			expectedOpts: []helpers.FilterOptions{
				{
					Field:      "tripStartTime",
					Comparator: "GreaterThanOrEqual",
				},
				{
					Field:      "tripStartTime",
					Comparator: "LessThan",
				},
			},
			expectedError: false,
		},
		{
			name: "invalid filter field name",
			yamlContent: `
apiEndpoints:
  - type: ListRecords
    listRecordsSpec:
      filterOptions:
        - field: tripStartTime
          comparator: GreaterThanOrEqual
`,
			filters: helpers.FilterFieldToValueMap{
				"invalidFieldName": 1719817200000,
			},
			expectedOpts:  []helpers.FilterOptions{},
			expectedError: true,
		},
		{
			name: "valid yaml with no matching comparators",
			yamlContent: `
apiEndpoints:
  - type: ListRecords
    listRecordsSpec:
      filterOptions:
        - field: assetId
          comparator: In
        - field: tripId
          comparator: In
`,
			filters:       nil,
			expectedOpts:  []helpers.FilterOptions{},
			expectedError: false,
		},
		{
			name: "yaml with no ListRecords endpoint",
			yamlContent: `
apiEndpoints:
  - type: OtherEndpoint
    listRecordsSpec:
      filterOptions:
        - field: tripStartTime
          comparator: GreaterThanOrEqual
`,
			filters:       nil,
			expectedOpts:  nil,
			expectedError: true,
		},
		{
			name: "invalid yaml",
			yamlContent: `
invalid: yaml: content
`,
			filters:       nil,
			expectedOpts:  nil,
			expectedError: true,
		},
	}

	activity := &ExtractFilterOptionsFromYamlActivity{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file relative to backend root
			tempDir := filepath.Join(filepathhelpers.BackendRoot, "tmp")
			err := os.MkdirAll(tempDir, 0755)
			require.NoError(t, err)

			tempFile, err := os.CreateTemp(tempDir, "test-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tempFile.Name())

			// Write the test YAML content
			_, err = tempFile.WriteString(tt.yamlContent)
			require.NoError(t, err)
			require.NoError(t, tempFile.Close())

			// Get relative path from backend root
			relativePath, err := filepath.Rel(filepathhelpers.BackendRoot, tempFile.Name())
			require.NoError(t, err)

			result, err := activity.Execute(context.Background(), &ExtractFilterOptionsFromYamlActivityArgs{
				Path:    relativePath,
				Filters: tt.filters,
			})

			if tt.expectedError {
				assert.Error(t, err)
				assert.Empty(t, result.FilterOptions)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedOpts, result.FilterOptions)
			}
		})
	}
}
