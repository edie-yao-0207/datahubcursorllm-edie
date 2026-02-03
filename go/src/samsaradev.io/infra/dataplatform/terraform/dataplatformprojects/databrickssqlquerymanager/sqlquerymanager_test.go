package databrickssqlquerymanager

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

func TestExtractMetadata(t *testing.T) {
	// Setup
	mockMetadataContent := `
    {
        "owner": "testOwner",
        "sql_warehouse": "testWarehouse",
        "description": "testDescription",
        "region": ["us-west-1", "us-east-1"],
        "visualizations": [],
        "parameters": []
    }`
	tempFile, err := os.CreateTemp("", "metadata.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name()) // clean up

	_, err = tempFile.WriteString(mockMetadataContent)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	// Execute
	metadata, err := extractMetadata(tempFile.Name())

	// Verify
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, "testOwner", metadata.Owner)
	require.Equal(t, "testWarehouse", metadata.SqlWarehouse)
}

// TODO: Fix this test to allow comparison of the map
// func TestWarehouses(t *testing.T) {
// 	// Test input and expected output
// 	warehouseSet := map[string]struct{}{
// 		"warehouse1": {},
// 		"warehouse2": {},
// 	}
// 	expected := map[string][]genericresource.Data{
// 		"warehouse": []genericresource.Data{
// 			{
// 				// Assuming the Type and Name are the critical parts to check
// 				Type: "databricks_sql_warehouse",
// 				Name: "warehouse1",
// 				Args: SqlWarehouseArgs{
// 					Name: "warehouse1",
// 				},
// 			},
// 			{
// 				Type: "databricks_sql_warehouse",
// 				Name: "warehouse2",
// 				Args: SqlWarehouseArgs{
// 					Name: "warehouse2",
// 				},
// 			},
// 		},
// 	}

// 	// Execute the function with the test input
// 	result := Warehouses(warehouseSet)

// 	// Verify the function returns the expected output
// 	if !reflect.DeepEqual(result, expected) {
// 		t.Errorf("Expected %v, got %v", expected, result)
// 	}
// }

func TestConvertParameterToHCL(t *testing.T) {
	tests := []struct {
		name        string
		param       ParameterJson
		expected    *databricksresource_official.SqlQueryParameter
		expectError bool
	}{
		{
			name: "Text Parameter",
			param: ParameterJson{
				Name:  "testText",
				Title: "Test Text Parameter",
				Text:  &TextParam{Value: "example text"},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testText",
				Title: "Test Text Parameter",
				Text:  databricksresource_official.QueryParameterText{Value: "example text"},
			},
		},
		{
			name: "Number Parameter",
			param: ParameterJson{
				Name:   "testNumber",
				Title:  "Test Number Parameter",
				Number: &NumberParam{Value: 42},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:   "testNumber",
				Title:  "Test Number Parameter",
				Number: databricksresource_official.QueryParameterNumber{Value: 42},
			},
		},
		{
			name: "Date Parameter",
			param: ParameterJson{
				Name:  "testDate",
				Title: "Test Date Parameter",
				Date:  &DateParam{Value: "2023-01-01"},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testDate",
				Title: "Test Date Parameter",
				Date:  databricksresource_official.QueryParameterDateLike{Value: "2023-01-01"},
			},
		},
		{
			name: "DateTime Parameter",
			param: ParameterJson{
				Name:     "testDateTime",
				Title:    "Test DateTime Parameter",
				DateTime: &DateParam{Value: "2023-01-01T15:04:05Z"},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:     "testDateTime",
				Title:    "Test DateTime Parameter",
				DateTime: databricksresource_official.QueryParameterDateLike{Value: "2023-01-01T15:04:05Z"},
			},
		},
		{
			name: "DateRange Parameter",
			param: ParameterJson{
				Name:  "testDateRange",
				Title: "Test DateRange Parameter",
				DateRange: &DateRangeParam{
					Range: &Range{
						Start: "2023-01-01",
						End:   "2023-01-31",
					},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testDateRange",
				Title: "Test DateRange Parameter",
				DateRange: databricksresource_official.QueryParameterDateRangeLike{
					Range: databricksresource_official.QueryParameterDateRangeValue{
						Start: "2023-01-01",
						End:   "2023-01-31",
					},
				},
			},
		},
		{
			name: "DateTimeRange Parameter",
			param: ParameterJson{
				Name:  "testDateTimeRange",
				Title: "Test DateTimeRange Parameter",
				DateTimeRange: &DateRangeParam{
					Range: &Range{
						Start: "2023-01-01T08:00:00Z",
						End:   "2023-01-01T17:00:00Z",
					},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testDateTimeRange",
				Title: "Test DateTimeRange Parameter",
				DateTimeRange: databricksresource_official.QueryParameterDateRangeLike{
					Range: databricksresource_official.QueryParameterDateRangeValue{
						Start: "2023-01-01T08:00:00Z",
						End:   "2023-01-01T17:00:00Z",
					},
				},
			},
		},
		{
			name: "Enum Parameter Single Value without AllowMultiple",
			param: ParameterJson{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: &EnumParam{
					Value:   "option1",
					Options: []string{"option1", "option2", "option3"},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: databricksresource_official.QueryParameterEnum{
					Values:  []string{"option1"},
					Options: []string{"option1", "option2", "option3"},
				},
			},
		},
		{
			name: "Enum Parameter Multiple Values with AllowMultiple",
			param: ParameterJson{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: &EnumParam{
					Values:  []string{"option1", "option2"},
					Options: []string{"option1", "option2", "option3"},
					Multiple: &Multiple{
						Prefix:    "\"",
						Suffix:    "\"",
						Separator: ",",
					},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: databricksresource_official.QueryParameterEnum{
					Values:  []string{"option1", "option2"},
					Options: []string{"option1", "option2", "option3"},
					Multiple: databricksresource_official.QueryParameterAllowMultiple{
						// The `\"`` is doubly escaped for the string generated
						// in the HCL code to be the intended `\"`
						Prefix:    "\\\"",
						Suffix:    "\\\"",
						Separator: ",",
					},
				},
			},
		},
		{
			name: "Enum Parameter Single Value with AllowMultiple",
			param: ParameterJson{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: &EnumParam{
					Value:   "option1",
					Options: []string{"option1", "option2", "option3"},
					Multiple: &Multiple{
						Prefix:    "\"",
						Suffix:    "\"",
						Separator: ",",
					},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: databricksresource_official.QueryParameterEnum{
					Values:  []string{"option1"},
					Options: []string{"option1", "option2", "option3"},
					Multiple: databricksresource_official.QueryParameterAllowMultiple{
						// The `\"`` is doubly escaped for the string generated
						// in the HCL code to be the intended `\"`
						Prefix:    "\\\"",
						Suffix:    "\\\"",
						Separator: ",",
					},
				},
			},
		},
		{
			name: "Error: Enum Parameter Values without AllowMultiple",
			param: ParameterJson{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: &EnumParam{
					Values:  []string{"option1", "option2"},
					Options: []string{"option1", "option2", "option3"},
				},
			},
			expectError: true,
		},
		{
			name: "Error: Enum Parameter No Value or Values",
			param: ParameterJson{
				Name:  "testEnum",
				Title: "Test Enum Parameter",
				Enum: &EnumParam{
					Options: []string{"option1", "option2", "option3"},
				},
			},
			expectError: true,
		},
		{
			name: "Query Parameter Query uuid",
			param: ParameterJson{
				Name:  "testQuery",
				Title: "Test Query Parameter",
				Query: &QueryParam{
					Type:  "uuid",
					Value: "abc",
					QueryIdMap: map[string]string{
						"us-west-2": "98765432-4321-4321-4321-987654321cba",
						"eu-west-1": "23456789-1234-1234-1234-abc123456789",
					},
					Multiple: &Multiple{
						Prefix:    "\"",
						Suffix:    "\"",
						Separator: ",",
					},
					Region: "us-west-2",
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testQuery",
				Title: "Test Query Parameter",
				Query: databricksresource_official.QueryParameterQuery{
					Values:  []string{"abc"},
					QueryId: "98765432-4321-4321-4321-987654321cba",
					Multiple: databricksresource_official.QueryParameterAllowMultiple{
						Prefix:    "\\\"",
						Suffix:    "\\\"",
						Separator: ",",
					},
				},
			},
		},
		{
			name: "Query Parameter Query name",
			param: ParameterJson{
				Name:  "testQuery",
				Title: "Test Query Parameter",
				Query: &QueryParam{
					Type:      "name",
					Value:     "abc",
					QueryName: "simplequery",
					Multiple: &Multiple{
						Prefix:    "\"",
						Suffix:    "\"",
						Separator: ",",
					},
				},
			},
			expected: &databricksresource_official.SqlQueryParameter{
				Name:  "testQuery",
				Title: "Test Query Parameter",
				Query: databricksresource_official.QueryParameterQuery{
					Values:  []string{"abc"},
					QueryId: "${databricks_sql_query.simplequery.id}",
					Multiple: databricksresource_official.QueryParameterAllowMultiple{
						Prefix:    "\\\"",
						Suffix:    "\\\"",
						Separator: ",",
					},
				},
			},
		},
		{
			name: "Query Parameter Cannot have both Value and Values",
			param: ParameterJson{
				Name:  "testQuery",
				Title: "Test Query Parameter",
				Query: &QueryParam{
					Type:      "name",
					Value:     "abc",
					Values:    []string{"abc"},
					QueryName: "query_name",
					Multiple: &Multiple{
						Prefix:    "\"",
						Suffix:    "\"",
						Separator: ",",
					},
				},
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertParameterToHCL(tc.param)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error for test case '%s', but none occurred", tc.name)
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error for test case '%s', but got: %v", tc.name, err)
				}
				if !reflect.DeepEqual(result, tc.expected) {
					t.Errorf("convertParameterToHCL(%+v) = %+v, expected %+v", tc.param, result, tc.expected)
				}
			}
		})
	}
}

func TestFindQueryFolders(t *testing.T) {
	// Create temporary files and directories for testing
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test files with .sql extension in different directories
	dirs := []string{
		"dir1",
		"dir2",
		"dir3",
	}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), os.ModePerm)
		require.NoError(t, err)

		if dir == "dir2" {
			file, err := os.Create(filepath.Join(tempDir, dir, "test.sql"))
			require.NoError(t, err)
			file.Close()
		}
	}

	// Execute the function
	queryFolders, err := findQueryFolders(tempDir)

	// Verify the results
	require.NoError(t, err)
	expectedFolders := []string{"dir2"}
	require.ElementsMatch(t, expectedFolders, queryFolders)
}

func TestGenerateACLs(t *testing.T) {
	// Test input
	tests := []struct {
		name        string
		permInput   Permission
		runAsRole   databricksresource_official.RunAsRole
		expected    *databricksresource_official.AccessControl
		expectError bool
	}{
		{
			name: "Group Permission",
			permInput: Permission{
				GroupName: "group1",
				Level:     "CAN_VIEW",
			},
			runAsRole: databricksresource_official.RunAsRoleOwner,
			expected: &databricksresource_official.AccessControl{
				GroupName:       "group1",
				PermissionLevel: databricks.PermissionLevelCanView,
			},
		},
		{
			name: "User Permission",
			permInput: Permission{
				UserName: "abc.def@samsara.com",
				Level:    "CAN_RUN",
			},
			runAsRole: databricksresource_official.RunAsRoleViewer,
			expected: &databricksresource_official.AccessControl{
				UserName:        "abc.def@samsara.com",
				PermissionLevel: databricks.PermissionLevelCanRun,
			},
		},
		{
			name: "Error: Invalid Permission Level for SQL Queries",
			permInput: Permission{
				UserName: "abc.def@samsara.com",
				Level:    "CAN_RESTART",
			},
			runAsRole:   databricksresource_official.RunAsRoleOwner,
			expectError: true,
		},
		{
			name: "Error: No Permission Level for runAsRole owner",
			permInput: Permission{
				UserName: "abc.def@samsara.com",
				Level:    "CAN_MANAGE",
			},
			runAsRole:   "owner",
			expectError: true,
		},
		{
			name: "Error: No Principal",
			permInput: Permission{
				Level: "CAN_RUN",
			},
			runAsRole:   databricksresource_official.RunAsRoleViewer,
			expectError: true,
		},
		{
			name: "Error: No Permission Level",
			permInput: Permission{
				UserName: "abc.def@samsara.com",
			},
			runAsRole:   databricksresource_official.RunAsRoleOwner,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := generateACL(tc.permInput, tc.runAsRole)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error for test case '%s', but none occurred", tc.name)
				}
			} else {
				if err != nil {
					t.Errorf("Did not expect an error for test case '%s', but got: %v", tc.name, err)
				}
				if !reflect.DeepEqual(result, tc.expected) {
					t.Errorf("generateACL(%+v) = %+v, expected %+v", tc.permInput, result, tc.expected)
				}
			}
		})
	}
}

// MockFileReader is a mock implementation of the ReadFileFunc
type MockFileReader struct {
	mock.Mock
}

// ReadFile mocks the ReadFileFunc
func (m *MockFileReader) ReadFile(filename string) ([]byte, error) {
	args := m.Called(filename)
	return args.Get(0).([]byte), args.Error(1)
}

func TestSqlQuery(t *testing.T) {
	// Setup
	queriesAbsPath := filepath.Join(filepathhelpers.BackendRoot, BackendRepoQueryRootDir)
	folderPath := "folder_name"
	queryName := "query_name"
	metadata := QueryMetadata{
		Parameters: []ParameterJson{
			{
				Name: "param1",
			},
		},
		Description: "Test description",
		RunAsRole:   "owner",
		Visualizations: []VisConfig{
			{
				VisualizationFilename: "viz1",
				Type:                  "type1",
				Name:                  "viz1",
				Description:           "viz1 description",
			},
		},
		Permissions: []Permission{
			{
				GroupName: "test-group",
				Level:     "CAN_VIEW",
			},
		},
		Owner: "owner1",
	}

	// Mocking file contents
	sqlFileContent := `SELECT * FROM table WHERE column = "value" and column2 = \"value2\";`
	visualizationContent := `{"type": "bar", "options": {}}`

	// Mocking the ReadFile function
	mockFileReader := new(MockFileReader)
	mockFileReader.On("ReadFile", filepath.Join(queriesAbsPath, folderPath, queryName+".sql")).Return([]byte(sqlFileContent), nil)
	mockFileReader.On("ReadFile", filepath.Join(queriesAbsPath, "visualizations", "viz1.json")).Return([]byte(visualizationContent), nil)

	c := dataplatformconfig.DatabricksConfig{Region: "us-west-2"}
	result, err := SqlQuery(c, folderPath, queryName, metadata, mockFileReader.ReadFile)

	// Asserts
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Checking the SQL content escaping
	escapedSQL := `SELECT * FROM table WHERE column = \"value\" and column2 = \"value2\";`
	assert.Contains(t, result[queryName][0].(*databricksresource_official.SqlQuery).Query, escapedSQL)

	// Checking the visualization content escaping
	escapedVis := `{\"type\": \"bar\", \"options\": {}}`
	assert.Contains(t, result[queryName][1].(*databricksresource_official.SqlVisualization).Options, escapedVis)

	// Additional checks for resources
	assert.Equal(t, queryName, result[queryName][0].(*databricksresource_official.SqlQuery).Name)
	assert.Equal(t, queryName+"_viz1", result[queryName][1].(*databricksresource_official.SqlVisualization).Name)
	assert.Equal(t, metadata.Description, result[queryName][0].(*databricksresource_official.SqlQuery).Description)

	// Permissions
	assert.Len(t, result[queryName], 3) // SqlQuery, SqlVisualization, and Permissions
	assert.Equal(t, "owner1", result[queryName][2].(*databricksresource_official.Permissions).AccessControls[1].GroupName)

	mockFileReader.AssertExpectations(t)
}

func TestEscapeQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Unescaped quotes",
			input:    `He said, "Hello, World!"`,
			expected: `He said, \"Hello, World!\"`,
		},
		{
			name:     "Already escaped quotes",
			input:    `He said, \"Hello, World!\"`,
			expected: `He said, \"Hello, World!\"`,
		},
		{
			name:     "Mixed escaped and unescaped quotes",
			input:    `She replied, "\"Indeed!\" he exclaimed."`,
			expected: `She replied, \"\"Indeed!\" he exclaimed.\"`,
		},
		{
			name:     "Escaped backslashes before quotes",
			input:    `He said, \\\"Hello, World!\\\"`,
			expected: `He said, \\\"Hello, World!\\\"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeQuotes(tt.input)
			if result != tt.expected {
				t.Errorf("escapeQuotes(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}
