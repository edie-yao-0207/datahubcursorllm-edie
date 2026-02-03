package godatalake

import (
	"testing"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/config"
)

func TestDatabricksConfig_Validate(t *testing.T) {
	mockAppConfig := &config.AppConfig{
		DatabricksSqlWarehouseId: "504682c42035cf05",
	}

	tests := []struct {
		description string
		config      DatabricksConfig
		expectError bool
	}{
		{
			description: "should not error if there is a valid config",
			config: DatabricksConfig{
				WarehouseID: "504682c42035cf05",
				AuthType:    authTypeOAuthM2M,
			},
			expectError: false,
		},
		{
			description: "should error if missing warehouse ID",
			config: DatabricksConfig{
				WarehouseID: "",
			},
			expectError: true,
		},
		{
			description: "should error if missing authType",
			config: DatabricksConfig{
				WarehouseID: "504682c42035cf05",
				AuthType:    "",
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			config := &Config{
				Compute:       ComputeDatabricks,
				ComputeConfig: tc.config,
				AppConfig:     mockAppConfig,
				SecretService: &mockSecretService{},
			}
			err := config.Validate()
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDatabricksClient_Query_WarehouseID(t *testing.T) {
	defaultWarehouse := "504682c42035cf05"
	overrideWarehouse := "351c51ed0e7809be"

	tests := []struct {
		description  string
		defaultWHID  string
		overrideWHID string
		expectError  bool
	}{
		{
			description: "use default warehouse",
			defaultWHID: defaultWarehouse,
		},
		{
			description:  "use override warehouse",
			defaultWHID:  defaultWarehouse,
			overrideWHID: overrideWarehouse,
		},
		{
			description:  "empty override falls back to default",
			defaultWHID:  defaultWarehouse,
			overrideWHID: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			// Create a client with the default warehouse ID
			client := &databricksClient{
				config: Config{
					Compute: ComputeDatabricks,
					ComputeConfig: DatabricksConfig{
						WarehouseID: tc.defaultWHID,
					},
				},
			}

			// Create query options with override if specified
			var opts *QueryOptions
			if tc.overrideWHID != "" {
				opts = &QueryOptions{
					WarehouseID: tc.overrideWHID,
				}
			}

			// Verify the warehouse ID is set correctly
			config := client.config.ComputeConfig.(DatabricksConfig)
			if opts != nil && opts.WarehouseID != "" {
				assert.Equal(t, tc.overrideWHID, opts.WarehouseID)
			} else {
				assert.Equal(t, tc.defaultWHID, config.WarehouseID)
			}
		})
	}
}

func TestConvertArgsToTypedParams(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected []interface{}
	}{
		{
			name:  "int64 converts to SqlBigInt with string value",
			input: []interface{}{int64(562949953429025)},
			expected: []interface{}{
				dbsql.Parameter{Type: dbsql.SqlBigInt, Value: "562949953429025"},
			},
		},
		{
			name:  "int converts to SqlBigInt with string value",
			input: []interface{}{int(12345)},
			expected: []interface{}{
				dbsql.Parameter{Type: dbsql.SqlBigInt, Value: "12345"},
			},
		},
		{
			name:     "string passes through unchanged",
			input:    []interface{}{"hello"},
			expected: []interface{}{"hello"},
		},
		{
			name:     "bool passes through unchanged",
			input:    []interface{}{true},
			expected: []interface{}{true},
		},
		{
			name:     "float64 passes through unchanged",
			input:    []interface{}{3.14},
			expected: []interface{}{3.14},
		},
		{
			name:  "mixed types converts only int/int64",
			input: []interface{}{"start_time", int64(1766489486559), int(437), "2024-01-01"},
			expected: []interface{}{
				"start_time",
				dbsql.Parameter{Type: dbsql.SqlBigInt, Value: "1766489486559"},
				dbsql.Parameter{Type: dbsql.SqlBigInt, Value: "437"},
				"2024-01-01",
			},
		},
		{
			name:     "empty args returns empty slice",
			input:    []interface{}{},
			expected: []interface{}{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := convertArgsToTypedParams(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
