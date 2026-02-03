package godatalake

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/security/secrets"
)

// mockComputeConfig implements ComputeConfig interface for testing purposes. It
// provides a simplified implementation that only tracks the compute type.
type mockComputeConfig struct {
	computeType ComputeType
}

// Validate ensures the compute type is not empty.
// Returns an error if validation fails.
func (m *mockComputeConfig) Validate() error {
	if m.computeType == "" {
		return errors.New("compute type cannot be empty")
	}
	return nil
}

// Type returns the compute type of this configuration.
func (m *mockComputeConfig) Type() ComputeType {
	return m.computeType
}

// mockSecretService implements secrets.SecretService for testing. It provides a
// simplified implementation that returns static mock values for all
// secret-related operations.
type mockSecretService struct{}

// GetSecretValue implements secrets.SecretService by returning a mock secret
// value. This implementation ignores the provided options and always returns
// the same mock value.
func (m *mockSecretService) GetSecretValue(opts secrets.GetSecretOptions) (secrets.GetSecretResponse, error) {
	return secrets.GetSecretResponse{
		StringValue: "mock-secret-value",
	}, nil
}

// GetSecretVersionIds implements secrets.SecretService by returning a mock version ID.
// This implementation ignores the provided key and always returns a static version ID.
func (m *mockSecretService) GetSecretVersionIds(key string) (secrets.GetSecretVersionIdsResponse, error) {
	return secrets.GetSecretVersionIdsResponse{
		VersionIds: []string{"v1"},
	}, nil
}

// GetSecretValueFromKey implements secrets.SecretService by returning a mock secret value.
// This implementation ignores the provided key and always returns the same mock value.
func (m *mockSecretService) GetSecretValueFromKey(key string) (secrets.GetSecretResponse, error) {
	return secrets.GetSecretResponse{
		StringValue: "mock-secret-value",
	}, nil
}

// GetSecretMetadata implements secrets.SecretService by returning a mock metadata response.
func (m *mockSecretService) GetSecretMetadata(key string) (secrets.GetSecretMetadataResponse, error) {
	now := time.Now()
	return secrets.GetSecretMetadataResponse{
		CreatedDate:      &now,
		LastChangedDate:  &now,
		LastAccessedDate: &now,
	}, nil
}

// TestBuildGoDataLakeConfig verifies that buildGoDataLakeConfig correctly
// handles various input configurations and properly sets default values.
func TestBuildGoDataLakeConfig(t *testing.T) {
	tests := []struct {
		name                             string
		input                            Config
		expectedType                     ComputeType
		expectedDatabricksSqlWarehouseId string
		expectError                      bool
	}{
		{
			name: "should use default configs when no computeType and warehouseID are provided",
			input: Config{
				AppConfig: &config.AppConfig{
					DatabricksSqlWarehouseId: "504682c42035cf05",
				},
			},
			expectedType:                     ComputeDatabricks,
			expectedDatabricksSqlWarehouseId: "504682c42035cf05",
		},
		{
			name: "should use explicit configs when provided",
			input: Config{
				Compute: ComputeDatabricks,
				ComputeConfig: DatabricksConfig{
					WarehouseID: "custom_warehouse",
				},
				AppConfig: &config.AppConfig{
					DatabricksSqlWarehouseId: "504682c42035cf05",
				},
			},
			expectedType:                     ComputeDatabricks,
			expectedDatabricksSqlWarehouseId: "custom_warehouse",
		},
		{
			name: "should error if missing AppConfig",
			input: Config{
				Compute: ComputeDatabricks,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectError {
				assert.Panics(t, func() {
					buildGoDataLakeConfig(&tt.input)
				})
				return
			}

			result := buildGoDataLakeConfig(&tt.input)
			assert.Equal(t, tt.expectedType, result.Compute)

			dbConfig, ok := result.ComputeConfig.(DatabricksConfig)
			require.True(t, ok)
			assert.Equal(t, tt.expectedDatabricksSqlWarehouseId, dbConfig.WarehouseID)
		})
	}
}
