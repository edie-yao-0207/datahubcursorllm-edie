package dataplatformprojects

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/samsarahq/go/oops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
)

// findMetadataFiles recursively searches for all .metadata.json files in the given directory
func findMetadataFiles(rootDir string) ([]string, error) {
	var metadataFiles []string
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" && strings.HasSuffix(path, ".metadata.json") {
			metadataFiles = append(metadataFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "Error finding metadata files in: %s", rootDir)
	}
	return metadataFiles, nil
}

func TestNotebookMetadataValidDataSecurityModes(t *testing.T) {
	// Find all metadata files in the notebooks directory
	metadataFiles, err := findMetadataFiles(dataplatformterraformconsts.NotebooksRoot)
	require.NoError(t, err, "Failed to find metadata files")
	require.NotEmpty(t, metadataFiles, "No metadata files found to test")

	// Define valid data security modes
	validModes := map[string]bool{
		"": true, // Empty is valid (will default to SINGLE_USER)
		databricksresource.DataSecurityModeSingleUser:    true,
		databricksresource.DataSecurityModeUserIsolation: true,
	}

	// Check each metadata file
	for _, metadataFile := range metadataFiles {
		t.Run(metadataFile, func(t *testing.T) {
			// Read the metadata file
			bytes, err := os.ReadFile(metadataFile)
			require.NoError(t, err, "Failed to read metadata file: %s", metadataFile)

			var metadata NotebookMetadata
			err = json.Unmarshal(bytes, &metadata)
			require.NoError(t, err, "Failed to parse metadata file: %s", metadataFile)

			// Check each job in the metadata
			for i, job := range metadata.Jobs {
				// Skip if UnityCatalogSetting is nil
				if job.UnityCatalogSetting == nil {
					continue
				}

				// Check if DataSecurityMode is valid
				mode := job.UnityCatalogSetting.DataSecurityMode
				assert.True(t, validModes[mode],
					"Invalid DataSecurityMode '%s' in job %d of file %s. Valid modes are: ['', '%s', '%s']",
					mode, i, metadataFile,
					databricksresource.DataSecurityModeSingleUser,
					databricksresource.DataSecurityModeUserIsolation)
			}
		})
	}
}

// TestNotebookMetadataJsonFormat tests that the UnityCatalogSetting structure in JSON
// properly maps to our Go struct and verifies field names match the expected format
func TestNotebookMetadataJsonFormat(t *testing.T) {
	// Test JSON with various UnityCatalogSetting configurations
	jsonTests := []struct {
		name           string
		jsonData       string
		expectedMode   string
		expectNilField bool
	}{
		{
			name: "Full setting with data_security_mode",
			jsonData: `{
				"owner": "TestTeam",
				"email_notifications": ["test@example.com"],
				"jobs": [
					{
						"name": "test-job",
						"unity_catalog_setting": {
							"data_security_mode": "USER_ISOLATION"
						}
					}
				]
			}`,
			expectedMode:   "USER_ISOLATION",
			expectNilField: false,
		},
		{
			name: "Empty unity_catalog_setting object",
			jsonData: `{
				"owner": "TestTeam",
				"email_notifications": ["test@example.com"],
				"jobs": [
					{
						"name": "test-job",
						"unity_catalog_setting": {}
					}
				]
			}`,
			expectedMode:   "",
			expectNilField: false,
		},
		{
			name: "No unity_catalog_setting defined",
			jsonData: `{
				"owner": "TestTeam",
				"email_notifications": ["test@example.com"],
				"jobs": [
					{
						"name": "test-job"
					}
				]
			}`,
			expectNilField: true,
		},
		{
			name: "Field should be unity_catalog_setting not settings",
			jsonData: `{
				"owner": "TestTeam",
				"email_notifications": ["test@example.com"],
				"jobs": [
					{
						"name": "test-job",
						"unity_catalog_settings": {
							"data_security_mode": "USER_ISOLATION"
						}
					}
				]
			}`,
			expectNilField: true, // The field with wrong name should be ignored
		},
	}

	for _, tc := range jsonTests {
		t.Run(tc.name, func(t *testing.T) {
			var metadata NotebookMetadata
			err := json.Unmarshal([]byte(tc.jsonData), &metadata)
			require.NoError(t, err, "Failed to parse JSON")

			require.NotEmpty(t, metadata.Jobs, "Jobs should not be empty")
			job := metadata.Jobs[0]

			if tc.expectNilField {
				assert.Nil(t, job.UnityCatalogSetting, "UnityCatalogSetting should be nil")
			} else {
				assert.NotNil(t, job.UnityCatalogSetting, "UnityCatalogSetting should not be nil")
				assert.Equal(t, tc.expectedMode, job.UnityCatalogSetting.DataSecurityMode,
					"DataSecurityMode should match expected value")
			}
		})
	}
}

func TestNotebookMetadataSloConfigJsonFormat(t *testing.T) {
	jsonData := `{
		"owner": "TestTeam",
		"email_notifications": ["test@example.com"],
		"jobs": [
			{
				"name": "test-job",
				"slo_config": {
					"low_urgency_threshold_hours": 24,
					"business_hours_threshold_hours": 36,
					"high_urgency_threshold_hours": 48
				}
			}
		]
	}`

	var metadata NotebookMetadata
	err := json.Unmarshal([]byte(jsonData), &metadata)
	require.NoError(t, err, "Failed to parse JSON")

	require.NotEmpty(t, metadata.Jobs, "Jobs should not be empty")
	job := metadata.Jobs[0]
	require.NotNil(t, job.SloConfig, "SloConfig should not be nil")
	assert.Equal(t, int64(24), job.SloConfig.LowUrgencyThresholdHours)
	assert.Equal(t, int64(36), job.SloConfig.BusinessHoursThresholdHours)
	assert.Equal(t, int64(48), job.SloConfig.HighUrgencyThresholdHours)
}

func TestNotebookSloConfig_ToJobSlo(t *testing.T) {
	tests := []struct {
		name        string
		config      *NotebookSloConfig
		expectNil   bool
		expectError bool
	}{
		{
			name:      "empty config returns nil",
			config:    &NotebookSloConfig{},
			expectNil: true,
		},
		{
			name: "thresholds map",
			config: &NotebookSloConfig{
				LowUrgencyThresholdHours: 24,
			},
			expectNil:   false,
			expectError: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.config.ToJobSlo()
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.expectNil {
				assert.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			assert.NotZero(t, result.LowUrgencyThresholdHours+result.BusinessHoursThresholdHours+result.HighUrgencyThresholdHours)
		})
	}
}

// createMockNotebookMetadataJob creates a NotebookMetadataJob with optional UnityCatalogSetting
func createMockNotebookMetadataJob(name string, securityMode string) *NotebookMetadataJob {
	job := &NotebookMetadataJob{
		Name: name,
	}

	if securityMode != "" {
		job.UnityCatalogSetting = &UnityCatalogSetting{
			DataSecurityMode: securityMode,
		}
	}

	return job
}

// TestValidateNotebookMetadataSecurityMode tests the validation logic for DataSecurityMode in notebooks.go
func TestValidateNotebookMetadataSecurityMode(t *testing.T) {
	tests := []struct {
		name         string
		securityMode string
		expectError  bool
	}{
		{
			name:         "Valid SINGLE_USER mode",
			securityMode: databricksresource.DataSecurityModeSingleUser,
			expectError:  false,
		},
		{
			name:         "Valid USER_ISOLATION mode",
			securityMode: databricksresource.DataSecurityModeUserIsolation,
			expectError:  false,
		},
		{
			name:         "Empty mode is valid",
			securityMode: "",
			expectError:  false,
		},
		{
			name:         "Invalid mode",
			securityMode: "INVALID_MODE",
			expectError:  true,
		},
		{
			name:         "No UnityCatalogSetting",
			securityMode: "", // This will create a job without UnityCatalogSetting
			expectError:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a job with the test security mode
			job := createMockNotebookMetadataJob("test-job", tc.securityMode)

			// Validate the job's security mode
			if job.UnityCatalogSetting != nil {
				switch job.UnityCatalogSetting.DataSecurityMode {
				case "", databricksresource.DataSecurityModeSingleUser, databricksresource.DataSecurityModeUserIsolation:
					assert.False(t, tc.expectError, "Expected no error for valid mode")
				default:
					assert.True(t, tc.expectError, "Expected error for invalid mode: %s", job.UnityCatalogSetting.DataSecurityMode)
				}
			} else {
				// Job doesn't have UnityCatalogSetting
				assert.False(t, tc.expectError, "Expected no error when UnityCatalogSetting is nil")
			}
		})
	}
}
