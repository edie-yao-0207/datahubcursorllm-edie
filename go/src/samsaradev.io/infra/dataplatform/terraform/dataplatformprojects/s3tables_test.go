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

	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/libs/ni/infraconsts"
)

// TestTableLocationValidation tests that table locations don't conflict with S3 volumes
func TestTableLocationValidation(t *testing.T) {
	// Find all table definition files
	tableDefFiles, err := findTableDefinitionFiles()
	require.NoError(t, err, "Failed to find table definition files")
	require.NotEmpty(t, tableDefFiles, "No table definition files found to test")

	for _, tableFile := range tableDefFiles {
		t.Run(filepath.Base(tableFile), func(t *testing.T) {
			// Read the table definition file
			bytes, err := os.ReadFile(tableFile)
			require.NoError(t, err, "Failed to read table definition file: %s", tableFile)

			var tableDef map[string]interface{}
			err = json.Unmarshal(bytes, &tableDef)
			require.NoError(t, err, "Failed to parse table definition file: %s", tableFile)

			// Check if the table has region_to_location
			regionToLocation, exists := tableDef["region_to_location"]
			if !exists {
				return // Skip tables without location definitions
			}

			regionMap, ok := regionToLocation.(map[string]interface{})
			require.True(t, ok, "region_to_location should be a map")

			// Check each region's location
			for region, locationInterface := range regionMap {
				location, ok := locationInterface.(string)
				require.True(t, ok, "Location should be a string for region %s", region)

				// Validate that locations don't point to databricks-workspace outside of managed_tables
				assertValidTableLocation(t, location, region, tableFile)
			}
		})
	}
}

// findTableDefinitionFiles finds all table definition JSON files
func findTableDefinitionFiles() ([]string, error) {
	var tableFiles []string

	// Use BackendRoot to get the absolute path to the table definitions directory
	definitionsDir := filepath.Join(dataplatformterraformconsts.BackendRoot, "dataplatform", "tables", "s3data", "definitions")

	err := filepath.Walk(definitionsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			tableFiles = append(tableFiles, path)
		}
		return nil
	})

	if err != nil {
		return nil, oops.Wrapf(err, "Error finding table definition files in: %s", definitionsDir)
	}

	return tableFiles, nil
}

// assertValidTableLocation validates that a table location doesn't conflict with S3 volumes
func assertValidTableLocation(t *testing.T, location, region, tableFile string) {
	// Check if location points to a Samsara databricks-workspace bucket
	if !isSamsaraDatabricksWorkspaceBucket(location) {
		return // Not a Samsara databricks-workspace bucket, skip validation
	}

	// Extract the path after the bucket name
	// Expected format: s3://bucket-name/path/to/file
	parts := strings.SplitN(location, "/", 4)
	if len(parts) < 4 {
		return // Invalid S3 URL format, skip validation
	}

	path := parts[3] // Everything after s3://bucket-name/

	// Check if path is in managed_tables folder (valid)
	if strings.HasPrefix(path, "managed_tables/") {
		return // Valid location in managed_tables
	}

	// If it's in a Samsara databricks-workspace bucket but not in managed_tables, it's invalid
	assert.Fail(t,
		"Invalid table location conflicts with S3 volumes",
		"Table location '%s' in region '%s' of file '%s' points to Samsara databricks-workspace bucket outside of managed_tables folder. "+
			"Locations in Samsara databricks-workspace buckets (except managed_tables/) are reserved for S3 volumes. "+
			"Use managed_tables/ subfolder for table data.",
		location, region, tableFile)
}

// isSamsaraDatabricksWorkspaceBucket checks if the S3 location points to a Samsara databricks-workspace bucket
func isSamsaraDatabricksWorkspaceBucket(location string) bool {
	// Extract bucket name from S3 URL
	// Expected format: s3://bucket-name/path/to/file
	parts := strings.SplitN(location, "/", 4)
	if len(parts) < 3 {
		return false // Invalid S3 URL format
	}

	bucketName := parts[2] // Everything after s3://

	// Check if bucket name matches Samsara databricks-workspace bucket patterns
	samsaraDatabricksWorkspaceBuckets := []string{
		infraconsts.SamsaraClouds.USProd.S3BucketPrefix + "databricks-workspace",
		infraconsts.SamsaraClouds.EUProd.S3BucketPrefix + "databricks-workspace",
		infraconsts.SamsaraClouds.CAProd.S3BucketPrefix + "databricks-workspace",
	}

	for _, expectedBucket := range samsaraDatabricksWorkspaceBuckets {
		if bucketName == expectedBucket {
			return true
		}
	}

	return false
}
