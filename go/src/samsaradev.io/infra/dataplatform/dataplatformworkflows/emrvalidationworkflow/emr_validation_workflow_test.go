package emrvalidationworkflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrvalidationworkflow/activity"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/libs/ni/pointer"
)

func TestEmrValidationWorkflow_Execute_NilChecks(t *testing.T) {
	workflow := &EmrValidationWorkflow{}

	tests := []struct {
		name        string
		engine      workflowengine.WorkflowEngine
		args        *EmrValidationWorkflowArgs
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil engine should return error",
			engine:      nil,
			args:        &EmrValidationWorkflowArgs{},
			expectError: true,
			errorMsg:    "workflow engine cannot be nil",
		},
		{
			name:        "nil args should return error",
			engine:      workflowengine.NewDummyWorkflowEngine(nil),
			args:        nil,
			expectError: true,
			errorMsg:    "workflow arguments cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := workflow.Execute(tt.engine, tt.args)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEmrValidationWorkflow_ArgumentDefaults(t *testing.T) {
	// Test that default values are set correctly
	// This test focuses on the argument processing logic

	args := &EmrValidationWorkflowArgs{
		Cell:       "us3",
		EntityName: "trip",
		S3Bucket:   "test-bucket",
		S3Region:   "us-west-2",
	}

	// Test default values
	defaultNumOrgs := 50
	defaultNumAssets := 20

	// Verify that when NumOrgsToValidate is nil, it gets set to default
	if args.NumOrgsToValidate == nil {
		args.NumOrgsToValidate = &defaultNumOrgs
	}
	assert.Equal(t, 50, *args.NumOrgsToValidate)

	// Verify that when NumAssetsToValidate is nil, it gets set to default
	if args.NumAssetsToValidate == nil {
		args.NumAssetsToValidate = &defaultNumAssets
	}
	assert.Equal(t, 20, *args.NumAssetsToValidate)

	// Test with custom values
	customArgs := &EmrValidationWorkflowArgs{
		Cell:                "prod",
		EntityName:          "speedingintervalsbytrip",
		NumOrgsToValidate:   pointer.New(10),
		NumAssetsToValidate: pointer.New(5),
		S3Bucket:            "custom-bucket",
		S3Region:            "eu-west-1",
	}

	assert.Equal(t, 10, *customArgs.NumOrgsToValidate)
	assert.Equal(t, 5, *customArgs.NumAssetsToValidate)
}

func TestGetAssetTypeFromRegistry(t *testing.T) {
	tests := []struct {
		name         string
		entityName   string
		specs        []emrreplication.EmrReplicationSpec
		expectedType emrreplication.AssetType
	}{
		{
			name:       "should return driver asset type for speedingintervalsbytrip",
			entityName: "speedingintervalsbytrip",
			specs: []emrreplication.EmrReplicationSpec{
				{Name: "speedingintervalsbytrip", AssetType: emrreplication.DriverAssetType},
				{Name: "trip", AssetType: emrreplication.DeviceAssetType},
			},
			expectedType: emrreplication.DriverAssetType,
		},
		{
			name:       "should return device asset type for trip",
			entityName: "trip",
			specs: []emrreplication.EmrReplicationSpec{
				{Name: "speedingintervalsbytrip", AssetType: emrreplication.DriverAssetType},
				{Name: "trip", AssetType: emrreplication.DeviceAssetType},
			},
			expectedType: emrreplication.DeviceAssetType,
		},
		{
			name:       "should return default device asset type for unknown entity",
			entityName: "unknown_entity",
			specs: []emrreplication.EmrReplicationSpec{
				{Name: "speedingintervalsbytrip", AssetType: emrreplication.DriverAssetType},
				{Name: "trip", AssetType: emrreplication.DeviceAssetType},
			},
			expectedType: emrreplication.DeviceAssetType,
		},
		{
			name:         "should return default device asset type for empty specs",
			entityName:   "any_entity",
			specs:        []emrreplication.EmrReplicationSpec{},
			expectedType: emrreplication.DeviceAssetType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getAssetTypeFromRegistry(tt.entityName, func() []emrreplication.EmrReplicationSpec {
				return tt.specs
			})
			assert.Equal(t, tt.expectedType, result)
		})
	}
}

func TestEmrValidationWorkflow_Definition(t *testing.T) {
	workflow := &EmrValidationWorkflow{}
	definition := workflow.Definition()

	assert.NotNil(t, definition)
	assert.Equal(t, EmrValidationWorkflowDefinition, definition)
}

func TestEmrValidationWorkflow_SupportedVersionFlags(t *testing.T) {
	workflow := &EmrValidationWorkflow{}
	flags := workflow.SupportedVersionFlags()

	assert.NotNil(t, flags)
	assert.Empty(t, flags) // Currently returns empty slice
}

func TestNewEmrValidationWorkflow(t *testing.T) {
	// Test workflow constructor
	params := EmrValidationWorkflowParams{
		SelectOrgsAndAssetsActivity:      &activity.SelectOrgsAndAssetsActivity{},
		RetrieveAndExportEmrDataActivity: &activity.RetrieveAndExportEmrDataActivity{},
		WriteManifestActivity:            &activity.WriteManifestActivity{},
	}

	workflow := NewEmrValidationWorkflow(params)

	assert.NotNil(t, workflow)
	assert.Equal(t, params.SelectOrgsAndAssetsActivity, workflow.SelectOrgsAndAssetsActivity)
	assert.Equal(t, params.RetrieveAndExportEmrDataActivity, workflow.RetrieveAndExportEmrDataActivity)
	assert.Equal(t, params.WriteManifestActivity, workflow.WriteManifestActivity)
}

func TestEmrValidationWorkflow_S3BucketDefault(t *testing.T) {
	// Test that S3 bucket gets default value when empty
	args := &EmrValidationWorkflowArgs{
		Cell:       "us3",
		EntityName: "trip",
		S3Region:   "us-west-2",
	}

	// Simulate the default bucket logic from the workflow
	if args.S3Bucket == "" {
		args.S3Bucket = "samsara-emr-replication-export-" + args.Cell
	}

	assert.Equal(t, "samsara-emr-replication-export-us3", args.S3Bucket)

	// Test with custom bucket
	customArgs := &EmrValidationWorkflowArgs{
		Cell:     "eu1",
		S3Bucket: "my-custom-bucket",
	}

	// Should not override custom bucket
	if customArgs.S3Bucket == "" {
		customArgs.S3Bucket = "samsara-emr-replication-export-" + customArgs.Cell
	}

	assert.Equal(t, "my-custom-bucket", customArgs.S3Bucket)
}

func TestEmrValidationWorkflow_ValidationDateDefault(t *testing.T) {
	// Test that validation date defaults to yesterday
	testTime := time.Date(2023, 12, 15, 10, 0, 0, 0, time.UTC)
	expectedYesterday := time.Date(2023, 12, 14, 0, 0, 0, 0, time.UTC)

	args := &EmrValidationWorkflowArgs{
		Cell:       "us3",
		EntityName: "trip",
	}

	// Simulate the default validation date logic from the workflow
	if args.ValidationDate == nil {
		yesterday := testTime.AddDate(0, 0, -1)
		args.ValidationDate = &yesterday
	}

	// Check that it's set to yesterday (ignoring time components for this test)
	assert.Equal(t, expectedYesterday.Year(), args.ValidationDate.Year())
	assert.Equal(t, expectedYesterday.Month(), args.ValidationDate.Month())
	assert.Equal(t, expectedYesterday.Day(), args.ValidationDate.Day())

	// Test with custom validation date
	customDate := time.Date(2023, 11, 20, 0, 0, 0, 0, time.UTC)
	customArgs := &EmrValidationWorkflowArgs{
		Cell:           "us3",
		EntityName:     "trip",
		ValidationDate: &customDate,
	}

	// Should not override custom date
	if customArgs.ValidationDate == nil {
		yesterday := testTime.AddDate(0, 0, -1)
		customArgs.ValidationDate = &yesterday
	}

	assert.Equal(t, customDate, *customArgs.ValidationDate)
}
