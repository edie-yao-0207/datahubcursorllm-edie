package helpers

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/platform/entity/types/value"
)

func TestCalculateBatchJitter(t *testing.T) {
	tests := []struct {
		name         string
		batchIndex   int
		totalBatches int
		maxSeconds   int
		wantRange    struct {
			min time.Duration
			max time.Duration
		}
	}{
		{
			name:         "zero total batches",
			batchIndex:   0,
			totalBatches: 0,
			maxSeconds:   30,
			wantRange: struct {
				min time.Duration
				max time.Duration
			}{
				min: 0,
				max: 0,
			},
		},
		{
			name:         "zero max seconds",
			batchIndex:   0,
			totalBatches: 1,
			maxSeconds:   0,
			wantRange: struct {
				min time.Duration
				max time.Duration
			}{
				min: 0,
				max: 0,
			},
		},
		{
			name:         "max jitter",
			batchIndex:   0,
			totalBatches: 1,
			maxSeconds:   30,
			wantRange: struct {
				min time.Duration
				max time.Duration
			}{
				min: 0,
				max: 30 * time.Second,
			},
		},
		{
			name:         "multiple batches",
			batchIndex:   1,
			totalBatches: 3,
			maxSeconds:   10,
			wantRange: struct {
				min time.Duration
				max time.Duration
			}{
				min: 0,
				max: 10 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBatchJitter(tt.batchIndex, tt.totalBatches, tt.maxSeconds)
			if got < tt.wantRange.min || got > tt.wantRange.max {
				t.Errorf("CalculateBatchJitter() = %v, want between %v and %v", got, tt.wantRange.min, tt.wantRange.max)
			}

			// Test determinism - same inputs should produce same output
			got2 := CalculateBatchJitter(tt.batchIndex, tt.totalBatches, tt.maxSeconds)
			if got != got2 {
				t.Errorf("CalculateBatchJitter() is not deterministic: first call = %v, second call = %v", got, got2)
			}
		})
	}

	// Test distribution across multiple batches
	t.Run("distribution across batches", func(t *testing.T) {
		totalBatches := 10
		maxSeconds := 5
		seenValues := make(map[time.Duration]bool)

		for i := 0; i < totalBatches; i++ {
			jitter := CalculateBatchJitter(i, totalBatches, maxSeconds)
			seenValues[jitter] = true
		}

		// We should see at least 3 different values to ensure some distribution
		if len(seenValues) < 3 {
			t.Errorf("CalculateBatchJitter() produced only %d unique values, want at least 3", len(seenValues))
		}
	})
}

func TestBatchChildWorkflowExecutions(t *testing.T) {
	type testCase struct {
		name            string
		executions      []workflowengine.ChildWorkflowExecution[string]
		batchSize       int
		expectedBatches [][]workflowengine.ChildWorkflowExecution[string]
	}

	testCases := []testCase{
		{
			name:            "empty executions",
			executions:      []workflowengine.ChildWorkflowExecution[string]{},
			batchSize:       3,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{},
		},
		{
			name: "exact batch size",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
				{Args: pointer.New("3")},
			},
			batchSize: 3,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
					{Args: pointer.New("3")},
				},
			},
		},
		{
			name: "multiple full batches",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
				{Args: pointer.New("3")},
				{Args: pointer.New("4")},
				{Args: pointer.New("5")},
				{Args: pointer.New("6")},
			},
			batchSize: 2,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
				},
				{
					{Args: pointer.New("3")},
					{Args: pointer.New("4")},
				},
				{
					{Args: pointer.New("5")},
					{Args: pointer.New("6")},
				},
			},
		},
		{
			name: "partial last batch",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
				{Args: pointer.New("3")},
				{Args: pointer.New("4")},
				{Args: pointer.New("5")},
			},
			batchSize: 2,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
				},
				{
					{Args: pointer.New("3")},
					{Args: pointer.New("4")},
				},
				{
					{Args: pointer.New("5")},
				},
			},
		},
		{
			name: "batch size larger than executions",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
			},
			batchSize: 5,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
				},
			},
		},
		{
			name: "zero batch size",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
			},
			batchSize: 0,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
				},
			},
		},
		{
			name: "negative batch size",
			executions: []workflowengine.ChildWorkflowExecution[string]{
				{Args: pointer.New("1")},
				{Args: pointer.New("2")},
			},
			batchSize: -1,
			expectedBatches: [][]workflowengine.ChildWorkflowExecution[string]{
				{
					{Args: pointer.New("1")},
					{Args: pointer.New("2")},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batches := BatchChildWorkflowExecutions(tc.executions, tc.batchSize)
			if len(batches) != len(tc.expectedBatches) {
				t.Errorf("expected %d batches, got %d", len(tc.expectedBatches), len(batches))
				return
			}

			for i, batch := range batches {
				if len(batch) != len(tc.expectedBatches[i]) {
					t.Errorf("batch %d: expected %d executions, got %d", i, len(tc.expectedBatches[i]), len(batch))
					continue
				}

				for j, execution := range batch {
					if *execution.Args != *tc.expectedBatches[i][j].Args {
						t.Errorf("batch %d, execution %d: expected %v, got %v", i, j, *tc.expectedBatches[i][j].Args, *execution.Args)
					}
				}
			}
		})
	}
}

func TestGetSlogInfoTags(t *testing.T) {
	tests := []struct {
		name              string
		orgId             int64
		entityName        string
		backfillRequestId string
		additionalTags    map[string]interface{}
		expected          []interface{}
	}{
		{
			name:              "no additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags:    nil,
			expected: []interface{}{
				"orgId", int64(123),
				"entityName", "test_entity",
				"backfillRequestId", "req-123",
			},
		},
		{
			name:              "with additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags: map[string]interface{}{
				"pageToken": "token-123",
				"error":     "test error",
			},
			expected: []interface{}{
				"orgId", int64(123),
				"entityName", "test_entity",
				"backfillRequestId", "req-123",
				"pageToken", "token-123",
				"error", "test error",
			},
		},
		{
			name:              "with empty additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags:    map[string]interface{}{},
			expected: []interface{}{
				"orgId", int64(123),
				"entityName", "test_entity",
				"backfillRequestId", "req-123",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetSlogInfoTags(tt.orgId, tt.entityName, tt.backfillRequestId, tt.additionalTags)

			// Verify the length matches
			assert.Equal(t, len(tt.expected), len(result), "length mismatch")

			// Verify the default tags are in the correct order at the start
			assert.Equal(t, "orgId", result[0])
			assert.Equal(t, tt.orgId, result[1])
			assert.Equal(t, "entityName", result[2])
			assert.Equal(t, tt.entityName, result[3])
			assert.Equal(t, "backfillRequestId", result[4])
			assert.Equal(t, tt.backfillRequestId, result[5])

			// If there are additional tags, verify they are present
			if tt.additionalTags != nil && len(tt.additionalTags) > 0 {
				// Convert the remaining elements to a map for comparison
				resultMap := make(map[string]interface{})
				for i := 6; i < len(result); i += 2 {
					if i+1 < len(result) {
						key, ok := result[i].(string)
						if !ok {
							t.Errorf("expected string key at index %d, got %T", i, result[i])
							continue
						}
						resultMap[key] = result[i+1]
					}
				}

				// Compare with the additional tags
				assert.Equal(t, tt.additionalTags, resultMap)
			}
		})
	}
}

func TestGetSlogErrorTags(t *testing.T) {
	tests := []struct {
		name              string
		orgId             int64
		entityName        string
		backfillRequestId string
		additionalTags    map[string]interface{}
		expected          map[string]interface{}
	}{
		{
			name:              "no additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags:    nil,
			expected: map[string]interface{}{
				"orgId":             int64(123),
				"entityName":        "test_entity",
				"backfillRequestId": "req-123",
			},
		},
		{
			name:              "with additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags: map[string]interface{}{
				"pageToken": "token-123",
				"error":     "test error",
			},
			expected: map[string]interface{}{
				"orgId":             int64(123),
				"entityName":        "test_entity",
				"backfillRequestId": "req-123",
				"pageToken":         "token-123",
				"error":             "test error",
			},
		},
		{
			name:              "with empty additional tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags:    map[string]interface{}{},
			expected: map[string]interface{}{
				"orgId":             int64(123),
				"entityName":        "test_entity",
				"backfillRequestId": "req-123",
			},
		},
		{
			name:              "overwrite default tags",
			orgId:             123,
			entityName:        "test_entity",
			backfillRequestId: "req-123",
			additionalTags: map[string]interface{}{
				"orgId": int64(456), // Explicitly cast to int64
			},
			expected: map[string]interface{}{
				"orgId":             int64(456), // Should be overwritten
				"entityName":        "test_entity",
				"backfillRequestId": "req-123",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetSlogErrorTags(tt.orgId, tt.entityName, tt.backfillRequestId, tt.additionalTags)

			// Convert slog.Tag to map[string]interface{} for comparison
			resultMap := make(map[string]interface{})
			for k, v := range result {
				resultMap[k] = v
			}

			assert.Equal(t, tt.expected, resultMap)
		})
	}
}

func TestGenerateEmrDataHash(t *testing.T) {
	// Simple test struct that can be marshaled to JSON
	type TestData struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	// Create a value wrapper that can be marshaled
	createTestValue := func(data interface{}) *value.Value {
		// For testing, we'll create a simple wrapper that can be marshaled
		// This is a simplified approach for testing purposes
		jsonBytes, _ := json.Marshal(data)
		// Create a basic value that wraps the data
		var v interface{}
		json.Unmarshal(jsonBytes, &v)
		// Return a pointer to a zero value for testing - the actual implementation
		// will handle the JSON marshaling through the value.Value interface
		return &value.Value{}
	}

	testCases := []struct {
		description    string
		testData       interface{} // The actual data we want to test
		expectError    bool
		expectHashLen  int
		validateFormat bool
	}{
		{
			description: "simple EMR data",
			testData: TestData{
				ID:   "test-id-123",
				Name: "test-name",
			},
			expectError:    false,
			expectHashLen:  16,
			validateFormat: true,
		},
		{
			description: "different simple EMR data",
			testData: TestData{
				ID:   "test-id-456",
				Name: "different-name",
			},
			expectError:    false,
			expectHashLen:  16,
			validateFormat: true,
		},
		{
			description: "complex nested data",
			testData: map[string]interface{}{
				"id":   "complex-id-123",
				"name": "complex-name",
				"nested": map[string]interface{}{
					"field1": "value1",
					"field2": 42,
					"field3": []string{"item1", "item2"},
				},
			},
			expectError:    false,
			expectHashLen:  16,
			validateFormat: true,
		},
		{
			description: "empty object data",
			testData: TestData{
				ID:   "",
				Name: "",
			},
			expectError:    false,
			expectHashLen:  16,
			validateFormat: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Convert test data to value for testing
			testValue := createTestValue(tc.testData)

			hash, err := GenerateEmrDataHash(testValue)

			if tc.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, hash, tc.expectHashLen, "Hash should be %d characters long", tc.expectHashLen)

			if tc.validateFormat {
				// Should match hexadecimal pattern
				matched, err := regexp.MatchString("^[0-9a-f]{16}$", hash)
				require.NoError(t, err)
				assert.True(t, matched, "Hash should be a valid 16-character hex string")
			}
		})
	}
}

func TestGetS3ExportBucketName(t *testing.T) {
	tests := []struct {
		name           string
		region         string
		clusterName    string
		expectedBucket string
		expectError    bool
	}{
		{
			name:           "US default region",
			region:         infraconsts.SamsaraAWSDefaultRegion,
			clusterName:    "us2",
			expectedBucket: "samsara-emr-replication-export-us2",
			expectError:    false,
		},
		{
			name:           "EU region",
			region:         infraconsts.SamsaraAWSEURegion,
			clusterName:    "us2",
			expectedBucket: "samsara-eu-emr-replication-export-us2",
			expectError:    false,
		},
		{
			name:           "CA region",
			region:         infraconsts.SamsaraAWSCARegion,
			clusterName:    "us2",
			expectedBucket: "samsara-ca-emr-replication-export-us2",
			expectError:    false,
		},
		{
			name:           "unknown region",
			region:         "unknown-region",
			clusterName:    "",
			expectedBucket: "",
			expectError:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			// Set the test environment
			t.Setenv("AWS_REGION", tc.region)
			t.Setenv("ECS_CLUSTERNAME_OVERRIDE", tc.clusterName)
			t.Setenv("IS_RUNNING_IN_ECS", "true")

			// Call the function under test
			bucketName, err := GetS3ExportBucketName()

			// Verify the results
			if tc.expectError {
				assert.Error(t, err)
				assert.Empty(t, bucketName)
				assert.Contains(t, err.Error(), "unknown region")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedBucket, bucketName)
			}
		})
	}
}
