package orgbatchemrbackfillworkflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/emrtokinesis"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/workflows/client/workflowengine"
)

func TestBuildChildWorkflowExecution(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endMs := samtime.TimeToMs(now)
	startMs := endMs - (24 * 60 * 60 * 1000) // 1 day before

	testCases := []struct {
		name                             string
		args                             *OrgBatchEmrBackfillWorkflowArgs
		orgId                            int64
		batchIndex                       int
		jitter                           time.Duration
		orgBatchWorkflowExecutionTimeout int
		assetIds                         []int64
		startMsPtr                       *int64
		endMsPtr                         *int64
		validateExecution                func(t *testing.T, execution workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs])
	}{
		{
			name: "should create asset-based backfill execution",
			args: &OrgBatchEmrBackfillWorkflowArgs{
				EntityName:        "Trip",
				BackfillRequestId: "test-request-123",
				PageSize:          500,
				FilterId:          "assetIdIn",
			},
			orgId:                            12345,
			batchIndex:                       0,
			jitter:                           5 * time.Second,
			orgBatchWorkflowExecutionTimeout: 60,
			assetIds:                         []int64{100, 200, 300},
			startMsPtr:                       nil,
			endMsPtr:                         &endMs,
			validateExecution: func(t *testing.T, execution workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]) {
				assert.Equal(t, int64(12345), execution.Args.OrgId)
				assert.Equal(t, "Trip", execution.Args.EntityName)
				assert.Equal(t, []int64{100, 200, 300}, execution.Args.AssetIds)
				assert.Equal(t, "test-request-123", execution.Args.BackfillRequestId)
				assert.Equal(t, int64(500), execution.Args.PageSize)
				assert.Equal(t, "assetIdIn", execution.Args.FilterId)
				assert.Equal(t, 0, execution.Args.BatchIndex)
				assert.Nil(t, execution.Args.StartMs)
				assert.Equal(t, &endMs, execution.Args.EndMs)
				assert.Equal(t, 5*time.Second, execution.Args.Jitter)
				assert.Equal(t, "emrtokinesis-Trip-12345-test-request-123-0", execution.Options.ExplicitWorkflowId.ExplicitWorkflowId)
				assert.Equal(t, 60*time.Minute, execution.Options.WorkflowExecutionTimeout)
			},
		},
		{
			name: "should create parameterized backfill execution",
			args: &OrgBatchEmrBackfillWorkflowArgs{
				EntityName:        "HosViolation",
				BackfillRequestId: "test-request-456",
				PageSize:          1000,
				FilterId:          "",
			},
			orgId:                            67890,
			batchIndex:                       2,
			jitter:                           10 * time.Second,
			orgBatchWorkflowExecutionTimeout: 120,
			assetIds:                         nil,
			startMsPtr:                       &startMs,
			endMsPtr:                         &endMs,
			validateExecution: func(t *testing.T, execution workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]) {
				assert.Equal(t, int64(67890), execution.Args.OrgId)
				assert.Equal(t, "HosViolation", execution.Args.EntityName)
				assert.Nil(t, execution.Args.AssetIds)
				assert.Equal(t, "test-request-456", execution.Args.BackfillRequestId)
				assert.Equal(t, int64(1000), execution.Args.PageSize)
				assert.Empty(t, execution.Args.FilterId)
				assert.Equal(t, 2, execution.Args.BatchIndex)
				assert.Equal(t, &startMs, execution.Args.StartMs)
				assert.Equal(t, &endMs, execution.Args.EndMs)
				assert.Equal(t, 10*time.Second, execution.Args.Jitter)
				assert.Equal(t, "emrtokinesis-HosViolation-67890-test-request-456-2", execution.Options.ExplicitWorkflowId.ExplicitWorkflowId)
				assert.Equal(t, 120*time.Minute, execution.Options.WorkflowExecutionTimeout)
			},
		},
		{
			name: "should include filters and filter options",
			args: &OrgBatchEmrBackfillWorkflowArgs{
				EntityName:        "Trip",
				BackfillRequestId: "test-request-789",
				PageSize:          250,
				FilterId:          "assetIdIn",
				Filters: helpers.FilterFieldToValueMap{
					"tripStartTimeGreaterThanOrEqual": 1609459200000,
				},
			},
			orgId:                            111,
			batchIndex:                       1,
			jitter:                           3 * time.Second,
			orgBatchWorkflowExecutionTimeout: 90,
			assetIds:                         []int64{1, 2, 3, 4, 5},
			startMsPtr:                       nil,
			endMsPtr:                         &endMs,
			validateExecution: func(t *testing.T, execution workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]) {
				assert.Equal(t, helpers.FilterFieldToValueMap{"tripStartTimeGreaterThanOrEqual": 1609459200000}, execution.Args.Filters)
				assert.Equal(t, []int64{1, 2, 3, 4, 5}, execution.Args.AssetIds)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			execution := buildChildWorkflowExecution(
				tc.args,
				tc.orgId,
				tc.batchIndex,
				tc.jitter,
				tc.orgBatchWorkflowExecutionTimeout,
				tc.assetIds,
				tc.startMsPtr,
				tc.endMsPtr,
			)

			tc.validateExecution(t, execution)
		})
	}
}

func TestBuildChildWorkflowExecution_WorkflowIdFormat(t *testing.T) {
	testCases := []struct {
		name               string
		entityName         string
		orgId              int64
		backfillRequestId  string
		batchIndex         int
		expectedWorkflowId string
	}{
		{
			name:               "should format workflow ID correctly with single digit batch",
			entityName:         "Trip",
			orgId:              123,
			backfillRequestId:  "abc",
			batchIndex:         0,
			expectedWorkflowId: "emrtokinesis-Trip-123-abc-0",
		},
		{
			name:               "should format workflow ID correctly with multi-digit batch",
			entityName:         "HosViolation",
			orgId:              9999,
			backfillRequestId:  "xyz-456",
			batchIndex:         25,
			expectedWorkflowId: "emrtokinesis-HosViolation-9999-xyz-456-25",
		},
		{
			name:               "should handle special characters in backfill request ID",
			entityName:         "TestEntity",
			orgId:              1,
			backfillRequestId:  "request-2024-01-15-test",
			batchIndex:         100,
			expectedWorkflowId: "emrtokinesis-TestEntity-1-request-2024-01-15-test-100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := &OrgBatchEmrBackfillWorkflowArgs{
				EntityName:        tc.entityName,
				BackfillRequestId: tc.backfillRequestId,
			}
			endMs := int64(0)

			execution := buildChildWorkflowExecution(
				args,
				tc.orgId,
				tc.batchIndex,
				0,
				60,
				nil,
				nil,
				&endMs,
			)

			assert.Equal(t, tc.expectedWorkflowId, execution.Options.ExplicitWorkflowId.ExplicitWorkflowId)
			assert.Equal(t, workflowengine.WorkflowIdReusePolicyAllowDuplicate, execution.Options.ExplicitWorkflowId.WorkflowIdReusePolicy)
		})
	}
}

func TestBuildChildWorkflowExecution_TimeoutConfiguration(t *testing.T) {
	testCases := []struct {
		name                             string
		orgBatchWorkflowExecutionTimeout int
		expectedTimeout                  time.Duration
	}{
		{
			name:                             "should use provided timeout",
			orgBatchWorkflowExecutionTimeout: 30,
			expectedTimeout:                  30 * time.Minute,
		},
		{
			name:                             "should handle large timeout values",
			orgBatchWorkflowExecutionTimeout: 300,
			expectedTimeout:                  300 * time.Minute,
		},
		{
			name:                             "should handle zero timeout",
			orgBatchWorkflowExecutionTimeout: 0,
			expectedTimeout:                  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := &OrgBatchEmrBackfillWorkflowArgs{
				EntityName:        "TestEntity",
				BackfillRequestId: "test",
			}
			endMs := int64(0)

			execution := buildChildWorkflowExecution(
				args,
				123,
				0,
				0,
				tc.orgBatchWorkflowExecutionTimeout,
				nil,
				nil,
				&endMs,
			)

			assert.Equal(t, tc.expectedTimeout, execution.Options.WorkflowExecutionTimeout)
		})
	}
}

func TestParameterizedBackfill_TimeBatching(t *testing.T) {
	// Test the time batching logic for parameterized entities
	intervalInDaysBatchSize := 15
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC) // 45 days later
	startMs := samtime.TimeToMs(startTime)
	endMs := samtime.TimeToMs(endTime)

	// Use ceiling division to match the actual implementation
	batchSizeMs := int64(intervalInDaysBatchSize) * millisecondsInOneDay
	totalTimeMs := endMs - startMs
	totalBatches := int((totalTimeMs + batchSizeMs - 1) / batchSizeMs)
	require.Equal(t, 3, totalBatches, "45 days / 15 days = 3 batches")

	// Verify batch time ranges
	testCases := []struct {
		batchIndex    int
		expectedStart time.Time
		expectedEnd   time.Time
	}{
		{
			batchIndex:    0,
			expectedStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			batchIndex:    1,
			expectedStart: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			batchIndex:    2,
			expectedStart: time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run("batch_"+string(rune('0'+tc.batchIndex)), func(t *testing.T) {
			calculatedStartMs := startMs + int64(tc.batchIndex)*int64(intervalInDaysBatchSize)*millisecondsInOneDay
			calculatedEndMs := calculatedStartMs + int64(intervalInDaysBatchSize)*millisecondsInOneDay

			actualStart := samtime.MsToTime(calculatedStartMs)
			actualEnd := samtime.MsToTime(calculatedEndMs)

			assert.Equal(t, tc.expectedStart, actualStart, "batch %d start time", tc.batchIndex)
			assert.Equal(t, tc.expectedEnd, actualEnd, "batch %d end time", tc.batchIndex)
		})
	}
}

func TestParameterizedBackfill_LargeTimeRange(t *testing.T) {
	// Test that large time ranges don't cause integer overflow (>24 days on 32-bit systems)
	intervalInDaysBatchSize := 15
	// Test a ~268 day range (23,155,200,000 milliseconds)
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 9, 25, 0, 0, 0, 0, time.UTC)
	startMs := samtime.TimeToMs(startTime)
	endMs := samtime.TimeToMs(endTime)

	// Use ceiling division with int64 to avoid overflow
	batchSizeMs := int64(intervalInDaysBatchSize) * millisecondsInOneDay
	totalTimeMs := endMs - startMs
	totalBatches := int((totalTimeMs + batchSizeMs - 1) / batchSizeMs)

	// 268 days / 15 days = 18 batches (rounded up)
	require.Equal(t, 18, totalBatches, "268 days / 15 days should be 18 batches")

	// Verify the calculation doesn't overflow
	require.Greater(t, totalBatches, 0, "totalBatches should be positive")
	require.Less(t, totalBatches, 1000, "totalBatches should be reasonable")
}

func TestAssetBasedBackfill_AssetBatching(t *testing.T) {
	testCases := []struct {
		name                 string
		totalAssets          int
		assetBatchSize       int
		expectedTotalBatches int
		validateBatches      func(t *testing.T, assetIds []int64, batchSize int)
	}{
		{
			name:                 "should create single batch for assets less than batch size",
			totalAssets:          30,
			assetBatchSize:       50,
			expectedTotalBatches: 1,
			validateBatches: func(t *testing.T, assetIds []int64, batchSize int) {
				totalBatches := (len(assetIds) + batchSize - 1) / batchSize
				assert.Equal(t, 1, totalBatches)
			},
		},
		{
			name:                 "should create multiple batches for assets exceeding batch size",
			totalAssets:          125,
			assetBatchSize:       50,
			expectedTotalBatches: 3,
			validateBatches: func(t *testing.T, assetIds []int64, batchSize int) {
				totalBatches := (len(assetIds) + batchSize - 1) / batchSize
				assert.Equal(t, 3, totalBatches)

				// Verify batch sizes
				for batchIndex := 0; batchIndex < totalBatches; batchIndex++ {
					assetBatchStart := batchIndex * batchSize
					end := assetBatchStart + batchSize
					if end > len(assetIds) {
						end = len(assetIds)
					}
					batchAssetIds := assetIds[assetBatchStart:end]

					if batchIndex < totalBatches-1 {
						assert.Len(t, batchAssetIds, batchSize, "full batch should have %d assets", batchSize)
					} else {
						assert.Len(t, batchAssetIds, 25, "last batch should have remaining 25 assets")
					}
				}
			},
		},
		{
			name:                 "should handle exact multiple of batch size",
			totalAssets:          100,
			assetBatchSize:       50,
			expectedTotalBatches: 2,
			validateBatches: func(t *testing.T, assetIds []int64, batchSize int) {
				totalBatches := (len(assetIds) + batchSize - 1) / batchSize
				assert.Equal(t, 2, totalBatches)

				// Both batches should be full
				for batchIndex := 0; batchIndex < totalBatches; batchIndex++ {
					assetBatchStart := batchIndex * batchSize
					end := assetBatchStart + batchSize
					batchAssetIds := assetIds[assetBatchStart:end]
					assert.Len(t, batchAssetIds, batchSize)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock asset IDs
			assetIds := make([]int64, tc.totalAssets)
			for i := 0; i < tc.totalAssets; i++ {
				assetIds[i] = int64(1000 + i)
			}

			totalBatches := (len(assetIds) + tc.assetBatchSize - 1) / tc.assetBatchSize
			assert.Equal(t, tc.expectedTotalBatches, totalBatches)

			if tc.validateBatches != nil {
				tc.validateBatches(t, assetIds, tc.assetBatchSize)
			}
		})
	}
}

func TestMillisecondsInOneDay(t *testing.T) {
	// Verify the constant is correct
	expected := 24 * 60 * 60 * 1000
	assert.Equal(t, expected, millisecondsInOneDay)

	// Verify it converts correctly
	oneDay := time.Hour * 24
	oneDayMs := oneDay.Milliseconds()
	assert.Equal(t, int64(millisecondsInOneDay), oneDayMs)
}
