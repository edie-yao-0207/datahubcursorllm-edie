package orgbatchemrbackfillworkflow

import (
	"fmt"
	"time"

	"go.uber.org/fx"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/emrtokinesis"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/orgbatchemrbackfillworkflow/activity"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/workflows"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/infra/workflows/workflowsplatformconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team/teamnames"
)

func init() {
	workflowregistry.MustRegisterWorkflowConstructor(NewOrgBatchEmrBackfillWorkflow)
}

type OrgBatchEmrBackfillWorkflowParams struct {
	fx.In
	GetAllAssetIdsActivity        *activity.GetAllAssetIdsActivity
	WriteBackfillMetadataActivity *activity.WriteBackfillMetadataActivity
}

type OrgBatchEmrBackfillWorkflowArgs struct {
	EntityName string
	// OrgIds is an optional list of org IDs to filter the data by. If not set,
	// all orgs will be included.
	OrgIds          []int64
	AssetIdsByOrgId *map[int64][]int64
	// ExcludeOrgIds is an optional list of org IDs to exclude from the data. If
	// not set, no orgs will be excluded.
	ExcludeOrgIds *[]int64
	// StartMs is the timestamp in milliseconds to start querying from. If not
	// set, no time-based filtering will be applied.
	StartMs *int64
	// EndMs is the timestamp in milliseconds to end querying at. If not set, no time-based filtering will be applied.
	EndMs *int64
	// Jitter is the number of seconds to add before we query for EMR data. This
	// prevents us from hitting the EMR API with a burst of concurrent requests.
	Jitter *int

	OrgJitter time.Duration

	// AssetBatchSize is the number of asset IDs to process in a single batch.
	AssetBatchSize *int

	// IntervalInDaysBatchSize is the window of time in days to process in a single batch.
	IntervalInDaysBatchSize *int

	// NumConcurrentEmrToKinesisExecutions is the number of emrtokinesis child
	// workflows to run concurrently.
	NumConcurrentEmrToKinesisExecutions *int

	// EmrToKinesisWorkflowTimeoutMinutes is the timeout for the emrtokinesis
	// child workflow.
	EmrToKinesisWorkflowTimeoutMinutes *int

	// EmrToKinesisWorkflowMaximumAttempts is the maximum number of attempts to
	// retry the emrtokinesis child workflow.
	EmrToKinesisWorkflowMaximumAttempts *int32

	// OrgBatchWorkflowExecutionTimeoutMinutes is the timeout for the
	// orgbatchemrbackfill child workflow.
	OrgBatchWorkflowExecutionTimeoutMinutes *int

	// Filters is a map of filter fields to their values. The keys here
	// should match the YAML file's listRecordsSpec.filterOptions.field values.

	// For example, if we want to backfill all trips with a startTime > June 1,
	// 2025, we would set this:
	//
	// Filters: helpers.FilterFieldToValueMap{
	// 	"tripStartTimeGreaterThanOrEqual": 1748761200000,
	// }
	Filters helpers.FilterFieldToValueMap

	FilterOptions []helpers.FilterOptions

	PageSize int64

	AssetType emrreplication.AssetType

	FilterId string

	BackfillRequestId string
}

var OrgBatchEmrBackfillWorkflowDefinition = workflows.CreateWorkflowDefinition[OrgBatchEmrBackfillWorkflowArgs]("org_batch_emr_backfill", teamnames.DataPlatform, workflowsplatformconsts.Emrbackfillworkflowsworker)

type OrgBatchEmrBackfillWorkflow struct {
	workflowengine.WorkflowImpl

	GetAllAssetIdsActivity        *activity.GetAllAssetIdsActivity
	WriteBackfillMetadataActivity *activity.WriteBackfillMetadataActivity
}

func NewOrgBatchEmrBackfillWorkflow(p OrgBatchEmrBackfillWorkflowParams) *OrgBatchEmrBackfillWorkflow {
	return &OrgBatchEmrBackfillWorkflow{
		GetAllAssetIdsActivity:        p.GetAllAssetIdsActivity,
		WriteBackfillMetadataActivity: p.WriteBackfillMetadataActivity,
	}
}

func (m OrgBatchEmrBackfillWorkflow) Definition() workflows.WorkflowDefinition[OrgBatchEmrBackfillWorkflowArgs] {
	return OrgBatchEmrBackfillWorkflowDefinition
}

func (m OrgBatchEmrBackfillWorkflow) SupportedVersionFlags() []workflowengine.FlagKey {
	return []workflowengine.FlagKey{}
}

const millisecondsInOneDay = 24 * 60 * 60 * 1000

// executeChildWorkflows executes a list of child workflows in batches and returns whether any failed.
func executeChildWorkflows(
	engine workflowengine.WorkflowEngine,
	childWorkflowExecutions []workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs],
	numConcurrentExecutions int,
	totalBatches int,
) bool {
	// Set default number of concurrent executions.
	if numConcurrentExecutions <= 0 {
		numConcurrentExecutions = 10
	}
	// Take the minimum of numConcurrentExecutions and total number of batches.
	numConcurrentExecutions = min(numConcurrentExecutions, totalBatches)
	// Execute them in batches of numConcurrentExecutions.
	batches := helpers.BatchChildWorkflowExecutions(childWorkflowExecutions, numConcurrentExecutions)
	orgBackfillFailed := false
	for _, batch := range batches {
		childWorkflowErrors := workflowengine.BatchExecuteVoidChildWorkflow(engine, emrtokinesis.EmrToKinesisWorkflowDefinition, batch)
		for _, childWorkflowError := range childWorkflowErrors {
			if childWorkflowError != nil {
				orgBackfillFailed = true
				break
			}
		}
	}
	return orgBackfillFailed
}

// buildChildWorkflowExecution creates a child workflow execution with common configuration.
func buildChildWorkflowExecution(
	args *OrgBatchEmrBackfillWorkflowArgs,
	orgId int64,
	batchIndex int,
	jitter time.Duration,
	orgBatchWorkflowExecutionTimeout int,
	assetIds []int64,
	startMs *int64,
	endMs *int64,
) workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs] {
	return workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]{
		Args: &emrtokinesis.EmrToKinesisWorkflowArgs{
			OrgId:                               orgId,
			EntityName:                          args.EntityName,
			AssetIds:                            assetIds,
			PageSize:                            args.PageSize,
			StartMs:                             startMs,
			EndMs:                               endMs,
			BackfillRequestId:                   args.BackfillRequestId,
			Jitter:                              jitter,
			EmrToKinesisWorkflowTimeoutMinutes:  args.EmrToKinesisWorkflowTimeoutMinutes,
			EmrToKinesisWorkflowMaximumAttempts: args.EmrToKinesisWorkflowMaximumAttempts,
			Filters:                             args.Filters,
			FilterOptions:                       args.FilterOptions,
			BatchIndex:                          batchIndex,
			FilterId:                            args.FilterId,
		},
		Options: workflowengine.ChildWorkflowOptions{
			ExplicitWorkflowId: workflowengine.ExplicitWorkflowId{
				ExplicitWorkflowId:    fmt.Sprintf("emrtokinesis-%s-%d-%s-%d", args.EntityName, orgId, args.BackfillRequestId, batchIndex),
				WorkflowIdReusePolicy: workflowengine.WorkflowIdReusePolicyAllowDuplicate,
			},
			WorkflowExecutionTimeout: time.Minute * time.Duration(orgBatchWorkflowExecutionTimeout),
		},
	}
}

func (m OrgBatchEmrBackfillWorkflow) Execute(engine workflowengine.WorkflowEngine, args *OrgBatchEmrBackfillWorkflowArgs) error {
	selector := engine.Selector()
	// Attach a timer to the workflow.
	selector.Timer(args.OrgJitter, nil)
	// Block execution of workflow until Timer fires.
	selector.Select()

	// Track failed org IDs in deterministic order
	var failedOrgIds []int64
	// Set an upper bound on this workflow to 5 hours.
	orgBatchWorkflowExecutionTimeout := 5 * 60
	if args.OrgBatchWorkflowExecutionTimeoutMinutes != nil {
		orgBatchWorkflowExecutionTimeout = *args.OrgBatchWorkflowExecutionTimeoutMinutes
	}
	// Calculate jitter
	maxJitter := 30
	if args.Jitter != nil {
		maxJitter = *args.Jitter
	}

	for _, orgId := range args.OrgIds {
		// This is for asset-based backfills
		if args.FilterId != "" {
			// Get all asset IDs for this org.
			var assetIds []int64
			// here
			if args.AssetIdsByOrgId != nil && (*args.AssetIdsByOrgId)[orgId] != nil {
				assetIds = (*args.AssetIdsByOrgId)[orgId]
			} else {
				getAllAssetIdsResult, err := workflowengine.ExecuteActivity(engine,
					m.GetAllAssetIdsActivity,
					&activity.GetAllAssetIdsActivityArgs{
						OrgId:     orgId,
						AssetType: args.AssetType,
					}, &workflowengine.ActivityOptions{
						StartToCloseTimeout: pointer.New(time.Minute * 1),
					},
				)
				if err != nil {
					return workflowengine.WrapActivityErrorf(err, "failed to get all asset ids for org %d", orgId)
				}

				assetIds = getAllAssetIdsResult.AssetIds
			}
			assetBatchSize := 50
			if args.AssetBatchSize != nil {
				assetBatchSize = *args.AssetBatchSize
			}
			totalBatches := (len(assetIds) + assetBatchSize - 1) / assetBatchSize // Ceiling division

			// First, collect all child workflow executions
			var childWorkflowExecutions []workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]
			for batchIndex := 0; batchIndex < totalBatches; batchIndex++ {
				assetBatchStart := batchIndex * assetBatchSize
				end := assetBatchStart + assetBatchSize
				if end > len(assetIds) {
					end = len(assetIds)
				}
				batchAssetIds := assetIds[assetBatchStart:end]

				jitter := helpers.CalculateBatchJitter(batchIndex, totalBatches, maxJitter)
				endMs := samtime.TimeToMs(engine.Now())
				if args.EndMs != nil {
					endMs = *args.EndMs
				}

				childWorkflowExecutions = append(childWorkflowExecutions,
					buildChildWorkflowExecution(args, orgId, batchIndex, jitter, orgBatchWorkflowExecutionTimeout, batchAssetIds, args.StartMs, &endMs))
			}

			// Execute all child workflows and track failures.
			numConcurrentExecutions := 0
			if args.NumConcurrentEmrToKinesisExecutions != nil {
				numConcurrentExecutions = *args.NumConcurrentEmrToKinesisExecutions
			}
			if executeChildWorkflows(engine, childWorkflowExecutions, numConcurrentExecutions, totalBatches) {
				failedOrgIds = append(failedOrgIds, orgId)
			}

		} else {
			// This is for parameterized backfills
			var childWorkflowExecutions []workflowengine.ChildWorkflowExecution[emrtokinesis.EmrToKinesisWorkflowArgs]
			intervalInDaysBatchSize := 15
			if args.IntervalInDaysBatchSize != nil {
				intervalInDaysBatchSize = *args.IntervalInDaysBatchSize
			}

			// Validate required time range parameters for parameterized backfills
			if args.StartMs == nil || args.EndMs == nil {
				return workflowengine.WrapActivityErrorf(nil, "parameterized backfill requires both StartMs and EndMs for org %d", orgId)
			}

			batchSizeMs := int64(intervalInDaysBatchSize) * millisecondsInOneDay
			totalTimeMs := *args.EndMs - *args.StartMs
			totalBatches := int((totalTimeMs + batchSizeMs - 1) / batchSizeMs) // Ceiling division

			for batchIndex := 0; batchIndex < totalBatches; batchIndex++ {
				startMs := *args.StartMs + int64(batchIndex)*int64(intervalInDaysBatchSize)*millisecondsInOneDay
				endMs := startMs + int64(intervalInDaysBatchSize)*millisecondsInOneDay
				// Cap endMs to not exceed the intended end time
				if endMs > *args.EndMs {
					endMs = *args.EndMs
				}
				jitter := helpers.CalculateBatchJitter(batchIndex, totalBatches, maxJitter)

				childWorkflowExecutions = append(childWorkflowExecutions,
					buildChildWorkflowExecution(args, orgId, batchIndex, jitter, orgBatchWorkflowExecutionTimeout, nil, &startMs, &endMs))
			}

			// Execute all child workflows and track failures.
			numConcurrentExecutions := 0
			if args.NumConcurrentEmrToKinesisExecutions != nil {
				numConcurrentExecutions = *args.NumConcurrentEmrToKinesisExecutions
			}
			if executeChildWorkflows(engine, childWorkflowExecutions, numConcurrentExecutions, totalBatches) {
				failedOrgIds = append(failedOrgIds, orgId)
			}
		}
	}

	// Write backfill metadata once at the end if there are any failures.
	if len(failedOrgIds) > 0 {
		_, err := workflowengine.ExecuteActivity(engine,
			m.WriteBackfillMetadataActivity,
			&activity.WriteBackfillMetadataActivityArgs{
				BackfillRequestId: args.BackfillRequestId,
				EntityName:        args.EntityName,
				FailedOrgIds:      failedOrgIds,
			}, &workflowengine.ActivityOptions{
				StartToCloseTimeout: pointer.New(time.Minute * 5),
			},
		)
		if err != nil {
			return workflowengine.WrapActivityErrorf(err, "failed to write org completion metadata for orgs %v", failedOrgIds)
		}
	}

	return nil
}
