package emrbackfillworkflow

import (
	"fmt"

	"go.uber.org/fx"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/activity"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/orgbatchemrbackfillworkflow"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/workflows"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/infra/workflows/workflowsplatformconsts"
	"samsaradev.io/team/teamnames"
)

func init() {
	workflowregistry.MustRegisterWorkflowConstructor(NewEmrBackfillWorkflow)
}

type EmrBackfillWorkflowParams struct {
	fx.In
	ExtractFilterOptionsFromYamlActivity *activity.ExtractFilterOptionsFromYamlActivity
	GetAllOrganizationsActivity          *activity.GetAllOrganizationsActivity
	UpdateBackfillCheckpointFile         *activity.UpdateBackfillCheckpointFile
}

type EmrBackfillWorkflowArgs struct {
	EntityName string
	// OrgIds is an optional list of org IDs to filter the data by. If not set,
	// all orgs will be included.
	OrgIds *[]int64
	// AssetIdsByOrgId is an optional map of org IDs to asset IDs to filter the
	// data by. If not set, all assets will be included.
	AssetIdsByOrgId *map[int64][]int64
	// ExcludeOrgIds is an optional list of org IDs to exclude from the data. If
	// not set, no orgs will be excluded.
	ExcludeOrgIds *[]int64
	// StartMs is the timestamp in milliseconds to start querying from. If not
	// set, no time-based filtering will be applied.
	StartMs *int64
	// EndMs is the timestamp in milliseconds to end querying at. If not set, no
	// time-based filtering will be applied.
	EndMs *int64

	// Jitter is the number of seconds to add before we query for EMR data. This
	// prevents us from hitting the EMR API with a burst of concurrent requests.
	Jitter *int

	// AssetBatchSize is the number of asset IDs to process in a single batch.
	AssetBatchSize *int

	// IntervalInDaysBatchSize is the window of time in days to process in a
	// single batch.
	IntervalInDaysBatchSize *int

	// OrgBatchSize is the number of org IDs to process in a single batch.
	OrgBatchSize *int

	// NumConcurrentEmrToKinesisExecutions is the number of emrtokinesis child
	// workflows to run concurrently.
	NumConcurrentEmrToKinesisExecutions *int

	// NumConcurrentOrgBatchesExecutions is the number of orgbatchemrbackfill
	// child workflows to run concurrently.
	NumConcurrentOrgBatchesExecutions *int

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
}

type EmrBackfillWorkflow struct {
	workflowengine.WorkflowImpl

	ExtractFilterOptionsFromYamlActivity *activity.ExtractFilterOptionsFromYamlActivity
	GetAllOrganizationsActivity          *activity.GetAllOrganizationsActivity
	UpdateBackfillCheckpointFile         *activity.UpdateBackfillCheckpointFile
}

func NewEmrBackfillWorkflow(p EmrBackfillWorkflowParams) *EmrBackfillWorkflow {
	return &EmrBackfillWorkflow{
		ExtractFilterOptionsFromYamlActivity: p.ExtractFilterOptionsFromYamlActivity,
		GetAllOrganizationsActivity:          p.GetAllOrganizationsActivity,
		UpdateBackfillCheckpointFile:         p.UpdateBackfillCheckpointFile,
	}
}

var EmrBackfillWorkflowDefinition = workflows.CreateWorkflowDefinition[EmrBackfillWorkflowArgs]("emr_backfill", teamnames.DataPlatform, workflowsplatformconsts.Emrbackfillworkflowsworker)

func (EmrBackfillWorkflow) Definition() workflows.WorkflowDefinition[EmrBackfillWorkflowArgs] {
	return EmrBackfillWorkflowDefinition
}

func (EmrBackfillWorkflow) SupportedVersionFlags() []workflowengine.FlagKey {
	return []workflowengine.FlagKey{}
}

// getPageSizeFromRegistry looks up the page size for an entity in the EMR
// replication registry. Returns nil if the entity is not found or has no page
// size override.
func getPageSizeFromRegistry(entityName string, getAllSpecs func() []emrreplication.EmrReplicationSpec) int64 {
	for _, spec := range getAllSpecs() {
		if spec.Name == entityName {
			if spec.InternalOverrides != nil && spec.InternalOverrides.PageSize != 0 {
				return spec.InternalOverrides.PageSize
			}
			break
		}
	}
	return 500
}

func getYamlPathFromRegistry(entityName string, getAllSpecs func() []emrreplication.EmrReplicationSpec) string {
	for _, spec := range getAllSpecs() {
		if spec.Name == entityName {
			return spec.YamlPath
		}
	}
	return ""
}

func getAssetTypeFromRegistry(entityName string, getAllSpecs func() []emrreplication.EmrReplicationSpec) emrreplication.AssetType {
	for _, spec := range getAllSpecs() {
		if spec.Name == entityName {
			return spec.AssetType
		}
	}
	return ""
}

func getFilterIdFromRegistry(entityName string, getAllSpecs func() []emrreplication.EmrReplicationSpec) string {
	for _, spec := range getAllSpecs() {
		if spec.Name == entityName {
			return spec.FilterId
		}
	}
	return ""
}

func (m EmrBackfillWorkflow) Execute(engine workflowengine.WorkflowEngine, args *EmrBackfillWorkflowArgs) error {
	currentWorkflowId := engine.CurrentWorkflowId()
	logger := engine.GetLogger()
	backfillStartTime := samtime.TimeToMs(engine.Now())

	pageSize := getPageSizeFromRegistry(args.EntityName, emrreplication.GetAllEmrReplicationSpecs)
	assetType := getAssetTypeFromRegistry(args.EntityName, emrreplication.GetAllEmrReplicationSpecs)
	yamlPath := getYamlPathFromRegistry(args.EntityName, emrreplication.GetAllEmrReplicationSpecs)
	filterId := getFilterIdFromRegistry(args.EntityName, emrreplication.GetAllEmrReplicationSpecs)
	logger.Info("starting EMR backfill workflow",
		"backfillRequestId", currentWorkflowId,
		"entityName", args.EntityName,
		"pageSize", pageSize,
		"assetType", assetType,
		"yamlPath", yamlPath,
		"filterId", filterId,
	)

	filterOptions := make([]helpers.FilterOptions, 0)
	if args.Filters != nil {
		// Validate input filters and extract filter options from YAML using an
		// activity. Validating input filters require iterating over a map, which is
		// non-deterministic. The yaml.v3 package is also non-deterministic.
		// Non-deterministic behaviors aren't allowed in a workflow. We extract both
		// pieces of logic to a singular activity since:
		// 1. activities are allowed to be non-deterministic
		// 2. it's ideal to keep the number of activities to a minimum
		extractFilterOptionsResult, err := workflowengine.ExecuteActivity(engine,
			m.ExtractFilterOptionsFromYamlActivity,
			&activity.ExtractFilterOptionsFromYamlActivityArgs{
				Path:    yamlPath,
				Filters: args.Filters,
			},
		)
		if err != nil {
			return workflowengine.WrapActivityErrorf(err, "failed to extract filter options from YAML")
		}
		filterOptions = extractFilterOptionsResult.FilterOptions
	}

	getAllOrganizationsResult, err := workflowengine.ExecuteActivity(engine,
		m.GetAllOrganizationsActivity,
		&activity.GetAllOrganizationsActivityArgs{
			OrgIds:        args.OrgIds,
			ExcludeOrgIds: args.ExcludeOrgIds,
		},
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to get all org ids")
	}

	orgBatchSize := 500
	if args.OrgBatchSize != nil {
		orgBatchSize = *args.OrgBatchSize
	}
	orgIds := getAllOrganizationsResult.OrgIds
	totalOrgBatches := (len(orgIds) + orgBatchSize - 1) / orgBatchSize // Ceiling division
	batchOrgIndex := 0

	var orgBatchExecutions []workflowengine.ChildWorkflowExecution[orgbatchemrbackfillworkflow.OrgBatchEmrBackfillWorkflowArgs]
	for orgBatchStart := 0; orgBatchStart < len(orgIds); orgBatchStart += orgBatchSize {
		end := orgBatchStart + orgBatchSize
		if end > len(orgIds) {
			end = len(orgIds)
		}
		batchOrgIds := orgIds[orgBatchStart:end]
		// Calculate jitter for this batch (max 30 seconds)
		maxJitter := 30
		if args.Jitter != nil {
			maxJitter = *args.Jitter
		}
		orgJitter := helpers.CalculateBatchJitter(batchOrgIndex, totalOrgBatches, maxJitter)

		orgBatchExecutions = append(orgBatchExecutions, workflowengine.ChildWorkflowExecution[orgbatchemrbackfillworkflow.OrgBatchEmrBackfillWorkflowArgs]{
			Args: &orgbatchemrbackfillworkflow.OrgBatchEmrBackfillWorkflowArgs{
				OrgIds:                                  batchOrgIds,
				AssetIdsByOrgId:                         args.AssetIdsByOrgId,
				EntityName:                              args.EntityName,
				StartMs:                                 args.StartMs,
				EndMs:                                   args.EndMs,
				Filters:                                 args.Filters,
				FilterOptions:                           filterOptions,
				AssetType:                               assetType,
				PageSize:                                pageSize,
				FilterId:                                filterId,
				AssetBatchSize:                          args.AssetBatchSize,
				IntervalInDaysBatchSize:                 args.IntervalInDaysBatchSize,
				Jitter:                                  args.Jitter,
				NumConcurrentEmrToKinesisExecutions:     args.NumConcurrentEmrToKinesisExecutions,
				EmrToKinesisWorkflowTimeoutMinutes:      args.EmrToKinesisWorkflowTimeoutMinutes,
				EmrToKinesisWorkflowMaximumAttempts:     args.EmrToKinesisWorkflowMaximumAttempts,
				OrgBatchWorkflowExecutionTimeoutMinutes: args.OrgBatchWorkflowExecutionTimeoutMinutes,
				OrgJitter:                               orgJitter,
				BackfillRequestId:                       currentWorkflowId,
			},
			Options: workflowengine.ChildWorkflowOptions{
				ExplicitWorkflowId: workflowengine.ExplicitWorkflowId{
					ExplicitWorkflowId:    fmt.Sprintf("orgbatchemrbackfill-%s-%s-%d", args.EntityName, currentWorkflowId, batchOrgIndex),
					WorkflowIdReusePolicy: workflowengine.WorkflowIdReusePolicyAllowDuplicate,
				},
			},
		})
		batchOrgIndex++
	}

	// Set default number of concurrent executions.
	numConcurrentExecutions := 10
	if args.NumConcurrentOrgBatchesExecutions != nil {
		numConcurrentExecutions = *args.NumConcurrentOrgBatchesExecutions
	}
	// Take the minimum of numConcurrentExecutions and total number of asset
	// batches.
	numConcurrentExecutions = min(numConcurrentExecutions, totalOrgBatches)
	// Then, execute them in batches of numConcurrentExecutions
	batches := helpers.BatchChildWorkflowExecutions(orgBatchExecutions, numConcurrentExecutions)
	for _, batch := range batches {
		childWorkflowErrors := workflowengine.BatchExecuteVoidChildWorkflow(engine, orgbatchemrbackfillworkflow.OrgBatchEmrBackfillWorkflowDefinition, batch)
		for index, childWorkflowError := range childWorkflowErrors {
			if childWorkflowError != nil {
				logger.Info("EMR Backfill: orgbatchemrbackfillworkflow failed",
					"backfillRequestId", currentWorkflowId,
					"entityName", args.EntityName,
					"orgIds", batch[index].Args.OrgIds,
					"err", childWorkflowError.Error(),
					"batchIndex", index,
				)
			}
		}
	}

	// Calculate end time in Unix milliseconds
	backfillEndTime := samtime.TimeToMs(engine.Now())

	// Update metadata.json with for this backfill's checkpoint.
	_, activityErr := workflowengine.ExecuteActivity(engine,
		m.UpdateBackfillCheckpointFile,
		&activity.UpdateBackfillCheckpointFileArgs{
			BackfillRequestId: currentWorkflowId,
			EntityName:        args.EntityName,
			BackfillStartTime: backfillStartTime,
			BackfillEndTime:   backfillEndTime,
		},
	)
	if activityErr != nil {
		return workflowengine.WrapActivityErrorf(activityErr, "failed to update backfill checkpoint file")
	}

	logger.Info("completed EMR backfill workflow",
		"backfillRequestId", currentWorkflowId,
		"entityName", args.EntityName,
	)

	return nil
}
