package emrtokinesis

import (
	"time"

	"go.uber.org/fx"

	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/emrtokinesis/activity"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/workflows"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/infra/workflows/workflowsplatformconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team/teamnames"
)

func init() {
	workflowregistry.MustRegisterWorkflowConstructor(NewEmrToKinesisWorkflow)
}

type EmrToKinesisWorkflowParams struct {
	fx.In
	StreamEmrDataToKinesisActivity *activity.StreamEmrDataToKinesisActivity
}

type EmrToKinesisWorkflowArgs struct {
	OrgId                               int64
	EntityName                          string
	AssetIds                            []int64
	StartMs                             *int64
	EndMs                               *int64
	BackfillRequestId                   string
	PageSize                            int64
	Jitter                              time.Duration
	EmrToKinesisWorkflowTimeoutMinutes  *int
	EmrToKinesisWorkflowMaximumAttempts *int32
	Filters                             helpers.FilterFieldToValueMap
	FilterOptions                       []helpers.FilterOptions
	BatchIndex                          int
	FilterId                            string
}

var EmrToKinesisWorkflowDefinition = workflows.CreateWorkflowDefinition[EmrToKinesisWorkflowArgs]("emr_to_kinesis", teamnames.DataPlatform, workflowsplatformconsts.Emrbackfillworkflowsworker)

type EmrToKinesisWorkflow struct {
	workflowengine.WorkflowImpl

	StreamEmrDataToKinesisActivity *activity.StreamEmrDataToKinesisActivity
}

func NewEmrToKinesisWorkflow(p EmrToKinesisWorkflowParams) *EmrToKinesisWorkflow {
	return &EmrToKinesisWorkflow{
		StreamEmrDataToKinesisActivity: p.StreamEmrDataToKinesisActivity,
	}
}

func (m EmrToKinesisWorkflow) Definition() workflows.WorkflowDefinition[EmrToKinesisWorkflowArgs] {
	return EmrToKinesisWorkflowDefinition
}

func (m EmrToKinesisWorkflow) SupportedVersionFlags() []workflowengine.FlagKey {
	return []workflowengine.FlagKey{}
}

func (m EmrToKinesisWorkflow) Execute(engine workflowengine.WorkflowEngine, args *EmrToKinesisWorkflowArgs) error {
	selector := engine.Selector()
	// Attach a timer to the workflow.
	selector.Timer(args.Jitter, nil)
	// Block execution of workflow until Timer fires.
	selector.Select()
	logger := engine.GetLogger()

	timeoutMinutes := 30
	if args.EmrToKinesisWorkflowTimeoutMinutes != nil {
		timeoutMinutes = *args.EmrToKinesisWorkflowTimeoutMinutes
	}

	var retryPolicy *workflowengine.ActivityRetryPolicy
	if args.EmrToKinesisWorkflowMaximumAttempts != nil {
		maximumAttempts := *args.EmrToKinesisWorkflowMaximumAttempts
		retryPolicy = &workflowengine.ActivityRetryPolicy{
			MaximumAttempts: pointer.Int32Ptr(int32(maximumAttempts)),
		}
	}

	err := workflowengine.ExecuteVoidActivity(engine,
		m.StreamEmrDataToKinesisActivity,
		&activity.StreamEmrDataToKinesisActivityArgs{
			OrgId:             args.OrgId,
			EntityName:        args.EntityName,
			PageToken:         nil,
			PageSize:          args.PageSize,
			AssetIds:          args.AssetIds,
			StartMs:           args.StartMs,
			EndMs:             args.EndMs,
			BackfillRequestId: args.BackfillRequestId,
			Filters:           args.Filters,
			FilterOptions:     args.FilterOptions,
			BatchIndex:        args.BatchIndex,
			FilterId:          args.FilterId,
		}, &workflowengine.ActivityOptions{
			StartToCloseTimeout: pointer.New(time.Minute * time.Duration(timeoutMinutes)),
			RetryPolicy:         retryPolicy,
		},
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to stream EMR data to Kinesis")
	}

	logger.Info("completed processing asset batch",
		"backfillRequestId", args.BackfillRequestId,
		"entityName", args.EntityName,
		"orgId", args.OrgId,
		"assetCount", len(args.AssetIds),
	)

	return nil
}
