package activity

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/fx"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/workflows/workflowregistry"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewTriggerDatabricksJobActivity)
}

type TriggerDatabricksJobActivityArgs struct {
	// Databricks job name to trigger
	JobName *string `json:"jobName"`

	// Parameters to pass to the Databricks job
	Parameters []string `json:"parameters"`
}

type TriggerDatabricksJobActivityResult struct {
	RunId int64 `json:"runId"`
}

type TriggerDatabricksJobActivity struct {
	databricksClient *databricks.Client
}

type TriggerDatabricksJobActivityParams struct {
	fx.In
	DatabricksClient *databricks.Client
}

func NewTriggerDatabricksJobActivity(p TriggerDatabricksJobActivityParams) *TriggerDatabricksJobActivity {
	return &TriggerDatabricksJobActivity{
		databricksClient: p.DatabricksClient,
	}
}

func (a TriggerDatabricksJobActivity) Name() string {
	return "TriggerDatabricksJobActivity"
}

func (a TriggerDatabricksJobActivity) Execute(ctx context.Context, args *TriggerDatabricksJobActivityArgs) (TriggerDatabricksJobActivityResult, error) {
	if args.JobName == nil || *args.JobName == "" {
		return TriggerDatabricksJobActivityResult{}, errors.New("JobName must be specified")
	}

	return a.runExistingJob(ctx, args)
}

func (a TriggerDatabricksJobActivity) runExistingJob(ctx context.Context, args *TriggerDatabricksJobActivityArgs) (TriggerDatabricksJobActivityResult, error) {
	// First, find the job by name
	listInput := &databricks.ListJobsInput{
		Name:  *args.JobName,
		Limit: 25, // Should be enough to find the job
	}

	listResult, err := a.databricksClient.ListJobs(ctx, listInput)
	if err != nil {
		return TriggerDatabricksJobActivityResult{}, fmt.Errorf("failed to list Databricks jobs: %w", err)
	}

	// Find the job with the exact name match
	var jobId int64
	found := false
	for _, job := range listResult.Jobs {
		if job.JobSettings.Name == *args.JobName {
			jobId = job.JobId
			found = true
			break
		}
	}

	if !found {
		return TriggerDatabricksJobActivityResult{}, fmt.Errorf("job with name '%s' not found", *args.JobName)
	}

	// Run the existing job
	runInput := &databricks.RunNowInput{
		JobId:        jobId,
		PythonParams: args.Parameters,
	}

	result, err := a.databricksClient.RunNow(ctx, runInput)
	if err != nil {
		return TriggerDatabricksJobActivityResult{}, fmt.Errorf("failed to run Databricks job '%s' (ID: %d): %w", *args.JobName, jobId, err)
	}

	return TriggerDatabricksJobActivityResult{
		RunId: result.RunId,
	}, nil
}
