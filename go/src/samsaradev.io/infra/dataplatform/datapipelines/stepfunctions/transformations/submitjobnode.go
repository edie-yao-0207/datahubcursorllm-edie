package transformations

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	team_components "samsaradev.io/team/components"
)

type SparkExecutionEnvironment struct {
	SparkVersion            sparkversion.SparkVersion
	Libraries               []*databricks.Library
	SparkConfOverrides      map[string]string
	SparkEnvVars            map[string]string
	ClusterLogConf          *databricks.ClusterLogConf
	DriverInstancePoolName  string
	WorkerInstancePoolName  string
	InstanceProfileOverride *string
	MaxWorkers              *int
}

type SubmitJobArgs struct {
	TransformationName   string
	Team                 *team_components.TeamInfo
	ExecutionEnvironment SparkExecutionEnvironment

	// NOTE: We can refactor this in the future to allow any job type.
	// For now, it's assuming a python job just to simplify the interface.
	S3Path     string
	Parameters []string

	// Have the job specify how it wants to accept the start / end date parameters.
	// For now these are not optional because jobs using this sfn are expected to
	// be run on the data pipelines framework.
	StartDateParam string
	EndDateParam   string

	// Allows hardcoding what the exact start and end date should be. Most reports should NOT use or set this value,
	// we only support it for some special case reports like cm_health_report.
	// NOTE: this gets evaluated directly in SQL, so it either must be a valid SQL expression e.g. `DATE_SUB(current_date, 2)`
	// or a quoted literal, e.g. `"2021-01-01"`
	HardcodedStartDate *string
	HardcodedEndDate   *string

	// RnDCostAllocation specifies percent (0 to 1) of the job's cost to be allocated
	// to R&D. By default jobs are allocated to COGS.
	RnDCostAllocation float64

	// IsProduction should be true when this is a production job.
	IsProduction bool

	JobType dataplatformconsts.DataPlatformJobType
}

/*
SubmitJobStepFunction generates a node execution step function for managing metastore, and launching a databricks job.

	   ┌──────────────────────────────────┐
	   │            add_context           │
	   └──────────────────────────────────┘
	     │
	     │
	     ▼
	   ┌──────────────────────────────────┐
	   │         trigger_backfill         │
	   └──────────────────────────────────┘
	     │
	     │
	     ▼
	   ┌──────────────────────────────────┐
	   │ pipeline_metastore_handle_start  │
	   └──────────────────────────────────┘
	     │
	     │
	     ▼
	   ┌──────────────────────────────────┐
	┌─ │     metastore_initial_choice     │
	│  └──────────────────────────────────┘
	│    │
	│    │
	│    ▼
	│  ┌──────────────────────────────────┐
	│  │           submit_job             │
	│  └──────────────────────────────────┘
	│    │
	│    │
	│    ▼
	│  ┌──────────────────────────────────┐
	│  │        databricks_poller         │ ◀┐
	│  └──────────────────────────────────┘  │
	│    │                                   │
	│    │                                   │
	│    ▼                                   │

┌───────────────────────────────┐       │  ┌──────────────────────────────────┐  │
│ pipeline_metastore_handle_end │ ◀─────┼─ │        databricks_choice         │  │
└───────────────────────────────┘       │  └──────────────────────────────────┘  │

	│                                     │    │                                   │
	│                                     │    │                                   │
	│                                     │    ▼                                   │
	│                                     │  ┌──────────────────────────────────┐  │
	│                                     │  │         databricks_wait          │ ─┘
	│                                     │  └──────────────────────────────────┘
	|                                     |
	│                                     │  ┌──────────────────────────────────┐
	│                                     └▶ │ pipeline_metastore_handle_status │ ◀┐
	│                                        └──────────────────────────────────┘  │
	│                                          │                                   │
	│                                          │                                   │
	│                                          ▼                                   │
	│                                        ┌──────────────────────────────────┐  │
	│                                      ┌─│         metastore_choice         │  |
	│                                      | └──────────────────────────────────┘  |
	│                                      |    │                                  |
	│                                      |    │                                  |
	│                                      |    ▼                                  |
	│                                      |  ┌──────────────────────────────────┐ |
	│                                      |  │          metastore_wait          │─┘
	│                                      |  └──────────────────────────────────┘
	|                                      |
	│                                      | ┌──────────────────────────────────┐     ┌──────────────┐
	└──────────────────────────────────────▶ │           final_choice           │ ──▶ │ node_succeed │
	                                         └──────────────────────────────────┘     └──────────────┘
	                                           │
	                                           │
	                                           ▼
	                                         ┌──────────────────────────────────┐
	                                         │            node_fail             │
	                                         └──────────────────────────────────┘
*/
func SubmitJobStepFunction(providerGroup string, region string, args SubmitJobArgs) (statemachine.StepFunctionDefinition, error) {
	// "add_context" adds the current execution's ARN to the state input from the context object.
	// https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
	addContext := statemachine.CreatePassState(
		"add_context",
		nil,
		"Add current Step Function execution context to input",
	)
	addContext.Parameters = map[string]interface{}{
		"execution_arn.$": "$$.Execution.Id",
	}
	addContext.ResultPath = statemachine.Path("$.nesf_execution_context")

	// Before we start processing a task, fire off an asynchronous check to catch any unprocessed backfills.
	triggerBackfillSfnName := util.HashTruncate("nesf-"+strings.ReplaceAll(args.TransformationName, ".", "-")+"-triggerbackfill", 80, 8, "")
	backfillArn := fmt.Sprintf(
		"arn:aws:states:%s:%d:stateMachine:%s",
		region,
		infraconsts.GetDatabricksAccountIdForRegion(region),
		triggerBackfillSfnName,
	)
	triggerBackfill := statemachine.CreateStepFunctionTaskState(
		"pipeline_backfill",
		backfillArn,
		stepfunctions.GetSubmitJobBackfillInput(map[string]interface{}{
			"start_date.$": "$.backfill_start_date",
			"end_date.$":   "$.start_date",
			"children.$":   "$.children",
			"parents.$":    "$.parents",
		}),
		nil,
		"Fire off data_pipeline_backfill step function",
		statemachine.StateMachineAsynchronousExecution,
	)
	triggerBackfill.ResultPath = statemachine.Path("$.backfill_result")

	// Check the metastore to see if there is an existing run; if not, add an entry.
	metastoreHandleStart := statemachine.CreateLambdaTaskState(
		MetastoreHandleStart,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleStart)).Reference(),
		nil,
		"Check if a node_id & execution_id has begun, if not, load entry into the metastore.")

	// If there was an existing run in the metastore, skip to the end; otherwise don't.
	metastoreInitialChoice := statemachine.CreateEmptyChoiceState(InitialChoice, "Dictates whether a run will go down the databricks branch or the metastore branch.")

	// Call the lambda to submit our job to databricks, building the parameters from the provided arguments.
	submitJob := statemachine.CreateLambdaTaskState(
		fmt.Sprintf("submit_job_%s\n", args.TransformationName),
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", SubmitJob)).Reference(),
		nil,
		"Lambda function that submits databricks jobs.")
	submitJob.ResultPath = statemachine.Path("$.submitJobOutput")

	params, err := buildSubmitJobParams(providerGroup, region, args)
	if err != nil {
		return statemachine.StepFunctionDefinition{}, err
	}
	submitJob.Parameters = params

	// Poll databricks to see what the status of our job is.
	databricksPoller := statemachine.CreateLambdaTaskState(
		DatabricksPoller,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksPoller)).Reference(),
		nil,
		"Lambda function polling Databricks for the status of the job.")

	// Pass the specific params necessary + parse out the output from the previous task.
	databricksPoller.Parameters = map[string]interface{}{
		"node_id.$":           "$.node_id",
		"execution_id.$":      "$.execution_id",
		"run_id.$":            "$.submitJobOutput.run_id",
		"transformation_name": args.TransformationName,

		// We need to preserve this in the inputs to the lambda, because:
		// - the lambda returns all its inputs and outputs together
		// - the poller gets called in a loop
		// So preserving this in the input to the lambda ensures its present for the next loop iteration.
		"submitJobOutput.$": "$.submitJobOutput",
	}

	// If the databricks job is complete, continue onwards; otherwise go back to polling.
	databricksChoice := statemachine.CreateEmptyChoiceState(DatabricksChoice, "Switch on the state of the Databricks job.")

	// Wait for a specified time before polling databricks again for the job status.
	databricksWait := statemachine.CreateWaitState(DatabricksWait, nil, databricksPollerWaitTime, "Wait state during polling the Databricks job API for job status.") // To handle a cycle, we set a nil state here and then update once downstream is created

	// Update the metastore entry.
	metastoreHandleEnd := statemachine.CreateLambdaTaskState(
		MetastoreHandleEnd,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleEnd)).Reference(),
		nil,
		"Update entry in the metastore with the result of the Databricks job.")

	// In the case that we didn't run the submit job in this state function (because its being run elsewhere),
	// we still need to wait for it to complete, which is done using this loop of handleStatus -> wait -> choice.
	metastoreHandleStatus := statemachine.CreateLambdaTaskState(
		MetastoreHandleStatus,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleStatus)).Reference(),
		nil,
		"Check the status of a node_id & execution_id in the metastore.")

	// Waits for the read from the metastore to finish.
	metastoreWait := statemachine.CreateWaitState(MetastoreWait, nil, metastorePollerWaitTime, "Wait state during polling of metastore for the status of a node_id & execution_id.")

	// If the metastore check is done, continue; otherwise wait.
	metastoreChoice := statemachine.CreateEmptyChoiceState(MetastoreChoice, "Metastore choice state that exists polling on SUCCESS/FAILURE and continues polling otherwise.")

	// Finally, did we succeed or not?
	finalChoice := statemachine.CreateEmptyChoiceState(FinalChoice, "Final choice state that switches on SUCCESS or FAILURE.")

	nodeSucceed := statemachine.CreateSucceedState(NodeSucceed, "Success state of SubmitJob.")
	nodeFail := statemachine.CreateFailState(NodeFail, "", "", "Fail state of SubmitJob.")

	// Link the states together, now that we've instantiated them. The states are listed
	// vertically as they appear on the dag.
	// Unfortunately, `End` is a property on the struct and cannot be easily changed to a computed
	// property, so we have to set it to `false` here for each node.
	addContext.Next = triggerBackfill.Reference()
	addContext.End = false

	triggerBackfill.Next = metastoreHandleStart.Reference()
	triggerBackfill.End = false

	metastoreHandleStart.Next = metastoreInitialChoice.Reference()
	metastoreHandleStart.End = false

	metastoreInitialChoice.AddIfBooleanChoice(startedVariable, submitJob.Reference())
	metastoreInitialChoice.AddDefaultChoice(metastoreHandleStatus.Reference())
	metastoreInitialChoice.End = false

	submitJob.Next = databricksPoller.Reference()
	submitJob.End = false

	databricksPoller.Next = databricksChoice.Reference()
	databricksPoller.End = false

	databricksChoice.AddIfStringChoice(resultStateVariable, runningStatus, databricksWait.Reference())
	databricksChoice.AddDefaultChoice(metastoreHandleEnd.Reference())
	databricksChoice.End = false

	metastoreHandleEnd.Next = finalChoice.Reference()
	metastoreHandleEnd.End = false

	databricksWait.Next = databricksPoller.Reference()
	databricksWait.End = false

	metastoreWait.Next = metastoreHandleStatus.Reference()
	metastoreWait.End = false

	metastoreHandleStatus.Next = metastoreChoice.Reference()
	metastoreHandleStatus.End = false

	metastoreChoice.AddIfBooleanChoice(completedVariable, finalChoice.Reference())
	metastoreChoice.AddDefaultChoice(metastoreWait.Reference())
	metastoreChoice.End = false

	finalChoice.AddIfStringChoice(resultStateVariable, "SUCCESS", nodeSucceed.Reference())
	finalChoice.AddDefaultChoice(nodeFail.Reference())
	finalChoice.End = false

	return statemachine.StepFunctionDefinition{
		StartAt: addContext,
		States: []statemachine.State{
			addContext,
			triggerBackfill,
			metastoreHandleStart,
			metastoreInitialChoice,
			submitJob,
			databricksPoller,
			databricksChoice,
			metastoreHandleEnd,
			databricksWait,
			metastoreWait,
			metastoreHandleStatus,
			metastoreChoice,
			finalChoice,
			nodeSucceed,
			nodeFail,
		},
	}, nil
}

/*
SubmitJobBackfillSfn generates a stripped down step function for backfilling a submit job task.

	                     ┌───────────────────────────────┐
	                     │           submit_job          │
	                     └───────────────────────────────┘
	                       │
	                       │
	                       ▼
	                     ┌───────────────────────────────┐
	                     │       databricks_poller       │ ◀┐
	                     └───────────────────────────────┘  │
	                       │                                │
	                       │                                │
	                       ▼                                │
	┌──────────────┐     ┌───────────────────────────────┐  │
	│   node_end   │ ◀── │       databricks_choice       │  │
	└──────────────┘     └───────────────────────────────┘  │
	                       │                                │
	                       │                                │
	                       ▼                                │
	                     ┌───────────────────────────────┐  │
	                     │        databricks_wait        │ ─┘
	                     └───────────────────────────────┘
*/
func SubmitJobBackfillSfn(providerGroup string, region string, args SubmitJobArgs) (statemachine.StepFunctionDefinition, error) {
	// Submit the job to databricks
	submitJob := statemachine.CreateLambdaTaskState(
		fmt.Sprintf("submit_job_%s\n", args.TransformationName),
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", SubmitJob)).Reference(),
		nil,
		"Lambda function that submits databricks jobs.")
	submitJob.ResultPath = statemachine.Path("$.submitJobOutput")

	params, err := buildSubmitJobParams(providerGroup, region, args)
	if err != nil {
		return statemachine.StepFunctionDefinition{}, err
	}
	submitJob.Parameters = params

	// Poll the job for completion status.
	databricksPoller := statemachine.CreateLambdaTaskState(
		DatabricksPoller,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksPoller)).Reference(),
		nil,
		"Lambda function polling Databricks for the status of the job.")

	// Pass the specific params necessary + parse out the output from the previous task.
	databricksPoller.Parameters = map[string]interface{}{
		"node_id.$":      "$.node_id",
		"execution_id.$": "$.execution_id",
		"run_id.$":       "$.submitJobOutput.run_id",

		// We need to preserve this in the inputs to the lambda, because:
		// - the lambda returns all its inputs and outputs together
		// - the poller gets called in a loop
		// So preserving this in the input to the lambda ensures its present for the next loop iteration.
		"submitJobOutput.$": "$.submitJobOutput",
	}

	// If the job is done, complete the node; otherwise wait.
	databricksChoice := statemachine.CreateEmptyChoiceState(DatabricksChoice, "Switch on the state of the Databricks job.")

	// Wait state
	databricksWait := statemachine.CreateWaitState(DatabricksWait, nil, databricksPollerWaitTime, "Wait state during polling the Databricks job API for job status.")

	// End state
	success := statemachine.CreateSucceedState(NodeSucceed, "End State of the Backfill Transformation.")

	// Link the states together, now that we've instantiated them. The states are listed
	// vertically as they appear on the dag.
	// Unfortunately, `End` is a property on the struct and cannot be easily changed to a computed
	// property, so we have to set it to `false` here for each node.
	submitJob.Next = databricksPoller.Reference()
	submitJob.End = false

	databricksPoller.Next = databricksChoice.Reference()
	databricksPoller.End = false

	databricksChoice.AddIfStringChoice(resultStateVariable, runningStatus, databricksWait.Reference())
	databricksChoice.AddDefaultChoice(success.Reference())
	databricksChoice.End = false

	databricksWait.Next = databricksPoller.Reference()
	databricksWait.End = false

	return statemachine.StepFunctionDefinition{
		StartAt: submitJob,
		States: []statemachine.State{
			submitJob,
			databricksPoller,
			databricksChoice,
			databricksWait,
			success,
		},
	}, nil
}

func buildSubmitJobParams(providerGroup string, region string, args SubmitJobArgs) (map[string]interface{}, error) {
	profileArn, err := dataplatformresource.InstanceProfileArn(providerGroup, pointer.StringValOr(args.ExecutionEnvironment.InstanceProfileOverride, "data-pipelines-cluster"))
	if err != nil {
		return nil, oops.Wrapf(err, "could not fetch instance profile")
	}

	if args.RnDCostAllocation < 0 || args.RnDCostAllocation > 1 {
		return nil, oops.Errorf("invalid RnDCostAllocation: %f", args.RnDCostAllocation)
	}

	if args.JobType == "" {
		return nil, oops.Errorf("Must specify job type")
	}

	// Add the overrides from the provided spark configuration on top of the default configuration.
	sparkConf := (dataplatformresource.SparkConf{
		DisableQueryWatchdog: true,
		Region:               region,
		Overrides:            args.ExecutionEnvironment.SparkConfOverrides,
	}).ToMap()

	params := args.Parameters
	params = append(params, args.StartDateParam)
	if args.HardcodedStartDate != nil {
		params = append(params, *args.HardcodedStartDate)
	} else {
		params = append(params, "$.start_date")
	}
	params = append(params, args.EndDateParam)
	if args.HardcodedEndDate != nil {
		params = append(params, *args.HardcodedEndDate)
	} else {
		params = append(params, "$.end_date")
	}

	// Pass the pipeline execution time into the databricks job
	// TODO: To roll out safely, only do this for the dataplatform
	// test pipelines now, but we can remove this check when we confirm
	// it works.
	if strings.Contains(args.TransformationName, "dataplatform") {
		params = append(params, "--pipeline-execution-time", "$.pipeline_execution_time")
	}

	// If we use jsonpath substitution to pass through the start date or end date
	// (i.e. if at least one of them isn't hardcoded), then we have to use the intrinsic States.Array
	// to have them actually get evaluated.
	// However, if we do NOT have any jsonpath then we must build an array string as the implicit cannot be used
	// in situations where there is no jsonpath.
	// https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-intrinsic-functions.html
	var paramString string
	shouldUseParameterSubstitution := false
	if args.HardcodedEndDate == nil || args.HardcodedStartDate == nil {
		shouldUseParameterSubstitution = true
		paramString = buildParamStringWithIntrinsic(params)
	}

	sparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
	sparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
	sparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
	sparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

	// Construct the params that the lambda needs.
	runInput := databricks.SubmitRunInput{
		RunName: args.TransformationName,
		NewCluster: &databricks.NewCluster{
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: pointer.IntValOr(args.ExecutionEnvironment.MaxWorkers, 16),
			},
			SparkVersion: args.ExecutionEnvironment.SparkVersion,
			SparkConf:    sparkConf,
			SparkEnvVars: args.ExecutionEnvironment.SparkEnvVars,
			AwsAttributes: &databricks.ClusterAwsAttributes{
				InstanceProfileArn: profileArn,
			},
			ClusterLogConf: args.ExecutionEnvironment.ClusterLogConf,
			CustomTags: map[string]string{
				"samsara:pooled-job:service":               fmt.Sprintf("data-pipeline-node-%s", strings.ReplaceAll(args.TransformationName, ".", "-")),
				"samsara:pooled-job:team":                  args.Team.TeamName,
				"samsara:pooled-job:product-group":         team.TeamProductGroup[args.Team.TeamName],
				"samsara:pooled-job:rnd-allocation":        strconv.FormatFloat(args.RnDCostAllocation, 'f', -1, 64),
				"samsara:pooled-job:is-production-job":     strconv.FormatBool(args.IsProduction),
				"samsara:pooled-job:dataplatform-job-type": string(args.JobType),
				"samsara:pipeline-node-name":               args.TransformationName,
			},
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		},
		Libraries: args.ExecutionEnvironment.Libraries,
		SparkPythonTask: &databricks.SparkPythonTask{
			PythonFile: args.S3Path,
			Parameters: params,
		},
	}

	// Convert struct into a map[string]interface{} to pass into params.
	marshaled, err := json.Marshal(runInput)
	if err != nil {
		return nil, oops.Wrapf(err, "could not marshal submit job params")
	}
	var runInputJson map[string]interface{}
	err = json.Unmarshal(marshaled, &runInputJson)
	if err != nil {
		return nil, oops.Wrapf(err, "could not unmarshal submit job params")
	}

	// Because we want aws step functions to perform parameter substitution, we need to fill in the parameters field by hand to have it be called "parameters.$" instead of "parameters"
	// This is significantly less than ideal but there doesn't seem to be any other way to really do this well
	// (without constructing the entire input json by hand, which seems similarly less than ideal.)
	if shouldUseParameterSubstitution {
		runInputJson["spark_python_task"].(map[string]interface{})["parameters.$"] = paramString
		delete(runInputJson["spark_python_task"].(map[string]interface{}), "parameters")
	}

	// Construct pools parameter
	pools := &databricks.SubmitJobPools{
		InstancePoolName: args.ExecutionEnvironment.WorkerInstancePoolName,
		DriverPoolName:   aws.String(args.ExecutionEnvironment.DriverInstancePoolName),
	}

	return map[string]interface{}{
		"runInput": runInputJson,
		"pools":    pools,
	}, nil
}

func buildParamStringWithIntrinsic(params []string) string {
	if len(params) == 0 {
		return ""
	}

	var wrappedParams = make([]string, 0, len(params))

	for _, param := range params {
		if len(param) > 0 && param[0] == '$' {
			wrappedParams = append(wrappedParams, param)
		} else {
			wrappedParams = append(wrappedParams, "'"+param+"'")
		}
	}

	return "States.Array(" + strings.Join(wrappedParams, ", ") + ")"
}
