package transformations

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
	"samsaradev.io/libs/ni/infraconsts"
)

const (
	// resultStateVariable is the string representation of the JSON variable for accessing the result_state passed
	// between states from lambda functions
	resultStateVariable = "$.result_state"

	// startedVariable is the string representation of the JSON variable on whether an SQL Transformation has started
	startedVariable = "$.started"

	// completedVariable is the string representation of the JSON variable on whether a metastore entry has been marked as completed
	// and can move onto the final choice of whether the sql transformation node succeeded or failed
	completedVariable = "$.completed"

	// runningStatus is a string representating that a SQL Transformation is currently running on Databricks
	runningStatus = "RUNNING"
)

const (
	// databricksPollerWaitTime is the amount of time (in seconds) for the databricks wait state.
	databricksPollerWaitTime = 30

	// metastorePollerWaitTime is the amount of time (in seconds) for the metastore wait state.
	metastorePollerWaitTime = 30
)

const (
	MetastoreHandleStart        = "pipeline_metastore_handle_start"
	MetastoreHandleStatus       = "pipeline_metastore_handle_status"
	MetastoreHandleEnd          = "pipeline_metastore_handle_end"
	InitialChoice               = "metastore_initial_choice"
	MetastoreChoice             = "metastore_choice"
	MetastoreWait               = "metastore_wait"
	DatabricksSQLTransformation = "databricks_sql_transformation"
	DatabricksPoller            = "databricks_poller"
	DatabricksChoice            = "databricks_choice"
	DatabricksWait              = "databricks_wait"
	FinalChoice                 = "final_choice"
	NodeSucceed                 = "node_succeed"
	NodeFail                    = "node_fail"
	NodeEnd                     = "node_end"
	SubmitJob                   = "submit_job"
)

/*
	PipelineNESF generates a node execution step function for triggering backfill, managing metastore, and launching a sql transformation.
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
	                                       │  │  databricks_sql_transformation   │
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
	│                                     │  ┌──────────────────────────────────┐
	│                                     │  │          metastore_wait          │ ◀┐
	│                                     │  └──────────────────────────────────┘  │
	│                                     │    │                                   │
	│                                     │    │                                   │
	│                                     │    ▼                                   │
	│                                     │  ┌──────────────────────────────────┐  │
	│                                     └▶ │ pipeline_metastore_handle_status │  │
	│                                        └──────────────────────────────────┘  │
	│                                          │                                   │
	│                                          │                                   │
	│                                          ▼                                   │
	│                                        ┌──────────────────────────────────┐  │
	│                                        │         metastore_choice         │ ─┘
	│                                        └──────────────────────────────────┘
	│                                          │
	│                                          │
	│                                          ▼
	│                                        ┌──────────────────────────────────┐     ┌──────────────┐
	└──────────────────────────────────────▶ │           final_choice           │ ──▶ │ node_succeed │
	                                         └──────────────────────────────────┘     └──────────────┘
	                                           │
	                                           │
	                                           ▼
	                                         ┌──────────────────────────────────┐
	                                         │            node_fail             │
	                                         └──────────────────────────────────┘
*/
func PipelineNESF(region string, isSharded bool) statemachine.StepFunctionDefinition {
	var states []statemachine.State

	nodeFail := statemachine.CreateFailState(NodeFail, "", "", "Fail state of SQL Transformation.")
	nodeSucceed := statemachine.CreateSucceedState(NodeSucceed, "Success state of SQL Transformation.")
	finalChoice := statemachine.CreateIfElseStringChoiceState(FinalChoice, resultStateVariable, "SUCCESS", nodeSucceed.Reference(), nodeFail.Reference(), "Final choice state that switches on SUCCESS or FAILURE.")
	states = append(states, nodeFail, nodeSucceed, finalChoice)

	metastoreWait := statemachine.CreateWaitState(MetastoreWait, nil, metastorePollerWaitTime, "Wait state during polling of metastore for the status of a node_id & execution_id.") // To handle a cycle, we set a nil state here and then update once downstream is created
	metastoreChoice := statemachine.CreateIfElseBooleanChoiceState(MetastoreChoice, completedVariable, finalChoice.Reference(), metastoreWait.Reference(), "Metastore choice state that exist polling for SUCCESS or FAILURE and continues polling otherwise.")
	metastoreHandleStatus := statemachine.CreateLambdaTaskState(
		MetastoreHandleStatus,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleStatus)).Reference(),
		metastoreChoice.Reference(),
		"Check the status of a node_id & execution_id in the metastore.")
	metastoreWait.BaseState.Next = metastoreHandleStatus.Reference()
	states = append(states, metastoreWait, metastoreChoice, metastoreHandleStatus)

	metastoreHandleEnd := statemachine.CreateLambdaTaskState(
		MetastoreHandleEnd,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleEnd)).Reference(),
		finalChoice.Reference(),
		"Update entry in the metastore with the result of the Databricks job.")
	databricksWait := statemachine.CreateWaitState(DatabricksWait, nil, databricksPollerWaitTime, "Wait state during polling the Databricks job API for job status.") // To handle a cycle, we set a nil state here and then update once downstream is created
	databricksChoiceState := statemachine.CreateIfElseStringChoiceState(DatabricksChoice, resultStateVariable, runningStatus, databricksWait.Reference(), metastoreHandleEnd.Reference(), "Switch on the state of the Databricks job.")
	databricksPollerTask := statemachine.CreateLambdaTaskState(
		DatabricksPoller,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksPoller)).Reference(),
		databricksChoiceState.Reference(),
		"Lambda function polling Databricks for the status of the job.")
	databricksWait.BaseState.Next = databricksPollerTask.Reference()
	databricksSQLTransformationTask := statemachine.CreateLambdaTaskState(
		DatabricksSQLTransformation,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksSQLTransformation)).Reference(),
		databricksPollerTask.Reference(),
		"Lambda function that executes SQL Transformation job on Databricks.")

	states = append(states, metastoreHandleEnd, databricksWait, databricksChoiceState, databricksPollerTask, databricksSQLTransformationTask)

	initialChoice := statemachine.CreateIfElseBooleanChoiceState(InitialChoice, startedVariable, databricksSQLTransformationTask.Reference(), metastoreHandleStatus.Reference(), "Initial choice during a SQL Transformation. Dictates whether a run will go down databricks branch or metastore branch.")
	metastoreHandleStart := statemachine.CreateLambdaTaskState(
		MetastoreHandleStart,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", MetastoreHandleStart)).Reference(),
		initialChoice.Reference(),
		"Check if a node_id & execution_id has begun, if not, load entry into the metastore.")
	states = append(states, initialChoice, metastoreHandleStart)

	// Before we start processing a task, we fire off an asynchronous check to catch any unprocessed backfills
	backfillArn := fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:data_pipeline_backfill_sfn", region, infraconsts.GetDatabricksAccountIdForRegion(region))
	if isSharded {
		backfillArn = fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:data_pipeline_backfill_sfn_sharded", region, infraconsts.GetDatabricksAccountIdForRegion(region))
	}
	pipelineBackfillTriggerState := statemachine.CreateStepFunctionTaskState(
		"pipeline_backfill",
		backfillArn,
		stepfunctions.GetBackfillInput(map[string]interface{}{
			"start_date.$": "$.backfill_start_date",
			"end_date.$":   "$.start_date",
			"children.$":   "$.children",
			"parents.$":    "$.parents",
		}),
		metastoreHandleStart.Reference(),
		"Fire off data_pipeline_backfill step function",
		statemachine.StateMachineAsynchronousExecution,
	)
	pipelineBackfillTriggerState.ResultPath = statemachine.Path("$.backfill_result")
	states = append(states, pipelineBackfillTriggerState)

	// "add_context" state adds current execution's ARN
	// (https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html)
	// to state input at "$.nesf_execution_context.execution_arn". Because in subsequent
	// Lambda Task states we pass entire input to lambdas functions, this will
	// ensure the current execution ARN is passed to lambda functions.
	// https://stackoverflow.com/questions/54452861/aws-step-function-adding-dynamic-value-to-pass-state-type
	addContextState := statemachine.CreatePassState(
		"add_context",
		pipelineBackfillTriggerState.Reference(),
		"Add current Step Function execution context to input",
	)
	addContextState.Parameters = map[string]interface{}{
		"execution_arn.$": "$$.Execution.Id",
	}
	addContextState.ResultPath = statemachine.Path("$.nesf_execution_context")
	states = append(states, addContextState)

	return statemachine.StepFunctionDefinition{
		StartAt: addContextState,
		States:  states,
	}
}

/*
BackfillNESF generates a stripped down node execution step function for backfilling a given sql transformation.

	                     ┌───────────────────────────────┐
	                     │ databricks_sql_transformation │
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
	│ node_end     │ ◀── │       databricks_choice       │  │
	└──────────────┘     └───────────────────────────────┘  │
	                       │                                │
	                       │                                │
	                       ▼                                │
	                     ┌───────────────────────────────┐  │
	                     │        databricks_wait        │ ─┘
	                     └───────────────────────────────┘
*/
func BackfillNESF() statemachine.StepFunctionDefinition {
	var states []statemachine.State

	nodeEnd := statemachine.CreateSucceedState(NodeSucceed, "End State of the Backfill Transformation.")
	states = append(states, nodeEnd)
	databricksWait := statemachine.CreateWaitState(DatabricksWait, nil, databricksPollerWaitTime, "Wait state during polling the Databricks job API for job status.") // To handle a cycle, we set a nil state here and then update once downstream is created
	databricksChoiceState := statemachine.CreateIfElseStringChoiceState(DatabricksChoice, resultStateVariable, runningStatus, databricksWait.Reference(), nodeEnd.Reference(), "Switch on the state of the Databricks job.")
	databricksPollerTask := statemachine.CreateLambdaTaskState(
		DatabricksPoller,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksPoller)).Reference(),
		databricksChoiceState.Reference(),
		"Lambda function polling Databricks for the status of the job.")
	databricksWait.BaseState.Next = databricksPollerTask.Reference()
	databricksSQLTransformationTask := statemachine.CreateLambdaTaskState(
		DatabricksSQLTransformation,
		tf.LocalId(fmt.Sprintf("%s_lambda_arn", DatabricksSQLTransformation)).Reference(),
		databricksPollerTask.Reference(),
		"Lambda function that executes SQL Transformation job on Databricks.")

	states = append(states, databricksWait, databricksChoiceState, databricksPollerTask, databricksSQLTransformationTask)

	return statemachine.StepFunctionDefinition{
		StartAt: databricksSQLTransformationTask,
		States:  states,
	}
}
