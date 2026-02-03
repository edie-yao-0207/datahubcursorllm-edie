package datapipelineprojects

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsdynamodbresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions"
	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/lambdas"
	"samsaradev.io/infra/app/generate_terraform/tf"
	stepfunctions2 "samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

const dynamoBackfillTableName = "datapipeline-backfill-state"
const backfillRunning = "RUNNING"

const backfillStatusCheckLambdaFunction = "datapipeline_backfill_status_check"
const backfillStartExecutionsLambdaFunction = "datapipeline_backfill_start_executions"
const backfillPollExecutionsLambdaFunction = "datapipeline_backfill_poll_executions"

func pipelineBackfillProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	dynamoResources := dynamoResources()

	sfnResources := backfillSfnDefinition(providerGroup, config.Region, "data_pipeline_backfill", NESFBackfillName, stepfunctions2.GetBackfillInput(map[string]interface{}{}))
	sfnShardedResources := backfillSFNSharded(config.Region)

	lambdaExtraFiles, lambdaResources, err := allLambdaFunctions(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "error getting lambda function resources")
	}

	return &project.Project{
		RootTeam:        datapipelinesTerraformProjectName,
		Provider:        providerGroup,
		Class:           "data-pipelines",
		Name:            "backfill",
		GenerateOutputs: true,
		ResourceGroups:  project.MergeResourceGroups(dynamoResources, lambdaResources, sfnResources, sfnShardedResources),
		ExtraFiles:      lambdaExtraFiles,
	}, nil
}

func dynamoResources() map[string][]tf.Resource {
	dynamoTable := &awsdynamodbresource.DynamoDBTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		Name:        "datapipeline-backfill-state",
		BillingMode: awsdynamodbresource.PayPerRequest,
		HashKey:     "node_id",
		Attributes: []awsdynamodbresource.DynamoDBTableAttribute{
			{
				KeyName: "node_id",
				Type:    awsdynamodbresource.String,
			},
		},
		Tags: map[string]string{
			"Name":                   "datapipeline-backfill-state",
			"samsara:service":        "datapipeline-backfill-state",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}
	return map[string][]tf.Resource{
		"dynamodb": {dynamoTable},
	}
}

func allLambdaFunctions(region string) (map[string]*project.ExtraFile, map[string][]tf.Resource, error) {

	backfillStatusCheckLambdaFunctionTags := map[string]string{
		"samsara:service":       backfillStatusCheckLambdaFunction,
		"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
		"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
	}
	statusCheckExtraFiles, statusCheckResources, err := lambdaFunction(
		region,
		backfillStatusCheckLambdaFunction,
		"go/src/samsaradev.io/infra/dataplatform/lambdafunctions/datapipelines/backfill/checkbackfillstatuses/main.go",
		120,
		[]policy.AWSPolicyStatement{},
		backfillStatusCheckLambdaFunctionTags,
	)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "error getting lambda resources")
	}

	backfillStartExecutionsLambdaFunctionTags := map[string]string{
		"samsara:service":       backfillStartExecutionsLambdaFunction,
		"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
		"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
	}
	startExecutionsExtraFiles, startExecutionsResources, err := lambdaFunction(
		region,
		backfillStartExecutionsLambdaFunction,
		"go/src/samsaradev.io/infra/dataplatform/lambdafunctions/datapipelines/backfill/startshardedexecutions/main.go",
		120,
		[]policy.AWSPolicyStatement{
			{
				Action: []string{"states:StartExecution"},
				Effect: "Allow",
				Resource: []string{
					fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), NESFBackfillName),
				},
			},
		},
		backfillStartExecutionsLambdaFunctionTags,
	)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "error getting lambda resources")
	}

	backfillPollExecutionsLambdaFunctionTags := map[string]string{
		"samsara:service":       backfillPollExecutionsLambdaFunction,
		"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
		"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
	}
	pollExecutionsExtraFiles, pollExecutionsResources, err := lambdaFunction(
		region,
		backfillPollExecutionsLambdaFunction,
		"go/src/samsaradev.io/infra/dataplatform/lambdafunctions/datapipelines/backfill/pollshardedexecutions/main.go",
		120,
		[]policy.AWSPolicyStatement{
			{
				Action: []string{"states:DescribeExecution"},
				Effect: "Allow",
				Resource: []string{
					fmt.Sprintf("arn:aws:states:%s:%d:execution:%s*", region, infraconsts.GetDatabricksAccountIdForRegion(region), NESFBackfillName),
				},
			},
		},
		backfillPollExecutionsLambdaFunctionTags,
	)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "error getting lambda resources")
	}

	lambdaFiles := mergeExtraFiles(statusCheckExtraFiles, startExecutionsExtraFiles, pollExecutionsExtraFiles)
	lambdaResources := project.MergeResourceGroups(statusCheckResources, startExecutionsResources, pollExecutionsResources)

	return lambdaFiles, lambdaResources, nil
}

func mergeExtraFiles(groups ...map[string]*project.ExtraFile) map[string]*project.ExtraFile {
	merged := make(map[string]*project.ExtraFile)
	for _, group := range groups {
		for key, values := range group {
			merged[key] = values
		}
	}
	return merged
}

func lambdaFunction(region string, name string, filePath string, timeoutSeconds int, policies []policy.AWSPolicyStatement, tags map[string]string) (map[string]*project.ExtraFile, map[string][]tf.Resource, error) {
	lambda := &lambdas.GoLambdaFunction{
		Name:                  name,
		MainFile:              filePath,
		Region:                region,
		TimeoutSeconds:        timeoutSeconds,
		ExtraPolicyStatements: policies,
		Tags:                  tags,
	}

	lambdaFiles := make(map[string]*project.ExtraFile)
	lambdaBucket := awsregionconsts.RegionPrefix[region] + "dataplatform-deployed-artifacts"
	extraFiles, resources, err := lambda.FilesAndResources(lambdaBucket)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "%s", lambda.LambdaFunctionName())
	}

	for k, v := range extraFiles {
		lambdaFiles[k] = v
	}

	return lambdaFiles, map[string][]tf.Resource{
		fmt.Sprintf("lambda_%s", name): resources,
	}, nil
}

/*
	+--------------------------------+
	|             start              |
	+--------------------------------+
	  |
	  |
	  v
	+--------------------------------+
	|       backfill_key_check       | -+
	+--------------------------------+  |
	  |                                 |
	  |                                 |
	  v                                 |
	+--------------------------------+  |
	|    get_node_backfill_status    |  |
	+--------------------------------+  |
	  |                                 |
	  |                                 |
	  v                                 |
	+--------------------------------+  |
	|   get_parent_backfill_status   |  |
	+--------------------------------+  |
	  |                                 |
	  |                                 |
	  v                                 |
	+--------------------------------+  |
	|     backfill_status_check      |  |
	+--------------------------------+  |
	  |                                 |
	  |                                 |
	  v                                 |
	+--------------------------------+  |

+- |     backfill_status_choice     |  |
|  +--------------------------------+  |
|    |                                 |
|    |                                 |
|    v                                 |
|  +--------------------------------+  |
|  |   put_node_backfill_running    |  |
|  +--------------------------------+  |
|    |                                 |
|    |                                 |
|    v                                 |
|  +--------------------------------+  |
|  |  delete_child_backfill_status  |  |
|  +--------------------------------+  |
|    |                                 |
|    |                                 |
|    v                                 |
|  +--------------------------------+  |
|  |       fire_nesf_backfill       |  |
|  +--------------------------------+  |
|    |                                 |
|    |                                 |
|    v                                 |
|  +--------------------------------+  |
|  | update_node_backfill_completed |  |
|  +--------------------------------+  |
|    |                                 |
|    |                                 |
|    v                                 |
|  +--------------------------------+  |
+> |     backfill_sfn_complete      | <+

	+--------------------------------+
	  |
	  |
	  v
	+--------------------------------+
	|              end               |
	+--------------------------------+
*/
func backfillSfnDefinition(providerGroup string, region string, name string, backfillSfnName string, backfillInputs map[string]interface{}) map[string][]tf.Resource {
	// Success state on successful backfill
	successState := statemachine.CreateSucceedState("backfill_sfn_complete", "Pipeline Backfill sfn complete")

	// update_node_backfill_completed updates a node entry in the dynamo backfill table that a node backfill is completed.
	dynamoUpdateNodeBackfillTaskState := statemachine.CreateDynamoAPITaskState(
		"update_node_backfill_completed",
		successState.Reference(),
		statemachine.DynamoUpdate,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{
					"S.$": "$.node_id",
				},
			},
			"UpdateExpression": "SET backfill_status = :c, databricks_url = :d",
			"ExpressionAttributeValues": map[string]interface{}{
				":c": map[string]string{
					"S.$": "$.backfill_nesf_output.Output.result_state",
				},
				":d": map[string]string{
					"S.$": "$.backfill_nesf_output.Output.run_page_url",
				},
			},
		},
		"Updates a node entry in the dynamo backfill table that a node backfill is completed utilizing the dynamo updateItem API Call",
	)
	dynamoUpdateNodeBackfillTaskState.ResultPath = statemachine.Path("$.dynamo_delete_results")

	defaultInputs := map[string]interface{}{
		"start_date.$":   "$.backfill_start_date",
		"end_date":       "DATE_SUB(CURRENT_DATE(), 2)", // Backfill should not overlap with 3hr merge
		"execution_id.$": "$.backfill_check_result.backfill_execution_id",
	}
	for key, val := range defaultInputs {
		backfillInputs[key] = val
	}

	// fire_nesf_backfill makes a StepFunction StartExecution call to the Backfill NESF
	nesfARN := fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), backfillSfnName)
	fireNESFState := statemachine.CreateStepFunctionTaskState(
		"fire_nesf_backfill",
		nesfARN,
		backfillInputs,
		dynamoUpdateNodeBackfillTaskState.Reference(),
		"StepFunction StartExecution call to the NESF to execute the backfill",
		statemachine.StateMachineSynchronousExecutionJSONReturn,
	)
	fireNESFState.ResultPath = statemachine.Path("$.backfill_nesf_output")

	// delete_child_backfill_statuses is a Task state utilized in a MapState to delete all child nodes backfill entries.
	// This is done because when a node is backfilled all downstream child nodes need to be rebackfilled as well.
	dynamoDeleteState := statemachine.CreateDynamoAPITaskState(
		"delete_child_backfill_statuses",
		nil,
		statemachine.DynamoDeleteItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$"},
			},
		},
		"delete downstream node entries from dynamo so they can be rebackfilled",
	)

	// delete_child_backfill_status_map_state is a map state that maps each element of the children to the delete_child_backfill_statuses
	// task state to delete the dynamo entry for a backfill
	dynamoDeleteNodesMapState := statemachine.CreateMapState(
		"delete_child_backfill_status_map_state",
		fireNESFState.Reference(),
		&statemachine.StepFunctionDefinition{
			Name:    "delete_child_nodes_inner_sfn",
			StartAt: dynamoDeleteState,
			States:  []statemachine.State{dynamoDeleteState},
		},
		"$.children",
		"Delete all nodes in the children array in the input. This makes sure all downstream nodes will be backfilled in subsequent runs.",
	)
	dynamoDeleteNodesMapState.ResultPath = statemachine.Path("$.dynamo_delete_results")

	// put_node_backfill_running puts an entry in the dynamo backfill table indicating a node backfill is currently running
	dynamoPutNodeBackfillRunningTaskState := statemachine.CreateDynamoAPITaskState(
		"put_node_backfill_running",
		dynamoDeleteNodesMapState.Reference(),
		statemachine.DynamoPutItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Item": map[string]interface{}{
				"node_id": map[string]interface{}{
					"S.$": "$.node_id",
				},
				"backfill_start_date": map[string]interface{}{
					"S.$": "$.backfill_start_date",
				},
				"backfill_status": map[string]interface{}{
					"S": backfillRunning,
				},
				"databricks_url": map[string]interface{}{
					"S": "",
				},
			},
		},
		"Put new entry in Dynamo backfill table showing the table has been backfilled to the entry key",
	)
	dynamoPutNodeBackfillRunningTaskState.ResultPath = statemachine.Path("$.dynamo_put_results")

	// backfill_status_choice is a choice state that comes after the computation of whether a backfill should execute.
	// It branches either down the path of executing a backfill or completed the step function.
	backfillStatusChoiceState := statemachine.CreateChoiceState(
		"backfill_status_choice",
		[]*statemachine.Choice{
			{
				Variable:      "$.backfill_check_result.execute_backfill",
				BooleanEquals: aws.Bool(false),
				Next:          successState.Reference(),
			},
		},
		dynamoPutNodeBackfillRunningTaskState.Reference(),
		"",
	)

	// backfill_status_check executes a lambda function that computes whether a backfill is required
	backfillStatusCheckTaskState := statemachine.CreateLambdaTaskState(
		"backfill_status_check",
		dataplatformresource.LambdaArn(providerGroup, region, backfillStatusCheckLambdaFunction),
		backfillStatusChoiceState.Reference(),
		"Check if the backfill key in the input json is populated.",
	)
	backfillStatusCheckTaskState.ResultPath = statemachine.Path("$.backfill_check_result")

	// get_parent_backfill_status is a task state that makes an GET API call to dynamo to see the status of the parent
	// It is utilized inside a map state to get the status of all of the nodes parents
	dynamoGetParent := statemachine.CreateDynamoAPITaskState(
		"get_parent_backfill_status",
		nil,
		statemachine.DynamoGetItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$"},
			},
			"ProjectionExpression": "backfill_status",
		},
		"Delete downstream nodes from dynamo",
	)

	// check_parents is a map state that calls a dynamo GET on all the parents to get their backfill statuses
	dynamoCheckParentsMapState := statemachine.CreateMapState(
		"check_parents",
		backfillStatusCheckTaskState.Reference(),
		&statemachine.StepFunctionDefinition{
			Name:    "get_parents_inner_sfn",
			StartAt: dynamoGetParent,
			States:  []statemachine.State{dynamoGetParent},
		},
		"$.parents",
		"Get each parent entry from the dynamo table",
	)
	dynamoCheckParentsMapState.ResultPath = statemachine.Path("$.parent_backfill_statuses")

	// dynamo_check_node makes an GET call to dynamo to see if the node has been backfilled
	dynamoCheckNodeTaskState := statemachine.CreateDynamoAPITaskState(
		"get_node_backfill_status",
		dynamoCheckParentsMapState.Reference(),
		statemachine.DynamoGetItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$.node_id"},
			},
		},
		"Check if the dynamo backfill table has an entry for a node",
	)
	dynamoCheckNodeTaskState.ResultPath = statemachine.Path("$.node_backfill_status")

	// backfill_key_check checks the JSON key `backfill_key_check` from the input
	// If the key is populated, this means a developer is explicitly requesting a backfill. GOTO: dynamo_check_node
	// If the key is not populated, the node does not need to be backfilled. GOTO: backfill_sfn_complete
	backfillKeyChoiceState := statemachine.CreateIfElseStringChoiceState(
		"backfill_key_check",
		"$.backfill_start_date",
		"",
		successState.Reference(),
		dynamoCheckNodeTaskState.Reference(),
		"Check if the backfill key in the input json is populated.",
	)

	policyArns := []string{"arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"}
	policyStatements := []policy.AWSPolicyStatement{
		{
			Action: []string{"states:StartExecution", "states:StopExecution", "state:DescribeExecution"},
			Effect: "Allow",
			Resource: []string{
				nesfARN,
			},
		},
		{
			Action: []string{"events:PutTargets", "events:PutRule", "events:DescribeRule"},
			Effect: "Allow",
			Resource: []string{
				fmt.Sprintf("arn:aws:events:%s:%d:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			},
		},
		{
			Action: []string{"dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem", "dynamodb:UpdateItem"},
			Effect: "Allow",
			Resource: []string{
				fmt.Sprintf("arn:aws:dynamodb:%s:%d:table/datapipeline-backfill-state", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			},
		},
		{
			Action: []string{"lambda:InvokeFunction"},
			Effect: "Allow",
			Resource: []string{
				dataplatformresource.LambdaArn(providerGroup, region, backfillStatusCheckLambdaFunction),
			},
		},
	}

	roleId, roleResources := dataplatformresource.StepFunctionRole(name, policyArns, policyStatements)
	sfnDefinition := statemachine.StepFunctionDefinition{
		Name:    name,
		StartAt: backfillKeyChoiceState,
		States: []statemachine.State{
			successState,
			dynamoPutNodeBackfillRunningTaskState,
			dynamoDeleteNodesMapState,
			backfillKeyChoiceState,
			backfillStatusCheckTaskState,
			dynamoCheckNodeTaskState,
			dynamoCheckParentsMapState,
			dynamoUpdateNodeBackfillTaskState,
			fireNESFState,
			backfillStatusChoiceState,
		},
	}

	// Preserve the resource name of the backfill sfn as `data_pipeline_backfill_sfn`
	// instead of using the name `data_pipeline_backfill`. We can change the name, but this should
	// be done in an independent change (separate from the commit where this comment was added).
	resourceName := name
	if resourceName == "data_pipeline_backfill" {
		resourceName += "_sfn"
	}
	stepFunctionResource := &stepfunctions.StepFunction{
		Name:       resourceName,
		Definition: &sfnDefinition,
		RoleARN:    roleId.ReferenceAttr("arn"),
		Tags: map[string]string{
			"samsara:service": resourceName,
			// TODO do we need to plumb the team owner from the caller?
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	return map[string][]tf.Resource{
		"iam": roleResources,
		"sfn": {stepFunctionResource},
	}
}

// TODO: use dataplatformresource.LambdaArn to build arns instead of using references
func backfillSFNSharded(region string) map[string][]tf.Resource {
	// Success state on successful backfill
	successState := statemachine.CreateSucceedState("backfill_sfn_complete", "Pipeline Backfill sfn complete")

	// update_node_backfill_completed updates a node entry in the dynamo backfill table that a node backfill is completed.
	dynamoUpdateNodeBackfillTaskState := statemachine.CreateDynamoAPITaskState(
		"update_node_backfill_completed",
		successState.Reference(),
		statemachine.DynamoUpdate,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{
					"S.$": "$.node_id",
				},
			},
			"UpdateExpression": "SET backfill_status = :c, databricks_url = :d, failed_databricks_urls = :e",
			"ExpressionAttributeValues": map[string]interface{}{
				":c": map[string]string{
					"S.$": "$.poll_executions_output.result_state",
				},
				":d": map[string]string{
					"SS.$": "$.poll_executions_output.run_page_urls",
				},
				":e": map[string]string{
					"SS.$": "$.poll_executions_output.failed_run_page_urls",
				},
			},
		},
		"Updates a node entry in the dynamo backfill table that a node backfill is completed utilizing the dynamo updateItem API Call",
	)
	dynamoUpdateNodeBackfillTaskState.ResultPath = statemachine.Path("$.dynamo_delete_results")

	// backfill_polling_wait waits for 30 minutes before kicking of the polling lambda again
	// To handle a cycle, we set a nil state here and then update once downstream is created
	waitDurationSeconds := 60 * 30
	backfillPollingWait := statemachine.CreateWaitState("backfill_polling_wait", nil, waitDurationSeconds, "Wait state during polling nesf execution for completion status")

	// backfill_polling_choice chooses to kick off the wait state if any executions are still running, if not it kicks off the dynamo update
	backfillPollingChoice := statemachine.CreateIfElseStringChoiceState(
		"backfill_polling_choice",
		"$.poll_executions_output.result_state",
		"RUNNING",
		backfillPollingWait.Reference(),
		dynamoUpdateNodeBackfillTaskState.Reference(),
		"Chooses to either keep polling or move on to updating dynamo table",
	)

	// Poller lambda calls the describeExecutions aws sfn api endpoint to get the status of each sharded execution
	// It decides on a result state based on all the executions and also accumulates all job urls
	backfillPollingLambda := statemachine.CreateLambdaTaskState(
		"poll_sharded_executions",
		awsresource.LambdaFunctionResourceId(backfillPollExecutionsLambdaFunction).ReferenceAttr("arn"),
		backfillPollingChoice.Reference(),
		"lambda that starts n executions of different date ranges in parallel where n varies based on the duration of each shard",
	)
	backfillPollingLambda.ResultPath = statemachine.Path("$.poll_executions_output")
	backfillPollingWait.BaseState.Next = backfillPollingLambda.Reference()

	// Start executions lambda starts n executions of different date ranges in parallel
	startExecutionsLambda := statemachine.CreateLambdaTaskState(
		"start_sharded_executions",
		awsresource.LambdaFunctionResourceId(backfillStartExecutionsLambdaFunction).ReferenceAttr("arn"),
		backfillPollingLambda.Reference(),
		"lambda that starts n executions of different date ranges in parallel where n varies based on the duration of each shard",
	)
	startExecutionsLambda.ResultPath = statemachine.Path("$.start_executions_result")

	// delete_child_backfill_statuses is a Task state utilized in a MapState to delete all child nodes backfill entries.
	// This is done because when a node is backfilled all downstream child nodes need to be rebackfilled as well.
	dynamoDeleteState := statemachine.CreateDynamoAPITaskState(
		"delete_child_backfill_statuses",
		nil,
		statemachine.DynamoDeleteItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$"},
			},
		},
		"delete downstream node entries from dynamo so they can be rebackfilled",
	)

	// delete_child_backfill_status_map_state is a map state that maps each element of the children to the delete_child_backfill_statuses
	// task state to delete the dynamo entry for a backfill
	dynamoDeleteNodesMapState := statemachine.CreateMapState(
		"delete_child_backfill_status_map_state",
		startExecutionsLambda.Reference(),
		&statemachine.StepFunctionDefinition{
			Name:    "delete_child_nodes_inner_sfn",
			StartAt: dynamoDeleteState,
			States:  []statemachine.State{dynamoDeleteState},
		},
		"$.children",
		"Delete all nodes in the children array in the input. This makes sure all downstream nodes will be backfilled in subsequent runs.",
	)
	dynamoDeleteNodesMapState.ResultPath = statemachine.Path("$.dynamo_delete_results")

	// put_node_backfill_running puts an entry in the dynamo backfill table indicating a node backfill is currently running
	dynamoPutNodeBackfillRunningTaskState := statemachine.CreateDynamoAPITaskState(
		"put_node_backfill_running",
		dynamoDeleteNodesMapState.Reference(),
		statemachine.DynamoPutItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Item": map[string]interface{}{
				"node_id": map[string]interface{}{
					"S.$": "$.node_id",
				},
				"backfill_start_date": map[string]interface{}{
					"S.$": "$.backfill_start_date",
				},
				"backfill_status": map[string]interface{}{
					"S": backfillRunning,
				},
				"databricks_url": map[string]interface{}{
					"S": "",
				},
				"failed_databricks_urls": map[string]interface{}{
					"S": "",
				},
			},
		},
		"Put new entry in Dynamo backfill table showing the table has been backfilled to the entry key",
	)
	dynamoPutNodeBackfillRunningTaskState.ResultPath = statemachine.Path("$.dynamo_put_results")

	// backfill_status_choice is a choice state that comes after the computation of whether a backfill should execute.
	// It branches either down the path of executing a backfill or completed the step function.
	backfillStatusChoiceState := statemachine.CreateChoiceState(
		"backfill_status_choice",
		[]*statemachine.Choice{
			{
				Variable:      "$.backfill_check_result.execute_backfill",
				BooleanEquals: aws.Bool(false),
				Next:          successState.Reference(),
			},
		},
		dynamoPutNodeBackfillRunningTaskState.Reference(),
		"",
	)

	// backfill_status_check executes a lambda function that computes whether a backfill is required
	backfillStatusCheckTaskState := statemachine.CreateLambdaTaskState(
		"backfill_status_check",
		awsresource.LambdaFunctionResourceId(backfillStatusCheckLambdaFunction).ReferenceAttr("arn"),
		backfillStatusChoiceState.Reference(),
		"Check if the backfill key in the input json is populated.",
	)
	backfillStatusCheckTaskState.ResultPath = statemachine.Path("$.backfill_check_result")

	// get_parent_backfill_status is a task state that makes an GET API call to dynamo to see the status of the parent
	// It is utilized inside a map state to get the status of all of the nodes parents
	dynamoGetParent := statemachine.CreateDynamoAPITaskState(
		"get_parent_backfill_status",
		nil,
		statemachine.DynamoGetItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$"},
			},
			"ProjectionExpression": "backfill_status",
		},
		"Delete downstream nodes from dynamo",
	)

	// check_parents is a map state that calls a dynamo GET on all the parents to get their backfill statuses
	dynamoCheckParentsMapState := statemachine.CreateMapState(
		"check_parents",
		backfillStatusCheckTaskState.Reference(),
		&statemachine.StepFunctionDefinition{
			Name:    "get_parents_inner_sfn",
			StartAt: dynamoGetParent,
			States:  []statemachine.State{dynamoGetParent},
		},
		"$.parents",
		"Get each parent entry from the dynamo table",
	)
	dynamoCheckParentsMapState.ResultPath = statemachine.Path("$.parent_backfill_statuses")

	// dynamo_check_node makes an GET call to dynamo to see if the node has been backfilled
	dynamoCheckNodeTaskState := statemachine.CreateDynamoAPITaskState(
		"get_node_backfill_status",
		dynamoCheckParentsMapState.Reference(),
		statemachine.DynamoGetItem,
		map[string]interface{}{
			"TableName": dynamoBackfillTableName,
			"Key": map[string]interface{}{
				"node_id": map[string]interface{}{"S.$": "$.node_id"},
			},
		},
		"Check if the dynamo backfill table has an entry for a node",
	)
	dynamoCheckNodeTaskState.ResultPath = statemachine.Path("$.node_backfill_status")

	// backfill_key_check checks the JSON key `backfill_key_check` from the input
	// If the key is populated, this means a developer is explicitly requesting a backfill. GOTO: dynamo_check_node
	// If the key is not populated, the node does not need to be backfilled. GOTO: backfill_sfn_complete
	backfillKeyChoiceState := statemachine.CreateIfElseStringChoiceState(
		"backfill_key_check",
		"$.backfill_start_date",
		"",
		successState.Reference(),
		dynamoCheckNodeTaskState.Reference(),
		"Check if the backfill key in the input json is populated.",
	)

	sfnName := "data_pipeline_backfill_sharded"
	policyArns := []string{"arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"}
	policyStatements := []policy.AWSPolicyStatement{
		{
			Action: []string{"states:StartExecution", "states:StopExecution", "state:DescribeExecution"},
			Effect: "Allow",
			Resource: []string{
				fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), NESFBackfillName),
			},
		},
		{
			Action: []string{"events:PutTargets", "events:PutRule", "events:DescribeRule"},
			Effect: "Allow",
			Resource: []string{
				fmt.Sprintf("arn:aws:events:%s:%d:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			},
		},
		{
			Action: []string{"dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem", "dynamodb:UpdateItem"},
			Effect: "Allow",
			Resource: []string{
				fmt.Sprintf("arn:aws:dynamodb:%s:%d:table/datapipeline-backfill-state", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			},
		},
		{
			Action: []string{"lambda:InvokeFunction"},
			Effect: "Allow",
			Resource: []string{
				awsresource.LambdaFunctionResourceId(backfillStatusCheckLambdaFunction).ReferenceAttr("arn"),
				awsresource.LambdaFunctionResourceId(backfillStartExecutionsLambdaFunction).ReferenceAttr("arn"),
				awsresource.LambdaFunctionResourceId(backfillPollExecutionsLambdaFunction).ReferenceAttr("arn"),
			},
		},
	}

	roleId, roleResources := dataplatformresource.StepFunctionRole(sfnName, policyArns, policyStatements)
	sfnDefinition := statemachine.StepFunctionDefinition{
		Name:    sfnName,
		StartAt: backfillKeyChoiceState,
		States: []statemachine.State{
			successState,
			dynamoPutNodeBackfillRunningTaskState,
			dynamoDeleteNodesMapState,
			startExecutionsLambda,
			backfillPollingLambda,
			backfillPollingChoice,
			backfillPollingWait,
			backfillKeyChoiceState,
			backfillStatusCheckTaskState,
			dynamoCheckNodeTaskState,
			dynamoCheckParentsMapState,
			dynamoUpdateNodeBackfillTaskState,
			backfillStatusChoiceState,
		},
	}

	stepFunctionResource := &stepfunctions.StepFunction{
		Name:       "data_pipeline_backfill_sfn_sharded",
		Definition: &sfnDefinition,
		RoleARN:    roleId.ReferenceAttr("arn"),
		Tags: map[string]string{
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:service":       "data_pipeline_backfill_sfn_sharded",
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	return map[string][]tf.Resource{
		"iam": roleResources,
		"sfn": {stepFunctionResource},
	}
}
