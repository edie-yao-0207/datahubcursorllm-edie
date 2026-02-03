package stepfunctions

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions"
	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const MetricsLoggerLambdaName = "metrics_logger"

type TransformationFiles struct {
	SQLPath          string
	JSONMetadataPath string
	ExpectationPath  string
	TeamOwner        components.TeamInfo
	Region           string
}

type DagWithTerraformSfn struct {
	Dag      *graphs.DirectedAcyclicGraph
	Resource *stepfunctions.StepFunction
}

// GenerateDataPipelineOrchestrationStepFunction creates an sfn that puts creates a parallel state for all data pipeline sfns.
// This step function is then a single target of CloudWatch and fires off all other sfns
func GenerateDataPipelineOrchestrationStepFunction(dagsWithSfns []*DagWithTerraformSfn) *statemachine.StepFunctionDefinition {
	sfnInput := map[string]interface{}{
		"start_date.$":              "$.start_date",
		"end_date.$":                "$.end_date",
		"execution_id.$":            "$.execution_id",
		"pipeline_execution_time.$": "$.pipeline_execution_time",
	}

	var parallelSFNs []statemachine.StepFunctionDefinition
	for _, dagAndSfn := range dagsWithSfns {
		dag := dagAndSfn.Dag
		sfnName := dag.Name()
		sfnARN := dagAndSfn.Resource.ResourceId().ReferenceAttr("id")
		pipelineTaskState := statemachine.CreateStepFunctionTaskState(sfnName, sfnARN, sfnInput, nil, fmt.Sprintf("Task State calling %s sfn", sfnName), statemachine.StateMachineAsynchronousExecution)
		parallelSFNs = append(parallelSFNs, statemachine.StepFunctionDefinition{
			Name:    sfnName,
			Comment: fmt.Sprintf("%s parallel state", sfnName),
			StartAt: pipelineTaskState,
			States:  []statemachine.State{pipelineTaskState},
		})
	}

	// Sort for deterministic TF output
	sort.Slice(parallelSFNs, func(i, j int) bool {
		return parallelSFNs[i].Name > parallelSFNs[j].Name
	})

	// Spread out executions over 10 minutes.
	parallelWrapperState := statemachine.CreateParallelWithoutCancelState("parallel_sf_wrapper", nil, parallelSFNs, "", pointer.UInt32Ptr(1200))
	return &statemachine.StepFunctionDefinition{
		Name:    "data_pipeline_orchestration_sfn",
		StartAt: parallelWrapperState,
		States:  []statemachine.State{parallelWrapperState},
	}
}

// GenerateDailyOrchestrationSfn creates an sfn that creates a parallel state for all data pipeline sfns.
func GenerateDailyOrchestrationSfn(dagsWithSfns []*DagWithTerraformSfn) *statemachine.StepFunctionDefinition {
	var parallelSFNs []statemachine.StepFunctionDefinition
	for _, dagAndSfn := range dagsWithSfns {
		dag := dagAndSfn.Dag

		// The DAGs we get here are once we've partitioned into pipelines, so we only have
		// 1 leaf node.
		leafNodes := dag.GetLeafNodes()
		if len(leafNodes) != 1 {
			panic(fmt.Sprintf("dag %s had more than 1 leaf node %v\n", dag.Name(), leafNodes))
		}
		leafNode := leafNodes[0]
		if leafNode.DailyRunConfig() == nil {
			continue
		}

		startDateOffset := leafNodes[0].DailyRunConfig().StartDateOffset

		// For each pipeline provide the start_date corresponding to its config.
		sfnInput := map[string]interface{}{
			"start_date":                fmt.Sprintf("DATE_ADD(CURRENT_DATE(), -%d)", startDateOffset),
			"end_date":                  "DATE_ADD(CURRENT_DATE(), -2)",
			"execution_id.$":            "$.execution_id",
			"pipeline_execution_time.$": "$.pipeline_execution_time",
		}

		sfn := dagAndSfn.Resource
		sfnARN := sfn.ResourceId().ReferenceAttr("id")
		pipelineTaskState := statemachine.CreateStepFunctionTaskState(
			sfn.Name,
			sfnARN,
			sfnInput,
			nil,
			fmt.Sprintf("Task State calling %s sfn", sfn.Name),
			statemachine.StateMachineAsynchronousExecution)
		parallelSFNs = append(parallelSFNs, statemachine.StepFunctionDefinition{
			Name:    sfn.Name,
			Comment: fmt.Sprintf("%s parallel state", sfn.Name),
			StartAt: pipelineTaskState,
			States:  []statemachine.State{pipelineTaskState},
		})
	}

	// Sort for deterministic TF output
	sort.Slice(parallelSFNs, func(i, j int) bool {
		return parallelSFNs[i].Name > parallelSFNs[j].Name
	})

	// Spread out executions over 5 minutes.
	parallelWrapperState := statemachine.CreateParallelWithoutCancelState("parallel_sf_wrapper", nil, parallelSFNs, "", pointer.UInt32Ptr(300))
	return &statemachine.StepFunctionDefinition{
		Name:    "daily_data_pipeline_orchestration_sfn",
		StartAt: parallelWrapperState,
		States:  []statemachine.State{parallelWrapperState},
	}
}

type dagWithSfn struct {
	Dag           *graphs.DirectedAcyclicGraph
	SfnDefinition *statemachine.StepFunctionDefinition
}

func GenerateDataPipelineStepFunctions(transformationFiles map[string]*TransformationFiles, region string) ([]*dagWithSfn, error) {
	dags, err := graphs.BuildDAGs()
	if err != nil {
		return nil, err
	}

	sort.Slice(dags, func(i, j int) bool {
		return dags[i].Name() > dags[j].Name()
	})

	// If transformation doesn't exist in transformationFiles, that pipeline is disabled in the current region. We do not need to generate a step function for it
	enabledDags := []*graphs.DirectedAcyclicGraph{}
	for _, dag := range dags {
		dagName := dag.Name()
		if _, ok := transformationFiles[dagName]; ok {
			enabledDags = append(enabledDags, dag)
		}
	}

	dagsWithSFNs := make([]*dagWithSfn, 0, len(enabledDags))
	for _, dag := range enabledDags {
		dagName := dag.Name()
		sfnDefinition, err := generateStepFunctionDefinition(dag, transformationFiles)
		if err != nil {
			return nil, err
		}
		sfnDefinition.Name = dagName
		dagsWithSFNs = append(dagsWithSFNs, &dagWithSfn{Dag: dag, SfnDefinition: sfnDefinition})
	}

	return dagsWithSFNs, err
}

type nodeNamer struct {
	counts map[string]int
}

func newNodeNamer() *nodeNamer {
	return &nodeNamer{
		counts: make(map[string]int),
	}
}

func (c *nodeNamer) nextName(rootName string) string {
	currentCount := c.counts[rootName]
	c.counts[rootName]++
	return fmt.Sprintf("%s_%d", rootName, currentCount)
}

func generateStepFunctionDefinition(dag *graphs.DirectedAcyclicGraph, transformationFiles map[string]*TransformationFiles) (*statemachine.StepFunctionDefinition, error) {
	leafNodes := dag.GetLeafNodes()
	if len(leafNodes) != 1 {
		return nil, oops.Errorf("could not uniquely identify step function terminal node for dag %s", dag.Name())
	}

	dagMachine, err := generateStateMachineBranch(leafNodes[0], dag, transformationFiles, newNodeNamer())
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to generate state machine for pipeline %s", dag.Name())
	}

	// these branches are vestigial and kept to prevent diff; they will be removed in a subsequent PR
	if len(leafNodes[0].PipelineDependencies()) == 0 {
		for i, state := range dagMachine.States {
			if taskState, ok := state.(*statemachine.TaskState); ok {
				taskState.StateName = strings.TrimSuffix(taskState.StateName, "_0")
				dagMachine.States[i] = taskState
			}
		}
	} else {
		parallelWrapperState := statemachine.CreateEndParallelState(
			"parallel_sf_wrapper",
			[]statemachine.StepFunctionDefinition{dagMachine}, "")
		dagMachine = statemachine.StepFunctionDefinition{
			StartAt: parallelWrapperState,
			States:  []statemachine.State{parallelWrapperState},
		}
	}

	return &dagMachine, nil
}

// generateStateMachineBranch generates a branch of a step function using a recursive traversal algorithm.
// The function walks the dependencies for the currNode passed in and creates a branch for each one.
// Note: a node could end up in multiple branches if mulitple branches have a dependency on it.
// This is ok and expected. The step function implementation and will make sure it only gets executed once.
func generateStateMachineBranch(currNode nodetypes.Node, dag *graphs.DirectedAcyclicGraph, transformationFiles map[string]*TransformationFiles, namer *nodeNamer) (statemachine.StepFunctionDefinition, error) {
	currNodeStateName := namer.nextName(currNode.Name())
	nodeTransformationFiles, ok := transformationFiles[currNode.Name()]
	if !ok {
		return statemachine.StepFunctionDefinition{}, oops.Errorf("Transformation Files for node %s do not exist. Please verify the name of your node matches the folder structure. i.e x.y node should exist as x/y.{json|sql}. NOTE: This could also be due to node disabling. If you are disabling a node make sure to disable all of it's downstream consumers or else when constructing a downstream consumer step function it will attempt to find the disabled node and fail.", currNode.Name())
	}

	region := nodeTransformationFiles.Region
	acctId := infraconsts.GetDatabricksAccountIdForRegion(region)
	nesfARN := GenerateNESFARNForNode(
		region,
		acctId,
		util.HashTruncate(strings.ReplaceAll("nesf-"+currNode.Name(), ".", "-"), 80, 8, ""))

	nodeInput, err := getDatapipelinesNodeInput(currNode, dag.GetNodeChildren(currNode), dag.GetNodeParents(currNode), nodeTransformationFiles)
	if err != nil {
		return statemachine.StepFunctionDefinition{}, oops.Wrapf(err, "")
	}

	var states []statemachine.State
	var next *statemachine.StateReference

	// If necessary, create a sqlite export node that runs after the node itself.
	if currNode.SqliteReportConfig() != nil {
		sqliteNodeName := *currNode.SqliteNodeName()
		sqliteNesfARN := GenerateNESFARNForNode(
			region,
			acctId,
			util.HashTruncate(strings.ReplaceAll("nesf-"+sqliteNodeName, ".", "-"), 80, 8, ""))

		sqliteTask := statemachine.CreateStepFunctionTaskState(
			sqliteNodeName,
			sqliteNesfARN,
			getSubmitJobInput(sqliteNodeName, currNode.BackfillStartDate(), []nodetypes.Node{}, []nodetypes.Node{currNode}),
			nil,
			fmt.Sprintf("Sqlite Export step for %s\n", currNode.Name()),
			statemachine.StateMachineSynchronousExecutionJSONReturn)
		states = append(states, sqliteTask)

		metricsLoggerState := createMetricsLoggerTaskState(currNodeStateName+"-"+MetricsLoggerLambdaName, "$.Output", sqliteTask)
		metricsLoggerState.InputPath = statemachine.Path("$.reportOutput")
		metricsLoggerState.ResultPath = statemachine.Path("null")
		next = metricsLoggerState.Reference()
		states = append(states, metricsLoggerState)
	} else {
		metricsLoggerState := createMetricsLoggerTaskState(currNodeStateName+"-"+MetricsLoggerLambdaName, "$.Output", nil)
		next = metricsLoggerState.Reference()
		states = append(states, metricsLoggerState)
	}

	currNodeTaskState := statemachine.CreateStepFunctionTaskState(
		currNodeStateName,
		nesfARN,
		nodeInput,
		next,
		currNode.Description(),
		statemachine.StateMachineSynchronousExecutionStringReturn,
	)

	errorMetricsLoggerStates := generateMetricsLoggerErrorCatchAndStates(currNodeTaskState)
	states = append(states, errorMetricsLoggerStates...)

	// The report node uses the default resultpath setting which replaces the entire input
	// with the output of the node. This preserves the input where it is and adds the
	// report node's output at the $.reportOutput path.
	// This is likely safe to do in general (not just the sqlite case), so once we are sure
	// that it is we can just set this directly.
	if currNode.SqliteReportConfig() != nil {
		currNodeTaskState.ResultPath = statemachine.Path("$.reportOutput")
	}

	states = append(states, currNodeTaskState)
	var startAt statemachine.State = currNodeTaskState

	var branches []statemachine.StepFunctionDefinition

	for _, dep := range currNode.PipelineDependencies() {
		depNode := dag.GetNodeFromName(dep.Name())
		branchMachine, err := generateStateMachineBranch(depNode, dag, transformationFiles, namer)
		if err != nil {
			return statemachine.StepFunctionDefinition{}, oops.Wrapf(err, "")
		}
		branches = append(branches, branchMachine)
	}

	if len(branches) > 0 {
		dependenciesState := statemachine.CreateParallelWithoutCancelState(
			fmt.Sprintf("%s_dependencies", currNodeStateName),
			currNodeTaskState.Reference(),
			branches,
			fmt.Sprintf("%s dependencies", currNodeStateName),
			nil)

		states = append(states, dependenciesState)
		startAt = dependenciesState

		// We are rolling out the new metrics_logger tasks for catalogging node failues
		// Add the error catch to fire the metrics_logger task when a serial node fails
		if currNode.NonProduction() {
			errorCatchStates := generateMetricsLoggerErrorCatchAndStates(currNodeTaskState)
			states = append(states, errorCatchStates...)
		}
	}

	return statemachine.StepFunctionDefinition{
		StartAt: startAt,
		States:  states,
	}, nil
}

func GenerateNESFARNForNode(region string, acctId int, sfnName string) string {
	return fmt.Sprintf("arn:aws:states:%s:%d:stateMachine:%s", region, acctId, sfnName)
}

// getSubmitJobInput builds the input to when we call the submit job state function.
// This state function needs very little input, as the individual instances of the
// state function have most of the information necessary.
func getSubmitJobInput(nodeName string, backfillStartDate string, nodeChildren []nodetypes.Node, nodeParents []nodetypes.Node) map[string]interface{} {
	children := nodeNameSlice(nodeChildren)
	parents := nodeNameSlice(nodeParents)
	input := map[string]interface{}{
		"AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
		"start_date.$":              "$.start_date",
		"end_date.$":                "$.end_date",
		"backfill_start_date":       "",
		"pipeline_execution_time.$": "$.pipeline_execution_time",
		"execution_id.$":            "$.execution_id",
		"children":                  children,
		"parents":                   parents,

		// This data feels like it could be in the SFN itself and doesn't need to be passed in
		// but unfortunately a lot of the lambdas require this at the top level which isn't possible
		// to do using just the input/output tooling of states.
		"node_id": nodeName,
	}
	if backfillStartDate != "" {
		input["backfill_start_date"] = fmt.Sprintf("'%s'", backfillStartDate)
	}
	return input
}

func getDatapipelinesNodeInput(node nodetypes.Node, nodeChildren []nodetypes.Node, nodeParents []nodetypes.Node, s3Files *TransformationFiles) (map[string]interface{}, error) {
	children := nodeNameSlice(nodeChildren)
	parents := nodeNameSlice(nodeParents)

	// This is a bit of a hack, but there's not clearly a better way or place to do this given the current code.
	// We need to make sure to add the sqlite node to the list of children. It doesn't get added by default
	// because it's not a "real" node, however for backfill purposes we need it to show up in the child list.
	if node.SqliteReportConfig() != nil {
		children = append(children, node.Name()+"-sqlite")
	}

	input := map[string]interface{}{
		"AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
		"node_id":                   node.Name(),
		"transformation_name":       node.Name(),
		"start_date.$":              "$.start_date",
		"end_date.$":                "$.end_date",
		"pipeline_execution_time.$": "$.pipeline_execution_time",
		"s3_sql_path":               s3Files.SQLPath,
		"s3_json_metadata_path":     s3Files.JSONMetadataPath,
		"s3_expectation_path":       s3Files.ExpectationPath,
		"execution_id.$":            "$.execution_id",
		"children":                  children,
		"team":                      strings.ToLower(node.Owner().TeamName),
		"databricks_owner_group":    node.Owner().DatabricksAccountGroupName(),
		"product_group":             team.TeamProductGroup[node.Owner().TeamName],
		"parents":                   parents,
		"backfill_start_date":       "",
	}

	if node.NonProduction() {
		input["nonproduction"] = node.NonProduction()
	}

	if node.BackfillStartDate() != "" {
		input["backfill_start_date"] = fmt.Sprintf("'%s'", node.BackfillStartDate())
	}

	// GeoSpark needs an explicit parameter because it must be explicitly installed on the node
	input["geospark"] = needsGeoSpark(node)

	sparkEnv, err := generateNodeExecutionEnvironment(s3Files.Region, node)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating spark environment for node %s", node.Name())
	}
	input["node_execution_environment"] = sparkEnv

	return input, nil
}

func nodeNameSlice(nodes []nodetypes.Node) []string {
	nameSlice := make([]string, len(nodes))
	for idx, node := range nodes {
		nameSlice[idx] = node.Name()
	}
	return nameSlice
}

func generateNodeExecutionEnvironment(region string, node nodetypes.Node) (string, error) {
	sparkConf := (dataplatformresource.SparkConf{
		DisableQueryWatchdog: true,
		Region:               region,
		Overrides:            node.SparkExecutionEnvironment().SparkConfOverrides,

		// https://help.databricks.com/s/case/5008Y00001uSk7bQAC/small-job-timing-out
		AggressiveWindowDownSeconds: 300,
		NetworkTimeoutSeconds:       320,
	}).ToMap()
	// Explictly enable delta disk caching.
	// We use delta disk caching optimized instances but only i3* instance enable this by default
	sparkConf["spark.databricks.io.cache.enabled"] = "true"

	sparkVersion := sparkversion.DataPipelinesProductionDbrVersion
	if node.NonProduction() {
		sparkVersion = sparkversion.DataPipelinesNonProductionDbrVersion
	}

	if node.SparkExecutionEnvironment().SparkVersion != "" {
		sparkVersion = sparkversion.SparkVersion(node.SparkExecutionEnvironment().SparkVersion)
	}

	supportedLibraries, err := dataplatformresource.SupportedAPILibraries(sparkVersion, region)
	if err != nil {
		return "", oops.Wrapf(err, "Failed getting supported API libraries")
	}

	jobOwner, err := dataplatformconfig.GetDataPipelineServicePrincipalAppIdByRegion(region)
	if err != nil {
		return "", oops.Wrapf(err, "no data pipeline service principal app id for region %s", region)
	}

	dataSecurityMode := databricksresource.DataSecurityModeSingleUser
	singleUserName := jobOwner

	// We set the initial catalog name to default to use Unity Catalog resources by default.
	sparkConf["spark.databricks.sql.initial.catalog.name"] = "default"

	// We set the following cache/modtimeCheck settings to false to avoid issues with tables
	// that are too frequently updated so that the job doesn't fail.
	sparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
	sparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
	sparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

	// TODO: at this time we don't support volume init scripts for datapipeline nodes
	// because no datapipelines are using unity catalog. When we enable it, we'll need to update this.
	var initScripts []*databricks.InitScript
	for _, initScript := range node.SparkExecutionEnvironment().S3InitScripts {
		initScripts = append(initScripts, &databricks.InitScript{
			S3: &databricks.S3StorageInfo{
				Destination: dataplatformprojecthelpers.InitScriptS3URI(region, initScript),
				Region:      region,
			},
		})

		// If the Apache Sedona library is being added, also add the required spark configs.
		if initScript == "sedona-shaded-1.4.1-init.sh" {
			sparkConf["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
			sparkConf["spark.kryo.registrator"] = "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
			sparkConf["spark.sql.extensions"] = "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
		}
	}

	var apiLibraries []*databricks.Library
	for _, nodeLibrary := range node.SparkExecutionEnvironment().Libraries {
		// HACK: GeoSpark is special because it needs multiple jars
		if nodeLibrary == "geospark" {
			geosparkJars := dataplatformresource.GeoSparkJars(sparkVersion)
			for _, jar := range geosparkJars {
				apiLib, err := jar.ApiPayload(region)
				if err != nil {
					return "", oops.Wrapf(err, "")
				}
				apiLibraries = append(apiLibraries, apiLib)
			}
			continue
		}
		sparkLibrary, ok := supportedLibraries[nodeLibrary]
		if !ok {
			return "", oops.Errorf("Library %s is not supported currently. Reach out to Data Platform with your use case if you need a new library for a data pipeline node", nodeLibrary)
		}
		apiLibraries = append(apiLibraries, sparkLibrary)
	}

	// Datadog is required by all Data Pipeline Nodes to send metrics
	apiLibraries = append(apiLibraries, supportedLibraries[string(dataplatformresource.SparkPyPIDatadog)])

	sparkEnvVars := map[string]string{
		"AWS_DEFAULT_REGION":   region,
		"AWS_REGION":           region,
		"GOOGLE_CLOUD_PROJECT": "samsara-data",
		"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "data-pipelines-replication",

		// Runtime 7.x BigQuery connector configurations.
		// https://docs.databricks.com/data/data-sources/google/bigquery.html
		"GOOGLE_APPLICATION_CREDENTIALS": databricks.BigQueryCredentialsLocation(),
	}

	executionEnvironmentConfig := map[string]interface{}{
		"spark_version":  sparkVersion,
		"libraries":      apiLibraries,
		"spark_conf":     sparkConf,
		"spark_env_vars": sparkEnvVars,
		"cluster_log_conf": &databricks.ClusterLogConf{
			S3: &databricks.S3StorageInfo{
				Destination: fmt.Sprintf("s3://%sdatabricks-cluster-logs/datapipelines/%s", awsregionconsts.RegionPrefix[region], node.Name()),
				Region:      region,
			},
		},
	}

	executionEnvironmentConfig["data_security_mode"] = dataSecurityMode
	executionEnvironmentConfig["single_user_name"] = singleUserName

	if initScripts != nil {
		executionEnvironmentConfig["init_scripts"] = initScripts
	}

	executionEnvironment, err := json.MarshalIndent(executionEnvironmentConfig, "", "  ")
	if err != nil {
		return "", oops.Wrapf(err, "marshal cluster config")
	}

	return string(executionEnvironment), nil
}

func needsGeoSpark(node nodetypes.Node) bool {
	for _, lib := range node.SparkExecutionEnvironment().Libraries {
		if lib == "geospark" {
			return true
		}
	}

	return false
}

func createMetricsLoggerTaskState(stateName string, inputPath string, next statemachine.State) *statemachine.TaskState {
	state := statemachine.CreateAsyncLambdaTaskState(
		stateName,
		MetricsLoggerLambdaName,
		map[string]interface{}{
			"Output.$": inputPath,
		},
		nil,
		"Log result of transformation to datadog",
	)

	if next != nil {
		state.Next = next.Reference()
		state.End = false
	}

	return state
}

func generateMetricsLoggerErrorCatchAndStates(state *statemachine.TaskState) []statemachine.State {
	failedState := statemachine.CreateFailState(state.Name()+"-FAILED", "", "", state.Name()+" failed")
	failedMetricsLogger := createMetricsLoggerTaskState(state.Name()+"-FAILED_"+MetricsLoggerLambdaName, "$.error.Cause", failedState)
	failedMetricsLogger.ResultPath = statemachine.Path("null")
	state.Catch = []statemachine.ErrorCatcher{
		{
			ErrorEquals: []string{"States.ALL"},
			Next:        failedMetricsLogger.Reference(),
			ResultPath:  statemachine.Path("$.error"),
		},
	}

	return []statemachine.State{
		failedMetricsLogger,
		failedState,
	}
}
