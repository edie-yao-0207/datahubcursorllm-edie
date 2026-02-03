package datapipelineprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions"
	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	stepfunctions2 "samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
	"samsaradev.io/infra/dataplatform/datapipelines/stepfunctions/transformations"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/deploy/helpers"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/reports/sparkreportregistry"
	"samsaradev.io/team"
)

var databricksLambdas = []string{
	transformations.DatabricksSQLTransformation,
	transformations.DatabricksPoller,
	transformations.SubmitJob,
}

var metastoreLambdas = []string{
	transformations.MetastoreHandleStart,
	transformations.MetastoreHandleStatus,
	transformations.MetastoreHandleEnd,
}

const (
	NESFName          = "sql-transformation-node"
	NESFBackfillName  = "sql-transformation-backfill-node"
	NESFPerNodePrefix = "nesf"
)

func sqlTransformationNodeExecutionStepFunction(region string) ([]tf.Resource, error) {
	var resources []tf.Resource
	policyArns := []string{
		"arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
		"arn:aws:iam::aws:policy/AWSStepFunctionsReadOnlyAccess",
	}

	lambdaNames := databricksLambdas
	lambdaNames = append(lambdaNames, metastoreLambdas...)
	resourceArns := make([]string, len(lambdaNames))
	for i, lambdaName := range lambdaNames {
		resourceArns[i] = tf.LocalId(fmt.Sprintf("%s_lambda_arn", lambdaName)).Reference()
	}
	policyStatements := []policy.AWSPolicyStatement{
		{
			Action:   []string{"lambda:InvokeFunction"},
			Effect:   "Allow",
			Resource: resourceArns,
		},
	}
	roleId, roleResources := dataplatformresource.StepFunctionRole(NESFName, policyArns, policyStatements)
	resources = append(resources, roleResources...)

	backfillNesfDefinition := transformations.BackfillNESF()
	nesfDefinition := transformations.PipelineNESF(region, false)
	backfillRndAllocation := "0"

	resources = append(resources,
		&stepfunctions.StepFunction{
			Name:       NESFBackfillName,
			Definition: &backfillNesfDefinition,
			RoleARN:    roleId.ReferenceAttr("arn"),
			Tags: map[string]string{
				"samsara:service":        "data-pipelines-backfill-nesf",
				"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
				"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
				"samsara:rnd-allocation": backfillRndAllocation,
			},
		},
		&stepfunctions.StepFunction{
			Name:       NESFName,
			Definition: &nesfDefinition,
			RoleARN:    roleId.ReferenceAttr("arn"),
			Tags: map[string]string{
				"samsara:service":       "data-pipelines-backfill-nesf",
				"samsara:team":          strings.ToLower(team.DataPlatform.Name()),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			},
		},
	)

	return resources, nil
}

func perNodeSQLTransformationResources(providerGroup string, region string) (map[string][]tf.Resource, error) {
	nodes, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		return nil, oops.Wrapf(err, "Error getting node configurations")
	}

	aggScript, err := dataplatformresource.DeployedArtifactObject(region, "python3/samsaradev/infra/dataplatform/reports/report_aggregator.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources := make(map[string][]tf.Resource, len(nodes))
	resources["report_aggregator"] = []tf.Resource{
		aggScript,
		&genericresource.StringLocal{
			Name:  "report_aggregator_url",
			Value: fmt.Sprintf(`"%s"`, aggScript.URL()),
		},
	}

	for _, node := range nodes {

		// Nodes are usually named like `<pipeline_name>.<table_name>`. Any `.` characters cause terraform plan
		// to break, so we sanitize them out here.
		sanitizedName := strings.ReplaceAll(node.Name, ".", "-")
		if _, ok := resources[sanitizedName]; ok {
			return nil, oops.Errorf("Node names must be unique; more than 1 node with name %s\n", node.Name)
		}
		nodeResources, err := generateNesf(node, providerGroup, region)
		if err != nil {
			return nil, oops.Wrapf(err, "Couldn't construct nesfs for node %s\n", node.Name)
		}

		resources[sanitizedName] = nodeResources
	}

	return resources, nil
}

func generateNesf(node configvalidator.NodeConfiguration, providerGroup string, region string) ([]tf.Resource, error) {
	var resources []tf.Resource
	policyArns := []string{
		"arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
		"arn:aws:iam::aws:policy/AWSStepFunctionsReadOnlyAccess",
	}

	lambdaNames := databricksLambdas
	lambdaNames = append(lambdaNames, metastoreLambdas...)
	resourceArns := make([]string, len(lambdaNames))
	for i, lambdaName := range lambdaNames {
		resourceArns[i] = tf.LocalId(fmt.Sprintf("%s_lambda_arn", lambdaName)).Reference()
	}
	policyStatements := []policy.AWSPolicyStatement{
		{
			Action:   []string{"lambda:InvokeFunction"},
			Effect:   "Allow",
			Resource: resourceArns,
		},
	}

	baseName := strings.ReplaceAll(NESFPerNodePrefix+"-"+node.Name, ".", "-")
	awsFriendyNodeNESFName := util.HashTruncate(baseName, 80, 8, "")
	roleId, roleResources := dataplatformresource.StepFunctionRole(awsFriendyNodeNESFName, policyArns, policyStatements)
	resources = append(resources, roleResources...)
	rndAllocation := "0"
	if node.NonProduction {
		rndAllocation = "1"
	}

	toShard := (node.Output.IsDateSharded() && !node.UnshardedBackfills) || strings.Contains(node.Name, "albert") || strings.Contains(node.Name, "jack")
	nesfDefinition := transformations.PipelineNESF(region, toShard)
	resources = append(resources,
		&stepfunctions.StepFunction{
			Name:       awsFriendyNodeNESFName,
			Definition: &nesfDefinition,
			RoleARN:    roleId.ReferenceAttr("arn"),
			Tags: map[string]string{
				"samsara:service":        "data-pipelines-nesf",
				"samsara:team":           strings.ToLower(node.Owner.Name()),
				"samsara:product-group":  strings.ToLower(team.TeamProductGroup[node.Owner.Name()]),
				"samsara:rnd-allocation": rndAllocation,
				"samsara:production":     fmt.Sprintf("%v", !node.NonProduction),
			},
		},
	)

	if node.SqliteReportConfig != nil {
		sqliteNesf, backfillSfn, sqliteResources, err := generateSqliteNodeResources(providerGroup, region, node)
		if err != nil {
			return nil, oops.Wrapf(err, "error generating sqlite state machine for node %s\n", node.Name)
		}

		sqliteNodeName := util.HashTruncate(baseName+"-sqlite", 80, 8, "")
		sqliteBackfillName := util.HashTruncate(baseName+"-sqlite-backfill", 80, 8, "")
		sqliteTriggerBackfillName := util.HashTruncate(baseName+"-sqlite-triggerbackfill", 80, 8, "")

		// create a sqlite node
		resources = append(resources,
			&stepfunctions.StepFunction{
				Name:       sqliteNodeName,
				Definition: sqliteNesf,
				RoleARN:    roleId.ReferenceAttr("arn"),
				Tags: map[string]string{
					"samsara:service":        "data-pipelines-nesf-sqlite",
					"samsara:team":           strings.ToLower(node.Owner.Name()),
					"samsara:product-group":  strings.ToLower(team.TeamProductGroup[node.Owner.Name()]),
					"samsara:rnd-allocation": rndAllocation,
					"samsara:production":     fmt.Sprintf("%v", !node.NonProduction),
				},
			},
		)
		resources = append(resources,
			&stepfunctions.StepFunction{
				Name:       sqliteBackfillName,
				Definition: backfillSfn,
				RoleARN:    roleId.ReferenceAttr("arn"),
				Tags: map[string]string{
					"samsara:service":        "data-pipelines-nesf-sqlite-backfill",
					"samsara:team":           strings.ToLower(node.Owner.Name()),
					"samsara:product-group":  strings.ToLower(team.TeamProductGroup[node.Owner.Name()]),
					"samsara:rnd-allocation": rndAllocation,
					"samsara:production":     fmt.Sprintf("%v", !node.NonProduction),
				},
			},
		)
		resources = append(resources, sqliteResources...)
		triggerBackfillResources := backfillSfnDefinition(providerGroup, region, sqliteTriggerBackfillName, sqliteBackfillName, stepfunctions2.GetSubmitJobBackfillInput(map[string]interface{}{}))
		resources = append(resources, triggerBackfillResources["iam"]...)
		resources = append(resources, triggerBackfillResources["sfn"]...)
	}

	return resources, nil
}

func generateSqliteNodeResources(providerGroup string, region string, node configvalidator.NodeConfiguration) (*statemachine.StepFunctionDefinition, *statemachine.StepFunctionDefinition, []tf.Resource, error) {
	var resources []tf.Resource

	// Find a report with the name specified in the node configuration.
	reports := sparkreportregistry.AllReportsInRegion(region)
	var report *sparkreportregistry.SparkReport
	reportName := (*node.SqliteReportConfig).ReportName
	for ind := range reports {
		r := reports[ind]
		if r.Name == reportName {
			report = &r
			break
		}
	}
	if report == nil {
		return nil, nil, nil, oops.Errorf("Could not find report in registry with name %s\n", reportName)
	}

	params, additionalResources, err := getSqliteParamsAndResources(report, region)
	if err != nil {
		return nil, nil, nil, err
	}
	resources = append(resources, additionalResources...)

	sparkVersion := sparkversion.DataPipelinesSqliteDbrVersion

	// This sparkConf variable was not being used in the original code.
	// TODO: Determine whether we need to use it.
	sparkConf := map[string]string{
		// Optimize range join on millisecond timestamp in BIGINT.
		// This potentially makes range join on TIMESTAMP type less performant.
		// https://docs.databricks.com/delta/join-performance/range-join.html
		"spark.databricks.optimizer.rangeJoin.binSize": "3600000",
		"spark.databricks.io.cache.enabled":            "true",
	}

	for i, conf := range report.SparkConf {
		sparkConf[i] = conf
	}

	libraries := []*databricks.Library{
		{
			Pypi: &databricks.PypiLibrary{
				Package: string(dataplatformresource.SparkPyPISnappy),
			},
		},
	}
	if report.GeoSparkRequired {
		for _, jar := range dataplatformresource.GeoSparkJars(sparkVersion) {
			libraries = append(libraries, &databricks.Library{Jar: string(jar)})
		}
	}

	name := node.Name + "-sqlite"
	driverPool := "datapipelines-nonproduction-driver"
	workerPool := "datapipelines-nonproduction-worker"
	rndAllocation := 1.0
	if !node.NonProduction {
		// If node is production, get pool based on random availability zone
		// For CA region, use "auto" for automatic availability zone selection to match pool creation logic
		var zoneId string
		if region == infraconsts.SamsaraAWSCARegion {
			zoneId = "auto"
		} else {
			allZones := infraconsts.GetDatabricksAvailabilityZones(region)
			zoneId = allZones[dataplatformhelpers.Hash(node.Name)%uint32(len(allZones))]
		}
		driverPool = fmt.Sprintf("datapipelines-production-ondemand-driver-fleet-%s", zoneId)
		workerPool = fmt.Sprintf("datapipelines-production-spot-worker-fleet-%s", zoneId)

		if region == infraconsts.SamsaraAWSEURegion || region == infraconsts.SamsaraAWSCARegion || name == "safety_report.score_event_report_v4-sqlite" {
			workerPool = fmt.Sprintf("datapipelines-production-ondemand-worker-fleet-%s", zoneId)
		}

		rndAllocation = 0.0
	}

	sparkConfNew := map[string]string{
		"spark.databricks.sql.initial.catalog.name":            "default",
		"databricks.loki.fileStatusCache.enabled":              "false",
		"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
		"spark.databricks.scan.modTimeCheck.enabled":           "false",
	}

	args := transformations.SubmitJobArgs{
		TransformationName: name,
		Team:               &node.Owner,
		ExecutionEnvironment: transformations.SparkExecutionEnvironment{
			SparkVersion: sparkVersion,
			ClusterLogConf: &databricks.ClusterLogConf{
				S3: &databricks.S3StorageInfo{
					Destination: fmt.Sprintf("s3://%sdatabricks-cluster-logs/datapipelines/%s", awsregionconsts.RegionPrefix[region], name),
					Region:      region,
				},
			},
			Libraries:               libraries,
			DriverInstancePoolName:  driverPool,
			WorkerInstancePoolName:  workerPool,
			InstanceProfileOverride: pointer.StringPtr(dataplatformprojects.ReportAggregationInstanceProfile),
			MaxWorkers:              pointer.IntPtr(dataplatformprojects.ReportAggregatorMaxWorkers),
			SparkConfOverrides:      sparkConfNew,
		},
		S3Path:         tf.LocalId("report_aggregator_url").Reference(),
		Parameters:     params,
		StartDateParam: "--start-date",
		EndDateParam:   "--end-date",

		// These are nil by default for most reports.
		HardcodedStartDate: node.SqliteReportConfig.HardcodedStartDate,
		HardcodedEndDate:   node.SqliteReportConfig.HardcodedEndDate,

		RnDCostAllocation: rndAllocation,
		IsProduction:      !node.NonProduction,

		JobType: dataplatformconsts.DataPipelinesSqliteNode,
	}

	sfn, err := transformations.SubmitJobStepFunction(providerGroup, region, args)
	backfillSfn, err := transformations.SubmitJobBackfillSfn(providerGroup, region, args)
	return &sfn, &backfillSfn, resources, err
}

// Return all the params necessary besides `--start_date` and `--end_date`.
// Most of this is copied directly from `infra/dataplatform/terraform/dataplatformprojects/datapipelineprojects/stepfunctions.go`
func getSqliteParamsAndResources(report *sparkreportregistry.SparkReport, region string) ([]string, []tf.Resource, error) {
	var resources []tf.Resource
	jobName := "sqlite-datapipeline"

	// Find the report.sql file
	queryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/report.sql", report.Name)
	query, err := dataplatformresource.DeployedArtifactObject(region, queryPath)
	if err != nil {
		return nil, nil, err
	}
	resources = append(resources, query)

	var rollup *awsresource.S3BucketObject
	if report.CustomRollupFile {
		rollupPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/rollup.sql", report.Name)
		rollup, err = dataplatformresource.DeployedArtifactObject(region, rollupPath)
		if err != nil {
			return nil, nil, oops.Wrapf(err, "query script object")
		}
		resources = append(resources, rollup)
	}

	var eventIntervalsQuery, detailedReportQuery *awsresource.S3BucketObject
	if report.HasDetailedReport {
		if report.HasEventIntervals {
			eventIntervalsQueryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/event_intervals.sql", report.Name)
			eventIntervalsQuery, err = dataplatformresource.DeployedArtifactObject(region, eventIntervalsQueryPath)
			if err != nil {
				return nil, nil, oops.Wrapf(err, "query script object")
			}
			resources = append(resources, eventIntervalsQuery)
		}

		detailedReportQueryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/detailed_report.sql", report.Name)
		detailedReportQuery, err = dataplatformresource.DeployedArtifactObject(region, detailedReportQueryPath)
		if err != nil {
			return nil, nil, oops.Wrapf(err, "query script object")
		}
		resources = append(resources, detailedReportQuery)
	}

	var aggColumns []string
	for _, col := range report.SelectColumns {
		aggColumns = append(aggColumns, strings.ToLower(col.String()))
	}

	entityKeyStrings := make([]string, 0, len(report.EntityKeys))
	for _, entityKey := range report.EntityKeys {
		entityKeyStrings = append(entityKeyStrings, string(entityKey))
	}

	s3Prefix := helpers.GetS3PrefixByRegion(region)

	parameters := []string{
		"--report-name", report.Name,
		"--s3sqlfile", query.URL(),
		"--temp-table-location", fmt.Sprintf("s3://%sreport-staging-tables/temp/%s/%s/", s3Prefix, report.Name, jobName),
		"--staging-table-location", fmt.Sprintf("s3://%sreport-staging-tables/report_aggregator/%s/", s3Prefix, report.Name),
		"--sink-path", fmt.Sprintf("s3://%spartial-report-aggregation/csv/%s/job=%s/", s3Prefix, report.Name, jobName),
		"--entity-keys", strings.Join(entityKeyStrings, ","),
		"--aggregation-columns", strings.Join(aggColumns, ","),
	}

	if !report.EnableSlidingWindows {
		parameters = append(parameters, "--tumbling-windows", "true")
	}
	if report.HasTumblingWindowsSunday {
		parameters = append(parameters, "--tumbling-windows-sunday", "true")
	}

	if report.GeoSparkRequired {
		parameters = append(parameters, "--geospark", "True")
	}
	if report.CustomRollupFile {
		parameters = append(parameters,
			"--custom-rollup-sql-file", rollup.URL(),
		)
	}
	if !report.DontSoftDeleteFromStagingTable {
		parameters = append(parameters, "--soft-delete-staging-table", "true")
	}
	if report.SoftDeleteFromMySQLTable {
		parameters = append(parameters, "--soft-delete-rollup", "true")
	}

	if len(report.OutputSortKeys) != 0 {
		parameters = append(parameters, "--output-sort-keys", strings.Join(report.OutputSortKeys, ","))
	}

	if report.SQLiteOnly {
		parameters = append(parameters, "--sqlite-only", "true")
	}

	if report.HasDetailedReport {
		detailedParameters := []string{
			"--has-detailed-report", "true",
			"--s3-detailed-report-sql-file", detailedReportQuery.URL(),
			"--detailed-temp-table-location", fmt.Sprintf("s3://%sreport-staging-tables/temp/%s/%s/", s3Prefix, report.DetailedTableName, jobName),
			"--detailed-sink-path", fmt.Sprintf("s3://%spartial-report-aggregation/csv/%s/job=%s/", s3Prefix, report.DetailedTableName, jobName),
		}

		if len(report.DetailedOutputSortKeys) != 0 {
			detailedParameters = append(detailedParameters,
				"--detailed-output-sort-keys", strings.Join(report.DetailedOutputSortKeys, ","),
			)
		}

		if report.HasEventIntervals {
			detailedParameters = append(detailedParameters,
				"--has-event-intervals", "true",
				"--s3-event-intervals-sql-file", eventIntervalsQuery.URL(),
				"--event-staging-table-location", fmt.Sprintf("s3://%sreport-staging-tables/report_aggregator/event_intervals_%s/", s3Prefix, report.Name),
				"--detailed-event-keys", strings.Join(report.DetailedEventKeys, ","),
			)
		}

		parameters = append(parameters, detailedParameters...)
	}

	if report.OnlyComputeDetailedReport {
		parameters = append(parameters, "--only-compute-detailed-report", "true")
	}

	parameters = append(parameters,
		"--write-sqlite-output", "true",
		"--sqlite-output-bucket", fmt.Sprintf("%spartial-report-aggregation", s3Prefix),
		"--sqlite-output-prefix", fmt.Sprintf("sqlite/%s/", report.Name),
		"--sqlite-table-name", report.Name,
		"--drop-empty", "true",
	)
	if report.HasDetailedReport {
		parameters = append(parameters,
			"--sqlite-detailed-output-prefix", fmt.Sprintf("sqlite/%s/", report.DetailedTableName),
			"--sqlite-detailed-table-name", report.DetailedTableName,
			"--detailed-time-column-name", report.DetailedTimeColumnName,
			"--detailed-primary-keys", strings.Join(report.DetailedPrimaryKeys, ","),
		)
	}

	// Add our flag to aggregate and sqlite
	parameters = append(parameters, "--aggregate-and-sqlite", "true")

	return parameters, resources, nil
}

// dataPipelineStepFunctions generates terraform resources for all data-pipelines.
func dataPipelineStepFunctions(transformationFiles map[string]*stepfunctions2.TransformationFiles, region string) (map[string][]tf.Resource, error) {
	resourceGroups := make(map[string][]tf.Resource)
	var iamResources []tf.Resource
	policyArns := []string{
		"arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
		"arn:aws:iam::aws:policy/AWSStepFunctionsReadOnlyAccess",
		"arn:aws:iam::aws:policy/CloudWatchEventsFullAccess",
	}

	dagsWithSfns, err := stepfunctions2.GenerateDataPipelineStepFunctions(transformationFiles, region)
	if err != nil {
		return nil, err
	}

	roleId, roleResources := dataplatformresource.StepFunctionRole(
		"pipelines",
		policyArns,
		[]policy.AWSPolicyStatement{
			{
				Action: []string{"lambda:InvokeFunction"},
				Effect: "Allow",
				Resource: []string{
					fmt.Sprintf(
						"arn:aws:lambda:%s:%d:function:%s",
						region,
						infraconsts.GetDatabricksAccountIdForRegion(region),
						stepfunctions2.MetricsLoggerLambdaName,
					),
				},
			},
		},
	)

	iamResources = append(iamResources, roleResources...)
	resourceGroups["iam"] = iamResources

	var sfnResources []tf.Resource
	var dagsWithResources []*stepfunctions2.DagWithTerraformSfn

	for _, dagWithSfn := range dagsWithSfns {
		dag := dagWithSfn.Dag
		sfn := dagWithSfn.SfnDefinition

		rndAllocation := "1"
		if dag.IsProduction() {
			rndAllocation = "0"
		}

		sfnResource := &stepfunctions.StepFunction{
			Name:       strings.ReplaceAll(util.HashTruncate(sfn.Name, 64, 10, "_"), ".", "-"),
			Definition: sfn,
			RoleARN:    roleId.ReferenceAttr("arn"),
			Tags: map[string]string{
				"samsara:service":        "data-pipelines-pipeline",
				"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
				"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
				"samsara:rnd-allocation": rndAllocation,
			},
		}
		sfnResources = append(sfnResources, sfnResource)
		dagsWithResources = append(dagsWithResources, &stepfunctions2.DagWithTerraformSfn{Dag: dag, Resource: sfnResource})
		resourceGroups[strings.Replace(fmt.Sprintf("%s_sfn", sfn.Name), ".", "-", -1)] = []tf.Resource{sfnResource}
	}

	// Set up an orchestration function for our normal 3 hour runs.
	orchestationSFN := stepfunctions2.GenerateDataPipelineOrchestrationStepFunction(dagsWithResources)
	orchestationSFNResource := &stepfunctions.StepFunction{
		Name:       orchestationSFN.Name, // data_pipeline_orchestration_sfn
		Definition: orchestationSFN,
		RoleARN:    roleId.ReferenceAttr("arn"),
		Tags: map[string]string{
			"samsara:service":        "data-pipelines-three-hour-orchestration",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}
	resourceGroups["orchestration_stepfunction"] = []tf.Resource{orchestationSFNResource}

	// Set up an orchestration function for our once per day run.
	dailyOrchestrationSfn := stepfunctions2.GenerateDailyOrchestrationSfn(dagsWithResources)
	dailyOrchestrationSfnResource := &stepfunctions.StepFunction{
		Name:       dailyOrchestrationSfn.Name, // daily_data_pipeline_orchestration_sfn
		Definition: dailyOrchestrationSfn,
		RoleARN:    roleId.ReferenceAttr("arn"),
		Tags: map[string]string{
			"samsara:service":        "data-pipelinees-daily-orchestration",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}
	resourceGroups["daily_orchestration_stepfunction"] = []tf.Resource{dailyOrchestrationSfnResource}

	return resourceGroups, nil
}
