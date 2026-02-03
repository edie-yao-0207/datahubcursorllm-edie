package datapipelineprojects

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

var TransformationDir = filepath.Join(dataplatformterraformconsts.DataPlatformRoot, "tables/transformations")

// datapipelinesTerraformProjectName is the name of the datapipelines terraform buildkite pipeline.
// https://github.com/samsara-dev/backend/blob/master/terraform/buildkite/team-pipelines.tf#L997
// https://buildkite.com/samsara/terraform-aws-project-datapipelines
const datapipelinesTerraformProjectName = "DataPlatform-DataPipelines"

func AllProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	var projects []*project.Project

	databricksProject, err := pipelineDatabricksProjects(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "databricks project")
	}
	projects = append(projects, databricksProject)

	databricksProjectRemote, err := databricksProject.RemoteStateResource(nil)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating remote for databricks data pipelines project")
	}
	databricksLambdaResources := []tf.Resource{databricksProjectRemote}
	for _, lambdaName := range databricksLambdas {
		databricksLambdaResources = append(databricksLambdaResources, &genericresource.StringLocal{
			Name:  fmt.Sprintf("%s_lambda_arn", lambdaName),
			Value: fmt.Sprintf(`"%s"`, databricksProjectRemote.ResourceId().ReferenceOutput(fmt.Sprintf("aws_lambda_function_%s_arn", lambdaName))),
		})
	}

	backfillProject, err := pipelineBackfillProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "backfill project")
	}
	projects = append(projects, backfillProject)

	metastoreProject, err := pipelineMetastoreProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "metastore project")
	}
	projects = append(projects, metastoreProject)

	metastoreProjectRemote, err := metastoreProject.RemoteStateResource(nil)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating remote for metastore data pipelines project")
	}
	metastoreLambdaResources := []tf.Resource{metastoreProjectRemote}
	for _, lambdaName := range metastoreLambdas {
		metastoreLambdaResources = append(metastoreLambdaResources, &genericresource.StringLocal{
			Name:  fmt.Sprintf("%s_lambda_arn", lambdaName),
			Value: fmt.Sprintf(`"%s"`, metastoreProjectRemote.ResourceId().ReferenceOutput(fmt.Sprintf("aws_lambda_function_%s_arn", lambdaName))),
		})
	}

	nodeExecutionResources, err := sqlTransformationNodeExecutionStepFunction(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	perNodeNESFResources, err := perNodeSQLTransformationResources(providerGroup, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	sqlTransformationNodeProject := &project.Project{
		RootTeam:        datapipelinesTerraformProjectName,
		Provider:        providerGroup,
		Class:           "data-pipelines",
		Name:            "step-functions",
		Group:           "sql-transformation-node",
		GenerateOutputs: true,
		ResourceGroups: project.MergeResourceGroups(
			map[string][]tf.Resource{
				"stepfunction":              nodeExecutionResources,
				"databricks_lambdas_remote": databricksLambdaResources,
				"metastore_lambdas_remote":  metastoreLambdaResources,
			},
			perNodeNESFResources,
		),
	}
	projects = append(projects, sqlTransformationNodeProject)

	sqlTransformationNodeRemote, err := sqlTransformationNodeProject.RemoteStateResource(nil)
	if err != nil {
		return nil, oops.Wrapf(err, "Error create remote state resources for sqltransformationnode project")
	}
	sqlTransformationNodeResources := []tf.Resource{
		sqlTransformationNodeRemote,
		&genericresource.StringLocal{
			Name:  "sql_transformation_node_arn",
			Value: fmt.Sprintf(`"%s"`, sqlTransformationNodeRemote.ResourceId().ReferenceOutput("aws_sfn_state_machine_sql-transformation-node")),
		},
	}

	transformationArtifactResources, sqlTransformationInfo, err := getSqlTransformationInfo(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	transformationS3Files := sqlTransformationInfo.nodeNameToS3Files

	sfnResourceMap, err := dataPipelineStepFunctions(transformationS3Files, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	cloudWatch, err := cloudWatchResources(sfnResourceMap["orchestration_stepfunction"][0], sfnResourceMap["daily_orchestration_stepfunction"][0])
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	costAttributionTags, err := generateCostAttributionTags(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating cost attribution tags")
	}

	sfnPipelinesProject := &project.Project{
		RootTeam: datapipelinesTerraformProjectName,
		Provider: providerGroup,
		Class:    "data-pipelines",
		Name:     "step-functions",
		Group:    "pipelines",
		ResourceGroups: map[string][]tf.Resource{
			"sql_transformation_node_remote": sqlTransformationNodeResources,
			"sql_transformation_artifacts":   transformationArtifactResources,
			"databricks_provider":            dataplatformresource.DatabricksOauthProvider(config.Hostname),
			"cloudwatch":                     cloudWatch,
			"cost_attribution_tags":          costAttributionTags,
		},
	}

	sfnPipelinesProject.ResourceGroups = project.MergeResourceGroups(sfnPipelinesProject.ResourceGroups, sfnResourceMap)
	projects = append(projects, sfnPipelinesProject)

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}
	return projects, nil
}

type sqlTransformationInfo struct {
	nodeNameToS3Files      map[string]*stepfunctions.TransformationFiles
	nodeNameToIsProduction map[string]bool
}

func getSqlTransformationInfo(region string) ([]tf.Resource, sqlTransformationInfo, error) {
	var s3bucketResources []tf.Resource
	nodeNameToS3Files := make(map[string]*stepfunctions.TransformationFiles)
	nodeNameToIsProduction := make(map[string]bool)
	disabledPipelines := []string{}
	walkErr := filepath.Walk(TransformationDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || strings.HasSuffix(info.Name(), ".test.json") || info.Name() == "CODEREVIEW" {
			return nil
		}

		relativePath, err := filepath.Rel(dataplatformterraformconsts.BackendRoot, path)
		if err != nil {
			return oops.Wrapf(err, "")
		}

		s3BucketObj, err := dataplatformresource.DeployedArtifactObject(region, relativePath)
		if err != nil {
			return oops.Wrapf(err, "")
		}
		s3bucketResources = append(s3bucketResources, s3BucketObj)

		splitFilePath := strings.Split(path, "/")
		transformationName := strings.TrimSuffix(strings.Join(splitFilePath[len(splitFilePath)-2:], "."), filepath.Ext(info.Name()))
		transformationName = strings.TrimSuffix(transformationName, ".expectation")
		if _, ok := nodeNameToS3Files[transformationName]; !ok {
			nodeNameToS3Files[transformationName] = &stepfunctions.TransformationFiles{Region: region}
		}

		if filepath.Ext(info.Name()) == ".json" {
			nodeNameToS3Files[transformationName].JSONMetadataPath = s3BucketObj.URL()

			// small unmarshal to:
			// 1. get team owner into transformation files struct to be used in periodic audit job
			// 2. figure out whether pipeline is disabled in this region
			var metadata struct {
				TeamOwner       string   `json:"owner"`
				DisabledRegions []string `json:"disabled_regions"`
				NonProduction   bool     `json:"nonproduction"`
			}

			fileBytes, err := ioutil.ReadFile(path)
			if err != nil {
				return oops.Wrapf(err, "error reading config file %s", path)
			}

			jsonErr := json.Unmarshal(fileBytes, &metadata)
			if jsonErr != nil {
				return oops.Wrapf(jsonErr, "error unmarshaling JSON")
			}

			// if disabled, add to array so it can be deleted from final result
			for _, disabledRegion := range metadata.DisabledRegions {
				if disabledRegion == region {
					disabledPipelines = append(disabledPipelines, transformationName)
				}
			}

			// convert team owner string to team info struct
			if teamInfo, ok := team.TeamByName[metadata.TeamOwner]; ok {
				nodeNameToS3Files[transformationName].TeamOwner = teamInfo
			} else {
				return oops.Errorf(
					"No team owner found for transformation %s. Please check that team owner is defined in the json config file and spelled correctly. Teams can be found under the team directory", transformationName)
			}

			nodeNameToIsProduction[transformationName] = !metadata.NonProduction
		}

		if strings.HasSuffix(info.Name(), ".expectation.sql") {
			nodeNameToS3Files[transformationName].ExpectationPath = s3BucketObj.URL()
		} else if filepath.Ext(info.Name()) == ".sql" {
			if strings.Contains(transformationName, "material_usage") {
				s3BucketObjNoHash, err := dataplatformresource.DeployedArtifactObjectNoHash(region, relativePath)
				if err != nil {
					return oops.Wrapf(err, "")
				}
				s3bucketResources = append(s3bucketResources, s3BucketObjNoHash)
				nodeNameToS3Files[transformationName].SQLPath = s3BucketObjNoHash.URL()
			} else {
				nodeNameToS3Files[transformationName].SQLPath = s3BucketObj.URL()
			}
		}
		return nil
	})

	if walkErr != nil {
		return nil, sqlTransformationInfo{}, oops.Wrapf(walkErr, "Error walking transformations directory to generate terraform artifacts")
	}

	// If disabled, delete from nodeNameToS3Files
	for _, pipeline := range disabledPipelines {
		delete(nodeNameToS3Files, pipeline)
	}

	return s3bucketResources, sqlTransformationInfo{nodeNameToS3Files: nodeNameToS3Files, nodeNameToIsProduction: nodeNameToIsProduction}, nil
}

func cloudWatchResources(threeHourOrchestrationSfn tf.Resource, dailyOrchestrationSfn tf.Resource) ([]tf.Resource, error) {
	var resources []tf.Resource

	cloudWatchRole := &awsresource.IAMRole{
		Name:        "datapipelines-start-sfn-execution-cloudwatch",
		Description: "Role assumed when starting Step Function execution from CloudWatch rule",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Principal: map[string]string{
						"Service": "events.amazonaws.com",
					},
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "datapipelines-start-sfn-execution-cloudwatch",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	cloudWatchSfnExecution := &awsresource.IAMRolePolicy{
		Name: "cloudwatch-sfn-start-execution-policy-attachment",
		Role: cloudWatchRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{"states:StartExecution"},
					Resource: []string{
						threeHourOrchestrationSfn.ResourceId().ReferenceAttr("id"),
						dailyOrchestrationSfn.ResourceId().ReferenceAttr("id"),
					},
				},
			},
		},
	}
	resources = append(resources, cloudWatchRole, cloudWatchSfnExecution)

	// CloudWatch Event rule to start data pipeline sfn's every 3 hours
	cloudWatchEventRule3hrResource := &awsresource.CloudWatchEventRule{
		Name:                "pipelines-state-machine-start",
		Description:         "Start data pipelines state machines every 3 hours",
		ScheduledExpression: "cron(59 0/3 * * ? *)",
		IsEnabled:           true,
		Tags: map[string]string{
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:service":       "data-pipelines",
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, cloudWatchEventRule3hrResource)

	rawInputTemplate3hr := `{
\"execution_id\": <id>,
\"start_date\": \"DATE_SUB(CURRENT_DATE(), 2)\",
\"end_date\": \"DATE_ADD(CURRENT_DATE(), 1)\",
\"pipeline_execution_time\": <time>
}`

	cloudWatchEventTarget3hrResource := awsresource.CloudWatchEventTarget{
		Rule:    cloudWatchEventRule3hrResource.ResourceId().ReferenceAttr("name"),
		Arn:     threeHourOrchestrationSfn.ResourceId().ReferenceAttr("id"),
		RoleArn: cloudWatchRole.ResourceId().ReferenceAttr("arn"),
		InputTransformer: &awsresource.InputTransformer{
			InputPaths:    map[string]string{"id": "$.id", "time": "$.time"},
			InputTemplate: rawInputTemplate3hr,
		},
	}.WithContext(tf.Context{
		Detail: fmt.Sprintf("datapipelines-3hr-execution-%s", threeHourOrchestrationSfn.ResourceId().Name),
	})
	resources = append(resources, cloudWatchEventTarget3hrResource)

	// CloudWatch Event rule to start data pipeline sfn's every 3 hours
	cloudWatchEventRuleDaily := &awsresource.CloudWatchEventRule{
		Name:                "pipelines-state-machine-start-daily",
		Description:         "Start the daily data pipeline state machine.",
		ScheduledExpression: "cron(59 1 * * ? *)",
		IsEnabled:           true,
		Tags: map[string]string{
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:service":       "data-pipelines",
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, cloudWatchEventRuleDaily)

	rawTemplateDaily := `{
\"execution_id\": <id>,
\"pipeline_execution_time\": <time>
}`

	cloudWatchEventTargetDaily := awsresource.CloudWatchEventTarget{
		Rule:    cloudWatchEventRuleDaily.ResourceId().ReferenceAttr("name"),
		Arn:     dailyOrchestrationSfn.ResourceId().ReferenceAttr("id"),
		RoleArn: cloudWatchRole.ResourceId().ReferenceAttr("arn"),
		InputTransformer: &awsresource.InputTransformer{
			InputPaths:    map[string]string{"id": "$.id", "time": "$.time"},
			InputTemplate: rawTemplateDaily,
		},
	}.WithContext(tf.Context{
		Detail: fmt.Sprintf("datapipelines-daily-execution-%s", dailyOrchestrationSfn.ResourceId().Name),
	})
	resources = append(resources, cloudWatchEventTargetDaily)

	return resources, nil
}

func generateCostAttributionTags(region string) ([]tf.Resource, error) {

	pipelines, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "error building DAGs")
	}

	localPath := "go/src/samsaradev.io/infra/dataplatform/datapipelines/nodes_cost_attribution_tags.csv"
	path := filepath.Join(filepathhelpers.BackendRoot, localPath)
	csvFile, err := os.Create(path)
	defer csvFile.Close()
	if err != nil {
		return nil, oops.Wrapf(err, "failed creating csv file")
	}
	csvWriter := csv.NewWriter(csvFile)

	tagInfoSlice := [][]string{}

	for _, pipeline := range pipelines {
		for _, node := range pipeline.GetNodes() {

			rndAllocation := "1"
			if node.NonProduction() {
				rndAllocation = "0"
			}

			tags := map[string]string{
				"samsara:pooled-job:service":               fmt.Sprintf("data-pipeline-node-%s", strings.Replace(node.Name(), ".", "_", -1)),
				"samsara:pooled-job:team":                  node.Owner().TeamName,
				"samsara:pooled-job:product-group":         team.TeamProductGroup[node.Owner().TeamName],
				"samsara:pipeline-node-name":               node.Name(),
				"samsara:pooled-job:rnd-allocation":        rndAllocation,
				"samsara:pooled-job:is-production-job":     strconv.FormatBool(!node.NonProduction()),
				"samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation",
			}

			jsonString, err := json.Marshal(tags)
			if err != nil {
				return nil, oops.Wrapf(err, "error marshalling mapping to json")
			}

			tagInfoSlice = append(tagInfoSlice, []string{
				node.Name(),
				string(jsonString),
			})

		}
	}

	// Sort the tagInfoSlice by node name to ensure consistent order.
	sort.Slice(tagInfoSlice, func(i, j int) bool {
		return tagInfoSlice[i][0] < tagInfoSlice[j][0]
	})

	for _, tagInfo := range tagInfoSlice {
		if err := csvWriter.Write(tagInfo); err != nil {
			return nil, oops.Wrapf(err, "csvWriter.Write")
		}

	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, oops.Wrapf(err, "csv error")

	}

	s3Resource := &awsresource.S3BucketObject{
		ResourceName: "datapipelines_node_tags",
		Bucket:       dataplatformresource.ArtifactBucket(region),
		Key:          "others/dataplatform/datapipelines/nodes_cost_attribution_tags.csv",
		Source: filepath.Join(
			tf.LocalId("backend_root").Reference(),
			localPath,
		),
	}
	return []tf.Resource{s3Resource}, nil
}
