package datapipelineprojects

import (
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsdynamodbresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/lambdas"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func pipelineMetastoreProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	resourceGroups := make(map[string][]tf.Resource)

	dynamoTable := &awsdynamodbresource.DynamoDBTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		Name:        "datapipeline-execution-state",
		BillingMode: awsdynamodbresource.PayPerRequest,
		HashKey:     "node_id",
		RangeKey:    "execution_id",
		Attributes: []awsdynamodbresource.DynamoDBTableAttribute{
			{
				KeyName: "node_id",
				Type:    awsdynamodbresource.String,
			},
			{
				KeyName: "execution_id",
				Type:    awsdynamodbresource.String,
			},
		},
		Tags: map[string]string{
			"Name":                   "datapipeline-execution-state",
			"samsara:service":        "datapipeline-execution-state",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}
	resourceGroups["dynamodb"] = []tf.Resource{dynamoTable}

	readPolicy := &awsresource.IAMPolicy{
		Name:        "datapipeline-execution-state-read",
		Description: "Read access to the data pipeline metastore",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Resource: []string{
						dynamoTable.ResourceId().ReferenceAttr("arn"),
					},
					Action: []string{
						"dynamodb:GetItem",
					},
				},
			},
		},
	}
	writePolicy := &awsresource.IAMPolicy{
		Name:        "datapipeline-execution-state-write",
		Description: "Write access to the data pipeline metastore",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Resource: []string{
						dynamoTable.ResourceId().ReferenceAttr("arn"),
					},
					Action: []string{
						"dynamodb:PutItem",
						"dynamodb:UpdateItem",
					},
				},
			},
		},
	}
	readArn := readPolicy.ResourceId().ReferenceAttr("arn")
	writeArn := writePolicy.ResourceId().ReferenceAttr("arn")
	resourceGroups["iam"] = []tf.Resource{
		readPolicy,
		writePolicy,
	}

	metastoreFunctionAndPolicies := map[string][]string{
		"handle_start":  {readArn, writeArn},
		"handle_end":    {readArn, writeArn},
		"handle_status": {readArn},
	}
	handlerFunctions := make(map[string]lambdas.LambdaFunction, len(metastoreFunctionAndPolicies))

	pythonLambdaFunctionRuntime := awsresource.LambdaFunctionRuntimePython310

	for handler, policyArns := range metastoreFunctionAndPolicies {
		handlerFunctions[handler] = &lambdas.PythonLambdaFunction{
			Name:            "pipeline_metastore_" + handler,
			SourceBasePath:  "python3/samsaradev/infra/dataplatform/lambda/data_pipelines/metastore",
			Region:          config.Region,
			Handler:         "handlers." + handler,
			ExtraPolicyArns: policyArns,
			Runtime:         pythonLambdaFunctionRuntime,
			RequirementsFiles: []string{
				"python3/requirements.lambda.txt",
			},
			TimeoutSeconds: 120,
			Tags: map[string]string{
				"samsara:service":       "pipeline_metastore",
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			},
		}
	}

	lambdaFiles := make(map[string]*project.ExtraFile)
	for handler, lambdaFunction := range handlerFunctions {
		functionFiles, functionResources, err := lambdaFunction.FilesAndResources(dataplatformresource.ArtifactBucket(config.Region))
		if err != nil {
			return nil, oops.Wrapf(err, "resources for handler %s", handler)
		}
		resourceGroups["lambda_"+handler] = functionResources

		for k, v := range functionFiles {
			lambdaFiles[k] = v
		}
	}

	return &project.Project{
		RootTeam:        datapipelinesTerraformProjectName,
		Provider:        providerGroup,
		Class:           "data-pipelines",
		Name:            "lambdas",
		Group:           "metastore",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
		ExtraFiles:      lambdaFiles,
	}, nil
}
