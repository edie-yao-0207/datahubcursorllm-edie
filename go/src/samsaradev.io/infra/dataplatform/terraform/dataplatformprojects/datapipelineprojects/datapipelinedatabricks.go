package datapipelineprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsdynamodbresource"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/lambdas"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func pipelineDatabricksProjects(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	resourceGroups := make(map[string][]tf.Resource)

	datapipelinesVPC, err := dataPipelinesVPC(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating the data pipelines vpc")
	}
	resourceGroups["vpc"] = datapipelinesVPC.Resources()
	// Add Route53ResolverDnssecConfig resource
	dnssecConfig := &awsresource.Route53ResolverDnssecConfig{
		Name:  fmt.Sprintf("datapipelines-%s-dnssec", config.Region),
		VpcID: datapipelinesVPC.VPC.ResourceId().Reference(),
	}
	resourceGroups["vpc"] = append(resourceGroups["vpc"], dnssecConfig)
	// set up route 53 DNS resolver with logging enabled
	dnsResolverResources := dataplatformresource.DNSResolverResources(config.Region, "datapipelines", datapipelinesVPC.VPC.ResourceId().Reference())
	resourceGroups["vpc"] = append(resourceGroups["vpc"], dnsResolverResources...)

	resourceGroups["vpc_flow_logs"] = dataplatformresource.VpcFlowLogsResource(
		fmt.Sprintf("vpc-flow-logs-datapipelines-lambda-%s", config.Region),
		datapipelinesVPC.VPC.ResourceId().Reference(),
		config.Region,
	)

	lambdaFunctions, extraResources, err := databricksLambdaFunctions(config.Region, providerGroup, datapipelinesVPC.VPC.ResourceId().Reference())
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resourceGroups["s3_artifacts"] = extraResources

	lambdaFiles := make(map[string]*project.ExtraFile)
	for _, lambdaFunction := range lambdaFunctions {
		functionFiles, functionResources, err := lambdaFunction.FilesAndResources(dataplatformresource.ArtifactBucket(config.Region))
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		resourceGroups["lambda_"+lambdaFunction.LambdaFunctionName()] = functionResources

		for k, v := range functionFiles {
			lambdaFiles[k] = v
		}
	}

	resourceGroups["databricks_provider"] = dataplatformresource.DatabricksOauthProvider(config.Hostname)

	dynamoResources := nesfDynamoResources()
	resourceGroups = project.MergeResourceGroups(resourceGroups, dynamoResources)

	return &project.Project{
		RootTeam:        datapipelinesTerraformProjectName,
		Provider:        providerGroup,
		Class:           "data-pipelines",
		Name:            "lambdas",
		Group:           "databricks",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
		ExtraFiles:      lambdaFiles,
	}, nil
}

func databricksLambdaFunctions(region string, providerGroup string, vpcId string) ([]lambdas.LambdaFunction, []tf.Resource, error) {
	var extraResources []tf.Resource
	var lambdaFunctions []lambdas.LambdaFunction

	sqlTransformationScript, err := dataplatformresource.DeployedArtifactObject(region, "python3/samsaradev/infra/dataplatform/datapipelines/sql_transformation.py")
	if err != nil {
		return nil, nil, oops.Wrapf(err, "")
	}
	extraResources = append(extraResources, sqlTransformationScript)

	databricksReadParameterStorePolicy := policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{"ssm:GetParameter"}, Resource: []string{
			fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/DATABRICKS_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/E2_DATABRICKS_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
			fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/DATADOG_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
		},
	}

	subnetIds := []string{
		fmt.Sprintf("${aws_subnet.datapipelines-lambdas-%sa-subnet-private.id}", region),
		fmt.Sprintf("${aws_subnet.datapipelines-lambdas-%sb-subnet-private.id}", region),
	}

	// Canada region doesnt support `c` AZ, but instead we have `d`.
	if region == infraconsts.SamsaraAWSCARegion {
		subnetIds = append(subnetIds, fmt.Sprintf("${aws_subnet.datapipelines-lambdas-%sd-subnet-private.id}", region))
	} else {
		subnetIds = append(subnetIds, fmt.Sprintf("${aws_subnet.datapipelines-lambdas-%sc-subnet-private.id}", region))
	}

	// This is a completely open security group from egress. The ingress is locked to
	// only the internal traffic and port 443 is opened to outside.
	securityGroup := awsresource.SecurityGroup{
		Name:        fmt.Sprintf("datapipelines-lambdas-%s-sg", region),
		Description: "Security group for datapipelines-lambdas",
		VpcID:       vpcId,
		Tags:        vpcTags(),
	}

	extraResources = append(extraResources, &securityGroup)

	// Restrict ingress only to internal network within SG.
	ingressTcpRule := awsresource.SecurityGroupRule{
		Name:            fmt.Sprintf("datapipelines-lambdas-%s-all-internal-tcp-sg-ingress-rule", region),
		Type:            awsresource.SecurityGroupIngress,
		SecurityGroupID: securityGroup.ResourceId().Reference(),
		Description:     "Ingress rule for datapipelines-lambdas security group to allow-tcp-ingress-internal",
		FromPort:        awsresource.SecurityGroupPortAny,
		ToPort:          awsresource.SecurityGroupPortAny,
		Protocol:        "tcp",
		Self:            true,
	}
	ingressUdpRule := awsresource.SecurityGroupRule{
		Name:            fmt.Sprintf("datapipelines-lambdas-%s-all-internal-udp-sg-ingress-rule", region),
		Type:            awsresource.SecurityGroupIngress,
		SecurityGroupID: securityGroup.ResourceId().Reference(),
		Description:     "Ingress rule for datapipelines-lambdas security group to allow-udp-ingress-internal",
		FromPort:        awsresource.SecurityGroupPortAny,
		ToPort:          awsresource.SecurityGroupPortAny,
		Protocol:        "udp",
		Self:            true,
	}
	ingress443Rule := awsresource.SecurityGroupRule{
		Name:            fmt.Sprintf("datapipelines-lambdas-%s-sg-ingress-443-rule", region),
		Type:            awsresource.SecurityGroupIngress,
		SecurityGroupID: securityGroup.ResourceId().Reference(),
		Description:     "Ingress rule for datapipelines-lambdas security group to allow 443",
		CIDRBlocks:      []string{"0.0.0.0/0"},
		FromPort:        "443",
		ToPort:          "443",
		Protocol:        "tcp",
	}

	extraResources = append(extraResources, &ingressTcpRule, &ingressUdpRule, &ingress443Rule)

	egressRule := awsresource.SecurityGroupRule{
		Name:            fmt.Sprintf("datapipelines-lambdas-%s-sg-egress-rule", region),
		Type:            awsresource.SecurityGroupEgress,
		SecurityGroupID: securityGroup.ResourceId().Reference(),
		Description:     "Egress rule for datapipelines-lambdas security group",
		CIDRBlocks:      []string{"0.0.0.0/0"},
		FromPort:        awsresource.SecurityGroupPortAny,
		ToPort:          awsresource.SecurityGroupPortAny,
		Protocol:        awsresource.SecurityGroupProtocolAny,
	}

	extraResources = append(extraResources, &egressRule)

	securityGroupIds := []string{
		securityGroup.ResourceId().Reference(),
	}

	datapipelinesArn, err := dataplatformresource.InstanceProfileArn(providerGroup, "data-pipelines-cluster")
	if err != nil {
		return nil, nil, oops.Wrapf(err, "")
	}

	// IAM policy so sql transformation can interface with dynamo retry count table
	retryCountDynamoAccessPolicy := policy.AWSPolicyStatement{
		Effect: "Allow",
		Action: []string{"dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem", "dynamodb:UpdateItem"},
		Resource: []string{
			fmt.Sprintf("arn:aws:dynamodb:%s:%d:table/datapipeline-node-retry-count", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
		},
	}

	pythonLambdaFunctionRuntime := awsresource.LambdaFunctionRuntimePython310

	databricksSQLTransformationLambda := &lambdas.PythonLambdaFunction{
		Name:           "databricks_sql_transformation",
		SourceBasePath: "python3/samsaradev/infra/dataplatform/lambda/data_pipelines/databricks",
		Region:         region,
		Handler:        "sql_transformation.lambda_handler",
		Runtime:        pythonLambdaFunctionRuntime,
		Environment: map[string]string{
			"SQL_TRANSFORMATION_SCRIPT_S3_PATH": sqlTransformationScript.URL(),
			"INSTANCE_PROFILE_ARN":              datapipelinesArn,
		},
		ExtraPolicyStatements: []policy.AWSPolicyStatement{databricksReadParameterStorePolicy, retryCountDynamoAccessPolicy},
		RequirementsFiles: []string{
			"python3/requirements.lambda.txt",
		},
		RunInVPC:         true,
		SubnetIds:        subnetIds,
		SecurityGroupIds: securityGroupIds,
		TimeoutSeconds:   900, // 15 minutes
		Tags: map[string]string{
			"samsara:service":       "databricks_sql_transformation",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	lambdaFunctions = append(lambdaFunctions, databricksSQLTransformationLambda)

	databricksPollerLambda := &lambdas.PythonLambdaFunction{
		Name:                  "databricks_poller",
		SourceBasePath:        "python3/samsaradev/infra/dataplatform/lambda/data_pipelines/databricks",
		Region:                region,
		Handler:               "poller.lambda_handler",
		Runtime:               pythonLambdaFunctionRuntime,
		ExtraPolicyStatements: []policy.AWSPolicyStatement{databricksReadParameterStorePolicy},
		RequirementsFiles: []string{
			"python3/requirements.lambda.txt",
		},
		RunInVPC:         true,
		SubnetIds:        subnetIds,
		SecurityGroupIds: securityGroupIds,
		TimeoutSeconds:   120,
		Tags: map[string]string{
			"samsara:service":       "databricks_poller",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	lambdaFunctions = append(lambdaFunctions, databricksPollerLambda)

	submitJobsLambda := &lambdas.GoLambdaFunction{
		Name:     "submit_job",
		MainFile: "go/src/samsaradev.io/infra/dataplatform/lambdafunctions/datapipelines/submit_job/main.go",
		Region:   region,
		ExtraPolicyStatements: []policy.AWSPolicyStatement{
			policy.AWSPolicyStatement{
				Effect: "Allow",
				Action: []string{"ssm:GetParameter"}, Resource: []string{
					fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/E2_DATABRICKS_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
					fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/DATABRICKS_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
				},
			},
		},
		RunInVPC:         true,
		SubnetIds:        subnetIds,
		SecurityGroupIds: securityGroupIds,
		TimeoutSeconds:   60,
		Tags: map[string]string{
			"samsara:service":       "submit_job",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		Publish: pointer.BoolPtr(true),
	}
	lambdaFunctions = append(lambdaFunctions, submitJobsLambda)

	logDatadogMetricsLambda := &lambdas.GoLambdaFunction{
		Name:     "metrics_logger",
		MainFile: "go/src/samsaradev.io/infra/dataplatform/lambdafunctions/datapipelines/metrics_logger/main.go",
		Region:   region,
		ExtraPolicyStatements: []policy.AWSPolicyStatement{
			{
				Effect: "Allow",
				Action: []string{"ssm:GetParameter"},
				Resource: []string{
					fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/E2_DATABRICKS_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
					fmt.Sprintf("arn:aws:ssm:%s:%d:parameter/DATADOG_*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"states:DescribeExecution",
					"states:DescribeStateMachine",
					"states:DescribeStateMachineForExecution",
					"states:GetExecutionHistory",
				},
				Resource: []string{"*"},
			},
		},
		RunInVPC:         true,
		SubnetIds:        subnetIds,
		SecurityGroupIds: securityGroupIds,
		TimeoutSeconds:   5 * 60,
		MemorySizeInMB:   1024,
		Tags: map[string]string{
			"samsara:service":       "submit_job",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		Publish: pointer.BoolPtr(true),
	}
	lambdaFunctions = append(lambdaFunctions, logDatadogMetricsLambda)

	return lambdaFunctions, extraResources, nil

}

func nesfDynamoResources() map[string][]tf.Resource {
	// This dynamo table keeps track of the number of retries for a node in any given run
	// More information here: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/8515461/Fix+Sql+Transformation+Erroring+Plan+3d99
	dynamoTable := &awsdynamodbresource.DynamoDBTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		Name:        "datapipeline-node-retry-count",
		BillingMode: awsdynamodbresource.PayPerRequest,
		HashKey:     "node_id",
		Attributes: []awsdynamodbresource.DynamoDBTableAttribute{
			{
				KeyName: "node_id",
				Type:    awsdynamodbresource.String,
			},
		},
		Tags: map[string]string{
			"Name":                   "datapipeline-node-retry-count",
			"samsara:service":        "datapipeline-node-retry-count",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
	}
	return map[string][]tf.Resource{
		"dynamodb": {dynamoTable},
	}
}
