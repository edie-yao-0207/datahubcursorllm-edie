package dataplatformprojecthelpers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/service/components"
	"samsaradev.io/team"
)

type IndividualPoolArguments struct {
	PoolName string
	// poolResourceId is the terraform resource ID. If not specified, will use the local ID of the resource with the poolName
	PoolResourceId string
}
type PoolArguments struct {
	DriverPool IndividualPoolArguments
}

// CreateVacuumResources creates vacuum jobs for a given list of s3 paths that we want to vacuum.
func CreateVacuumResources(s3Paths []string, config dataplatformconfig.DatabricksConfig, jobNamePrefix string, poolArgs PoolArguments, jobType dataplatformconsts.DataPlatformJobType, retentionDays int64, timeoutHours int) ([]tf.Resource, error) {
	var resources []tf.Resource

	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/vacuum.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	sort.Strings(s3Paths)

	driverPoolName := poolArgs.DriverPool.PoolName
	driverPoolResourceId := tf.LocalId(driverPoolName).ReferenceKey("id")

	if poolArgs.DriverPool.PoolResourceId != "" {
		driverPoolResourceId = poolArgs.DriverPool.PoolResourceId
	}

	var allParameters []string
	for _, s3Path := range s3Paths {
		allParameters = append(allParameters, fmt.Sprintf("--path %s", s3Path))
	}
	// Databricks JSON job specification has 10000 bytes limit.
	// Limit number of tables per job to avoid hitting this limit.
	// We noticed that after the 10000 byte limit, the requests to databricks had 300-600 bytes
	// so we're temporarily decreasing it to 9000 bytes to allow the changes to apply,
	const maxNumBytesForParameters = 9000
	parameterChunks, err := chunkParamsSlice(allParameters, maxNumBytesForParameters)
	if err != nil {
		return nil, oops.Wrapf(err, "error chunking params slice for vacuum jobs")
	}

	for idx, parameters := range parameterChunks {
		parametersSplitBySpace := splitParamsBySpace(parameters)
		parametersSplitBySpace = append(parametersSplitBySpace, "--retention-days", strconv.FormatInt(retentionDays, 10))
		jobName := fmt.Sprintf("%s-%d", jobNamePrefix, idx)

		// Randomize job start time based on the name to prevent all vacuum jobs starting at the same moment.
		min := dataplatformresource.RandomFromNameHash(jobName, 0, 60)
		hour := dataplatformresource.RandomFromNameHash(jobName, 0, 24)
		cronSchedule := fmt.Sprintf("0 %d %d * * ?", min, hour)

		if config.Region == infraconsts.SamsaraAWSCARegion {
			// TODO_CA: The CA launch is temporarily on hold.
			// pause vacuum jobs in Canada.
			cronSchedule = ""
		}

		jobOwner, err := dataplatformconfig.GetCIUserByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no job owner for region %s", config.Region)
		}
		ucSettings := dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   jobOwner,
		}

		jobOwnerServicePrincipalName := ""
		var runAs *databricks.RunAsSetting

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
		}
		runAs = &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		}
		ucSettings.SingleUserName = ciServicePrincipalAppId
		jobOwnerServicePrincipalName = ciServicePrincipalAppId

		overrideSparkConf := map[string]string{}

		overrideSparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
		overrideSparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
		overrideSparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
		overrideSparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

		job := dataplatformresource.JobSpec{
			Name:         jobName,
			Region:       config.Region,
			Owner:        team.DataPlatform,
			Script:       script,
			SparkVersion: sparkversion.VacuumJobDbrVersion,
			SparkConf: dataplatformresource.SparkConf{
				Overrides: overrideSparkConf,
			}.ToMap(),
			Parameters:                   parametersSplitBySpace,
			SingleNodeJob:                true,
			MaxRetries:                   1,
			MinRetryInterval:             10 * time.Minute,
			Pool:                         driverPoolResourceId,
			Profile:                      tf.LocalId("instance_profile").Reference(),
			Cron:                         cronSchedule,
			TimeoutSeconds:               timeoutHours * int((time.Hour).Seconds()),
			EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
			JobOwnerServicePrincipalName: jobOwnerServicePrincipalName,
			Tags: map[string]string{
				"pool-name":                     driverPoolName,
				dataplatformconsts.DATABASE_TAG: fmt.Sprintf("batch-%d", idx),
			},
			RnDCostAllocation: 1,
			IsProduction:      false,
			JobType:           jobType,
			Format:            databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
			SloConfig: &dataplatformconsts.JobSlo{
				SloTargetHours:           72,
				LowUrgencyThresholdHours: 72,
			},
			UnityCatalogSetting: ucSettings,
			RunAs:               runAs,
		}

		jobResource, err := job.TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job resource")
		}
		permissionsResource, err := job.PermissionsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job permissions resource")
		}
		resources = append(resources, jobResource, permissionsResource)
	}

	return resources, nil
}

// chunkParamsSlice chunks the input slice into a slice of slices based on the maximum number of bytes that can exist in a single slice.
func chunkParamsSlice(allParameters []string, maxBytesPerSlice int) ([][]string, error) {
	var parameterChunks [][]string
	currentChunkByteSize := 0
	var currentChunk []string

	if len(allParameters) == 0 {
		return [][]string{{}}, nil
	}

	currentParamIndex := 0
	currentChunkIndex := 0
	for {
		param := allParameters[currentParamIndex]
		paramLength := len(param)
		if currentChunkIndex > 0 {
			// Add 1 b/c we also need an extra space character to add this param after the previous one
			paramLength++
		}

		if paramLength > maxBytesPerSlice {
			return nil, oops.Errorf("%s is longer than the max bytes per slice %d", param, maxBytesPerSlice)
		}

		if currentChunkByteSize+paramLength <= maxBytesPerSlice {
			// Add the param to the current chunk of parameters
			currentChunk = append(currentChunk, param)
			currentChunkByteSize += paramLength
		} else {
			// If adding this param to the list of params brings us over our max byte size,
			// then add the current list of params to the list of chunks and reset for a new chunk of params
			parameterChunks = append(parameterChunks, currentChunk)
			currentChunk = []string{}
			currentChunkIndex = 0
			currentChunkByteSize = 0
			continue
		}

		currentParamIndex++
		currentChunkIndex++
		if currentParamIndex == len(allParameters) {
			parameterChunks = append(parameterChunks, currentChunk)
			break
		}
	}

	return parameterChunks, nil
}

// splitParamsBySpace splits an input list of parameters into separate
// parameters based on white space
func splitParamsBySpace(parameters []string) []string {
	var outputParams []string
	for _, param := range parameters {
		parts := strings.Split(param, " ")
		outputParams = append(outputParams, parts...)
	}

	return outputParams
}

func GenerateEcrLifecyclePolicy(repo awsresource.ECRRepository) awsresource.ECRLifecyclePolicy {
	return awsresource.ECRLifecyclePolicy{
		Repository: repo.ResourceId(),
		Policy: policy.AWSECRLifecyclePolicy{
			Rules: []policy.AWSECRLifecycleRule{
				{
					RulePriority: 1,
					Description:  "Keep last 5 images",
					Selection: policy.AWSECRLifecycleSelection{
						TagStatus:     components.AWSLifecycleTagStatusTagged,
						TagPrefixList: []string{"20"}, // Images are prefixed with the date they are created on.
						CountType:     components.AWSLifecycleCountTypeMoreThan,
						CountNumber:   5,
					},
					Action: map[string]string{
						"type": "expire",
					},
				},
			},
		},
	}
}

// generateAWSLoadBalancerControllerPolicy generates the IAM Policy to allows the Load Balancer to make AWS API calls
// This policy was copied from the JSON version in the kubernetes-sigs github from the EKS Load Balancer guide
// https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html
// https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v2.2.0/docs/install/iam_policy.json
func GenerateAWSLoadBalancerControllerPolicy() policy.AWSPolicy {
	return policy.AWSPolicy{
		Version: policy.AWSPolicyVersion,
		Statement: []policy.AWSPolicyStatement{
			{
				Effect: "Allow",
				Action: []string{
					"iam:CreateServiceLinkedRole",
					"ec2:DescribeAccountAttributes",
					"ec2:DescribeAddresses",
					"ec2:DescribeAvailabilityZones",
					"ec2:DescribeInternetGateways",
					"ec2:DescribeVpcs",
					"ec2:DescribeSubnets",
					"ec2:DescribeSecurityGroups",
					"ec2:DescribeInstances",
					"ec2:DescribeNetworkInterfaces",
					"ec2:DescribeTags",
					"ec2:GetCoipPoolUsage",
					"ec2:DescribeCoipPools",
					"elasticloadbalancing:DescribeLoadBalancers",
					"elasticloadbalancing:DescribeLoadBalancerAttributes",
					"elasticloadbalancing:DescribeListeners",
					"elasticloadbalancing:DescribeListenerAttributes",
					"elasticloadbalancing:DescribeListenerCertificates",
					"elasticloadbalancing:DescribeSSLPolicies",
					"elasticloadbalancing:DescribeRules",
					"elasticloadbalancing:DescribeTargetGroups",
					"elasticloadbalancing:DescribeTargetGroupAttributes",
					"elasticloadbalancing:DescribeTargetHealth",
					"elasticloadbalancing:DescribeTags",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"cognito-idp:DescribeUserPoolClient",
					"acm:ListCertificates",
					"acm:DescribeCertificate",
					"iam:ListServerCertificates",
					"iam:GetServerCertificate",
					"waf-regional:GetWebACL",
					"waf-regional:GetWebACLForResource",
					"waf-regional:AssociateWebACL",
					"waf-regional:DisassociateWebACL",
					"wafv2:GetWebACL",
					"wafv2:GetWebACLForResource",
					"wafv2:AssociateWebACL",
					"wafv2:DisassociateWebACL",
					"shield:GetSubscriptionState",
					"shield:DescribeProtection",
					"shield:CreateProtection",
					"shield:DeleteProtection",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"ec2:AuthorizeSecurityGroupIngress",
					"ec2:RevokeSecurityGroupIngress",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"ec2:CreateSecurityGroup",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"ec2:CreateTags",
				},
				Resource: []string{"arn:aws:ec2:*:*:security-group/*"},
				Condition: &policy.AWSPolicyCondition{
					StringEquals: map[string]string{
						"ec2:CreateAction": "CreateSecurityGroup",
					},
					Null: map[string]string{
						"aws:RequestTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"ec2:CreateTags",
					"ec2:DeleteTags",
				},
				Resource: []string{"arn:aws:ec2:*:*:security-group/*"},
				Condition: &policy.AWSPolicyCondition{
					Null: map[string]string{
						"aws:RequestTag/elbv2.k8s.aws/cluster":  "true",
						"aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"ec2:AuthorizeSecurityGroupIngress",
					"ec2:RevokeSecurityGroupIngress",
					"ec2:DeleteSecurityGroup",
				},
				Resource: []string{"*"},
				Condition: &policy.AWSPolicyCondition{
					Null: map[string]string{
						"aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:CreateLoadBalancer",
					"elasticloadbalancing:CreateTargetGroup",
				},
				Resource: []string{"*"},
				Condition: &policy.AWSPolicyCondition{
					Null: map[string]string{
						"aws:RequestTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:CreateListener",
					"elasticloadbalancing:DeleteListener",
					"elasticloadbalancing:CreateRule",
					"elasticloadbalancing:DeleteRule",
				},
				Resource: []string{"*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:AddTags",
					"elasticloadbalancing:RemoveTags",
				},
				Resource: []string{
					"arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
					"arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
					"arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*",
				},
				Condition: &policy.AWSPolicyCondition{
					Null: map[string]string{
						"aws:RequestTag/elbv2.k8s.aws/cluster":  "true",
						"aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:AddTags",
					"elasticloadbalancing:RemoveTags",
				},
				Resource: []string{
					"arn:aws:elasticloadbalancing:*:*:listener/net/*/*/*",
					"arn:aws:elasticloadbalancing:*:*:listener/app/*/*/*",
					"arn:aws:elasticloadbalancing:*:*:listener-rule/net/*/*/*",
					"arn:aws:elasticloadbalancing:*:*:listener-rule/app/*/*/*",
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:ModifyLoadBalancerAttributes",
					"elasticloadbalancing:SetIpAddressType",
					"elasticloadbalancing:SetSecurityGroups",
					"elasticloadbalancing:SetSubnets",
					"elasticloadbalancing:DeleteLoadBalancer",
					"elasticloadbalancing:ModifyTargetGroup",
					"elasticloadbalancing:ModifyTargetGroupAttributes",
					"elasticloadbalancing:DeleteTargetGroup",
				},
				Resource: []string{"*"},
				Condition: &policy.AWSPolicyCondition{
					Null: map[string]string{
						"aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
					},
				},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:RegisterTargets",
					"elasticloadbalancing:DeregisterTargets",
				},
				Resource: []string{"arn:aws:elasticloadbalancing:*:*:targetgroup/*/*"},
			},
			{
				Effect: "Allow",
				Action: []string{
					"elasticloadbalancing:SetWebAcl",
					"elasticloadbalancing:ModifyListener",
					"elasticloadbalancing:AddListenerCertificates",
					"elasticloadbalancing:RemoveListenerCertificates",
					"elasticloadbalancing:ModifyRule",
				},
				Resource: []string{"*"},
			},
		},
	}
}

func InitScriptS3URI(region string, initScriptFilename string) string {
	return fmt.Sprintf("s3://%sdataplatform-deployed-artifacts/init_scripts/%s", awsregionconsts.RegionPrefix[region], initScriptFilename)
}
