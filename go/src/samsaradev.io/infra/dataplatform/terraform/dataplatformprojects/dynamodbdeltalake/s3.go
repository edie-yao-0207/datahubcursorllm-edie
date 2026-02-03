package dynamodbdeltalake

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func CreateDynamodbDeltaLakeS3Resources(config dataplatformconfig.DatabricksConfig) []tf.Resource {

	var resources []tf.Resource

	// Create a bucket to store the raw dynamodb streams
	// These streams are read by Lambda and written to S3
	dynamodbRawStreamsBucket := dataplatformresource.Bucket{
		Name:                             "dynamodb-export",
		Region:                           config.Region,
		LoggingBucket:                    awsregionconsts.RegionPrefix[config.Region] + "databricks-s3-logging",
		NonCurrentExpirationDaysOverride: 2,
		Metrics:                          true,
		RnDCostAllocation:                0,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			{
				ID:      "expire-firehose-cdc-files-after-14-days",
				Enabled: true,
				Prefix:  "kinesis_streams/data/",
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 14,
				},
			},
		},
	}

	resources = append(resources, dynamodbRawStreamsBucket.Resources()...)

	ssoRegionalPrincipalArn := []string{}

	// Add regional Principal Arn
	// Please note the region us-west-2 is intentional in the ARN
	if config.Region == infraconsts.SamsaraAWSEURegion {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_eu-dataplatformadmin*", config.AWSAccountId))
	} else if config.Region == infraconsts.SamsaraAWSCARegion {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_ca-dataplatformadmin*", config.AWSAccountId))
	} else {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_dataplatformadmin*", config.AWSAccountId))
	}

	dynamodbRawStreamsBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dynamodbRawStreamsBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid: "AllowDynamDbExportToWriteDynamoDBBuckets",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/buildkite-ci-terraform",
							config.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:GetBucketLocation",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowFirehoseToWriteDynamoDBStreams",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/firehose-dynamodb-streams-cross-account-source-role",
							config.AWSAccountId,
						),
					},
					Effect: "Allow",
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:GetBucketLocation",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "AllowDataPlatformAdminToWriteDynamoDBExports",
					Principal: map[string]string{"AWS": "*"},
					Effect:    "Allow",
					Action: []string{
						"s3:ListBucket",
						"s3:PutObjectAcl",
						"s3:AbortMultipartUpload",
						"s3:PutObject",
					},
					Resource: []string{
						dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						ForAnyValueStringLike: map[string][]string{
							"aws:PrincipalArn": ssoRegionalPrincipalArn,
						},
					},
				},
			},
		},
	}

	// This role will be assumed by the firehose IAM in the main AWS
	// to write to the bucket in the databricks AWS account
	crossAccountTargetRole := &awsresource.IAMRole{
		Name: "firehose-dynamodb-streams-cross-account-target-role",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/firehose-dynamodb-streams-cross-account-source-role",
							config.AWSAccountId,
						),
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "firehose-dynamodb-streams-cross-account-target-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	resources = append(resources, crossAccountTargetRole)

	// Create a policy for the firehose to write to the bucket
	crossAccountTargetRolePolicy := &awsresource.IAMRolePolicy{
		Name: "firehose-dynamodb-streams-cross-account-target-role-policy",
		Role: crossAccountTargetRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowFirehoseToWriteKinesisDataStreams",
					Effect: "Allow",
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:GetBucketLocation",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Resource: []string{
						dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", dynamodbRawStreamsBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			},
		},
	}

	resources = append(resources, crossAccountTargetRolePolicy)

	return append(
		resources,
		dynamodbRawStreamsBucketPolicy,
	)
}
