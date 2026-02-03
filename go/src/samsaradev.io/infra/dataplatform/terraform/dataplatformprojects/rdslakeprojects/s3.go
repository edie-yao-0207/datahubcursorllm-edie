package rdslakeprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

// kinesisExportS3Project creates a project containing the S3 export bucket
// for storing Kinesis Data Firehose output from DMS replication
// This project deploys to the Databricks account (like EMR)
func kinesisExportS3Project(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	// Create S3 export bucket resources
	s3Resources, err := createKinesisExportBucketResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating kinesis export bucket resources")
	}

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup, // Deploy to Databricks account
		Class:           "kinesis-export-s3",
		ResourceGroups:  s3Resources,
		GenerateOutputs: true,
	}

	awsProviderVersion := resource.AWSProviderVersion5
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
		"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
	})

	return p, nil
}

// createKinesisExportBucketResources creates the S3 export bucket and related resources
// for storing Kinesis Data Firehose output from DMS replication
func createKinesisExportBucketResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var resources []tf.Resource

	// Create the main export bucket
	exportBucket := dataplatformresource.Bucket{
		Name:                             "rds-kinesis-export",
		Region:                           config.Region,
		LoggingBucket:                    awsregionconsts.RegionPrefix[config.Region] + "databricks-s3-logging",
		NonCurrentExpirationDaysOverride: 2,
		Metrics:                          true,
		RnDCostAllocation:                dataplatformconsts.RdsStorageRndAllocation,
		SkipPublicAccessBlock:            true, // SCP forbids setting the public access block in CA region
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			{
				ID:      "expire-kinesis-export-dms-kinesis-after-14-days",
				Enabled: true,
				Prefix:  "dms-kinesis/",
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 14,
				},
			},
		},
	}

	resources = append(resources, exportBucket.Resources()...)

	// Create cross-account target role for Firehose delivery streams (like EMR pattern)
	// This role is in the Databricks account and can be assumed by source roles in main account
	crossAccountTargetRoleResources, err := createCrossAccountTargetRoleResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating cross-account target role resources")
	}
	resources = append(resources, crossAccountTargetRoleResources...)

	// Create bucket policy for Firehose access
	bucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: exportBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:       "RequireTLS",
					Effect:    "Deny",
					Principal: map[string]string{"AWS": "*"},
					Action:    []string{"s3:*"},
					Resource: []string{
						exportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", exportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
					},
				},
				{
					Sid: "AllowFirehoseToWriteKinesisExportTargetRole",
					Principal: map[string]string{
						"AWS": "${aws_iam_role.firehose-rds-replication-export-cross-account-target-role.arn}",
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
						exportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", exportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid: "AllowFirehoseToWriteKinesisExportSourceRole",
					Principal: map[string]string{
						"AWS": fmt.Sprintf(
							"arn:aws:iam::%d:role/firehose-rds-replication-export-cross-account-source-role",
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
						exportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", exportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Sid:       "AllowDataPlatformAdminToWriteKinesisExport",
					Principal: map[string]string{"AWS": "*"},
					Effect:    "Allow",
					Action: []string{
						"s3:ListBucket",
						"s3:PutObjectAcl",
						"s3:AbortMultipartUpload",
						"s3:PutObject",
					},
					Resource: []string{
						exportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", exportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						ForAnyValueStringLike: map[string][]string{
							"aws:PrincipalArn": getSSORegionalPrincipalArns(config),
						},
					},
				},
			},
		},
	}

	resources = append(resources, bucketPolicy)

	return map[string][]tf.Resource{
		"kinesis_export_bucket": resources,
	}, nil
}

// getSSORegionalPrincipalArns returns the appropriate SSO principal ARNs for the region
// Please note the region us-west-2 is intentional in the ARN for all the regions.
func getSSORegionalPrincipalArns(config dataplatformconfig.DatabricksConfig) []string {
	var ssoRegionalPrincipalArn []string

	if config.Region == infraconsts.SamsaraAWSEURegion {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_eu-dataplatformadmin*", config.AWSAccountId))
	} else if config.Region == infraconsts.SamsaraAWSCARegion {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_ca-dataplatformadmin*", config.AWSAccountId))
	} else {
		ssoRegionalPrincipalArn = append(ssoRegionalPrincipalArn, fmt.Sprintf("arn:aws:iam::%d:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_dataplatformadmin*", config.AWSAccountId))
	}

	return ssoRegionalPrincipalArn
}

// createCrossAccountTargetRoleResources creates the cross-account target role in the Databricks account
// This role can be assumed by source roles in the main account (like EMR pattern)
func createCrossAccountTargetRoleResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	// Create cross-account target role (in Databricks account)
	targetRole := &awsresource.IAMRole{
		Name: "firehose-rds-replication-export-cross-account-target-role",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowRdsReplicationSourceRolesToAssumeTargetRole",
					Action: []string{"sts:AssumeRole"},
					MultiPrincipal: map[string][]string{
						"AWS": {
							fmt.Sprintf(
								"arn:aws:iam::%d:role/firehose-rds-replication-export-cross-account-source-role",
								config.AWSAccountId,
							),
						},
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "firehose-rds-replication-export-cross-account-target-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, targetRole)

	// Create IAM policy for the target role
	targetRolePolicy := &awsresource.IAMRolePolicy{
		Name: "firehose-rds-replication-export-cross-account-target-role-policy",
		Role: targetRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowFirehoseToWriteRdsReplicationExport",
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
						fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[config.Region]+"rds-kinesis-export"),
						fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[config.Region]+"rds-kinesis-export"),
					},
				},
			},
		},
	}
	resources = append(resources, targetRolePolicy)

	return resources, nil
}

// GetAllKinesisExportBucketNames returns the list of all Kinesis export bucket names
func GetAllKinesisExportBucketNames(config dataplatformconfig.DatabricksConfig) []string {
	return []string{awsregionconsts.RegionPrefix[config.Region] + "rds-kinesis-export"}
}
