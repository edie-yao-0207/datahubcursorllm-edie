package emrreplicationproject

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	s3emitters "samsaradev.io/infra/app/generate_terraform/s3"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func s3Project(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	emrReplicationDeltaLakeBucketResourcesMap, err := emrReplicationDeltaLakeBucketResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "getting emr replication delta lake bucket resources")
	}

	emrReplicationExportBucketResourcesMap, err := emrReplicationExportBucketResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "getting emr replication export bucket resources")
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emr_replication",
		Name:     "emr_replication_buckets",
		ResourceGroups: project.MergeResourceGroups(
			emrReplicationDeltaLakeBucketResourcesMap,
			emrReplicationExportBucketResourcesMap,
		),
		GenerateOutputs: true,
	}, nil
}

func emrReplicationDeltaLakeBucketResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var emrReplicationDeltaLakeBucketResources []tf.Resource
	var emrReplicationDeltaLakeBucketPolicies []tf.Resource
	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "getting cells for region %s", config.Region)
	}
	for _, cell := range cells {
		emrReplicationBucket := dataplatformresource.Bucket{
			Name:                             fmt.Sprintf("emr-replication-delta-lake-%s", cell),
			Region:                           config.Region,
			LoggingBucket:                    awsregionconsts.RegionPrefix[config.Region] + "databricks-s3-logging",
			NonCurrentExpirationDaysOverride: 1,
			EnableS3IntelligentTiering:       true,
			RnDCostAllocation:                0, // Could this be non-zero since it will be customer facing?
		}
		emrReplicationDeltaLakeBucketResources = append(emrReplicationDeltaLakeBucketResources, emrReplicationBucket.Resources()...)

		emrReplicationDeltaLakeBucketPolicy := &awsresource.S3BucketPolicy{
			Bucket: emrReplicationBucket.Bucket().ResourceId(),
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: append([]policy.AWSPolicyStatement{
					{
						Sid:       "RequireTLS",
						Effect:    "Deny",
						Principal: map[string]string{"AWS": "*"},
						Action:    []string{"s3:*"},
						Resource: []string{
							emrReplicationBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
						Condition: &policy.AWSPolicyCondition{
							Bool: &policy.AWSPolicyConditionBool{SecureTransportEnabled: pointer.BoolPtr(false)},
						},
					},
					{
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:root",
								config.AWSAccountId,
							),
						},
						Effect: "Allow",
						Action: []string{"s3:Get*", "s3:List*", "s3:Put*"},
						Resource: []string{
							emrReplicationBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
				}, s3emitters.RayCrossAccountReadOnlyBucketPolicyStatements(config.Cloud, emrReplicationBucket.Name)...),
			},
		}
		emrReplicationDeltaLakeBucketPolicies = append(emrReplicationDeltaLakeBucketPolicies, emrReplicationDeltaLakeBucketPolicy)
	}
	return map[string][]tf.Resource{
		"emr_replication_delta_lake_bucket_resources": emrReplicationDeltaLakeBucketResources,
		"emr_replication_delta_lake_bucket_policies":  emrReplicationDeltaLakeBucketPolicies,
	}, nil
}

func getTargetRoleName() string {
	return "firehose-emr-replication-export-cross-account-target-role"
}

func emrReplicationExportBucketResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	resourceGroups := make(map[string][]tf.Resource)

	// Create a shared cross-account role for all cells
	crossAccountTargetRole := &awsresource.IAMRole{
		Name: getTargetRoleName(),
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowEmrReplicationSourceRolesToAssumeTargetRole",
					Action: []string{"sts:AssumeRole"},
					MultiPrincipal: map[string][]string{
						"AWS": {
							fmt.Sprintf(
								"arn:aws:iam::%d:role/%s",
								config.AWSAccountId,
								getSourceRole(streamTypeCDC).Name,
							),
							fmt.Sprintf(
								"arn:aws:iam::%d:role/%s",
								config.AWSAccountId,
								getSourceRole(streamTypeBackfill).Name,
							),
						},
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       getTargetRoleName(),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	// Get all cells for the region
	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "getting cells for region %s", config.Region)
	}

	// Create a slice to store all bucket ARNs
	var allBucketArns []string

	// First create all buckets and collect their ARNs
	for _, cell := range cells {
		var resources []tf.Resource

		emrReplicationExportBucket := dataplatformresource.Bucket{
			Name:                             fmt.Sprintf("emr-replication-export-%s", cell),
			Region:                           config.Region,
			LoggingBucket:                    awsregionconsts.RegionPrefix[config.Region] + "databricks-s3-logging",
			NonCurrentExpirationDaysOverride: 2,
			Metrics:                          true,
			RnDCostAllocation:                0,
			ExtraLifecycleRules: []awsresource.S3LifecycleRule{
				{
					ID:      "expire-emr-replication-files-after-7-days",
					Enabled: true,
					Prefix:  "cdc/",
					Expiration: awsresource.S3LifecycleRuleExpiration{
						Days: 7,
					},
				},
				{
					ID:      "expire-emr-replication-backfill-files-after-90-days",
					Enabled: true,
					Prefix:  "backfill/",
					Expiration: awsresource.S3LifecycleRuleExpiration{
						Days: 90,
					},
				},
				{
					ID:      "expire-emr-replication-validation-files-after-7-days",
					Enabled: true,
					Prefix:  "validation/",
					Expiration: awsresource.S3LifecycleRuleExpiration{
						Days: 7,
					},
				},
			},
		}

		resources = append(resources, emrReplicationExportBucket.Resources()...)

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

		emrReplicationExportBucketPolicy := &awsresource.S3BucketPolicy{
			Bucket: emrReplicationExportBucket.Bucket().ResourceId(),
			Policy: policy.AWSPolicy{
				Version: policy.AWSPolicyVersion,
				Statement: []policy.AWSPolicyStatement{
					// TODO: deprecate this policy statement after verifying that the new policy statement below for the
					// cross-account source role works as expected
					{
						Sid: "AllowFirehoseToWriteEMRReplicationExportTargetRole",
						Principal: map[string]string{
							"AWS": crossAccountTargetRole.ResourceId().ReferenceAttr("arn"),
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
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid: "AllowFirehoseToWriteEMRReplicationExportSourceRole",
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:role/firehose-emr-cdc-export-cross-account-source-role",
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
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid: "AllowFirehoseToWriteEMRBackfillExportSourceRole",
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:role/firehose-emr-backfill-export-cross-account-source-role",
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
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid: "AllowEmrBackfillWorkflowsWorkerToWriteToExportBucket",
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:role/emrbackfillworkflowsworker_service",
								config.AWSAccountId,
							),
						},
						Effect: "Allow",
						Action: []string{
							"s3:GetObject",
							"s3:GetObjectAcl",
							"s3:GetObjectTagging",
							"s3:GetObjectVersion",
							"s3:GetObjectVersionAcl",
							"s3:PutObject",
							"s3:PutObjectAcl",
							"s3:PutObjectTagging",
							"s3:PutObjectVersionAcl",
						},
						Resource: []string{
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid: "AllowEmrValidationWorkflowsWorkerToWriteToExportBucket",
						Principal: map[string]string{
							"AWS": fmt.Sprintf(
								"arn:aws:iam::%d:role/emrvalidationworkflowsworker_service",
								config.AWSAccountId,
							),
						},
						Effect: "Allow",
						Action: []string{
							"s3:GetObject",
							"s3:GetObjectAcl",
							"s3:GetObjectTagging",
							"s3:GetObjectVersion",
							"s3:GetObjectVersionAcl",
							"s3:PutObject",
							"s3:PutObjectAcl",
							"s3:PutObjectTagging",
							"s3:PutObjectVersionAcl",
						},
						Resource: []string{
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
					},
					{
						Sid:       "AllowEmrReplicationSqsWorkerToWriteToExportBucket",
						Principal: map[string]string{"AWS": "*"},
						Effect:    "Allow",
						Action: []string{
							"s3:GetObject",
							"s3:GetObjectAcl",
							"s3:GetObjectTagging",
							"s3:GetObjectVersion",
							"s3:GetObjectVersionAcl",
							"s3:PutObject",
							"s3:PutObjectAcl",
							"s3:PutObjectTagging",
							"s3:PutObjectVersionAcl",
						},
						Resource: []string{
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
						},
						Condition: &policy.AWSPolicyCondition{
							StringLike: map[string][]string{
								"aws:PrincipalArn": []string{
									fmt.Sprintf("arn:aws:iam::%d:role/emrreplicationsqsworker*_service", config.AWSAccountId),
								},
							},
						},
					},
					{
						Sid:       "AllowDataPlatformAdminToWriteEMRReplicationExport",
						Principal: map[string]string{"AWS": "*"},
						Effect:    "Allow",
						Action: []string{
							"s3:ListBucket",
							"s3:PutObjectAcl",
							"s3:AbortMultipartUpload",
							"s3:PutObject",
						},
						Resource: []string{
							emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
							fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
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

		resources = append(resources, emrReplicationExportBucketPolicy)

		// Collect bucket ARNs for the shared role policy
		allBucketArns = append(allBucketArns,
			emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn"),
			fmt.Sprintf("%s/*", emrReplicationExportBucket.Bucket().ResourceId().ReferenceAttr("arn")),
		)

		resourceGroups["emr_replication_export_bucket"] = append(resourceGroups["emr_replication_export_bucket"], resources...)
	}

	// Create a single policy for the shared role that can access all buckets
	crossAccountTargetRolePolicy := &awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-policy", getTargetRoleName()),
		Role: crossAccountTargetRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    "AllowFirehoseToWriteEMRReplicationExport",
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
					Resource: allBucketArns,
				},
			},
		},
	}

	// Add the shared role and its policy to the resource group
	resourceGroups["emr_replication_export_bucket"] = append(
		resourceGroups["emr_replication_export_bucket"],
		crossAccountTargetRole,
		crossAccountTargetRolePolicy,
	)

	return resourceGroups, nil
}

// Function to return the list of all emr replication delta lake buckets names in a list of strings
func GetAllEmrReplicationDeltaLakeBucketNames(config dataplatformconfig.DatabricksConfig) []string {
	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil
	}
	emrReplicationDeltaLakeBucketNames := []string{}
	for _, cell := range cells {
		emrReplicationDeltaLakeBucketNames = append(emrReplicationDeltaLakeBucketNames, fmt.Sprintf("emr-replication-delta-lake-%s", cell))
	}
	return emrReplicationDeltaLakeBucketNames
}

func GetAllEmrReplicationExportBucketNames(config dataplatformconfig.DatabricksConfig) []string {
	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil
	}
	emrReplicationExportBucketNames := []string{}
	for _, cell := range cells {
		emrReplicationExportBucketNames = append(emrReplicationExportBucketNames, fmt.Sprintf("emr-replication-export-%s", cell))
	}
	return emrReplicationExportBucketNames
}
