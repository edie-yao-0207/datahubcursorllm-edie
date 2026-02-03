package dataplatformprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

const MetastoreBucketName = "unity-catalog-metastore"
const MetastoreBucketRole = "databricks-metastore-role"

// Set up buckets for the Unity Catalog Metastore, as well as an associated role that will
// be used by databricks to access the bucket.
func UnityCatalogMetastoreBuckets(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	metastoreBucket := dataplatformresource.Bucket{
		Name:              MetastoreBucketName,
		Region:            c.Region,
		LoggingBucket:     tf.LocalId("logging_bucket").Reference(),
		Metrics:           true,
		RnDCostAllocation: 1.0,
	}

	bucketArn := metastoreBucket.Bucket().ResourceId().ReferenceAttr("arn")

	roleArn, err := dataplatformresource.IAMRoleArn(c.DatabricksProviderGroup, "databricks-metastore-role")
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create instance profile arn")
	}

	metastoreRole := &awsresource.IAMRole{
		ResourceName: "databricks_metastore_role",
		Name:         MetastoreBucketRole,
		Description:  "Role assumed by databricks to access Unity Catalog Metastores.",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				// Allow databricks to assume role, from a specific role on their end and
				// with a specific externalid provided.
				{
					Principal: map[string]string{
						"AWS": dataplatformconsts.DatabricksUCAwsRoleReference,
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": tf.LocalId("databricks_external_id").Reference(),
						},
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
				},
				// Allow the role to be self-assuming.
				{
					Principal: map[string]string{
						"AWS": roleArn,
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": tf.LocalId("databricks_external_id").Reference(),
						},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "databricks-metastore-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	s3Access := &awsresource.IAMRolePolicy{
		ResourceName: "databricks_metastore_role_s3_access",
		Name:         "databricks-metastore-role-s3-access",
		Role:         metastoreRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"s3:GetObject",
						"s3:PutObject",
						"s3:DeleteObject",
						"s3:ListBucket",
						"s3:GetBucketLocation",
					},
					Resource: []string{
						bucketArn,
						fmt.Sprintf("%s/*", bucketArn),
					},
				},
			},
		},
	}

	return map[string][]tf.Resource{
		"metastore_buckets": append(metastoreBucket.Resources(), metastoreRole, s3Access),
	}, nil
}
