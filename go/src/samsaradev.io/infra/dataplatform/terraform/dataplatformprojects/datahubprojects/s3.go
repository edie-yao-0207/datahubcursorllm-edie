package datahubprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
)

func s3Resources(config dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	var s3Resources []tf.Resource
	metadataIngestionBucket := dataplatformresource.Bucket{
		Name:                             "datahub-metadata",
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
		Region:                           config.Region,
		CurrentExpirationDays:            90,
		NonCurrentExpirationDaysOverride: 2,
		RnDCostAllocation:                1,
		ExtraLifecycleRules: []awsresource.S3LifecycleRule{
			{
				ID:      "delete-delta-extraction-files",
				Enabled: true,
				Prefix:  "delta/",
				Expiration: awsresource.S3LifecycleRuleExpiration{
					Days: 7,
				},
			},
		},
	}
	s3Resources = append(s3Resources, metadataIngestionBucket.Resources()...)

	metadataIngestionBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: metadataIngestionBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSDatabricksAccountID),
					},
					Effect: "Allow",
					Action: []string{"s3:Put*", "s3:Get*", "s3:List*"},
					Resource: []string{
						"arn:aws:s3:::samsara-datahub-metadata",
						"arn:aws:s3:::samsara-datahub-metadata/*",
					},
				},
				// Application and Classic Load Balancer logs.
				{
					Principal: infraconsts.GetELBPrincipalForRegion(infraconsts.SamsaraAWSDefaultRegion),
					Effect:    "Allow",
					Action:    []string{"s3:PutObject"},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::samsara-datahub-metadata/AWSLogs/%d/*", infraconsts.SamsaraAWSDatabricksAccountID),
					},
				},
				// Allow productgptagent service to read DataHub metadata for query agent functionality.
				// This is cross-account access from the main Samsara account (where productgptagent runs)
				// to the Databricks account (where this bucket resides).
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:role/productgptagent_service", infraconsts.SamsaraAWSAccountID),
					},
					Effect: "Allow",
					Action: []string{"s3:GetObject"},
					Resource: []string{
						"arn:aws:s3:::samsara-datahub-metadata/metadata/exports/prod/schemas/datahub.json",
					},
				},
			},
		},
	}
	s3Resources = append(s3Resources, metadataIngestionBucketPolicy)

	return map[string][]tf.Resource{
		"s3": s3Resources,
	}
}
