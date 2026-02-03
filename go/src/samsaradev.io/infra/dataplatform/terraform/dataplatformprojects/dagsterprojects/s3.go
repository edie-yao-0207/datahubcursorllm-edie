package dagsterprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/policy"
)

func s3Resources(config dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	var s3Resources []tf.Resource
	prefix := awsregionconsts.RegionPrefix[config.Region]
	accoundId := awsregionconsts.RegionDatabricksAccountID[config.Region]
	dagsterBucket := dataplatformresource.Bucket{
		Name:                             "dataplatform-dagster",
		Region:                           config.Region,
		CurrentExpirationDays:            90,
		NonCurrentExpirationDaysOverride: 2,
		RnDCostAllocation:                1,
		LoggingBucket:                    fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
	}
	s3Resources = append(s3Resources, dagsterBucket.Resources()...)

	dagsterBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: dagsterBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", accoundId),
					},
					Effect: "Allow",
					Action: []string{"s3:Put*", "s3:Get*", "s3:List*"},
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster", prefix),
						fmt.Sprintf("arn:aws:s3:::%sdataplatform-dagster/*", prefix),
					},
				},
			},
		},
	}
	s3Resources = append(s3Resources, dagsterBucketPolicy)

	return map[string][]tf.Resource{
		"s3": s3Resources,
	}
}
