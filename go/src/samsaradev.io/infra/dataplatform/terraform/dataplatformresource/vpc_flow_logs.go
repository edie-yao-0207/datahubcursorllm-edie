package dataplatformresource

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func VpcFlowLogsResource(resourceName string, vpcid string, region string) []tf.Resource {

	resources := []tf.Resource{}

	type S3BucketArgs struct {
		Bucket string `hcl:"bucket"`
	}

	vpcFlowBucket := &genericresource.Data{
		Type: "aws_s3_bucket",
		Name: "databricks_vpc_flow_bucket",
		Args: S3BucketArgs{
			Bucket: fmt.Sprintf("%sdatabricks-aws-vpc-flow-logs", awsregionconsts.RegionPrefix[region]),
		},
	}

	resources = append(resources, vpcFlowBucket)

	flowLogs := &awsresource.VPCFlowLogs{
		ResourceName:       resourceName,
		LogDestination:     vpcFlowBucket.ResourceId().ReferenceAttr("arn"),
		LogDestinationType: awsresource.VPCFlowLogDestinationTypeS3,
		TrafficType:        awsresource.VPCFlowTrafficTypeAll,
		VpcID:              vpcid,
		DestinationOptions: awsresource.DestinationOptions{
			FileFormat:               awsresource.VPCFlowDestinationOptionsFileFormatParquet,
			PerHourPartition:         true,
			HiveCompatiblePartitions: true,
		},
		Tags: awsresource.ConvertAWSTagsToTagMap([]*awsresource.Tag{
			{
				Key:   "Name",
				Value: resourceName,
			},
		}),
		LogFormat: "$${version} $${account-id} $${interface-id} $${srcaddr} $${dstaddr} $${srcport} $${dstport} $${protocol} $${packets} $${bytes} $${start} $${end} $${action} $${log-status} $${vpc-id} $${subnet-id}",
	}
	resources = append(resources, flowLogs)

	return resources
}
