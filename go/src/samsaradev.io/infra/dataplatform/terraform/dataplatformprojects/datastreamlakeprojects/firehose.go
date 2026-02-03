package datastreamlakeprojects

import (
	"fmt"
	"strconv"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

// firehosePartitionPrefix is the prefix used when creating a firehose so firehose automatically
// partitions the parquet tables by date for us in S3. This allows for efficient querying when
// accessing the data from Databricks.
// More Info: https://docs.aws.amazon.com/firehose/latest/dev/s3-prefixes.html
const firehosePartitionPrefix = "date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/"

const datalakeFirehosesLogGroupName = "/aws/datalakefirehoses"

func firehoseProjects(config dataplatformconfig.DatabricksConfig) ([]*project.Project, error) {
	var projects []*project.Project

	cloudWatchLogGroup := &awsresource.CloudwatchLogGroup{
		ResourceName:    "data_lake_firehoses_cloudwatch_log_group",
		Name:            datalakeFirehosesLogGroupName,
		RetentionInDays: 90,
		Tags: map[string]string{
			"samsara:service":       "datalakestream-firehose-logging",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: config.AWSProviderGroup,
		Class:    "datastreamlake",
		Group:    "cloudwatch",
		Name:     "firehose",
		ResourceGroups: map[string][]tf.Resource{
			"cloudwatch": {cloudWatchLogGroup},
		},
	})

	for _, entry := range datastreamlake.Registry {
		samsaraTags := map[string]string{
			"samsara:service":       fmt.Sprintf("datalakestream-firehose-%s", entry.StreamName),
			"samsara:team":          strings.ToLower(entry.Owner.Name()),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[entry.Owner.Name()]),
		}
		firehoseRoleResource, firehoseRolePolicyResource := firehoseRoleResources(entry, config.Region)
		firehoseResource := &awsresource.KinesisFirehoseDeliveryStream{
			Name:        entry.StreamName,
			Tags:        samsaraTags,
			Destination: "extended_s3",
			ExtendedS3Configuration: awsresource.KinesisFirehoseDeliveryStreamExtendedS3Configuration{
				RoleARN:           firehoseRoleResource.ResourceId().ReferenceAttr("arn"),
				BucketARN:         fmt.Sprintf("arn:aws:s3:::%sdata-stream-lake", awsregionconsts.RegionPrefix[config.Region]),
				Prefix:            fmt.Sprintf("%s/data/%s", entry.StreamName, firehosePartitionPrefix),
				ErrorOutputPrefix: fmt.Sprintf("%s/errors/%s!{firehose:error-output-type}", entry.StreamName, firehosePartitionPrefix),
				// BufferSize & BufferInterval are configurable values that tell Firehose how to buffer the data
				// before writing to S3. Right now, we have these set to the defaults but in the future we may
				// tweak these based on the size of the data or give the option to developers defining streams.
				BufferSize:     128,
				BufferInterval: 600,
				CloudwatchLoggingOptions: awsresource.KinesisFirehoseDeliveryStreamCloudwatchLoggingOptions{
					Enabled:       true,
					LogGroupName:  cloudWatchLogGroup.Name,
					LogStreamName: entry.StreamName,
				},
				DataFormatConversionConfiguration: awsresource.KinesisFirehoseDeliveryStreamDataFormatConversionConfiguration{
					InputFormatConfiguration: awsresource.KinesisFirehoseDeliveryStreamInputFormatConfiguration{
						Deserializer: awsresource.KinesisFirehoseDeliveryStreamDeserializer{
							OpenXJSONSerDe: awsresource.KinesisFirehoseDeliveryStreamOpenXJSONSerDeConfiguration{},
						},
					},
					OutputFormatConfiguration: awsresource.KinesisFirehoseDeliveryStreamOutputFormatConfiguration{
						Serializer: awsresource.KinesisFirehoseDeliveryStreamSerializer{
							ParquetSerDe: awsresource.KinesisFirehoseDeliveryStreamParquetSerDeConfiguration{},
						},
					},
					SchemaConfiguration: awsresource.KinesisFirehoseDeliveryStreamSchemaConfiguration{
						DatabaseName: "datastreams_schema",
						RoleARN:      firehoseRoleResource.ResourceId().ReferenceAttr("arn"),
						TableName:    entry.StreamName,
						CatalogID:    strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
					},
				},
			},
		}

		// @JackR currently testing different BufferIntervals and BufferSizes on Webhook Logs to see
		// how cutting down the latency can affect the performance from the Databricks side
		if entry.StreamName == "webhook_logs" {
			firehoseResource.ExtendedS3Configuration.BufferInterval = 300 // Set BufferInterval to 5 minutes
		}
		if entry.StreamName == "cable_selection_logs" {
			firehoseResource.ExtendedS3Configuration.BufferInterval = 60
		}

		cloudWatchLogStream := &awsresource.CloudwatchLogStream{
			Name:         entry.StreamName,
			LogGroupName: cloudWatchLogGroup.Name,
		}

		firehoseResource.ServerSideEncryption = awsresource.KinesisFirehoseServerSideEncryption{
			Enabled: true,
			KeyType: awsresource.AwsOwnedCmk,
		}

		projects = append(projects, &project.Project{
			RootTeam: entry.Owner.TeamName,
			Provider: config.AWSProviderGroup,
			Class:    "datastreamlake",
			Group:    entry.StreamName,
			Name:     "firehose",
			ResourceGroups: map[string][]tf.Resource{
				"iam":        {firehoseRoleResource, firehoseRolePolicyResource},
				"firehose":   {firehoseResource},
				"cloudwatch": {cloudWatchLogStream},
			},
		})
	}

	return projects, nil
}

func firehoseRoleResources(registryEntry datastreamlake.DataStream, region string) (tf.Resource, tf.Resource) {
	firehoseRole := &awsresource.IAMRole{
		Name:        fmt.Sprintf("datastream_firehose_%s", registryEntry.StreamName),
		Description: fmt.Sprintf("Role used by Firehose %s for sending data to the Databricks data lake", registryEntry.StreamName),
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"Service": "firehose.amazonaws.com",
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("datastream_firehose_%s", registryEntry.StreamName),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	firehoseRolePolicy := &awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("firehose_%s_databricks_glue_s3_access", firehoseRole.Name),
		Role: firehoseRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:    fmt.Sprintf("Allow%sFirehoseReadAndWriteAccessToDatabricksAccountDataStreamBucketTablePrefix", strings.Replace(registryEntry.StreamName, "_", "", -1)),
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
						fmt.Sprintf("arn:aws:s3:::%sdata-stream-lake", awsregionconsts.RegionPrefix[region]),
						fmt.Sprintf("arn:aws:s3:::%sdata-stream-lake/%s/*", awsregionconsts.RegionPrefix[region], registryEntry.StreamName),
					},
				},
				{
					Sid:    fmt.Sprintf("Allow%sFirehoseGetAccessToDatabricksAccountGlueTableDefinition", strings.Replace(registryEntry.StreamName, "_", "", -1)),
					Effect: "Allow",
					Action: []string{
						"glue:GetTable",
						"glue:GetTableVersion",
						"glue:GetTableVersions",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:glue:%s:%d:catalog", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
						fmt.Sprintf("arn:aws:glue:%s:%d:database/datastreams", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
						fmt.Sprintf("arn:aws:glue:%s:%d:table/datastreams/%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), registryEntry.StreamName),
						fmt.Sprintf("arn:aws:glue:%s:%d:database/datastreams_schema", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
						fmt.Sprintf("arn:aws:glue:%s:%d:table/datastreams_schema/%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), registryEntry.StreamName),
					},
				},
				{
					Sid:    fmt.Sprintf("Allow%sFirehoseWriteToCloudWatch", strings.Replace(registryEntry.StreamName, "_", "", -1)),
					Effect: "Allow",
					Action: []string{
						"logs:PutLogEvents",
					},
					Resource: []string{
						fmt.Sprintf("arn:aws:logs:%s:%d:log-group:%s:log-stream:%s", region, infraconsts.GetAccountIdForRegion(region), datalakeFirehosesLogGroupName, registryEntry.StreamName),
					},
				},
			},
		},
	}

	return firehoseRole, firehoseRolePolicy
}
