package dynamodbdeltalake

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

var kinesisShardLevelMetrics = []string{
	"IncomingBytes",
	"IncomingRecords",
	"IteratorAgeMilliseconds",
	"OutgoingBytes",
	"OutgoingRecords",
	"ReadProvisionedThroughputExceeded",
	"WriteProvisionedThroughputExceeded",
}

const DynamodbDeltaLakeKinesisDataStreams = "dynamodb_deltalake_kinesis_data_streams"

var tags = map[string]string{
	"samsara:service":       DynamodbDeltaLakeResourceBaseName + "_cdc",
	"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
	"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
}

// Generate KMS key for kinesis data streams
var KinesisKey = &awsresource.KMSKey{
	ResourceName:      DynamodbDeltaLakeKinesisDataStreams + "_kms_key",
	Description:       "KMS key for dynamodb delta lake kinesis data streams",
	EnableKeyRotation: true,
	Tags:              tags,
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func dynamodbCdcProject(providerGroup string) (*project.Project, error) {

	// CDC project is reponsible for creating the infrastructure for the dynamodb tables cdc
	// This includes creating the kinesis data streams for the dynamodb tables
	// and firehose delivery streams for the kinesis data streams to write to s3.
	// Plug this kinensis data streams into the dynamodb tables.

	//     [DynamaoDB Tables]
	//
	//				 		|
	//      	 		|
	//     			 \|/  CDC changes gets pushed to Kinesis Data Streams
	//
	// 		[Kinesis Data Streams]
	//
	//					 /|\
	//					 	|  Firehose Delivery Stream reads from Kinesis Data Streams
	//		 			  |  and writes to S3 based on dynamic partitioning by table name
	//
	// 		[Firehose Delivery Stream]
	//
	//					 /|\            /|\
	//					/	| \            |
	//				 /	|  \           |
	//				/	  |   \       [CROSS ACCOUNT IAM ROLE]
	//			 /	  |    \                1. Access to read Kinesis data streams in Main AWS account
	//		 \|/	 \|/   \|/              2. Access to write to S3 bucket in DBX AWS account
	//     S3     S3    S3
	//     Path   Path  Path
	//      |			 |			|
	//			|			 |			|    These paths are read by the DBX merge job
	//			|			 |			|
	//   Table1  Table2 Table3

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	resourceGroups := make(map[string][]tf.Resource)
	kinesisDataStreamsResources, err := createKinesisDataStreams(config)

	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate resources for Kinesis Data Streams")
	}

	resourceGroups[DynamodbDeltaLakeKinesisDataStreams] = kinesisDataStreamsResources

	resourceGroups[DynamodbDeltaLakeKinesisDataStreams+"_firehose_delivery"] = createKinesisFirehoseDelivery(config)

	resourceGroups[DynamodbDeltaLakeKinesisDataStreams+"_dynamodb_kinesis_streaming_destination"] = CreateDynamodbKinesisStreamingDestination(config)

	return &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDynamodbDeltaLakeProjectPipeline,
		Provider:        config.AWSProviderGroup,
		Class:           DynamodbDeltaLakeResourceBaseName,
		Name:            DynamodbDeltaLakeResourceBaseName + "-cdc",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
	}, nil

}

// createKinesisDataStreams creates the kinesis data streams for the dynamodb tables
func createKinesisDataStreams(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	var shards int

	switch config.Region {
	case infraconsts.SamsaraAWSDefaultRegion:
		shards = 48
	case infraconsts.SamsaraAWSEURegion:
		shards = 6
	case infraconsts.SamsaraAWSCARegion:
		// Keep Canada to 2 and scale up when fully ready to launch.
		shards = 2
	default:
		return nil, oops.Errorf("unsupported region: %s", config.Region)
	}

	resources = append(resources, KinesisKey)

	kmsKeyAlias := &awsresource.KMSAlias{
		Name:        fmt.Sprintf("alias/%s", KinesisKey.ResourceName),
		TargetKeyId: KinesisKey.ResourceId().ReferenceAttr("arn"),
	}

	resources = append(resources, kmsKeyAlias)

	stream := &awsresource.KinesisStream{
		Name:            DynamodbDeltaLakeKinesisDataStreams,
		ShardCount:      shards,
		RetentionPeriod: 24,
		EncryptionType:  "KMS",
		KMSKey:          KinesisKey.ResourceId(),

		ShardLevelMetrics: kinesisShardLevelMetrics,
		Tags:              tags,

		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
	}

	resources = append(resources, stream)

	return resources, nil
}

func createKinesisFirehoseDelivery(config dataplatformconfig.DatabricksConfig) []tf.Resource {
	var resources []tf.Resource

	crossAccountSourceRole := &awsresource.IAMRole{
		Name: "firehose-dynamodb-streams-cross-account-source-role",
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
			"samsara:service":       "firehose-dynamodb-streams-cross-account-source-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	resources = append(resources, crossAccountSourceRole)

	firehoseName := DynamodbDeltaLakeKinesisDataStreams + "_firehose"

	cloudWatchLogGroup := &awsresource.CloudwatchLogGroup{
		ResourceName:    firehoseName + "_log_group",
		Name:            fmt.Sprintf("/aws/kinesisfirehose/%s", firehoseName),
		RetentionInDays: 14,
		Tags:            tags,
	}

	resources = append(resources, cloudWatchLogGroup)

	cloudWatchLogStream := &awsresource.CloudwatchLogStream{
		ResourceName: firehoseName + "_log_stream",
		Name:         DynamodbDeltaLakeKinesisDataStreams,
		LogGroupName: cloudWatchLogGroup.Name,
	}

	resources = append(resources, cloudWatchLogStream)

	kinesisArn := fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s", config.Region, config.AWSAccountId, DynamodbDeltaLakeKinesisDataStreams)

	// Create a policy for the firehose to assume the cross account role
	// in the DBX account which has access to write to the S3 bucket
	// Allows Firehose to read from kinesis data streams
	// Allows Firehose to put log events
	crossAccountSourceRolePolicy := &awsresource.IAMRolePolicy{
		Name: "firehose-dynamodb-streams-cross-account-source-role-policy",
		Role: crossAccountSourceRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:      "AllowFirehoseToWriteKinesisDataStreamsByAssumingCrossAccountRole",
					Effect:   "Allow",
					Action:   []string{"sts:AssumeRole"},
					Resource: []string{fmt.Sprintf("arn:aws:iam::%d:role/firehose-dynamodb-streams-cross-account-target-role", config.DatabricksAWSAccountId)},
				},
				{
					Sid:    "AllowFireHoseToReadKinesisStreams",
					Effect: "Allow",
					Action: []string{
						"kinesis:DescribeStream",
						"kinesis:GetShardIterator",
						"kinesis:GetRecords",
						"kinesis:ListShards",
					},
					Resource: []string{
						kinesisArn,
					},
				},
				{
					Sid:    "PutLogEvents",
					Effect: "Allow",
					Action: []string{
						"logs:PutLogEvents",
					},
					Resource: []string{
						cloudWatchLogGroup.ResourceId().ReferenceAttr("arn") + ":log-stream:*",
					},
				},

				{
					Sid:    "FireHoseDecryptsKMS",
					Effect: "Allow",
					Action: []string{
						"kms:Decrypt",
						"kms:GenerateDataKey",
					},
					Resource: []string{
						KinesisKey.ResourceId().ReferenceAttr("arn"),
					},
				},
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
						fmt.Sprintf("arn:aws:s3:::%sdynamodb-export", awsregionconsts.RegionPrefix[config.Region]),
						fmt.Sprintf("arn:aws:s3:::%sdynamodb-export/*", awsregionconsts.RegionPrefix[config.Region]),
					},
				},
			},
		},
	}

	resources = append(resources, crossAccountSourceRolePolicy)

	// Extact the table name using the metadata extraction processor
	// firehouse uses this table name to write to S3 path
	firehosePartition := "!{partitionKeyFromQuery:tableName}"

	// firehose writes errors to this path which catches the errors from the firehose
	firehoseErrorPartition := "!{firehose:error-output-type}"

	// Create a firehose delivery stream to write to S3
	firehoseResource := &awsresource.KinesisFirehoseDeliveryStream{
		Name:        firehoseName,
		Tags:        tags,
		Destination: "extended_s3",
		KinesisSourceConfiguration: awsresource.KinesisSourceConfiguration{
			KinesisStreamARN: kinesisArn,
			RoleARN:          crossAccountSourceRole.ResourceId().ReferenceAttr("arn"),
		},
		ExtendedS3Configuration: awsresource.KinesisFirehoseDeliveryStreamExtendedS3Configuration{
			RoleARN:           crossAccountSourceRole.ResourceId().ReferenceAttr("arn"),
			BucketARN:         fmt.Sprintf("arn:aws:s3:::%sdynamodb-export", awsregionconsts.RegionPrefix[config.Region]),
			Prefix:            fmt.Sprintf("kinesis_streams/data/%s/!{timestamp:yyyy-MM-dd-HH}-", firehosePartition),
			ErrorOutputPrefix: fmt.Sprintf("kinesis_streams/errors/%s/", firehoseErrorPartition),
			BufferSize:        128,
			BufferInterval:    300,

			// Enable dynamic partitioning to write to S3 based on table name extracted from the metadata extraction processor
			DynamicPartitioningConfiguration: awsresource.KinesisFirehoseDynamicPartitioningConfiguration{
				Enabled:       true,
				RetryDuration: 300,
			},
			CloudwatchLoggingOptions: awsresource.KinesisFirehoseDeliveryStreamCloudwatchLoggingOptions{
				Enabled:       true,
				LogGroupName:  cloudWatchLogGroup.Name,
				LogStreamName: cloudWatchLogStream.Name,
			},

			// Use JQ to extract the table name from the json
			// firehose uses this table name to write to S3 path
			// also append a new line delimiter to the record
			ProcessingConfiguration: awsresource.KinesisFirehoseProcessingConfiguration{
				Enabled: true,
				Processors: []awsresource.KinesisFirehoseProcessingConfigurationProcessor{
					{
						Type: "MetadataExtraction",
						Parameters: []awsresource.KinesisFirehoseProcessingConfigurationProcessorParameter{
							{
								ParameterName:  "JsonParsingEngine",
								ParameterValue: "JQ-1.6",
							},
							{
								ParameterName:  "MetadataExtractionQuery",
								ParameterValue: "{tableName:.tableName}",
							},
						},
					},
					{
						Type: "AppendDelimiterToRecord",
					},
				},
			},
		},
	}

	resources = append(resources, firehoseResource)

	return resources
}

func CreateDynamodbKinesisStreamingDestination(config dataplatformconfig.DatabricksConfig) []tf.Resource {

	var resources []tf.Resource

	kinesisArn := fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s", config.Region, config.AWSAccountId, DynamodbDeltaLakeKinesisDataStreams)

	allTables := dynamodbdeltalake.AllTables()
	for _, table := range allTables {

		if contains(table.InternalOverrides.UnavailableRegions, config.Region) || table.InternalOverrides.DisableKinesisStreamingDestination {
			// if the dynamo table is not available in this region, skip it
			// if the dynamo table has disabled kinesis streaming destination, skip it
			continue
		}

		resources = append(resources, &awsresource.DynamodbKinesisStreamingDestination{
			ResourceName: "dynamodb_kinesis_streaming_destination_" + table.TableName,
			TableName:    table.TableName,
			StreamArn:    kinesisArn,
		})
	}

	return resources

}
