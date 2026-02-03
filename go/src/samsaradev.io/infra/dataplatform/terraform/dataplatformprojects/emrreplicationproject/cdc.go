package emrreplicationproject

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

type streamType string

const (
	streamTypeCDC      streamType = "cdc"
	streamTypeBackfill streamType = "backfill"
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

const EmrReplicationKinesisDataStreams = "emr_replication_kinesis_data_streams"

func KmsKeyName(streamName string) string {
	return fmt.Sprintf("%s_kms_key", streamName)
}

var tags = map[string]string{
	"samsara:service":       "emr_replication_cdc",
	"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
	"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
}

// Generate KMS key for kinesis data streams - shared across all cells
var KinesisKey = &awsresource.KMSKey{
	ResourceName:      KmsKeyName(EmrReplicationKinesisDataStreams),
	Description:       "KMS key for EMR replication kinesis data streams",
	EnableKeyRotation: true,
	Tags:              tags,
}

func emrCdcProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	resourceGroups := make(map[string][]tf.Resource)

	// Get all cells for the region
	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "getting cells for region %s", config.Region)
	}

	// Create Kinesis streams for each cell
	kinesisDataStreamsResources, err := createKinesisDataStreamsForStreamType(config, cells, streamTypeCDC)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate resources for Kinesis Data Streams")
	}
	resourceGroups[EmrReplicationKinesisDataStreams] = kinesisDataStreamsResources

	// Create Firehose delivery streams for each cell
	firehoseResources, err := createKinesisFirehoseDeliveryForStreamType(config, cells, streamTypeCDC)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate resources for Firehose Delivery Streams")
	}
	resourceGroups[EmrReplicationKinesisDataStreams+"_firehose_delivery"] = firehoseResources

	return &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider:        config.AWSProviderGroup,
		Class:           "emr_replication",
		Name:            "emr_replication_cdc",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
	}, nil
}

// getKinesisDataStreamNamePrefix returns the appropriate stream name prefix based on the stream type
func getKinesisDataStreamNamePrefix(streamType streamType) (string, error) {
	switch streamType {
	case streamTypeCDC:
		return EmrReplicationKinesisDataStreams, nil
	case streamTypeBackfill:
		return EmrReplicationBackfillKinesisDataStreams, nil
	default:
		return "", oops.Errorf("unsupported stream type: %s", streamType)
	}
}

// getKMSKeyForStreamType returns the appropriate KMS key based on the stream
// type.
func getKMSKeyForStreamType(streamType streamType) *awsresource.KMSKey {
	switch streamType {
	case streamTypeBackfill:
		return BackfillKinesisKey
	default:
		return KinesisKey
	}
}

// getKMSAliasForStreamType returns the appropriate KMS alias based on the stream
// type.
func getKMSAliasForStreamType(streamType streamType) *awsresource.KMSAlias {
	kmsKey := getKMSKeyForStreamType(streamType)
	return &awsresource.KMSAlias{
		Name:        fmt.Sprintf("alias/%s", kmsKey.ResourceName),
		TargetKeyId: kmsKey.ResourceId().ReferenceAttr("arn"),
	}
}

// createKinesisDataStreamsForStreamType creates the kinesis data streams for each cell in EMR replication
func createKinesisDataStreamsForStreamType(config dataplatformconfig.DatabricksConfig, cells []string, streamType streamType) ([]tf.Resource, error) {
	var resources []tf.Resource

	// Add shared KMS key and alias
	kmsKey := getKMSKeyForStreamType(streamType)
	resources = append(resources, kmsKey, getKMSAliasForStreamType(streamType))

	streamNamePrefix, err := getKinesisDataStreamNamePrefix(streamType)
	if err != nil {
		return nil, err
	}

	// Create a stream for each cell
	for _, cell := range cells {
		streamName := fmt.Sprintf("%s_%s", streamNamePrefix, cell)
		stream := &awsresource.KinesisStream{
			Name: streamName,
			StreamModeDetails: &awsresource.StreamModeDetails{
				StreamMode: "ON_DEMAND",
			},
			RetentionPeriod: 24,
			EncryptionType:  "KMS",
			KMSKey:          getKMSAliasForStreamType(streamType).ResourceId(),

			ShardLevelMetrics: kinesisShardLevelMetrics,
			Tags: map[string]string{
				"samsara:service":       "emr_replication_" + string(streamType),
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				"samsara:cell":          cell,
			},

			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					Lifecycle: tf.Lifecycle{
						PreventDestroy: true,
					},
				},
			},
		}

		resources = append(resources, stream)
	}

	return resources, nil
}

func getSourceRoleName(streamType streamType) string {
	return fmt.Sprintf("firehose-emr-%s-export-cross-account-source-role", streamType)
}

func getSourceRole(streamType streamType) *awsresource.IAMRole {
	crossAccountSourceRole := &awsresource.IAMRole{
		Name: getSourceRoleName(streamType),
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
			"samsara:service":       getSourceRoleName(streamType),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	return crossAccountSourceRole
}

func createKinesisFirehoseDeliveryForStreamType(config dataplatformconfig.DatabricksConfig, cells []string, streamType streamType) ([]tf.Resource, error) {
	var resources []tf.Resource

	crossAccountSourceRole := getSourceRole(streamType)

	resources = append(resources, crossAccountSourceRole)

	// Collect all Kinesis stream ARNs for the policy
	var kinesisStreamArns []string
	var bucketArns []string
	var logGroupNames []string
	for _, cell := range cells {
		streamNamePrefix, err := getKinesisDataStreamNamePrefix(streamType)
		if err != nil {
			return nil, oops.Wrapf(err, "getting stream name prefix for cell %s", cell)
		}
		streamName := fmt.Sprintf("%s_%s", streamNamePrefix, cell)
		kinesisArn := fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s", config.Region, config.AWSAccountId, streamName)
		kinesisStreamArns = append(kinesisStreamArns, kinesisArn)

		// Collect bucket ARNs for each cell
		bucketName := fmt.Sprintf("%semr-replication-export-%s", awsregionconsts.RegionPrefix[config.Region], cell)
		bucketArns = append(bucketArns,
			fmt.Sprintf("arn:aws:s3:::%s", bucketName),
			fmt.Sprintf("arn:aws:s3:::%s/*", bucketName),
		)

		logGroupNames = append(logGroupNames,
			fmt.Sprintf("arn:aws:logs:%s:%d:log-group:/aws/kinesisfirehose/%s:*", config.Region, config.AWSAccountId, fmt.Sprintf("%s_firehose", streamName)),
		)
	}

	// Create shared IAM policy that covers all cell-specific streams
	crossAccountSourceRolePolicy := &awsresource.IAMRolePolicy{
		Name: fmt.Sprintf("%s-policy", getSourceRoleName(streamType)),
		Role: crossAccountSourceRole.ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Sid:      "AllowFirehoseToAssumeCrossAccountRole",
					Effect:   "Allow",
					Action:   []string{"sts:AssumeRole"},
					Resource: []string{fmt.Sprintf("arn:aws:iam::%d:role/%s", config.DatabricksAWSAccountId, getTargetRoleName())},
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
					Resource: kinesisStreamArns,
				},
				{
					Sid:    "FireHoseDecryptsKMS",
					Effect: "Allow",
					Action: []string{
						"kms:Decrypt",
						"kms:GenerateDataKey",
					},
					Resource: []string{
						getKMSKeyForStreamType(streamType).ResourceId().ReferenceAttr("arn"),
					},
				},
				{
					Sid:    "AllowFirehoseToWriteToEMRReplicationExportBucket",
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
					Resource: bucketArns,
				},
				{
					Sid:    "AllowPutLogEvents",
					Effect: "Allow",
					Action: []string{
						"logs:PutLogEvents",
					},
					Resource: logGroupNames,
				},
			},
		},
	}

	resources = append(resources, crossAccountSourceRolePolicy)

	// Create cell-specific resources
	for _, cell := range cells {
		streamNamePrefix, err := getKinesisDataStreamNamePrefix(streamType)
		if err != nil {
			return nil, oops.Wrapf(err, "getting stream name prefix for cell %s", cell)
		}
		streamName := fmt.Sprintf("%s_%s", streamNamePrefix, cell)
		firehoseName := fmt.Sprintf("%s_firehose", streamName)
		kinesisArn := fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s", config.Region, config.AWSAccountId, streamName)

		// Create cell-specific CloudWatch log group
		cloudWatchLogGroup := &awsresource.CloudwatchLogGroup{
			ResourceName:    firehoseName + "_log_group",
			Name:            fmt.Sprintf("/aws/kinesisfirehose/%s", firehoseName),
			RetentionInDays: 14,
			Tags: map[string]string{
				"samsara:service":       "emr_replication_" + string(streamType),
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				"samsara:cell":          cell,
			},
		}
		resources = append(resources, cloudWatchLogGroup)

		// Create cell-specific CloudWatch log stream
		cloudWatchLogStream := &awsresource.CloudwatchLogStream{
			ResourceName: firehoseName + "_log_stream",
			Name:         streamName,
			LogGroupName: cloudWatchLogGroup.Name,
		}
		resources = append(resources, cloudWatchLogStream)

		// Extract the entity name and backfillRequestId for partitioning
		firehosePartition := "!{partitionKeyFromQuery:entityName}"
		firehoseErrorPartition := "!{firehose:error-output-type}"

		// Create cell-specific firehose delivery stream
		firehoseResource := &awsresource.KinesisFirehoseDeliveryStream{
			Name: firehoseName,
			Tags: map[string]string{
				"samsara:service":       "emr_replication_" + string(streamType),
				"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
				"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				"samsara:cell":          cell,
			},
			Destination: "extended_s3",
			KinesisSourceConfiguration: awsresource.KinesisSourceConfiguration{
				KinesisStreamARN: kinesisArn,
				RoleARN:          crossAccountSourceRole.ResourceId().ReferenceAttr("arn"),
			},
			ExtendedS3Configuration: awsresource.KinesisFirehoseDeliveryStreamExtendedS3Configuration{
				RoleARN:           crossAccountSourceRole.ResourceId().ReferenceAttr("arn"),
				BucketARN:         fmt.Sprintf("arn:aws:s3:::%semr-replication-export-%s", awsregionconsts.RegionPrefix[config.Region], cell),
				Prefix:            getS3PrefixForStreamType(streamType, firehosePartition),
				ErrorOutputPrefix: fmt.Sprintf("%s/errors/%s/", string(streamType), firehoseErrorPartition),
				BufferSize:        128,
				BufferInterval:    300,

				DynamicPartitioningConfiguration: awsresource.KinesisFirehoseDynamicPartitioningConfiguration{
					Enabled:       true,
					RetryDuration: 300,
				},
				CloudwatchLoggingOptions: awsresource.KinesisFirehoseDeliveryStreamCloudwatchLoggingOptions{
					Enabled:       true,
					LogGroupName:  cloudWatchLogGroup.Name,
					LogStreamName: cloudWatchLogStream.Name,
				},

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
									ParameterValue: getMetadataExtractionQueryForStreamType(streamType),
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
	}

	return resources, nil
}

// getS3PrefixForStreamType returns the appropriate S3 prefix based on the
// stream type.
func getS3PrefixForStreamType(streamType streamType, firehosePartition string) string {
	switch streamType {
	case streamTypeBackfill:
		backfillRequestIdPartition := "!{partitionKeyFromQuery:backfillRequestId}"
		return fmt.Sprintf("%s/data/%s/%s/!{timestamp:yyyy-MM-dd-HH}-", string(streamType), firehosePartition, backfillRequestIdPartition)
	default:
		return fmt.Sprintf("%s/data/%s/!{timestamp:yyyy-MM-dd-HH}-", string(streamType), firehosePartition)
	}
}

// getMetadataExtractionQueryForStreamType returns the appropriate metadata
// extraction query based on the stream type.
func getMetadataExtractionQueryForStreamType(streamType streamType) string {
	switch streamType {
	case streamTypeBackfill:
		return "{entityName:.entityName,backfillRequestId:.backfillRequestId}"
	default:
		return "{entityName:.entityName}"
	}
}
