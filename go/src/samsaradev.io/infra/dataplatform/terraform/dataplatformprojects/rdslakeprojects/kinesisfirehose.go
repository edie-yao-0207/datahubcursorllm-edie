package rdslakeprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

const KinesisFirehoseNamePrefix = "rds-kinesis-firehose"

// kinesisFirehoseProject creates a project containing Kinesis Data Firehose delivery streams
// for the new DMS->Kinesis architecture
// This project deploys to the main account
func kinesisFirehoseProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	// Create cross-account source role for Firehose delivery streams
	sourceRoleResources, err := createCrossAccountSourceRoleResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating cross-account source role resources")
	}

	// Create Firehose delivery streams for all databases with Kinesis stream destination enabled
	firehoseResources, err := createKinesisFirehoseResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating kinesis firehose resources")
	}

	// Merge all resources
	allResources := make(map[string][]tf.Resource)
	for k, v := range sourceRoleResources {
		allResources[k] = v
	}
	for k, v := range firehoseResources {
		allResources[k] = v
	}

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider:        config.AWSProviderGroup,
		Class:           "kinesis-firehose-delivery-streams",
		ResourceGroups:  allResources,
		GenerateOutputs: true,
	}

	// Use AWS provider version AWSProviderVersion5 = "~> 5.0"
	// With the older version, the buffering fields were not supported with dynamic partitioning.
	const awsProviderVersion = resource.AWSProviderVersion5
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
		"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
	})

	return p, nil
}

// createCrossAccountSourceRoleResources creates the cross-account source role in the main account
// This role can assume the target role in the Databricks account (like EMR pattern)
func createCrossAccountSourceRoleResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var resources []tf.Resource

	// Create cross-account source role (in main account)
	sourceRole := &awsresource.IAMRole{
		Name: "firehose-rds-replication-export-cross-account-source-role",
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
			"samsara:service":       "firehose-rds-replication-export-cross-account-source-role",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, sourceRole)

	// Create IAM policy for the source role
	sourceRolePolicy := &awsresource.IAMRolePolicy{
		Role: sourceRole.ResourceId(),
		Name: "firehose-rds-replication-export-cross-account-source-role-policy",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{
						"kinesis:DescribeStream",
						"kinesis:GetShardIterator",
						"kinesis:GetRecords",
						"kinesis:ListShards",
					},
					Effect: "Allow",
					Resource: []string{
						// Allow access to all Kinesis streams for databases with Kinesis stream destination enabled
						fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s-*", config.Region, config.AWSAccountId, KinesisStreamNamePrefix),
					},
				},
				{
					Action: []string{
						"sts:AssumeRole",
					},
					Effect: "Allow",
					Resource: []string{
						// Allow assuming the target role in Databricks account
						fmt.Sprintf("arn:aws:iam::%d:role/firehose-rds-replication-export-cross-account-target-role", config.DatabricksAWSAccountId),
					},
				},
				{
					Action: []string{
						"kms:Decrypt",
						"kms:GenerateDataKey",
						"kms:DescribeKey",
					},
					Effect:   "Allow",
					Resource: []string{"*"},
					Condition: &policy.AWSPolicyCondition{
						ForAllValuesStringLike: map[string][]string{
							"kms:ResourceAliases": getKMSAliasesForDatabases(config),
						},
					},
				},
				{
					Action: []string{
						"s3:AbortMultipartUpload",
						"s3:GetBucketLocation",
						"s3:GetObject",
						"s3:ListBucket",
						"s3:ListBucketMultipartUploads",
						"s3:PutObject",
						"s3:PutObjectAcl",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[config.Region]+"rds-kinesis-export"),
						fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[config.Region]+"rds-kinesis-export"),
					},
				},
				{
					Action: []string{
						"logs:PutLogEvents",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:logs:%s:%d:log-group:/aws/kinesisfirehose/*", config.Region, config.AWSAccountId),
					},
				},
			},
		},
	}
	resources = append(resources, sourceRolePolicy)

	return map[string][]tf.Resource{
		"cross_account_source_role": resources,
	}, nil
}

// createKinesisFirehoseResources creates Kinesis Data Firehose delivery streams for all databases
// that have EnableKinesisStreamDestination enabled
// Always creates one Firehose per database with dynamic partitioning to route records to correct S3 prefix
func createKinesisFirehoseResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var resources []tf.Resource

	// Get all databases that have Kinesis stream destination enabled
	databases := rdsdeltalake.AllDatabases()
	for _, db := range databases {
		if !db.IsInRegion(config.Region) || !db.InternalOverrides.EnableKinesisStreamDestination {
			continue
		}

		shards := db.RegionToShards[config.Region]
		if len(shards) == 0 {
			continue
		}

		// Always create a single Firehose per database with dynamic partitioning
		// The _dms_shard_name field in records will be used to route to correct S3 prefix
		firehoseResources, err := createFirehoseDeliveryStreamForDatabase(config, db)
		if err != nil {
			return nil, oops.Wrapf(err, "creating firehose delivery stream for database %s", db.MySqlDb)
		}
		resources = append(resources, firehoseResources...)
	}

	return map[string][]tf.Resource{
		"kinesis_firehose_delivery_streams": resources,
	}, nil
}

// createFirehoseDeliveryStreamForDatabase creates a Kinesis Data Firehose delivery stream
// for a database using dynamic partitioning to route records to S3
// S3 prefix structure: dms-kinesis/{shard-name}/{task-type}/{table-name-with-version}/{date-partition}/
// - Example: dms-kinesis/prod-dataplatformtestinternal-shard-5db/load/json_notes_v0/2025-10-30/
// - Matches the DMS parquet output structure for consistency
func createFirehoseDeliveryStreamForDatabase(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) ([]tf.Resource, error) {
	var resources []tf.Resource

	// Use first shard to determine metadata (all shards belong to same database)
	shards := db.RegionToShards[config.Region]
	if len(shards) == 0 {
		return nil, oops.Errorf("database %s has no shards in region %s", db.MySqlDb, config.Region)
	}

	firstShard := shards[0]
	dbName := getDbName(db.MySqlDb, firstShard)
	teamOwner, ok := dbregistry.DbToTeam[strings.TrimSuffix(dbName, "db")]
	if !ok {
		return nil, oops.Errorf("Cannot find owner of this database: %s", dbName)
	}

	hasProduction := db.HasProductionTable()
	rndAllocation := "1"
	if hasProduction {
		rndAllocation = "0"
	}

	serviceTag := fmt.Sprintf("%s-%s", KinesisFirehoseNamePrefix, dbName)

	tags := map[string]string{
		"samsara:service":        serviceTag,
		"samsara:team":           strings.ToLower(teamOwner.Name()),
		"samsara:product-group":  strings.ToLower(team.TeamProductGroup[teamOwner.Name()]),
		"samsara:rnd-allocation": rndAllocation,
		"samsara:production":     fmt.Sprintf("%v", hasProduction),
		"owner":                  strings.ToLower(team.DataPlatform.TeamName),
	}

	// Create Firehose delivery stream with dynamic partitioning
	deliveryStreamName := fmt.Sprintf("%s-%s", KinesisFirehoseNamePrefix, dbName)

	// Determine stream name (all shards write to same stream)
	streamName := KinesisStreamName(dbName)

	// Create CloudWatch log group for Firehose delivery stream
	cloudWatchLogGroup := &awsresource.CloudwatchLogGroup{
		ResourceName:    deliveryStreamName + "_log_group",
		Name:            fmt.Sprintf("/aws/kinesisfirehose/%s", deliveryStreamName),
		RetentionInDays: 14,
		Tags:            tags,
	}
	resources = append(resources, cloudWatchLogGroup)

	// Create CloudWatch log stream for Firehose delivery stream
	cloudWatchLogStream := &awsresource.CloudwatchLogStream{
		ResourceName: deliveryStreamName + "_log_stream",
		Name:         deliveryStreamName,
		LogGroupName: cloudWatchLogGroup.Name,
	}
	resources = append(resources, cloudWatchLogStream)

	deliveryStream := &awsresource.KinesisFirehoseDeliveryStream{
		Name:        deliveryStreamName,
		Destination: "extended_s3",
		KinesisSourceConfiguration: awsresource.KinesisSourceConfiguration{
			KinesisStreamARN: fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s", config.Region, config.AWSAccountId, streamName),
			RoleARN:          "${aws_iam_role.firehose-rds-replication-export-cross-account-source-role.arn}",
		},
		ExtendedS3Configuration: awsresource.KinesisFirehoseDeliveryStreamExtendedS3Configuration{
			RoleARN:           "${aws_iam_role.firehose-rds-replication-export-cross-account-source-role.arn}",
			BucketARN:         fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[config.Region]+"rds-kinesis-export"),
			Prefix:            "dms-kinesis/!{partitionKeyFromQuery:shardName}/!{partitionKeyFromQuery:taskType}/!{partitionKeyFromQuery:tableName}/!{timestamp:yyyy-MM-dd}/",
			ErrorOutputPrefix: "dms-kinesis-errors/!{firehose:error-output-type}/!{timestamp:yyyy-MM-dd}/",
			BufferSize:        128, // Required minimum 64MB when dynamic partitioning is enabled
			BufferInterval:    300, // 5 minutes
			CompressionFormat: "GZIP",
			// Enable dynamic partitioning to route records to S3 based on shard name, task type, table name, and date
			DynamicPartitioningConfiguration: awsresource.KinesisFirehoseDynamicPartitioningConfiguration{
				Enabled:       true,
				RetryDuration: 300,
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
								// Extract partitioning fields from the DMS Kinesis record
								// DMS Kinesis format for JSON includes:
								//   - .data: column values including _dms_shard_name and _dms_task_type (added via DMS transformations)
								//   - .metadata[\"table-name\"]: table name with version suffix (e.g., "json_notes_v0")
								// For sharded databases, shardName will be the shard name (e.g., "prod-audits-shard-4db")
								// For unsharded databases, shardName will be the database name (e.g., "prod-clouddb")
								// taskType will be "cdc" or "load" based on which DMS task wrote the record
								// tableName includes the version suffix added by DMS transformation (e.g., "json_notes_v0")
								ParameterName:  "MetadataExtractionQuery",
								ParameterValue: "{shardName:.data._dms_shard_name,taskType:.data._dms_task_type,tableName:.metadata[\\\"table-name\\\"]}",
							},
						},
					},
					{
						Type: "AppendDelimiterToRecord",
					},
				},
			},
			// Add CloudWatch logging for monitoring and debugging
			CloudwatchLoggingOptions: awsresource.KinesisFirehoseDeliveryStreamCloudwatchLoggingOptions{
				Enabled:       true,
				LogGroupName:  cloudWatchLogGroup.Name,
				LogStreamName: cloudWatchLogStream.Name,
			},
		},
		Tags: tags,
	}
	resources = append(resources, deliveryStream)

	return resources, nil
}

// getKMSAliasesForDatabases returns the KMS aliases for all databases with Kinesis stream destination enabled
// Streams use database-level aliases (alias/rds-kinesis-kms-key-{dbname}), not shard-level aliases
func getKMSAliasesForDatabases(config dataplatformconfig.DatabricksConfig) []string {
	databases := rdsdeltalake.AllDatabases()
	aliases := make([]string, 0)
	for _, db := range databases {
		if !db.IsInRegion(config.Region) || !db.InternalOverrides.EnableKinesisStreamDestination {
			continue
		}
		// Resolve database name to match alias creation in createKinesisStreamForDatabase
		shards := db.RegionToShards[config.Region]
		if len(shards) == 0 {
			continue
		}
		resolvedDbName := getDbName(db.MySqlDb, shards[0])
		aliases = append(aliases, fmt.Sprintf("alias/%s-%s", KinesisStreamKeyNamePrefix, resolvedDbName))
	}
	return aliases
}
