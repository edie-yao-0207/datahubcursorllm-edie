package rdslakeprojects

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/rdslakeprojects/config"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

type rdsKinesisReplicationSpec struct {
	// MySqlDbName is the MySQL database name (e.g., "productsdb") used for identifying the database
	// in the registry and for constructing resource names and tags.
	MySqlDbName string

	// DbShardName is the specific shard identifier (e.g., "prod-products-shard-1") used to uniquely
	// identify the DMS replication instance and endpoints for this database shard.
	DbShardName string

	// WriterHostname is the Aurora writer endpoint hostname that DMS connects to as the source
	// for reading database changes (e.g., "prod-products-shard-1.cluster-xyz.us-east-1.rds.amazonaws.com").
	WriterHostname string

	// Region is the AWS region where the DMS resources and Kinesis stream are created
	// (e.g., "us-east-1").
	Region string

	// Tables is the list of tables to replicate, including their names, versions, and columns
	// to exclude from replication for each table.
	Tables []rdsReplicationSpecTable

	// DmsEngineVersion specifies the AWS DMS replication engine version to use (e.g., "3.5.4"),
	// which determines available features and compatibility with the source/target endpoints.
	DmsEngineVersion awsresource.DmsEngineVersion

	// HasProduction indicates whether this database contains production data, used for setting
	// the samsara:production tag and samsara:rnd-allocation cost allocation tags.
	HasProduction bool

	// KinesisStreamArn is the full ARN of the target Kinesis Data Stream where DMS will write
	// the replicated change data in JSON format (e.g., "arn:aws:kinesis:us-east-1:123456:stream/rds-kinesis-stream-clouddb").
	KinesisStreamArn string
}

// Pre-provisioned DMS certificate ARNs by region to enable verify-full at create time.
var dmsRegionToCertificateArn = map[string]string{
	"us-west-2":    "arn:aws:dms:us-west-2:781204942244:cert:F6MQKRU2KAUSPFSDSGZRQM5XCJGAKUAJ37QPMZY",
	"eu-west-1":    "arn:aws:dms:eu-west-1:947526550707:cert:TDJQGDUIO5F25EUKW43FHLIIWM",
	"ca-central-1": "arn:aws:dms:ca-central-1:589535615186:cert:VX2AM6UE7RCSXGW5BRECXQCBIQ",
}

func KinesisStreamCdcTaskName(dbshard string) string {
	return "kinesis-task-cdc-" + dbshard
}

func KinesisStreamLoadTaskName(dbshard string) string {
	return "kinesis-task-load-" + dbshard
}

// kinesisStreamDestinationProjects creates projects that combine Kinesis Data Streams and DMS replication
// resources into a single Terraform project. This eliminates remote state dependencies and simplifies deployment.
// Creates a stream (rds-kinesis-stream-{dbname}) that all DMS tasks write to, for both single-shard and multi-shard databases.
// This ensures consistency with Firehose delivery streams which always expect stream naming.
func kinesisStreamDestinationProjects(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) ([]*project.Project, error) {
	// Only create projects for databases that have the EnableKinesisStreamDestination flag enabled
	if !db.InternalOverrides.EnableKinesisStreamDestination {
		return nil, nil
	}

	shards := db.RegionToShards[config.Region]
	if len(shards) == 0 {
		return nil, nil
	}

	// Always create stream project (works for both single and multiple shards)
	// This ensures consistency with Firehose which always expects stream naming (rds-kinesis-stream-{dbname})
	return createKinesisStreamProject(config, db, shards)
}

// createKinesisStreamProject creates a single project with one Kinesis stream
// and all DMS tasks (one per shard) that write to that stream.
// Works for both single-shard and multi-shard databases to ensure consistency with Firehose
// which always expects stream naming (rds-kinesis-stream-{dbname}).
func createKinesisStreamProject(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase, shards []string) ([]*project.Project, error) {
	var allDMSResources []tf.Resource
	var kinesisStreamResources []tf.Resource

	hasProduction := db.HasProductionTable()
	dmsEngineVersion := awsresource.DmsEngineVersion(awsresource.DmsEngine3_5_4)
	if db.InternalOverrides.DmsEngineVersion != "" {
		dmsEngineVersion = db.InternalOverrides.DmsEngineVersion
	}

	// Create Kinesis stream (one stream for all shards)
	streamResources, stream, err := createKinesisStreamForDatabase(config, db)
	if err != nil {
		return nil, oops.Wrapf(err, "create kinesis stream for database %s", db.MySqlDb)
	}
	kinesisStreamResources = append(kinesisStreamResources, streamResources...)

	// Reference the stream ARN
	streamArnReference := stream.ResourceId().ReferenceAttr("arn")

	// Create DMS tasks for each shard, all pointing to the stream
	tables := make([]rdsReplicationSpecTable, 0, len(db.TablesInRegion(config.Region)))
	for _, table := range db.TablesInRegion(config.Region) {
		tables = append(tables, rdsReplicationSpecTable{
			Name:                  table.TableName,
			Version:               table.VersionInfo.DMSOutputVersionParquet(),
			CdcVersion:            table.CdcVersion_DO_NOT_SET,
			ExcludeColumns:        table.ExcludeColumns,
			PartitionKeyAttribute: table.PartitionKeyAttribute,
		})
	}

	for _, shard := range shards {
		writerHostnamePrefix := shard
		if db.Name == "clouddb" && config.Region == infraconsts.SamsaraAWSDefaultRegion {
			writerHostnamePrefix += "-cluster"
		} else if db.Name == "statsdb" && config.Region == infraconsts.SamsaraAWSEURegion {
			writerHostnamePrefix = "stats-aurora-replica-2-cluster"
		} else if config.Region == infraconsts.SamsaraAWSCARegion { // make regionconditionalchecker happy
			// do nothing
		}

		// Handle aurora 2 blue cluster cutover
		shardWithoutProdPrefix := strings.ReplaceAll(shard, "prod-", "")
		if shouldCutover, err := dbregistry.ShouldCutoverToBlue(shardWithoutProdPrefix, config.Region); err != nil {
			return nil, oops.Wrapf(err, "error checking whether shard %s in region %s should be cut over to the blue cluster", shardWithoutProdPrefix, config.Region)
		} else if shouldCutover {
			writerHostnamePrefix += "-blue"
		}

		// All DMS tasks write to the stream
		dms, err := kinesisReplicationResources(rdsKinesisReplicationSpec{
			MySqlDbName:      db.MySqlDb,
			DbShardName:      shard,
			Region:           config.Region,
			WriterHostname:   fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", writerHostnamePrefix, config.RDSClusterIdentifier, config.Region),
			Tables:           tables,
			DmsEngineVersion: dmsEngineVersion,
			HasProduction:    hasProduction,
			KinesisStreamArn: streamArnReference, // All shards use the same stream
		})
		if err != nil {
			return nil, oops.Wrapf(err, "create DMS resources for shard %s", shard)
		}
		allDMSResources = append(allDMSResources, dms...)
	}

	// Create single project containing Kinesis stream and all DMS tasks
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.AWSProviderGroup,
		Class:    "rdsreplication-kinesis",
		Name:     db.Name,
		ResourceGroups: map[string][]tf.Resource{
			"kinesis-streams": kinesisStreamResources,
			"dms-kinesis":     allDMSResources,
		},
		GenerateOutputs: true,
	}

	awsProviderVersion := resource.AWSProviderVersion5
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"remote_state": resource.MonolithRemoteState(config.AWSProviderGroup, p.Cloud()),
		"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
		"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
	})

	return []*project.Project{p}, nil
}

// buildKinesisDMSTableMappings creates DMS table mapping rules for RDS replication via KDS.
// If isCdc is true, uses CdcVersion (fixed version), otherwise uses Version (load version).
func buildKinesisDMSTableMappings(spec rdsKinesisReplicationSpec, isCdc bool) awsresource.DMSTableMapping {
	mapping := awsresource.DMSTableMapping{
		Rules: []*awsresource.DMSTableMappingRule{
			{
				RuleId:   aws.Int(0),
				RuleName: aws.String(fmt.Sprintf("%d", 0)),
				RuleType: aws.String("transformation"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String("%"),
					TableName:  aws.String("%"),
				},
				RuleAction: aws.String("add-column"),
				RuleTarget: aws.String("column"),
				Value:      aws.String("_rowid"),
				Expression: aws.String("$AR_H_CHANGE_SEQ"),
				DataType: &awsresource.DMSTableMappingRuleDataType{
					Type:   aws.String("string"),
					Length: aws.Int(50),
				},
			},
			// Add shard name column for dynamic partitioning in Firehose
			// This allows all DMS tasks to write to a single Kinesis stream while Firehose routes to correct S3 prefix
			{
				RuleId:   aws.Int(1),
				RuleName: aws.String(fmt.Sprintf("%d", 1)),
				RuleType: aws.String("transformation"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String("%"),
					TableName:  aws.String("%"),
				},
				RuleAction: aws.String("add-column"),
				RuleTarget: aws.String("column"),
				Value:      aws.String("_dms_shard_name"),
				// Use CONCAT to inject the literal shard name as a string value
				// DMS will include this in every record, allowing Firehose to partition by it
				Expression: aws.String(fmt.Sprintf("CONCAT('%s')", spec.DbShardName)),
				DataType: &awsresource.DMSTableMappingRuleDataType{
					Type:   aws.String("string"),
					Length: aws.Int(200),
				},
			},
			// Add task type column to distinguish between CDC and load tasks
			// This allows Firehose to route to correct S3 prefix (cdc/ or load/)
			{
				RuleId:   aws.Int(2),
				RuleName: aws.String(fmt.Sprintf("%d", 2)),
				RuleType: aws.String("transformation"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String("%"),
					TableName:  aws.String("%"),
				},
				RuleAction: aws.String("add-column"),
				RuleTarget: aws.String("column"),
				Value:      aws.String("_dms_task_type"),
				// Use CONCAT to inject the task type ("cdc" or "load") based on isCdc parameter
				Expression: func() *string {
					taskType := "load"
					if isCdc {
						taskType = "cdc"
					}
					return aws.String(fmt.Sprintf("CONCAT('%s')", taskType))
				}(),
				DataType: &awsresource.DMSTableMappingRuleDataType{
					Type:   aws.String("string"),
					Length: aws.Int(10),
				},
			},
		},
	}

	// Add selection and transformation rules for each table
	// Pattern: selection with "explicit" action, followed by add-suffix transformation for version
	for _, table := range spec.Tables {
		// Selection rule with "explicit" action
		ruleId := *mapping.Rules[len(mapping.Rules)-1].RuleId + 1
		mapping.Rules = append(mapping.Rules, &awsresource.DMSTableMappingRule{
			RuleId:   aws.Int(ruleId),
			RuleName: aws.String(fmt.Sprintf("%d", ruleId)),
			RuleType: aws.String("selection"),
			ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
				SchemaName: aws.String(spec.MySqlDbName),
				TableName:  aws.String(table.Name),
			},
			RuleAction: aws.String("explicit"),
		})

		// Transformation rule to add version suffix
		// CDC uses CdcVersion (fixed), load uses Version (can change)
		version := table.Version
		if isCdc {
			version = table.CdcVersion
		}

		ruleId = *mapping.Rules[len(mapping.Rules)-1].RuleId + 1
		mapping.Rules = append(mapping.Rules, &awsresource.DMSTableMappingRule{
			RuleId:     aws.Int(ruleId),
			RuleName:   aws.String(fmt.Sprintf("%d", ruleId)),
			RuleType:   aws.String("transformation"),
			RuleTarget: aws.String("table"),
			ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
				SchemaName: aws.String(spec.MySqlDbName),
				TableName:  aws.String(table.Name),
			},
			RuleAction: aws.String("add-suffix"),
			Value:      aws.String(fmt.Sprintf("_v%d", version)),
		})

		// Add object-mapping rule for custom partition key if PartitionKeyAttribute is configured.
		// This uses the specified column's value as the Kinesis partition key instead of the
		// default schema.table name, ensuring records with the same value go to the same shard.
		if table.PartitionKeyAttribute != "" {
			ruleId = *mapping.Rules[len(mapping.Rules)-1].RuleId + 1
			mapping.Rules = append(mapping.Rules, &awsresource.DMSTableMappingRule{
				RuleId:   aws.Int(ruleId),
				RuleName: aws.String(fmt.Sprintf("%d", ruleId)),
				RuleType: aws.String("object-mapping"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String(spec.MySqlDbName),
					TableName:  aws.String(table.Name),
				},
				RuleAction: aws.String("map-record-to-record"),
				MappingParameters: &awsresource.DMSTableMappingParameters{
					PartitionKeyType: aws.String("attribute-name"),
					PartitionKeyName: aws.String(table.PartitionKeyAttribute),
				},
			})
		}
	}

	return mapping
}

func kinesisReplicationResources(spec rdsKinesisReplicationSpec) ([]tf.Resource, error) {
	var resources []tf.Resource

	dbName := getDbName(spec.MySqlDbName, spec.DbShardName)

	teamOwner, ok := dbregistry.DbToTeam[strings.TrimSuffix(dbName, "db")]
	if !ok {
		return nil, oops.Errorf("Cannot find owner of this database: %s", dbName)
	}

	rndAllocation := "1"
	if spec.HasProduction {
		rndAllocation = "0"
	}

	serviceTag := fmt.Sprintf("dms-kinesis-%s", dbName)

	tags := map[string]string{
		"samsara:service":        serviceTag,
		"samsara:team":           strings.ToLower(teamOwner.Name()),
		"samsara:product-group":  strings.ToLower(team.TeamProductGroup[teamOwner.Name()]),
		"samsara:rnd-allocation": rndAllocation,
		"samsara:production":     fmt.Sprintf("%v", spec.HasProduction),
		"owner":                  strings.ToLower(team.DataPlatform.TeamName),
	}

	accountId := infraconsts.GetAccountIdForRegion(spec.Region)

	// Note: Kinesis Data Stream is created separately in kinesisDataStreamsProjects
	// We reference it by ARN here for the DMS endpoints

	// Create DMS source endpoint
	sourceId := fmt.Sprintf("rds-kinesis-%s", spec.DbShardName)
	source := &awsresource.DMSEndpoint{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"password"},
				},
			},
		},
		EndpointId:     sourceId,
		EndpointType:   "source",
		EngineName:     "aurora", // MySQL/Aurora - does not need database_name
		SslMode:        "verify-full",
		Username:       "prod_admin",
		ServerName:     spec.WriterHostname,
		Password:       "invalidpassword",
		Port:           3306,
		Tags:           tags,
		CertificateArn: dmsRegionToCertificateArn[spec.Region],
	}
	// Note: MySQL endpoints should NOT have database_name set - databases are specified in table mappings
	resources = append(resources, source)

	// Create DMS replication instance
	var engineVersion *string
	if string(spec.DmsEngineVersion) != "" {
		engineVersion = pointer.StringPtr(string(spec.DmsEngineVersion))
	}

	settingsJson, err := json.MarshalIndent(config.DesiredDMSReplicationTaskSettings, "", " ")
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal DMS settings")
	}

	size := "dms.t3.medium"
	if strings.Contains(spec.DbShardName, "compliance") || strings.Contains(spec.DbShardName, "cmassets") {
		size = "dms.c6i.large"
	}
	replicationInstanceName := "kinesis-" + spec.DbShardName

	replicationInstance := &awsresource.DMSReplicationInstance{
		ResourceName:             replicationInstanceName,
		InstanceId:               util.HashTruncate(replicationInstanceName, 63, 8, "-"),
		InstanceClass:            size,
		AutoMinorVersionUpgrade:  aws.Bool(false),
		AllowMajorVersionUpgrade: aws.Bool(true),
		ApplyImmediately:         aws.Bool(true),
		MultiAZ:                  aws.Bool(false),
		SubnetGroupId:            "rds-replication",
		VpcSecurityGroupIds: []string{
			tf.LocalId("security_groups").ReferenceKey("prod_app"),
			tf.LocalId("security_groups").ReferenceKey("prod_devices"),
			tf.LocalId("security_groups").ReferenceKey("prod_rds"),
		},
		Tags:          tags,
		EngineVersion: engineVersion,
		Timeouts: &awsresource.Timeouts{
			Create: "1h",
		},
	}
	resources = append(resources, replicationInstance)

	// Create Kinesis target endpoints for CDC and load
	// For Kinesis, we use the "kinesis" engine and configure via KinesisSettings
	cdcDestination := &awsresource.DMSEndpoint{
		EndpointId:   "kinesis-cdc-" + spec.DbShardName,
		EndpointType: "target",
		EngineName:   "kinesis",
		KinesisSettings: &awsresource.KinesisSettings{
			ServiceAccessRoleArn: fmt.Sprintf("arn:aws:iam::%d:role/dms-replication-write-kinesis", accountId),
			StreamArn:            spec.KinesisStreamArn,
			MessageFormat:        "json",
		},
		Tags: tags,
	}

	loadDestination := &awsresource.DMSEndpoint{
		EndpointId:   "kinesis-load-" + spec.DbShardName,
		EndpointType: "target",
		EngineName:   "kinesis",
		KinesisSettings: &awsresource.KinesisSettings{
			ServiceAccessRoleArn: fmt.Sprintf("arn:aws:iam::%d:role/dms-replication-write-kinesis", accountId),
			StreamArn:            spec.KinesisStreamArn,
			MessageFormat:        "json",
		},
		Tags: tags,
	}
	resources = append(resources, cdcDestination, loadDestination)

	// Create separate table mappings for CDC and load tasks
	// CDC uses CdcVersion_DO_NOT_SET (fixed version that never changes)
	// Load uses DMSOutputVersionParquet (version that can change)
	cdcTableMapping := buildKinesisDMSTableMappings(spec, true)
	loadTableMapping := buildKinesisDMSTableMappings(spec, false)

	cdcTableMappingJson, err := json.Marshal(cdcTableMapping)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to jsonify CDC mappings for kinesis task")
	}

	loadTableMappingJson, err := json.Marshal(loadTableMapping)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to jsonify load mappings for kinesis task")
	}

	// Create CDC task (uses CDC version)
	cdcTask := &awsresource.DMSReplicationTask{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"replication_task_settings", "cdc_start_position", "table_mappings"},
				},
			},
		},
		TaskId:            KinesisStreamCdcTaskName(spec.DbShardName),
		TaskSettings:      strings.ReplaceAll(string(settingsJson), `"`, `\"`),
		TableMappings:     strings.ReplaceAll(string(cdcTableMappingJson), `"`, `\"`),
		MigrationType:     "cdc",
		InstanceArn:       replicationInstance.ResourceId().ReferenceAttr("replication_instance_arn"),
		SourceEndpointArn: source.ResourceId().ReferenceAttr("endpoint_arn"),
		TargetEndpointArn: cdcDestination.ResourceId().ReferenceAttr("endpoint_arn"),
		Tags:              tags,
	}

	// Create initial load task (uses load version)
	initialLoadTask := &awsresource.DMSReplicationTask{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"replication_task_settings", "table_mappings"},
				},
			},
		},
		TaskId:            KinesisStreamLoadTaskName(spec.DbShardName),
		TaskSettings:      strings.ReplaceAll(string(settingsJson), `"`, `\"`),
		TableMappings:     strings.ReplaceAll(string(loadTableMappingJson), `"`, `\"`),
		MigrationType:     "full-load",
		InstanceArn:       replicationInstance.ResourceId().ReferenceAttr("replication_instance_arn"),
		SourceEndpointArn: source.ResourceId().ReferenceAttr("endpoint_arn"),
		TargetEndpointArn: loadDestination.ResourceId().ReferenceAttr("endpoint_arn"),
		Tags:              tags,
	}
	resources = append(resources, cdcTask, initialLoadTask)

	return resources, nil
}

// kinesisIAMProject creates a single project containing the IAM role and policy that allows DMS to write to Kinesis Data Streams.
// This project is created once per region, outside the per-database loop, to avoid role name conflicts.
func kinesisIAMProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	iamResources := createDMSKinesisIAMRole(config)

	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.AWSProviderGroup,
		Class:    "dms-kinesis-iam",
		ResourceGroups: map[string][]tf.Resource{
			"iam-roles": iamResources,
		},
		GenerateOutputs: true,
	}

	awsProviderVersion := resource.AWSProviderVersion5
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
		"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
	})

	return p, nil
}

// createDMSKinesisIAMRole creates the IAM role and policy resources that allow DMS to write to Kinesis Data Streams.
// This role is referenced by DMS endpoints via ServiceAccessRoleArn.
func createDMSKinesisIAMRole(config dataplatformconfig.DatabricksConfig) []tf.Resource {
	var resources []tf.Resource

	accountId := infraconsts.GetAccountIdForRegion(config.Region)

	// Create IAM role that DMS can assume
	kinesisRole := &awsresource.IAMRole{
		Name: "dms-replication-write-kinesis",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"Service": "dms.amazonaws.com",
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "dms-replication-write-kinesis",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, kinesisRole)

	// Create IAM policy that allows writing to Kinesis streams
	kinesisPolicy := &awsresource.IAMRolePolicy{
		Role: kinesisRole.ResourceId(),
		Name: "kinesis-write",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{
						"kinesis:DescribeStream",
						"kinesis:PutRecord",
						"kinesis:PutRecords",
					},
					Effect: "Allow",
					Resource: []string{
						// Allow DMS to write to all RDS Kinesis streams in this region
						fmt.Sprintf("arn:aws:kinesis:%s:%d:stream/%s-*", config.Region, accountId, KinesisStreamNamePrefix),
					},
				},
				{
					// Allow DMS to use KMS for Kinesis stream encryption
					Action: []string{
						"kms:Decrypt",
						"kms:GenerateDataKey",
						"kms:DescribeKey",
					},
					Effect:   "Allow",
					Resource: []string{"*"},
					Condition: &policy.AWSPolicyCondition{
						ForAllValuesStringLike: map[string][]string{
							"kms:ResourceAliases": {fmt.Sprintf("alias/%s-*", KinesisStreamKeyNamePrefix)},
						},
					},
				},
			},
		},
	}
	resources = append(resources, kinesisPolicy)

	return resources
}
