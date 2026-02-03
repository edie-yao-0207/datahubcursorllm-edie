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
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

type rdsReplicationSpecTable struct {
	Name string
	// Version is the load version that gets increased every time a table needs to be
	// reloaded via a table version bump.
	Version rdsdeltalake.TableVersion
	// CdcVersion is the CDC version that should never change because the same CDC task
	// is used when tables are reloaded via table version bumps. This is set to DO_NOT_SET
	// in the registry because there are legacy tables that has set this prior to the change
	// in reloading mechanism. For new tables, the version is automatically set to 0 and this
	// field should not be set.
	CdcVersion rdsdeltalake.TableVersion
	// List of column names to be excluded from replication to the data lake.
	ExcludeColumns []string
	// PartitionKeyAttribute specifies the column to use as the Kinesis partition key.
	// When set, DMS uses this column's value as the partition key instead of the
	// default schema.table name.
	//
	// This could be extended to support a slice of columns for composite partition
	// keys (e.g., org_id + driver_id) by adding a transformation rule that creates
	// a synthetic column combining the values, then referencing that synthetic column.
	// See: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html#CHAP_Target.Kinesis.ObjectMapping
	PartitionKeyAttribute string
}

type RdsEngineType string

const (
	RdsEngineAuroraMySQL    RdsEngineType = "aurora" // we cannot use infraconsts.AuroraDatabaseEngine here because it is not a valid engine name for DMS
	RdsEngineAuroraPostgres RdsEngineType = "aurora-postgresql"
)

type RdsEngineSettings struct {
	DatabasePort int
}

var rdsEngineSettings = map[RdsEngineType]RdsEngineSettings{
	RdsEngineAuroraMySQL: {
		DatabasePort: 3306,
	},
	RdsEngineAuroraPostgres: {
		DatabasePort: 5432,
	},
}

type rdsReplicationSpec struct {
	MySqlDbName      string
	DbShardName      string
	WriterHostname   string
	Region           string
	MySQLSchema      string
	Tables           []rdsReplicationSpecTable
	DmsEngineVersion awsresource.DmsEngineVersion
	HasProduction    bool
	RdsEngineType    RdsEngineType
}

func ParquetCdcTaskName(dbshard string) string {
	return "parquet-task-cdc-" + dbshard
}

func ParquetLoadTaskName(dbshard string) string {
	return "parquet-task-load-" + dbshard
}

func DmsOutputPrefix(dbname string) string {
	return fmt.Sprintf("dms-parquet/%s", dbname)
}

func RdsExportBucket(region string) string {
	return awsregionconsts.RegionPrefix[region] + "rds-export"
}

func replicationProjects(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) ([]*project.Project, error) {
	var allprojects []*project.Project
	for _, shard := range db.RegionToShards[config.Region] {
		// Create the DMS replication resources for the project
		var tables []rdsReplicationSpecTable
		hasProduction := db.HasProductionTable()
		for _, table := range db.TablesInRegion(config.Region) {
			tables = append(tables, rdsReplicationSpecTable{
				Name:           table.TableName,
				Version:        table.VersionInfo.DMSOutputVersionParquet(),
				CdcVersion:     table.CdcVersion_DO_NOT_SET,
				ExcludeColumns: table.ExcludeColumns,
			})
		}
		writerHostnamePrefix := shard
		if db.Name == "clouddb" && config.Region == infraconsts.SamsaraAWSDefaultRegion {
			writerHostnamePrefix += "-cluster"
		} else if db.Name == "statsdb" && config.Region == infraconsts.SamsaraAWSEURegion {
			// The statsdb instance in the EU is at a very particular and different location.
			// We don't know the history behind this, but we override the values here to make sure that
			// the DMS setup works correctly.
			writerHostnamePrefix = "stats-aurora-replica-2-cluster"
		} else if config.Region == infraconsts.SamsaraAWSCARegion { // make regionconditionalchecker happy
			// do nothing
		}

		// For the aurora 2 upgrade, we had to cutover some dbs to a new "blue" cluster.
		// This is meant to be temporary. If SRE changes this, they will let us know.
		// Long term solution is we can use the cname, or verify in a test that our server names match
		// what the cnames point to
		shardWithoutProdPrefix := strings.ReplaceAll(shard, "prod-", "")
		if shouldCutover, err := dbregistry.ShouldCutoverToBlue(shardWithoutProdPrefix, config.Region); err != nil {
			return nil, oops.Wrapf(err, "error checking whether shard %s in region %s should be cut over to the blue cluster", shardWithoutProdPrefix, config.Region)
		} else if shouldCutover {
			writerHostnamePrefix += "-blue"
		}

		dmsEngineVersion := awsresource.DmsEngineVersion(awsresource.DmsEngine3_5_4)
		if db.InternalOverrides.DmsEngineVersion != "" {
			dmsEngineVersion = db.InternalOverrides.DmsEngineVersion
		}

		rdsEngineType := RdsEngineAuroraMySQL
		if db.InternalOverrides.AuroraDatabaseEngine == infraconsts.AuroraDatabaseEnginePostgres17 {
			rdsEngineType = RdsEngineAuroraPostgres
		}

		dms, err := dmsReplicationResources(rdsReplicationSpec{
			MySqlDbName:      db.MySqlDb,
			DbShardName:      shard,
			Region:           config.Region,
			MySQLSchema:      db.MySqlDb,
			WriterHostname:   fmt.Sprintf("%s.%s.%s.rds.amazonaws.com", writerHostnamePrefix, config.RDSClusterIdentifier, config.Region),
			Tables:           tables,
			DmsEngineVersion: dmsEngineVersion,
			HasProduction:    hasProduction,
			RdsEngineType:    rdsEngineType,
		})
		if err != nil {
			return nil, err
		}

		p := &project.Project{
			RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
			Provider: config.AWSProviderGroup,
			Class:    "rdsreplication",
			Name:     db.Name,
			Group:    shard,
			ResourceGroups: map[string][]tf.Resource{
				"dms": dms,
			},
			GenerateOutputs: true,
		}

		awsProviderVersion := resource.AWSProviderVersion5
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"remote_state": resource.MonolithRemoteState(config.AWSProviderGroup, p.Cloud()),
			"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
			"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
		})

		allprojects = append(allprojects, p)
	}

	return allprojects, nil
}

func getDbName(mySqlDbName string, dbShardName string) string {
	dbName := mySqlDbName
	if dbName == "prod_db" {
		if strings.Contains(dbShardName, "cloud") {
			dbName = "clouddb"
		} else if strings.Contains(dbShardName, "products") {
			dbName = "productsdb"
		}
	}
	if dbName == "alerts_db" {
		dbName = "alertsdb"
	}
	if dbName == "stats_db" {
		dbName = "statsdb"
	}
	return dbName
}

func dmsReplicationResources(spec rdsReplicationSpec) ([]tf.Resource, error) {
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

	serviceTag := fmt.Sprintf("dms-%s", spec.MySqlDbName)
	// Since productsdb has the same mysql db name as clouddb, use productsdb here instead of prod_db to prevent name collision.
	if dbName == "productsdb" {
		serviceTag = fmt.Sprintf("dms-%s", dbName)
	}
	tags := map[string]string{
		"samsara:service":        serviceTag,
		"samsara:team":           strings.ToLower(teamOwner.Name()),
		"samsara:product-group":  strings.ToLower(team.TeamProductGroup[teamOwner.Name()]),
		"samsara:rnd-allocation": rndAllocation,
		"samsara:production":     fmt.Sprintf("%v", spec.HasProduction),
		"owner":                  strings.ToLower(team.DataPlatform.TeamName),
	}

	sourceId := fmt.Sprintf("rds-%s", spec.DbShardName)
	source := &awsresource.DMSEndpoint{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"password"},
				},
			},
		},
		EndpointId:   sourceId,
		EndpointType: "source",
		EngineName:   string(spec.RdsEngineType),
		Username:     "prod_admin",
		ServerName:   spec.WriterHostname,
		Password:     "invalidpassword",
		Port:         rdsEngineSettings[spec.RdsEngineType].DatabasePort,
		Tags:         tags,
	}
	// PostgreSQL requires the database_name parameter, but MySQL does not (and should not have it)
	// See: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html
	if spec.RdsEngineType == RdsEngineAuroraPostgres {
		source.DatabaseName = spec.MySqlDbName
		source.ExtraConnectionAttributes = config.PostgresExtraConnAttributes()
		source.SslMode = "require"
	}
	resources = append(resources, source)
	accountId := infraconsts.GetAccountIdForRegion(spec.Region)
	rdsExportBucket := RdsExportBucket(spec.Region)

	// If the engine version is empty, it means that this DMS task was made before
	// we began explicitly writing the engine version so it uses whatever default
	// was there when it was made.
	var engineVersion *string
	if string(spec.DmsEngineVersion) != "" {
		engineVersion = pointer.StringPtr(string(spec.DmsEngineVersion))
	}

	settingsJson, err := json.MarshalIndent(config.DesiredDMSReplicationTaskSettings, "", " ")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	size := "dms.t3.medium"
	// compliance and cmassets sometimes run into high source/target latency, and looking at datadog metrics
	// it looks like the parquet tasks are more resource intensive than their counterpart csv ones,
	// so we will try to increase the instance size to see if that helps.
	// This roughly doubles the cost of these instances, so we will monitor how it helps and if we can scale
	// down eventually.
	if strings.Contains(spec.DbShardName, "compliance") || strings.Contains(spec.DbShardName, "cmassets") {
		size = "dms.c6i.large"
	}
	replicationInstanceName := "parquet-" + spec.DbShardName

	replicationInstance := &awsresource.DMSReplicationInstance{
		ResourceName:  replicationInstanceName,
		InstanceId:    util.HashTruncate(replicationInstanceName, 63, 8, "-"),
		InstanceClass: size,
		// Disable auto minor version upgrade since we want to control that process.
		AutoMinorVersionUpgrade: aws.Bool(false),
		// This flag is required to be set explicitly to allow major verison upgrades via terraform.
		AllowMajorVersionUpgrade: aws.Bool(true),
		ApplyImmediately:         aws.Bool(true),
		// We'll run parquet instances in single AZ mode.
		MultiAZ:       aws.Bool(false),
		SubnetGroupId: "rds-replication",
		VpcSecurityGroupIds: []string{
			tf.LocalId("security_groups").ReferenceKey("prod_app"),
			tf.LocalId("security_groups").ReferenceKey("prod_devices"),
			tf.LocalId("security_groups").ReferenceKey("prod_rds"),
		},
		Tags:          tags,
		EngineVersion: engineVersion,
		// We've been seeing issues with DMS replication instances not being able to be created within the default
		// timeout of 30 minutes. This raises that timeout value to 1h; the last one we added took ~45min.
		Timeouts: &awsresource.Timeouts{
			Create: "1h",
		},
	}
	resources = append(resources, replicationInstance)

	// Setup cdc and load endpoints.
	// See https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring for more
	// information about the available values.
	cdcDestination := &awsresource.DMSEndpoint{
		EndpointId:   "s3-parquet-cdc-" + spec.DbShardName,
		EndpointType: "target",
		EngineName:   "s3",
		S3Settings:   config.GenerateS3SettingsForDMSEndpoint(accountId, rdsExportBucket, DmsOutputPrefix(spec.DbShardName), spec.DbShardName, "cdc"),
		Tags:         tags,
	}

	loadDestination := &awsresource.DMSEndpoint{
		EndpointId:   "s3-parquet-load-" + spec.DbShardName,
		EndpointType: "target",
		EngineName:   "s3",
		S3Settings:   config.GenerateS3SettingsForDMSEndpoint(accountId, rdsExportBucket, DmsOutputPrefix(spec.DbShardName), spec.DbShardName, "load"),
		Tags:         tags,
	}
	resources = append(resources, cdcDestination, loadDestination)

	// We need to construct the table mappings, because *normally* via terraform, we will only
	// set up the initial "exclude everything" rule, and the initialload step function
	// will later update the task. For parquet, we just want it to have the right
	// list from the beginning.

	dmsTableMapping := awsresource.DMSTableMapping{
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
				// Add a _rowid column which is a 35 digit unique incrementing number from the
				// source database that consists of a timestamp and an auto-incrementing number.
				// Row order is not preserved when reading data via spark.read.parquet, so we
				// need to add a row number in advance to determine the last change to a given row.
				Expression: aws.String("$AR_H_CHANGE_SEQ"),
				DataType: &awsresource.DMSTableMappingRuleDataType{
					Type:   aws.String("string"),
					Length: aws.Int(50),
				},
			},

			// DMS Requires tasks to be instantiated with at least 1 selection rule.
			// Instead of trying to create some arbitrary rule per table, we create a
			// selection rule that excludes everything. When orchestrating the task
			// via dagster, we'll remove this rule.
			{
				RuleType: aws.String("selection"),
				RuleId:   aws.Int(1),
				RuleName: aws.String("exclude-all"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String(`%`),
					TableName:  aws.String(`%`),
				},
				RuleAction: aws.String("exclude"),
			},
		},
	}

	if spec.RdsEngineType == RdsEngineAuroraPostgres {
		// iterate over all tables and get all schema names
		schemaNames := make(map[string]bool)
		for _, table := range spec.Tables {
			split := strings.Split(table.Name, "__")
			if len(split) != 2 {
				return nil, oops.Errorf("table name %s is not in the correct format", table.Name)
			}
			schemaNames[split[0]] = true
		}
		// rename all schemas to the database name
		for schemaName := range schemaNames {
			ruleId := *dmsTableMapping.Rules[len(dmsTableMapping.Rules)-1].RuleId + 1
			dmsTableMapping.Rules = append(dmsTableMapping.Rules, &awsresource.DMSTableMappingRule{
				RuleId:   aws.Int(ruleId),
				RuleName: aws.String("rename-postgres-schema"),
				RuleType: aws.String("transformation"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String(schemaName),
				},
				RuleAction: aws.String("rename"),
				RuleTarget: aws.String("schema"),
				Value:      aws.String(spec.MySqlDbName),
			})
		}
	}

	for _, table := range spec.Tables {
		for _, columnName := range table.ExcludeColumns {
			// Generate RuleId by incrementing the last rule's ID.
			ruleId := *dmsTableMapping.Rules[len(dmsTableMapping.Rules)-1].RuleId + 1
			schemaName := "%"
			tableName := table.Name
			// For Postgres we need to explicitly include the schema name
			if spec.RdsEngineType == RdsEngineAuroraPostgres {
				split := strings.Split(table.Name, "__")
				if len(split) != 2 {
					return nil, oops.Errorf("table name %s is not in the correct format", table.Name)
				}
				schemaName = split[0]
				tableName = split[1]
			}
			dmsTableMapping.Rules = append(dmsTableMapping.Rules, &awsresource.DMSTableMappingRule{
				RuleId:   aws.Int(ruleId),
				RuleName: aws.String(fmt.Sprintf("remove-column-%d", ruleId)),
				RuleType: aws.String("transformation"),
				ObjectLocator: &awsresource.DMSTableMappingRuleObjectLocator{
					SchemaName: aws.String(schemaName),
					TableName:  aws.String(tableName),
					ColumnName: aws.String(columnName),
				},
				RuleAction: aws.String("remove-column"),
				RuleTarget: aws.String("column"),
			})
		}
	}

	dmsTableMappingJson, err := json.Marshal(dmsTableMapping)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to jsonify mappings for parquet task")
	}

	cdcTask := &awsresource.DMSReplicationTask{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					// Ignore replication_task_settings because we set partial settings.
					// Ignore cdc_start_position because it gets  set when the task first runs, and
					// is set to that timestamp. After that, we don't want to change it anymore,
					// however terraform will try to set it to null since we don't initialize it here.
					// Ignore table_mappings because we manage them via dagster.
					IgnoreChanges: []string{"replication_task_settings", "cdc_start_position", "table_mappings"},
				},
			},
		},
		TaskId:            ParquetCdcTaskName(spec.DbShardName),
		TaskSettings:      strings.ReplaceAll(string(settingsJson), `"`, `\"`),
		TableMappings:     strings.ReplaceAll(string(dmsTableMappingJson), `"`, `\"`),
		MigrationType:     "cdc",
		InstanceArn:       replicationInstance.ResourceId().ReferenceAttr("replication_instance_arn"),
		SourceEndpointArn: source.ResourceId().ReferenceAttr("endpoint_arn"),
		TargetEndpointArn: cdcDestination.ResourceId().ReferenceAttr("endpoint_arn"),
		Tags:              tags,
	}

	initialLoadTask := &awsresource.DMSReplicationTask{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					// Ignore replication_task_settings because we set partial settings.
					// Ignore table_mappings because we manage them via dagster.
					IgnoreChanges: []string{"replication_task_settings", "table_mappings"},
				},
			},
		},
		TaskId:            ParquetLoadTaskName(spec.DbShardName),
		TaskSettings:      strings.ReplaceAll(string(settingsJson), `"`, `\"`),
		TableMappings:     strings.ReplaceAll(string(dmsTableMappingJson), `"`, `\"`),
		MigrationType:     "full-load",
		InstanceArn:       replicationInstance.ResourceId().ReferenceAttr("replication_instance_arn"),
		SourceEndpointArn: source.ResourceId().ReferenceAttr("endpoint_arn"),
		TargetEndpointArn: loadDestination.ResourceId().ReferenceAttr("endpoint_arn"),
		Tags:              tags,
	}
	resources = append(resources, cdcTask, initialLoadTask)

	return resources, nil
}
