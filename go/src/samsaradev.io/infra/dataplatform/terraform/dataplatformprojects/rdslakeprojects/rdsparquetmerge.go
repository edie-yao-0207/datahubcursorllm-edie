package rdslakeprojects

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/databricksjobnameshelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

func ParquetMergeBucket(region string) string {
	regionPrefix := awsregionconsts.RegionPrefix[region]
	return fmt.Sprintf("%srds-delta-lake", regionPrefix)
}

const TablePrefix string = "table-parquet"
const CheckpointPrefix string = "s3listcheckpoints"

// The client version for the RDS merge job serverless compute by region.
// This controls what version of DBR is used by the merge job.
// Refer to the following documentation for more details: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
// Changing this value will change the version of DBR jobs that run on serverless.
// Its important to rollout changes to this value in a controlled manner.
var RdsMergeServerlessEnvironmentClientVersionByRegion = map[string]databricks.ClientVersion{
	infraconsts.SamsaraAWSDefaultRegion: databricks.ClientVersion3,
	infraconsts.SamsaraAWSCARegion:      databricks.ClientVersion4,
	infraconsts.SamsaraAWSEURegion:      databricks.ClientVersion3,
}

func isServerlessEnabled(region string, table rdsdeltalake.RegistryTable) bool {

	// If the table has DisableServerless set to true, then serverless is disabled for the table.
	if table.DisableServerless {
		return false
	}
	// Additionally, honor any regional override that disables serverless for this table.
	if regionOverrides, ok := table.InternalOverrides.RegionToJobOverrides[region]; ok {
		if regionOverrides.DisableServerless {
			return false
		}
	}

	// If the table is non-production, then serverless is enabled for the table unless explicitly disabled.
	if !table.Production {
		return true
	}

	// If the table has a serverless config, or the database has a serverless config, then serverless is enabled for the table.
	return table.ServerlessConfig != nil
}

func parquetMergeProjects(config dataplatformconfig.DatabricksConfig, remotes remoteResources, db rdsdeltalake.RegistryDatabase) ([]*project.Project, error) {
	var allProjects []*project.Project
	for _, shard := range db.RegionToShards[config.Region] {
		rdsParquetMergeResources, err := rdsParquetMergeSparkJobResources(config, db, shard)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		allProjects = append(allProjects, &project.Project{
			RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
			Provider: config.DatabricksProviderGroup,
			Class:    "rdsparquetmerge",
			Name:     db.Name,
			Group:    shard,
			ResourceGroups: project.MergeResourceGroups(
				rdsParquetMergeResources, map[string][]tf.Resource{
					"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
					"infra_remote":        remotes.infra,
					"pool_remote":         remotes.instancePool,
				},
			),
		})
	}
	return allProjects, nil
}

func protoColMapJsonString(region string, table rdsdeltalake.RegistryTable) (string, error) {
	protoColMap := make(map[string]map[string]string)
	for protoCol, protoDtl := range table.ProtoSchema {
		protoMsg := protoDtl.Message
		// Proto Descriptor File Key is the file path in samsara-dataplatform-deployed-artifacts bucket
		// Format: databricks-dev-(us|eu)/proto_descriptors/{package_path}/{proto_name}.proto.desc
		protoDescKey := fmt.Sprintf(
			"%s/proto_descriptors/%s.desc",
			awsregionconsts.RegionDatabricksPrefix[region],
			strings.TrimPrefix(protoDtl.ProtoPath, "go/src/samsaradev.io/"),
		)
		msgTypeFull := reflect.TypeOf(protoMsg).String() // Returns something like "*workflowsproto.IncidentProto"
		re := regexp.MustCompile(`^\*\w+\.(\w+)$`)
		// Match returned as something like: ["*workflowsproto.IncidentProto", "IncidentProto"]
		match := re.FindStringSubmatch(msgTypeFull)
		if len(match) < 2 {
			return "", oops.Errorf("error parsing the proto message name from the full path %s", protoMsg)
		}
		nestedProtoNames := strings.Join(protoDtl.NestedProtoNames, ",")
		enablePermissiveMode := strconv.FormatBool(protoDtl.EnablePermissiveMode)
		protoColMap[protoCol] = map[string]string{
			"descKey":              protoDescKey,
			"message":              match[1],
			"nestedProtoNames":     nestedProtoNames,
			"enablePermissiveMode": enablePermissiveMode,
		}
	}

	protoColMapJson, err := json.Marshal(protoColMap)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of proto column to proto info")
	}
	return strings.ReplaceAll(string(protoColMapJson), "\"", "\\\""), nil
}

func schemaFile(region string, dbname string, table rdsdeltalake.RegistryTable) string {
	return fmt.Sprintf("s3://%sdataplatform-deployed-artifacts/%s/rds_schemas/%s.%s.sql",
		awsregionconsts.RegionPrefix[region],
		awsregionconsts.RegionDatabricksPrefix[region],
		dbname,
		table.TableName,
	)
}

func DMSTargetS3PathName(shard, taskType, dbName, tableName, versionName string) string {
	return fmt.Sprintf("dms-parquet/%s/%s/%s/%s_%s/", shard, taskType, dbName, tableName, versionName)
}

func rdsServerlessEnvVarsJsonString(envs map[string]string) (string, error) {
	envMapJson, err := json.Marshal(envs)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of serverless env vars")
	}
	return strings.ReplaceAll(string(envMapJson), "\"", "\\\""), nil
}

func rdsParquetMergeSparkJobResources(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase, shard string) (map[string][]tf.Resource, error) {
	parquetMergeScript, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/rdsdeltalake/parquet_merge.py")
	if err != nil {
		return nil, oops.Wrapf(err, "make s3 object for parquet_merge.py")
	}
	resourceGroups := map[string][]tf.Resource{
		"merge_script": {parquetMergeScript},
	}

	for _, table := range db.TablesInRegion(config.Region) {

		dbname := rdsdeltalake.GetSparkFriendlyRdsDBName(shard, db.Sharded, true)
		regionPrefix := awsregionconsts.RegionPrefix[config.Region]

		sparkConf := map[string]string{
			"spark.databricks.delta.schema.autoMerge.enabled": "true",
		}

		var tableResources []tf.Resource
		fixedCdcVersion := table.CdcVersion_DO_NOT_SET
		for _, version := range table.VersionInfo.VersionsParquet() {
			parameters := []string{
				"--path", fmt.Sprintf("s3://%s/%s/%s/", ParquetMergeBucket(config.Region), TablePrefix, db.TableS3PathNameWithVersion(shard, table.TableName, version)),
				"--description", table.BuildDescription(),
				"--dbname", dbname,
				"--tablename", table.TableName,
				"--primarykeys", strings.Join(table.PrimaryKeys, ","),
				"--partitionkey", table.PartitionStrategy.Key,
				"--partitionexpr", table.PartitionStrategy.Expr,
				"--version", version.Name(),
				"--dms-source-s3-bucket", fmt.Sprintf("%srds-export", regionPrefix),
				"--dms-load-source-s3-prefix", DMSTargetS3PathName(shard, "load", db.MySqlDb, table.TableName, version.Name()),
				"--dms-cdc-source-s3-prefix", DMSTargetS3PathName(shard, "cdc", db.MySqlDb, table.TableName, fixedCdcVersion.Name()),
				"--checkpoint-s3-bucket", ParquetMergeBucket(config.Region),
				"--checkpoint-s3-prefix", fmt.Sprintf("%s/%s/", CheckpointPrefix, db.TableS3PathNameWithVersion(shard, table.TableName, version)),
				"--table-schema-url", schemaFile(config.Region, db.Name, table),
			}

			if table.EmptyStringsCastToNull_DO_NOT_SET {
				parameters = append(parameters, "--empty-strings-cast-to-null")
			}

			// Update main view if current table version is current SparkVersion
			if version == table.VersionInfo.SparkVersionParquet() {
				parameters = append(parameters, "--mainversion")
			}

			if len(table.ProtoSchema) > 0 {
				protoColMapJson, err := protoColMapJsonString(config.Region, table)
				if err != nil {
					return nil, oops.Wrapf(err, "error getting proto column to proto message json map")
				}
				protoDescriptorFileBucket := fmt.Sprintf("%sdataplatform-deployed-artifacts", regionPrefix)
				protoParams := []string{
					"--proto-desc-bucket", protoDescriptorFileBucket,
					"--proto-col-info-map", protoColMapJson,
				}
				parameters = append(parameters, protoParams...)
			}

			rndCostAllocation := float64(1)
			if table.Production {
				rndCostAllocation = 0
				parameters = append(parameters, "--production")
			}
			if db.HasProductionTable() {
				parameters = append(parameters, "--dbproduction")
			}

			jobName := databricksjobnameshelpers.GetRDSParquetMergeJobName(databricksjobnameshelpers.GetRDSMergeJobNameInput{
				Shard:        shard,
				MySqlDb:      db.MySqlDb,
				TableName:    table.TableName,
				TableVersion: int(version),
			})

			// Choose the correct pool based on region, AZ (derived from job name), and production/nonproduction
			az := dataplatformresource.JobNameToAZ(config.Region, jobName)
			idents := getPoolIdentifiers(config.Region, table.Production)[az]
			worker := tf.LocalId(idents.WorkerPoolIdentifier)
			driver := tf.LocalId(idents.DriverPoolIdentifier)

			// Set the max worker count to 4 in the US, and 2 for all other regions.
			maxWorkers := 4
			if config.Region != infraconsts.SamsaraAWSDefaultRegion {
				maxWorkers = 2
			}
			// Set to 2 for nonproduction jobs regardless of region.
			if !table.Production {
				maxWorkers = 2
			}
			if regionOverrides, ok := table.InternalOverrides.RegionToJobOverrides[config.Region]; ok {
				// Override the max workers for all shards in the region.
				if regionOverrides.MaxWorkers != nil {
					maxWorkers = *regionOverrides.MaxWorkers
				}
				for _, config := range regionOverrides.CustomSparkConfigurations {
					for k, v := range config {
						sparkConf[k] = v
					}
				}
				// Override the max workers for a single shard. Match based on the shard string, which has the format "prod-[dbname]-shard-[i]db".
				if len(regionOverrides.ShardOverrides) > 0 {
					for _, shardOverride := range regionOverrides.ShardOverrides {
						if shardOverride.ShardNumber != nil && shardOverride.MaxWorkers != nil && regexp.MustCompile(fmt.Sprintf(".*-shard-%d(db)?", *shardOverride.ShardNumber)).MatchString(shard) {
							maxWorkers = *shardOverride.MaxWorkers
							break
						}
					}
				}
			}

			// For production jobs, spread their start times out in the first 10 min of the hour every 3 hours.
			// Nonproduction jobs run every 6 hours except when production jobs are running.
			// Note that we make sure that all shards of a DB start at the same time, to reduce the period in which
			// different shards have different amounts of data.
			nameForHash := db.Name + "." + table.TableName

			// If its nonproduction, choose a minute between 10 and 180, and then also choose whether its in the first
			// 3 hour period or the second of the 6 hour interval. This logic is weird but we need it to avoid conflicting
			// with production jobs over the entire 6 hour window.
			//                   period 1                          period 2
			// [ 0 - 10 P][     10 - 180        NP][ 0 - 10 P][     10 - 180        NP]
			minuteInWindow := dataplatformresource.RandomFromNameHash(nameForHash, 10, 180)
			zeroOrOne := dataplatformresource.RandomFromNameHash(nameForHash, 0, 2)
			hour := minuteInWindow / 60
			if zeroOrOne == 1 {
				hour += 3
			}

			cronHour := fmt.Sprintf("%d/%s", hour, table.GetReplicationFrequencyHour())
			if table.DataModelTable {
				cronHour = fmt.Sprintf("%d/%s", hour, table.GetReplicationFrequencyHour())
			}

			cronMinute := minuteInWindow % 60
			if table.InternalOverrides.CronHourSchedule != "" {
				cronHour = table.InternalOverrides.CronHourSchedule
			} else if table.Production {
				cronHour = fmt.Sprintf("*/%s", table.GetReplicationFrequencyHour())
				cronMinute = dataplatformresource.RandomFromNameHash(nameForHash, 0, 10)
			}

			cronSchedule := fmt.Sprintf(`0 %d %s * * ?`, cronMinute, cronHour)

			sloConfig := table.SloConfig()

			ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "no ci service principal app id for region %s", config.Region)
			}

			ucSettings := dataplatformresource.UnityCatalogSetting{
				DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
				SingleUserName:   ciServicePrincipalAppId,
			}
			instanceProfileName := "uc_instance_profile"

			// Add UC specific spark configs.
			sparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
			sparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
			sparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
			sparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

			// Configure all rds deltalake merge jobs to use DBR 15.4 LTS Spark with Scala 2.12.
			sparkVersion := sparkversion.RdsDbrVersion

			tags := map[string]string{
				dataplatformconsts.DATABASE_TAG: shard,
				dataplatformconsts.TABLE_TAG:    table.TableName,
			}

			if isServerlessEnabled(config.Region, table) {
				serverlessEnvVars, err := rdsServerlessEnvVarsJsonString(map[string]string{
					"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "rds-replication",
					"AWS_REGION":         config.Region,
					"AWS_DEFAULT_REGION": config.Region,
				})
				if err != nil {
					return nil, oops.Wrapf(err, "error getting serverless env vars json string")
				}
				parameters = append(parameters, "--use-serverless-compute", "true", "--serverless-env-vars", serverlessEnvVars)
			} else {
				// If the table is not serverless, we need to tag the pool names
				tags["pool-name"] = worker.ReferenceKey("name")
				tags["driver-pool-name"] = driver.ReferenceKey("name")
			}

			merge := dataplatformresource.JobSpec{
				Name:   jobName,
				Region: config.Region,
				// Data Platform owns the code for RDS merge, so mark them as owner despite underlying tables being owned by other teams.
				Owner:                        team.DataPlatform,
				SparkVersion:                 sparkVersion,
				Script:                       parquetMergeScript,
				Parameters:                   parameters,
				MinWorkers:                   1,
				MaxWorkers:                   maxWorkers,
				Profile:                      tf.LocalId(instanceProfileName).Reference(),
				Cron:                         cronSchedule,
				TimeoutSeconds:               43200, // 12hrs
				MinRetryInterval:             10 * time.Minute,
				EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
				JobOwnerServicePrincipalName: ciServicePrincipalAppId,
				Pool:                         worker.ReferenceKey("id"),
				DriverPool:                   driver.ReferenceKey("id"),
				Tags:                         tags,
				SparkConf:                    sparkConf,
				SparkEnvVars: map[string]string{
					"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "rds-replication",
				},
				RnDCostAllocation: rndCostAllocation,
				IsProduction:      table.Production,
				JobType:           dataplatformconsts.RdsDeltaLakeIngestionMerge,
				Libraries: dataplatformresource.JobLibraryConfig{
					PyPIs: []dataplatformresource.PyPIName{
						dataplatformresource.SparkPyPIDatadog,
					},
					Jars: []dataplatformresource.JarName{
						dataplatformresource.SamsaraSparkProtobufJar,
					},
				},
				Format: databricks.MultiTaskKey,
				JobTags: map[string]string{
					"format": databricks.MultiTaskKey,
				},
				SloConfig:           &sloConfig,
				UnityCatalogSetting: ucSettings,
				RunAs: &databricks.RunAsSetting{
					ServicePrincipalName: ciServicePrincipalAppId,
				},
			}

			// If the table is non-production and has no serverless config, then we
			// need to set the serverless config to the default. TODO: This is for
			// rollout purposes and once the rollout is complete, we can remove this.
			// Avoids pain of having to manually set the serverless config on each
			// non-prod table if not set
			if !table.Production && table.ServerlessConfig == nil {
				table.ServerlessConfig = &databricks.ServerlessConfig{
					EnvironmentKey:    databricks.DefaultServerlessEnvironmentKey,
					PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
					BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
				}
			}

			// For production tables that have serverless enabled but no ServerlessConfig set,
			// ensure ServerlessConfig is set to avoid nil pointer dereference when accessing ServerlessConfig fields.
			if table.Production && table.ServerlessConfig == nil && isServerlessEnabled(config.Region, table) {
				table.ServerlessConfig = &databricks.ServerlessConfig{
					EnvironmentKey:    databricks.DefaultServerlessEnvironmentKey,
					PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
					BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
				}
			}

			if isServerlessEnabled(config.Region, table) {
				sc := &databricks.ServerlessConfig{
					EnvironmentKey:    table.ServerlessConfig.EnvironmentKey,
					PerformanceTarget: table.ServerlessConfig.PerformanceTarget,
					Environments:      []databricks.ServerlessEnvironment{},
					BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
				}

				// For RDS merge jobs the client version is determined by table level > database level > region default.
				clientVersion, ok := RdsMergeServerlessEnvironmentClientVersionByRegion[config.Region]
				if !ok {
					return nil, oops.Errorf("client version not found for region %s", config.Region)
				}

				// Override the default client version for the serverless compute by region.
				// If not set, use the default client version for the region.
				// This is set for the database itself.
				if db.InternalOverrides.ServerlessConfig != nil && db.InternalOverrides.ServerlessConfig.ClientVersionByRegionOverride != nil {
					overrideClientVersion, ok := db.InternalOverrides.ServerlessConfig.ClientVersionByRegionOverride[config.Region]
					if ok && overrideClientVersion != "" {
						clientVersion = overrideClientVersion
					}
				}

				// Override the default client version & database level client version override if set at table level.
				// If not set, use the default client version for the region.
				// This is set for the table itself.
				if table.ServerlessConfig != nil && table.ServerlessConfig.ClientVersionByRegionOverride != nil {
					overrideClientVersion, ok := table.ServerlessConfig.ClientVersionByRegionOverride[config.Region]
					if ok && overrideClientVersion != "" {
						clientVersion = overrideClientVersion
					}
				}
				// Add the default environment to the serverless config.
				sc.Environments = append(sc.Environments, databricks.ServerlessEnvironment{
					EnvironmentKey: databricks.DefaultServerlessEnvironmentKey,
					Spec: &databricks.ServerlessEnvironmentSpec{
						Client:       string(clientVersion),
						Dependencies: []string{dataplatformresource.SparkPyPIDatadog.PinnedString()},
					},
				})

				merge.ServerlessConfig = sc

				// Add the tags to job tags in serverless mode.
				for k, v := range tags {
					merge.JobTags[k] = v
				}
			}

			mergeJobResource, err := merge.TfResource()
			if err != nil {
				return nil, oops.Wrapf(err, "building job resource")
			}
			permissionsResource, err := merge.PermissionsResource()
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			tableResources = append(tableResources, mergeJobResource, permissionsResource)
		}
		resourceGroups[table.TableName] = tableResources
	}
	return resourceGroups, nil
}
