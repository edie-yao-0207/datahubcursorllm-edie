package dynamodbdeltalake

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/databricksjobnameshelpers"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const DynamoReplicationCredential string = "dynamodb-replication"

// The client version for the DynamoDB merge job serverless compute by region.
// This controls what version of DBR is used by the merge job.
// Refer to the following documentation for more details: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
// Changing this value will change the version of DBR jobs that run on serverless.
// Its important to rollout changes to this value in a controlled manner.
var DynamodbMergeServerlessEnvironmentClientVersionByRegion = map[string]databricks.ClientVersion{
	infraconsts.SamsaraAWSDefaultRegion: databricks.ClientVersion4,
	infraconsts.SamsaraAWSCARegion:      databricks.ClientVersion4,
	infraconsts.SamsaraAWSEURegion:      databricks.ClientVersion4,
}

type remoteResources struct {
	infra        []tf.Resource
	instancePool []tf.Resource
}

func dynamoDbMergeBucket(region string) string {
	return fmt.Sprintf("%sdynamodb-delta-lake", region)
}

func dynamoDbExportBucket(region string) string {
	return fmt.Sprintf("%sdynamodb-export", region)
}

func dynamoDbCheckpointBucket(region string) string {
	return dynamoDbMergeBucket(region)
}

func dynamoDbLoadSourceBucket(region string) string {
	return dynamoDbExportBucket(region)
}

func dynamoDbStreamSourceBucket(region string) string {
	return dynamoDbExportBucket(region)
}

func dynamoDbTableDeltaTablePath(region, tableName string) string {
	return fmt.Sprintf("s3://%s/table/%s/", dynamoDbMergeBucket(region), tableName)
}

func dynamoDbTableCheckpointPrefix(tableName string) string {
	return fmt.Sprintf("checkpoint/%s/", tableName)
}

func dynamoDbTableLoadSourcePrefix(tableName string) string {
	return fmt.Sprintf("load/%s/AWSDynamoDB/", tableName)
}

func dynamoDbTableStreamSourcePrefix(tableName string) string {
	return fmt.Sprintf("kinesis_streams/data/%s/", tableName)
}

func dynamoDbServerlessEnvVarsJsonString(envs map[string]string) (string, error) {
	envMapJson, err := json.Marshal(envs)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of serverless env vars")
	}
	return strings.ReplaceAll(string(envMapJson), "\"", "\\\""), nil
}

// dynamoDbMergeSparkJobProject creates a tf project for a test table in the US
// region for testing the new dynamodb merge job.
func dynamoDbMergeSparkJobProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources) (*project.Project, error) {

	mergeScript, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/dynamodbdeltalake/merge.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading merge script")
	}

	resourceGroups := map[string][]tf.Resource{
		"merge_script": {mergeScript},
	}

	upsertOnlyScript, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/dynamodbdeltalake/auditlog/upsert.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading upsert script")
	}
	resourceGroups["upsert_only_script"] = []tf.Resource{upsertOnlyScript}

	for _, table := range dynamodbdeltalake.AllTables() {

		if contains(table.InternalOverrides.UnavailableRegions, config.Region) {
			// if the dynamo table is not available in this region, skip it
			continue
		}

		primaryKeys := []string{table.PartitionKey}
		if table.SortKey != "" {
			primaryKeys = append(primaryKeys, table.SortKey)
		}

		parameters := []string{
			"--delta-table-location", dynamoDbTableDeltaTablePath(awsregionconsts.RegionPrefix[config.Region], table.TableName),
			"--partition-key", table.PartitionStrategy.Key,
			"--partition-expr", table.PartitionStrategy.Expr,
			"--primary-keys", strings.Join(primaryKeys, ","),
			"--tablename", table.TableName,
			"--description", table.BuildDescription(),
			"--checkpoint-s3-bucket", dynamoDbCheckpointBucket(awsregionconsts.RegionPrefix[config.Region]),
			"--checkpoint-s3-prefix", dynamoDbTableCheckpointPrefix(table.TableName),
			"--load-source-s3-bucket", dynamoDbLoadSourceBucket(awsregionconsts.RegionPrefix[config.Region]),
			"--load-source-s3-prefix", dynamoDbTableLoadSourcePrefix(table.TableName),
			"--stream-source-s3-bucket", dynamoDbStreamSourceBucket(awsregionconsts.RegionPrefix[config.Region]),
			"--stream-source-s3-prefix", dynamoDbTableStreamSourcePrefix(table.TableName),
		}

		if table.ServerlessConfig != nil {
			// Set the environment variables for the merge job in the case of serverless compute.
			serverlessEnvVars, err := dynamoDbServerlessEnvVarsJsonString(map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": DynamoReplicationCredential,
				"AWS_REGION":         config.Region,
				"AWS_DEFAULT_REGION": config.Region,
			})
			if err != nil {
				return nil, oops.Wrapf(err, "error marshalling map of serverless env vars")
			}
			parameters = append(parameters, "--use-serverless-compute", "true", "--serverless-env-vars", serverlessEnvVars)
		}

		jobName := databricksjobnameshelpers.GetDynamoDBMergeJobName(databricksjobnameshelpers.GetDynamoDBMergeJobNameInput{
			TableName: table.TableName,
			Region:    config.Region,
		})

		// Choose the correct pool based on region, AZ (derived from job name), and production/nonproduction
		az := dataplatformresource.JobNameToAZ(config.Region, jobName)
		idents := getPoolIdentifiers(config.Region, table.Production)[az]
		worker := tf.LocalId(idents.WorkerPoolIdentifier)
		driver := tf.LocalId(idents.DriverPoolIdentifier)

		// We don't need dynamodb jobs to run frequently for now. They should run
		// twice a day, spread out over the entire day.
		cronHour := fmt.Sprintf("*/%s", table.GetReplicationFrequencyHour())
		cronMinute := dataplatformresource.RandomFromNameHash(jobName, 0, 60)
		cronSchedule := fmt.Sprintf("0 %d %s * * ?", cronMinute, cronHour)

		rndCostAllocation := float64(1)
		if table.Production {
			rndCostAllocation = 0
		}

		sloConfig := table.SloConfig()

		// DynamoDB tables are technically schemaless. Other than the primary key
		// (which is a composite of the partition key and optional sort key), all
		// other attributes can be added or removed post table creation. To handle
		// schema evolution, the Spark session configuration
		// `spark.databricks.delta.schema.autoMerge.enabled` must be set to true
		// before the merge command.
		sparkConf := map[string]string{
			"spark.databricks.delta.schema.autoMerge.enabled":      "true",
			"spark.databricks.sql.initial.catalog.name":            "default",
			"databricks.loki.fileStatusCache.enabled":              "false",
			"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
			"spark.databricks.scan.modTimeCheck.enabled":           "false",
		}

		// Add custom spark configurations if they are specified.
		if table.CustomSparkConfigurations != nil {
			for k, v := range table.CustomSparkConfigurations {
				sparkConf[k] = v
			}
		}

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
		}
		unityCatalogSetting := dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   ciServicePrincipalAppId,
		}
		instanceProfile := tf.LocalId("uc_instance_profile").Reference()

		maxWorkers := 3
		if workers, ok := table.InternalOverrides.MaxWorkersByRegion[config.Region]; ok {
			maxWorkers = workers
		}

		tags := map[string]string{
			dataplatformconsts.DATABASE_TAG: "dynamodb",
			dataplatformconsts.TABLE_TAG:    table.TableName,
		}

		// If the table is not serverless, we need to tag the pool names
		if table.ServerlessConfig == nil {
			tags["pool-name"] = worker.ReferenceKey("name")
			tags["driver-pool-name"] = driver.ReferenceKey("name")
		}

		// Choose the script based on IsUsedForDataLakeDeletion flag. Tables used
		// for data lake deletion use the upsert script, otherwise use the merge
		// script.
		script := mergeScript
		if table.InternalOverrides.IsUsedForDataLakeDeletion {
			script = upsertOnlyScript
		}

		merge := dataplatformresource.JobSpec{
			Name:                         jobName,
			Region:                       config.Region,
			Owner:                        team.DataPlatform,
			AdminTeams:                   []components.TeamInfo{table.TeamOwner},
			SparkVersion:                 sparkversion.DynamoDbrVersion,
			Script:                       script,
			Parameters:                   parameters,
			MinWorkers:                   1,
			MaxWorkers:                   maxWorkers,
			Profile:                      instanceProfile,
			Cron:                         cronSchedule,
			TimeoutSeconds:               72000, // 20 hours
			MinRetryInterval:             10 * time.Minute,
			EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
			JobOwnerServicePrincipalName: ciServicePrincipalAppId,
			Pool:                         worker.ReferenceKey("id"),
			DriverPool:                   driver.ReferenceKey("id"),
			Tags:                         tags,
			SparkConf:                    sparkConf,
			SparkEnvVars: map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": DynamoReplicationCredential,
			},
			RnDCostAllocation: rndCostAllocation,
			IsProduction:      table.Production,
			JobType:           dataplatformconsts.DynamoDbDeltaLakeIngestionMerge,
			Libraries: dataplatformresource.JobLibraryConfig{
				PyPIs: []dataplatformresource.PyPIName{
					dataplatformresource.SparkPyPIDatadog,
				},
			},
			Format: databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
			SloConfig:           &sloConfig,
			UnityCatalogSetting: unityCatalogSetting,
			RunAs: &databricks.RunAsSetting{
				ServicePrincipalName: ciServicePrincipalAppId,
			},
		}

		if table.ServerlessConfig != nil {
			sc := &databricks.ServerlessConfig{
				EnvironmentKey:    table.ServerlessConfig.EnvironmentKey,
				PerformanceTarget: table.ServerlessConfig.PerformanceTarget,
				Environments:      []databricks.ServerlessEnvironment{},
				BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
			}

			// Get the default client version for the serverless compute by region.
			clientVersion, ok := DynamodbMergeServerlessEnvironmentClientVersionByRegion[config.Region]
			if !ok {
				return nil, oops.Errorf("client version not found for region %s", config.Region)
			}

			// Override the default client version for the serverless compute by region.
			// If not set, use the default client version for the region.
			// This is set in the table registry.
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

		if table.Queue {
			merge.Queue = &databricks.JobQueue{
				Enabled: true,
			}
		}

		mergeJobResource, err := merge.TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job resource")
		}
		permissionsResource, err := merge.PermissionsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "permissions resource")
		}
		resourceGroups[table.TableName] = []tf.Resource{mergeJobResource, permissionsResource}

	}

	vacuum, err := dynamodVacuumPipeline(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create vacuum resources for DynamoDB tables")
	}

	resourceGroups["dynamodb_vacuum_resources"] = vacuum

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformDynamodbDeltaLakeProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "dynamodbmerge",
		Name:     "dynamodbmerge",
		ResourceGroups: project.MergeResourceGroups(
			resourceGroups, map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":        remotes.infra,
				"pool_remote":         remotes.instancePool,
			},
		),
	}, nil
}

func CreateLoadResource(config dataplatformconfig.DatabricksConfig, table dynamodbdeltalake.Table) *awsresource.DynamoDBTableExport {
	if table.ExportTime == "" {
		return nil
	}

	tableAwsAccountId := strconv.Itoa(awsregionconsts.RegionAccountID[config.Region])
	bucketAwsAccountId := strconv.Itoa(awsregionconsts.RegionDatabricksAccountID[config.Region])

	return &awsresource.DynamoDBTableExport{
		TableArn:       fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", config.Region, tableAwsAccountId, table.TableName),
		ExportTime:     table.ExportTime,
		ExportType:     awsresource.FullExport,
		ExportFormat:   awsresource.DynamoDBJson,
		S3Bucket:       fmt.Sprintf("%sdynamodb-export", awsregionconsts.RegionPrefix[config.Region]),
		S3Prefix:       fmt.Sprintf("load/%s", table.TableName),
		S3BucketOwner:  bucketAwsAccountId,
		S3SseAlgorithm: awsresource.Aes256,
	}
}
