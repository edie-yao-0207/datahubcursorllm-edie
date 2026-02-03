package ksdeltalake

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/databricksjobnameshelpers"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/objectstatownership"
	"samsaradev.io/libs/ni/infraconsts"
	ksdeltalakecomponents "samsaradev.io/service/components/ksdeltalake"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/teamnames"
)

// The client version for the KS merge job serverless compute by region.
// This controls what version of DBR is used by the merge job.
// Refer to the following documentation for more details: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
// Changing this value will change the version of DBR jobs that run on serverless.
// Its important to rollout changes to this value in a controlled manner.
var KsMergeServerlessEnvironmentClientVersionByRegion = map[string]databricks.ClientVersion{
	infraconsts.SamsaraAWSDefaultRegion: databricks.ClientVersion1,
	infraconsts.SamsaraAWSCARegion:      databricks.ClientVersion4,
	infraconsts.SamsaraAWSEURegion:      databricks.ClientVersion1,
}

func ksServerlessEnvVarsJsonString(envs map[string]string) (string, error) {
	envMapJson, err := json.Marshal(envs)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of serverless env vars")
	}
	return strings.ReplaceAll(string(envMapJson), "\"", "\\\""), nil
}

func kinesisStatsMergePipeline(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	resourceGroups := make(map[string][]tf.Resource)

	script, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/ksdeltalake/merge.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading merge script")
	}

	// Create a directory resource for the KS merge script.
	subdir := "/backend/dataplatform/ksdeltalake"
	directoryResource := &databricksresource.Directory{
		Path: subdir,
	}

	workspaceScriptPath := filepath.Join(subdir, "ks_merge_script.py")
	deleteScriptPath := filepath.Join(subdir, "ks_delete_script.py")
	resourceGroups["ks_merge_script_directory"] = []tf.Resource{directoryResource}

	mergeScriptWorkspaceFile := &databricksresource.WorkspaceFile{
		ResourceName: "ks_merge_script",
		Path:         workspaceScriptPath,
		Format:       databricks.ExportFormatRAW,
		Content:      fmt.Sprintf(`${file("%s")}`, filepath.Join(filepathhelpers.BackendRoot, "python3/samsaradev/infra/dataplatform/ksdeltalake/merge.py")),
	}

	resourceGroups["ks_merge_script_workspace_file"] = []tf.Resource{mergeScriptWorkspaceFile}

	deleteScriptWorkspaceFile := &databricksresource.WorkspaceFile{
		ResourceName: "ks_delete_script",
		Path:         deleteScriptPath,
		Format:       databricks.ExportFormatRAW,
		Content:      fmt.Sprintf(`${file("%s")}`, filepath.Join(filepathhelpers.BackendRoot, "python3/samsaradev/infra/dataplatform/ksdeltalake/delete.py")),
	}

	resourceGroups["ks_delete_script_workspace_file"] = []tf.Resource{deleteScriptWorkspaceFile}

	queues, err := ksdeltalakecomponents.GenerateDeltaLakeMergeKinesisStatsQueues()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	for _, table := range ksdeltalake.AllTables() {
		var resources []tf.Resource

		schemaPath := fmt.Sprintf("go/src/samsaradev.io/infra/dataplatform/ksdeltalake/schemas/%s.sql", table.QualifiedName())
		schema, err := dataplatformresource.DeployedArtifactObject(config.Region, schemaPath)
		if err != nil {
			return nil, err
		}
		resources = append(resources, schema)

		primaryKeys := "org_id,object_type,object_id,time"
		minWorkers := 1

		// Let's set up production jobs to have 4 machines in the US, 2 in the EU.
		// And for nonproduction jobs, we'll have max 2 machines.
		// We are continuing to iterate on these values until we find the right balance.
		maxWorkers := 4
		if config.Region == infraconsts.SamsaraAWSEURegion || config.Region == infraconsts.SamsaraAWSCARegion {
			maxWorkers = 2
		}
		if !table.Production {
			maxWorkers = 2
		}

		s3FilesPerChunk := dataplatformconsts.KinesisstatsDefaultS3FilesPerChunk
		// Respect any overrides that have been set
		if table.InternalOverrides != nil {
			if table.InternalOverrides.PrimaryKeysOverride != nil {
				primaryKeys = *table.InternalOverrides.PrimaryKeysOverride
			}

			if info, ok := table.InternalOverrides.RegionOverrides[config.Region]; ok {
				if info.MinWorkersOverride != nil {
					minWorkers = *info.MinWorkersOverride
				}

				if info.MaxWorkersOverride != nil {
					maxWorkers = *info.MaxWorkersOverride
				}

				if info.S3FilesPerChunk != nil {
					s3FilesPerChunk = *info.S3FilesPerChunk
				}
			}
		}

		for _, sched := range table.Jobs {
			params := []string{
				"--schema", schema.URL(),
				"--sink", ksdeltalake.TableLocation(config.Region, ksdeltalake.TableTypeDeduplicated, table.Name),
				"--partitionkey", "date",
				"--table", fmt.Sprintf("kinesisstats.%s", table.Name),
				"--primarykeys", primaryKeys,
				"--s3files", ksdeltalake.S3FilesTableLocation(config.Region, table.Name, sched.Name),
				"--num-s3files-per-chunk", fmt.Sprintf("%d", s3FilesPerChunk),
			}

			if table.Production {
				params = append(params, "--isProduction", "true")
			} else {
				params = append(params, "--isProduction", "false")
			}

			// These tables contain fields that may overflow. See this doc for a problem
			// summary: https://samsaradev.atlassian.net/wiki/x/k8xrlw
			overflowTables := []string{ //lint:sorted
				"location",
				"osDAccelerometer",
				"osDAccelerometerShadow",
				"osDCloudBackupUploadAttemptInfo",
				"osDDashcamDriverObstruction",
				"osDDashcamSeatbelt",
				"osDDashcamStreamStatus",
				"osDEdgeSpeedLimitV2",
				"osDEngineGauge",
				"osDGatewayMicroConfig",
				"osDGpsTimeToFirstFixMs",
				"osDIncrementalCellularUsage",
				"osDMcuAppEvent",
				"osDModiDeviceInfo",
				"osDNVR10CameraInfo",
				"osDVgCableInfo",
				"osDVgMcuMetrics",
			}

			if slices.Contains(overflowTables, table.Name) {
				params = append(params, "--mode", "PERMISSIVE")
			} else {
				params = append(params, "--mode", "FAILFAST")
			}

			// If a table has multiple jobs, we've made sure that they don't write to the same partitions so that
			// they don't conflict with one another if they happen to run at the same time.
			// However, metadata updates (like updating the table description using ALTER TABLE)
			// are on the entire table and this can cause a MetadataChangedException if multiple jobs attempt
			// to write to the table at the same time.
			// It's unfortunate, but let's get around this by not writing a description for these tables for now.
			// It's worth noting that we only really need to do this 1 time, and we haven't added a new tiered job
			// in a very long time, so this probably won't have much practical effect.
			if len(table.Jobs) == 1 {
				params = append(params, "--description", table.KsDescription())
			} else {
				params = append(params, "--description", "")
			}

			if sched.Filter != "" {
				params = append(params, "--rangefilter", sched.Filter)
			}

			sparkVersion := sparkversion.KinesisstatsDbrVersion

			if sched.Json2Parquet {
				queue, ok := queues[table.Name]
				if !ok {
					return nil, oops.Errorf("queue not found for table %s", table.Name)
				}
				// Some stats have multiple jobs (eg location, which has a 3hr and daily
				// job) and therefore have multiple s3filestables to keep track of
				// progress.
				var s3files []string
				for _, job := range table.Jobs {
					s3files = append(s3files, ksdeltalake.S3FilesTableLocation(config.Region, table.Name, job.Name))
				}

				params = append(params,
					"--json2parquet", "true",
					"--json2parquet-queue", queue.SqsUrl(config.Region),
					"--json2parquet-s3files", strings.Join(s3files, ","),
				)
			}
			osName := strings.ToLower(table.Name)
			teamName, shared := objectstatownership.GetTeamOwnerNameForStatName(osName)

			statOwner := team.DataPlatform
			if !shared && osName != "location" {
				if teamName == "" {
					return nil, oops.Errorf("Cannot process object stat with no owner")
				}
				statOwner = team.TeamByName[teamName]
			}

			var adminTeams []components.TeamInfo
			adminTeams = append(adminTeams, statOwner)
			for _, team := range table.AdditionalAdminTeams {
				// don't re-add the job owner, or dataplatform
				if team.TeamName == statOwner.TeamName || team.TeamName == teamnames.DataPlatform {
					continue
				}
				adminTeams = append(adminTeams, team)
			}

			jobName := databricksjobnameshelpers.GetKSMergeJobName(databricksjobnameshelpers.GetKSMergeJobNameInput{
				TableName:   table.Name,
				JobSchedule: sched.Name,
			})

			// Production jobs should start within the first 10 minutes every 3 hours (0, 3, 6...). This schedule
			// runs them right before datapipelines.
			// Nonproduction jobs will be spread out over the entire day (except when production jobs are running to prevent
			// too many jobs starting at once) since theres no schedule they need to be on.
			minuteInWindow := dataplatformresource.RandomFromNameHash(jobName, 10, 180)

			var cronHour string
			frequencyInHours := table.GetReplicationFrequencyHour()
			if table.InternalOverrides != nil && table.InternalOverrides.HourlyCronSchedule {
				// Run every hour, and don't skip hour 0.
				cronHour = fmt.Sprintf("*/%s", frequencyInHours)
			} else {
				// Spread non-production jobs across hours 0-2 based on hash.
				// Jobs at hour 0 start at minute 10+ to avoid overlap with production jobs (minute 0-10).
				// Jobs starting at hour 1 or 2 end up skipping hour 0, and running every X hours.
				hourToStartFrom := minuteInWindow / 60
				cronHour = fmt.Sprintf("%d/%s", hourToStartFrom, frequencyInHours)
			}
			cronMinute := minuteInWindow % 60

			if table.Production {
				// If production, don't skip hour 0, and run every X hours.
				cronHour = fmt.Sprintf("*/%s", frequencyInHours)
				cronMinute = dataplatformresource.RandomFromNameHash(jobName, 0, 10)
			}

			partitionChunkSize := dataplatformconsts.KinesisstatsDefaultPartitionChunkSize
			isDaily := sched.Name == dataplatformconsts.KsJobTypeDaily
			if isDaily {
				partitionChunkSize = dataplatformconsts.KinesisstatsDailyPartitionChunkSize
			}
			if table.InternalOverrides != nil {
				if info, ok := table.InternalOverrides.RegionOverrides[config.Region]; ok {
					if isDaily && info.DailyPartitionChunkSize != nil {
						partitionChunkSize = *info.DailyPartitionChunkSize
					} else if !isDaily && info.PartitionChunkSize != nil {
						partitionChunkSize = *info.PartitionChunkSize
					}
				}
			}

			if partitionChunkSize != 0 {
				params = append(params, "--partitionchunksize", fmt.Sprintf("%d", partitionChunkSize))
			}

			// Daily jobs will continue to be scheduled at the same time, regardless of production or nonproduction.
			sparkConf := make(map[string]string)

			scheduleType := "3hr"
			if isDaily {
				sparkVersion = sparkversion.KinesisstatsDailyDbrVersion
				scheduleType = "daily"
				cronHour = "1,13"
				maxWorkers = 2 * maxWorkers
				sparkConf["spark.driver.maxResultSize"] = "0"
				// Give machines under high load more time to respond.
				sparkConf["spark.network.timeout"] = "600"
			} else if sched.Name == dataplatformconsts.KsJobTypeEvery1Hr {
				scheduleType = "1hr"
				cronHour = "*/1"
			}

			cronSchedule := fmt.Sprintf("0 %d %s * * ?", cronMinute, cronHour)

			// Working with Databricks team to determine the right set of spark configurations to best
			// optimize the merge operations against data skew problems.
			sparkConf["spark.shuffle.accurateBlockSkewedFactor"] = "5"

			rndCostAllocation := float64(1)
			if table.Production {
				rndCostAllocation = 0
			}
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

			// Set up the correct instance pools based on production and region.
			az := dataplatformresource.JobNameToAZ(config.Region, jobName)
			identifiers := getPoolIdentifiers(config.Region, table.Production)[az]
			worker := tf.LocalId(identifiers.WorkerPoolIdentifier)
			driver := tf.LocalId(identifiers.DriverPoolIdentifier)

			sloConfig := table.KsSloConfig(sched.Name)

			serverlessConfig := table.ServerlessConfig
			if table.InternalOverrides != nil && table.InternalOverrides.DisableServerless {
				serverlessConfig = nil
			}
			// If production and not in CA region, disable serverless.
			// TODO: Once we have rolled out serverless to prod stats in all regions, we can remove this.
			if config.Region == infraconsts.SamsaraAWSDefaultRegion && table.Production {
				// Special case for osDSaltSpreaderStateActivelySpreading which is a prod stat that is already running on serverless.
				// TODO: Once we have rolled out serverless to prod stats in all regions, we can remove this special casing.
				if table.Name != "osDSaltSpreaderStateActivelySpreading" {
					serverlessConfig = nil
				}
			}

			tags := map[string]string{
				dataplatformconsts.DATABASE_TAG: "kinesisstats",
				dataplatformconsts.TABLE_TAG:    osName,
				dataplatformconsts.SCHEDULE_TAG: scheduleType,
			}

			if serverlessConfig == nil {
				tags["pool-name"] = worker.ReferenceKey("name")
				tags["driver-pool-name"] = driver.ReferenceKey("name")
			}

			taskTimeoutSeconds := int(sched.Timeout.Seconds())
			taskMaxRetries := 1
			minRetryInterval := 10 * time.Minute
			taskMinRetryIntervalMillis := int((minRetryInterval).Milliseconds())
			// TODO: we can converge these to a single environment key once we move
			// off the custom image.
			mergeTaskEnvironmentKey := databricks.DefaultServerlessEnvironmentKey
			deleteTaskEnvironmentKey := "DELETE_ENVIRONMENT"
			mergeParams := params
			jobTags := map[string]string{
				"format": databricks.MultiTaskKey,
			}

			serverlessEnvVars, err := ksServerlessEnvVarsJsonString(map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "kinesisstats-replication",
				"AWS_REGION":         config.Region,
				"AWS_DEFAULT_REGION": config.Region,
			})
			if err != nil {
				return nil, oops.Wrapf(err, "error getting serverless env vars json string")
			}
			// Create the job spec for the merge job and delete job.
			var job dataplatformresource.JobSpec

			// Roll out execute mode if EnableDeletionTask is true.
			executeMode := false
			// Roll out execute mode for:
			// 1. All jobs in the EU and CA regions
			if (config.Region == infraconsts.SamsaraAWSCARegion || config.Region == infraconsts.SamsaraAWSEURegion) || !table.Production {
				executeMode = true
			}

			// Create the main merge task key.
			// Truncate table name to preserve the prefix for uniqueness.
			mergePrefix := "merge-"
			mergeTableName := table.Name
			if len(mergePrefix)+len(mergeTableName) > databricks.TaskKeyMaxLength {
				mergeTableName = mergeTableName[:databricks.TaskKeyMaxLength-len(mergePrefix)]
			}
			mergeTaskKey := mergePrefix + mergeTableName

			// Create the delete task key.
			// Truncate table name to preserve the prefix for uniqueness.
			deletePrefix := "delete-"
			deleteTableName := table.Name
			if len(deletePrefix)+len(deleteTableName) > databricks.TaskKeyMaxLength {
				deleteTableName = deleteTableName[:databricks.TaskKeyMaxLength-len(deletePrefix)]
			}
			deleteTaskKey := deletePrefix + deleteTableName

			// Main merge task
			mergeTask := &databricks.JobTaskSettings{
				TaskKey:                      mergeTaskKey,
				TimeoutSeconds:               taskTimeoutSeconds,
				MaxRetries:                   taskMaxRetries,
				MinRetryIntervalMilliseconds: taskMinRetryIntervalMillis,
				SparkPythonTask: &databricks.SparkPythonTask{
					PythonFile: filepath.Join("/Workspace", workspaceScriptPath),
					Parameters: mergeParams,
				},
			}

			var deleteTask *databricks.JobTaskSettings
			// We don't need to run the deletion task for daily jobs since we already
			// delete the data within the every hour/3hr jobs.
			if !isDaily && (table.InternalOverrides == nil || !table.InternalOverrides.DisableDeletionTask) {
				// Delete task that depends on the main merge task.
				// For now, use the same timeout as the merge task. We might need to
				// modify this if we want to allow the delete task to run for a longer
				// time.
				deleteTaskParams := buildDeleteTaskParams(config, table, osName, executeMode)

				deleteTask = &databricks.JobTaskSettings{
					TaskKey:                      deleteTaskKey,
					TimeoutSeconds:               taskTimeoutSeconds,
					MaxRetries:                   taskMaxRetries,
					MinRetryIntervalMilliseconds: taskMinRetryIntervalMillis,
					DependsOn: []*databricks.TaskDependency{
						{TaskKey: mergeTaskKey},
					},
					SparkPythonTask: &databricks.SparkPythonTask{
						PythonFile: filepath.Join("/Workspace", deleteScriptPath),
						Parameters: deleteTaskParams,
					},
				}
			}

			job = dataplatformresource.JobSpec{
				Name:                         jobName,
				Region:                       config.Region,
				Owner:                        team.DataPlatform, // Data Platform owns the code for KS merge, so mark them as owner despite underlying tables being owned by other teams
				SparkVersion:                 sparkVersion,
				MinWorkers:                   minWorkers,
				MaxWorkers:                   maxWorkers,
				MaxRetries:                   taskMaxRetries,
				MinRetryInterval:             minRetryInterval,
				Profile:                      tf.LocalId(instanceProfileName).Reference(),
				Cron:                         cronSchedule,
				TimeoutSeconds:               taskTimeoutSeconds,
				EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
				Tags:                         tags,
				Pool:                         worker.ReferenceKey("id"),
				DriverPool:                   driver.ReferenceKey("id"),
				JobOwnerServicePrincipalName: ciServicePrincipalAppId,
				RnDCostAllocation:            rndCostAllocation,
				IsProduction:                 table.Production,
				JobType:                      dataplatformconsts.KinesisStatsDeltaLakeIngestionMerge,
				Libraries: dataplatformresource.JobLibraryConfig{
					PyPIs: []dataplatformresource.PyPIName{
						dataplatformresource.SparkPyPIDatadog,
					},
				},
				SparkConf: sparkConf,
				SparkEnvVars: map[string]string{
					"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "kinesisstats-replication",
				},
				AdminTeams:          adminTeams,
				Format:              databricks.MultiTaskKey,
				JobTags:             jobTags,
				SloConfig:           &sloConfig,
				UnityCatalogSetting: ucSettings,
				RunAs: &databricks.RunAsSetting{
					ServicePrincipalName: ciServicePrincipalAppId,
				},
				Tasks: []*databricks.JobTaskSettings{mergeTask},
			}
			if deleteTask != nil {
				job.Tasks = append(job.Tasks, deleteTask)
			}

			if serverlessConfig != nil {
				// Set the environment key on tasks only for serverless compute.
				mergeTask.EnvironmentKey = mergeTaskEnvironmentKey
				if deleteTask != nil {
					deleteTask.EnvironmentKey = deleteTaskEnvironmentKey
				}
				sc := &databricks.ServerlessConfig{
					EnvironmentKey:    serverlessConfig.EnvironmentKey,
					PerformanceTarget: serverlessConfig.PerformanceTarget,
					Environments:      []databricks.ServerlessEnvironment{},
					BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
				}

				clientVersion, ok := KsMergeServerlessEnvironmentClientVersionByRegion[config.Region]
				if !ok {
					return nil, oops.Errorf("client version not found for region %s", config.Region)
				}

				// Override the default client version for the serverless compute by region.
				// If not set, use the default client version for the region.
				// This is set in the table registry.
				if serverlessConfig.ClientVersionByRegionOverride != nil {
					overrideClientVersion, ok := serverlessConfig.ClientVersionByRegionOverride[config.Region]
					if ok && overrideClientVersion != "" {
						clientVersion = overrideClientVersion
					}
				}
				// Add the environments to the serverless config.
				sc.Environments = append(sc.Environments,
					databricks.ServerlessEnvironment{
						EnvironmentKey: mergeTaskEnvironmentKey,
						Spec: &databricks.ServerlessEnvironmentSpec{
							Client: string(clientVersion),
						},
					},
				)
				if deleteTask != nil {
					sc.Environments = append(sc.Environments,
						databricks.ServerlessEnvironment{
							EnvironmentKey: deleteTaskEnvironmentKey,
							Spec: &databricks.ServerlessEnvironmentSpec{
								Client:       string(clientVersion),
								Dependencies: []string{dataplatformresource.SparkPyPIDatadog.PinnedString()},
							},
						},
					)
				}

				job.ServerlessConfig = sc

				// Add the tags to job tags in serverless mode.
				for k, v := range tags {
					job.JobTags[k] = v
				}

				// Compute serverless params with the full job tags (after all tags are added).
				jobTagsString, err := ksServerlessEnvVarsJsonString(job.JobTags)
				if err != nil {
					return nil, oops.Wrapf(err, "error getting serverless jobTagsString json string")
				}
				mergeTask.SparkPythonTask.Parameters = append(params, "--use-serverless-compute", "true", "--serverless-env-vars", serverlessEnvVars, "--serverless-tags", jobTagsString)
				if deleteTask != nil {
					deleteTask.SparkPythonTask.Parameters = append(deleteTask.SparkPythonTask.Parameters, "--use-serverless-compute", "true", "--serverless-env-vars", serverlessEnvVars, "--serverless-tags", jobTagsString)
				}
			}

			// Enable queueing based on region-specific configuration
			if table.InternalOverrides != nil {
				if regionOverrides, ok := table.InternalOverrides.RegionOverrides[config.Region]; ok {
					for _, queueSchedule := range regionOverrides.QueueEnabledSchedules {
						if sched.Name == queueSchedule {
							job.Queue = &databricks.JobQueue{
								Enabled: true,
							}
							break
						}
					}
				}
			}

			jobResource, err := job.TfResource()
			if err != nil {
				return nil, oops.Wrapf(err, "building job resource")
			}
			permissionsResource, err := job.PermissionsResource()
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			resources = append(resources, jobResource, permissionsResource)
		}

		resourceGroups[table.Name] = resources
	}

	if len(resourceGroups) > 0 {
		resourceGroups["merge_script"] = []tf.Resource{script}
	}

	return resourceGroups, nil
}

func buildDeleteTaskParams(
	config dataplatformconfig.DatabricksConfig,
	table ksdeltalake.Table,
	tableName string,
	executeMode bool,
) []string {
	params := []string{
		"--retention-s3prefix", ksdeltalake.S3RetentionConfigLocation(config.Region, table.Name),
		"--sink", ksdeltalake.TableLocation(config.Region, ksdeltalake.TableTypeDeduplicated, table.Name),
		"--stat-type", fmt.Sprintf("%d", table.StatType),
	}
	if tableName != "" {
		params = append(params, "--table", tableName)
	}
	if executeMode {
		params = append(params, "--execute")
	}
	return params
}
