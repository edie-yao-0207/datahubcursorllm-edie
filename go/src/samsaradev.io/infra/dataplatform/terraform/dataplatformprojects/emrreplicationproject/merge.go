package emrreplicationproject

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/databricksjobnameshelpers"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const EmrReplicationCredential string = "emr-replication"

// The client version for the EMR replication merge job serverless compute by region.
// This controls what version of DBR is used by the merge job.
// Refer to the following documentation for more details: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/
// Changing this value will change the version of DBR jobs that run on serverless.
// Its important to rollout changes to this value in a controlled manner.
var EmrReplicationServerlessEnvironmentClientVersionByRegion = map[string]databricks.ClientVersion{
	infraconsts.SamsaraAWSDefaultRegion: databricks.ClientVersion1,
	infraconsts.SamsaraAWSCARegion:      databricks.ClientVersion4,
	infraconsts.SamsaraAWSEURegion:      databricks.ClientVersion1,
}

type remoteResources struct {
	infra        []tf.Resource
	instancePool []tf.Resource
}

func emrReplicationDeltaLakeBucket(cell string) string {
	return fmt.Sprintf("emr-replication-delta-lake-%s", cell)
}

func emrReplicationExportBucket(cell string) string {
	return fmt.Sprintf("emr-replication-export-%s", cell)
}

func emrReplicationDatabase(cell string) string {
	return fmt.Sprintf("emr_%s", cell)
}

func emrServerlessEnvVarsJsonString(envs map[string]string) (string, error) {
	envMapJson, err := json.Marshal(envs)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of serverless env vars")
	}
	return strings.ReplaceAll(string(envMapJson), "\"", "\\\""), nil
}

// emrMergeSparkJobProject creates a tf project for EMR replication across regions
func emrMergeSparkJobProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources) (*project.Project, error) {
	script, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/emrreplication/emr_merge.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading merge script")
	}

	resourceGroups := map[string][]tf.Resource{
		"merge_script": {script},
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	for _, entity := range emrreplication.GetAllEmrReplicationSpecs() {
		cells, err := emrhelpers.GetCellsForEntity(entity, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "getting cells for entity %s", entity.Name)
		}
		for _, cell := range cells {
			jobName := databricksjobnameshelpers.GetEmrReplicationMergeJobName(databricksjobnameshelpers.GetEmrReplicationMergeJobNameInput{
				Entity: entity.Name,
				Cell:   cell,
				Region: config.Region,
			})

			// Choose the correct pool based on region and AZ
			az := dataplatformresource.JobNameToAZ(config.Region, jobName)
			idents := getPoolIdentifiers(config.Region, entity.Production)[az]
			worker := tf.LocalId(idents.WorkerPoolIdentifier)
			driver := tf.LocalId(idents.DriverPoolIdentifier)

			// Run jobs every hour with randomized minutes to spread load
			startMinute := dataplatformresource.RandomFromNameHash(jobName, 0, 59)
			cronSchedule := fmt.Sprintf("0 %d 0/1 * * ?", startMinute)

			serverlessEnvVarsJsonString, err := emrServerlessEnvVarsJsonString(map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": EmrReplicationCredential,
				"AWS_REGION":         config.Region,
				"AWS_DEFAULT_REGION": config.Region,
			})
			if err != nil {
				return nil, oops.Wrapf(err, "error marshalling map of serverless env vars")
			}

			parameters := []string{
				"--region", config.Region,
				"--database", emrReplicationDatabase(cell),
				"--entityname", entity.Name,
				"--delta-lake-bucket", emrReplicationDeltaLakeBucket(cell),
				"--export-bucket", emrReplicationExportBucket(cell),
				"--date-partition-expression", entity.DatePartitionExpression,
				"--yaml-path", entity.YamlPath,
				"--serverless-env-vars", serverlessEnvVarsJsonString,
			}

			unityCatalogSetting := dataplatformresource.UnityCatalogSetting{
				DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
				SingleUserName:   ciServicePrincipalAppId,
			}
			instanceProfile := tf.LocalId("unity_catalog_instance_profile").Reference()

			rndCostAllocation := float64(1)
			if entity.Production {
				rndCostAllocation = 0
			}

			sloConfig := dataplatformconsts.JobSlo{
				SloTargetHours:              6,
				Urgency:                     dataplatformconsts.JobSloHigh,
				BusinessHoursThresholdHours: 6,
				HighUrgencyThresholdHours:   12,
			}

			clientVersion, ok := EmrReplicationServerlessEnvironmentClientVersionByRegion[config.Region]
			if !ok {
				return nil, oops.Errorf("client version not found for region %s", config.Region)
			}

			merge := dataplatformresource.JobSpec{
				Name:                         jobName,
				Region:                       config.Region,
				Owner:                        team.DataPlatform,
				AdminTeams:                   []components.TeamInfo{team.DataPlatform},
				SparkVersion:                 sparkversion.EmrReplicationDbrVersion,
				Script:                       script,
				Parameters:                   parameters,
				MinWorkers:                   1,
				MaxWorkers:                   3,
				Profile:                      instanceProfile,
				Cron:                         cronSchedule,
				TimeoutSeconds:               72000, // 20 hours
				MinRetryInterval:             10 * time.Minute,
				EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
				JobOwnerServicePrincipalName: ciServicePrincipalAppId,
				Pool:                         worker.ReferenceKey("id"),
				DriverPool:                   driver.ReferenceKey("id"),
				Tags: map[string]string{
					dataplatformconsts.CELL_TAG: cell,
					"pool-name":                 worker.ReferenceKey("name"),
					"driver-pool-name":          driver.ReferenceKey("name"),
				},
				SparkConf: map[string]string{
					"spark.databricks.sql.initial.catalog.name": "default",
				},
				SparkEnvVars: map[string]string{
					"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": EmrReplicationCredential,
				},
				RnDCostAllocation: rndCostAllocation,
				IsProduction:      entity.Production,
				JobType:           dataplatformconsts.EmrDeltaLakeIngestionMerge,
				Libraries: dataplatformresource.JobLibraryConfig{
					PyPIs: []dataplatformresource.PyPIName{
						dataplatformresource.SparkPyPIDatadog,
						dataplatformresource.SparkPyPIYaml,
					},
				},
				Format: databricks.MultiTaskKey,
				JobTags: map[string]string{
					"format":           databricks.MultiTaskKey,
					"samsara:database": emrReplicationDatabase(cell),
					"samsara:table":    entity.Name,
				},
				UnityCatalogSetting: unityCatalogSetting,
				RunAs: &databricks.RunAsSetting{
					ServicePrincipalName: ciServicePrincipalAppId,
				},
				SloConfig: &sloConfig,
				ServerlessConfig: &databricks.ServerlessConfig{
					PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
					Environments: []databricks.ServerlessEnvironment{
						{
							EnvironmentKey: databricks.DefaultServerlessEnvironmentKey,
							Spec: &databricks.ServerlessEnvironmentSpec{
								Client: string(clientVersion),
								Dependencies: []string{
									dataplatformresource.SparkPyPIDatadog.PinnedString(),
									dataplatformresource.SparkPyPIYaml.PinnedString(),
								},
							},
						},
					},
					EnvironmentKey: databricks.DefaultServerlessEnvironmentKey,
					BudgetPolicyId: dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
				},
			}

			mergeJobResource, err := merge.TfResource()
			if err != nil {
				return nil, oops.Wrapf(err, "building job resource for cell %s", cell)
			}
			permissionsResource, err := merge.PermissionsResource()
			if err != nil {
				return nil, oops.Wrapf(err, "permissions resource for cell %s", cell)
			}
			resourceGroups[fmt.Sprintf("emr_replication_merge_%s_%s", entity.Name, cell)] = []tf.Resource{mergeJobResource, permissionsResource}
		}
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emr_replication",
		Name:     "emr_replication_merge",
		ResourceGroups: project.MergeResourceGroups(
			resourceGroups, map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":        remotes.infra,
				"pool_remote":         remotes.instancePool,
			},
		),
		GenerateOutputs: true,
	}, nil
}

// Serverless is enabled for all regions except for CA region because there
// might be something wrong with how we set up the configuration for it. We can
// remove this once we get it working with Databricks support's help:
// https://samsara-rd.slack.com/archives/C0123925GV7/p1757434199298989
func serverlessEnabledForRegion(region string) bool {
	return region != infraconsts.SamsaraAWSCARegion
}
