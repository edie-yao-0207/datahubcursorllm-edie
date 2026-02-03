package emrreplicationproject

import (
	"fmt"
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

// emrValidationSparkJobProject creates a tf project for EMR replication validation across regions
func emrValidationSparkJobProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources) (*project.Project, error) {
	script, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/emrreplication/emr_validation.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading validation script")
	}

	resourceGroups := map[string][]tf.Resource{
		"validation_script": {script},
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
			jobName := databricksjobnameshelpers.GetEmrValidationJobName(databricksjobnameshelpers.GetEmrValidationJobNameInput{
				Entity: entity.Name,
				Cell:   cell,
				Region: config.Region,
			})

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
				"--export-bucket", emrReplicationExportBucket(cell),
				"--serverless-env-vars", serverlessEnvVarsJsonString,
				"--decimal-tolerance",
			}

			unityCatalogSetting := dataplatformresource.UnityCatalogSetting{
				DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
				SingleUserName:   ciServicePrincipalAppId,
			}

			rndCostAllocation := float64(1)
			if entity.Production {
				rndCostAllocation = 0
			}

			// TODO: Set the SLO once we have a stable job run schedule based on
			// the triggers from the temporal workflow.
			// sloConfig := dataplatformconsts.JobSlo{
			// 	SloTargetHours:              6,
			// 	Urgency:                     dataplatformconsts.JobSloHigh,
			// 	BusinessHoursThresholdHours: 6,
			// 	HighUrgencyThresholdHours:   12,
			// }

			validation := dataplatformresource.JobSpec{
				Name:                         jobName,
				Region:                       config.Region,
				Owner:                        team.DataPlatform,
				AdminTeams:                   []components.TeamInfo{team.DataPlatform},
				SparkVersion:                 sparkversion.EmrValidationDbrVersion,
				Script:                       script,
				Parameters:                   parameters,
				MinWorkers:                   1,
				MaxWorkers:                   4,
				Cron:                         "",   // No cron schedule - triggered by temporal
				TimeoutSeconds:               3600, // 1 hour
				MinRetryInterval:             10 * time.Minute,
				EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
				JobOwnerServicePrincipalName: ciServicePrincipalAppId,
				Tags: map[string]string{
					dataplatformconsts.CELL_TAG: cell,
				},
				SparkConf: map[string]string{
					"spark.databricks.sql.initial.catalog.name": "default",
				},
				RnDCostAllocation: rndCostAllocation,
				IsProduction:      entity.Production,
				JobType:           dataplatformconsts.EmrDeltaLakeIngestionValidation,
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
				ServerlessConfig: &databricks.ServerlessConfig{
					PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
					Environments: []databricks.ServerlessEnvironment{
						{
							EnvironmentKey: databricks.DefaultServerlessEnvironmentKey,
							Spec: &databricks.ServerlessEnvironmentSpec{
								Client: "1",
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
				// SloConfig: &sloConfig,
			}

			if config.Region == infraconsts.SamsaraAWSCARegion {
				validation.Profile = tf.LocalId("unity_catalog_instance_profile").Reference()
			}

			validationJobResource, err := validation.TfResource()
			if err != nil {
				return nil, oops.Wrapf(err, "building validation job resource for cell %s", cell)
			}
			permissionsResource, err := validation.PermissionsResource()
			if err != nil {
				return nil, oops.Wrapf(err, "permissions resource for cell %s", cell)
			}
			resourceGroups[fmt.Sprintf("emr_replication_validation_%s_%s", entity.Name, cell)] = []tf.Resource{validationJobResource, permissionsResource}
		}
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emr_replication",
		Name:     "emr_validation",
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
