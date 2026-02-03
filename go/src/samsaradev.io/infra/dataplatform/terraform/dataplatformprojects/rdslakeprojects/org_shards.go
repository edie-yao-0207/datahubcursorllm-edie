package rdslakeprojects

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
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

func serverlessEnvVarsJsonString(envs map[string]string) (string, error) {
	envMapJson, err := json.Marshal(envs)
	if err != nil {
		return "", oops.Wrapf(err, "error marshalling map of serverless env vars")
	}
	return strings.ReplaceAll(string(envMapJson), "\"", "\\\""), nil
}

// While this table is not very specific to rds, its majorly used by combined views of sharded rds replicated tables.
// The V1 is was created manually and schema was checked into rds generator code.
// V2 is fully managed by code.
func orgShardsV2TableSparkJobProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources) (*project.Project, error) {

	resources_uc, err := orgShardsV2TableSparkJob(config)

	if err != nil {
		return nil, oops.Wrapf(err, "error generated org shards v2 table spark job resources")
	}
	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "org_shards_v2",
		Name:     "org_shards_v2",
		ResourceGroups: project.MergeResourceGroups(
			map[string][]tf.Resource{
				"databricks_provider":     dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":            remotes.infra,
				"org_shards_uc_resources": resources_uc,
			},
		),
	}, nil
}

func orgShardsV2TableSparkJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	script, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/orgshardstablev2/orgshardstablev2.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error loading orgshardstablev2 script")
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	// Run every 12 hours
	cronSchedule := "0 5 */12 * * ?"

	regionalPrefix := ""
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		regionalPrefix = ""
	} else if config.Region == infraconsts.SamsaraAWSCARegion {
		regionalPrefix = "ca-"
	} else if config.Region == infraconsts.SamsaraAWSEURegion {
		regionalPrefix = "eu-"
	} else {
		return nil, oops.Wrapf(err, "invalid region %s", config.Region)
	}

	serverlessEnvVars, err := serverlessEnvVarsJsonString(map[string]string{
		"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "rds-replication",
		"AWS_REGION":         config.Region,
		"AWS_DEFAULT_REGION": config.Region,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error getting serverless env vars json string")
	}

	parameters := []string{
		"--input-path", fmt.Sprintf("s3://samsara-%sprod-app-configs/org-shards-csv/all.csv", regionalPrefix),
		"--output-path", fmt.Sprintf("s3://samsara-%srds-delta-lake/others/org-shards-table-v2/all.csv", regionalPrefix),
		"--serverless-env-vars", serverlessEnvVars,
	}

	rndCostAllocation := float64(1)

	sloConfig := dataplatformconsts.JobSlo{
		SloTargetHours:              24,
		Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
		BusinessHoursThresholdHours: 25,
		HighUrgencyThresholdHours:   96,
	}

	sparkJob := dataplatformresource.JobSpec{
		Name:                         "org-shards-table-v2",
		Region:                       config.Region,
		Owner:                        team.DataPlatform,
		AdminTeams:                   []components.TeamInfo{team.DataPlatform},
		SparkVersion:                 sparkversion.OrgShardsTableV2DbrVersion,
		Script:                       script,
		Parameters:                   parameters,
		Cron:                         cronSchedule,
		TimeoutSeconds:               1800, // 30 minutes
		MinRetryInterval:             10 * time.Minute,
		EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		RnDCostAllocation:            rndCostAllocation,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.OrgShardsTableV2,
		Format:                       databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		UnityCatalogSetting: dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   ciServicePrincipalAppId,
		},
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
						Client:       "1",
						Dependencies: []string{dataplatformresource.SparkPyPIDatadog.PinnedString()},
					},
				},
			},
			EnvironmentKey: databricks.DefaultServerlessEnvironmentKey,
			BudgetPolicyId: dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
		},
	}

	sparkJobResource, err := sparkJob.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource for org shards table v2")
	}
	permissionsResource, err := sparkJob.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "permissions resource for org shards table v2")
	}

	return []tf.Resource{script, sparkJobResource, permissionsResource}, nil
}
