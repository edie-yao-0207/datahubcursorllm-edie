package dataplatformprojects

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const dataplatformClusterInstanceProfile = "dataplatform-cluster"

func CostMonitoringProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)

	// Cost data only exists in the US, so only generate these resources in the US
	if config.Region != infraconsts.SamsaraAWSDefaultRegion {
		return []*project.Project{}, nil
	}

	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	profileArn, err := dataplatformresource.InstanceProfileArn(providerGroup, dataplatformClusterInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, profileArn),
		},
	}

	var projects []*project.Project

	weeklyCostResources, err := weeklyCostDashboardJob(config)
	if err != nil {
		return nil, err
	}

	dailyCostResources, err := dailyCostAlertsJob(config)
	if err != nil {
		return nil, err
	}

	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "costmonitoring",
		ResourceGroups: map[string][]tf.Resource{
			"weekly_dashboard":    weeklyCostResources,
			"daily_cost_alerts":   dailyCostResources,
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
	})

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
			"infra_remote": instanceProfileResources,
		})
	}
	return projects, nil
}

/**
 * Set of overrides for alert destinations.
 * Please use a lowercase team name as the key,
 * and the value as the channel you want to
 * receive notifications from dataplatform.
 */
var teamDestinationOverrides = map[string]string{
	strings.ToLower(team.DataPlatform.TeamName): "alerts-data-platform",
	strings.ToLower(team.Support.TeamName):      "alerts-data-platform",
	strings.ToLower(team.Firmware.TeamName):     "databricks-cost-firmware",
}

// Compute the alert destinations for each team, using the overrides and
// falling back to dataplatform's alert channel in case they have no place else to go.
func getTeamChannel(t components.TeamInfo) string {
	teamName := strings.ToLower(t.TeamName)
	if val, ok := teamDestinationOverrides[teamName]; ok {
		return val
	} else if t.SlackAlertsChannel == "" {
		return getTeamChannel(team.DataPlatform)
	} else {
		return t.SlackAlertsChannel
	}
}

func weeklyCostDashboardJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/costmonitoring/weekly_dashboard_slack.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	// Compute the alert destinations for each team, using the overrides and
	// falling back to dataplatform's alert channel in case they have no place else to go.
	teamMap := make(map[string]string)
	for _, t := range team.AllTeams {
		teamMap[strings.ToLower(t.TeamName)] = getTeamChannel(t)
	}

	bytes, err := json.Marshal(teamMap)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to serialize inventory config")
	}

	parameters := []string{
		"--teammap",
		strings.ReplaceAll(string(bytes), "\"", "\\\""),
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	sparkConf := map[string]string{
		"spark.databricks.sql.initial.catalog.name":            "default",
		"databricks.loki.fileStatusCache.enabled":              "false",
		"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
		"spark.databricks.scan.modTimeCheck.enabled":           "false",
	}

	job := dataplatformresource.JobSpec{
		Name:               "weekly-cost-dashboard",
		Region:             config.Region,
		Owner:              team.DataPlatform,
		Script:             script,
		SparkVersion:       sparkversion.CostMonitoringDbrVersion,
		Parameters:         parameters,
		Profile:            tf.LocalId("instance_profile").Reference(),
		MaxWorkers:         2,
		DriverNodeType:     "rd-fleet.xlarge",
		WorkerNodeType:     "rd-fleet.xlarge",
		EmailNotifications: []string{team.TimothyKim.Email},
		// Thursdays at 10am PT. Do it on thursdays since cost data is delayed
		// and we want to try to do it at a time when the most full weeks are available
		Cron:                         "19 0 10 ? * Thu",
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		RnDCostAllocation:            1,
		JobType:                      dataplatformconsts.ScheduledNotebook, // eventually change this!
		Format:                       databricks.MultiTaskKey,
		Libraries: dataplatformresource.JobLibraryConfig{
			PyPIs: []dataplatformresource.PyPIName{
				dataplatformresource.SparkPyPISlack,
			},
		},
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		SparkConf: sparkConf,
		SparkEnvVars: map[string]string{
			"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "standard-read-parameters-ssm",
		},
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
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

	return resources, nil
}

func dailyCostAlertsJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/costmonitoring/cost_alerts.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	var parameters []string

	teamMap := make(map[string]string)
	for _, t := range team.AllTeams {
		teamMap[strings.ToLower(t.TeamName)] = getTeamChannel(t)
	}

	bytes, err := json.Marshal(teamMap)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to serialize inventory config")
	}

	parameters = append(parameters, "--teammap", strings.ReplaceAll(string(bytes), "\"", "\\\""))

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	sparkConf := map[string]string{
		"spark.databricks.sql.initial.catalog.name":            "default",
		"databricks.loki.fileStatusCache.enabled":              "false",
		"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
		"spark.databricks.scan.modTimeCheck.enabled":           "false",
	}

	job := dataplatformresource.JobSpec{
		Name:                         "daily-cost-alerts",
		Region:                       config.Region,
		Owner:                        team.DataPlatform,
		Script:                       script,
		SparkVersion:                 sparkversion.CostMonitoringDbrVersion,
		Parameters:                   parameters,
		Profile:                      tf.LocalId("instance_profile").Reference(),
		MaxWorkers:                   2,
		DriverNodeType:               "rd-fleet.xlarge",
		WorkerNodeType:               "rd-fleet.xlarge",
		Cron:                         "0 0 12 * * ?", // every day at 12pm PT, runs 3 hours after the cost update job.
		EmailNotifications:           []string{"dev-data-platform@samsara.com"},
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		JobType:                      dataplatformconsts.ScheduledNotebook, // eventually change this!
		Format:                       databricks.MultiTaskKey,
		Libraries: dataplatformresource.JobLibraryConfig{
			PyPIs: []dataplatformresource.PyPIName{
				dataplatformresource.SparkPyPISlack,
			},
		},
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		SparkConf: sparkConf,
		SparkEnvVars: map[string]string{
			"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "standard-read-parameters-ssm",
		},
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
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

	return resources, nil
}
