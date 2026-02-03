package mixpanel

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
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const MixpanelMaxWorkers = 4
const MixpanelInstanceProfile = "dataengineering-cluster"

func MixpanelProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	mixpanelArn, err := dataplatformresource.InstanceProfileArn(providerGroup, MixpanelInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, mixpanelArn),
		},
	}

	ingestionJobResources, err := mixpanelJob(config)
	if err != nil {
		return nil, err
	}

	createTableJobResources, err := mixpanelCreateTableJob(config)
	if err != nil {
		return nil, err
	}
	var projects []*project.Project
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "mixpanel",
		ResourceGroups: map[string][]tf.Resource{
			"ingestionJob":        ingestionJobResources,
			"createTableJob":      createTableJobResources,
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

func mixpanelJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	mixpanelScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/mixpanel/mixpanel_daily_ingestion.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, mixpanelScript)

	emailAlerts := []string{team.DataEngineering.SlackAlertsChannelEmail.Email}

	// Provide the db names and the skip table path.
	var parameters []string
	tableMap := make(map[string]string) // Create an empty map

	for _, table := range MixpanelTableRegistry {
		tableMap[table.S3Directory] = table.TableName
	}

	bytes, err := json.MarshalIndent(tableMap, "", "  ")
	if err != nil {
		return nil, oops.Wrapf(err, "json marshaling of Mixpanel table spec")
	}

	// we need to escape any quotes
	contents := string(bytes)
	contents = strings.ReplaceAll(contents, `"`, `\"`)

	s3filespec, err := dataplatformresource.DeployedArtifactByContents(config.Region, contents, "generated_artifacts/mixpanel/mixpanel_ingestion_table_registry.json")

	parameters = append(parameters, "--s3file", s3filespec.URL())
	parameters = append(parameters, "--catalog", "default")
	parameters = append(parameters, "--schema-name", "mixpanel_samsara")
	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal for region %s", config.Region)
	}

	job := dataplatformresource.JobSpec{
		Name:                         "mixpanel_daily_ingestion",
		Region:                       config.Region,
		Owner:                        team.DataEngineering,
		Script:                       mixpanelScript,
		SparkVersion:                 sparkversion.MixpanelIngestionDbrVersion,
		Parameters:                   parameters,
		Profile:                      tf.LocalId("instance_profile").Reference(),
		Cron:                         "0 0 11 * * ?",
		MaxWorkers:                   MixpanelMaxWorkers,
		SparkConf:                    map[string]string{},
		DriverNodeType:               "rd-fleet.xlarge",
		WorkerNodeType:               "rd-fleet.xlarge",
		EmailNotifications:           emailAlerts,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		AdminTeams:                   []components.TeamInfo{},
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.Mixpanel,
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
	}

	jobResource, err := job.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := job.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, s3filespec, jobResource, permissionsResource)

	return resources, nil
}

func mixpanelCreateTableJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	mixpanelScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/mixpanel/mixpanel_create_table.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, mixpanelScript)

	emailAlerts := []string{team.DataEngineering.SlackAlertsChannelEmail.Email}

	// Provide the db names and the skip table path.
	var parameters []string
	tableMap := make(map[string]string) // Create an empty map

	for _, table := range MixpanelTableRegistry {
		tableMap[table.S3Directory] = table.TableName
	}

	bytes, err := json.MarshalIndent(tableMap, "", "  ")
	if err != nil {
		return nil, oops.Wrapf(err, "json marshaling of Mixpanel table spec")
	}

	// we need to escape any quotes
	contents := string(bytes)
	contents = strings.ReplaceAll(contents, `"`, `\"`)

	s3filespec, err := dataplatformresource.DeployedArtifactByContents(config.Region, contents, "generated_artifacts/mixpanel/mixpanel_create_table_registry.json")

	parameters = append(parameters, "--s3file", s3filespec.URL())
	parameters = append(parameters, "--catalog", "default")
	parameters = append(parameters, "--schema-name", "mixpanel_samsara")
	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal for region %s", config.Region)
	}

	job := dataplatformresource.JobSpec{
		Name:                         "mixpanel_create_table",
		Region:                       config.Region,
		Owner:                        team.DataEngineering,
		Script:                       mixpanelScript,
		SparkVersion:                 sparkversion.MixpanelIngestionDbrVersion,
		Parameters:                   parameters,
		Profile:                      tf.LocalId("instance_profile").Reference(),
		Cron:                         "0 0 0/3 ? * * *",
		MaxWorkers:                   MixpanelMaxWorkers,
		SparkConf:                    map[string]string{},
		DriverNodeType:               "rd-fleet.xlarge",
		WorkerNodeType:               "rd-fleet.xlarge",
		EmailNotifications:           emailAlerts,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		AdminTeams:                   []components.TeamInfo{},
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.Mixpanel,
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
	}

	jobResource, err := job.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := job.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, s3filespec, jobResource, permissionsResource)

	return resources, nil
}
