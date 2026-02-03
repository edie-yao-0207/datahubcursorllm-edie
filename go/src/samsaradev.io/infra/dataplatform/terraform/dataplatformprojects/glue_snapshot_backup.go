package dataplatformprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

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
)

const GlueBackupMaxWorkers = 1
const GlueBackupInstanceProfile = "dataplatform-cluster"

func GlueBackupProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	glueBackupArn, err := dataplatformresource.InstanceProfileArn(providerGroup, GlueBackupInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, glueBackupArn),
		},
	}

	var projects []*project.Project

	jobResources, err := GlueBackupJob(config)
	if err != nil {
		return nil, err
	}
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "glue-snapshot-backup",
		ResourceGroups: map[string][]tf.Resource{
			"job":                 jobResources,
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

func GlueBackupJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	glueBackupScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/internaltools/glue_snapshot_backup.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, glueBackupScript)

	emailAlerts := []string{team.DataPlatform.SlackAlertsChannelEmail.Email}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	job := dataplatformresource.JobSpec{
		Name:                         "glue-snapshot-backup",
		Region:                       config.Region,
		Owner:                        team.DataPlatform,
		Script:                       glueBackupScript,
		SparkVersion:                 sparkversion.GlueSnapshotBackupDbrVersion,
		Profile:                      tf.LocalId("instance_profile").Reference(),
		Cron:                         "0 0 12 ? * MON",
		MaxWorkers:                   GlueBackupMaxWorkers,
		DriverNodeType:               "m5dn.large",
		WorkerNodeType:               "m5dn.large",
		EmailNotifications:           emailAlerts,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.GlueSnapshotBackup,
		Format:                       databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := job.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource for glue backup")
	}
	permissionsResource, err := job.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, jobResource, permissionsResource)

	return resources, nil
}
