package dataplatformprojects

import (
	"encoding/json"
	"fmt"
	"strings"

	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/s3inventories"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

const inventoryDeltaInstanceProfile = "s3-inventory-delta-cluster"

func S3InventoryDeltaProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	profileArn, err := dataplatformresource.InstanceProfileArn(providerGroup, inventoryDeltaInstanceProfile)
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

	jobResources, err := s3InventoryDeltaJob(config)
	if err != nil {
		return nil, err
	}
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "s3inventorydelta",
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

func s3InventoryDeltaJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/s3inventory/s3_inventories_delta.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	emailAlerts := []string{team.TimothyKim.Email}

	var parameters []string

	inventories := s3inventories.GetInventoriesForRegion(config.Region)
	if len(inventories) == 0 {
		// no inventories for this region, so no point in making a Databricks job for this region
		return nil, nil
	}
	bytes, err := json.Marshal(inventories)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to serialize inventory config")
	}

	parameters = append(parameters, "--config", strings.ReplaceAll(string(bytes), "\"", "\\\""))

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no job owner for region %s", config.Region)
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
		Name:                         "s3-inventory-job",
		Region:                       config.Region,
		Owner:                        team.DataPlatform,
		Script:                       script,
		SparkVersion:                 sparkversion.S3InventoryDeltaDbrVersion,
		Parameters:                   parameters,
		Profile:                      tf.LocalId("instance_profile").Reference(),
		MaxWorkers:                   16,
		DriverNodeType:               "rd-fleet.xlarge",
		WorkerNodeType:               "rd-fleet.xlarge",
		Cron:                         "0 0 2 * * ?", // Everyday at 2AM UTC.
		EmailNotifications:           emailAlerts,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		JobType:                      dataplatformconsts.ScheduledNotebook, // eventually change this!
		Format:                       databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		SparkConf: sparkConf,
		SparkEnvVars: map[string]string{
			"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "s3-inventory-read",
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
