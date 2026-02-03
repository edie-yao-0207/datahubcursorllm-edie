package amundsen

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/team"
)

// amundsenDatabuilderVersion is the version of the amundsen-databuilder pypi that we are currently running
// https://pypi.org/project/amundsen-databuilder/
// this is an intentional legacy use of the Amundsen Databuilder package.
const AmundsenDatabuilderPackageString = "amundsen-databuilder==6.6.0"

func dbxResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var resources []tf.Resource

	dataPreviewGeneratorResources, err := deltaDataPreviewResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating delta data preview generator resources")
	}
	resources = append(resources, dataPreviewGeneratorResources...)

	return map[string][]tf.Resource{
		"databricks": resources,
	}, nil
}

func deltaDataPreviewResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/datahub/delta_data_preview_generator.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error generating deployed s3 artifact for delta data preview script")
	}
	resources = append(resources, script)

	datahubClusterProfileArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, "datahub-metadata-extraction-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "error generating instance profile arn for datahub")
	}

	jobName := "datahub_delta_data_preview_generator"
	task := &databricks.JobTaskSettings{
		TaskKey: jobName,
		SparkPythonTask: &databricks.SparkPythonTask{
			PythonFile: script.URL(),
		},
		TimeoutSeconds: 43200, // 12 hours. This job can take long when it regens previews.
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no job owner for region %s", config.Region)
	}
	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	overrideSparkConf := map[string]string{}

	overrideSparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
	overrideSparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

	jobSpec := dataplatformresource.JobSpec{
		Name:              jobName,
		Region:            config.Region,
		Tags:              awsresource.ConvertAWSTagsToTagMap(getAmundsenTags("delta-data-preview-generator")),
		Owner:             team.DataPlatform,
		Script:            script,
		RnDCostAllocation: 1,
		Libraries: dataplatformresource.JobLibraryConfig{
			PyPIs: []dataplatformresource.PyPIName{
				AmundsenDatabuilderPackageString,
			},
		},
		EmailNotifications: []string{
			team.DataPlatform.SlackAlertsChannelEmail.Email,
			"dev-data-platform@samsara.com",
		},
		DriverNodeType:               "rd-fleet.2xlarge",
		WorkerNodeType:               "md-fleet.2xlarge",
		MinWorkers:                   0,
		MaxWorkers:                   16,
		JobType:                      dataplatformconsts.Datahub,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Profile:                      datahubClusterProfileArn,
		SparkVersion:                 sparkversion.DeltaDataPreviewGeneratorDbrVersion,
		SparkConf: dataplatformresource.SparkConf{
			Overrides: overrideSparkConf,
		}.ToMap(),
		SparkEnvVars: map[string]string{
			"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "amundsen-metadata-readwrite",
		},
		// Re-enabling for a small group of databases after disabling for 2 months.
		// see: https://samsara.atlassian-us-gov-mod.net/browse/DATAPLAT-800.
		Cron:   "0 0 0 ? * * *", // Run job once a day.
		Format: "MULTI_TASK",
		Tasks:  []*databricks.JobTaskSettings{task},
		JobTags: map[string]string{
			"format": "MULTI_TASK",
		},
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := jobSpec.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "error generating job tf resources for delta data preview generator job")
	}
	resources = append(resources, jobResource)

	jobPermissions, err := jobSpec.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "error generating permissions tf resources for delta data preview generator job")
	}
	resources = append(resources, jobPermissions)

	return resources, nil
}
