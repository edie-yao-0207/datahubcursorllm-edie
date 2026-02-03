// This file recreates all databricks related resources from the data pipelines projects for use in E2. This project should not be built in non-E2 workspaces.
package datapipelineprojects

import (
	"fmt"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

type DbInfo struct {
	Dbname string
	Tables []string
}

func AllDbxProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	var projects []*project.Project

	dataPipelinesDbxProject, err := DataPipelinesDbxProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dataPipelinesDbxProject)

	dataPipelinesDbxRemote, err := dataPipelinesDbxProject.RemoteStateResource(project.RemoteStateResourceOptionalWithDefaults(
		map[string]string{
			"local_datapipelines_nonproduction_driver_pool": "PLACEHOLDER_INVALID_CLUSTER",
		},
	))
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Read the node configurations and extract all the tables
	configs, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		return nil, oops.Wrapf(err, "Error fetching node configurations")
	}

	dbInfos := make(map[string]*DbInfo, len(configs))
	for _, config := range configs {
		fileName := strings.Split(config.Name, ".")
		if len(fileName) != 2 {
			continue
		}
		databaseName := fileName[0]
		tableName := fileName[1]
		if _, ok := dbInfos[databaseName]; !ok {
			dbInfos[databaseName] = &DbInfo{
				Dbname: databaseName,
				Tables: make([]string, 0, len(configs)),
			}
		}
		dbInfos[databaseName].Tables = append(dbInfos[databaseName].Tables, tableName)
	}

	// Create a vaccuum project for every database.
	for _, dbInfo := range dbInfos {
		vacuumP, err := vacuumProject(config, dbInfo, dataPipelinesDbxRemote)
		if err != nil {
			return nil, oops.Wrapf(err, "vacuum project")
		}
		projects = append(projects, vacuumP)
	}

	return projects, nil
}

// dataPipelineInstanceTypes is a list of instance types to use for data
// pipeline drivers and workers. We only use i3 and r5 (and its x86-based
// variants supported by Databricks) types here because we've had memory issues
// using m and c series.
var dataPipelineInstanceTypes = []string{
	"i3",
	"r5d",
	"r5dn",
	"i4i",
}

func DataPipelinesDbxProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	resourceGroups := make(map[string][]tf.Resource)

	azPools := []tf.Resource{}

	// For CA region, use "auto" for automatic availability zone selection to avoid capacity issues
	// For other regions, create pools in each availability zone
	zones := infraconsts.GetDatabricksAvailabilityZones(config.Region)
	if config.Region == infraconsts.SamsaraAWSCARegion {
		zones = []string{"auto"}
	}

	for _, zoneId := range zones {
		ondemandWorker, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-ondemand-worker-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         "i3.4xlarge",
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			OnDemand:                           true,
			RnDCostAllocation:                  0,
			ZoneId:                             zoneId,
		}, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		azPools = append(azPools, ondemandWorker...)

		ondemandDriver, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-ondemand-driver-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         "m5d.xlarge",
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			OnDemand:                           true,
			RnDCostAllocation:                  0,
			ZoneId:                             zoneId,
		}, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		azPools = append(azPools, ondemandDriver...)

		spotWorker, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-spot-worker-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         "i3.4xlarge",
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			RnDCostAllocation:                  0,
			ZoneId:                             zoneId,
		}, config.Region)
		azPools = append(azPools, spotWorker...)

		// Use specific instance types for CA region instead of fleet types
		spotWorkerNodeType := "rd-fleet.4xlarge"
		if config.Region == infraconsts.SamsaraAWSCARegion {
			spotWorkerNodeType = "r6i.4xlarge"
		}

		spotWorkerFleetConfig := dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-spot-worker-fleet-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         spotWorkerNodeType,
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			RnDCostAllocation:                  0,
			ZoneId:                             zoneId,
		}
		if zoneId == "eu-west-1b" {
			spotWorkerFleetConfig.MinIdleInstances = 1
			spotWorkerFleetConfig.IdleInstanceAutoterminationMinutes = 15
		}

		spotWorkerFleet, err := dataplatformresource.InstancePool(spotWorkerFleetConfig, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		azPools = append(azPools, spotWorkerFleet...)

		// Use specific instance types for CA region instead of fleet types
		onDemandWorkerNodeType := "rd-fleet.4xlarge"
		if config.Region == infraconsts.SamsaraAWSCARegion {
			onDemandWorkerNodeType = "r6i.4xlarge"
		}

		onDemandWorkerFleet, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-ondemand-worker-fleet-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         onDemandWorkerNodeType,
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			RnDCostAllocation:                  0,
			OnDemand:                           true,
			ZoneId:                             zoneId,
		}, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		azPools = append(azPools, onDemandWorkerFleet...)

		// Use specific instance types for CA region instead of fleet types
		onDemandDriverNodeType := "rd-fleet.2xlarge"
		if config.Region == infraconsts.SamsaraAWSCARegion {
			onDemandDriverNodeType = "r6i.2xlarge"
		}

		onDemandDriverFleet, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
			Name:                               fmt.Sprintf("datapipelines-production-ondemand-driver-fleet-%s", zoneId),
			MinIdleInstances:                   0,
			NodeTypeId:                         onDemandDriverNodeType,
			IdleInstanceAutoterminationMinutes: 5,
			PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
			Owner:                              team.DataPlatform,
			OnDemand:                           true,
			RnDCostAllocation:                  0,
			ZoneId:                             zoneId,
		}, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		azPools = append(azPools, onDemandDriverFleet...)
	}

	// The following pools are only created in 1 AZ, so we choose the first one
	// from our list. For CA region, use "auto" for automatic zone selection.
	zonesForSinglePool := infraconsts.GetDatabricksAvailabilityZones(config.Region)
	zoneId := zonesForSinglePool[0]
	if config.Region == infraconsts.SamsaraAWSCARegion {
		zoneId = "auto"
	}

	rndNonProductionWorkerPool, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
		Name:                               "datapipelines-nonproduction-worker",
		MinIdleInstances:                   0,
		NodeTypeId:                         "rd-fleet.2xlarge", // uses fleet clusters from DBX
		ZoneId:                             zoneId,
		IdleInstanceAutoterminationMinutes: 5,
		PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
		Owner:                              team.DataPlatform,
		RnDCostAllocation:                  1,
	}, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	rndNonProductionDriverPool, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
		Name:                               "datapipelines-nonproduction-driver",
		MinIdleInstances:                   0,
		NodeTypeId:                         "rd-fleet.2xlarge", // uses fleet clusters from DBX
		IdleInstanceAutoterminationMinutes: 5,
		PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
		Owner:                              team.DataPlatform,
		OnDemand:                           true,
		RnDCostAllocation:                  1,
		ZoneId:                             zoneId,
	}, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	remotePoolResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "datapipelines_nonproduction_driver_pool",
			Value: fmt.Sprintf(`"%s"`, rndNonProductionDriverPool[0].ResourceId().ReferenceAttr("id")),
		},
	}

	resourceGroups["datapipelines_instance_pool"] = remotePoolResources

	dataPipelinesDailyJobWorkerPool, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
		Name:                               "datapipelines-daily-job-worker",
		MinIdleInstances:                   0,
		NodeTypeId:                         "rd-fleet.4xlarge",
		ZoneId:                             zoneId,
		IdleInstanceAutoterminationMinutes: 5,
		PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
		Owner:                              team.DataPlatform,
		RnDCostAllocation:                  1,
	}, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	dataPipelinesDailyJobDriverPool, err := dataplatformresource.InstancePool(dataplatformresource.InstancePoolConfig{
		Name:                               "datapipelines-daily-job-driver",
		MinIdleInstances:                   0,
		NodeTypeId:                         "m5d.2xlarge",
		IdleInstanceAutoterminationMinutes: 5,
		PreloadedSparkVersion:              sparkversion.DataPipelinesPoolDbrVersion,
		Owner:                              team.DataPlatform,
		OnDemand:                           true,
		RnDCostAllocation:                  1,
		ZoneId:                             zoneId,
	}, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// TODO: i don't believe this is being used as the variable is misspelled
	remoteDailyJobPoolResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "datapipelines_daiy_driver_pool",
			Value: fmt.Sprintf(`"%s"`, dataPipelinesDailyJobDriverPool[0].ResourceId().ReferenceAttr("id")),
		},
	}

	resourceGroups["datapipelines_daily_instance_pool"] = remoteDailyJobPoolResources

	resourceGroups["instance_pool"] = append(rndNonProductionWorkerPool, rndNonProductionDriverPool...)
	resourceGroups["instance_pool"] = append(resourceGroups["instance_pool"], dataPipelinesDailyJobWorkerPool...)
	resourceGroups["instance_pool"] = append(resourceGroups["instance_pool"], dataPipelinesDailyJobDriverPool...)
	resourceGroups["instance_pool"] = append(resourceGroups["instance_pool"], azPools...)

	resourceGroups["databricks_provider"] = dataplatformresource.DatabricksOauthProvider(config.Hostname)

	transformationArtifactResources, sqlTransformationInfo, err := getSqlTransformationInfo(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resourceGroups["sql_transformation_artifacts"] = transformationArtifactResources

	periodicExpectationResources, err := e2DataExpectationPeriodicAudit(sqlTransformationInfo, config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resourceGroups["periodic_data_audit"] = periodicExpectationResources
	p := &project.Project{
		RootTeam:        datapipelinesTerraformProjectName,
		Provider:        providerGroup,
		Class:           "data-pipelines",
		Name:            "e2databricks",
		Group:           "databricks",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}

func e2DataExpectationPeriodicAudit(sqlTransformationInfo sqlTransformationInfo, config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	region := config.Region

	script, err := dataplatformresource.DeployedArtifactObject(region, "python3/samsaradev/infra/dataplatform/datapipelines/audit_expectation.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	for transformationName, s3Files := range sqlTransformationInfo.nodeNameToS3Files {
		if s3Files.ExpectationPath == "" {
			continue
		}
		jsonFile := s3Files.JSONMetadataPath
		if jsonFile == "" {
			return nil, oops.Errorf("Expectation file present, but json metadata file not found for transformation %s. Please check the expectation file is named correctly and that the node exists.", transformationName)
		}

		params := []string{
			"--json-file", jsonFile,
			"--expectation-file", s3Files.ExpectationPath,
		}
		datapipelinesArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, "data-pipelines-cluster")
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		isProduction := sqlTransformationInfo.nodeNameToIsProduction[transformationName]
		rndCostAllocation := float64(1)
		if isProduction {
			rndCostAllocation = 0
		}

		sanitizedName := strings.ReplaceAll(transformationName, ".", "-")
		jobName := fmt.Sprintf("audit-node-%s", sanitizedName)

		// once a day at midnight
		scheduleCron := fmt.Sprintf(
			"0 %d 5 ? * %d *",
			dataplatformresource.RandomFromNameHash(jobName, 0, 30),
			dataplatformresource.JobNameCronDay(jobName))

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no ci service principal app id for region %s", config.Region)
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
			Name:                         jobName,
			Region:                       region,
			Owner:                        s3Files.TeamOwner,
			Script:                       script,
			Parameters:                   params,
			Profile:                      datapipelinesArn,
			Cron:                         scheduleCron,
			TimeoutSeconds:               int(time.Hour.Seconds()),
			JobOwnerServicePrincipalName: ciServicePrincipalAppId,
			RnDCostAllocation:            rndCostAllocation,
			IsProduction:                 isProduction,
			JobType:                      dataplatformconsts.DataPipelinesAudit,
			Format:                       databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
			SparkConf: sparkConf,
			SparkEnvVars: map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "data-pipelines-replication",
			},
			UnityCatalogSetting: ucSettings,
			SparkVersion:        sparkversion.DataPipelinesPoolDbrVersion,
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
	}
	return resources, nil
}
