package dataplatformprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsdynamodbresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/objectstatownership"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	ksdeltalakecomponents "samsaradev.io/service/components/ksdeltalake"
	"samsaradev.io/service/pipelines"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/teamnames"
)

func S3BigStatsDeltaLakeProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// TODO: remove old instance profile once everything is migrated to UC cluster.
	s3BigStatsImportARN, err := dataplatformresource.InstanceProfileArn(providerGroup, "s3bigstats-import-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	unityCatalogArn, err := dataplatformresource.InstanceProfileArn(providerGroup, "s3bigstats-import-uc-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "failed building uc arn")
	}

	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, s3BigStatsImportARN),
		},
		&genericresource.StringLocal{
			Name:  "uc_instance_profile",
			Value: fmt.Sprintf(`"%s"`, unityCatalogArn),
		},
	}

	poolResources, err := S3BigStatsInstancePoolResources(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating instance pool resources")
	}
	var allPoolResources []tf.Resource
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetOndemand...)

	pipelineResources, err := s3BigStatsMergePipelineResources(config, poolResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating merge pipeline resources")
	}

	vacuumResources, err := s3BigStatsVacuumResource(config, poolResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating vacuum resources")
	}

	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "s3bigstatsmerge",
		ResourceGroups: map[string][]tf.Resource{
			"pool":                allPoolResources,
			"pipeline":            pipelineResources,
			"vacuum":              vacuumResources,
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
		GenerateOutputs: true,
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
		"infra_remote": instanceProfileResources,
	})

	backFillProject := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeIngestionTerraformProjectPipeline,
		Provider: config.AWSProviderGroup,
		Class:    "s3bigstatsbackfill",
		ResourceGroups: map[string][]tf.Resource{
			"dynamo": backfillDynamoResources(),
		},
		GenerateOutputs: true,
	}

	backFillProject.ResourceGroups = project.MergeResourceGroups(backFillProject.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(backFillProject),
		"tf_backend":   resource.ProjectTerraformBackend(backFillProject),
	})

	return []*project.Project{p, backFillProject}, nil
}

func backfillDynamoResources() []tf.Resource {
	dynamoTable := &awsdynamodbresource.DynamoDBTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					PreventDestroy: true,
				},
			},
		},
		Name:        "s3bigstats-backfill-state",
		BillingMode: awsdynamodbresource.PayPerRequest,
		HashKey:     "bigstat_name",
		Attributes: []awsdynamodbresource.DynamoDBTableAttribute{
			{
				KeyName: "bigstat_name",
				Type:    awsdynamodbresource.String,
			},
		},
		Tags: map[string]string{
			"Name":                   "s3bigstats-backfill-state",
			"samsara:service":        "s3bigstats-backfill-state",
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:rnd-allocation": "1",
		},
		// deletion protection is not possible on this table currently due to
		// terraform-aws-project-datalake-ks-ingestion terraform version being on 0.12.31
		// https://buildkite.com/samsara/terraform-aws-project-datalake-ks-ingestion/settings/steps
		// TODO: enable deletion protection when the terraform version is updated
		// DeletionProtectionEnabled: true,
	}
	return []tf.Resource{dynamoTable}
}

func s3BigStatsMergePipelineResources(config dataplatformconfig.DatabricksConfig, poolResources InstancePoolTfResources) ([]tf.Resource, error) {
	jobs, resources, err := S3BigStatsDatabricksJobSpecs(config, poolResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error getting s3bigstats databricks job specs")
	}

	// Get the schema resource and add it to the parameters passed in
	region := config.Region
	for _, job := range jobs {
		schema, err := dataplatformresource.DeployedArtifactObject(region, job.SchemaPath)
		if err != nil {
			return nil, oops.Wrapf(err, "error getting schema resource")
		}
		job.DatabricksJobSpec.Parameters = append(job.DatabricksJobSpec.Parameters, "--schema-file", schema.URL())
		resources = append(resources, schema)
	}
	// Get the python script resource and add it to the job spec
	mergeScript, err := dataplatformresource.DeployedArtifactObjectNoHash(region, "python3/samsaradev/infra/dataplatform/ksdeltalake/s3bigstats_merge.py")
	if err != nil {
		return nil, oops.Wrapf(err, "error getting s3bigstats merge script")
	}
	resources = append(resources, mergeScript)
	for _, job := range jobs {
		job.DatabricksJobSpec.Script = mergeScript
	}

	for _, job := range jobs {
		jobResource, err := job.DatabricksJobSpec.TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job resource")
		}
		// Add this line if we no longer want terraforn to manage the job
		// jobResource.Lifecycle.IgnoreChanges = []string{tf.IgnoreAllChanges}
		permissionsResource, err := job.DatabricksJobSpec.PermissionsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		resources = append(resources, jobResource, permissionsResource)
	}
	return resources, nil
}

type InstancePoolTfResources struct {
	NonproductionFleetWorker []tf.Resource
	NonproductionFleetDriver []tf.Resource
	ProductionFleetWorker    []tf.Resource
	ProductionFleetDriver    []tf.Resource
	ProductionFleetOndemand  []tf.Resource
}

func S3BigStatsInstancePoolResources(region string) (InstancePoolTfResources, error) {
	nodeSize := "4xlarge"
	if region == infraconsts.SamsaraAWSEURegion || region == infraconsts.SamsaraAWSCARegion {
		nodeSize = "2xlarge"
	}

	driverNodeSize := "xlarge"

	// There are so few s3bigstats jobs that we'll just run them all in one AZ rather than creating
	// a lot of pools in different azs.
	defaultAZ := infraconsts.GetDatabricksAvailabilityZones(region)[0]

	// Actual pools
	nonproductionWorkerFleetPoolConfig := dataplatformresource.InstancePoolConfig{
		Name:                               "s3bigstatsmerge-nonproduction-worker-fleet",
		MinIdleInstances:                   0,
		IdleInstanceAutoterminationMinutes: 5,
		NodeTypeId:                         "rd-fleet." + nodeSize, // uses fleet clusters from DBX
		PreloadedSparkVersion:              sparkversion.S3BigstatsInstancePoolDbrVersion,
		Owner:                              team.DataPlatform,
		RnDCostAllocation:                  1,
		ZoneId:                             defaultAZ,
	}
	nonproductionFleetWorker, err := dataplatformresource.InstancePool(nonproductionWorkerFleetPoolConfig, region)
	if err != nil {
		return InstancePoolTfResources{}, err
	}

	nonproductionDriverFleetPoolConfig := dataplatformresource.InstancePoolConfig{
		Name:                               "s3bigstatsmerge-nonproduction-driver-fleet",
		MinIdleInstances:                   0,
		IdleInstanceAutoterminationMinutes: 5,
		NodeTypeId:                         "rd-fleet." + driverNodeSize, // uses fleet clusters from DBX
		PreloadedSparkVersion:              sparkversion.S3BigstatsInstancePoolDbrVersion,
		Owner:                              team.DataPlatform,
		RnDCostAllocation:                  1,
		OnDemand:                           true,
		ZoneId:                             defaultAZ,
	}
	nonproductionFleetDriver, err := dataplatformresource.InstancePool(nonproductionDriverFleetPoolConfig, region)
	if err != nil {
		return InstancePoolTfResources{}, err
	}

	// For production jobs, we'll create
	// - driver pool using ondemand instances
	// - worker pool using spot instances
	// - ondemand pool using ondemand instances, for outage mitigation.
	productionWorkerFleetPoolConfig := dataplatformresource.InstancePoolConfig{
		Name:                               "s3bigstatsmerge-production-worker-fleet",
		MinIdleInstances:                   0,
		IdleInstanceAutoterminationMinutes: 5,
		NodeTypeId:                         "rd-fleet." + nodeSize, // uses fleet clusters from DBX
		PreloadedSparkVersion:              sparkversion.S3BigstatsInstancePoolDbrVersion,
		Owner:                              team.DataPlatform,
		OnDemand:                           false,
		RnDCostAllocation:                  0,
		ZoneId:                             defaultAZ,
	}
	productionFleetWorker, err := dataplatformresource.InstancePool(productionWorkerFleetPoolConfig, region)
	if err != nil {
		return InstancePoolTfResources{}, err
	}

	productionDriverFleetPoolConfig := dataplatformresource.InstancePoolConfig{
		Name:                               "s3bigstatsmerge-production-driver-fleet",
		MinIdleInstances:                   0,
		IdleInstanceAutoterminationMinutes: 5,
		NodeTypeId:                         "rd-fleet." + driverNodeSize, // uses fleet clusters from DBX
		PreloadedSparkVersion:              sparkversion.S3BigstatsInstancePoolDbrVersion,
		Owner:                              team.DataPlatform,
		OnDemand:                           true,
		RnDCostAllocation:                  0,
		ZoneId:                             defaultAZ,
	}
	productionFleetDriver, err := dataplatformresource.InstancePool(productionDriverFleetPoolConfig, region)
	if err != nil {
		return InstancePoolTfResources{}, err
	}

	productionOndemandFleetPoolConfig := dataplatformresource.InstancePoolConfig{
		Name:                               "s3bigstatsmerge-production-ondemand-fleet",
		MinIdleInstances:                   0,
		IdleInstanceAutoterminationMinutes: 5,
		NodeTypeId:                         "rd-fleet." + nodeSize, // uses fleet clusters from DBX
		PreloadedSparkVersion:              sparkversion.S3BigstatsInstancePoolDbrVersion,
		Owner:                              team.DataPlatform,
		OnDemand:                           true,
		RnDCostAllocation:                  0,
		ZoneId:                             defaultAZ,
	}
	productionFleetOndemand, err := dataplatformresource.InstancePool(productionOndemandFleetPoolConfig, region)
	if err != nil {
		return InstancePoolTfResources{}, err
	}

	return InstancePoolTfResources{
		NonproductionFleetWorker: nonproductionFleetWorker,
		NonproductionFleetDriver: nonproductionFleetDriver,
		ProductionFleetWorker:    productionFleetWorker,
		ProductionFleetDriver:    productionFleetDriver,
		ProductionFleetOndemand:  productionFleetOndemand,
	}, nil
}

func s3BigStatsVacuumResource(config dataplatformconfig.DatabricksConfig, poolResources InstancePoolTfResources) ([]tf.Resource, error) {
	s3BigStatTables := ksdeltalake.AllS3BigStatTables()
	var s3Paths []string
	for _, table := range s3BigStatTables {
		s3Paths = append(s3Paths, ksdeltalake.S3BigStatTableLocation(config.Region, table.Name))
		s3Paths = append(s3Paths, ksdeltalake.S3BigStatS3FilesLocation(config.Region, table.Name))
	}

	driver := poolResources.NonproductionFleetDriver[0].ResourceId()

	retentionDays := int64(7)
	timeoutHours := int(1)

	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		s3Paths, config, "s3bigstats-vacuum",
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolName:       driver.ReferenceAttr("instance_pool_name"),
				PoolResourceId: driver.ReferenceAttr("id"),
			},
		},
		dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionVacuum, retentionDays, timeoutHours)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}
	return resources, nil
}

func S3BigStatsDatabricksJobSpecs(config dataplatformconfig.DatabricksConfig, poolResources InstancePoolTfResources) ([]*dataplatformresource.DatabricksJob, []tf.Resource, error) {
	var resources []tf.Resource
	region := config.Region
	maxWorkers := 8
	queues, err := ksdeltalakecomponents.GenerateDeltaLakeMergeS3BigStatQueues()
	if err != nil {
		return nil, nil, oops.Wrapf(err, "")
	}

	jobs := make([]*dataplatformresource.DatabricksJob, len(ksdeltalake.AllS3BigStatTables()))

	for i, table := range ksdeltalake.AllS3BigStatTables() {
		schemaPath := fmt.Sprintf("go/src/samsaradev.io/infra/dataplatform/ksdeltalake/s3bigstatschemas/%s.sql", table.S3BigStatsName())
		queue, ok := queues[table.Name]
		if !ok {
			return nil, nil, oops.Errorf("queue not found for table %s for s3 big stats merge", table.Name)
		}

		// Temporary hack to keep the osdReportedDeviceConfig description short so that we don't breach the glue entity size limit.
		// TODO: revert this once we have our UC migration complete.
		description := table.BigStatsDescription()
		if table.Name == "osDReportedDeviceConfig" {
			description = "Current config proto in use on a device\n\n Stat sent to samsara cloud when a config changes or on boot.\n\nData Freshness: This table will be 24+ hours fresh... (concatenated)"
		}

		params := []string{
			"--queue", queue.SqsUrl(region),
			"--target", ksdeltalake.S3BigStatTableLocation(config.Region, table.Name),
			"--s3-files-path", ksdeltalake.S3BigStatS3FilesLocation(config.Region, table.Name),
			"--s3-checkpoint-files-path", ksdeltalake.S3BigStatCheckpointLocation(config.Region, table.Name),
			"--s3-dummy-output-files-path", ksdeltalake.S3BigStatStreamOutputLocation(config.Region, table.Name),
			"--table", fmt.Sprintf("s3bigstats.%s", table.Name),
			"--description", description,
		}

		if table.ContinuousIngestion {
			params = append(params, "--continuous-ingestion")
			maxWorkers = 2
		}

		lowerName := strings.ToLower(table.Name)
		teamName, shared := objectstatownership.GetTeamOwnerNameForStatName(lowerName)
		statOwner := team.DataPlatform
		if !shared {
			statOwner = team.TeamByName[teamName]
		}

		jobName := fmt.Sprintf("s3bigstats-merge-%s", table.Name)

		// Production jobs should start within the first 5 min to be in schedule with datapipelines.
		// Nonproduction jobs can run anytime.
		freq := table.GetReplicationFrequencyHour()
		cronHour := fmt.Sprintf("%d/%s", dataplatformresource.RandomFromNameHash(jobName, 0, 3), freq)
		cronMinute := dataplatformresource.RandomFromNameHash(jobName, 0, 60)
		if table.BigStatProduction {
			cronHour = fmt.Sprintf("*/%s", freq)
			cronMinute = dataplatformresource.RandomFromNameHash(jobName, 0, 5)
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

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, nil, oops.Wrapf(err, "no ci service principal app id for region %s", config.Region)
		}

		ucSettings := dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   ciServicePrincipalAppId,
		}
		instanceProfileName := "uc_instance_profile"

		sparkConf := map[string]string{
			// We have been seeing performance problems for certain jobs after upgrading to 11.3. In an attempt to solve
			// these, we are disabling low shuffle merge. In the future we will work with databricks to reenable this.
			"spark.databricks.delta.merge.enableLowShuffle": "false",

			// We set the initial catalog name to default to use Unity Catalog resources by default.
			"spark.databricks.sql.initial.catalog.name": "default",

			// We set the following cache/modtimeCheck settings to false to avoid issues with tables
			// that are too frequently updated so that the job doesn't fail.
			"databricks.loki.fileStatusCache.enabled":              "false",
			"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
			"spark.databricks.scan.modTimeCheck.enabled":           "false",
		}

		sloConfig := table.BigStatsSloConfig()
		job := dataplatformresource.JobSpec{
			Name:               jobName,
			Region:             region,
			Owner:              team.DataPlatform, // Data Platform owns the code for big stats, so mark them as owner despite underlying tables being owned by other teams
			AdminTeams:         adminTeams,
			SparkVersion:       sparkversion.S3BigstatsDbrVersion,
			Parameters:         params,
			MinWorkers:         0,
			MaxWorkers:         maxWorkers,
			Profile:            tf.LocalId(instanceProfileName).Reference(),
			EmailNotifications: []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
			Tags: map[string]string{
				dataplatformconsts.DATABASE_TAG: "s3bigstats",
				dataplatformconsts.TABLE_TAG:    lowerName,
			},
			JobOwnerServicePrincipalName: ciServicePrincipalAppId,
			Libraries: dataplatformresource.JobLibraryConfig{
				PyPIs: []dataplatformresource.PyPIName{
					dataplatformresource.SparkPyPIDatadog,
				},
			},
			// The jobs time out at 12 hours. In continuous mode they run until the timeout. In batch mode it will run
			// until the data is up to date (ingested a datapoint published after the job was started) or the timeout limit is reached
			TimeoutSeconds:    43200,
			RnDCostAllocation: 1,
			IsProduction:      table.BigStatProduction,
			JobType:           dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionMerge,
			Format:            databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
			SparkConf: sparkConf,
			SparkEnvVars: map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "s3bigstats-replication",
			},
			SloConfig:           &sloConfig,
			UnityCatalogSetting: ucSettings,
			RunAs: &databricks.RunAsSetting{
				ServicePrincipalName: ciServicePrincipalAppId,
			},
		}

		// osDAccelerometer job is paused because the S3bigstats merge job is not able to
		// handle the increase in the ingested data volume. Based on the fact that we haven't
		// been able to find any consumers who are using this table, and the job has been costing
		// over 8k per day to try to keep up with the incoming data volume, we have decided to pause the job.
		// For more context: https://app.incident.io/samsara-rd/incidents/1680
		// TODO: remove this once we have a proper solution for osDAccelerometer which could be
		// Option A - Optimize the S3bigstats merge job to handle the increase in the ingested data volume.
		// Option B - Deprecate the table and remove it from the data lake.
		// or some other option that we haven't thought of yet.
		if table.Name != "osDAccelerometer" {
			job.Cron = fmt.Sprintf("0 %d %s * * ?", cronMinute, cronHour)
		}

		// Use the production pool for production jobs, and hybrid pools for nonproduction jobs.
		if table.BigStatProduction {
			worker := poolResources.ProductionFleetWorker[0].ResourceId()
			driver := poolResources.ProductionFleetDriver[0].ResourceId()
			job.Pool = worker.ReferenceAttr("id")
			job.DriverPool = driver.ReferenceAttr("id")
			job.Tags["pool-name"] = worker.ReferenceAttr("instance_pool_name")
			job.Tags["driver-pool-name"] = driver.ReferenceAttr("instance_pool_name")
		} else {
			worker := poolResources.NonproductionFleetWorker[0].ResourceId()
			driver := poolResources.NonproductionFleetDriver[0].ResourceId()
			job.Pool = worker.ReferenceAttr("id")
			job.DriverPool = driver.ReferenceAttr("id")
			job.Tags["pool-name"] = worker.ReferenceAttr("instance_pool_name")
			job.Tags["driver-pool-name"] = driver.ReferenceAttr("instance_pool_name")
		}

		databricksJob := &dataplatformresource.DatabricksJob{
			DatabricksJobSpec: job,
			ScriptPath:        "python3/samsaradev/infra/dataplatform/ksdeltalake/s3bigstats_merge.py",
			SchemaPath:        schemaPath,
			Type:              dataplatformresource.DatabricksJobTypeS3BigStats,
		}

		if jobName == "osDReportedDeviceConfig" {
			databricksJob.RolloutStage = pipelines.DatabricksJobProdRolloutState
		} else {
			databricksJob.RolloutStage = pipelines.DatabricksJobBetaRolloutState
		}
		jobs[i] = databricksJob

	}
	return jobs, resources, nil
}
