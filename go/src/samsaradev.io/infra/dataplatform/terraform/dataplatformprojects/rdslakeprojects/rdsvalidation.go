package rdslakeprojects

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/team"
)

func validationProjects(config dataplatformconfig.DatabricksConfig, remotes remoteResources, db rdsdeltalake.RegistryDatabase) ([]*project.Project, error) {

	var allProjects []*project.Project
	rdsValidationResources, err := rdsValidationSparkJobResources(config, db)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	allProjects = append(allProjects, &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "rdsvalidation",
		Name:     db.Name,
		ResourceGroups: project.MergeResourceGroups(
			rdsValidationResources, map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":        remotes.infra,
				"pool_remote":         remotes.instancePool,
			},
		),
	})
	return allProjects, nil
}

func rdsValidationSparkJobResources(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) (map[string][]tf.Resource, error) {
	parquetMergeValidationScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/rdsdeltalake/rds_validation.py")
	if err != nil {
		return nil, oops.Wrapf(err, "make s3 object for rds_validation.py")
	}
	resourceGroups := map[string][]tf.Resource{
		"rds_validation_script": {parquetMergeValidationScript},
	}

	sparkConf := map[string]string{
		"spark.databricks.delta.schema.autoMerge.enabled": "true",
	}

	var validationResources []tf.Resource
	dbName := db.Name
	if db.Sharded {
		dbName += "_shards"
	}
	parameters := []string{
		"--dbname", dbName,
	}

	for _, table := range db.TablesInRegion(config.Region) {
		version := table.VersionInfo.VersionsParquet()[0]
		parameters = append(parameters, "--table", table.TableName+"_"+version.Name())
		parameters = append(parameters, "--primarykeys", strings.Join(table.PrimaryKeys, ","))

		// Even if there are no proto fields, we should add this.
		protofields := []string{}
		for field := range table.ProtoSchema {
			protofields = append(protofields, field)
		}

		sort.Strings(protofields)
		parameters = append(parameters, "--protocols", strings.Join(protofields, ","))
	}

	jobName := fmt.Sprintf("rds-validation-%s", db.Name)

	// Choose the correct pool based on region, AZ (derived from job name)
	az := dataplatformresource.JobNameToAZ(config.Region, jobName)
	idents := getPoolIdentifiers(config.Region, false)[az]
	worker := tf.LocalId(idents.WorkerPoolIdentifier)
	driver := tf.LocalId(idents.DriverPoolIdentifier)

	maxWorkers := 4

	merge := dataplatformresource.JobSpec{
		Name:   jobName,
		Region: config.Region,
		// Data Platform owns the code for RDS validation, so mark them as owner despite underlying tables being owned by other teams.
		Owner:              team.DataPlatform,
		SparkVersion:       sparkversion.SparkVersion122xScala212, // Use DBR 12.2+ in order to support proto parsing.
		Script:             parquetMergeValidationScript,
		Parameters:         parameters,
		MinWorkers:         1,
		MaxWorkers:         maxWorkers,
		Profile:            tf.LocalId("instance_profile").Reference(),
		MinRetryInterval:   10 * time.Minute,
		EmailNotifications: []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
		JobOwnerUser:       config.MachineUsers.CI.Email,
		Pool:               worker.ReferenceKey("id"),
		DriverPool:         driver.ReferenceKey("id"),
		Tags: map[string]string{
			"pool-name":        worker.ReferenceKey("name"),
			"driver-pool-name": driver.ReferenceKey("name"),
		},
		SparkConf:         sparkConf,
		RnDCostAllocation: float64(1),
		IsProduction:      false,
		JobType:           dataplatformconsts.RdsDeltaLakeIngestionValidation,
		Libraries: dataplatformresource.JobLibraryConfig{
			PyPIs: []dataplatformresource.PyPIName{
				dataplatformresource.SparkPyPIDatadog,
			},
			Jars: []dataplatformresource.JarName{
				dataplatformresource.SamsaraSparkProtobufJar,
			},
			Mavens: []dataplatformresource.MavenName{
				dataplatformresource.SparkMavenGResearch,
			},
		},
		Format: databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
	}

	mergeValidationJobResource, err := merge.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := merge.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	validationResources = append(validationResources, mergeValidationJobResource, permissionsResource)
	resourceGroups["rds_validation_job"] = validationResources
	return resourceGroups, nil
}
