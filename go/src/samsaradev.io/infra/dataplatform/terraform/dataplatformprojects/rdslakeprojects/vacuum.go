package rdslakeprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
)

func vacuumProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources, db rdsdeltalake.RegistryDatabase) (*project.Project, error) {
	vacuumResourceGroups, err := rdsVacuumJobResources(config, db)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "rdsvacuum",
		Name:     db.Name,
		ResourceGroups: project.MergeResourceGroups(
			vacuumResourceGroups,
			map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":        remotes.infra,
				"pool_remote":         remotes.instancePool,
			},
		),
	}, nil
}

func rdsVacuumJobResources(config dataplatformconfig.DatabricksConfig, db rdsdeltalake.RegistryDatabase) (map[string][]tf.Resource, error) {
	var s3Paths []string
	for _, table := range db.TablesInRegion(config.Region) {
		for _, shard := range db.RegionToShards[config.Region] {
			for _, version := range table.VersionInfo.VersionsParquet() {
				s3Paths = append(s3Paths, fmt.Sprintf("s3://%srds-delta-lake/table-parquet/%s/", awsregionconsts.RegionPrefix[config.Region], db.TableS3PathNameWithVersion(shard, table.TableName, version)))
			}
		}
	}

	// It's hard to get a job name for each job the way its set up now, so let's just pick an AZ for all the vacuum jobs
	retentionDays := int64(28)
	timeoutHours := int(1)

	az := dataplatformresource.JobNameToAZ(config.Region, "rdsmerge-vacuum-jobs")
	driverIdent := getPoolIdentifiers(config.Region, false)[az].DriverPoolIdentifier
	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		s3Paths, config, fmt.Sprintf("rds-vacuum-%s", db.Name),
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolName: driverIdent,
			},
		},
		dataplatformconsts.RdsDeltaLakeIngestionVacuum, retentionDays, timeoutHours)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}

	resourceGroups := map[string][]tf.Resource{
		"vacuum": resources,
	}

	return resourceGroups, nil
}
