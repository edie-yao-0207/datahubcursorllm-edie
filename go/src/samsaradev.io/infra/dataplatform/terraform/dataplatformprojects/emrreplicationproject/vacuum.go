package emrreplicationproject

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
)

func emrVacuumProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources, entity emrreplication.EmrReplicationSpec, cells []string, jobSuffix string) (*project.Project, error) {
	resourceGroups, err := buildEntityJob(config, entity, cells, jobSuffix)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emrvacuum",
		Name:     fmt.Sprintf("%s-%s", entity.Name, jobSuffix),
		ResourceGroups: project.MergeResourceGroups(
			resourceGroups,
			map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":        remotes.infra,
				"pool_remote":         remotes.instancePool,
			},
		),
	}, nil
}

func buildEntityJob(config dataplatformconfig.DatabricksConfig, entity emrreplication.EmrReplicationSpec, cells []string, jobSuffix string) (map[string][]tf.Resource, error) {
	retentionDays := int64(7)
	timeoutHours := int(12)

	az := dataplatformresource.JobNameToAZ(config.Region, "emr-vacuum-jobs")
	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		gatherEntityS3Paths(cells, config.Region, strings.ToLower(entity.Name)),
		config,
		fmt.Sprintf("emr-vacuum-%s-minibatch-%s", strings.ToLower(entity.Name), jobSuffix),
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolName: getPoolIdentifiers(config.Region, false)[az].DriverPoolIdentifier,
			},
		},
		dataplatformconsts.EmrDeltaLakeIngestionVacuum,
		retentionDays,
		timeoutHours,
	)

	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}

	resourceGroups := map[string][]tf.Resource{
		"vacuum": resources,
	}

	return resourceGroups, nil
}

func gatherEntityS3Paths(cells []string, region string, entityName string) []string {
	var s3Paths []string
	for _, cell := range cells {
		s3Paths = append(s3Paths, buildEntityS3Path(region, cell, entityName))
	}

	return s3Paths
}

func buildEntityS3Path(region string, cell string, tableName string) string {
	return fmt.Sprintf(
		"s3://%semr-replication-delta-lake-%s/table/%s",
		awsregionconsts.RegionPrefix[region],
		cell,
		tableName,
	)
}
