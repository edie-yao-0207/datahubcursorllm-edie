package datapipelineprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func vacuumProject(config dataplatformconfig.DatabricksConfig, dbInfo *DbInfo, dataPipelinesDbxRemote tf.Resource) (*project.Project, error) {
	vacuumResourceGroups, err := dataPipelineVacuumJobResources(config, dbInfo, dataPipelinesDbxRemote)
	if err != nil {
		return nil, oops.Wrapf(err, "Gettingg data pipeline vacuum job resources failed.")
	}

	p := &project.Project{
		RootTeam: datapipelinesTerraformProjectName,
		Provider: config.DatabricksProviderGroup,
		Class:    "datapipelinevacuum",
		Name:     dbInfo.Dbname,
		ResourceGroups: project.MergeResourceGroups(
			vacuumResourceGroups,
			map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
			},
		),
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"tf_backend":   resource.ProjectTerraformBackend(p),
			"aws_provider": resource.ProjectAWSProvider(p),
		},
	)

	return p, nil
}

func dataPipelineVacuumJobResources(config dataplatformconfig.DatabricksConfig, dbInfo *DbInfo, dataPipelinesDbxRemote tf.Resource) (map[string][]tf.Resource, error) {
	s3Paths := make([]string, 0, len(dbInfo.Tables))
	for _, tableName := range dbInfo.Tables {
		s3Paths = append(s3Paths, fmt.Sprintf("s3://%sdata-pipelines-delta-lake/%s/%s/", awsregionconsts.RegionPrefix[config.Region], dbInfo.Dbname, tableName))
	}

	datapipelinesArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, "data-pipelines-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "Getting data-pipelines-cluster arn failed.")
	}

	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, datapipelinesArn),
		},
	}

	retentionDays := int64(7)
	timeoutHours := int(1)

	resources, err := dataplatformprojecthelpers.CreateVacuumResources(
		s3Paths,
		config,
		fmt.Sprintf("data-pipeline-vacuum-%s", dbInfo.Dbname),
		dataplatformprojecthelpers.PoolArguments{
			DriverPool: dataplatformprojecthelpers.IndividualPoolArguments{
				PoolResourceId: dataPipelinesDbxRemote.ResourceId().ReferenceOutput("local_datapipelines_nonproduction_driver_pool"),
			},
		},
		dataplatformconsts.DataPipelinesVacuum,
		retentionDays,
		timeoutHours,
	)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum jobs")
	}

	return map[string][]tf.Resource{
		dbInfo.Dbname:  resources,
		"infra_remote": append(instanceProfileResources, dataPipelinesDbxRemote),
	}, nil
}
