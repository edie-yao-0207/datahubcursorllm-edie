package ksdeltalake

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func KinesisStatsCollectorProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	var projects []*project.Project

	poolResources, err := poolProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "building pool projects")
	}
	projects = append(projects, poolResources.projects...)

	ksImportArn, err := dataplatformresource.InstanceProfileArn(providerGroup, "kinesisstats-import-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	unityCatalogArn, err := dataplatformresource.InstanceProfileArn(providerGroup, "kinesisstats-import-uc-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "failed building uc arn")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, ksImportArn),
		},
		&genericresource.StringLocal{
			Name:  "uc_instance_profile",
			Value: fmt.Sprintf(`"%s"`, unityCatalogArn),
		},
	}

	mergeResourceGroups, err := kinesisStatsMergePipeline(config)
	if err != nil {
		return nil, err
	}
	if len(mergeResourceGroups) > 0 {
		projects = append(projects, &project.Project{
			RootTeam: dataplatformterraformconsts.DataLakeIngestionTerraformProjectPipeline,
			Provider: providerGroup,
			Class:    "job",
			Name:     "datacollector",
			Group:    "kinesisstats-merge-stream",
			ResourceGroups: project.MergeResourceGroups(
				map[string][]tf.Resource{
					"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
					"pool":                poolResources.remoteResources,
				},
				mergeResourceGroups,
			),
		})
	}

	vacuum, err := kinesisStatsVacuumPipeline(config)
	if err != nil {
		return nil, err
	}
	if vacuum != nil {
		projects = append(projects, &project.Project{
			RootTeam: dataplatformterraformconsts.DataLakeIngestionTerraformProjectPipeline,
			Provider: providerGroup,
			Class:    "job",
			Name:     "datacollector",
			Group:    "kinesisstats-vacuum",
			ResourceGroups: map[string][]tf.Resource{
				"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"job":                 vacuum,
				"pool":                poolResources.remoteResources,
			},
		})
	}

	// Add ks diffing script
	ksdiffscript, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/tools/ksdifftool.py")
	if err != nil {
		return nil, oops.Wrapf(err, "couldn't create ksdifftool artifact")
	}
	projects = append(projects, &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeIngestionTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "job",
		Name:     "datacollector",
		Group:    "ksdifftool",
		ResourceGroups: map[string][]tf.Resource{
			"ksdifftool": {ksdiffscript},
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
