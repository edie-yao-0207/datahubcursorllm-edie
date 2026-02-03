package dataplatformprojects

import (
	"fmt"
	"strconv"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

func E2WorkspaceProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	devProject, err := E2DevWorkspaceProject(providerGroup, config)
	if err != nil {
		return nil, oops.Wrapf(err, "Could not create dev workspace project")
	}
	if config.Region != infraconsts.SamsaraAWSDefaultRegion {
		// Only want EU dev workspace for now
		// EU prod workspace will will come in part 2 of E2 workspace migration
		// RFC: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5339716/RFC:+E2+Migration+Part+1+c6ce
		return []*project.Project{devProject}, nil
	}

	// VPC CIDR range assignments:
	// - dev: 10.48.0.0/16
	// - staging: 10.18.0.0/16
	// - prod-us-west-2: 10.15.0.0/16
	// - prod-eu-west-1: 10.14.0.0/16
	// https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/8608122/VPC+Design+Patterns+c660

	prodWorkspace := dataplatformresource.E2Workspace{
		Environment:                "prod",
		Region:                     config.Region,
		AWSAccountId:               strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
		DatabricksAccountId:        dataplatformconfig.DatabricksAccountId,
		LoggingBucket:              "samsara-databricks-s3-logging",
		VPCCidr16:                  15,
		ProviderGroup:              providerGroup,
		EnableVpcFlowLogs:          true,
		EnableDNSResolverQueryLogs: true,
	}

	prodResources, err := prodWorkspace.Resources()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	prodProject := &project.Project{
		RootTeam:        team.DataPlatform.Name(),
		Provider:        providerGroup,
		Class:           "workspace",
		Name:            "infrastructure",
		Group:           prodWorkspace.Name(),
		ResourceGroups:  prodResources,
		GenerateOutputs: true,
	}

	var projects []*project.Project

	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		// "dev" is our production workspace. We are create "dev-staging"
		// to experiment with new databricks features. For now we are
		// creating the staging workspace resources only in the US region.
		devStageWorkspace := dataplatformresource.E2Workspace{
			Environment:                "dev-staging",
			Region:                     config.Region,
			AWSAccountId:               strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
			DatabricksAccountId:        dataplatformconfig.DatabricksAccountId,
			LoggingBucket:              fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
			VPCCidr16:                  18,
			ProviderGroup:              providerGroup,
			EnableVpcFlowLogs:          true,
			EnableDNSResolverQueryLogs: true,
		}
		devStageResources, err := devStageWorkspace.Resources()
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		devStageProject := &project.Project{
			RootTeam:        team.DataPlatform.Name(),
			Provider:        providerGroup,
			Class:           "workspace",
			Name:            "infrastructure",
			Group:           devStageWorkspace.Name(),
			ResourceGroups:  devStageResources,
			GenerateOutputs: true,
		}

		devStageProject.ResourceGroups = project.MergeResourceGroups(
			devStageProject.ResourceGroups,
			map[string][]tf.Resource{
				"aws_provider": resource.ProjectAWSProvider(devStageProject),
				"tf_backend":   resource.ProjectTerraformBackend(devStageProject),
			},
		)

		projects = append(projects, devStageProject)
	}

	prodProject.ResourceGroups = project.MergeResourceGroups(
		prodProject.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(prodProject),
			"tf_backend":   resource.ProjectTerraformBackend(prodProject),
		},
	)

	projects = append(projects, prodProject, devProject)
	return projects, nil
}

func E2DevWorkspaceProject(providerGroup string, config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	var vpcVal int
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		vpcVal = 48
	} else {
		vpcVal = 49
	}

	devWorkspace := dataplatformresource.E2Workspace{
		Environment:                "dev",
		Region:                     config.Region,
		AWSAccountId:               strconv.Itoa(infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
		DatabricksAccountId:        dataplatformconfig.DatabricksAccountId,
		LoggingBucket:              fmt.Sprintf("%sdatabricks-s3-logging", awsregionconsts.RegionPrefix[config.Region]),
		VPCCidr16:                  vpcVal,
		ProviderGroup:              providerGroup,
		EnableVpcFlowLogs:          true,
		EnableDNSResolverQueryLogs: true,
	}
	devResources, err := devWorkspace.Resources()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	devProject := &project.Project{
		RootTeam:        team.DataPlatform.Name(),
		Provider:        providerGroup,
		Class:           "workspace",
		Name:            "infrastructure",
		Group:           devWorkspace.Name(),
		ResourceGroups:  devResources,
		GenerateOutputs: true,
	}
	devProject.ResourceGroups = project.MergeResourceGroups(
		devProject.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(devProject),
			"tf_backend":   resource.ProjectTerraformBackend(devProject),
		},
	)
	return devProject, nil
}
