package dagsterprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

const DagsterResourceBaseName = "dagster"

// https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html
func DagsterAWSProject(databricksProviderGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	vpcResourceGroups, vpcIds, err := vpcResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster vpc resources")
	}

	eksResourceGroups, err := eksResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster eks resources")
	}

	loadBalancerResourceGroups, err := loadBalancerResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster load balancer resources")
	}

	rdsResourceGroups, err := rdsResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster rds resources")
	}

	ssoResourceGroups := ssoResources(config)
	s3ResourceGroups := s3Resources(config)
	ecrResourceGroups := ecrResources()

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDagsterTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           "dagster",
		ResourceGroups: project.MergeResourceGroups(
			vpcResourceGroups,
			eksResourceGroups,
			loadBalancerResourceGroups,
			rdsResourceGroups,
			ssoResourceGroups,
			s3ResourceGroups,
			ecrResourceGroups,
		),
	}

	awsProviderVersion := "5.25.0"
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
			"tf_backend":   resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
		})

	return p, nil
}

func DagsterDatabricksProject(databricksProviderGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	clusterResourceGroups, err := clusterResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster databricks cluster resources")
	}

	poolResourceGroup, err := clusterPoolResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster databricks pool resources")
	}

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDagsterTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           "dagster",
		ResourceGroups: project.MergeResourceGroups(
			clusterResourceGroups,
			poolResourceGroup,
		),
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider":        resource.ProjectAWSProvider(p),
			"tf_backend":          resource.ProjectTerraformBackend(p),
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		})

	return p, nil
}

// getDagsterTags generates a slice of tags for a Dagster resource based on the service name
func getDagsterTags(serviceName string) []*awsresource.Tag {
	return []*awsresource.Tag{
		{
			Key:   "samsara:team",
			Value: strings.ToLower(team.DataPlatform.TeamName),
		},
		{
			Key:   "samsara:product-group",
			Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		{
			Key:   "samsara:service",
			Value: fmt.Sprintf("dagster-%s", serviceName),
		},
		{
			Key:   "samsara:rnd-allocation",
			Value: "1",
		},
	}
}
