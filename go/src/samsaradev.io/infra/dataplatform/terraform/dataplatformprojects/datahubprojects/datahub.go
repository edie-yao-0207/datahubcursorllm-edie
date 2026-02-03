package datahubprojects

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

const DatahubResourceBaseName = "datahub"

// https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html
func DatahubAWSProject(databricksProviderGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	vpcResourceGroups, vpcIds, err := vpcResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating datahub vpc resources")
	}

	eksResourceGroups, err := eksResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating datahub eks resources")
	}

	loadBalancerResourceGroups, err := loadBalancerResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating datahub load balancer resources")
	}

	elasticsearchResourceGroups, err := elasticsearchResources(vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating datahub elasticsearch resources")
	}

	rdsResourceGroups, err := rdsResources(config, vpcIds)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating dagster rds resources")
	}

	ssoResourceGroups := ssoResources(config)
	ecrResourceGroups := ecrResources()
	s3ResourceGroups := s3Resources(config)

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDatahubTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           "datahub",
		ResourceGroups: project.MergeResourceGroups(
			vpcResourceGroups,
			eksResourceGroups,
			loadBalancerResourceGroups,
			elasticsearchResourceGroups,
			ssoResourceGroups,
			ecrResourceGroups,
			s3ResourceGroups,
			rdsResourceGroups,
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

// https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html
func DatahubDatabricksProject(databricksProviderGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDatahubTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           "datahub",
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider":        resource.ProjectAWSProvider(p),
			"tf_backend":          resource.ProjectTerraformBackend(p),
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		})

	return p, nil
}

// getDatahubTags generates a slice of tags for a Datahub resource based on the service name
func getDatahubTags(serviceName string) []*awsresource.Tag {
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
			Value: fmt.Sprintf("datahub-%s", serviceName),
		},
		{
			Key:   "samsara:rnd-allocation",
			Value: "1",
		},
	}
}
