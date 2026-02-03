package dataplatformprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// Give DS&S teams access to test workspace to test out databricks features
// Add teams here to give them access to the test workspace
var teamWithAccess = []components.TeamInfo{
	team.DataPlatform,
	team.DataTools,
	team.DataEngineering,
	team.DataAnalytics,
	team.DecisionScience,
}

// ProdWorkspaceProject returns all projects for our test workspace
// Prod is a slight misnomer but maps to the actual URL which is prod-us-west-2.databricks.com
// We use this for testing databricks features
func ProdWorkspaceProject(config dataplatformconfig.DatabricksConfig, providerGroup string) (*project.Project, error) {
	ipAllowList, err := IPAllowList(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	p := project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "infrastructure",
	}

	ciMachineUserResourceGroup := map[string][]tf.Resource{
		"machine_user": {
			&databricksresource.User{
				UserName:    config.MachineUsers.CI.Email,
				DisplayName: config.MachineUsers.CI.Name + " (Managed by Terraform)",
			},
		},
	}

	// clusterName := "test-workspace"
	// clusterResourceMap, err := clusterResources(config, clusterName)
	// if err != nil {
	// 	return nil, oops.Wrapf(err, "error generating cluster resources")
	// }

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		ciMachineUserResourceGroup,
		ipAllowList,
		WorkspaceConf(),
		teamResources(),
		// The follow two resource groups are giving TF grief.
		// Commenting out for now since this was for mulit-task jobs which has since been turned on in the main workspace.
		// ec2Resources(clusterName),
		// clusterResourceMap,
		map[string][]tf.Resource{
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
			"tf_backend":          resource.ProjectTerraformBackend(&p),
		},
	)

	return &p, nil
}

func ec2Resources(clusterName string) map[string][]tf.Resource {
	var resources []tf.Resource

	role := &awsresource.IAMRole{
		ResourceName: "ec2_test_workspace_profile",
		Name:         clusterName,
		Path:         "/ec2-instance/",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:    "Allow",
					Action:    []string{"sts:AssumeRole"},
					Principal: map[string]string{"Service": "ec2.amazonaws.com"},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("%s-role", clusterName),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, role)

	instanceProfile := &awsresource.IAMInstanceProfile{
		ResourceName: "ec2_test_workspace_profile",
		Name:         clusterName,
		Role:         role.ResourceId().Reference(),
	}
	resources = append(resources, instanceProfile)

	return map[string][]tf.Resource{
		"ec2": resources,
	}
}

func teamResources() map[string][]tf.Resource {
	var teamResources []tf.Resource
	uniqueUsers := make(map[string]struct{})
	for _, t := range teamWithAccess {
		teamResources = append(teamResources, generateDatabricksGroupResources(t, uniqueUsers)...)
	}

	return map[string][]tf.Resource{
		"team": teamResources,
	}
}

func generateDatabricksGroupResources(team components.TeamInfo, uniqueUsers map[string]struct{}) []tf.Resource {
	var resources []tf.Resource
	users := []*DatabricksUser{}
	for _, member := range team.Members {
		if _, ok := uniqueUsers[member.Name]; ok {
			continue
		}

		uniqueUsers[member.Name] = struct{}{}
		user := &DatabricksUser{
			Name:               member.Name,
			Email:              member.Email,
			SQLAnalyticsAccess: true,
		}
		resources = append(resources, user.Resource(false))
		users = append(users, user)
	}

	dbxGroup := &DatabricksGroup{
		Name:  team.LegacyDatabricksWorkspaceGroupName(),
		Team:  &team,
		Users: users,
	}
	resources = append(resources, dbxGroup.Resource(false))

	return resources
}

func clusterResources(config dataplatformconfig.DatabricksConfig, clusterName string) (map[string][]tf.Resource, error) {
	instanceProfile, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, instanceProfileName(clusterName))
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	dbxInstanceProfile := &databricksresource.InstanceProfile{
		ResourceName:       "test-cluster-instance-profile",
		InstanceProfileArn: instanceProfile,
		SkipValidation:     true,
	}

	var users []dataplatformresource.DatabricksPrincipal
	for _, team := range teamWithAccess {
		users = append(users, dataplatformresource.TeamPrincipal{Team: team})
	}
	testClusterConfig := dataplatformresource.ClusterConfig{
		Name:                    clusterName,
		Owner:                   team.DataPlatform,
		Users:                   users,
		Region:                  config.Region,
		InstanceProfile:         instanceProfile,
		IncludeDefaultLibraries: true,
	}

	clusterResources, err := dataplatformresource.InteractiveCluster(testClusterConfig)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	clusterResources = append(clusterResources, dbxInstanceProfile)

	return map[string][]tf.Resource{
		"cluster": clusterResources,
	}, nil
}
