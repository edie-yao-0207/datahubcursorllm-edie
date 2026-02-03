package dataplatformprojects

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func DojoDagsterProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, err
	}

	iamResources := iamResources(config.Region)

	p := &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "dojo-dagster",
		Name:     "permissions",
		ResourceGroups: map[string][]tf.Resource{
			"iam": iamResources,
		},
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil

}

func iamResources(region string) []tf.Resource {

	// Create a role in the dbx that can be assumed by the dagster role in dojo.
	// This role will contain all the permissions necessary for our dagster initial load
	// and reload dags to operate.
	// Also, allow this to be assumed from the main aws account so that the dataplatadmin role
	// can use it.

	dagsterDmsRole := &awsresource.IAMRole{
		Name: "dataplatform-dagster-dms-role-dbx",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": emitters.DAGSTER_DMS_ROLE,
					},
					Effect: "Allow",
				},
				{
					Action: []string{"sts:AssumeRole"},
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.SamsaraAWSAccountID),
					},
					Effect: "Allow",
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       "dagster-dms-role-dbx",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}

	// Dagster needs to be able to manage s3 resources in samsara-rds-delta-lake
	s3Policy := &awsresource.IAMRolePolicy{
		Role: dagsterDmsRole.ResourceId(),
		Name: "dagster-permissions",
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: append([]policy.AWSPolicyStatement{
				{
					Action: []string{
						"s3:*",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%s/table-parquet/*", awsregionconsts.RegionPrefix[region]+"rds-delta-lake"),
						fmt.Sprintf("arn:aws:s3:::%s/s3listcheckpoints/*", awsregionconsts.RegionPrefix[region]+"rds-delta-lake"),
					},
				},
				{
					Action: []string{
						"s3:ListBucket",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[region]+"rds-delta-lake"),
					},
				},
				{
					Action: []string{
						"s3:List*",
						"s3:Get*",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[region]+"dataplatform-deployed-artifacts"),
						fmt.Sprintf("arn:aws:s3:::%s/*", awsregionconsts.RegionPrefix[region]+"dataplatform-deployed-artifacts"),
					},
				},
				{
					Action: []string{
						"secretsmanager:GetSecretValue",
						"secretsmanager:DescribeSecret",
					},
					Effect: "Allow",
					Resource: []string{
						fmt.Sprintf("arn:aws:secretsmanager:%s:%d:secret:dagster-dbx-token*", region, infraconsts.GetDatabricksAccountIdForRegion(region)),
					},
				},
			},

				// Allow the dagster role to read secrets from secrets manager.
				emitters.GenSecretsManagerReadPolicyStatement()...,
			),
		},
	}
	return []tf.Resource{dagsterDmsRole, s3Policy}

}
