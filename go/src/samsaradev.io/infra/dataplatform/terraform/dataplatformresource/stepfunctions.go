package dataplatformresource

import (
	"fmt"
	"sort"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func StepFunctionRole(stepFunctionName string, policyArns []string, policyStatements []policy.AWSPolicyStatement) (tf.ResourceId, []tf.Resource) {
	var resources []tf.Resource
	roleName := "sfn-" + stepFunctionName
	sfnRole := &awsresource.IAMRole{
		ResourceName: roleName,
		Name:         util.HashTruncate(roleName, 64, 8, ""),
		Description:  fmt.Sprintf("Role assumed when running %s step function", stepFunctionName),
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Principal: map[string]string{
						"Service": "states.amazonaws.com",
					},
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("%s-role", roleName),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	resources = append(resources, sfnRole)

	if len(policyStatements) > 0 {
		customPolicy := &awsresource.IAMRolePolicy{
			Role: sfnRole.ResourceId(),
			Name: "sfn-custom-" + stepFunctionName,
			Policy: policy.AWSPolicy{
				Version:   policy.AWSPolicyVersion,
				Statement: policyStatements,
			},
		}
		resources = append(resources, customPolicy)
	}

	sort.Strings(policyArns)
	for i, policyArn := range policyArns {
		attachment := &awsresource.IAMRolePolicyAttachment{
			Name:      fmt.Sprintf("sfn-%02d-%s", i, stepFunctionName),
			Role:      sfnRole.ResourceId().Reference(),
			PolicyARN: policyArn,
		}
		resources = append(resources, attachment)
	}

	return sfnRole.ResourceId(), resources
}
