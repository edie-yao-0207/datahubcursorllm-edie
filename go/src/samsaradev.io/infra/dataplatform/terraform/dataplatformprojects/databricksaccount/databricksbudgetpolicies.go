package databricksaccount

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/team"
)

func getTeamBasedTags(teamName string) []databricksresource_official.CustomTag {

	productGroup, ok := team.TeamProductGroup[teamName]
	if !ok {
		productGroup = teamName
	}

	return []databricksresource_official.CustomTag{
		{
			Key:   "samsara:team",
			Value: strings.ToLower(teamName),
		},
		{
			Key:   "samsara:product-group",
			Value: strings.ToLower(productGroup),
		},
		{
			Key:   "samsara:service",
			Value: fmt.Sprintf("databricksserverless-%s", strings.ToLower(teamName)),
		},
		{
			Key:   "samsara:rnd-allocation",
			Value: "1",
		},
	}
}

func DatabricksBudgetPolicyResources() (map[string][]tf.Resource, error) {
	resources := make([]tf.Resource, 0)

	for _, t := range clusterhelpers.DatabricksClusterTeams() {

		policyName := fmt.Sprintf("%s-budget-policy", strings.ToLower(t.TeamName))
		policy := &databricksresource_official.DatabricksBudgetPolicy{
			ResourceName: policyName,
			PolicyName:   policyName,
			BindingWorkspaceIds: []int{
				dataplatformconsts.DatabricksDevUSWorkspaceId,
				dataplatformconsts.DatabricksDevEUWorkspaceId,
				dataplatformconsts.DatabricksDevCAWorkspaceId,
			},
			CustomTags: getTeamBasedTags(t.TeamName),
		}
		resources = append(resources, policy)

		group := &databricksresource_official.Group{
			DisplayName: t.DatabricksAccountGroupName(),
		}

		accessControlRuleSetName := fmt.Sprintf("accounts/%s/budgetPolicies/%s/ruleSets/default", dataplatformconfig.DatabricksAccountId, policy.ResourceId().ReferenceAttr("policy_id"))

		accessControlRuleSet := &databricksresource_official.DatabricksAccessControlRuleSet{
			ResourceName: policyName + "-access-control-rule-set",
			Name:         accessControlRuleSetName,
			GrantRules: []databricksresource_official.GrantRule{

				{
					Principals: []string{group.ResourceId().ReferenceAttr("acl_principal_id")},
					Role:       "roles/budgetPolicy.user",
				},
			},
		}
		resources = append(resources, accessControlRuleSet)
	}

	resources = append(resources, DataPlatformAutomatedJobBudgetPolicyResources()...)

	return map[string][]tf.Resource{
		"databricks_budget_policies": resources,
	}, nil
}

// DataPlatformAutomatedJobBudgetPolicyResources creates a default budget policy for the data platform automated job.
// These are to be used for all automated serverless jobs running in the data platform.
func DataPlatformAutomatedJobBudgetPolicyResources() []tf.Resource {
	resources := make([]tf.Resource, 0)

	// We just need to add a default tag to identify the job as a serverless job,
	// rest of the tags should be added by the job itself.
	// This is because anything we add here will override the job's tags
	// and we don't want to override the job's tags.
	// But we need a default policy so that cost allocation is done correctly.
	tags := []databricksresource_official.CustomTag{
		{
			Key:   "samsara:dataplatform-automated-serverless-job",
			Value: "true",
		},
	}

	policyName := "dataplatform-budget-policy-automated-job"
	policy := &databricksresource_official.DatabricksBudgetPolicy{
		ResourceName: policyName,
		PolicyName:   policyName,
		BindingWorkspaceIds: []int{
			dataplatformconsts.DatabricksDevUSWorkspaceId,
			dataplatformconsts.DatabricksDevEUWorkspaceId,
			dataplatformconsts.DatabricksDevCAWorkspaceId,
		},
		CustomTags: tags,
	}

	group := &databricksresource_official.Group{
		DisplayName: team.DataPlatform.DatabricksAccountGroupName(),
	}

	accessControlRuleSet := &databricksresource_official.DatabricksAccessControlRuleSet{
		ResourceName: policyName + "-access-control-rule-set",
		Name:         fmt.Sprintf("accounts/%s/budgetPolicies/%s/ruleSets/default", dataplatformconfig.DatabricksAccountId, policy.ResourceId().ReferenceAttr("policy_id")),
		GrantRules: []databricksresource_official.GrantRule{

			{
				Principals: []string{group.ResourceId().ReferenceAttr("acl_principal_id")},
				Role:       "roles/budgetPolicy.user",
			},
		},
	}
	resources = append(resources, policy, accessControlRuleSet)

	return resources

}
