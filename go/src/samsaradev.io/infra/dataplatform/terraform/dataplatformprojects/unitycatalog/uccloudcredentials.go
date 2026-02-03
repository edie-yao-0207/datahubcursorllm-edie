package unitycatalog

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksaccount"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksserviceprincipals"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

func serviceCredentialResources(providerGroup string) (map[string][]tf.Resource, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	resourceGroups := map[string][]tf.Resource{}

	resources := []tf.Resource{}

	regionalSp, ciServicePrincipalAppID, err := getServicePrincipalInfo(databricksserviceprincipals.CIServicePrincipal, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get CI service principal info")
	}
	regionalSpDataResource := servicePrincipalDataResource(regionalSp)
	resources = append(resources, regionalSpDataResource)

	dataPipelinesRegionalSp, dataPipelinesServicePrincipalAppID, err := getServicePrincipalInfo(databricksserviceprincipals.DataPipelinesLambdaServicePrincipal, config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get data pipelines service principal info")
	}
	dataPipelinesRegionalSpDataResource := servicePrincipalDataResource(dataPipelinesRegionalSp)
	resources = append(resources, dataPipelinesRegionalSpDataResource)

	for _, credential := range unitycatalog.AllCloudCredentials(config) {
		roleName := dataplatformconsts.CloudCredentialIamRoleName(credential.Name)
		roleArn, err := dataplatformresource.IAMRoleArn(config.DatabricksProviderGroup, roleName)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to build role arn")
		}

		credentialResource := databricksresource_official.DatabricksCredential{
			Name:    credential.Name,
			Purpose: databricksresource_official.CredentialPurposeService,
			AwsIamRole: databricksresource_official.AwsIamRole{
				RoleArn: roleArn,
			},
			Owner: ciServicePrincipalAppID,
		}
		resources = append(resources, &credentialResource)

		grantSpecs := make([]databricksresource_official.GrantSpec, 0, len(credential.TeamsWithAccess))
		for _, teamInfo := range credential.TeamsWithAccess {
			grantSpecs = append(grantSpecs, databricksresource_official.GrantSpec{
				Principal: teamInfo.DatabricksAccountGroupName(),
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantServiceCredentialAccess,
				},
			})
		}

		// Grant the CI service principal for the respective region ALL_PRIVILEGES access.
		grantSpecs = append(grantSpecs, databricksresource_official.GrantSpec{
			Principal: ciServicePrincipalAppID,
			Privileges: []databricksresource_official.GrantPrivilege{
				databricksresource_official.GrantServiceCredentialAllPrivileges,
			},
		})

		// For the data pipelines replication credential, we need to grant access the Data Pipelines CI service principal and machine user.
		if credential.Name == "data-pipelines-replication" {
			grantSpecs = append(grantSpecs, databricksresource_official.GrantSpec{
				Principal: dataPipelinesServicePrincipalAppID,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantServiceCredentialAccess,
				},
			})

			dataPipelinesMachineUsername, err := dataplatformconfig.GetDataPipelineLambdaUserByRegion(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to get data pipelines machine user")
			}
			grantSpecs = append(grantSpecs, databricksresource_official.GrantSpec{
				Principal: dataPipelinesMachineUsername,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantServiceCredentialAccess,
				},
			})
		}

		grant := &databricksresource_official.DatabricksGrants{
			ResourceName: fmt.Sprintf("credential_%s", credentialResource.Name),
			Credential:   credentialResource.ResourceId().Reference(),
			Grants:       grantSpecs,
		}
		resources = append(resources, grant)

		if credential.SelfAssumingRole {
			// External ID is the same for storage credentials and cloud credentials.
			externalId, err := dataplatformconsts.GetCredentialExternalId(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "getting storage credential external id")
			}

			statements := []policy.AWSPolicyStatement{
				// Allow databricks to assume role, from a specific role on their end and
				// with a specific externalid provided.
				{
					Principal: map[string]string{
						"AWS": dataplatformconsts.DatabricksUCAwsRoleReference,
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"sts:ExternalId": externalId,
						},
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
				},
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", config.DatabricksAWSAccountId),
					},
					Condition: &policy.AWSPolicyCondition{
						ARNLike: &policy.ARNLike{
							PrincipalARN: roleArn,
						},
					},
					Effect: "Allow",
					Action: []string{
						"sts:AssumeRole",
					},
				},
			}

			cloudCredentialRole := &awsresource.IAMRole{
				Name:        roleName,
				Description: "Cloud Credential Role assumed by databricks to access these cloud resources.",
				AssumeRolePolicy: policy.AWSPolicy{
					Version:   policy.AWSPolicyVersion,
					Statement: statements,
				},
				Tags: map[string]string{
					"samsara:service":       "databricks-cloud-credential-role",
					"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
					"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				},
			}

			selfAssumingPolicy := &awsresource.IAMRolePolicy{
				Name: "self-assuming-policy",
				Role: cloudCredentialRole.ResourceId(),
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						{
							Effect: "Allow",
							Action: []string{
								"sts:AssumeRole",
							},
							Resource: []string{
								roleArn,
							},
						},
					},
				},
			}

			resources = append(resources, cloudCredentialRole, selfAssumingPolicy)

			policyResources, err := databricksaccount.GetCloudCredentialPolicyResources(credential, *cloudCredentialRole, config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "getting cloud credential policy resources")
			}
			resources = append(resources, policyResources...)
		}
	}
	resourceGroups["service_credentials"] = resources

	return resourceGroups, nil
}
