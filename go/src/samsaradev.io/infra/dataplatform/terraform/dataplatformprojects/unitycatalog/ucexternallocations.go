package unitycatalog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksserviceprincipals"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
)

type ServicePrincipalDataResourceArgs struct {
	DisplayName string `hcl:"display_name"`
}

func servicePrincipalDataResource(displayName string) *genericresource.Data {
	return &genericresource.Data{
		Name: fmt.Sprintf("%s", displayName),
		Type: "databricks_service_principal",
		Args: ServicePrincipalDataResourceArgs{
			DisplayName: displayName,
		},
	}
}

// getServicePrincipalId returns the regional service principal name and the id of the service principal.
// Make sure to create the service principal data resource after calling this function.
func getServicePrincipalInfo(displayName string, region string) (string, string, error) {
	sp := databricksserviceprincipals.ServicePrincipal{
		DisplayName: displayName,
	}
	regionalSp, err := sp.RegionalServicePrincipalName(region)

	if err != nil {
		return "", "", oops.Wrapf(err, "failed to get metrics CI user by region")
	}

	principal := genericresource.DataResourceId("databricks_service_principal", regionalSp).ReferenceAttr("application_id")
	return regionalSp, principal, nil
}

func unityCatalogExternalLocations(providerGroup string) (map[string][]tf.Resource, error) {
	resourceGroups := map[string][]tf.Resource{}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Exit early in case we're not in the dev workspaces. Storage Locations exist a metastore level, but need to be created
	// from a workspace (a requirement of the terraform provider).
	// Storage *credentials* could be created at an account level, but a reference to them is necessary for creating
	// external locations so it's a lot more convenient to create them here.
	if !dataplatformresource.IsE2ProviderGroup(config.DatabricksProviderGroup) {
		return nil, nil
	}

	credentials, err := createStorageCredentialResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating storage credentials")
	}

	resourceGroups["storage_credentials"] = credentials

	return resourceGroups, nil
}

// Helper to create storage credentials and external locations.
func createStorageCredentialResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	dataResourceCreated := make(map[string]bool)

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)

	if err != nil {
		return nil, oops.Wrapf(err, "failed to get CI service principal app id by region")
	}

	// Skip if the region is in the disallowed regions list.
Outer:
	for _, location := range unitycatalog.AllStorageLocations(config) {
		for _, disallowed := range location.DisallowedRegions {
			if disallowed == config.Region {
				continue Outer
			}
		}

		bucketName := awsregionconsts.RegionPrefix[config.Region] + location.UnprefixedBucketName

		// Make the storage location, if necessary
		var roleArn string
		var err error

		if location.ExternalIAMRoleArn != "" {
			roleArn = location.ExternalIAMRoleArn
		} else {
			roleName := dataplatformconsts.StorageCredentialIamRoleName(bucketName)
			roleArn, err = dataplatformresource.IAMRoleArn(config.DatabricksProviderGroup, roleName)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to build role arn")
			}
		}

		owner := ciServicePrincipalAppId

		credential := &databricksresource_official.StorageCredential{
			Name: bucketName,
			AwsIamRole: databricksresource_official.AwsIamRole{
				RoleArn: roleArn,
			},
			Owner: owner,
		}

		if location.OwnerGroup != "" {
			owner = location.OwnerGroup
		}

		externalLocation := &databricksresource_official.ExternalLocation{
			Name:           bucketName,
			Url:            fmt.Sprintf("s3://%s", bucketName),
			CredentialName: credential.ResourceId().Reference(),
			Owner:          owner,

			// Validation tends to fail on these even if they work.
			// We need to work with databricks to understand exactly
			SkipValidation: true,
			ReadOnly:       location.ReadOnly,
		}

		Privileges := location.Privileges

		if len(Privileges) > 0 {
			grants := []databricksresource_official.GrantSpec{}
			// Get sorted principals to ensure consistent order.
			principals := make([]string, 0, len(Privileges))

			for principal := range Privileges {
				principals = append(principals, principal)
			}
			sort.Strings(principals)

			for _, principal := range principals {
				privileges := Privileges[principal]
				// Since pipeline users and CI users differ by region,
				// we need to create these "principal" names here,
				// but in future we plan to support group names like firmware-group or datatools-group as well
				if principal == "datapipelines-lambda" {
					dataPipelinesUser, err := dataplatformconfig.GetDataPipelineLambdaUserByRegion(config.Region)
					if err != nil {
						return nil, oops.Wrapf(err, "failed to get data pipeline lambda user by region")
					}
					principal = dataPipelinesUser
				} else if principal == "datapipelines-lambda-service-principal" {
					regionalSp, principalId, err := getServicePrincipalInfo(databricksserviceprincipals.DataPipelinesLambdaServicePrincipal, config.Region)
					if err != nil {
						return nil, oops.Wrapf(err, "failed to get data pipelines lambda service principal info")
					}
					if !dataResourceCreated[regionalSp] {
						regionalSpDataResource := servicePrincipalDataResource(regionalSp)
						resources = append(resources, regionalSpDataResource)
						dataResourceCreated[regionalSp] = true
					}
					principal = principalId
				} else if principal == "ci" {
					principal = owner
				} else if principal == "ci-service-principal" {
					regionalSp, principalId, err := getServicePrincipalInfo(databricksserviceprincipals.CIServicePrincipal, config.Region)
					if err != nil {
						return nil, oops.Wrapf(err, "failed to get CI service principal info")
					}
					if !dataResourceCreated[regionalSp] {
						regionalSpDataResource := servicePrincipalDataResource(regionalSp)
						resources = append(resources, regionalSpDataResource)
						dataResourceCreated[regionalSp] = true
					}
					principal = principalId
				} else if principal == "metrics-ci-user" {
					metricsUser, err := dataplatformconfig.GetMetricsCIUserByRegion(config.Region)
					if err != nil {
						return nil, oops.Wrapf(err, "failed to get metrics CI user by region")
					}
					principal = metricsUser
				} else if principal == "metrics-service-principal" {
					regionalSp, principalId, err := getServicePrincipalInfo(databricksserviceprincipals.MetricsServicePrincipal, config.Region)
					if err != nil {
						return nil, oops.Wrapf(err, "failed to get metrics service principal info")
					}
					if !dataResourceCreated[regionalSp] {
						regionalSpDataResource := servicePrincipalDataResource(regionalSp)
						resources = append(resources, regionalSpDataResource)
						dataResourceCreated[regionalSp] = true
					}
					principal = principalId
				} else { // TODO: handle group names like firmware-group or datatools-group
					return nil, oops.Wrapf(err, "invalid principal: %s", principal)
				}

				// Collect grants for each bucket.
				grants = append(grants, databricksresource_official.GrantSpec{Principal: principal, Privileges: privileges})
			}

			// Today CI user all the permissions to all external locations because its the owner.
			// We cant change the owner of the external locations until rollout is complete.
			// So we are granting all privileges to the ci-service-principal on all external locations.
			// TODO: Remove this once the rollout is complete and change the owner of the external locations.
			// to ci service principal.
			// https://samsara.atlassian-us-gov-mod.net/browse/DAT-414

			grants = append(grants,
				databricksresource_official.GrantSpec{
					Principal: ciServicePrincipalAppId,
					Privileges: []databricksresource_official.GrantPrivilege{
						databricksresource_official.GrantCatalogAllPrivileges,
					},
				})
			grant := &databricksresource_official.DatabricksGrants{
				ResourceName:     bucketName,
				ExternalLocation: bucketName,
				Grants:           grants,
			}
			resources = append(resources, grant)
		} else {
			// If no privileges are set, add the default privileges to the ci-service-principal which gives all privileges.
			grant := &databricksresource_official.DatabricksGrants{
				ResourceName:     bucketName,
				ExternalLocation: bucketName,
				Grants: []databricksresource_official.GrantSpec{
					{
						Principal: ciServicePrincipalAppId,
						Privileges: []databricksresource_official.GrantPrivilege{
							databricksresource_official.GrantCatalogAllPrivileges,
						},
					},
				},
			}
			resources = append(resources, grant)
		}

		resources = append(resources, credential, externalLocation)

		if location.SelfAssumingRole {
			externalId, err := dataplatformconsts.GetCredentialExternalId(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "getting storage credential external id")
			}

			for _, disallowed := range location.DisallowedRegions {
				if disallowed == config.Region {
					continue Outer
				}
			}

			bucketName := awsregionconsts.RegionPrefix[config.Region] + location.UnprefixedBucketName
			bucketArn := fmt.Sprintf("arn:aws:s3:::%s", bucketName)
			roleName := dataplatformconsts.StorageCredentialIamRoleName(bucketName)
			roleArn, err := dataplatformresource.IAMRoleArn(config.DatabricksProviderGroup, roleName)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to build role arn")
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

			storageLocationRole := &awsresource.IAMRole{
				Name:        roleName,
				Description: "Storage Credential Role assumed by databricks to access this bucket.",
				AssumeRolePolicy: policy.AWSPolicy{
					Version:   policy.AWSPolicyVersion,
					Statement: statements,
				},
				Tags: map[string]string{
					"samsara:service":       "databricks-storage-credential-role",
					"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
					"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
				},
			}

			s3Access := &awsresource.IAMRolePolicy{
				Name: "s3-access",
				Role: storageLocationRole.ResourceId(),
				Policy: policy.AWSPolicy{
					Version: policy.AWSPolicyVersion,
					Statement: []policy.AWSPolicyStatement{
						{
							Effect: "Allow",
							Action: []string{
								"s3:GetObject",
								"s3:PutObject",
								"s3:DeleteObject",
								"s3:ListBucket",
								"s3:GetBucketLocation",
							},
							Resource: []string{
								bucketArn,
								fmt.Sprintf("%s/*", bucketArn),
							},
						},
					},
				},
			}

			selfAssumingPolicy := &awsresource.IAMRolePolicy{
				Name: "self-assuming-policy",
				Role: storageLocationRole.ResourceId(),
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

			resources = append(resources, storageLocationRole, s3Access, selfAssumingPolicy)
		}
	}

	return resources, nil
}
