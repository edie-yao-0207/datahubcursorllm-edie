package dataplatformprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/team"
	"samsaradev.io/team/teamnames"
)

func SqlEndpoints(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	endpointConfigs := make(map[string]*dataplatformresource.SQLEndpointConfig)
	for _, t := range team.AllTeams {
		var endpointName string
		var billingProductGroup string

		switch t.Name() {
		case teamnames.EngineeringOperations, teamnames.Support, teamnames.ProductManagement, teamnames.QualityAssured, teamnames.BizTechEnterpriseData:
			// High usage teams have their own endpoints.
			endpointName = t.Name()
			billingProductGroup = t.Name()
		default:
			if pg := team.TeamProductGroup[t.Name()]; pg != "" {
				// Only eng teams have product groups.
				endpointName = pg
				billingProductGroup = pg
			} else {
				// Other teams share an endpoint, billed to Platform since it doesn't
				// belong to any single team.
				endpointName = "shared"
				billingProductGroup = team.TeamProductGroup[teamnames.DataPlatform]
			}
		}

		if endpointName == "" {
			return nil, oops.Errorf("team %s does not have an endpoint", t.Name())
		}

		if _, ok := endpointConfigs[endpointName]; !ok {
			endpointConfigs[endpointName] = &dataplatformresource.SQLEndpointConfig{
				Name:                   strings.ToLower(endpointName),
				BillingProductGroup:    billingProductGroup,
				AutoTerminationMinutes: 20,
				RnDCostAllocation:      float64(1),
			}
		}
		endpointConfigs[endpointName].Users = append(endpointConfigs[endpointName].Users, dataplatformresource.TeamPrincipal{Team: t})
	}

	endpoints := make(map[string][]tf.Resource)
	for _, config := range endpointConfigs {
		resources, err := dataplatformresource.SQLEndpoint(c, config)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		endpoints[strings.ToLower(config.Name)] = resources
	}

	dataScienceEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   strings.ToLower(team.DataScience.Name()),
		BillingProductGroup:    team.TeamProductGroup[team.DataScience.Name()],
		AutoTerminationMinutes: 20,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataAnalytics},
			dataplatformresource.TeamPrincipal{Team: team.DataEngineering},
			dataplatformresource.TeamPrincipal{Team: team.DataTools},
			dataplatformresource.TeamPrincipal{Team: team.DataScience},
		},
		RnDCostAllocation: float64(1),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints[team.DataScience.Name()] = dataScienceEndpointResources

	tableauExtractEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "tableau-extract",
		BillingProductGroup:    team.TeamProductGroup[team.DataAnalytics.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataAnalytics},
		},
		RnDCostAllocation: float64(1),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["tableau-extract"] = tableauExtractEndpointResources

	cursorQueryAgentTeams := dataplatformterraformconsts.CursorQueryAgentTeams()
	cursorQueryAgentUsers := make([]dataplatformresource.DatabricksPrincipal, len(cursorQueryAgentTeams))
	for i, t := range cursorQueryAgentTeams {
		cursorQueryAgentUsers[i] = dataplatformresource.TeamPrincipal{Team: t}
	}
	cursorQueryAgentEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "cursor-query-agent",
		BillingProductGroup:    team.TeamProductGroup[team.DataTools.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users:                  cursorQueryAgentUsers,
		RnDCostAllocation:      float64(1),
		ClusterSize:            databricks.ClusterSizeSmall,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["cursor-query-agent"] = cursorQueryAgentEndpointResources

	genieEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "genie",
		BillingProductGroup:    team.TeamProductGroup[team.DataTools.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.AiDev},
			dataplatformresource.TeamPrincipal{Team: team.AiX},
			dataplatformresource.TeamPrincipal{Team: team.DataAnalytics},
			dataplatformresource.TeamPrincipal{Team: team.DataEngineering},
			dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
			dataplatformresource.TeamPrincipal{Team: team.DataScience},
			dataplatformresource.TeamPrincipal{Team: team.DataTools},
			dataplatformresource.TeamPrincipal{Team: team.DecisionScience},
			dataplatformresource.TeamPrincipal{Team: team.Firmware},
			dataplatformresource.TeamPrincipal{Team: team.Hardware},
			dataplatformresource.TeamPrincipal{Team: team.MLCV},
			dataplatformresource.TeamPrincipal{Team: team.MLFirmware},
			dataplatformresource.TeamPrincipal{Team: team.MLInfra},
			dataplatformresource.TeamPrincipal{Team: team.MLInfraAdmin},
			dataplatformresource.TeamPrincipal{Team: team.MLScience},
			dataplatformresource.TeamPrincipal{Team: team.ProductManagement},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeSmall,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["genie"] = genieEndpointResources

	// This is part of a pipeline for serving customer requests.
	gqlEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "gql",
		BillingTeam:            team.DevEcosystem.Name(),
		BillingProductGroup:    team.TeamProductGroup[team.DevEcosystem.Name()],
		AutoTerminationMinutes: 60,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.UserPrincipal{Email: c.MachineUsers.ProdGQL.Email},
			dataplatformresource.TeamPrincipal{Team: team.DevEcosystem},
		},
		ClusterSize:       databricks.ClusterSizeSmall,
		RnDCostAllocation: float64(0),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["gql"] = gqlEndpointResources

	safetyDatasetWorkerEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "safety-dataset-worker",
		BillingTeam:            team.SafetyPlatform.Name(),
		BillingProductGroup:    team.TeamProductGroup[team.SafetyPlatform.Name()],
		AutoTerminationMinutes: 20,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.UserPrincipal{Email: c.MachineUsers.ProdGQL.Email},
			dataplatformresource.TeamPrincipal{Team: team.SafetyPlatform},
		},
		ClusterSize:       databricks.ClusterSizeSmall,
		RnDCostAllocation: float64(1),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["safety-dataset-worker"] = safetyDatasetWorkerEndpointResources

	perfDashboardsEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "perf-dashboards",
		AutoTerminationMinutes: 20,
		BillingTeam:            team.SRE.Name(),
		BillingProductGroup:    team.TeamProductGroup[team.SRE.Name()],
		Users: []dataplatformresource.DatabricksPrincipal{
			// The CI user is the user that will create, modify, and own SQL dashboard resources via terraform.
			// It must have permissions to the SQL endpoint in order to attach SQL Queries to the endpoint.
			dataplatformresource.UserPrincipal{Email: c.MachineUsers.CI.Email},
			dataplatformresource.TeamPrincipal{Team: team.SRE},
		},
		ClusterSize:       databricks.ClusterSizeSmall,
		RnDCostAllocation: float64(1),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["perf-dashboards"] = perfDashboardsEndpointResources

	goDataLakeEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "godatalake",
		BillingProductGroup:    team.TeamProductGroup[team.DataPlatform.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeMedium,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["godatalake"] = goDataLakeEndpointResources

	safetyRiskEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "safetyrisk",
		BillingProductGroup:    team.TeamProductGroup[team.SafetyRisk.Name()],
		AutoTerminationMinutes: 10,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
			dataplatformresource.TeamPrincipal{Team: team.SafetyRisk},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeMedium,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["safetyrisk"] = safetyRiskEndpointResources

	safetyDevboxEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "safetydevbox",
		BillingProductGroup:    team.TeamProductGroup[team.SafetyEventTriage.Name()],
		AutoTerminationMinutes: 10,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
			dataplatformresource.TeamPrincipal{Team: team.SafetyEventTriage},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeMedium,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["safetydevbox"] = safetyDevboxEndpointResources

	safetyProfilesEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "safetyprofiles",
		BillingProductGroup:    team.TeamProductGroup[team.SafetyEventTriage.Name()],
		AutoTerminationMinutes: 10,
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
			dataplatformresource.TeamPrincipal{Team: team.SafetyEventTriage},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeMedium,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["safetyprofiles"] = safetyProfilesEndpointResources

	productGPTSamsaraAssistantEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "productgpt-samsara-assistant",
		BillingProductGroup:    team.TeamProductGroup[team.DataTools.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users: []dataplatformresource.DatabricksPrincipal{
			// TODO: This can be replaced with just the app ID for ProductGPTSamsaraAssistantServicePrincipal
			// However the ProductGPTSamsaraAssistantServicePrincipal is not yet created, so we don't have the app ID yet.
			// Though once we migrate ProductGPT to using OAuth to use the user's credentials, we will need to either:
			// - a) teach the agent to first fetch what SQL warehouses the user has access to, or
			// - b) update the permissions here to allow all users with ProductGPT access to query this SQL warehouse.
			dataplatformresource.TeamPrincipal{Team: team.DataTools},
		},
		RnDCostAllocation: float64(1),
		ClusterSize:       databricks.ClusterSizeLarge,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["productgpt-samsara-assistant"] = productGPTSamsaraAssistantEndpointResources

	// Build users list for safety-firmware-dashboards endpoint: Firmware team + all SafetyProductGroup teams
	safetyFirmwareDashboardsUsers := []dataplatformresource.DatabricksPrincipal{
		dataplatformresource.TeamPrincipal{Team: team.Firmware},
	}
	for _, safetyTeam := range team.ProductGroupTeams["SafetyProductGroup"] {
		safetyFirmwareDashboardsUsers = append(safetyFirmwareDashboardsUsers, dataplatformresource.TeamPrincipal{Team: *safetyTeam})
	}

	safetyFirmwareDashboardsEndpointResources, err := dataplatformresource.SQLEndpoint(c, &dataplatformresource.SQLEndpointConfig{
		Name:                   "safety-firmware-dashboards",
		BillingProductGroup:    team.TeamProductGroup[team.Firmware.Name()],
		AutoTerminationMinutes: 10, // The minimum is 10.
		Users:                  safetyFirmwareDashboardsUsers,
		RnDCostAllocation:      float64(1),
		ClusterSize:            databricks.ClusterSizeLarge,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	endpoints["safety-firmware-dashboards"] = safetyFirmwareDashboardsEndpointResources

	return endpoints, nil
}

func unityCatalogSqlWarehouseIAMResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	var resources []tf.Resource
	resourceGroup := make(map[string][]tf.Resource)
	clusterName := "sql-endpoint-uc"
	instanceProfileName := instanceProfileName(clusterName)
	resourceBasename := strings.Replace("databricks_"+instanceProfileName, "-", "_", -1)
	workspaceId := dataplatformconsts.GetDevDatabricksWorkspaceIdForRegion(config.Region)
	if workspaceId == 0 {
		return nil, oops.Errorf("no workspace associated with region %s", config.Region)
	}

	role := makeIAMRoleForInstanceProfile(resourceBasename, instanceProfileName, workspaceId)
	roleId := role.ResourceId()
	roleRef := roleId.Reference()
	resources = append(resources, role)

	instanceProfile := &awsresource.IAMInstanceProfile{
		ResourceName: resourceBasename,
		Name:         instanceProfileName,
		Role:         roleRef,
	}
	instanceProfileArn := &genericresource.StringLocal{
		Name:  "cluster_profile_" + strings.Replace(clusterName, "-", "_", -1),
		Value: fmt.Sprintf(`"%s"`, instanceProfile.ResourceId().ReferenceAttr("arn")),
	}

	resources = append(resources, instanceProfile, instanceProfileArn)

	for _, policy := range []string{
		"write_cluster_logs",
		"read_dataplatform_deployed_artifacts",
	} {
		attachmentName := fmt.Sprintf("%s_%s", resourceBasename, policy)
		attachment := &awsresource.IAMRolePolicyAttachment{
			Name:      attachmentName,
			Role:      roleRef,
			PolicyARN: fmt.Sprintf("arn:aws:iam::%d:policy/%s", awsregionconsts.RegionDatabricksAccountID[config.Region], strings.ReplaceAll(policy, "_", "-")),
		}
		resources = append(resources, attachment)
	}

	// Add SES Policy for sending emails.
	resources = append(resources, getSesPolicyResource(resourceBasename, config.Region, roleId))
	resourceGroup["aws_iam_resources"] = resources

	dbxInstanceProfile, err := createDatabricksInstanceProfile(config.DatabricksProviderGroup, clusterName, instanceProfileName, roleId.ReferenceAttr("arn"))
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resourceGroup["databricks_resources"] = []tf.Resource{dbxInstanceProfile}

	return resourceGroup, nil
}

func makeIAMRoleForInstanceProfile(resourceBasename string, instanceProfileName string, workspaceId int) *awsresource.IAMRole {
	return &awsresource.IAMRole{
		ResourceName: resourceBasename,
		Name:         instanceProfileName,
		Path:         "/ec2-instance/",
		AssumeRolePolicy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect:    "Allow",
					Action:    []string{"sts:AssumeRole"},
					Principal: map[string]string{"Service": "ec2.amazonaws.com"},
				},
				{
					Effect:    "Allow",
					Action:    []string{"sts:AssumeRole"},
					Principal: map[string]string{"AWS": "arn:aws:iam::790110701330:role/serverless-customer-resource-role"},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{"sts:ExternalId": fmt.Sprintf("databricks-serverless-%d", workspaceId)},
					},
				},
			},
		},
		Tags: map[string]string{
			"samsara:service":       fmt.Sprintf("%s-role", instanceProfileName),
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
			"samsara:iac-managed":   "true",
		},
	}
}

func getSesPolicyResource(resourceBasename string, region string, roleId tf.ResourceId) tf.Resource {
	// Allow sending email to @samsara.com, @samsara-net.slack.com, and @samsara.pagerduty.com recipients from databricks-alerts.samsara.com domain.
	return &awsresource.IAMRolePolicy{
		ResourceName: resourceBasename + "_ses",
		Name:         "ses",
		Role:         roleId,
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Effect: "Allow",
					Action: []string{
						"ses:SendEmail",
						"ses:SendRawEmail",
					},
					Resource: []string{"*"},
					Condition: &policy.AWSPolicyCondition{
						ForAllValuesStringLike: map[string][]string{
							"ses:Recipients": {
								"*@samsara.com",
								"*@samsara-net.slack.com",
								"*@samsara.pagerduty.com",
							},
						},
						StringEquals: map[string]string{
							"ses:FromAddress": dataplatformresource.DatabricksAlertsSender(region),
						},
					},
				},
			},
		},
	}
}

func SqlEndpointsProject(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	endpoints, err := SqlEndpoints(config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	ucSqlWarehouseIAMResources, err := unityCatalogSqlWarehouseIAMResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	p := &project.Project{
		RootTeam:       team.DataPlatform.Name(),
		Provider:       providerGroup,
		Class:          "sqlendpoints",
		ResourceGroups: endpoints,
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, ucSqlWarehouseIAMResources, map[string][]tf.Resource{
		"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		"tf_backend":          resource.ProjectTerraformBackend(p),
		"aws_provider":        resource.ProjectAWSProvider(p),
	})

	return []*project.Project{p}, nil
}
