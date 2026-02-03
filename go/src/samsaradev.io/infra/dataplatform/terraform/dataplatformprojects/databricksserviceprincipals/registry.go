package databricksserviceprincipals

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

const (
	MetricsServicePrincipal                     = "dev-databricks-metrics"
	CIServicePrincipal                          = "dev-databricks-ci"
	DataPipelinesLambdaServicePrincipal         = "dev-databricks-datapipelines-lambda"
	DagsterServicePrincipal                     = "dev-databricks-dagster"
	ProdGQLServicePrincipal                     = "dev-databricks-prod-gql"
	ProdDatabricksServicePrincipal              = "prod-databricks-ci"
	GoDataLakeServicePrincipal                  = "dev-databricks-godatalake"
	AIMLRayServeServicePrincipal                = "dev-databricks-aimlray-serve"
	DatabricksQueryAgentServicePrincipal        = "dev-databricks-query-agent"
	DatabricksNonRndTestServicePrincipal        = "dev-databricks-non-rnd-test"
	GovRAMPGitHubActionsServicePrincipal        = "dev-databricks-govramp-github-actions"
	ProductGPTSamsaraAssistantServicePrincipal  = "dev-databricks-productgpt-samsara-assistant"
	DataplatformManagedNotebookServicePrincipal = "dev-databricks-dataplatform-managed-notebook"
	ReleaseManagementGithubServicePrincipal     = "dev-databricks-github-release-management-group"
)

type ServicePrincipal struct {
	DisplayName             string
	AllowClusterCreate      bool
	AllowInstancePoolCreate bool
	DatabricksSqlAccess     bool
	WorkspaceAccess         bool
	Active                  bool
	DatabricksGroups        []string
	NonRnd                  bool

	// ProdWorkspace is a flag to indicate if the service principal is used in the prod workspace.
	// If true, the service principal will be added to the prod workspace
	// If false, the service principal will not be added to the dev workspace
	ProdWorkspace bool
}

func (s *ServicePrincipal) RegionalServicePrincipalName(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return s.DisplayName, nil
	case infraconsts.SamsaraAWSEURegion:
		return s.DisplayName + "-eu", nil
	case infraconsts.SamsaraAWSCARegion:
		return s.DisplayName + "-ca", nil
	default:
		return "", oops.Errorf("unknown region: %s", region)
	}
}

var ServicePrincipals = []ServicePrincipal{
	{
		DisplayName:             "databricks-service-principal-test",
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
	},
	{
		DisplayName:             ProdDatabricksServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
		ProdWorkspace: true,
	},
	{
		DisplayName:             CIServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
			team.BizTechEnterpriseDataApplication.DatabricksAccountGroupName(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             DataPipelinesLambdaServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
	},

	{
		DisplayName:             DagsterServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
			team.DataEngineering.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             MetricsServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             ProdGQLServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             GoDataLakeServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             AIMLRayServeServicePrincipal,
		AllowClusterCreate:      true,
		AllowInstancePoolCreate: true,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.MLInfra.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             DatabricksQueryAgentServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataEngineering.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             DatabricksNonRndTestServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.UnityCatalogTeamMigrationTest.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             GovRAMPGitHubActionsServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.InfraPlatSec.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             ProductGPTSamsaraAssistantServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataTools.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             DataplatformManagedNotebookServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.DataPlatform.DatabricksAccountGroupName(),
		},
	},
	{
		DisplayName:             ReleaseManagementGithubServicePrincipal,
		AllowClusterCreate:      false,
		AllowInstancePoolCreate: false,
		DatabricksSqlAccess:     true,
		WorkspaceAccess:         true,
		Active:                  true,
		DatabricksGroups: []string{
			team.ReleaseManagement.DatabricksAccountGroupName(),
		},
	},
}
