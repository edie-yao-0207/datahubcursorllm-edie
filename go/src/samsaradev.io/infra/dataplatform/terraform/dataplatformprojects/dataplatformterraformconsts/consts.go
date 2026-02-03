package dataplatformterraformconsts

import (
	"path/filepath"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var BackendRoot = filepathhelpers.BackendRoot
var DataPlatformRoot = filepath.Join(BackendRoot, "dataplatform")

// NotebooksRoot is the base path for all Databricks notebooks.
var NotebooksRoot = filepath.Join(DataPlatformRoot, "notebooks")

var WorkflowsRoot = filepath.Join(DataPlatformRoot, "workflows")

var DatabricksAppsRoot = filepath.Join(DataPlatformRoot, "databricksapps")

// DataLakeIngestionTerraformProjectPipeline is the name of the terraform pipeline for ks and s3bigstats data lake
// ingestion infra https://buildkite.com/samsara/terraform-aws-project-datalakeingestion
// If you want something to go through this pipeline, set the RootTeam field of the project to this const
const DataLakeIngestionTerraformProjectPipeline = "DataPlatform-DataLakeIngestion"

// DataLakeRdsIngestionTerraformProjectPipeline is the name of the terraform pipeline for data lake rds ingestion
// infra.
const DataLakeRdsIngestionTerraformProjectPipeline = "DataPlatform-DataLakeRdsIngestion"

// DataPlatformDagsterTerraformProjectPipeline is the name of the terraform pipeline for the team's Dagster deployment infra.
const DataPlatformDagsterTerraformProjectPipeline = "DataPlatform-Dagster"

// DataPlatformDatahubTerraformProjectPipeline is the name of the terraform pipeline for the team's Datahub deployment infra.
const DataPlatformDatahubTerraformProjectPipeline = "DataPlatform-Datahub"

// DataPlatformSparkK8sTerraformProjectPipeline is the name of the terraform pipeline for the team's Spark EKS deployment infra.
const DataPlatformSparkK8sTerraformProjectPipeline = "DataPlatform-SparkK8s"

// DataPlatformDynamodbDeltaLakeProjectPipeline is the name of the terraform pipeline for the team's data lake dynamodb ingestion.
const DataPlatformDynamodbDeltaLakeProjectPipeline = "DataPlatform-DataLakeDynamodbIngestion"

// DataPlatformEmrReplicationProjectPipeline is the name of the terraform pipeline for the team's EMR replication.
const DataPlatformEmrReplicationProjectPipeline = "DataPlatform-EmrReplication"

// DataPlatformOfficialDatabricksProvider is the name of the terraform pipeline for the team's resources managed via Official Databricks Terraform Provider.
const DataPlatformOfficialDatabricksProviderTerraformProjectPipeline = "DataPlatform-OfficialDatabricksProvider"

// DataPlatformNonGovrampCustomerData is the name of the terraform pipeline for the team's resources managed via Non-Govramp Customer Data.
const DataPlatformNonGovrampCustomerDataTerraformProjectPipeline = "DataPlatform-NonGovrampCustomerData"

// DataPlatformTerraformOverride is a map for DataPlatform terraform specific team overrides
// Example: SafetyADAS does not have a terraform pipeline but their assets are managed by MLCV
// so place their resources in the MLCV pipeline
var DataPlatformTerraformOverride = map[string]components.TeamInfo{
	team.SafetyADAS.TeamName:  team.MLCV,
	team.Prototyping.TeamName: team.DataPlatform,
}

// Group containing all Samsara Databricks users.
const AllSamsaraUsersGroup = "samsara-users-group"

// Group containing all Samsara Non-RnD Databricks users.
const SamsaraNonRnDGroup = "samsara-nonrnd-group"

// Group containing all Samsara RnD Databricks users.
const SamsaraRnDGroup = "samsara-rnd-group"

// Unity Catalog placeholder instance profile with no permissions.
const UnityCatalogInstanceProfile = "unity-catalog-cluster"

// BudgetPolicyId for the dataplatform automated jobs.
const BudgetPolicyIdForDataplatformAutomatedJobs = "0df43141-8f0b-494c-803d-3d06bcd1e5c8"

// CursorQueryAgentTeams returns the list of teams that have access to the cursor-query-agent SQL endpoint
// and the DataHubMetadataBucket volume. These two lists must always match.
func CursorQueryAgentTeams() []components.TeamInfo {
	return []components.TeamInfo{
		team.AiDev,
		team.AiX,
		team.AiXContributors,
		team.AssetMaintenance,
		team.Compliance,
		team.ConnectedEquipment,
		team.ContextIntelligence,
		team.DataAnalytics,
		team.DataEngineering,
		team.DataPlatform,
		team.DataScience,
		team.DataTools,
		team.DecisionScience,
		team.DeviceServices,
		team.DevEcosystem,
		team.DriverManagement,
		team.EngineeringOperations,
		team.Firmware,
		team.Hardware,
		team.Inspections,
		team.MarketingDataAnalytics,
		team.MdmSdk,
		team.MediaExperience,
		team.MLCV,
		team.MLFirmware,
		team.MLInfra,
		team.MLInfraAdmin,
		team.MLScience,
		team.Navigation,
		team.Oem,
		team.OPX,
		team.PlatformReports,
		team.ProductDesign,
		team.ProductManagement,
		team.ProductOperations,
		team.ProductSupportEngineering,
		team.QualityAssured,
		team.RDStrategy,
		team.ReleaseManagement,
		team.Routing,
		team.SafetyCameraServices,
		team.SafetyEventDetection,
		team.SafetyEventIngestionAndFiltering,
		team.SafetyEventReviewDev,
		team.SafetyEventTriage,
		team.SafetyRisk,
		team.SafetyWorkflow,
		team.Support,
		team.SupportAutomation,
		team.Sustainability,
		team.TPM,
		team.Training,
	}
}
