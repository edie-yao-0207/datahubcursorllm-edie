package terraform

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/consts/ni/awsconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/amundsen"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dagsterprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksaccount"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksserviceprincipals"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databrickssqlquerymanager"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataengineeringprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/datahubprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/datapipelineprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatforminternallambda"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/datastreamlakeprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/deltalakedeletionprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/emrreplicationproject"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/ksdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/mixpanel"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/rdslakeprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/sparkk8s"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/unitycatalog"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/builder"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
)

// Builds the aws resources in our databricks account. This includes DMS, VPCs, SQS queues, etc.
// In addition, for legacy reasons, the construction of our deprecated legacy databricks workspaces is also done here.
// The EU workspace has already been deleted and the US workspace is planned for deletion 12/31/23.
type DatabricksAwsAndLegacyWorkspaceBuilder struct {
	ProviderGroup string
}

var _ builder.ProjectBuilder = (*DatabricksAwsAndLegacyWorkspaceBuilder)(nil)

func (b *DatabricksAwsAndLegacyWorkspaceBuilder) Projects() ([]*project.Project, error) {
	var projects []*project.Project
	config, err := dataplatformconfig.DatabricksProviderConfig(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	infraProject, err := dataplatformprojects.CoreInfraProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, infraProject)

	// TODO: seems strange that this is in this portion of the code rather than the DatabricksWorkspaceBuilder.
	workspaceProjects, err := dataplatformprojects.E2WorkspaceProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, workspaceProjects...)

	dagsterProject, err := dataplatformprojects.DojoDagsterProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dagsterProject)

	dataStreamIngestionProjects, err := datastreamlakeprojects.AllProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dataStreamIngestionProjects...)

	dataPipelinesProjects, err := datapipelineprojects.AllProjects(b.ProviderGroup)
	if err != nil {
		return nil, err
	}
	projects = append(projects, dataPipelinesProjects...)

	// These are aws resources necessary for unity catalog
	ucInfra, err := databricksaccount.DatabricksAccountAwsProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, ucInfra)

	generatedCSVS3FilesProject, err := dataengineeringprojects.GeneratedCSVS3FilesProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, generatedCSVS3FilesProject)

	// cutoff line for canada region.
	if config.Region == infraconsts.SamsaraAWSCARegion {
		return projects, nil
	}

	// Only build for US region
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		mixpanelConnectorProject, err := dataplatformprojects.MixpanelConnectorProject(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		projects = append(projects, mixpanelConnectorProject)

		dagsterProject, err := dagsterprojects.DagsterAWSProject(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to build dagster project")
		}
		projects = append(projects, dagsterProject)

		datahubProject, err := datahubprojects.DatahubAWSProject(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to build datahub project")
		}
		projects = append(projects, datahubProject)

		sparkk8sProject, err := sparkk8s.SparkK8sProject(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to build spark k8s project")
		}
		projects = append(projects, sparkk8sProject)
	}

	dataplatformInternalTestLambdas, err := dataplatforminternallambda.AllProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dataplatformInternalTestLambdas...)

	return projects, nil
}

// Build databricks account resources (i.e. groups).
type DatabricksAccountBuilder struct {
	ProviderGroup string
}

var _ builder.ProjectBuilder = (*DatabricksAccountBuilder)(nil)

func (b *DatabricksAccountBuilder) Projects() ([]*project.Project, error) {
	var projects []*project.Project

	accountGroupsProject, err := databricksaccount.DatabricksAccountProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, accountGroupsProject)

	return projects, nil
}

// Build *databricks* resources for the given region.
// This is databricks jobs, instance pools, etc.
type DatabricksWorkspaceBuilder struct {
	ProviderGroup string
}

var _ builder.ProjectBuilder = (*DatabricksWorkspaceBuilder)(nil)

func (b *DatabricksWorkspaceBuilder) Projects() ([]*project.Project, error) {
	var projects []*project.Project
	config, err := dataplatformconfig.DatabricksProviderConfig(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	S3InventoryProjects, err := dataplatformresource.S3InventoryProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, S3InventoryProjects...)

	unityCatalogProjects, err := unitycatalog.UnityCatalogProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create unity catalog projects")
	}
	projects = append(projects, unityCatalogProjects...)

	teamProject, err := dataplatformprojects.TeamProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, teamProject)

	S3DatabricksJobDeployProjects, err := dataplatformresource.S3DatabricksJobDeployProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, S3DatabricksJobDeployProjects...)

	interactiveClustersByTeamProjects, err := dataplatformprojects.InteractiveClustersByTeamProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, interactiveClustersByTeamProjects...)

	// Workspace Configuration Project only exists for E2 because it is where we house IP Access Listing which is only an E2 feature
	workspaceConfProject, err := dataplatformprojects.WorkspaceConfigurationProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, workspaceConfProject)

	dynamodbDeltaLakeProjects, err := dynamodbdeltalake.DynamodbDeltaLakeProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating dynamodb delta lake projects")
	}
	projects = append(projects, dynamodbDeltaLakeProjects...)

	dynamodbDeltaLakeAwsEngCloudProjects, err := dynamodbdeltalake.DynamodbDeltaLakeAwsEngCloudProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating dynamodb delta lake projects in aws eng cloud account")
	}
	projects = append(projects, dynamodbDeltaLakeAwsEngCloudProjects...)

	kinesisstatsProjects, err := ksdeltalake.KinesisStatsCollectorProjects(b.ProviderGroup)
	if err != nil {
		return nil, err
	}
	projects = append(projects, kinesisstatsProjects...)

	// Scheduled notebooks are not enabled in Canada. However, this project
	// contains the data_streams_live_ingestion scheduled notebook, which is
	// needed to get data streams into the data lake. When we want to roll out
	// scheduled notebooks in the Canada region, we will need to modify
	// ScheduledNotebookProjects and all of its downstream functions.
	notebookProjects, err := dataplatformprojects.ScheduledNotebookProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, notebookProjects...)

	dataPipelinesDbxProjects, err := datapipelineprojects.AllDbxProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "Create data pipeline projects and vacuum jobs failed.")
	}
	projects = append(projects, dataPipelinesDbxProjects...)

	reportProjects, err := dataplatformprojects.ReportAggregatorProjects(b.ProviderGroup)
	if err != nil {
		return nil, err
	}
	projects = append(projects, reportProjects...)

	sqlEndpointsProjects, err := dataplatformprojects.SqlEndpointsProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, sqlEndpointsProjects...)

	rdsLakeProjects, err := rdslakeprojects.AllProjects(b.ProviderGroup)
	if err != nil {
		return nil, err
	}
	projects = append(projects, rdsLakeProjects...)

	servicePrincipalProject, err := databricksserviceprincipals.DatabricksServicePrincipalsProject(b.ProviderGroup, false)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, servicePrincipalProject)

	databricksAppsProjects, err := dataplatformprojects.DatabricksAppsProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, databricksAppsProjects...)

	emrReplicationProjects, err := emrreplicationproject.EMRReplicationProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating emr replication project")
	}
	projects = append(projects, emrReplicationProjects...)

	dagsterProject, err := dagsterprojects.DagsterDatabricksProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dagsterProject)

	workflowProjects, err := dataplatformprojects.WorkflowProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, workflowProjects...)

	s3BigStatsProjects, err := dataplatformprojects.S3BigStatsDeltaLakeProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, s3BigStatsProjects...)

	unityCatalogSyncJobProjects, err := unitycatalog.UcSyncJobProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, unityCatalogSyncJobProjects...)

	s3UCTablesProject, err := dataplatformprojects.S3UCTablesProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, s3UCTablesProject...)

	s3UCViewsProject, err := dataplatformprojects.S3UCViewsProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, s3UCViewsProject...)

	clusterPolicyProject, err := dataplatformprojects.ClusterPolicyTfProject(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, clusterPolicyProject)

	s3InventoryDeltaProjects, err := dataplatformprojects.S3InventoryDeltaProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, s3InventoryDeltaProjects...)

	queryProjects, err := databrickssqlquerymanager.DatabricksSQLQueryManager(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, queryProjects...)

	glueBackupProjects, err := dataplatformprojects.GlueBackupProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, glueBackupProjects...)

	vacuumProjects, err := dataplatformprojects.DbVacuumProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, vacuumProjects...)

	dataDeletionProjects, err := deltalakedeletionprojects.DataDeletionProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, dataDeletionProjects...)

	costMonitoringProjects, err := dataplatformprojects.CostMonitoringProjects(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	projects = append(projects, costMonitoringProjects...)

	// Projects that are only available in the US region
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		mixpanelProjects, err := mixpanel.MixpanelProjects(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		projects = append(projects, mixpanelProjects...)

		govrampProjects, err := govramp.GovrampProjects(b.ProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "error creating govramp project")
		}
		projects = append(projects, govrampProjects...)
	}

	// Amundsen terraform resources only exist in us-west-2 so only generate them when it is the us-west-2 provider
	if b.ProviderGroup == "databricks-dev-us" && config.Region == awsconsts.Region.USWest2 {
		amundsenProject, err := amundsen.AmundsenProject(config)
		if err != nil {
			return nil, oops.Wrapf(err, "error creating amundsen project resources")
		}
		projects = append(projects, amundsenProject)

	}

	return projects, nil
}

// This builds our test "prod" workspace in Databricks
// for which we only generate some resources
type DatabricksTestProdWorkspaceBuilder struct {
	ProviderGroup string
}

var _ builder.ProjectBuilder = (*DatabricksTestProdWorkspaceBuilder)(nil)

func (b *DatabricksTestProdWorkspaceBuilder) Projects() ([]*project.Project, error) {
	var projects []*project.Project
	config, err := dataplatformconfig.DatabricksProviderConfig(b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// The databricks-prod-us workspace is our testing workspace and we only want to generate specific resources in it for testing
	prodWorkspaceProject, err := dataplatformprojects.ProdWorkspaceProject(config, b.ProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating prod workspace project")
	}
	projects = append(projects, prodWorkspaceProject)

	servicePrincipalProject, err := databricksserviceprincipals.DatabricksServicePrincipalsProject(b.ProviderGroup, true)
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to generate service principal project for prod workspace")
	}
	projects = append(projects, servicePrincipalProject)

	return projects, nil
}
