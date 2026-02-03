package sparkversion

import "strings"

// SparkVersion is a string identifying supported Spark versions on Databricks.
// The list of available versions can be retrieved via API.
// https://docs.databricks.com/en/release-notes/runtime/index.html for more info
// on the runtime versions and their respective end-of-support dates.
// https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4299383/Runbook+Databricks+Runtime+Upgrade+Policy
// for our Samsara's internal policy on upgrading Databricks runtimes.
type SparkVersion string

func (s SparkVersion) IsMl() bool {
	return strings.Contains(string(s), "ml")
}

const (
	// Regular Spark Versions
	SparkVersion122xScala212 = SparkVersion("12.2.x-scala2.12") // LTS - Mar 1, 2026
	SparkVersion133xScala212 = SparkVersion("13.3.x-scala2.12") // LTS - Aug 22, 2026
	SparkVersion143xScala212 = SparkVersion("14.3.x-scala2.12") // LTS - Feb 1, 2027
	SparkVersion154xScala212 = SparkVersion("15.4.x-scala2.12") // LTS - Aug 19, 2027
	SparkVersion164xScala212 = SparkVersion("16.4.x-scala2.12") // LTS - May 9, 2028
	SparkVersion173xScala213 = SparkVersion("17.3.x-scala2.13") // LTS - Oct 22, 2028

	// CPU ML Versions
	SparkVersion133xCpuMlScala212 = SparkVersion("13.3.x-cpu-ml-scala2.12") // LTS - Aug 22, 2026
	SparkVersion143xCpuMlScala212 = SparkVersion("14.3.x-cpu-ml-scala2.12") // LTS - Feb 1, 2027
	SparkVersion154xCpuMlScala212 = SparkVersion("15.4.x-cpu-ml-scala2.12") // LTS - Aug 19, 2027
	SparkVersion164xCpuMlScala212 = SparkVersion("16.4.x-cpu-ml-scala2.12") // LTS - May 9, 2028

	// GPU ML Versions
	SparkVersion133xGpuMlScala212 = SparkVersion("13.3.x-gpu-ml-scala2.12") // LTS - Aug 22, 2026
	SparkVersion143xGpuMlScala212 = SparkVersion("14.3.x-gpu-ml-scala2.12") // LTS - Feb 1, 2027
	SparkVersion154xGpuMlScala212 = SparkVersion("15.4.x-gpu-ml-scala2.12") // LTS - Aug 19, 2027
	SparkVersion164xGpuMlScala212 = SparkVersion("16.4.x-gpu-ml-scala2.12") // LTS - May 9, 2028

	// Variable used to upgrade the default spark version for interactive clusters and notebook jobs
	// Default Version
	DefaultSparkVersion            = SparkVersion133xScala212
	DefaultNotebookJobSparkVersion = SparkVersion154xScala212
	DefaultCpuMlSparkVersion       = SparkVersion133xCpuMlScala212
	DefaultGpuMlSparkVersion       = SparkVersion133xGpuMlScala212
	// Next Version
	NextSparkVersion            = SparkVersion154xScala212
	NextNotebookJobSparkVersion = SparkVersion154xScala212
	NextCpuMlSparkVersion       = SparkVersion154xCpuMlScala212
	NextGpuMlSparkVersion       = SparkVersion154xGpuMlScala212
)

// Centralized default versions for Databricks resources
// to simplify future updates and provide a clear overview.
// TODO: Divide out the version const into production/nonproduction
// for the relevant resources to allow for easier phased version upgrades.
const (
	// Data Platform Replication Resources
	DataStreamsNotebookJobSparkVersion   = SparkVersion133xScala212
	DynamoDbrVersion                     = SparkVersion154xScala212
	KinesisstatsDbrVersion               = SparkVersion154xScala212
	KinesisstatsDailyDbrVersion          = SparkVersion154xScala212
	KinesisstatsInstancePoolDbrVersion   = SparkVersion154xScala212
	RdsDbrVersion                        = SparkVersion154xScala212
	RdsCombineShardsDbrVersion           = SparkVersion154xScala212
	RdsInstancePoolDbrVersion            = SparkVersion154xScala212
	S3BigstatsDbrVersion                 = SparkVersion154xScala212
	S3BigstatsInstancePoolDbrVersion     = SparkVersion154xScala212
	EmrReplicationDbrVersion             = SparkVersion154xScala212
	EmrReplicationInstancePoolDbrVersion = SparkVersion154xScala212
	EmrValidationDbrVersion              = SparkVersion154xScala212
	OrgShardsTableV2DbrVersion           = SparkVersion154xScala212
	// Data Platform Direct Customer Facing Resources
	DataPipelinesNonProductionDbrVersion   = SparkVersion154xScala212
	DataPipelinesProductionDbrVersion      = SparkVersion154xScala212
	DataPipelinesPoolDbrVersion            = SparkVersion154xScala212
	DataPipelinesSqliteDbrVersion          = SparkVersion154xScala212
	DataPipelinesDeployMachineryDbrVersion = SparkVersion154xScala212
	SparkReportsDbrVersion                 = SparkVersion154xScala212

	// Data Platform Misc Resources
	CostMonitoringDbrVersion       = SparkVersion154xScala212
	DbVacuumJobDbrVersion          = SparkVersion154xScala212
	DeltaLakeDeletionDbrVersion    = SparkVersion154xScala212
	GlueSnapshotBackupDbrVersion   = SparkVersion154xScala212
	JobSpecDefaultDbrVersion       = SparkVersion154xScala212
	KsDiffToolDbrVersion           = SparkVersion154xScala212
	S3InventoryDeltaDbrVersion     = SparkVersion154xScala212
	UcSyncJobDbrVersion            = SparkVersion154xScala212
	VacuumJobDbrVersion            = SparkVersion154xScala212
	DefaultClusterPolicyDbrVersion = SparkVersion154xScala212

	// Data Tools Resources
	DagsterClusterPoolDbrVersion            = SparkVersion154xScala212
	DagsterClusterPoolDbrVersion_DEPRECATED = SparkVersion122xScala212
	DagsterDevUnityDbrVersion               = SparkVersion154xScala212
	DeltaDataPreviewGeneratorDbrVersion     = SparkVersion154xScala212
	MixpanelIngestionDbrVersion             = SparkVersion154xScala212
)
