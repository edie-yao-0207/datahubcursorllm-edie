package unitycatalog

import (
	"fmt"
	"strings"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/emrreplicationproject"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

func GetAllVolumeSchemas(config dataplatformconfig.DatabricksConfig) []VolumeSchema {
	var volumeSchemas []VolumeSchema
	volumeSchemas = append(volumeSchemas, S3BucketVolumeSchemas...)
	volumeSchemas = append(volumeSchemas, DatabricksWorkspaceVolumeSchema)
	volumeSchemas = append(volumeSchemas, GetDatabricksDeployedArtifactsVolumeSchemaWithRdsMergeResources(config.Region))
	volumeSchemas = append(volumeSchemas, GetAllEmrVolumeSchemas(config)...)

	return volumeSchemas
}

func GetDatabricksDeployedArtifactsVolumeSchemaWithRdsMergeResources(region string) VolumeSchema {

	databricksDeployedArtifactsVolumeSchema := DatabricksDeployedArtifactsVolumeSchema

	volumePath := ""
	if region == infraconsts.SamsaraAWSCARegion {
		volumePath = "databricks-dev-ca"
	} else if region == infraconsts.SamsaraAWSDefaultRegion {
		volumePath = "databricks-dev-us"
	} else if region == infraconsts.SamsaraAWSEURegion {
		volumePath = "databricks-dev-eu"
	} else {
		panic(fmt.Sprintf("Unsupported region: %s", region))
	}

	// Add a volume for proto descriptors.
	databricksDeployedArtifactsVolumeSchema.VolumePrefixes = append(databricksDeployedArtifactsVolumeSchema.VolumePrefixes, Volume{
		Name:      "rds_proto_descriptors",
		Prefix:    fmt.Sprintf("%s/proto_descriptors/", volumePath),
		ReadTeams: []components.TeamInfo{team.DataPlatform},
	})

	return databricksDeployedArtifactsVolumeSchema
}

var DatabricksDeployedArtifactsVolumeSchema = VolumeSchema{
	UnprefixedBucketLocation: "dataplatform-deployed-artifacts",
	VolumePrefixes: []Volume{
		{
			Name:      "init_scripts",
			Prefix:    "init_scripts/",
			ReadTeams: clusterhelpers.DatabricksClusterTeams(),
		},
		{
			Name:      "jars",
			Prefix:    "jars/",
			ReadTeams: clusterhelpers.DatabricksClusterTeams(),
		},
		{
			Name:      "wheels",
			Prefix:    "wheels/",
			ReadTeams: clusterhelpers.DatabricksClusterTeams(),
		},
		{
			Name:   "dagster_steps",
			Prefix: "dagster_steps/",
			ReadWriteTeams: []components.TeamInfo{
				team.DataAnalytics,
				team.DataEngineering,
				team.DataTools,
				team.FirmwareVdp,
				team.FirmwareTelematicsApps,
				team.Sustainability,
				team.MLScience,
			},
		},
		{
			Name:      "others",
			Prefix:    "others/",
			ReadTeams: clusterhelpers.DatabricksClusterTeams(),
		},
	},
}

type VolumeAccess struct {
	ReadTeams      []components.TeamInfo
	WriteTeams     []components.TeamInfo
	ReadWriteTeams []components.TeamInfo
}

func makeUnprefixedVolumeStruct(bucket string, teams VolumeAccess, excludeRegions []string) VolumeSchema {
	return VolumeSchema{
		UnprefixedBucketLocation: bucket,
		VolumePrefixes: []Volume{
			{
				Name:           "root",
				ReadTeams:      teams.ReadTeams,
				WriteTeams:     teams.WriteTeams,
				ReadWriteTeams: teams.ReadWriteTeams,
				ExcludeRegions: excludeRegions,
			},
		},
	}
}

func makePrefixedVolumeStruct(bucket string, prefixes []string, teams VolumeAccess, excludeRegions []string) VolumeSchema {
	volumeprefixes := make([]Volume, len(prefixes))
	for i, prefix := range prefixes {
		volumeprefixes[i] = Volume{
			Name:           prefix,
			Prefix:         prefix + "/",
			ReadTeams:      teams.ReadTeams,
			WriteTeams:     teams.WriteTeams,
			ReadWriteTeams: teams.ReadWriteTeams,
			ExcludeRegions: excludeRegions,
		}
	}
	return VolumeSchema{
		UnprefixedBucketLocation: bucket,
		VolumePrefixes:           volumeprefixes,
	}
}

var S3BucketVolumeSchemas = []VolumeSchema{
	makeUnprefixedVolumeStruct(databaseregistries.NetworkCoverageBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.STCEUnpowered,
			team.ConnectedEquipment,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SamsaraCvDataBucket, VolumeAccess{
		ReadTeams: []components.TeamInfo{
			team.ConnectedWorker,
			team.SafetyFirmware,
			team.DataEngineering,
			team.DataScience,
			team.SafetyADAS,
			team.SafetyCameraServices,
			team.SafetyPlatform,
			team.SafetyEventIngestionAndFiltering,
			team.MLCV,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SamsaraDashcamVideosBucket, VolumeAccess{
		ReadTeams: []components.TeamInfo{
			team.ConnectedWorker,
			team.SafetyFirmware,
			team.DataEngineering,
			team.DataScience,
			team.SafetyADAS,
			team.SafetyCameraServices,
			team.SafetyPlatform,
			team.SafetyEventIngestionAndFiltering,
			team.MLCV,
			team.FirmwareVdp,
			team.Firmware,
			team.Maps,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SamsaraMlModelsBucket, VolumeAccess{
		ReadTeams: []components.TeamInfo{
			team.ConnectedWorker,
			team.SafetyFirmware,
			team.DataEngineering,
			team.DataScience,
			team.SafetyADAS,
			team.SafetyCameraServices,
			team.SafetyPlatform,
			team.SafetyEventIngestionAndFiltering,
			team.MLCV,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SamsaraWorkforceVideoAssetsBucket, VolumeAccess{
		ReadTeams: []components.TeamInfo{
			team.ConnectedWorker,
			team.SafetyFirmware,
			team.DataEngineering,
			team.DataScience,
			team.SafetyADAS,
			team.SafetyCameraServices,
			team.SafetyPlatform,
			team.SafetyEventIngestionAndFiltering,
			team.MLCV,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SensorReplayBucket, VolumeAccess{
		ReadTeams: []components.TeamInfo{team.Firmware, team.FirmwareVdp},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.FirmwareTestAutomationBucket, VolumeAccess{
		ReadTeams:      []components.TeamInfo{team.Hardware},
		ReadWriteTeams: []components.TeamInfo{team.TestAutomation, team.Firmware, team.QualityAssured},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DatabricksNetsuiteInvoiceOutputBucket, VolumeAccess{
		ReadTeams:      []components.TeamInfo{team.BusinessSystems},
		ReadWriteTeams: []components.TeamInfo{team.PlatformOperations},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DatabricksKinesisstatsDiffsBucket, VolumeAccess{
		ReadTeams:      []components.TeamInfo{team.Security},
		ReadWriteTeams: []components.TeamInfo{team.DataPlatform},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.NetsuiteInvoiceExportsBucket, VolumeAccess{
		ReadTeams:      []components.TeamInfo{team.BusinessSystems},
		ReadWriteTeams: []components.TeamInfo{team.PlatformOperations},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.YearReviewMetricsBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.DataScience,
			team.DataEngineering,
			team.DataPlatform,
			team.DataAnalytics,
			team.PlatformReports,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.BenchmarkingMetricsBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.DataProducts,
			team.DataTools,
			team.DataScience,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DatasetsBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.SafetyFirmware,
			team.SafetyEventIngestionAndFiltering,
			team.SafetyPlatform,
			team.MLCV,
			team.ConnectedWorker,
			team.SafetyADAS,
			team.DataEngineering,
			team.DataScience,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.WorkforceDataBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.DataScience,
			team.SafetyFirmware,
			team.SafetyEventIngestionAndFiltering,
			team.SafetyPlatform,
			team.MLCV,
			team.ConnectedWorker,
			team.SafetyADAS,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.BigdataDatashareBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.DataEngineering,
			team.DataScience,
			team.SafetyFirmware,
			team.SafetyEventIngestionAndFiltering,
			team.SafetyPlatform,
			team.MLCV,
			team.ConnectedWorker,
			team.SafetyADAS,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DojoDataExchangeBucket, VolumeAccess{
		ReadWriteTeams: []components.TeamInfo{
			team.MLCV,
			team.DataScience,
		},
	}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.LoadBalancerAccesLogsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.Security, team.AssetsFirmware}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.ApiLoadBalancerAccesLogsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.Security, team.DevEcosystem}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.EC2LogsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.SRE}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.AWSReportsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.SRE}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.OrgWideStorageLensExportBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.SRE}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.PreferencesProductionBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.MarketingDataAnalytics}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.PreferencesSandboxBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.MarketingDataAnalytics}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.NetsuiteSamReportsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.PlatformOperations}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.AttSftpBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.DataScience, team.BizTechEnterpriseData, team.Operations, team.CoreServices, team.TimeSeries}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.PartialReportAggregationBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.DataScience}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DevmetricsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.DataScience, team.DeveloperExperience}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.ThorVariantsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.CoreServices}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.GtmsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.GTMS}}, []string{}),
	// The bucket does not have read permissions for cross account access.
	// TODO: if this volume is needed, modify the s3 bucket policy as shown here: https://github.com/samsara-dev/backend/pull/193676
	// makeUnprefixedVolumeStruct(databaseregistries.DetailedIftaReportsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataScience}}),
	makeUnprefixedVolumeStruct(databaseregistries.EpofinanceBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.EpoFinance}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.NetsuiteLicensesBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.PlatformOperations}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.FivetranSamsaraConnectorDemoBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DevEcosystem}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.PlatopsDatabricksMetadataBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.PlatformOperations}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DataInsightsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataScience, team.DataAnalytics}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DataScienceExploratoryBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataScience}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.MapTilesBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.SafetyPlatform, team.SafetyEventIngestionAndFiltering}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SupportCloudwatchLogsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.Support}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.SmartMapsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.SmartMaps, team.FleetSec, team.STCEUnpowered}}, []string{infraconsts.SamsaraAWSCARegion}),
	makePrefixedVolumeStruct(databaseregistries.SafetyMapDataSourcesBucket, []string{"maptiles", "osm", "raw-data", "temp", "vendored"}, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.SafetyPlatform, team.SafetyEventIngestionAndFiltering, team.Maps, team.Routing}}, []string{}),
	makePrefixedVolumeStruct(databaseregistries.MixpanelBucket, []string{"1192200"}, VolumeAccess{ReadTeams: []components.TeamInfo{team.DataEngineering, team.DataTools, team.DataScience, team.Prototyping}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.MapsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.Maps}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.GqlMappingBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.Cloud}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DynamoDbExportBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataPlatform}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.CommercialNavigationDailyDriverSessionCountBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.Navigation}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DataHubMetadataBucket, VolumeAccess{ReadTeams: dataplatformterraformconsts.CursorQueryAgentTeams()}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DssMlModelsBucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataTools, team.DataPlatform, team.DataEngineering, team.DecisionScience, team.DataScience, team.DataAnalytics}}, []string{}),

	makePrefixedVolumeStruct(databaseregistries.DatamodelWarehouseDevBucket, []string{"tmp"}, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataAnalytics, team.DataPlatform, team.DataTools, team.DataEngineering, team.FirmwareVdp, team.FirmwareTelematicsApps, team.Sustainability, team.FleetSec, team.MLScience}}, []string{}),
	makePrefixedVolumeStruct(databaseregistries.DatamodelWarehouseBucket, []string{"tmp"}, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataAnalytics, team.DataPlatform, team.DataTools, team.DataEngineering, team.FirmwareVdp, team.FirmwareTelematicsApps, team.MLScience}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DeviceInstallationMediaUploadsBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.ConnectedEquipment}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.CSVPostProcessedBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.PlatformAdmin}}, []string{}),
	makeUnprefixedVolumeStruct(databaseregistries.DojoSamsara365KBucket, VolumeAccess{ReadTeams: []components.TeamInfo{team.MLCV, team.MLDeviceFarm, team.MLInfra}}, []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion}),
	makePrefixedVolumeStruct(databaseregistries.QueryAgentsBucket, []string{"logs"}, VolumeAccess{ReadTeams: []components.TeamInfo{team.DataPlatform, team.DataEngineering, team.DataTools}, WriteTeams: dataplatformterraformconsts.CursorQueryAgentTeams()}, []string{}),
}

type teamWorkspaceVolume struct {
	team           components.TeamInfo
	readTeams      []components.TeamInfo
	readWriteTeams []components.TeamInfo
}

// This list was based initially on the list of prefixes in
// samsara-databricks-workspace that are currently in use.
// Teams may add a new entry for themselves if they want a volume,
// and they will by default receive read/write permissions.
var teamVolumePrefixes = []teamWorkspaceVolume{ // lint: +sorted
	{
		team: team.AccountsPayable,
	},
	{
		team: team.BizTechEnterpriseData,
	},
	{
		team: team.BusinessSystems,
	},
	{
		team: team.Cloud,
	},
	{
		team: team.Compliance,
	},
	{
		team: team.ConnectedEquipment,
	},
	{
		team: team.ConnectedWorker,
	},
	{
		team: team.ContentMarketing,
	},
	{
		team: team.CoreServices,
	},
	{
		team: team.DataAnalytics,
	},
	{
		team: team.DataEngineering,
	},
	{
		team: team.DataPlatform,
	},
	{
		team: team.DataScience,
	},
	{
		team: team.DecisionScience,
	},
	{
		team: team.DevEcosystem,
	},
	{
		team: team.DeveloperExperience,
	},
	{
		team:      team.Firmware,
		readTeams: []components.TeamInfo{team.TestAutomation, team.QualityAssured},
	},
	{
		team: team.FleetSec,
	},
	// Fuel Services team is deprecated.
	//{
	//	team:           team.FuelServices,
	//	readWriteTeams: []components.TeamInfo{team.Sustainability},
	//},
	{
		team:           team.Growth,
		readWriteTeams: []components.TeamInfo{team.MarketingDataAnalytics},
	},
	{
		team: team.Hardware,
		readTeams: []components.TeamInfo{
			team.STCEQuality,
			team.AssetsFirmware,
		},
	},
	{
		team: team.IAM,
	},
	{
		team: team.IftaCore,
	},
	{
		team: team.MarketingDataAnalytics,
	},
	{
		team: team.MLInfra,
	},
	{
		team: team.Mobile,
	},
	{
		team: team.Navigation,
	},
	{
		team: team.NewMarketsEUAndMX,
	},
	{
		team: team.Oem,
	},
	{
		team: team.OPX,
	},
	{
		team: team.PlatformAdmin,
	},
	{
		team: team.PlatformAlertsWorkflows,
	},
	{
		team: team.PlatformReports,
	},
	{
		team: team.ProductManagement,
	},
	{
		team: team.ReleaseManagement,
	},
	{
		team: team.Routing,
	},
	{
		team: team.SafetyADAS,
	},
	{
		team: team.SafetyCameraServices,
	},
	{
		team: team.SafetyEventReviewDev,
	},
	{
		team: team.SafetyFirmware,
	},
	{
		team: team.SafetyPlatform,
	},
	{
		team: team.SafetyScoringInsights,
	},
	{
		team: team.SafetyWorkflow,
	},
	{
		team: team.Security,
	},
	{
		team: team.SmartMaps,
	},
	{
		team: team.SupplyChain,
	},
	{
		team: team.Support,
	},
	{
		team: team.Sustainability,
	},
	{
		team: team.TelematicsData,
	},
	{
		team: team.TimeSeries,
	},
	{
		team: team.TPM,
	},
	{
		team: team.WorkerSafety,
	},
}

// In our current setup, we like to setup
// - s3 bucket as a `schema`
// - prefixes as `volumes`
var DatabricksWorkspaceVolumeSchema = VolumeSchema{
	UnprefixedBucketLocation: databaseregistries.DatabricksWorkspaceBucket,
	VolumePrefixes:           allTeamVolumePrefixes(),
}

// Create a prefix in samsara-databricks-workspace for every team.
// This is mostly to match what we already have. In the future, we may want
// to manage scratch spaces for teams differently.
func allTeamVolumePrefixes() []Volume {
	var volumes []Volume
	for _, entry := range teamVolumePrefixes {
		prefix := strings.ToLower(entry.team.TeamName)
		volumes = append(volumes, Volume{
			Name:           prefix,
			Prefix:         prefix + "/",
			ReadTeams:      entry.readTeams,
			ReadWriteTeams: append(entry.readWriteTeams, entry.team),
		})
	}

	// shared volume is used by all teams.
	volumes = append(volumes, Volume{
		Name:      "shared",
		Prefix:    "shared/",
		ReadTeams: team.AllTeams,
	})

	return volumes
}

func GetAllEmrVolumeSchemas(config dataplatformconfig.DatabricksConfig) []VolumeSchema {
	var volumeSchemas []VolumeSchema
	emrExportBucketNames := emrreplicationproject.GetAllEmrReplicationExportBucketNames(config)
	emrExportVolumeSchemas := make([]VolumeSchema, len(emrExportBucketNames))
	for i, bucket := range emrExportBucketNames {
		emrExportVolumeSchemas[i] = makeUnprefixedVolumeStruct(bucket, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataPlatform}}, []string{})
	}
	volumeSchemas = append(volumeSchemas, emrExportVolumeSchemas...)

	emrDeltaLakeBucketNames := emrreplicationproject.GetAllEmrReplicationDeltaLakeBucketNames(config)
	emrDeltaLakeVolumeSchemas := make([]VolumeSchema, len(emrDeltaLakeBucketNames))
	for i, bucket := range emrDeltaLakeBucketNames {
		emrDeltaLakeVolumeSchemas[i] = makePrefixedVolumeStruct(bucket, []string{"checkpoint"}, VolumeAccess{ReadWriteTeams: []components.TeamInfo{team.DataPlatform}}, []string{})
	}
	volumeSchemas = append(volumeSchemas, emrDeltaLakeVolumeSchemas...)

	return volumeSchemas
}
