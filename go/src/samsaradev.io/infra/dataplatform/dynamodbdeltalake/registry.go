package dynamodbdeltalake

import (
	"fmt"
	"sort"
	"strings"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type InternalOverride struct {
	// List of regions where the table is not being replicated.
	UnavailableRegions []string

	// Frequency of the table replication in hours.
	ReplicationFrequencyHour string

	// if set to true, the table CDC changes will not be streamed to the kinesis stream
	DisableKinesisStreamingDestination bool

	// Override the default max workers for this table's merge job per region.
	// If a region is not specified, the default of 2 workers will be used.
	MaxWorkersByRegion map[string]int

	// When true, enables data deletion support for this table using the
	// dynamodb/auditlog/upsert.py script. Only the data platform team should
	// set this field. Contact the team before enabling.
	IsUsedForDataLakeDeletion bool
}

type Table struct {
	TableName   string
	description string
	// Production is quite outdated and was used to mean the table was being used by datapipelines or in some customer facing capacity.
	Production bool
	// DataModelTable indicates whether if a non-prod table is consumed in downstream data model pipeline or equivalent.
	// Setting this will result in the table getting a smaller SLO window and faster replication (6H) over non-production tables (12h).
	// The priority of the tables is as follows: Production (3h freshness) > DataModelTable (6h freshness) > NonProduction (12h freshness).
	DataModelTable bool
	// PartitionKey + SortKey (optional) make up the table's primary key.
	PartitionKey string
	SortKey      string
	// PartitionStrategy represent the data is partitioned in the data lake.
	PartitionStrategy PartitionStrategy
	InternalOverrides InternalOverride
	TeamOwner         components.TeamInfo
	// Queue specifies whether the merge job should be queued, if there is already a job running.
	Queue bool
	// CustomSparkConfigurations specifies the spark configurations that will be applied on merge job's spark cluster.
	// *Note. This is only supported for classic spark compute. DO NOT set if ServerlessConfig has been specified.
	CustomSparkConfigurations map[string]string
	// ServerlessConfig specifies the serverless configuration for the table.
	ServerlessConfig *databricks.ServerlessConfig
	// Optional - When set with a rfc3339 in the tables point in time window, will export and load all data up to that timestamp
	ExportTime string
}

func (tb *Table) GetReplicationFrequencyHour() string {
	if tb.InternalOverrides.ReplicationFrequencyHour != "" {
		return tb.InternalOverrides.ReplicationFrequencyHour
	} else if tb.Production {
		return "3"
	} else if tb.DataModelTable {
		return "6"
	}
	return "12"
}

func (t Table) SloConfig() dataplatformconsts.JobSlo {
	// Create a separate SLO for Datamodel used tables. It is currently same as
	// the non-production tables, but once we have SLA with data tools team, we shall
	// populate the correct numbers for SLO here.
	if t.DataModelTable {
		return dataplatformconsts.JobSlo{
			SloTargetHours:              24,
			Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
			BusinessHoursThresholdHours: 24,
			HighUrgencyThresholdHours:   96,
		}
	}

	// For now, we'll set the slo target to 24 hours and also page high urgency business hours.
	// After 4 days we'll page high urgency so we don't hit retention policies.
	return dataplatformconsts.JobSlo{
		SloTargetHours:              24,
		Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
		BusinessHoursThresholdHours: 24,
		HighUrgencyThresholdHours:   96,
	}
}

func (t Table) BuildDescription() string {
	var parts []string
	if t.description != "" {
		parts = append(parts, t.description)
	}
	parts = append(parts, t.SloConfig().SloDescription())
	return strings.Join(parts, "\n\n")
}

type PartitionColumnType string

const (
	MillisecondsPartitionColumnType = PartitionColumnType("milliseconds")
	TimestampPartitionColumnType    = PartitionColumnType("timestamp")
)

type PartitionStrategy struct {
	Key              string
	Expr             string
	SourceColumnType PartitionColumnType
	SourceColumnName string
}

var SinglePartition = PartitionStrategy{
	Key: "partition",

	// Empty / null partitions are problematic.
	// https://kb.databricks.com/data/null-empty-strings.html
	Expr: "'0'",
}

func DateColumnFromMilliseconds(timeMsColumn string) PartitionStrategy {
	return PartitionStrategy{
		Key:              "date",
		Expr:             fmt.Sprintf("date(from_unixtime(%s / 1e3))", timeMsColumn),
		SourceColumnType: MillisecondsPartitionColumnType,
		SourceColumnName: timeMsColumn,
	}
}

func DateColumnFromTimestamp(timestampColumn string) PartitionStrategy {
	return PartitionStrategy{
		Key:              "date",
		Expr:             fmt.Sprintf("date(%s)", timestampColumn),
		SourceColumnType: TimestampPartitionColumnType,
		SourceColumnName: timestampColumn,
	}
}

// TODO: Add linting to make sure required fields (eg TableName, PartitionKeys,
// and PartitionStrategy) are not empty.
var registry = map[string]Table{ // lint: +sorted
	"asset-maintenance-settings": {
		TableName:         "asset-maintenance-settings",
		PartitionStrategy: SinglePartition,
		description:       "This table contains orgwide settings related to maintenance.",
		PartitionKey:      "OrgId",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"asset-relationships": {
		TableName:         "asset-relationships",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedMs"),
		description:       "Stores various relationships between entities in the assets domain.",
		PartitionKey:      "SourceEntity",
		SortKey:           "SK",
		TeamOwner:         team.AssetFoundations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"asset-status": {
		TableName:         "asset-status",
		PartitionStrategy: SinglePartition,
		description:       "This table contains custom asset statuses.",
		PartitionKey:      "OrgId",
		SortKey:           "StatusUUID",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"asset-warranties": {
		TableName:         "asset-warranties",
		PartitionStrategy: SinglePartition,
		description:       "This table contains asset warranties.",
		PartitionKey:      "OrgId",
		SortKey:           "WarrantyIdAssetId",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"authn-config": {
		TableName:         "authn-config",
		PartitionStrategy: SinglePartition,
		description:       "AuthnConfig table contains org level metadata data used for authentication, like SAML SSO connection details",
		PartitionKey:      "PK",
		SortKey:           "SK",
		TeamOwner:         team.IAM,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"automation-configurations": {
		TableName:         "automation-configurations",
		PartitionStrategy: SinglePartition,
		description:       "Stores automation configurations for automations product",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.PlatformAlertsWorkflows,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"aux-inputs-settings": {
		TableName:         "aux-inputs-settings",
		PartitionStrategy: SinglePartition,
		description:       "Stores aux input settings per device, including signal inversion (polarity) configuration for each port.",
		PartitionKey:      "OrgId",
		SortKey:           "DeviceId",
		TeamOwner:         team.DiagnosticsTools,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"benchmarks-org-category-selections-beta-canary": {
		TableName:         "benchmarks-org-category-selections-beta-canary",
		PartitionStrategy: SinglePartition,
		description:       "This table contains what cohort overrides a customer in cells sf1/sf2/us2/us3 has set on the Fleet Benchmark report.",
		PartitionKey:      "OrgId",
		SortKey:           "CreatedAt",
		TeamOwner:         team.DataProducts,
		InternalOverrides: InternalOverride{
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"benchmarks-org-category-selections-stable": {
		TableName:         "benchmarks-org-category-selections-stable",
		PartitionStrategy: SinglePartition,
		description:       "This table contains what cohort overrides a customer in cells prod/us4-11 has set on the Fleet Benchmark report.",
		PartitionKey:      "OrgId",
		SortKey:           "CreatedAt",
		TeamOwner:         team.DataProducts,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"benchmarks-org-tag-groups-beta-canary": {
		TableName:         "benchmarks-org-tag-groups-beta-canary",
		PartitionStrategy: SinglePartition,
		description:       "This table contains what tag groups a customer in cells sf1/sf2/us2/us3 has configured on the Fleet Benchmark page to map those to a different cohort.",
		PartitionKey:      "OrgId",
		SortKey:           "TagGroupId",
		TeamOwner:         team.DataProducts,
		InternalOverrides: InternalOverride{
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"benchmarks-org-tag-groups-stable": {
		TableName:         "benchmarks-org-tag-groups-stable",
		PartitionStrategy: SinglePartition,
		description:       "This table contains what tag groups a customer in cells prod/us4-11 has configured on the Fleet Benchmark page to map those to a different cohort.",
		PartitionKey:      "OrgId",
		SortKey:           "TagGroupId",
		TeamOwner:         team.DataProducts,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"cached-active-skus": {
		TableName:         "cached-active-skus",
		PartitionStrategy: SinglePartition,
		description:       "MRM table with org to sku mappings. This table is updated every 24 hours.",
		PartitionKey:      "Sku",
		SortKey:           "OrgId",
		TeamOwner:         team.ReleaseManagement,
		DataModelTable:    true,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"cached-feature-configs-v2": {
		TableName:         "cached-feature-configs-v2",
		PartitionStrategy: SinglePartition,
		description:       "MRM table with org to feature config mappings (v2 with inverted keys).",
		PartitionKey:      "OrgId",
		SortKey:           "FeatureConfigKey",
		TeamOwner:         team.ReleaseManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"chronicle-logs": {
		TableName:         "chronicle-logs",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Stores point-in-time logs tied to specific product entities.",
		PartitionKey:      "OrgId",
		SortKey:           "EventId",
		TeamOwner:         team.WorkerSafety,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"cohort-profile": {
		TableName:         "cohort-profile",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "CohortProfile table contains cohort profile data",
		PartitionKey:      "ProfileName",
		SortKey:           "FeatureType",
		TeamOwner:         team.MLInfra,
		InternalOverrides: InternalOverride{
			ReplicationFrequencyHour: "1",
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"cohort-profile-association": {
		TableName:         "cohort-profile-association",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "CohortProfileAssociation table contains cohort profile association data",
		PartitionKey:      "CohortName",
		SortKey:           "FeatureType",
		TeamOwner:         team.MLInfra,
		InternalOverrides: InternalOverride{
			ReplicationFrequencyHour: "1",
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"cohort-profile-association-change-audit": {
		TableName:         "cohort-profile-association-change-audit",
		PartitionStrategy: DateColumnFromTimestamp("EventTimestamp"),
		description:       "CohortProfileAssociationChangeAudit table contains cohort profile association change audit data",
		PartitionKey:      "CohortName",
		SortKey:           "EventTimestamp",
		TeamOwner:         team.MLInfra,
		InternalOverrides: InternalOverride{
			ReplicationFrequencyHour: "1",
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"competition_leaderboard_snapshots_v2": {
		TableName:         "competition_leaderboard_snapshots_v2",
		PartitionStrategy: DateColumnFromMilliseconds("Timestamp"),
		description:       "Stores paginated leaderboard snapshots for competitions with versioning and time-based partitioning.",
		PartitionKey:      "CompetitionId",
		SortKey:           "VersionPage",
		TeamOwner:         team.DriverManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-27T22:00:00Z",
	},
	"competition_participants": {
		TableName:         "competition_participants",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Maps participants to competitions for tracking participation and eligibility.",
		PartitionKey:      "PK",
		SortKey:           "CompetitionId",
		TeamOwner:         team.DriverManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-27T22:00:00Z",
	},
	"competitions": {
		TableName:         "competitions",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Stores competition configurations including goals, rewards, and scheduling information.",
		PartitionKey:      "CompetitionId",
		TeamOwner:         team.DriverManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-27T22:00:00Z",
	},
	"construction-zone-feedback": {
		TableName:         "construction-zone-feedback",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Construction zone feedback data from users about map annotations",
		PartitionKey:      "ConstructionZoneId",
		SortKey:           "CreatedAt",
		TeamOwner:         team.SmartMaps,
		InternalOverrides: InternalOverride{
			ReplicationFrequencyHour: "3",
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"crux-devices": {
		TableName:         "crux-devices",
		PartitionStrategy: SinglePartition,
		description:       "Table that stores metadata related to crux devices",
		PartitionKey:      "HardwareId",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"crux-relationships": {
		TableName: "crux-relationships",
		InternalOverrides: InternalOverride{
			ReplicationFrequencyHour: "3",
		},
		PartitionStrategy: DateColumnFromMilliseconds("StartMs"),
		description:       "Table that stores crux relationships",
		PartitionKey:      "PrimaryIndexId",
		SortKey:           "StartMs",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"custom-dashboards": {
		TableName:         "custom-dashboards",
		description:       "Table that stores custom dashboards",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.PlatformReports,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"custom-report-configurations": {
		TableName:         "custom-report-configurations",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Table that stores configs for Advanced Custom Reports. These objects are created by users, per report",
		PartitionKey:      "OrgId",
		SortKey:           "ReportId",
		TeamOwner:         team.PlatformReports,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"dataplatform-internal-test-dynamo": {
		TableName:         "dataplatform-internal-test-dynamo",
		PartitionStrategy: SinglePartition,
		description:       "Data platform's internal table that is used only for testing purposes",
		PartitionKey:      "ID",
		SortKey:           "DEVICENAME",
		TeamOwner:         team.DataPlatform,
		Queue:             true,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			ClientVersionByRegionOverride: map[string]databricks.ClientVersion{
				infraconsts.SamsaraAWSEURegion: databricks.ClientVersion4,
			},
		},
		ExportTime: "2025-11-25T23:00:00Z",
	},
	"default-asset-status": {
		TableName:         "default-asset-status",
		PartitionStrategy: SinglePartition,
		description:       "The default asset status for an org.",
		PartitionKey:      "OrgId",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-anomalies": {
		TableName:         "device-anomalies",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "This table stores anomalies for devices.",
		PartitionKey:      "OrgIdDeviceId",
		SortKey:           "TimeMsType",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-cost-centers": {
		TableName:         "device-cost-centers",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing what tags are designated cost centers for each device with itemized billing. The table is refreshed every 12 hours.",
		PartitionKey:      "OrgId",
		SortKey:           "DeviceId",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-geofence-history": {
		TableName:         "device-geofence-history",
		PartitionStrategy: SinglePartition,
		description:       "This table stores the list of geofences a device is located within over time.",
		PartitionKey:      "OrgIdDeviceId",
		SortKey:           "TimeMs",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-immobilizer-state": {
		TableName:         "device-immobilizer-state",
		PartitionStrategy: SinglePartition,
		description:       "Table stores requested and current immobilizer device states",
		PartitionKey:      "OrgId",
		SortKey:           "DeviceId",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-installation-media-records-dynamo": {
		TableName:         "device-installation-media-records-dynamo",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Device Installation Media Records",
		PartitionKey:      "OrgId",
		SortKey:           "DeviceInstallationMediaUUID",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-installation-mobile-logs-dynamo": {
		TableName:         "device-installation-mobile-logs-dynamo",
		PartitionStrategy: SinglePartition,
		description:       "Device Installation Mobile Logs",
		PartitionKey:      "OrgId",
		SortKey:           "EventUUID",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"device-peripheral-mappings": {
		TableName:         "device-peripheral-mappings",
		PartitionStrategy: SinglePartition,
		description:       "Table that stores peripheral mappings for devices",
		PartitionKey:      "ExternalPeripheralId",
		SortKey:           "IntegrationId",
		TeamOwner:         team.ConnectedEquipment,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2025-12-01T17:00:00Z",
	},
	"device-recovery-state": {
		TableName:         "device-recovery-state",
		PartitionStrategy: SinglePartition,
		description:       "Stores state of lost/theft recovery for customer devices.",
		PartitionKey:      "StateUuid",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"driver-assignment-settings": {
		TableName:         "driver-assignment-settings",
		PartitionStrategy: SinglePartition,
		description:       "Driver assignment settings table storing reminder and QR code settings for organizations and devices.",
		PartitionKey:      "OrgDeviceIdentifier",
		SortKey:           "SettingsTypeIdentifier",
		TeamOwner:         team.DriverManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"driver-avatar-summary-generations": {
		TableName:         "driver-avatar-summary-generations",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Stores metadata about driver avatar summary generation runs including review status and editing history.",
		PartitionKey:      "GenerationId",
		TeamOwner:         team.DriverEngagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-07T18:15:16Z",
	},
	"driver-start-of-day-history": {
		TableName:         "driver-start-of-day-history",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		PartitionKey:      "OrgIdDriverId",
		SortKey:           "CreatedAtMs",
		description:       "Driver start of day audio alert history",
		TeamOwner:         team.SafetyFirmware,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-30T00:00:00Z",
	},
	"drivers": {
		TableName:         "drivers",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Drivers data, migrated over from clouddb",
		PartitionKey:      "Id",
		TeamOwner:         team.CoreServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"dynamic-readings": {
		TableName:         "dynamic-readings",
		PartitionStrategy: SinglePartition,
		description:       "Dynamically registered EMR readings",
		PartitionKey:      "OrgId",
		SortKey:           "ReadingId",
		TeamOwner:         team.CoreServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"dynamic-settings-history": {
		TableName:         "dynamic-settings-history",
		PartitionStrategy: DateColumnFromMilliseconds("AppliedAtMs"),
		description:       "Stores history of dynamic settings for vehicles.",
		PartitionKey:      "OrgIdDeviceId",
		SortKey:           "AppliedAtMs",
		TeamOwner:         team.SmartMaps,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2025-12-04T22:00:00Z",
	},
	"entity-auth-data": {
		TableName:         "entity-auth-data",
		PartitionStrategy: SinglePartition,
		description:       "Stores auth data for entities, such as drivers.",
		PartitionKey:      "PK", // No sort key on the entity-auth-data table
		TeamOwner:         team.IAM,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"entity-cost-centers": {
		TableName:         "entity-cost-centers",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing what cost centers are associated for each entity with itemized billing. The table is refreshed every 12 hours.",
		PartitionKey:      "OrgId",
		SortKey:           "EntityTypeEntityId",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"entity-profile-images": {
		TableName:         "entity-profile-images",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Stores profile images for entities, such as drivers.",
		PartitionKey:      "OrgId",
		SortKey:           "EntityIdStorageKey",
		TeamOwner:         team.DriverManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"eod-summary-avatar-feedback": {
		TableName:         "eod-summary-avatar-feedback",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Stores admin and SER agent feedback on EOD summary avatar videos.",
		PartitionKey:      "GenerationId",
		SortKey:           "UserId",
		TeamOwner:         team.DriverEngagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"eod-summary-avatar-media-dynamo": {
		TableName:         "eod-summary-avatar-media-dynamo",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Stores avatar media records for driver's end-of-day summaries including HeyGen generation metadata and video URLs.",
		PartitionKey:      "FeedItemKey",
		TeamOwner:         team.DriverEngagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-07T18:15:16Z",
	},
	"escalations-backend": {
		TableName:         "escalations-backend",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Stores results from ML backend pipeline for a customer escalation.",
		PartitionKey:      "EscalationId",
		SortKey:           "PipelineVersionFeatureSettingId",
		TeamOwner:         team.MLInfra,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"escalations-backend-pipeline-metadata": {
		TableName:         "escalations-backend-pipeline-metadata",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Stores metadata for all the backend pipeline versions used for escalations.",
		PartitionKey:      "PipelineVersion",
		SortKey:           "FeatureType",
		TeamOwner:         team.MLInfra,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"ev-chargecontrol-chargeschedules": {
		TableName:         "ev-chargecontrol-chargeschedules",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Table to store charge schedules for EVs which are reflected in the Charge Insights report.",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.Sustainability,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"exchange-records-dynamo": {
		TableName:         "exchange-records-dynamo",
		PartitionStrategy: SinglePartition,
		description:       "Table that stores exchange records created by the Device Exchange Tool v2 and the Cable Exchange Tool v2.",
		PartitionKey:      "OrgIdDeviceId",
		SortKey:           "ExchangeId",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"fault-code-description-feedback": {
		TableName:         "fault-code-description-feedback",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Stores the customer feedback for AI generated fault code descriptions.",
		PartitionKey:      "OrgId",
		SortKey:           "FaultCodeFeedbackKey",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"feature-config-change-handler-logs": {
		TableName:         "feature-config-change-handler-logs",
		PartitionStrategy: DateColumnFromTimestamp("Timestamp"),
		description:       "Stores the feature config change logs for given orgs and targets.",
		PartitionKey:      "OrgId",
		SortKey:           "Timestamp",
		TeamOwner:         team.ReleaseManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"fuel-change-estimator-inference-feedback": {
		TableName:         "fuel-change-estimator-inference-feedback",
		PartitionStrategy: DateColumnFromMilliseconds("TriggerTimestamp"),
		description:       "Table stores user feedback for fuel change estimator inference",
		PartitionKey:      "DeviceId",
		SortKey:           "EventId",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"fuel-stations-prices": {
		TableName:         "fuel-stations-prices",
		PartitionStrategy: SinglePartition, // Single table design so we have stations and price history in the same table
		description:       "Table stores fuel station prices",
		PartitionKey:      "Sid",
		SortKey:           "Sk",
		TeamOwner:         team.Sustainability,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"functions-definition-event-log": {
		TableName:         "functions-definition-event-log",
		PartitionStrategy: DateColumnFromMilliseconds("SnapshotDate"),
		description:       "Historical log of all changes made to Function definitions with complete metadata snapshots",
		PartitionKey:      "OrgId",
		SortKey:           "DefinitionKey",
		TeamOwner:         team.DevEcosystem,
		// Table contains structs having the same name but different casing causing default spark to fail
		// inferring table schema. Need to explicitly set spark to be case sensitive. This is not
		// supported on DBX Serverless compute so using classic compute instead.
		CustomSparkConfigurations: map[string]string{
			"spark.sql.caseSensitive": "true",
		},
	},
	"functions-invocation-event-log": {
		TableName:         "functions-invocation-event-log",
		PartitionStrategy: DateColumnFromMilliseconds("InvokedAt"),
		description:       "Historical log of function invocations",
		PartitionKey:      "OrgId",
		SortKey:           "InvocationKey",
		TeamOwner:         team.DevEcosystem,
		CustomSparkConfigurations: map[string]string{
			"spark.sql.caseSensitive": "true",
		},
	},
	"gateway-iccid-association": {
		TableName:         "gateway-iccid-association",
		PartitionStrategy: SinglePartition,
		description:       "Links between our concept of gateways to the cellular providers concept of ICCID",
		PartitionKey:      "GatewayId",
		SortKey:           "Version",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"group-coaching": {
		TableName:         "group-coaching",
		PartitionStrategy: SinglePartition,
		description:       "This table contains group coaching reports metadata and contents.",
		PartitionKey:      "PK",
		SortKey:           "SK",
		TeamOwner:         team.SafetyInsights,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"happy-robot-runs": {
		TableName:         "happy-robot-runs",
		PartitionStrategy: DateColumnFromTimestamp("CreatedTimestamp"),
		description:       "Stores data related to happy robot drive eta project for voice agent functionality.",
		PartitionKey:      "OrgId",
		TeamOwner:         team.MLInfra,
		SortKey:           "Status",
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"hos-org-settings": {
		TableName:         "hos-org-settings",
		PartitionStrategy: SinglePartition,
		description:       "Stores HOS settings for each org",
		PartitionKey:      "OrgId",
		TeamOwner:         team.Compliance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"hubble-devices": {
		TableName:         "hubble-devices",
		PartitionStrategy: SinglePartition,
		description:       "Stores information about devices that are part of the Hubble network",
		PartitionKey:      "SamsaraHardwareId",
		TeamOwner:         team.STCEUnpowered,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"iccid-status": {
		TableName:         "iccid-status",
		PartitionStrategy: SinglePartition,
		description:       "Internal status value for each ICCID",
		PartitionKey:      "Iccid",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"inbox-notifications": {
		TableName:         "inbox-notifications",
		PartitionStrategy: DateColumnFromTimestamp("ReceivedAt"),
		description:       "Inbox notifications for users",
		PartitionKey:      "OrgIdRecipientUserId",
		SortKey:           "ReceivedAtNotificationId",
		TeamOwner:         team.WebExperience,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"incident-center-comments": {
		TableName:         "incident-center-comments",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Incident Center Comments",
		PartitionKey:      "PK",
		SortKey:           "SK",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"incident-center-incidents": {
		TableName:         "incident-center-incidents",
		PartitionStrategy: DateColumnFromMilliseconds("HappenedAtMs"),
		description:       "Incident Center Incidents",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"integration-auth-tokens": {
		TableName:         "integration-auth-tokens",
		PartitionStrategy: SinglePartition,
		PartitionKey:      "ProviderType",
		SortKey:           "IntegrationTarget",
		description:       "Stores auth tokens for various integrations",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"internal-access-grants": {
		TableName:         "internal-access-grants",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Internal access grants for employees accessing customer dashboards, related to the JIT Access Audit feature",
		PartitionKey:      "OrgId",
		SortKey:           "UserId_CreatedAt",
		TeamOwner:         team.IAM,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"internal-rma-required-device-dynamo": {
		TableName:         "internal-rma-required-device-dynamo",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Internal RMA required devices",
		PartitionKey:      "OrgId",
		SortKey:           "DeviceId",
		TeamOwner:         team.DeviceServices,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"inventory-location-definitions": {
		TableName:         "inventory-location-definitions",
		PartitionStrategy: SinglePartition,
		description:       "Stores asset maintenance inventory location definitions.",
		PartitionKey:      "OrgId",
		SortKey:           "LocationUuid",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"itemized-billing-configuration": {
		TableName:         "itemized-billing-configuration",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Table storing what each customer's itemized billing configuration. The table is refreshed every 12 hours.",
		PartitionKey:      "SamNumber",
		TeamOwner:         team.ReleaseManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"ks0-kinesisstats-deleter-journal": {
		TableName:         "ks0-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
	},
	"ks1-kinesisstats-deleter-journal": {
		TableName:         "ks1-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
	},
	// This table exists in US, EU and CA.
	"ks2-kinesisstats-deleter-journal": {
		TableName:         "ks2-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
		},
	},
	"ks3-kinesisstats-deleter-journal": {
		TableName:         "ks3-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
	},
	"ks4-kinesisstats-deleter-journal": {
		TableName:         "ks4-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
	},
	"ks5-kinesisstats-deleter-journal": {
		TableName:         "ks5-kinesisstats-deleter-journal",
		PartitionStrategy: SinglePartition,
		description:       "Table storing the journal of data deletion events for the kinesisstats deleter.",
		PartitionKey:      "Id",
		TeamOwner:         team.DataPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		InternalOverrides: InternalOverride{
			IsUsedForDataLakeDeletion: true,
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
	},
	"licenses": {
		TableName:         "licenses",
		PartitionStrategy: SinglePartition,
		description:       "Table storing Licenses.",
		PartitionKey:      "SamNumber",
		SortKey:           "OrderAndLineId",
		TeamOwner:         team.ReleaseManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"maintenance-prediction-events": {
		TableName:         "maintenance-prediction-events",
		PartitionStrategy: DateColumnFromMilliseconds("InferenceTimestampMs"),
		description:       "Stores maintenance prediction events",
		PartitionKey:      "OrgId",
		SortKey:           "PredictionEventUuid",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"manual-review-annotation": {
		TableName:         "manual-review-annotation",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Stores manual review annotations for safety events",
		PartitionKey:      "EventId",
		SortKey:           "OrgId",
		TeamOwner:         team.SafetyDetectionPrecision,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"mem-enterprise-applications": {
		TableName:         "mem-enterprise-applications",
		PartitionStrategy: SinglePartition,
		description:       "MemEnterpriseApplications contains information about an enterprise's MEM applications that are fetched from google.",
		PartitionKey:      "EnterpriseId",
		SortKey:           "PackageName",
		TeamOwner:         team.MdmSdk,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"mem-org-settings": {
		TableName:         "mem-org-settings",
		PartitionStrategy: SinglePartition,
		description:       "MemOrgSettings contains information about an org's MEM configuration.",
		PartitionKey:      "OrgId",
		TeamOwner:         team.MdmSdk,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"mem-scheduled-offboarding-events": {
		TableName:         "mem-scheduled-offboarding-events",
		PartitionStrategy: SinglePartition,
		description:       "Scheduled Offboarding events for MEM Orgs, the DeleteAt field represents the date an MEM Org will be deleted.",
		PartitionKey:      "OrgId",
		TeamOwner:         team.MdmSdk,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"ml-explainability-data": {
		TableName:         "ml-explainability-data",
		PartitionStrategy: DateColumnFromMilliseconds("AssetMs"),
		description:       "Stores data related to machine learning model explainability and interpretability features.",
		PartitionKey:      "EventId",
		TeamOwner:         team.MLInfra,
		SortKey:           "ClipIdNum",
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"model-registry-backend": {
		TableName:         "model-registry-backend",
		PartitionStrategy: SinglePartition,
		description:       "Model registry for backend models, contains model documentation, config and associated metadata.",
		PartitionKey:      "RegistryKey",
		TeamOwner:         team.MLInfra,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"model-registry-firmware": {
		TableName:         "model-registry-firmware",
		PartitionStrategy: SinglePartition,
		description:       "Model registry for firmware models, contains model documentation, config and associated metadata.",
		PartitionKey:      "RegistryKey",
		SortKey:           "Platform",
		TeamOwner:         team.MLInfra,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"msp-media-items-dynamo": {
		TableName:         "msp-media-items-dynamo",
		PartitionStrategy: DateColumnFromMilliseconds("StartMs"),
		description:       "Media Services Platform Media Items",
		PartitionKey:      "CompositePK",
		SortKey:           "MediaItemUuid",
		TeamOwner:         team.SafetyCameraPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"msp-requests": {
		TableName:         "msp-requests",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		PartitionKey:      "RequestUuid",
		description:       "Media Services Platform requests",
		TeamOwner:         team.SafetyCameraPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"notification-events": {
		TableName:         "notification-events",
		PartitionStrategy: DateColumnFromTimestamp("ReceivedAt"),
		description:       "Notification events for users",
		PartitionKey:      "OrgId",
		SortKey:           "SourceEventTypeSourceEventId",
		TeamOwner:         team.WebExperience,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"onboarding": {
		TableName:         "onboarding",
		PartitionStrategy: SinglePartition,
		description:       "Onboarding Assistant",
		PartitionKey:      "Pk",
		SortKey:           "Sk",
		TeamOwner:         team.ReleaseManagement,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-11T00:00:00Z",
	},
	"part-definitions": {
		TableName:         "part-definitions",
		PartitionStrategy: SinglePartition,
		description:       "Stores asset maintenance part definitions.",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"part-inventory-locations": {
		TableName:         "part-inventory-locations",
		PartitionStrategy: SinglePartition,
		description:       "Stores asset maintenance part inventory locations.",
		PartitionKey:      "OrgId",
		SortKey:           "CompositeKey",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"part-manufacturer-definitions": {
		TableName:         "part-manufacturer-definitions",
		PartitionStrategy: SinglePartition,
		description:       "Stores asset maintenance part manufacturer definitions.",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"public-alerts": {
		TableName:         "public-alerts",
		PartitionStrategy: DateColumnFromTimestamp("created_at"),
		PartitionKey:      "Id",
		description:       "Stores public alert data for weather and other threats",
		TeamOwner:         team.SmartMaps,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"reservations": {
		TableName:         "reservations",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAtMs"),
		description:       "Stores vehicle reservations for fleet management and scheduling.",
		PartitionKey:      "ReservationUUID",
		TeamOwner:         team.AssetFoundations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"safety-event-review-activity-log": {
		TableName:         "safety-event-review-activity-log",
		PartitionStrategy: SinglePartition,
		PartitionKey:      "JobUuid",
		SortKey:           "CompositeSK",
		description:       "Safety events review activity log",
		TeamOwner:         team.SafetyDetectionPrecision,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"safety-media-upload-requests": {
		TableName:         "safety-media-upload-requests",
		PartitionStrategy: DateColumnFromMilliseconds("AssetMs"),
		PartitionKey:      "PK",
		SortKey:           "SK",
		description:       "SafetyMediaUploadRequests holds information on all requests made to VGs to upload media from CMs and auxcams.",
		TeamOwner:         team.SafetyCameraPlatform,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"satellite-modem-mappings": {
		TableName:         "satellite-modem-mappings",
		PartitionStrategy: SinglePartition,
		description:       "Satellite modem mappings (IMEI -> GatewayId) together with the last updated timestamp, so that it is possible to determine which gateway is currently associated with given modem",
		PartitionKey:      "IMEI",
		SortKey:           "OrgId_GatewayId",
		TeamOwner:         team.AssetFoundations,
		InternalOverrides: InternalOverride{
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"sensor-readings-configs": {
		TableName:         "sensor-readings-configs",
		PartitionStrategy: SinglePartition,
		description:       "User-provided sensor configuration (e.g. for level monitoring sensors)",
		PartitionKey:      "OrgId_DeviceId",
		TeamOwner:         team.ConnectedEquipment,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"sensor-replay-traces": {
		TableName:         "sensor-replay-traces",
		PartitionStrategy: SinglePartition,
		description:       "Sensor replay traces holds all available traces for sensor replay.",
		PartitionKey:      "TraceUuid",
		SortKey:           "ItemTypeAndIdentifiers",
		TeamOwner:         team.SafetyPlatform,
		InternalOverrides: InternalOverride{
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"service-task-definitions": {
		TableName:         "service-task-definitions",
		PartitionStrategy: SinglePartition,
		description:       "Definitions of service tasks that can be used for work orders.",
		PartitionKey:      "OrgId",
		SortKey:           "ServiceTaskDefinitionKey",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"settings": {
		TableName:         "settings",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing Settings.",
		PartitionKey:      "EntityId",
		SortKey:           "Namespace",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"settings-groups": {
		TableName:         "settings-groups",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing Settings Groups.",
		PartitionKey:      "OrgId",
		SortKey:           "Id",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"settings-groups-assignments": {
		TableName:         "settings-groups-assignments",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing Settings Groups Assignments.",
		PartitionKey:      "EntityId",
		SortKey:           "GroupAssignmentKey",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"settings-snapshots": {
		TableName:         "settings-snapshots",
		PartitionStrategy: SinglePartition,
		description:       "EMBS table storing Settings Snapshots.",
		PartitionKey:      "EntityId",
		SortKey:           "Namespace",
		TeamOwner:         team.PlatformOperations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2025-11-28T18:00:00Z",
	},
	"soze-engine-immobilizer-device-configs": {
		TableName:         "soze-engine-immobilizer-device-configs",
		PartitionStrategy: DateColumnFromMilliseconds("Timestamp"),
		description:       "Device configs sent to the engine immobilizer service.",
		PartitionKey:      "DeviceId",
		SortKey:           "Timestamp",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"soze-engine-immobilizer-org-configs": {
		TableName:         "soze-engine-immobilizer-org-configs",
		PartitionStrategy: DateColumnFromMilliseconds("Timestamp"),
		description:       "Organization immobilization configs.",
		PartitionKey:      "OrgId",
		SortKey:           "Timestamp",
		TeamOwner:         team.FleetSec,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"speeding-event-feedback": {
		TableName:         "speeding-event-feedback",
		PartitionStrategy: DateColumnFromMilliseconds("CreatedAt"),
		description:       "Stores user feedback on speeding events.",
		PartitionKey:      "SafetyEventUuid",
		SortKey:           "CreatedAt",
		TeamOwner:         team.SafetyGeoDetection,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"user-custom-roles": {
		TableName:         "user-custom-roles",
		PartitionStrategy: SinglePartition,
		description:       "Custom roles that users create in orgs",
		PartitionKey:      "PK",
		SortKey:           "OrgId",
		TeamOwner:         team.IAM,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"user-org-directory": {
		TableName:         "user-org-directory",
		PartitionStrategy: SinglePartition,
		description:       "Contains per user per org overrides from sso to samsara maanaged auth",
		PartitionKey:      "OrgId",
		SortKey:           "UserId",
		TeamOwner:         team.IAM,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"vin-decoding-rules": {
		TableName:         "vin-decoding-rules",
		PartitionStrategy: SinglePartition,
		description:       "VIN decoding rules used by the internal VIN decoder",
		PartitionKey:      "Prefix",
		SortKey:           "Subgroup",
		TeamOwner:         team.DiagnosticsTools,
		InternalOverrides: InternalOverride{
			UnavailableRegions: []string{
				infraconsts.SamsaraClouds.EUProd.AWSRegion,
				infraconsts.SamsaraClouds.CAProd.AWSRegion,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"warranties": {
		TableName:         "warranties",
		PartitionStrategy: SinglePartition,
		description:       "This table contains warranties.",
		PartitionKey:      "OrgId",
		SortKey:           "WarrantyId",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
		ExportTime: "2026-01-23T14:00:00Z",
	},
	"wayfinder-settings": {
		TableName:         "wayfinder-settings",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Store configuration data from customer self-serve Wayfinder enablement on the device configuration page",
		PartitionKey:      "Version",
		TeamOwner:         team.ConnectedEquipment,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"wifi-ap-configs": {
		TableName:         "wifi-ap-configs",
		PartitionStrategy: SinglePartition,
		description:       "Store configuration of VG hotspots",
		PartitionKey:      "OrgId",
		TeamOwner:         team.AssetFoundations,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"work-orders": {
		TableName:         "work-orders",
		PartitionStrategy: SinglePartition,
		PartitionKey:      "OrgId",
		SortKey:           "CompositeKey",
		description:       "Work orders are a collection of service tasks completed on a specific asset.",
		TeamOwner:         team.AssetMaintenance,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"worker-safety-incidents": {
		TableName:         "worker-safety-incidents",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Stores worker safety Incidents.",
		PartitionKey:      "OrgId",
		SortKey:           "IncidentUuidKey",
		TeamOwner:         team.WorkerSafety,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"worker-safety-sos-signals": {
		TableName:         "worker-safety-sos-signals",
		PartitionStrategy: DateColumnFromMilliseconds("TriggeredAtMs"),
		description:       "Store SOS signal metadata from worker-safety product",
		PartitionKey:      "OrgId",
		SortKey:           "SosSignalUuidKey",
		TeamOwner:         team.WorkerSafety,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"worker-safety-timers": {
		TableName:         "worker-safety-timers",
		PartitionStrategy: DateColumnFromTimestamp("CreatedAt"),
		description:       "Stores timer information for worker safety check-ins/jobs.",
		PartitionKey:      "OrgIdRecipientIdKey",
		SortKey:           "TimerUuid",
		TeamOwner:         team.WorkerSafety,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"worker-safety-wearable-assignments": {
		TableName:         "worker-safety-wearable-assignments",
		PartitionStrategy: DateColumnFromMilliseconds("StartMs"),
		description:       "Stores wearable device assignments to workers.",
		PartitionKey:      "OrgIdDeviceIdKey",
		SortKey:           "StartMs",
		TeamOwner:         team.WorkerSafety,
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
	"workflow-coordinator-status": {
		TableName:         "workflow-coordinator-status",
		PartitionStrategy: DateColumnFromMilliseconds("StartedAtMs"),
		PartitionKey:      "OrgIdWorkflowUuidKey",
		description:       "Tracks kicked off workflows within the workflow coordinator service framework",
		TeamOwner:         team.SafetyPlatform,
		InternalOverrides: InternalOverride{
			MaxWorkersByRegion: map[string]int{
				infraconsts.SamsaraAWSDefaultRegion: 9,
			},
		},
		ServerlessConfig: &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
		},
	},
}

func AllTables() []Table {
	tables := make([]Table, 0, len(registry))
	for _, t := range registry {
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})

	return tables
}
