package emrreplication

import (
	"fmt"
	"strings"

	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/components/ni/sqs"
	"samsaradev.io/service/components/sqsregistry/sqscomponents"
	"samsaradev.io/team"
	teamcomponents "samsaradev.io/team/components"
)

type AssetType string

const (
	DriverAssetType AssetType = "driver"
	DeviceAssetType AssetType = "device"
	TagAssetType    AssetType = "tag"
)

// EmrReplicationSpec represents an entity that should be replicated to the data lake.
type EmrReplicationSpec struct {
	// Name is the unique identifier for this entity
	Name string
	// YamlPath is the path to the JSON file containing the entity definition
	YamlPath string
	// SqsDelaySeconds is the number of seconds to delay the queue for this entity
	// This is used to ensure that when the SQS worker receives the message and makes
	// a call to the ListRecords endpoint, the data is available.
	SqsDelaySeconds int
	// ServiceName is the name of the service. This can be used to get the cells for the entity
	ServiceName string
	// OwnerTeam is the team that owns this entity
	OwnerTeam teamcomponents.TeamInfo
	// Production is true if the entity is used in customer-facing products downstream of the data lake
	Production bool
	// InternalOverrides is a map of internal overrides for the entity
	InternalOverrides *EmrReplicationInternalOverrides
	// DatePartitionExpression is the expression to use for the date partition
	// Note: DatePartitionExpression should return a timestamp in seconds (e.g. "tripStartTime / 1000")
	DatePartitionExpression string
	// ViewDefinition is an optional field that can be used to define the view definition for the entity
	ViewDefinition *EmrReplicationViewDefinition

	// AssetType is the type of asset that this entity is for. For example,
	// SpeedingIntervalsByTrip relies on devices so it would use the
	// DeviceAssetType.
	AssetType AssetType

	// DisableCdc is a flag to disable change data capture resources that support ongoing replication for the entity. This should only be set to true when
	// an entity's changelog has not yet been implemented.
	DisableCdc bool

	// FilterId specifies which filter field to use for asset ID filtering in the
	// EMR backfill workflow. This field name corresponds to one of the "In"
	// comparator filter options defined in the entity's YAML file. While entities
	// may have multiple "In" filter options (e.g., idIn, assetIdIn, driverIdIn),
	// this field determines which specific filter should be used for the backfill
	// process based on the entity's asset type.
	//
	// IMPORTANT: For parameterized entities (those requiring startMs/endMs
	// parameters), do not set FilterId. The backfill workflow will automatically
	// detect parameterized entities and use time-range queries instead of asset
	// ID filtering.
	FilterId string

	// CdcConfig holds configuration for Change Data Capture (CDC) workers that stream
	// database changes to downstream systems like the Search Platform. If nil, the entity
	// does not participate in CDC pipelines.
	CdcConfig *CdcConfig
}

type CdcConfig struct {
	// DataSource is the default data source type for this entity across all clouds.
	// Most entities should only set this field.
	DataSource DataSourceType

	// DataSourceByCloud provides optional overrides for specific clouds.
	// Use this when an entity needs different data sources in different clouds
	// (e.g., Driver uses DynamoDB in US but RDS in EU/CA).
	DataSourceByCloud map[infraconsts.SamsaraCloud]DataSourceType

	// SourceTableName is the database table name where this entity's data originates.
	// CDC workers use this to match incoming change records to the correct entity configuration (case-sensitive).
	SourceTableName string

	// SourcePrimaryKeyName is the column name in the source table that contains the unique
	// record identifier (e.g., "id"). CDC workers use this to extract the record ID from
	// change events for downstream processing (case-sensitive).
	SourcePrimaryKeyName string

	// EmrEntityName is the entity name as defined in the Entity Metadata Registry (EMR).
	// This name is used by CDC workers when publishing change events to identify the entity type.
	// The value must match the entity name in EMR exactly (case-sensitive).
	EmrEntityName string
}

// GetDataSourceForCloud returns the data source type for the given cloud.
// It first checks DataSourceByCloud for an override, then falls back to DataSource.
func (c *CdcConfig) GetDataSourceForCloud(cloud infraconsts.SamsaraCloud) DataSourceType {
	if c.DataSourceByCloud != nil {
		if ds, ok := c.DataSourceByCloud[cloud]; ok {
			return ds
		}
	}
	return c.DataSource
}

type DataSourceType int

const (
	DataSourceTypeRDS DataSourceType = iota
	DataSourceTypeDynamoDB
)

// EmrReplicationInternalOverrides is a map of internal overrides for the
// entity.
type EmrReplicationInternalOverrides struct {
	// PageSize is the number of records to return per page. If not set, defaults to 500.
	// Maximum allowed value is 500.
	PageSize int64

	// DisableDlqMonitoring is a flag to disable the DLQ monitoring for the entity.
	// This should only be set when there is a known issue with the upstream service and
	// we have discussed with the team and decided to ignore the messages while the team works on a fix.
	DisableDlqMonitoring bool

	// SqsWorkerShape is the container shape to use for the SQS worker service per entity that is prefixed with `emrreplicationsqsworker`.
	// If not set, defaults to ComputeSmall.
	SqsWorkerShape *infraconsts.ContainerShape

	VacuumConfig *EMRVacuumConfig
}

type VacuumMode int

const (
	VacuumModeDefault          VacuumMode = iota // Defaults to the system default, 4 cells per job
	VacuumModeFixedCellsPerJob                   // Put a fixed number [1, total_num_cells] of cells per job
	VacuumModeAllCellsPerJob                     // Put all cells into a job
)

// Data Platform uses a variable number of vacuum jobs to remove staged
// replica data after the data has been loaded into the delta lake.
// This struct controls how those jobs are created.
type EMRVacuumConfig struct {
	// Defines how to interpret CellsPerJob
	Mode VacuumMode

	// Number of jobs is given by number of cells / CellsPerJob rounded up
	// to the nearest integer.
	//
	// * Any other positive value will get clipped according to
	// value_used := max(min(value_given, total_num_cells), 1))
	CellsPerJob int

	// CellsWithDedicatedJobs lets you cherrypick certain cells that should
	// be processed by their own job. Any cell not included in this list will be
	// handled according to the logic described above on CellsPerJob.
	CellsWithDedicatedJobs []string
}

type EmrReplicationViewDefinition struct {
	// ViewName is the name of the view
	ViewName string
	// SqlString is the view definition for the entity
	SqlString string
}

// emrReplicationRegistry holds all entities that should be replicated to the data lake.
// This is private to prevent direct modification; use GetAllEmrEntities() to access.
var emrReplicationRegistry = []EmrReplicationSpec{
	{
		Name:                    "SpeedingIntervalsByTrip",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/SpeedingIntervalsByTrip.yaml",
		SqsDelaySeconds:         60, // 1 minute delay to wait for cache read to expire. Can be reduced after confirming with Safety team.
		ServiceName:             "speedingservice",
		OwnerTeam:               team.SafetyGeoDetection,
		Production:              true,
		DatePartitionExpression: "tripStartTime / 1000",
		InternalOverrides: &EmrReplicationInternalOverrides{
			PageSize: 250,
			VacuumConfig: &EMRVacuumConfig{
				Mode:        VacuumModeFixedCellsPerJob,
				CellsPerJob: 1,
			},
		},
		AssetType: DeviceAssetType,
		ViewDefinition: &EmrReplicationViewDefinition{
			ViewName: "speeding_intervals",
			SqlString: `SELECT
	date,
	orgId,
	tripId,
	assetId,
	driverId,
	tripStartTime,
	si.startTime,
	si.endTime,
	si.postedSpeedLimit,
	si.severityLevel,
	si.maxSpeed,
	si.dismissStatus,
	si.location.*
FROM %s
LATERAL VIEW explode(speedingIntervals) AS si`,
		},
		FilterId: "assetIdIn",
	},
	{
		Name:                    "Trip",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/Trip.yaml",
		SqsDelaySeconds:         30, // 30 second delay to allow for trip data consistency
		ServiceName:             "tripentitiesservice",
		OwnerTeam:               team.AssetFoundations,
		Production:              true,
		DatePartitionExpression: "startTime / 1000",
		AssetType:               DeviceAssetType,
		InternalOverrides: &EmrReplicationInternalOverrides{
			PageSize:             500, // Use default page size for trips
			DisableDlqMonitoring: true,
		},
		FilterId: "assetIdIn",
	},
	{
		Name:                    "HosViolation",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/HosViolation.yaml",
		SqsDelaySeconds:         30, // 30 second delay to allow for HOS violation data consistency
		ServiceName:             "hosrulesserver",
		OwnerTeam:               team.Compliance,
		Production:              true,
		DatePartitionExpression: "startTime / 1000",
		AssetType:               DriverAssetType,
		InternalOverrides: &EmrReplicationInternalOverrides{
			PageSize:             500,
			DisableDlqMonitoring: true,
		},
		// FilterId is empty because HosViolation is a parameterized entity that
		// requires startMs/endMs parameters instead of asset ID filtering. The
		// backfill workflow automatically detects parameterized entities and
		// queries by time range for the entire org. The idIn filter exists but
		// expects violation UUIDs, not driver IDs.
	},
	{
		Name:                    "Driver",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/Driver.yaml",
		SqsDelaySeconds:         30, // 30 second delay to allow for data consistency
		ServiceName:             "driversserver",
		OwnerTeam:               team.PlatformAdmin,
		Production:              true,
		DatePartitionExpression: "createdAt / 1000",
		AssetType:               DriverAssetType,
		FilterId:                "idIn",
		CdcConfig: &CdcConfig{
			// Default to DynamoDB (US has dual-write enabled)
			DataSource: DataSourceTypeDynamoDB,
			// EU/CA don't have DynamoDB dual-write, so override to RDS
			DataSourceByCloud: map[infraconsts.SamsaraCloud]DataSourceType{
				infraconsts.SamsaraClouds.EUProd: DataSourceTypeRDS,
				infraconsts.SamsaraClouds.CAProd: DataSourceTypeRDS,
			},
			SourceTableName:      "drivers",
			SourcePrimaryKeyName: "id",
			EmrEntityName:        "Driver",
		},
	},
	{
		Name:                    "Asset",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/Asset.yaml",
		SqsDelaySeconds:         30, // 30 second delay to allow for data consistency
		ServiceName:             "assetserver",
		OwnerTeam:               team.AssetFoundations,
		Production:              true,
		DatePartitionExpression: "createdAt / 1000",
		AssetType:               DeviceAssetType,
		FilterId:                "idIn",
		CdcConfig: &CdcConfig{
			DataSource:           DataSourceTypeRDS,
			SourceTableName:      "devices",
			SourcePrimaryKeyName: "id",
			EmrEntityName:        "Asset",
		},
	},
	{
		Name:                    "Tag",
		YamlPath:                "go/src/samsaradev.io/platform/entity/entityregistry/entities/Tag.yaml",
		SqsDelaySeconds:         30, // 30 second delay to allow for data consistency
		ServiceName:             "tagservice",
		OwnerTeam:               team.PlatformAdmin,
		Production:              true,
		DatePartitionExpression: "createdAt / 1000",
		AssetType:               TagAssetType,
		FilterId:                "idIn",
		CdcConfig: &CdcConfig{
			DataSource:           DataSourceTypeRDS,
			SourceTableName:      "tags",
			SourcePrimaryKeyName: "id",
			EmrEntityName:        "Tag",
		},
	},
}

// GetAllEmrReplicationSpecs returns all registered EMR entities.
// This is the main function that other packages should use to access the registry.
func GetAllEmrReplicationSpecs() []EmrReplicationSpec {
	return emrReplicationRegistry
}

// GetEmrReplicationQueue returns the queue for a given entity name.
func GetEmrReplicationQueue(entityName string) sqscomponents.Queue {
	return sqscomponents.Queue{
		Name:  sqs.QueueName(fmt.Sprintf("samsara_emr_entity_replication_%s", strings.ToLower(entityName))),
		Owner: team.DataPlatform,
	}
}

// GetCdcConfigMapForCloud returns CDC configs for entities that use the specified
// data source type in the given cloud. This is the primary function CDC workers
// should use to determine which entities they are responsible for.
func GetCdcConfigMapForCloud(dataSourceType DataSourceType, cloud infraconsts.SamsaraCloud) map[string]*CdcConfig {
	result := make(map[string]*CdcConfig)
	for i := range emrReplicationRegistry {
		spec := &emrReplicationRegistry[i]
		if spec.CdcConfig == nil || spec.CdcConfig.SourceTableName == "" {
			continue
		}
		if spec.CdcConfig.GetDataSourceForCloud(cloud) == dataSourceType {
			result[spec.CdcConfig.SourceTableName] = spec.CdcConfig
		}
	}
	return result
}
