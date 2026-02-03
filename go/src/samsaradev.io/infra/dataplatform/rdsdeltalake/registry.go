package rdsdeltalake

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/samsarahq/go/oops"

	"github.com/gogo/protobuf/proto"

	"samsaradev.io/connectedworker/workforce/workforcevideomodels"
	"samsaradev.io/controllers/api/apiproto"
	"samsaradev.io/dvirs/dvirsmodels"
	"samsaradev.io/dvirs/dvirsproto"
	"samsaradev.io/fleet/assets/associations/associationsproto"
	"samsaradev.io/fleet/assets/jobschedules/jobschedulesproto"
	"samsaradev.io/fleet/assets/ni/assettype"
	"samsaradev.io/fleet/assets/pingschedules/pingschedulesproto"
	"samsaradev.io/fleet/compliance/compliancemodels/malfunction_diagnostic"
	"samsaradev.io/fleet/compliance/complianceproto"
	"samsaradev.io/fleet/compliance/eu/eucomplianceproto"
	"samsaradev.io/fleet/compliance/eu/euhosproto"
	"samsaradev.io/fleet/compliance/eu/timeoffproto"
	"samsaradev.io/fleet/compliance/ni/eldconst"
	"samsaradev.io/fleet/diagnostics/signalpromotion/signalpromotionproto"
	"samsaradev.io/fleet/dispatch/dispatchmodels"
	"samsaradev.io/fleet/dispatch/dispatchproto"
	"samsaradev.io/fleet/driverdocuments/driverdocumentsproto"
	"samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto"
	"samsaradev.io/fleet/fuel/fueltypeproto"
	"samsaradev.io/fleet/fuel/ifta/iftaproto"
	"samsaradev.io/fleet/oem/oemmodelsproto"
	"samsaradev.io/fleet/routingplatform/routeplanning/routeplanningproto"
	"samsaradev.io/fleet/sled/timecards/timecardsmodels"
	"samsaradev.io/fleet/vehicleproperties/vehiclepropertyproto"
	"samsaradev.io/forms/formsproto"
	"samsaradev.io/helpers/authzhelpers/authzproto"
	"samsaradev.io/helpers/geo/geohelpersproto"
	"samsaradev.io/helpers/maintenanceproto"
	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/eldproto"
	"samsaradev.io/hubproto/industrialhubproto"
	"samsaradev.io/hubproto/maptileproto"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/industrial/industrialcore/industrialcoreproto"
	"samsaradev.io/infra/api/developers/developersproto"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/releasemanagement/releasemanagementmodels"
	"samsaradev.io/infra/releasemanagement/releasemanagementproto"
	"samsaradev.io/integrations/integrationsproto"
	"samsaradev.io/libs/ni/developerconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/mediacatalog/mediacatalogtypesproto"
	"samsaradev.io/ml/agent/agentproto/agentprotomodels"
	"samsaradev.io/mobile/mdm/mdmproto"
	"samsaradev.io/mobile/remotesupport/remotesupportconsts"
	"samsaradev.io/mobile/remotesupport/remotesupportproto"
	"samsaradev.io/models"
	"samsaradev.io/platform/alerts/alertsproto"
	"samsaradev.io/platform/alerts/alerttypes"
	"samsaradev.io/platform/csvuploads/csvuploadsproto"
	"samsaradev.io/platform/finops/finopsproto"
	"samsaradev.io/platform/workflows/workflowsproto"
	"samsaradev.io/reports/proto/reportconfigproto"
	"samsaradev.io/safety/coaching/coachingproto"
	"samsaradev.io/safety/infra/cmassetsmodels/asseturlsproto"
	"samsaradev.io/safety/infra/safetyeventingestionmodels"
	"samsaradev.io/safety/infra/scoringmodels"
	"samsaradev.io/safety/noningestionspeedlimitproto"
	"samsaradev.io/safety/safetyeventreview/safetyeventreviewproto"
	"samsaradev.io/safety/safetyproto"
	"samsaradev.io/safety/safetyvision/facial_recognition/recognitionhelpersproto"
	"samsaradev.io/safety/speedlimitproto"
	"samsaradev.io/safety/videoretrievalproto"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/settings/deviceproto"
	"samsaradev.io/settings/orgproto"
	"samsaradev.io/shards/shardconsts"
	"samsaradev.io/shards/shardproto"
	"samsaradev.io/stats/statsproto"
	"samsaradev.io/stats/trips/tripsproto"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/training/trainingproto"
	"samsaradev.io/userpreferences/userpreferencesproto"
)

type TableVersion int

func (v TableVersion) Name() string {
	return fmt.Sprintf("v%d", v)
}

type ProtoDetail struct {
	Message   proto.Message
	ProtoPath string // Relative path to the .proto file corresponding to the Message

	// Some protos use a generic proto type, eg hubproto.ObjectStatBinaryMessage,
	// with many fields that are irrelevant to the table we are replicating to the
	// data lake. For these special case protos, we want to only replicate select
	// fields. Eg for cmassets_db.camera_upload_requests.request_proto, we expect
	// NestedProtoNames to contain "dashcam_report".
	NestedProtoNames []string

	// EnablePermissiveMode allows the merge job to process malformed protobuf messages as null results.
	// This should only be set as a temporary stopgap to prevent job failures while the issue is root caused and fixed.
	EnablePermissiveMode bool

	// Use this to skip replication of certain proto field using their json tag.
	// Example proto: EntitySchema entity_schema = 2 [ (gogoproto.jsontag) = "entitySchema" ];
	// if you include `entitySchema` it will not replicate this field.
	ExcludeProtoJsonTag []string
}

type ProtobufSchema = map[string]ProtoDetail

type PartitionColumnType string

const (
	MillisecondsPartitionColumnType = PartitionColumnType("milliseconds")
	TimestampPartitionColumnType    = PartitionColumnType("timestamp")
	DatePartitionColumnType         = PartitionColumnType("date")
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

func DateColumnFromDate(dateColumn string) PartitionStrategy {
	return PartitionStrategy{
		Key:              "date",
		Expr:             dateColumn,
		SourceColumnType: DatePartitionColumnType,
		SourceColumnName: dateColumn,
	}
}

// TableVersionInfo is used to manage table version migrations
type TableVersionInfo struct {
	allVersions []TableVersion // allVersions is a list of all table versions
	dmsOutput   TableVersion   // dmsOutput is the current version that DMS is exporting for the table
	spark       TableVersion   // sparkVersion is the  version that Spark is merging into the DataLake
}

func SingleVersion(version TableVersion) TableVersionInfo {
	return TableVersionInfo{
		allVersions: []TableVersion{version},
		dmsOutput:   version,
		spark:       version,
	}
}

func (v TableVersionInfo) VersionsParquet() []TableVersion {
	if len(v.allVersions) == 0 {
		return []TableVersion{0}
	}
	return v.allVersions
}

func (v TableVersionInfo) DMSOutputVersionParquet() TableVersion {
	return v.dmsOutput
}

func (v TableVersionInfo) SparkVersionParquet() TableVersion {
	return v.spark
}

type Table struct {
	DbName                    string
	RdsDb                     string
	RdsPasswordKey            string
	MySqlDb                   string
	TableName                 string
	VersionInfo               TableVersionInfo
	ProtoSchema               ProtobufSchema
	PrimaryKeys               []string
	AllowedRegions            []string // AllowedRegions locks a Table to a subset of regions (e.g. US only); default is all regions
	PartitionStrategy         PartitionStrategy
	DBSharded                 bool
	CustomCombineShardsSelect bool              // CustomCombineShardsSelect is a boolean telling whether the table requires a custom python script to create the combined shard view
	Production                bool              // Production flag indicating if a table is used in a customer facing report
	Description               string            // Description is a human readable explanation of the data in the table
	ColumnDescriptions        map[string]string // ColumnDescriptions is a map where the key are the column names and values are descriptions of the field.
	Tags                      []amundsentags.Tag
	// EmptyStringsCastToNull_DO_NOT_SET indicates whether empty strings should be overwritten with null to maintain parity
	// with the previous replication system. This should only be set for tables that existed in the old replication pipeline.
	// Do not set this field for newly added tables.
	EmptyStringsCastToNull_DO_NOT_SET bool
	ExcludeColumns                    []string // List of column names to be excluded from replication to the data lake.
	ServerlessConfig                  *databricks.ServerlessConfig
	DisableServerless                 bool   // Use this to disable serverless for a table when its globally enabled at DB level.
	PostgreSchemaName                 string // The schema name for the table in PostgreSQL. Default is "public".
}

var defaultColumns = map[string]string{
	"_filename":  "Internal column for Data Lake replication purposes",
	"_rowid":     "Internal column for Data Lake replication purposes",
	"_timestamp": "Internal column for Data Lake replication purposes",
}

var shardedDBExtraDefaultColumns = map[string]string{
	"shard_name": "Name of the database shard this row is from",
}

func (t *Table) GetAllColumnDescriptions() map[string]string {
	columnDescriptions := make(map[string]string)
	for col, description := range defaultColumns {
		columnDescriptions[col] = description
	}

	for col, description := range t.ColumnDescriptions {
		columnDescriptions[col] = description
	}

	if t.DBSharded {
		for col, description := range shardedDBExtraDefaultColumns {
			columnDescriptions[col] = description
		}
	}

	if t.PartitionStrategy != SinglePartition {
		columnDescriptions[t.PartitionStrategy.Key] = fmt.Sprintf("This partition column is derived from the %s column", t.PartitionStrategy.SourceColumnName)
	}

	return columnDescriptions
}

func (t *Table) S3PathName() string {
	pathComponents := []string{t.RdsDb, t.MySqlDb, t.TableName}
	return strings.Join(pathComponents, "/")
}

func (t *Table) S3PathNameWithVersion(version TableVersion) string {
	return fmt.Sprintf("%s_%s", t.S3PathName(), version.Name())
}

func (t *Table) QueueName(version TableVersion) string {
	queueSuffix := strings.ToLower(strings.Join([]string{t.RdsDb, t.MySqlDb, t.TableName, version.Name()}, "_"))
	return fmt.Sprintf("samsara_delta_lake_rds_%s", queueSuffix)
}

func (t *Table) CDCSchemaKey() string {
	return fmt.Sprintf("%s.%s", t.DbName, t.TableName)
}

func (t *Table) InRegion(region string) bool {
	if len(t.AllowedRegions) == 0 {
		return true
	}
	for _, tableRegion := range t.AllowedRegions {
		if tableRegion == region {
			return true
		}
	}
	return false
}

// Registry contains all Table definitions that are replicated from RDS to data lake.
// New tables for the registry are defined in databaseDefinitions below
var Registry = registryTables(databaseDefinitions, false)

func AllDatabases() []RegistryDatabase {
	allDatabases, err := getAllRegistryDatabases()
	if err != nil {
		panic("Failed to get all databases from the RDS deltalake registry: " + err.Error())
	}
	return allDatabases
}

func GetDatabaseByName(dbRegistryName string) (*RegistryDatabase, error) {
	allDatabases, err := getAllRegistryDatabases()
	if err != nil {
		return nil, oops.Wrapf(err, "Failed to get all databases from the RDS deltalake registry")
	}
	for _, db := range allDatabases {
		if db.RegistryName == dbRegistryName {
			return &db, nil
		}
	}
	return nil, oops.Errorf("%s not found in the RDS deltalake registry", dbRegistryName)
}

type tableDefinition struct {
	primaryKeys       []string
	PartitionStrategy PartitionStrategy
	ProtoSchema       ProtobufSchema
	versionInfo       TableVersionInfo
	// The pinned version for the CDC task which should never change. For any new tables,
	// the version is automatically set to 0 and this field should not be set.
	cdcVersion_DO_NOT_SET TableVersion
	allowedRegions        []string // default is all regions
	customCombineShards   bool

	// Production specifies whether this table is used in customer facing
	// features, such as reports.
	Production bool
	// DataModelTable indicates whether if a non-prod table is consumed in downstream data model pipeline or equivalent.
	// Setting this will result in the table getting a smaller SLO window and faster replication (6H) over non-production tables (12h).
	// The priority of the tables is as follows: Production (3h freshness) > DataModelTable (6h freshness) > NonProduction (12h freshness).
	DataModelTable bool

	Description string

	ColumnDescriptions map[string]string

	// Tags are a list of amundsentags.Tag that will enhance your rds tables discoverability within Amundsen
	// View all available tags at infra/dataplatform/amundsen/amundsentags/tags.go
	// Feel free to add new tags to enhance your rds table
	Tags []amundsentags.Tag

	InternalOverrides RdsTableInternalOverrides

	// EmptyStringsCastToNull_DO_NOT_SET indicates whether empty strings should be overwritten with null to maintain parity
	// with the previous replication system. This should only be set for tables that existed in the old replication pipeline.
	// Do not set this field for newly added tables.
	EmptyStringsCastToNull_DO_NOT_SET bool

	// List of column names to be excluded from replication to the data lake.
	ExcludeColumns []string

	ServerlessConfig *databricks.ServerlessConfig

	// DisableServerless indicates whether to disable serverless compute for the table.
	DisableServerless bool

	// PartitionKeyAttribute specifies the column to use as the Kinesis partition key
	// for DMS CDC records. When set, DMS uses an object-mapping rule to partition
	// records by this column's value.
	//
	// Example: "org_id" - All records from the same org share the same partition key.
	//
	// This ensures that records with the same partition key are processed in order.
	// Note: There's no strict guarantee records go to a single shard, as shards can
	// split or merge. However, a well-implemented KCL consumer should use the shard
	// lineage features to avoid processing records out of order even during resharding.
	//
	// If empty, DMS uses its default partition key (schema.table name).
	// The column must exist in the table schema.
	PartitionKeyAttribute string
}

type RdsDatabaseInternalOverrides struct {
	// Overrides the DMS replication instance version. (Current default: 3.5.1)
	DmsEngineVersion awsresource.DmsEngineVersion

	// Overrides the serverless config for all tables in the database.
	ServerlessConfig *databricks.ServerlessConfig

	// Creates a separate DMS replication instance and task for the table with
	// AWS Kinesis Data Streams as a destination.
	// In the interim, we may have both the existing DMS resources that output to
	// S3 for the RDS replication purposes and new DMS resources that output to
	// AWS Kinesis Data Streams for the same DBs. This is a temporary flag to
	// enable the new DMS resources for the tables that have this flag set to true.
	// Once we have fully migrated to the new DMS resources, we should start to cut
	// over the existing RDS replication jobs to use the Firehose output from the
	// AWS Kinesis Data Streams then deprecate the legacy DMS resources writing to S3.
	// RFC: https://samsaradev.atlassian.net/wiki/x/ugDg8w
	EnableKinesisStreamDestination bool

	// Overrides the database engine for all tables in the database.
	AuroraDatabaseEngine infraconsts.AuroraDatabaseEngine
}

type RdsJobShardOverrides struct {
	ShardNumber *int // Set to 0 for the main/prod shard.
	MaxWorkers  *int
}

type RdsJobOverrides struct {
	MaxWorkers                *int
	ShardOverrides            []RdsJobShardOverrides
	CustomSparkConfigurations []map[string]string
	DisableServerless         bool
}

type RdsTableInternalOverrides struct {
	RegionToJobOverrides map[string]RdsJobOverrides

	// LegacyUint64AsBigint converts uint64 to bigint instead of decimal(20,0) for legacy compatibility.
	// This should not be set for any new tables.
	LegacyUint64AsBigint bool

	// JsonpbInt64AsDecimalString converts int64 and uint64 to string in
	// accordance to Protobuf JSON specification. This is the behavior of
	// gogo/protobuf/jsonpb.
	JsonpbInt64AsDecimalString bool

	CronHourSchedule string

	// Used to override the schema name for the table - when database engine is Postgres 17 default value is public.
	DatabaseSchemaOverride string
}

type databaseDefinition struct {
	name                string
	schemaPath          string
	mysqlDbOverride     string
	passwordKeyOverride string
	Sharded             bool
	Tables              map[string]tableDefinition

	// Allows setting additional monitoring on a shard to ensure there is always CDC throughput.
	// This helps us catch silent DMS errors when the DMS tasks say "success", but they are not making progress.
	// This value should be set for all databases containing production tables. You can opt out by setting them to empty.
	// Maps from region -> shard monitors.
	CdcThroughputMonitors map[string]ShardMonitors

	InternalOverrides RdsDatabaseInternalOverrides

	// If this is set, the database will be read only by the given groups.
	// If this is not set, the database will be read by all teams with DBX clusters.
	CanReadGroups []components.TeamInfo

	// Skip the database from being synced to HMS.
	SkipSyncToHms bool

	// CanReadBiztechGroups is a list of biztech groups that can read the table in the default catalog
	// These groups are not managed by dataplatform like R&D teams in CanReadGroups, but are managed by the biztech team
	// and represent Non-R&D teams that use the biztech workspace.
	// The key is the group name and the value is the list of regions the group has access to.
	CanReadBiztechGroups map[string][]string
}

func (db databaseDefinition) Name() string {
	return fmt.Sprintf("%sdb", db.name)
}

func (db databaseDefinition) SchemaPath() string {
	return filepath.Join(filepathhelpers.BackendRoot, db.schemaPath)
}

func (db databaseDefinition) PasswordKey() string {
	if db.passwordKeyOverride != "" {
		return db.passwordKeyOverride
	}
	return fmt.Sprintf("%s_database_password", db.name)
}

func (db databaseDefinition) MySqlDb() string {
	if db.mysqlDbOverride != "" {
		return db.mysqlDbOverride
	}
	return db.Name()
}

// databaseDefinitions is the configuration of databases and tables to load into Registry
var databaseDefinitions = []databaseDefinition{ // lint: +sorted
	{
		name:       dbregistry.ActivationsDB,
		schemaPath: "go/src/samsaradev.io/fleet/activations/activationsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"pilot": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents a pilot (feature activation trial) configuration for organizations",
				ColumnDescriptions: map[string]string{
					"org_id":           metadatahelpers.OrgIdDefaultDescription,
					"uuid":             "Unique identifier for the pilot",
					"pilot_coverage":   "JSON defining the coverage scope of the pilot (devices, groups, etc.)",
					"start_ms":         "Pilot start time in milliseconds since epoch",
					"ongoing":          "Whether the pilot is currently active",
					"scheduled_end_ms": "Scheduled end time in milliseconds since epoch",
					"end_ms":           "Actual end time in milliseconds since epoch (null if ongoing)",
					"pilot_view":       "Enum representing the pilot view type",
					"pilot_type":       "Enum representing the pilot type",
					"pilot_features":   "JSON containing the features enabled for this pilot",
					"pilot_name":       "Human-readable name of the pilot",
					"author_id":        "User ID of the person who created the pilot",
					"created_at":       "Timestamp when the pilot was created",
					"updated_at":       "Timestamp when the pilot was last updated",
				},
			},
			"pilot_devices": {
				primaryKeys:       []string{"org_id", "pilot_uuid", "device_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Junction table associating devices with pilots",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"pilot_uuid": "UUID of the pilot this device is associated with",
					"device_id":  metadatahelpers.DeviceIdDefaultDescription,
				},
			},
		},
	},
	{
		name:       dbregistry.AgentDB,
		schemaPath: "go/src/samsaradev.io/ml/agent/agentmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"messages": {
				primaryKeys:       []string{"org_id", "message_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{
					"content": {
						Message:   &agentprotomodels.ChatMessageContent{},
						ProtoPath: "go/src/samsaradev.io/ml/agent/agentproto/agentprotomodels/chat_message_content.proto",
					},
				},
				Description: "AI assistant chat messages storing conversation content between users and the Samsara Assistant.",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"message_id": "UUID for the chat message.",
					"session_id": "UUID of the chat session this message belongs to.",
					"content":    "Structured message content containing parts (e.g., markdown text).",
					"created_by": "Actor ID identifying who created the message (user or assistant).",
					"created_at": "Timestamp when the message was created.",
					"updated_at": "Timestamp when the message was last updated.",
					"deleted_at": "Timestamp when the message was soft-deleted (null if active).",
				},
			},
			"sessions": {
				primaryKeys:       []string{"org_id", "session_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "AI assistant chat sessions grouping related messages into conversations.",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"session_id": "UUID for the chat session.",
					"title":      "Optional user-provided or auto-generated title for the session.",
					"created_by": "Actor ID identifying who created the session.",
					"created_at": "Timestamp when the session was created.",
					"updated_at": "Timestamp when the session was last updated.",
					"deleted_at": "Timestamp when the session was soft-deleted (null if active).",
				},
			},
		},
	},
	{
		name:            dbregistry.AlertsDB,
		schemaPath:      "go/src/samsaradev.io/platform/alerts/alertsmodels/db_schema.sql",
		mysqlDbOverride: "alerts_db",
		Sharded:         false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"alert_admin_recipients": {
				primaryKeys:                       []string{"org_id", "alert_id", "user_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"latency_metric_timestamps": {
				// TODO: the actual primary key for the table in alertsDB has an output_key
				// column which currently can't be handled by the deltalake due to us treating
				// empty strings as null. Many alert events have output_key as null as it is
				// only used by industrial alerts. This should be updated to include output_key once
				// the deltalake can support empty strings in the primary key.
				// See https://samsara-net.slack.com/archives/CUA5PTXGS/p1597778502113600.
				primaryKeys: []string{"org_id", "alert_id", "object_ids", "started_at"},
				ProtoSchema: ProtobufSchema{
					"timestamp_proto": {
						Message:   &alertsproto.LatencyTimestamps{},
						ProtoPath: "go/src/samsaradev.io/platform/alerts/alertsproto/alerts_server.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("started_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.Api2DB,
		schemaPath: "go/src/samsaradev.io/api/api2models/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"webhook_event_subscriptions": {
				primaryKeys:                       []string{"org_id", "webhook_id", "event_subscription_type"},
				PartitionStrategy:                 SinglePartition, // This is a configuration table, so we do not need timestamp partitioning.
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"webhook_workflow_subscriptions": {
				primaryKeys:                       []string{"org_id", "webhook_id", "workflow_config_uuid"},
				PartitionStrategy:                 SinglePartition, // This is a configuration table, so we do not need timestamp partitioning.
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"api_token_app_consent_requests": {
				Description:                       "The association of orgs to installed API token apps based on granted data sharing consent.",
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"stream_connectors": {
				Description:       "Represents a Connector sink (eg. a Kafka cluster) and how to access it.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
			},
			"stream_subscriptions": {
				Description:       "Represents a subscription to a particular data type for a Stream Connector.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.AssociationsDB,
		schemaPath: "go/src/samsaradev.io/fleet/assets/associations/associationsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"device_associations": {
				primaryKeys:       []string{"org_id", "group_id", "tractor_id", "trailer_id", "start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ProtoSchema: ProtobufSchema{
					"hook_metadata": {
						Message:   &associationsproto.AssociationMetadata{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/associations/associationsproto/associations.proto",
					},
					"drop_metadata": {
						Message:   &associationsproto.AssociationMetadata{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/associations/associationsproto/associations.proto",
					},
				},
				Description: "The pairings (associations) between tractor and trailers calculated by associationsworker",
				ColumnDescriptions: map[string]string{
					"tractor_id":    "The device id of the tractor.",
					"trailer_id":    "The device id of the trailer.",
					"start_ms":      "The time in ms the pairing became active.",
					"end_ms":        "The time is ms the pairing ended.",
					"version":       metadatahelpers.EnumDescription("The method used to calculate the pairing.", associationsproto.Version_name),
					"hook_metadata": "The data related to the calculation of the 'pairing' (hook) between a tractor and a trailer.",
					"drop_metadata": "The data related to the calculation of the 'unpairing' (drop) between a tractor and a trailer.",
				},
				Tags: []amundsentags.Tag{amundsentags.AssetsTag},
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"updated_groups": {
				primaryKeys:       []string{"org_id", "group_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Logs of when the tractor trailer association was last evaluated for an org.",
				ColumnDescriptions: map[string]string{
					"last_updated_ms": "A timestamp in ms when the org was last processed to calculate the pairings between tractors and trailers.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.AttributeDB,
		schemaPath: "go/src/samsaradev.io/platform/attributes/attributescore/attributemodels/db_schema.sql",
		Sharded:    true,
		// Attributedb doesn't receive consistent writes on any shard, so we can't monitor this.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"attribute_cloud_entities": {
				primaryKeys:                       []string{"org_id", "attribute_id", "entity_id", "attribute_value_id", "entity_type"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"attribute_values": {
				primaryKeys:                       []string{"org_id", "double_value", "string_value", "attribute_id", "date_value"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(2),
			},
			"attributes": {
				primaryKeys:                       []string{"org_id", "entity_type", "uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.AuditsDB,
		schemaPath: "go/src/samsaradev.io/platform/audits/auditsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"audits": {
				primaryKeys:       []string{"org_id", "happened_at", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("happened_at"),
				DataModelTable:    true,
				Description:       "Audit logs of all create, read, update, and delete events that happen in an organization’s dashboard.",
				ColumnDescriptions: map[string]string{
					"uuid":                "The unique identifier of the audit.",
					"audit_type_id":       "The type of action that was performed.",
					"org_id":              "The org that owns the action was performed on.",
					"user_id":             "The user that performed the action.",
					"device_id":           "The associated device id, if applicable.",
					"widget_id":           "The associated widget id, if applicable.",
					"summary":             "Summary of the audit log.",
					"happened_at":         "When the action was performed.",
					"remote_ip_address":   "Remote IP address of the user performing the action.",
					"proxy_ip_address":    "Proxy IP address of the user performing the action.",
					"server_ip_address":   "Server IP address the user was connected to when performing the action.",
					"user_agent":          "User agent in use by the user for the action.",
					"driver_id":           "Associated driver id, if applicable.",
					"tag_id":              "Associated tag id, if applicable",
					"id":                  "The associated id of the audit log. This columnn is used to verify migration from cloud_db to audits_db.",
					"supervising_user_id": "If exists, id of the supervising user performing the query.",
				},
			},
			"gql_audits": {
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: DateColumnFromTimestamp("completed_at"),
				Description:       "GQL mutations that happen in an organization’s dashboard.",
				ColumnDescriptions: map[string]string{
					"uuid":                "The unique identifier of the audit.",
					"org_id":              "The org tgihat owns the action was performed on.",
					"principal_type":      "The type of the principal that performed the query.",
					"principal_id":        "The id of the principal that performed the query.",
					"route":               "The route where the query was performed.",
					"query_name":          "The name of the query.",
					"query_variables":     "The variables of the query. This is a hex-encoded json string, you can unpack it by querying the field as `cast(unhex(query_variables) as string)`.",
					"completed_at":        "The time the query was completed.",
					"remote_ip_address":   "Remote IP address of the user performing the action.",
					"proxy_ip_address":    "Proxy IP address of the user performing the action.",
					"server_ip_address":   "Server IP address the user was connected to when performing the action.",
					"user_agent":          "User agent in use by the user for the action.",
					"supervising_user_id": "If exists, id of the supervising user performing the query.",
					"object_identifier":   "A name/identifier for the object that was mutated.",
					"proto":               "Additional information attached to the log.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(4),
						},
					},
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-legal":             {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:                dbregistry.CloudDB,
		schemaPath:          "go/src/samsaradev.io/models/db_schema.sql",
		mysqlDbOverride:     "prod_db",
		passwordKeyOverride: "database_password",
		Sharded:             false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
			EnableKinesisStreamDestination: true,
		},
		// Clouddb receives consistent traffic in the US, but more sporadically in the EU.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"addresses": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"geofence_proto": {
						Message:   &geohelpersproto.AddressesGeofence{},
						ProtoPath: "go/src/samsaradev.io/helpers/geo/geohelpersproto/addresses.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				Production:            true,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "Addresses created by customers",
				ColumnDescriptions: map[string]string{
					"id":                        "Unique identifier for the row.",
					"group_id":                  "Id for the group that owns this address.",
					"created_at":                "When this address was created.",
					"created_by":                "User id that created this address.",
					"name":                      "Customer entered name for this address.",
					"address":                   "Actual address (Ex: 12345 Fulton St., San Francisco, CA, 94121)",
					"latitude":                  "Latitude for this address.",
					"longitude":                 "Longitude for this address.",
					"auto_dismiss_rolled_stops": "Boolean for if we should allow rolled stops at this address.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_asset_labels": {
				primaryKeys:                       []string{"org_id", "alert_id", "asset_label_uuid", "asset_label_key_uuid"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_conditions": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"proto_data": {
						Message:   &alertsproto.AlertConditionData{},
						ProtoPath: "go/src/samsaradev.io/platform/alerts/alertsproto/alertconditiondata.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "This table includes configuration for alerts that indicate the type of the alert and are specific to the alert type. An alert can have one primary and multiple secondary alert_conditions. See alert_conditions.go for all types.",
				ColumnDescriptions: map[string]string{
					"id":                      "Unique identifier for the row.",
					"alert_id":                "Id for the alert config associated with this alert condition.",
					"type":                    metadatahelpers.EnumDescription("Enum value representing the type of the alert.", alerttypes.AlertConditionsNameToTypeMap),
					"int_value1":              "Primary integer configuration value",
					"int_value2":              "Secondary integer configuration value",
					"proto_data":              "Custom proto configuration values. See alertconditiondata.proto for more details.",
					"created_at":              "When this entry in the db was created.",
					"alert_subcondition_uuid": "Industrial alert specific field used for multicondition.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_contacts": {
				primaryKeys:                       []string{"alert_id", "contact_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_devices": {
				primaryKeys:                       []string{"alert_id", "device_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_drivers": {
				primaryKeys:                       []string{"alert_id", "driver_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_industrial_assets": {
				primaryKeys:                       []string{"org_id", "alert_id", "asset_uuid"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_machine_input_types": {
				primaryKeys:                       []string{"org_id", "alert_id", "machine_input_type_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_machine_inputs": {
				primaryKeys:                       []string{"alert_id", "machine_input_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_subconditions": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_tags": {
				primaryKeys:                       []string{"alert_id", "tag_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_webhooks": {
				primaryKeys:                       []string{"alert_id", "org_id", "webhook_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alert_widgets": {
				primaryKeys:                       []string{"alert_id", "widget_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"alerts": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"api_tokens": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"auth0_connections": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
				Description:                       "Contains user SSO connections that use Auth0.",
			},
			"contacts": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"custom_roles": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"permissions": {
						Message:   &authzproto.AuthzRole{},
						ProtoPath: "go/src/samsaradev.io/helpers/authzhelpers/authzproto/authz.proto",
					},
				},
				DataModelTable:        true,
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "Contains a list of custom roles developed by customers.",
				ColumnDescriptions: map[string]string{
					"org_id":          "The org that this role belongs to.",
					"uuid":            "Unique key for this entry.",
					"name":            "The customer set name for this role.",
					"permissions":     "A proto containing a list of permissions assigned to this custom role.",
					"created_at":      "When this entry in the db was created.",
					"updated_at":      "When this entry was last updated.",
					"name_identifier": "Lower case version of name.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dashboards": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialhubproto.Dashboard{},
						ProtoPath: "go/src/samsaradev.io/hubproto/industrialhubproto/dashboard.proto",
					},
					"graphical_config_proto": {
						Message:   &industrialhubproto.GraphicalDashboard{},
						ProtoPath: "go/src/samsaradev.io/hubproto/industrialhubproto/graphical_dashboard.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"developer_apps": {
				primaryKeys:       []string{"uuid", "version"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"app_metadata": {
						Message:   &integrationsproto.DeveloperApp{},
						ProtoPath: "go/src/samsaradev.io/integrations/integrationsproto/developer_app.proto",
					},
				},
				Description: "Thist table holds metadata information about different apps that third party developer had built using the Samsara api. These apps are published in our app marketplace.",
				ColumnDescriptions: map[string]string{
					"uuid":           "Unique identifier of the application row.",
					"version":        "Version number identifier of the application row.",
					"developer_id":   "Foreign key to the developers table. This identifies the developer that created/submitted the app to the marketplace.",
					"name":           "Name of the application.",
					"description":    "Description of the application.",
					"app_metadata":   "Contains extra information about the app metadata defined in the DeveloperApp proto.",
					"approval_state": fmt.Sprintf("Approval state of the application. Look at the AppApprovalState enum for more details. Possible values: %s, %s, %s, %s", developerconsts.PendingAppApprovalState, developerconsts.PublishedAppApprovalState, developerconsts.BetaAppApprovalState, developerconsts.UnpublishedAppApprovalState),
					"created_at":     "Timestamp of when this row was created.",
					"created_by":     "A user id to the user that created this row. This is a Foreign key to the user_id column in [clouddb.users](https://amundsen.internal.samsara.com/table_detail/cluster/delta/clouddb/users)",
					"deactivated":    "If the app is deactivated, this is set to 1.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_rating_actions": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_rulesets": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "Contains driver rulesets data.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"drivers": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
				Production:                        true,
			},
			"external_keys": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"external_values": {
				primaryKeys:                       []string{"org_id", "object_type", "object_id", "external_uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"features_orgs": {
				primaryKeys:                       []string{"feature_id", "organization_id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"feedbacks": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "Records of customer feedback for pages.",
				ColumnDescriptions: map[string]string{
					"id":                    "Unique ID of this entry",
					"user_id":               "User who submitted this feedback.",
					"text":                  "The text of the feedback itself.",
					"url":                   "The page the feedback was submitted on",
					"user_agent":            "Web browser info that the user was on.",
					"image_included":        "Is there an image the customer uploaded?",
					"image_url":             "The location of the image upload.",
					"created_at":            "When the feedback was submitted.",
					"user_email":            "Email of the user who submitted the feedback.",
					"ios_device_model":      "Device info from the submitted feedback.",
					"ios_system_version":    "Device info from the submitted feedback.",
					"ios_app_version":       "The version of the app the user was on.",
					"ios_app_short_version": "The version of the app the user was on.",
					"resolved":              "Was this resolved?",
					"resolved_at":           "When this was resolved.",
					"driver_id":             "Driver who submitted the feedback.",
					"product_team_owner":    "What team owns the page for this feedback.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"firmware_build_infos": {
				primaryKeys:                       []string{"edison_build_name"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"groups": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				Production:            true,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				DataModelTable:        true,
				Description:           "This table contains configuration information associated with a Samsara customer. This table maps 1-to-1 with organization, and will be deprecated long term.",
				ColumnDescriptions: map[string]string{
					"id":                       "The unique ID of the group",
					"organization_id":          metadatahelpers.OrgIdDefaultDescription,
					"name":                     "The name of the group",
					"created_at":               "When the group was created",
					"updated_at":               "The last time the group was updated",
					"firmware_id":              "The ID of the firmware used by devices belonging to this group/organization",
					"wifi_ap_enabled":          "Whether or not devices belonging to this group/organization can be used as a wifi hotspot",
					"wifi_ap_ssid":             "If wifi_ap_enabled is true, the SSID used to connect to devices as a wifi hotspot belonging to this group/organization",
					"wifi_ap_wpa_passphrase":   "If wifi_ap_enabled is true, the password used to connect to devices as a wifi hotspot belonging to this group/organization",
					"timezone":                 "The timezone the group or organization's Cloud dashboard uses",
					"carrier_name":             "The carrier name used for ELD compliance reasons",
					"carrier_address":          "The carrier address used for ELD compliance reasons",
					"driver_auto_duty_enabled": "Whether or not autoduty is enabled for the group to automatically create HOS logs using vehicle data",
					"mobile_firmware_id":       "The mobile app version used by drivers in the organization/group",
					"carrier_us_dot_number":    "The DOT number used for ELD compliance reasons",
					"canada_hos_enabled":       "Whether or not the group/organization is configured to use features related to Canadian ELD regulations",
					"rollout_group":            "This field is deprecated",
					"us_short_haul_enabled":    "Whether or not the group/organization is configured to use features related to short haul ELD ",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"historical_video_requests": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"dashcam_report_proto": {
						Message:   &hubproto.DashcamReport{},
						ProtoPath: "go/src/samsaradev.io/hubproto/camera.proto",
					},
					"camera_info_proto": {
						Message:   &videoretrievalproto.VideoRequestCameraInfo{},
						ProtoPath: "go/src/samsaradev.io/safety/videoretrievalproto/historicalvideorequest.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("created_at_ms"),
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description: "This table contains information about historical video requests " +
					"(aka HVR/video retrievals) made by users. A video retrieval is scoped to the " +
					"safety CM products only, and does not include other camera products (eg. sites).",
				ColumnDescriptions: map[string]string{
					"group_id": "The group ID that this HVR is being made for. We " +
						"should migrate this to org_id at some point.",
					"device_id":            "The VG device ID that the HVR is being made on.",
					"requested_by_user_id": "The user ID that made the HVR",
					"start_ms":             "The start timestamp (ms) that the user made the HVR for.",
					"end_ms":               "The end timestamp (ms) that the user made the HVR for.",
					"state": "The current state of the HVR. Refer to " +
						"historical_video_request.go for the most up to date enum values. This can " +
						"currently be Pending, Failed, Completed, Timed Out or Cancelled.",
					"asset_ms": "The starting timestamp (ms) of the actual video that is uploaded. This can " +
						"differ from the start_ms value in that a user might have requested for a video starting at " +
						"timestamp 10, but if the camera only has footage start at timestamp 11, the asset_ms will be 11.",
					"created_at_ms": "The timestamp (ms) where the HVR is created.",
					"asset_ready_at_ms": "The timestamp (ms) where the video was uploaded from the " +
						"device and is available to our backend for processing.",
					"completed_at_ms": "The timestamp (ms) that the HVR is complete and viewable on " +
						"the dashboard.",
					"get_every_nth_keyframe": "(Hyperlapse) If this value is 0, then the requested " +
						"video is not hyperlapsed. Any non-zero value indicates that the video is a " +
						"hyperlapse.",
					"slowdown_factor": "(Hyperlapse) If this value is 1, the requested video will not be " +
						"slowed down. Any value greater than 1 will indicate a hyperlapse video.",
					"is_multicam": "(Multicam) This value is true when the HVR is for multicam asset(s).",
					"camera_info_proto": "(Multicam) Contains extra camera specific information for " +
						"multicam HVRs",
					"camera_info_proto.requests": "The HVR requests made for multicam systems. Each " +
						"item in this array should correspond to one camera stream in the multicam system.",
					"camera_info_proto.requests.camera_id": "The camera ID of the particular stream " +
						"requested in this HVR.",
					"camera_info_proto.requests.track_id": metadatahelpers.DeprecatedDescription("The track ID of the particular stream requested in this HVR. This field isn't used and should be deprecated."),
					"camera_info_proto.completed_requests": "Used to track the state of video streams that " +
						"have been uploaded. We need this in order to keep track of which cameras are still " +
						"being uploaded, and which have completed.",
					"camera_info_proto.completed_requests.camera_id": "The camera ID of the particular stream " +
						"requested in this HVR.",
					"camera_info_proto.completed_requests.completed_at_ms": "The timestamp (ms) " +
						"that this camera uploaded its video.",
					"wifi_retrieval": "Indicates whether we want to make this retrieval over wi-fi only. " +
						"If this value is true, the VG will only try to upload the video when it has a wi-fi " +
						"connection available.",
					"is_hidden": "Flags whether the HVR is deleted by the user. This value is true " +
						"when a user has deleted the video in the UI. This is essentially a soft-delete.",
					"is_starred": "Flags whether a user has starred this HVR. This value is true when " +
						"a user has starred the HVR in the UI, meaning that it should appear in the " +
						"Starred tab in the video library.",
					"is_incognito": "Flags whether we want to hide this HVR from customer users. This " +
						"typically means that a HVR was made by an internal samsara user and we don't want " +
						"to surface that to users.",
					"stream_resolution_type": "When this field is set, we want to request a " +
						"specific resolution. This field maps to VideoResolutionEnum. The only " +
						"low-res stream we currently support is 640X360d. Default (null) indicates we " +
						"should retrieve high-res if possible, and fallback to low-res if it's available.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"industrial_local_variables": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machine_input_types": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machine_inputs": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialcoreproto.MachineInput{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/machine_input.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "Data inputs (formerly known as machine inputs), used as a layer over object stats for most Industrial products",
				ColumnDescriptions: map[string]string{
					"name":                   "The user-provided name of the data input.",
					"type_id":                "The data group ID. A data group is a general representation of the same data input or data output (IO) across assets.",
					"asset_uuid":             "The UUID of an associated industrial asset, if any. Technically a foreign key to industrialcoredb.assets.",
					"format":                 "An integer from type DataInputFormat. Specifies if this data input is numbers, FFT spectra, locations, strings, DTCs, or unknown.",
					"is_template":            "A boolean indicating if this is a data input template.",
					"template_data_input_id": "The data input ID of a template child's parent template. A key into the machine_inputs table.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machine_machine_inputs": {
				primaryKeys: []string{"machine_id", "machine_input_id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialcoreproto.MachineMachineInputConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/machine_input.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machines": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialcoreproto.MachineConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/machine_config.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"mobile_apps": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores mobile app installation information along with relevant mobile device information",
			},
			"netsuite_invoices": {
				primaryKeys: []string{"sam_number", "netsuite_internal_id"},
				ProtoSchema: ProtobufSchema{
					"proto_data": {
						Message:   &finopsproto.NetsuiteInvoiceData{},
						ProtoPath: "go/src/samsaradev.io/platform/finops/finopsproto/finops.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				Description:                       "This table is deprecated. Please use netsuite_data.netsuite_child_invoices.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"orders": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "This table contains order information received from NetSuite for the purpose of order activation",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_cells": {
				primaryKeys:           []string{"org_id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Production:            true,
				Description:           "This table maps each org to the shard they are hosted in.",
				ColumnDescriptions: map[string]string{
					"org_id":  metadatahelpers.OrgIdDefaultDescription,
					"cell_id": "In the US, one of sf{1,2,3} or us{2-11}. In eu, prod.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_invites": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores active, accepted, cancelled, and expired invites to an org.",
				ColumnDescriptions: map[string]string{
					"id":                "Unique ID",
					"inviting_user_id":  "ID of the user the invite is sent on behalf of",
					"inviting_org_id":   "ID of the org the invite is sent from",
					"inviting_group_id": "ID of the group the invite is sent from",
					"inviting_tag_id":   "ID of the tag the invite is sent from",
					"invited_email":     "Email address the invite is sent to",
					"invited_code":      "Code included in the invite",
					"accepted":          "Flag to signal invite was accepted",
					"accepted_at":       "Time that invite was accepted",
					"accepted_user_id":  "User ID who accepted the invite",
					"created_at":        "Created at timestamp for the row",
					"cancelled_at":      "Time the invite was canceled",
					"role_id":           "ID of the role of the user who was sent the invite",
					"custom_role_uuid":  "UUID of custom role of the user who was sent the invite",
					"expire_at":         "Time the invite expires at",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_sfdc_accounts": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				DataModelTable:        true,
				Description:           "Stores a mapping between orgs and SFDC (Sales Force Dot Com) accounts that live in clouddb.sfdc_accounts.",
				ColumnDescriptions: map[string]string{
					"id":                "Unique Id for this row.",
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"sfdc_account_id":   "Id for the sfdc account that is being joined here(clouddb.sfdc_accounts.id).",
					"created_at":        "When this record was created at.",
					"updated_at":        "When this record was updated at.",
					"sfdc_account_uuid": metadatahelpers.DeprecatedDefaultDescription,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_shards": {
				primaryKeys: []string{"org_id", "shard_type", "data_type"},
				ProtoSchema: ProtobufSchema{
					"status_proto": {
						Message:   &shardproto.OrgShardStatus{},
						ProtoPath: "go/src/samsaradev.io/shards/shardproto/org_shards.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				DataModelTable:        true,
				Description:           "This table tracks the database shards that a org uses for a given data type.",
				ColumnDescriptions: map[string]string{
					"org_id":                           metadatahelpers.OrgIdDefaultDescription,
					"shard_type":                       "The type of shard (Database, KinesisStats, etc.).",
					"data_type":                        "The data type of the org shard row.",
					"shard_name":                       "The name of the shard that the org shard row conxnects to. This maps to a database host in our config.",
					"read_only":                        "Whether the org shard is read only (writes will error).",
					"status_proto":                     "Contains various metadata about the org shard row.",
					"status_proto.kinesis_stats":       "Keeps track of the migration state of a KinesisStats shard",
					"status_proto.kinesis_stats.state": "Contains metadata about the current state of the migration",
					"status_proto.kinesis_stats.secondary_cluster_id":     "shard_name of the target cluster",
					"status_proto.kinesis_stats.updated_at_ms":            "timestamp (ms) of the last time the state was updated",
					"status_proto.kinesis_stats.orgmover_checkpoint":      "orgmover_checkpoint is used by the orgmover to keep track of progress for tasks that span multiple Poll() calls.The keys are kinesis stream shard ids and the values are the ids of kinesisstats series.",
					"status_proto.kinesis_stats.orgmover_shard_series":    "orgmover_shard_series maps kinesisstats shard ids to the ids of series that are used by the orgmover to verify that it's safe to begin copying data.",
					"status_proto.kinesis_stats.batch_job_id":             "batch_job_id is the job id of the s3 batch job that copies archive files from the source cluster to the target cluster, it can only be set by orgmover in COPY_ARCHIVE state and read in WAIT_FOR_BATCH_JOB state.",
					"status_proto.kinesis_stats.wait_duration_ms":         "wait_duration_ms is the milliseconds for which the org_mover must wait before processing the current state.",
					"status_proto.kinesis_stats.orgmover_writes_start_ms": "orgmover_writes_start_ms records the timestamp at which the orgmover started issuing writes for the series in orgmover_shard_series.",
					"status_proto.kinesis_stats.source_cluster_id":        "source_cluster_id is the source cluster from which the current cluster migrates from. It should only be used by orgmover when deleting data from source cluster, so it is an empty string expect in removal states.",
					"status_proto.read_org_shard_from_cloud_db":           "Whether the org should read org shard values from Cloud DB. By default, this is cached in ShardMapper.",
					"status_proto.db_migration_status":                    "Contains metadata about a migratedbdata migration. Used as a lock of sorts.",
					"status_proto.db_migration_status.source_shard_name":  "The source shard of the current migratedbdata migration.",
					"status_proto.db_migration_status.target_shard_name":  "The target shard of the current migratedbdata migration.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"organizations": {
				primaryKeys:    []string{"id"},
				DataModelTable: true,
				ProtoSchema: ProtobufSchema{
					"settings_proto": {
						Message:   &orgproto.OrganizationSettings{},
						ProtoPath: "go/src/samsaradev.io/settings/orgproto/organization_settings.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(3),
				cdcVersion_DO_NOT_SET: TableVersion(3),
				Production:            true,
				Description:           "This table contains configuration data and metadata associated with a Samsara customer",
				ColumnDescriptions: map[string]string{
					"id":                                 metadatahelpers.OrgIdDefaultDescription,
					"name":                               "Name for the organization.",
					"created_at":                         "When the organization was created.",
					"updated_at":                         "The last time the organization was updated.",
					"temp_units":                         "True is Celsius, False is Fahrenheit.",
					"join_fleet_token":                   metadatahelpers.DeprecatedDefaultDescription,
					"login_domain":                       "Admin created name for drivers to use to sign into the driver app.",
					"unit_system_override":               metadatahelpers.DeprecatedDefaultDescription,
					"message_push_notifications_enabled": "An organization setting. Turns on push notifications on the Driver App.",
					"trailer_selection_enabled":          "An organization setting. Turns on trailer selection on the Driver App.",
					"wifi_hotspot_data_cap_bytes_per_gateway": "The data cap when using VGs as a wifi hotspot, per device, in bytes",
					"message_text_to_speech_enabled":          "An organization setting. Turns on text to speech on the driver app.",
					"wake_on_motion_enabled":                  "An organization setting. Enables wake-on-motion pinging for AG 45 devices.",
					"rollout_stage":                           metadatahelpers.DeprecatedDefaultDescription,
					"driver_trailer_creation_enabled":         "An organization setting. Allows trailers (devices) to be created by drivers from the driver app.",
					"max_num_of_trailers_selected":            "An organization setting. Limits number of trailers a driver can select.",
					"internal_type":                           metadatahelpers.EnumDescription("An numerical ENUM of an organization's internal type setting ([See here](https://paper.dropbox.com/doc/Mini-RFC-Marking-orgs-as-internal--BfeBtVnnqf0CR_nZH7QSU3bYAg-xbCpxvoJoAkF2UJpqQCA6))", models.OrgInternalTypes),
					"is_early_adopter":                        metadatahelpers.DeprecatedDescription("See release_type_enum instead."),
					"release_type_enum":                       metadatahelpers.EnumDescription("The numerical ENUM value of an organization's release track setting ([See here](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/PROD/pages/17905798/Release+Tracks))", releasemanagementproto.OrgReleaseType_name),
				},
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"preventative_maintenance_schedules": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table tracks maintenance schedules that are associated with one or more vehicles and may be based on date, engine hours or odometer.",
				ColumnDescriptions: map[string]string{
					"id":              "Unique ID for this schedule",
					"group_id":        metadatahelpers.GroupIdDefaultDescription,
					"title":           "The user-provided maintenance schedule title",
					"description":     "The user-provided maintenance schedule description",
					"marshaled_proto": "An instance of maintenanceproto.PreventativeMaintenanceSchedule. The interval associated with the schedule, which may be by date, engine hours or odometer.",
				},
				ProtoSchema: ProtobufSchema{
					"marshaled_proto": {
						Message:   &maintenanceproto.PreventativeMaintenanceSchedule{},
						ProtoPath: "go/src/samsaradev.io/helpers/maintenanceproto/preventative_maintenance.proto",
					},
				},
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"sfdc_accounts": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				DataModelTable:        true,
				Description:           "This table contains information on customers SFDC (Salesforce Dot Com) Accounts.",
				ColumnDescriptions: map[string]string{
					"id":              "Unique identifier for the row.",
					"sfdc_account_id": "This is the unique Id for the account in Salesforce.",
					"sam_number":      metadatahelpers.SamNumberDefaultDescription,
					"created_at":      "When this row was created.",
					"updated_at":      "When this row was updated.",
					"segment":         "Segment that the customer is in. Examples of the set of values are (‘Enterprise’, ‘Mid Market’, etc).",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"slave_devices": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"connection_parameters_proto": {
						Message:   &industrialcoreproto.SlaveDeviceConnectionParameters{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/slave_device.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"slave_pins": {
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialcoreproto.SlavePinConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/slave_pin.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"stat_mappings": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &industrialcoreproto.StatMappingConfiguration{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/machine_input.proto",
					},
				},
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_addresses": {
				primaryKeys:       []string{"address_id", "tag_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table associates Tags with Addresses.",
				ColumnDescriptions: map[string]string{
					"tag_id":     "ID of the Tag",
					"address_id": "ID of the Address",
					"created_by": "UserID of the user who created the association",
					"created_at": "When the association was created",
				},
			},
			"tag_drivers": {
				primaryKeys:                       []string{"driver_id", "tag_id"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "This table maps tags to drivers.",
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_workforce_camera_devices": {
				primaryKeys:           []string{"org_id", "camera_device_id", "tag_id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "This table associates Tags with specific Site CameraDevices.",
				ColumnDescriptions: map[string]string{
					"tag_id":           "ID of the Tag (in CloudDB Tags table)",
					"camera_device_id": "ID of the CameraDevice (in WorkforceVideoDB CameraDevices table)",
					"created_by":       "UserID of the user who created the association",
					"created_at":       "When the association was created",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_workforce_sites": {
				primaryKeys:           []string{"org_id", "workforce_site_uuid", "tag_id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "This table associates Tags with specific 'Sites' (virtual rep. of physical location w/ Sites setup).",
				ColumnDescriptions: map[string]string{
					"tag_id":              "ID of the Tag (in CloudDB Tags table)",
					"workforce_site_uuid": "UUID of the Site (in WorkforceVideoDB Sites table)",
					"created_by":          "UserID of the user who created the association",
					"created_at":          "When the association was created",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tags": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"user_login_infos": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				versionInfo:       SingleVersion(1),
				DataModelTable:    true,
				Description:       "This table contains login information for Samsara users.",
				ColumnDescriptions: map[string]string{
					"id":                    "Unique identifier for this row.",
					"email":                 "Email address associated with this login record.",
					"created_at":            "When this row was created.",
					"updated_at":            "When this row was last updated.",
					"hashed_password":       "Hashed password for this login record (if not using SSO).",
					"google_oauth2":         "Boolean indicating if this login uses Google OAuth2.",
					"email_lower":           "Lowercased version of the email, used for indexing.",
					"is_auth0":              "Boolean indicating if this login uses Auth0 SSO.",
					"auth0_connection_name": "The name of the Auth0 connection associated with this login.",
					"remember_me":           "Boolean indicating if 'remember me' is enabled for this login.",
				},
			},
			"user_login_regions": {
				primaryKeys:       []string{"user_login_id", "region_id"},
				PartitionStrategy: SinglePartition,
				versionInfo:       SingleVersion(1),
				DataModelTable:    true,
				Description:       "This table contains the regions associated with each user login.",
				ColumnDescriptions: map[string]string{
					"user_login_id": "Key to the user_login_infos table for this mapping.",
					"region_id":     "Key to the regions table for this mapping.",
					"created_at":    "When this row was created.",
					"updated_at":    "When this row was last updated.",
				},
			},
			"users": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				DataModelTable:        true,
				Description:           "This table contains information Samsara users.",
				ColumnDescriptions: map[string]string{
					"id":                         "Unique identifier for this row.",
					"email":                      "Email contact for this user.",
					"hashed_password":            "Hashed password for this user (If they are not SSO).",
					"created_at":                 "When this row was created.",
					"updated_at":                 "When this row was updated.",
					"google_name":                "The user's name in their linked Google Account, if they use Google OAuth2 to sign in.",
					"google_id":                  "A unique google ID, if they use Google OAuth2 to sign in.",
					"google_picture_url":         "A url to the user's google profile picture, if they use Google OAuth2 to sign in.",
					"google_oauth2":              "A boolean representing if they use Google OAuth2 to sign in.",
					"email_lower":                "User's email in lowercase.",
					"name":                       "User's name.",
					"picture_url":                "URL for the user's profile picture.",
					"is_auth0":                   "Does this user use SSO for sign in?",
					"language_override":          "The language to show the dashboard in. If not set, will use whatever language is associated with the organization that the user is currently looking at.",
					"unit_system_override":       "The unit to show the dashboard in. If not set, will use whatever units are associated with the organization that the user is currently looking at",
					"timezone_override":          "The time zone to show the dashboard in. If not set, will use whatever time zone is associated with the organization that the user is currently looking at",
					"phone":                      "The user's phone number.",
					"use_whatsapp":               "Does the user use whats app for messaging?",
					"whatsapp_confirmation_sent": "Did we send a confirmation email to the user to use whatsapp?",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"users_organizations": {
				primaryKeys:           []string{"id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Production:            true,
				DataModelTable:        true,
				Description:           "This table contains a mapping between users and orgs. If a user has multiple roles within an organization, they will have multiple rows within this table.",
				ColumnDescriptions: map[string]string{
					"id":               "Unique identifier for this row.",
					"user_id":          "Key to the users table for this mapping.",
					"organization_id":  "Key to the organizations table for this mapping.",
					"role_id":          "If this is non zero, then it is the built in role (e.g Full Admin, Read only admin, etc) that this user has. If zero, it means that they have a custom role (see custom_role_uuid). A row in this table can either have a built in role or a custom role, but not both. You can find a list of roles and the corresponding ids here: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/iam/authz/roles.go",
					"created_at":       "When this row was created.",
					"updated_at":       "When this row was updated.",
					"driver_id":        "Key to the drivers table for this row. This can also map a driver to an org.",
					"group_id":         "The group id for this org.",
					"tag_id":           "If the user's role is scoped to a tag, then this will be not null and set to the tag ID that the role is scoped to (which can be joined to the tags table). If it is scoped to the entire organization, then this will be null.",
					"custom_role_uuid": "Key to the custom_roles table that this user has for this org. A row in this table can either have a built in role or a custom role, but not both.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_electrification_type": {
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				Description:       "The table contains information about Make, Model, Year, VIN and electrification parameters.",
				ColumnDescriptions: map[string]string{
					"device_id":             metadatahelpers.DeviceIdDefaultDescription,
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"vin":                   "The vin of the vehicle.",
					"make":                  "The make of the vehicle (from NHTSA).",
					"model":                 "The model of the vehicle (from NTHSA).",
					"year":                  "The year of the vehicle (from NHTSA).",
					"electrification_level": "The electrification level of the vehicle (from NHTSA).",
					"primary_fuel_type":     "The primary fuel type of the vehicle (from NHTSA).",
					"secondary_fuel_type":   "The secondary fuel type of the vehicle (from NHTSA).",
					"updated_at":            "The timestamp when this entry was last updated.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_maintenance_logs": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Log entries created by a user either by resolving a scheduled maintenance entry for a vehicle or by creating a manual log entry",
				ColumnDescriptions: map[string]string{
					"id":                                   "The unique identifier for the log. Auto-incrementing",
					"text":                                 "The text entered for the maintenance log",
					"group_id":                             metadatahelpers.GroupIdDefaultDescription,
					"vehicle_name":                         "The name of the vehicle associated with the log",
					"vehicle_id":                           "The device ID of the vehicle associated with the log",
					"vehicle_odometer":                     "The odometer reading as entered by the user",
					"serviced_at":                          "The time the service occurred as entered by the user",
					"created_at":                           "The time the log was created",
					"created_by":                           "The ID of the user who created the log",
					"created_by_email":                     "The email of the user who created the log",
					"updated_at":                           "The time the log was last updated",
					"updated_by":                           "The ID of the user who last updated the log",
					"deleted":                              "If one, the log is deleted. If zero, it's not deleted",
					"deleted_by":                           "The ID of the user who deleted the log, if applicable",
					"deleted_at":                           "The time the log was deleted, if applicable",
					"preventative_maintenance_schedule_id": "The ID of the maintenance schedule this log is associated with, if applicable",
					"preventative_maintenance_schedule_title": "The title of the maintenance schedule this log is associated with, if applicable",
					"cost_cents":   "The cost of the service in cents as entered by the user",
					"engine_hours": "The engine hours at the time of service as entered by the user",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_make_model_weights": {
				primaryKeys:           []string{"make", "model"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description: "Contains the gross vehicle weight rating (GVWR) for makes and models " +
					"as specified by the Federal Highway Administration.",
				ColumnDescriptions: map[string]string{
					"make":  "The make of the vehicle",
					"model": "The model of the vehicle",
					"gross_vehicle_weight_rating": "The US GVWR classification of the vehicle. " +
						"This contains the truck class and weight limit.",
					"max_weight_lbs": "The maximum operating weight of the vehicle as specified by the manufacturer. " +
						"This includes the vehicle's body, fuel, driver, passengers, and cargo, but not any trailers.",
					"updated_at":     "The timestamp at which the row was last updated.",
					"updated_by_vin": "The VIN used in VIN decoding that supplied us the given weight information.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"webhooks": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-supplychain-ops":        {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-legal":                  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-cs-sis":                 {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.CmAssetsDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/cmassetsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// Cmassets receives consistent traffic, and is extremely hard to reload so we want to aggressively
		// monitor the throughput.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"asset_urls": {
				primaryKeys:       []string{"org_id", "device_id", "asset_ms", "filename", "s3_bucket"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"detail": {
						Message:   &asseturlsproto.AssetUrlDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/cmassetsmodels/asseturlsproto/asset_url_detail.proto",
					},
				},
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Description: "The asset_urls table contains information about safety videos and " +
					"stills stored in S3. This serves as an index into s3 so that we don't have to " +
					"perform lookups via the S3 API to know what assets exist. The data here " +
					"only begins from ~March 2021 so if you need to query for assets before then, you'll " +
					"need to look directly at S3.",
				ColumnDescriptions: map[string]string{
					"asset_ms":   "The timestamp (ms) that the asset is uploaded.",
					"created_at": "The timestamp (ms) that this row was created in the database",
					"detail": "This proto blob contains assorted information that might be useful metadata. " +
						"These fields are mostly optional.",
					"detail.camera_ids": "The camera IDs that are associated with the asset. Only used " +
						"for multicam systems.",
					"detail.end_ms":     "This field contains the end_ms of video assets if it is populated",
					"detail.resolution": "The resolution of the asset uploaded",
					"detail.user_has_no_permissions": "This is only used on the read path to store a " +
						"temporary state variable as we check to see whether users have access to this asset.",
					"device_id": "The device that this asset belongs to. Note that assets from dashcam and multicam " +
						"devices will have separate device IDs even if they belong to the same vehicle.",
					"filename": "The file name of the asset that can be found in s3",
					"org_id":   metadatahelpers.OrgIdDefaultDescription,
					"url_type": "The type of the asset uploaded. You can look in safetys3#urls.go for which " +
						"int type here means.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(24),
						},
						infraconsts.SamsaraAWSEURegion: {
							MaxWorkers: pointer.IntPtr(4),
							ShardOverrides: []RdsJobShardOverrides{
								{
									ShardNumber: pointer.IntPtr(1),
									MaxWorkers:  pointer.IntPtr(8),
								},
							},
						},
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"camera_assets": {
				primaryKeys: []string{"org_id", "device_id", "asset_ms", "asset_type", "recall_id"},
				ProtoSchema: ProtobufSchema{
					"camera_event_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"camera_event"},
					},
					"uploaded_file_set_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"uploaded_file_set"},
					},
					"dashcam_report_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"dashcam_report"},
					},
				},
				InternalOverrides: RdsTableInternalOverrides{
					JsonpbInt64AsDecimalString: true,
				},
				PartitionStrategy: DateColumnFromMilliseconds("asset_ms"),
				Description:       "The camera_assets table contains information about uploaded assets from camera devices.",
			},
			"camera_upload_requests": {
				primaryKeys: []string{"org_id", "device_id", "asset_ms"},
				ProtoSchema: ProtobufSchema{
					"request_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"dashcam_report"},
					},
				},
				InternalOverrides: RdsTableInternalOverrides{
					JsonpbInt64AsDecimalString: true,
				},
				PartitionStrategy: DateColumnFromMilliseconds("asset_ms"),
				Description: "The camera_upload_requests table contains information about upload requests made from our backend " +
					"to VGs/CMs. These typically correspond to safety event videos and stills that our backend requests automatically, " +
					"and not user initiated requests.",
				ColumnDescriptions: map[string]string{
					"org_id": metadatahelpers.OrgIdDefaultDescription,
					"device_id": "The device that this asset belongs to. Note that assets from dashcam and multicam " +
						"devices will have separate device IDs even if they belong to the same vehicle.",
					"asset_ms":         "The timestamp (ms) of the asset being uploaded.",
					"state_updated_at": "The last timestamp (ms) that the state was updated.",
					"state":            "The state of the camera upload requests. Pending, requested, or completed.",
				},
			},
			"dashcam_assets": {
				primaryKeys: []string{"org_id", "device_id", "asset_ms"},
				ProtoSchema: ProtobufSchema{
					"dashcam_report_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"dashcam_report"},
					},
					"uploaded_file_set_proto": {
						Message:          &hubproto.ObjectStatBinaryMessage{},
						ProtoPath:        "go/src/samsaradev.io/hubproto/object_stat.proto",
						NestedProtoNames: []string{"uploaded_file_set"},
					},
				},
				InternalOverrides: RdsTableInternalOverrides{
					JsonpbInt64AsDecimalString: true,
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(16),
						},
						infraconsts.SamsaraAWSEURegion: {
							MaxWorkers: pointer.IntPtr(8),
						},
					},
				},
				PartitionStrategy: DateColumnFromMilliseconds("asset_ms"),
				Description:       "The dashcam_assets table contains information about uploaded assets from dashcam devices.",
			},
			"dashcam_assets_migration_progress": {
				primaryKeys: []string{"org_id"},
				InternalOverrides: RdsTableInternalOverrides{
					JsonpbInt64AsDecimalString: true,
				},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
				Description:                       "The dashcam_assets_migration_progress table keeps track of how far each organization has gotten in the dashcam_assetsbackfill process",
			},
			"inference_results": {
				primaryKeys:                       []string{"org_id", "device_id", "asset_ms", "uuid"},
				PartitionStrategy:                 DateColumnFromMilliseconds("asset_ms"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-legal":             {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.CoachingDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/coachingmodels/db_schema.sql",
		Sharded:    true,
		// Coaching receives some, but not a lot, of traffic every day. Shard 1 in the EU appears unused.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usNonProdShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"coach_assignments": {
				primaryKeys:                       []string{"org_id", "driver_id"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coachable_item": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coachable_item_disputes": {
				primaryKeys:       []string{"org_id", "coachable_item_uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores dispute workflow metadata for disputed coachable items",
			},
			"coachable_item_share": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				Description:                       "Stores coachable item sharing metadata",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coaching_session_notes": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				Description:                       "Stores information about comments made on coaching sessions",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coaching_session_reminders": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("sent_at"),
				Description:                       "Stores reminder metadata for coaching sessions. Currently unused but slated for use in FY23Q4.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coaching_session_signatures": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores signature metadata for coaching sessions",
				versionInfo:       SingleVersion(1),
			},
			"coaching_sessions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"metadata": {
						Message:   &coachingproto.CoachingSessionMetadata{},
						ProtoPath: "go/src/samsaradev.io/safety/coaching/coachingproto/coachingsession.proto",
					},
				},
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"coaching_sessions_behavior": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_streaks": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores information about driver streaks (e.g. consecutive hours of safe driving)",
			},
			"organization_settings": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: DateColumnFromTimestamp("updated_at"),
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"organization_setting": {
						Message:   &coachingproto.OrganizationSettings{},
						ProtoPath: "go/src/samsaradev.io/safety/coaching/coachingproto/organization_settings.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"positive_recognition_milestones": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores positive recognition milestones for drivers",
				versionInfo:       SingleVersion(1),
			},
			"positive_recognition_share_comments": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores comments made on positive recognition shares/kudos.",
			},
			"positive_recognition_share_tokens": {
				primaryKeys:       []string{"org_id", "token"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores share tokens for positive recognition shares, enabling public access via token URLs",
			},
			"positive_recognition_shares": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores information about positive recognition shares/kudos, which are public recognitions of good behavior from admins to drivers.",
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.ComplianceDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/compliancemodels/db_schema.sql",
		Sharded:    true,
		// Compliancedb receives consistent traffic on all shards in US region.
		// EU region receives less traffic and hence the threshold is set to alert when no traffic is observed in 2 days.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      euShardMonitors(ShardMonitor{TimeframeMinutes: 2880}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"auto_duty_events": {
				primaryKeys:                       []string{"org_id", "driver_id", "event_type", "timestamp", "vehicle_id", "deleted_at"},
				PartitionStrategy:                 DateColumnFromTimestamp("timestamp"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"autoduty_data_processed": {
				primaryKeys:       []string{"org_id", "device_id", "version", "data_type"},
				PartitionStrategy: SinglePartition,
				Description:       "This table tracks the latest timestamp of object stats processed by autoduty",
				ColumnDescriptions: map[string]string{
					"data_type":       metadatahelpers.EnumDescription("The type of object stat we have processed", eldconst.AutoDutyDataTypesMap),
					"processed_until": "The timestamp of the latest object stat",
				},
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(16),
						},
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"autoduty_progress": {
				primaryKeys:       []string{"org_id", "vehicle_id", "version"},
				Description:       "This table stores the high watermarks for incremental autoduty runs",
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"processed_until": "The timestamp we have processed autoduty until for this device",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"data_transfer_audit_logs": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 DateColumnFromTimestamp("requested_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_carrier_entries": {
				primaryKeys:                       []string{"org_id", "driver_id", "log_at_ms"},
				PartitionStrategy:                 DateColumnFromMilliseconds("log_at_ms"),
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_hos_audit_logs": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: DateColumnFromMilliseconds("updated_at_ms"),
				Description:       "This table tracks all the actions taken by drivers, admins, and autoduty that modify HOS logs and vehicle assignments.",
				ColumnDescriptions: map[string]string{
					"old_vehicle_id": "Stores the previous vehicle id of the HOS log/vehicle assignment before this action",
					"new_vehicle_id": "Stores the current vehicle id of the HOS log/vehicle assignment after this action",
					"status_code":    "The duty status on the HOS log if this action is related to a HOS log",
					"log_at_ms":      "The event that this action is referencing, e.g. the original timestamp of the HOS log or vehicle assignment",
					"action":         "An enum representing what action took place",
					"updated_at_ms":  "The timestamp of when the action took place",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_hos_charts": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 DateColumnFromTimestamp("start_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
				ProtoSchema: ProtobufSchema{
					"vehicle_assignments": {
						Message:   &complianceproto.HosVehicleAssignments{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/driver_hos_logs.proto",
					},
					"remarks": {
						Message:   &complianceproto.HosRemarks{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/driver_hos_logs.proto",
					},
					"chart_proto": {
						Message:   &complianceproto.DriverHosChartProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/driver_hos_chart.proto",
					},
					"certification_events": {
						Message:   &complianceproto.CertificationEvents{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/driver_hos_logs.proto",
					},
				},
				versionInfo: SingleVersion(1),
			},
			"driver_hos_exemption_claim_events": {
				primaryKeys:       []string{"org_id", "driver_id", "applies_at", "client_created_at", "exemption_type_id"},
				PartitionStrategy: DateColumnFromTimestamp("applies_at"),
				Description:       "Records the exemption claim events for drivers.",
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"driver_id":         metadatahelpers.DriverIdDefaultDescription,
					"applies_at":        "The time that the exemption applies to.",
					"exemption_type_id": "The type of exemption claim",
					"remark":            "User-entered text about the exemption claim",
				},
			},
			"driver_hos_logs": {
				primaryKeys: []string{"org_id", "id"},
				ProtoSchema: ProtobufSchema{
					"log_proto": {
						Message:   &complianceproto.DriverHosLogMetadata{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/driver_hos_logs.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("log_at"),
				DataModelTable:    true,
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(8),
						},
					},
					CronHourSchedule: "*/1", // ACR uses this for customer facing reports and needs faster data freshness.
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_hos_settings": {
				Description:                       "Driver settings relating to compliance and hours of service",
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_hos_vehicle_assignments": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 DateColumnFromTimestamp("start_at"),
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"eld_audit_actions": {
				primaryKeys: []string{"org_id", "action_type", "vehicle_id", "server_action_at", "driver_id", "eld_event_start_at"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &complianceproto.EldAuditActionProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/compliance_service.proto",
						// eld_audit_actions's proto column can contain invalid UTF-8 strings,
						// which is rejected by our protobuf parsing library (converting bytes
						// into a java object).
						EnablePermissiveMode: true,
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("server_action_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(2),
			},
			"eld_malfunction_and_diagnostic_critical_events": {
				primaryKeys:       []string{"org_id", "vehicle_id", "event_at", "server_deleted_at", "critical_event_type"},
				PartitionStrategy: DateColumnFromTimestamp("event_at"),
				Description:       "This tables holds persisted critical events which are used to calculate diagnostics & malfunctions",
				ColumnDescriptions: map[string]string{
					"critical_event_type": metadatahelpers.EnumDescription("A code representing the type of critical event", malfunction_diagnostic.EldMalfunctionAndDiagnosticCriticalEventTypeAmundsenMapping),
				},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &complianceproto.EldMalfunctionAndDiagnosticCriticalEventProtoData{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/eld_malfunctions_and_diagnostics.proto",
					},
				},
			},
			"eld_malfunction_or_diagnostic_events": {
				primaryKeys:       []string{"org_id", "vehicle_id", "deleted_at", "malfunction_or_diagnostic_code", "event_at", "driver_id"},
				PartitionStrategy: DateColumnFromTimestamp("event_at"),
				Description:       "This table holds persisted malfunction and diagnostic events generated for vehicles and drivers.",
				ColumnDescriptions: map[string]string{
					"event_code":                     metadatahelpers.EnumDescription("A code representing whether or not the event was either a diagnostic or malfunction which either was logged or which was cleared", eldconst.EventCodeForMalfunctionOrDataDiagnosticDetectionAmundsenDescriptionMapping),
					"malfunction_or_diagnostic_code": metadatahelpers.EnumDescription("A code representing the type of diagnostic/malfunction", malfunction_diagnostic.MalfunctionDiagnosticAmundsenDescriptionMapping),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(2),
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &complianceproto.MalfunctionOrDiagnosticEventProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/complianceproto/eld_malfunctions_and_diagnostics.proto",
					},
				},
			},
			"eld_malfunction_or_diagnostic_metadata": {
				primaryKeys:       []string{"org_id", "vehicle_id", "malfunction_or_diagnostic_code", "driver_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table holds high watermark information about malfunction and diagnostic event resolutions",
				ColumnDescriptions: map[string]string{
					"malfunction_or_diagnostic_code": metadatahelpers.EnumDescription("A code representing the type of diagnostic/malfunction", malfunction_diagnostic.MalfunctionDiagnosticAmundsenDescriptionMapping),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"odometer_jumps": {
				Description:       "This table contains odometer jumps detected on VG during power off intervals.",
				primaryKeys:       []string{"org_id", "vehicle_id", "odometer_jump_at"},
				PartitionStrategy: DateColumnFromTimestamp("odometer_jump_at"),
				versionInfo:       SingleVersion(2),
				ColumnDescriptions: map[string]string{
					"org_id":                      metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":                  metadatahelpers.DeviceIdDefaultDescription,
					"odometer_jump_at":            "The timestamp at which the odometer jump was detected",
					"vg_on_odometer_meters":       "The odometer value (in meters) detected after the vg had been turned on",
					"lat":                         "The latitude of the location where the odometer jump occurred",
					"lon":                         "The longitude of the location where the odometer jump occurred",
					"location_description":        "The description of the location where the odometer jump occurred",
					"vg_off_odometer_at":          "The timestamp of the last odometer reading before the vg had been turned off",
					"vg_off_odometer_meters":      "The odometer value (in meters) detected before the vg had been turned off",
					"vg_off_lat":                  "The latitude of the last location associated to the vehicle before the vg had been turned off",
					"vg_off_lon":                  "The longitude of the last location associated to the vehicle before the vg had been turned off",
					"vg_off_location_description": "The description of the last location associated to the vehicle before the vg had been turned off",
				},
				ExcludeColumns: []string{
					"vg_off_driver_id", // This column is not populated in the database.
				},
			},
			"org_carriers": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromMilliseconds("created_at_ms"),
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_regulation_mode_config_time_ranges": {
				primaryKeys:       []string{"org_id", "vehicle_id", "start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				Description:       "This table contains the regulation mode configurations for vehicles and the timeranges the vehicle had those configurations, for example if the vehicle was regulated or unregulated from time t1 to time t2. This table is part of the Mixed Use Unregulated Vehicles project.",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":      metadatahelpers.DeviceIdDefaultDescription,
					"regulation_mode": metadatahelpers.EnumDescription("The regulation mode", complianceproto.VehicleRegulationMode_name),
					"start_ms":        "The epoch ms timestamp of when the vehicle was set to the regulation mode",
					"end_ms":          "The epoch ms timestamp of when the vehicle was changed to another regulation mode. If this is null, the timerange is still ongoing to the present.",
					"created_at_ms":   "The epoch ms timestamp of when this timerange was created. Usually around the same time as start_ms.",
					"updated_at_ms":   "The epoch ms timestamp of when the timerange ended. This is null if the timerange is ongoing. Usually around the same time as end_ms.",
					"uuid":            "The uuid of the timerange.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_regulation_mode_driver_time_ranges": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				Description:       "This table contains the timeranges that a mixed use unregulated vehicle was regulated. Note that the start_ms/end_ms in this table are mutable which is why we have created_at_ms as our partition. This table is part of the Mixed Use Unregulated Vehicles project.",
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":    metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":     metadatahelpers.DriverIdDefaultDescription,
					"start_ms":      "The epoch ms timestamp of when the vehicle was set to regulated.",
					"end_ms":        "The epoch ms timestamp of when the vehicle was set back to unregulated. If this is null, the timerange is still ongoing to the present.",
					"created_at_ms": "The epoch ms timestamp of when this timerange was created.",
					"updated_at_ms": "The epoch ms timestamp of when the timerange ended. This is null if the timerange is ongoing.",
					"uuid":          "The uuid of the timerange.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.CsvUploadsDB,
		schemaPath: "go/src/samsaradev.io/platform/csvuploads/csvuploadsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"csv_uploads": {
				primaryKeys:       []string{"org_id", "uuid"},
				Description:       "The CSV Uploads table contains all CSV uploaded files for the platform uploaders",
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"uuid":                     "The UUID of the CSV upload",
					"entity_type":              metadatahelpers.EnumDescription("The type uploader", csvuploadsproto.EntityType_name),
					"created_by":               "User ID of the person who uploaded the file",
					"created_at":               "The timestamp of when the CSV was created",
					"updated_at":               "The timestamp of when the CSV was last updated",
					"file_name":                "The name of the file the user uploaded",
					"status":                   metadatahelpers.EnumDescription("The status of the file", csvuploadsproto.Status_name),
					"aws_s3_key_submitted_csv": "The key of the unprocessed AWS file",
					"aws_s3_key_processed_csv": "The key of the processed AWS file",
					"chunks_completed":         "The number of chunks complete",
					"total_rows_submitted":     "The total number of rows",
					"total_rows_diffs":         "The total number of rows containing differences",
					"total_rows_created":       "The total number of rows containing new entities",
					"total_rows_updated":       "The total number of rows containing updated entities",
					"total_rows_errored":       "The total number of rows containing errors",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.DataPlatformTestInternalDB,
		schemaPath: "go/src/samsaradev.io/infra/dataplatform/dataplatformtestinternalmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
			EnableKinesisStreamDestination: true,
		},
		CanReadGroups: []components.TeamInfo{
			team.DataPlatform,
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"json_notes": {
				primaryKeys:       []string{"org_id", "id"},
				Description:       "This table contains arbitrary test data.",
				PartitionStrategy: SinglePartition,
				allowedRegions:    []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSCARegion},
			},
			"notes": {
				primaryKeys:       []string{"org_id", "id"},
				Description:       "This table contains arbitrary test data.",
				PartitionStrategy: SinglePartition,
				allowedRegions:    []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSCARegion},
				ExcludeColumns:    []string{"note"},
				versionInfo:       SingleVersion(1),
			},
		},
	},
	{
		name:       dbregistry.DeployDB,
		schemaPath: "go/src/samsaradev.io/infra/deploymachinery/deploymodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"deploy_state": {
				primaryKeys:                       []string{"filename"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "deploy_state holds the current state of our deployment systems. ",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
			},
		},
	},
	{
		name:       dbregistry.DetentionDB,
		schemaPath: "go/src/samsaradev.io/fleet/detention/detentionmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"detention": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "A table to track device detentions at specific addresses.",
				ColumnDescriptions: map[string]string{
					"uuid":           "The unique identifier of the detention record.",
					"status":         "The status of the detention record",
					"detention_type": "The type of detention (0=UNKNOWN, 1=GEOFENCE, encoded as tinyint).",
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"device_id":      "The ID of the device involved in the detention.",
					"address_id":     "The ID of the address where the detention occurred.",
					"start_time":     "The time when the detention started.",
					"end_time":       "The time when the detention ended.",
					"created_at":     "The time when this record was created.",
					"updated_at":     "The time when this record was last updated.",
					"billing_settings_grace_period_in_seconds":        "The grace period in seconds before detention charges apply.",
					"billing_settings_grace_period_in_seconds_source": "The source of the grace period setting (0=DEFAULT, 1=SELF, 2=PARENT).",
					"billing_settings_day_rate_in_cents":              "The daily rate charged for detention in cents.",
					"billing_settings_day_rate_in_cents_source":       "The source of the day rate setting (0=DEFAULT, 1=SELF, 2=PARENT).",
					"billing_settings_valid_from":                     "The start time from which these billing settings are valid.",
					"billing_settings_valid_to":                       "The end time until which these billing settings are valid.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.DevelopersDB,
		schemaPath: "go/src/samsaradev.io/infra/api/developers/developersmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"developer_notification_subscribers": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"details": {
						Message:   &developersproto.SubscriberDetails{},
						ProtoPath: "go/src/samsaradev.io/infra/api/developers/developersproto/developer_subscriptions.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"developer_notification_subscriptions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"details": {
						Message:   &developersproto.SubscriptionDetails{},
						ProtoPath: "go/src/samsaradev.io/infra/api/developers/developersproto/developer_subscriptions.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"developer_test_org_association": {
				primaryKeys:       []string{"uuid", "org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "A table to track the developer org and its test org associations.",
				ColumnDescriptions: map[string]string{
					"uuid":        "The uuid of the org association.",
					"org_id":      metadatahelpers.OrgIdDefaultDescription,
					"test_org_id": "The org id of the test org in the association.",
					"status":      "The status of the org.",
					"created_at":  "The date this association was created on.",
					"updated_at":  "The date this association was updated on.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"developer_app_marketplace_data": {
				primaryKeys:       []string{"org_id", "app_uuid", "app_version", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "A table to track the developer app marketplace data.",
				ColumnDescriptions: map[string]string{
					"uuid":                  "The uuid of the app marketplace data.",
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"app_uuid":              "The uuid of the app.",
					"geography":             "The geography of the app.",
					"industry":              metadatahelpers.EnumDescription("The industry of the app.", developersproto.AppMarketplaceIndustry_name),
					"pricing_model":         metadatahelpers.EnumDescription("The pricing model of the app.", developersproto.AppMarketplacePricingModel_name),
					"tag_line":              "The tag line of the app.",
					"detailed_description":  "The detailed description of the app.",
					"feature_highlights":    "The feature highlights of the app.",
					"benefit_highlights":    "The benefit highlights of the app.",
					"install_instructions":  "The install instructions of the app.",
					"support_instructions":  "The support instructions of the app.",
					"tos_url":               "The terms of service URL of the app.",
					"integration_category":  metadatahelpers.EnumDescription("The integration category of the app.", developersproto.AppMarketplaceIntegrationCategory_name),
					"developer_name":        "The developer name of the app.",
					"version_release_notes": "The version release notes of the app.",
					"app_version":           "The version of the app.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.DeviceAssociationsDB,
		schemaPath: "go/src/samsaradev.io/fleet/deviceassociations/deviceassociationsmodels/db_schema.sql",
		Sharded:    true,
		// Deviceassociations receives almost no writes, so we don't monitor this.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"device_collection_associations": {
				primaryKeys:       []string{"org_id", "collection_uuid", "device_id", "product_id", "start_at"},
				PartitionStrategy: SinglePartition,
				Description: "This table contains information on which multicam devices are associated " +
					"with their VG devices. Each association is grouped in a single collection_uuid. " +
					"This table also contains active and inactive collections and serves as a log of " +
					"all associations that devices have gone through.",
				ColumnDescriptions: map[string]string{
					"collection_uuid": "The unique ID for a collection of devices. A collection " +
						"can contain multicam, and VG devices. Multiple rows can have the same " +
						"collection_uuid. This would indicate that they belong to the same collection.",
					"device_id": "The device ID for the device that is in the collection. For multicam devices, " +
						"the device_id corresponds to the VG (in a medusa system), or the NVR (in a perseus system).",
					"end_at":     "The timestamp (in ms) that the device was removed from the collection.",
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"product_id": "The product ID of the device that is in the collection.",
					"start_at":   "The timestamp (in ms) that the device was added to the collection.",
				},
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.DeviceMetadataDB,
		schemaPath: "go/src/samsaradev.io/fleet/devicemetadata/devicemetadatamodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"digi_types": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Table with registered custom digi types.",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"uuid":       "The uuid of the digi type.",
					"name":       "The name of the digi type.",
					"api_name":   "The api name of the digi type.",
					"created_at": "The timestamp of when the digi type was created.",
					"created_by": "The user id of the user who created the digi type.",
					"updated_at": "The timestamp of when the digi type was last updated.",
					"updated_by": "The user id of the user who last updated the digi type.",
					"deleted_at": "The timestamp of when the digi type was deleted.",
					"deleted_by": "The user id of the user who deleted the digi type.",
					"continuity": "Whether the digi type is continuous.",
				},
			},
		},
	},
	{
		name:       dbregistry.DispatchDB,
		schemaPath: "go/src/samsaradev.io/fleet/dispatch/dispatchmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"dispatch_job_exceptions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				versionInfo:       SingleVersion(1),
				Description:       "Stores exceptions for dispatch jobs.",
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "The UUID of the exception.",
					"dispatch_route_id": "The ID of the dispatch route that the dispatch job is assigned to.",
					"dispatch_job_id":   "The ID of the dispatch job.",
					"exception_type":    "The type of exception.",
					"is_resolved":       "Whether the exception has been resolved.",
					"description":       "The description of the exception.",
					"related_task_id":   "The ID of the related stop task. (optional)",
					"created_at":        "The timestamp of when the exception was created.",
					"resolved_at":       "The timestamp of when the exception was resolved.",
				},
			},
			"dispatch_manual_events": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"event_proto": {
						Message:   &dispatchproto.DispatchRouteEvent{},
						ProtoPath: "go/src/samsaradev.io/fleet/dispatch/dispatchproto/dispatch_routes.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("server_created_at"),
				Production:        false,
				Description:       "Manual events created by drivers and admins, used in dispatch route processing.",
				versionInfo:       SingleVersion(1),
			},
			"dispatch_org_settings": {
				primaryKeys:       []string{"org_id", "setting_type"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores Organization-level settings specific to Routing/Dispatch features.",
				Production:        false,
				ColumnDescriptions: map[string]string{
					"setting_type": metadatahelpers.EnumDescription("Enum corresponding to a particular setting.", dispatchmodels.DispatchOrgSettingTypesMap),
					"value":        "Integer representing the current setting; boolean toggles will show '0' or '1'.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"shardable_dispatch_audit_logs": {
				primaryKeys: []string{"org_id", "id"},
				ProtoSchema: ProtobufSchema{
					"audit_proto": {
						Message:   &dispatchproto.DispatchAuditLog{},
						ProtoPath: "go/src/samsaradev.io/fleet/dispatch/dispatchproto/dispatch_routes.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				Production:                        false,
				Description:                       "Audit logs for routes and jobs, shows route processing results and evolution over time. Same as dispatch_audit_logs, but includes shard-ready schema changes.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(2),
			},
			"shardable_dispatch_jobs": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				Description:                       "Same data as dispatch jobs, but with indices designed to support sharding",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
				versionInfo:                       SingleVersion(2),
				ExcludeColumns:                    []string{"custom_properties"},
				DisableServerless:                 true,
			},
			"shardable_dispatch_routes": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				Description:                       "Same data as dispatch routes, but with indices designed to support sharding",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.DriverAssignmentsDB,
		schemaPath: "go/src/samsaradev.io/platform/locationservices/driverassignment/driverassignmentsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"asset_qr_code_download_metadata": {
				Description:       "Tracks when asset QR codes are downloaded for printing.",
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"device_id":               metadatahelpers.DeviceIdDefaultDescription,
					"last_download_timestamp": "Unix timestamp in seconds of when the QR code was last downloaded.",
					"created_at":              "Time when the record was created.",
					"updated_at":              "Time when the record was last updated.",
				},
			},
			"driver_assignment_metadata": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_assignments": {
				primaryKeys:                       []string{"org_id", "device_id", "driver_id", "assignment_source", "start_at_ms"},
				PartitionStrategy:                 DateColumnFromTimestamp("start_at_ms"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:                dbregistry.DriverDocumentsDB,
		schemaPath:          "go/src/samsaradev.io/fleet/driverdocuments/driverdocumentsmodels/db_schema.sql",
		passwordKeyOverride: "driver_documents_database_password",
		Sharded:             true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_document_audit_logs": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"modification_proto": {
						Message:   &driverdocumentsproto.DriverDocumentModificationProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/driverdocuments/driverdocumentsproto/driver_document_audit_logs.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("modified_at_ms"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_document_pdfs": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("requested_at_ms"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_document_templates": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &driverdocumentsproto.DriverDocumentTemplateProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/driverdocuments/driverdocumentsproto/driver_document_templates.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_documents": {
				primaryKeys: []string{"org_id", "driver_id", "driver_created_at"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &driverdocumentsproto.DriverDocumentProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/driverdocuments/driverdocumentsproto/driver_documents.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromTimestamp("driver_created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.EcoDrivingDB,
		schemaPath: "go/src/samsaradev.io/fleet/ecodriving/ecodrivingmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"asset_efficiency_intervals": {
				Description:       "Table storing EcoDriving intervals.",
				primaryKeys:       []string{"uuid", "org_id"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                                        "The organization associated with the asset efficiency interval.",
					"uuid":                                          "Unique identifier for the asset efficiency interval.",
					"device_id":                                     "The id of the device associated with the asset efficiency interval.",
					"operator_id":                                   "The id of the operator associated with the asset efficiency interval.",
					"start_ms":                                      "The start time of the asset efficiency interval in milliseconds.",
					"duration_ms":                                   "The duration of the asset efficiency interval in milliseconds.",
					"enrichment_due_at":                             "Timestamp of when the asset efficiency interval is due for enrichment.",
					"last_enriched_at":                              "Timestamp of when the asset efficiency interval was last enriched.",
					"total_engage_duration_ms":                      "The total engage duration of the asset efficiency interval in milliseconds.",
					"total_idle_duration_ms":                        "The total idle duration of the asset efficiency interval in milliseconds.",
					"total_unproductive_idle_duration_ms":           "The total unproductive idle duration of the asset efficiency interval in milliseconds.",
					"cost_currency":                                 "The currency of the cost stored.",
					"engage_fuel_consumed_ml":                       "The total amount of fuel consumed during engage events.",
					"engage_fuel_cost_cents":                        "The total cost of the fuel consumed in engage events in cents (currency agnostic).",
					"engage_fuel_emissions_c_o2_grams":              "The total fuel emissions in grams of CO2 during engage events.",
					"productive_idling_fuel_consumed_ml":            "The total amount of fuel consumed during productive idling events.",
					"productive_idling_fuel_cost_cents":             "The total cost of the fuel consumed in productive idling events in cents (currency agnostic).",
					"productive_idling_fuel_emissions_c_o2_grams":   "The total fuel emissions in grams of CO2 during productive idling events.",
					"unproductive_idling_fuel_consumed_ml":          "The total amount of fuel consumed during unproductive idling events.",
					"unproductive_idling_fuel_cost_cents":           "The total cost of the fuel consumed in unproductive idling events in cents (currency agnostic).",
					"unproductive_idling_fuel_emissions_c_o2_grams": "The total fuel emissions in grams of CO2 during unproductive idling events.",
					"total_distance_meters":                         "The total distance traveled in meters during the asset efficiency interval.",
					"pending_fuel_distance":                         "Whether the interval has pending fuel or distance values.",
					"created_at":                                    "Timestamp of when the interval was created.",
					"updated_at":                                    "Timestamp of when the interval was updated.",
					"deleted_at":                                    "Timestamp of when the interval was deleted.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					CronHourSchedule: "*/1",
				},
				versionInfo: SingleVersion(1),
			},
			"daily_operator_efficiency_aggregation": {
				Description:       "Daily aggregated operator efficiency metrics including fuel consumption, emissions, and opportunities for improvement.",
				primaryKeys:       []string{"org_id", "org_timezone_day", "operator_id"},
				PartitionStrategy: DateColumnFromDate("org_timezone_day"),
				ColumnDescriptions: map[string]string{
					"org_id":                                        metadatahelpers.OrgIdDefaultDescription,
					"operator_id":                                   "The id of the operator associated with the daily aggregation.",
					"org_timezone_day":                              "The date in the organization's timezone.",
					"subintervals_list":                             "List of subintervals that make up this daily aggregation.",
					"total_distance_meters":                         "The total distance traveled in meters during the day.",
					"total_engage_duration_ms":                      "The total engage duration of the day in milliseconds.",
					"total_idle_duration_ms":                        "The total idle duration of the day in milliseconds.",
					"total_unproductive_idle_duration_ms":           "The total unproductive idle duration of the day in milliseconds.",
					"cost_currency":                                 "The currency of the cost stored.",
					"score":                                         "The efficiency score for the day.",
					"total_efficiency_l_per_100km":                  "The total efficiency in liters per 100 km for the day.",
					"engine_engage_efficiency_l_per_100km":          "The engine engage efficiency in liters per 100 km for the day.",
					"unproductive_idling_opportunity_ml":            "The amount of fuel that could have been saved by reducing unproductive idling in milliliters.",
					"hard_acceleration_opportunity_min_ml":          "The minimum amount of fuel that could have been saved by reducing hard acceleration in milliliters.",
					"hard_acceleration_opportunity_max_ml":          "The maximum amount of fuel that could have been saved by reducing hard acceleration in milliliters.",
					"speeding_opportunity_min_ml":                   "The minimum amount of fuel that could have been saved by reducing speeding in milliliters.",
					"speeding_opportunity_max_ml":                   "The maximum amount of fuel that could have been saved by reducing speeding in milliliters.",
					"cruise_control_opportunity_min_ml":             "The minimum amount of fuel that could have been saved by using cruise control in milliliters.",
					"cruise_control_opportunity_max_ml":             "The maximum amount of fuel that could have been saved by using cruise control in milliliters.",
					"hard_acceleration_opportunity_context":         "Additional context for hard acceleration opportunities.",
					"speeding_opportunity_context":                  "Additional context for speeding opportunities.",
					"cruise_control_opportunity_context":            "Additional context for cruise control opportunities.",
					"highway_percentage":                            "Percentage of highway driving in this day.",
					"uphill_downhill_percentage":                    "Percentage of uphill and downhill driving in this day.",
					"vehicle_weight_kg":                             "The weight of the vehicle in kilograms.",
					"benchmark_efficiency_l_per_100km":              "The benchmark efficiency in liters per 100 km for comparison.",
					"created_at":                                    "Timestamp of when the aggregation was created.",
					"updated_at":                                    "Timestamp of when the aggregation was updated.",
					"engage_fuel_consumed_ml":                       "The total amount of fuel consumed during engage events in milliliters.",
					"engage_fuel_cost_cents":                        "The total cost of the fuel consumed in engage events in cents (currency agnostic).",
					"engage_fuel_emissions_c_o2_grams":              "The total fuel emissions in grams of CO2 during engage events.",
					"productive_idling_fuel_consumed_ml":            "The total amount of fuel consumed during productive idling events in milliliters.",
					"productive_idling_fuel_cost_cents":             "The total cost of the fuel consumed in productive idling events in cents (currency agnostic).",
					"productive_idling_fuel_emissions_c_o2_grams":   "The total fuel emissions in grams of CO2 during productive idling events.",
					"unproductive_idling_fuel_consumed_ml":          "The total amount of fuel consumed during unproductive idling events in milliliters.",
					"unproductive_idling_fuel_cost_cents":           "The total cost of the fuel consumed in unproductive idling events in cents (currency agnostic).",
					"unproductive_idling_fuel_emissions_c_o2_grams": "The total fuel emissions in grams of CO2 during unproductive idling events.",
				},
			},
			"tmp_ml_inference_intervals": {
				Description:       "Temporary table storing ML inference intervals for EcoDriving.",
				primaryKeys:       []string{"org_id", "device_id", "interval_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("interval_start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"device_id":             metadatahelpers.DeviceIdDefaultDescription,
					"interval_uuid":         "Unique identifier of the interval.",
					"interval_start_ms":     "Interval start time in milliseconds since epoch.",
					"interval_end_ms":       "Interval end time in milliseconds since epoch.",
					"interval_ml_inference": "JSON blob containing ML inference results for the interval.",
				},
			},
		},
	},
	{
		name:       dbregistry.EldEventsDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/eldevents/eldeventsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"edit_proposal_to_edit_records": {
				Description:       "Linking table that maps edit proposals to their associated edit records, tracking the relationship between carrier-initiated edits and the resulting ELD records.",
				primaryKeys:       []string{"org_id", "edit_proposal_id", "edit_record_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":           metadatahelpers.OrgIdDefaultDescription,
					"edit_proposal_id": "UUID of the edit proposal.",
					"edit_record_id":   "UUID of the edit record associated with the proposal.",
					"relationship":     "Type of relationship between the proposal and record (e.g., created, modified).",
					"entity_type":      "Type of entity being edited (e.g., duty status, location).",
				},
			},
			"edit_proposals": {
				Description:       "Edit proposals represent carrier-initiated changes to driver ELD logs, tracking the full lifecycle from creation through driver acceptance/rejection.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"id":                 "UUID of the edit proposal.",
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"status":             "Current status of the edit proposal (e.g., pending, accepted, rejected).",
					"vehicle_ids":        "JSON array of vehicle IDs associated with this edit proposal.",
					"driver_ids":         "JSON array of driver IDs associated with this edit proposal.",
					"created_by_user_id": "User ID of the person who created the edit proposal.",
					"start_at":           "Start time of the time range affected by this edit.",
					"end_at":             "End time of the time range affected by this edit.",
					"created_at":         "Time when the edit proposal was created.",
					"updated_at":         "Time when the edit proposal was last updated.",
					"driver_acceptance":  "JSON object tracking driver acceptance/rejection status.",
					"before_statuses":    "JSON object containing duty statuses before the proposed edit.",
					"after_statuses":     "JSON object containing duty statuses after the proposed edit.",
					"remark":             "Optional remark or reason for the edit (max 60 chars).",
					"edit_types":         "JSON array of edit type codes indicating what was changed.",
					"metadata":           "Additional JSON metadata about the edit proposal.",
				},
			},
			"eld_audit_logs": {
				Description:       "Audit log entries tracking changes and actions taken on ELD records, providing a complete audit trail for compliance purposes.",
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: DateColumnFromTimestamp("action_at"),
				ColumnDescriptions: map[string]string{
					"id":                    "UUID of the audit log entry.",
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"driver_id":             metadatahelpers.DriverIdDefaultDescription,
					"audit_log_type":        "Type of audit action (e.g., status change, certification).",
					"action_at":             "Time when the audited action occurred.",
					"effective_at":          "Time when the action took effect.",
					"vehicle_id":            metadatahelpers.DeviceIdDefaultDescription,
					"created_at":            "Time when the audit log was created.",
					"created_by_actor_id":   "ID of the actor (user/driver) who performed the action.",
					"created_by_actor_role": "Role of the actor who performed the action.",
					"source_event_type":     "Type of source event that triggered this audit log.",
					"source_group_id":       "UUID grouping related audit events together.",
					"metadata":              "Additional JSON metadata about the audit event.",
					"eld_id":                "ELD device identifier that recorded the event.",
				},
			},
			"eld_border_crossing_events": {
				Description:       "Records of vehicle border crossing events between countries/states, important for determining applicable HOS regulations.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                 "UUID of the border crossing event.",
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":         metadatahelpers.DeviceIdDefaultDescription,
					"active_driver_id":   "ID of the driver who was active at the time of crossing.",
					"driver_ids":         "JSON array of all driver IDs present during the crossing.",
					"event_at":           "Time when the border crossing occurred.",
					"prev_country":       "Country code before the crossing.",
					"prev_state":         "State/province code before the crossing.",
					"prev_changed_at":    "Time when the previous location was established.",
					"next_country":       "Country code after the crossing.",
					"next_state":         "State/province code after the crossing.",
					"next_changed_at":    "Time when the new location was established.",
					"created_at":         "Time when the record was created on VG.",
					"created_op_id":      "Operation ID assigned at creation time.",
					"server_ingested_at": "Time when the record was ingested on the server.",
					"created_by_eld_id":  "ELD device identifier that created this record.",
				},
			},
			"eld_event_records": {
				Description:       "eld_event_records row represents a single ELD record generated on VG.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ExcludeColumns:    []string{"driver_ids"},
				ColumnDescriptions: map[string]string{
					"id":                          "The id of the ELD record.",
					"org_id":                      metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":                  metadatahelpers.DeviceIdDefaultDescription,
					"event_at":                    "The time the event occurred.",
					"event_type":                  "The type of the ELD event record.",
					"event_code":                  "The code of the ELD event record.",
					"record_status":               "The status of the ELD event record.",
					"record_origin":               "The origin of the ELD event record.",
					"diagnostic_malfunction_code": "The code of the diagnostic or malfunction.",
					"diagnostic_active":           "Whether the diagnostic is active at the time of recording.",
					"malfunction_active":          "Whether the malfunction is active at the time of recording.",
					"accumulated_distance_meters": "The accumulated distance in meters.",
					"elapsed_engine_seconds":      "The elapsed engine seconds.",
					"total_distance_meters":       "The total distance in meters.",
					"total_engine_seconds":        "The total engine seconds.",
					"distance_slvc_meters":        "The distance in meters from the SLVC.",
					"latitude":                    "The latitude of the event.",
					"longitude":                   "The longitude of the event.",
					"loc_validity":                "The validity of the location.",
					"loc_description":             "The description of the location.",
					"event_data_check_ca":         "The data check for the ELD event in Canada.",
					"event_data_check_usa":        "The data check for the ELD event in the USA.",
					"comment":                     "A comment about the ELD event.",
					"timezone_offset_minutes":     "The timezone offset in minutes.",
					"certified_date":              "The date the ELD event was certified.",
					"certified_day_start":         "Whether the day start is certified.",
					"created_at":                  "The time the record was created on VG. Not guaranteed to be accurate or constantly increasing due to timing malfunctions.",
					"created_op_id":               "The op id assigned to the record at the time of creation.",
					"updated_at":                  "The time the record was updated on VG. Not guaranteed to be accurate or constantly increasing due to timing malfunctions.Not guaranteed to be accurate or constantly increasing due to timing malfunctions.",
					"updated_op_id":               "The op id assigned to the record at the time of update.",
					"geolocation_usa":             "The geolocation description of the event in the USA.",
					"geolocation_ca":              "The geolocation description of the event in Canada.",
					"server_ingested_at":          "The time the record was ingested on our server, which is used for partitioning. We cannot trust timestamps from VG because of timing malfunctions.",
				},
				versionInfo:       SingleVersion(1),
				DisableServerless: true, // The spark config override is incompatible with serverless
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							CustomSparkConfigurations: []map[string]string{
								// adding to overcome UNSIGN_INT overflow error
								// this config will be incompatible with serverless as only limited
								// spark configs are supported https://docs.databricks.com/aws/en/spark/conf#serverless
								{"spark.sql.storeAssignmentPolicy": "LEGACY"},
							},
						},
						infraconsts.SamsaraAWSEURegion: {
							CustomSparkConfigurations: []map[string]string{
								{"spark.sql.storeAssignmentPolicy": "LEGACY"},
							},
						},
					},
				},
			},
			"eld_exemption_declarations": {
				Description:       "Records of driver exemption declarations (e.g., short-haul, agricultural exemptions) that affect HOS compliance calculations.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                      "UUID of the exemption declaration.",
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":              metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":               metadatahelpers.DriverIdDefaultDescription,
					"event_at":                "Time when the exemption declaration was made.",
					"claim_id":                "UUID of the exemption claim.",
					"declaration_type":        "Type of declaration (e.g., start, end).",
					"exemption_type":          "Type of exemption being declared (e.g., short-haul, agricultural).",
					"remark":                  "Optional remark explaining the exemption (max 60 chars).",
					"created_in_jurisdiction": "Jurisdiction code where the declaration was created.",
					"created_at":              "Time when the record was created on VG.",
					"created_op_id":           "Operation ID assigned at creation time.",
					"server_ingested_at":      "Time when the record was ingested on the server.",
					"created_by_eld_id":       "ELD device identifier that created this record.",
				},
			},
			"eld_shipping_document_association_declarations": {
				Description:       "Records of shipping document associations declared by drivers, linking shipping documents (bills of lading) to ELD duty status records.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                       "UUID of the shipping document declaration.",
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":               metadatahelpers.DeviceIdDefaultDescription,
					"event_at":                 "Time when the declaration was made.",
					"driver_id":                metadatahelpers.DriverIdDefaultDescription,
					"shipping_documents":       "JSON array of shipping document identifiers.",
					"remark":                   "Optional remark about the shipping documents (max 60 chars).",
					"status":                   "Status of the declaration (e.g., active, inactive).",
					"source":                   "Source of the declaration (e.g., driver, carrier).",
					"recorded_in_jurisdiction": "Jurisdiction code where the declaration was recorded.",
					"created_at":               "Time when the record was created on VG.",
					"created_op_id":            "Operation ID assigned at creation time.",
					"created_by_eld_id":        "ELD device identifier that created this record.",
					"created_by":               "User/driver ID who created this record.",
					"updated_at":               "Time when the record was last updated.",
					"updated_op_id":            "Operation ID assigned at last update.",
					"updated_by_eld_id":        "ELD device identifier that last updated this record.",
					"server_ingested_at":       "Time when the record was ingested on the server.",
				},
			},
			"eld_team_driving_configuration_changes": {
				Description:       "Records of team driving configuration changes, tracking when drivers are added/removed from team driving sessions.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"id":                        "UUID of the configuration change event.",
					"org_id":                    metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":                metadatahelpers.DeviceIdDefaultDescription,
					"event_at":                  "Time when the configuration change occurred.",
					"change_trigger":            "What triggered the configuration change.",
					"team_driving_session_id":   "UUID of the team driving session.",
					"driver_ids":                "JSON array of all driver IDs in the session after the change.",
					"active_driver_id":          "ID of the currently active driver.",
					"co_driver_ids":             "JSON array of co-driver IDs.",
					"added_driver_ids":          "JSON array of driver IDs added in this change.",
					"removed_driver_ids":        "JSON array of driver IDs removed in this change.",
					"previous_active_driver_id": "ID of the previously active driver.",
					"created_in_jurisdiction":   "Jurisdiction code where the change was recorded.",
					"created_at":                "Time when the record was created on VG.",
					"created_op_id":             "Operation ID assigned at creation time.",
					"updated_at":                "Time when the record was last updated.",
					"updated_op_id":             "Operation ID assigned at last update.",
					"created_by_eld_id":         "ELD device identifier that created this record.",
				},
			},
			"eld_trailer_association_declarations": {
				Description:       "Records of trailer associations declared by drivers, tracking which trailers are attached to vehicles during ELD duty periods.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                       "UUID of the trailer association declaration.",
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":               metadatahelpers.DeviceIdDefaultDescription,
					"event_at":                 "Time when the declaration was made.",
					"driver_id":                metadatahelpers.DriverIdDefaultDescription,
					"trailers":                 "JSON array of trailer identifiers.",
					"remark":                   "Optional remark about the trailer association (max 60 chars).",
					"status":                   "Status of the declaration (e.g., active, inactive).",
					"source":                   "Source of the declaration (e.g., driver, carrier).",
					"recorded_in_jurisdiction": "Jurisdiction code where the declaration was recorded.",
					"created_at":               "Time when the record was created on VG.",
					"created_op_id":            "Operation ID assigned at creation time.",
					"created_by_eld_id":        "ELD device identifier that created this record.",
					"created_by":               "User/driver ID who created this record.",
					"updated_at":               "Time when the record was last updated.",
					"updated_op_id":            "Operation ID assigned at last update.",
					"updated_by_eld_id":        "ELD device identifier that last updated this record.",
					"server_ingested_at":       "Time when the record was ingested on the server.",
				},
			},
			"eld_unassigned_segment_attribution_declarations": {
				Description:       "Records of unassigned driving segment attributions, where drivers claim responsibility for driving time that was not automatically assigned.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                       "UUID of the attribution declaration.",
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":               metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":                metadatahelpers.DriverIdDefaultDescription,
					"start_at":                 "Start time of the unassigned segment being attributed.",
					"end_at":                   "End time of the unassigned segment being attributed.",
					"status":                   "Status of the attribution (e.g., pending, accepted, rejected).",
					"recorded_in_jurisdiction": "Jurisdiction code where the declaration was recorded.",
					"created_at":               "Time when the record was created on VG.",
					"created_op_id":            "Operation ID assigned at creation time.",
					"created_by_eld_id":        "ELD device identifier that created this record.",
					"updated_at":               "Time when the record was last updated.",
					"updated_op_id":            "Operation ID assigned at last update.",
					"updated_by_eld_id":        "ELD device identifier that last updated this record.",
					"server_ingested_at":       "Time when the record was ingested on the server.",
				},
			},
			"eld_user_prompt_lifecycle_changes": {
				Description:       "Records of user prompt lifecycle events, tracking when drivers are prompted for actions and their responses.",
				primaryKeys:       []string{"id", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("server_ingested_at"),
				ColumnDescriptions: map[string]string{
					"id":                      "UUID of the prompt lifecycle event.",
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":              metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":               metadatahelpers.DriverIdDefaultDescription,
					"event_at":                "Time when the prompt event occurred.",
					"prompt_id":               "UUID of the prompt.",
					"prompt_type":             "Type of prompt shown to the user.",
					"prompt_lifecycle_stage":  "Stage of the prompt lifecycle (e.g., shown, acknowledged, dismissed).",
					"created_at":              "Time when the record was created on VG.",
					"created_op_id":           "Operation ID assigned at creation time.",
					"created_in_jurisdiction": "Jurisdiction code where the prompt was created.",
					"server_ingested_at":      "Time when the record was ingested on the server.",
					"created_by_eld_id":       "ELD device identifier that created this record.",
				},
			},
			"failed_eld_events": {
				Description:       "ELD Event object stats that failed to be ingested into the backend database table",
				primaryKeys:       []string{"id", "org_id", "created_at"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{
					"event_proto": {
						Message:   &eldproto.EldEvent{},
						ProtoPath: "go/src/samsaradev.io/hubproto/eldproto/eld.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"id":            "A unique identifier representing the attempt to ingest the ELD event",
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"event_proto":   "The protobuf serialized contents of the ELD event which failed to be ingested",
					"event_ids":     "A JSON array of the IDs for records & events contained within the ELD event.",
					"error_type":    "The type of error that occurred",
					"error_message": "The reason the ELD event was failed to be ingested",
					"created_at":    "The time the failed ELD event was created on our server.",
				},
			},
		},
	},
	{
		name:       dbregistry.EldHosDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/eldhos/eldhosmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_hos_ignored_violations": {
				Description:       "Violations generated by passing into the rules engine the HOS logs WITH ignored drive segments (ELD Driving overridden with new duty status).",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
			"driver_hos_shifts": {
				Description:       "Shifts used to determine computation ranges for invalidating violations.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
			"driver_hos_violations": {
				Description:       "Violations generated by passing into the rules engine the HOS logs WITHOUT ignored drive segments (ELD Driving overridden with new duty status).",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
			"us_short_haul_shift_metadata": {
				Description:       "Metadata used for rules engine input involving US Short Haul Shifts.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.EmissionsReportingDB,
		schemaPath: "go/src/samsaradev.io/fleet/emissionsreporting/emissionsreportingmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"carb_device_enrollments": {
				primaryKeys:       []string{"org_id", "enrollment_uuid"},
				PartitionStrategy: DateColumnFromTimestamp("enrolled_at"),
				Description:       "Table to store the device enrollments in the CARB testing program, including user-provided VIN and license plate details.",
				ColumnDescriptions: map[string]string{
					"org_id":               "Global identifier for the organization.",
					"enrollment_uuid":      "Unique identifier for the enrollment.",
					"device_id":            "Global identifier for the device.",
					"user_provided_vin":    "VIN provided by the user during enrollment.",
					"license_plate":        "License plate provided by the user during enrollment.",
					"state_registered":     "State where the device is registered (2-character code).",
					"month_registered":     "Month in which the device was registered, only applicable for California.",
					"enrolled_at":          "Timestamp when the device was enrolled.",
					"unenrolled_at":        "Timestamp when the device was unenrolled.",
					"unenrolled_at_unique": "Special column to allow a constraint for only a single enrolled device.",
				},
			},
			"carb_enrollment_collection_sessions": {
				primaryKeys:       []string{"org_id", "enrollment_uuid", "carb_session_message_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Records individual collection sessions, including report status, test performance, and other session metadata.",
				ColumnDescriptions: map[string]string{
					"org_id":                  "Global identifier for the organization.",
					"enrollment_uuid":         "Unique identifier for the enrollment session.",
					"carb_session_message_id": "ID of the session message sent to the VG for this collection.",
					"report_status":           "Status of the report generation for this session.",
					"report_status_details":   "Details regarding the report status, stored in JSON format.",
					"created_at":              "Timestamp for when the session was created.",
					"updated_at":              "Timestamp for when the session was last updated.",
					"software_version":        "Software version used during the session.",
					"filesize":                "Size of the final gpg report file generated during SFTP upload.",
					"test_performed_at":       "Timestamp for when the test was performed.",
					"test_received_at":        "Timestamp for when the test data was received.",
					"report_uploaded_at":      "Timestamp for when the report was uploaded.",
				},
				versionInfo: SingleVersion(1),
			},
			"carb_enrollment_collections": {
				primaryKeys:       []string{"org_id", "enrollment_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores information about the collection events tied to the enrollments (1:1), including status and scheduling.",
				ColumnDescriptions: map[string]string{
					"org_id":                  "Global identifier for the organization.",
					"enrollment_uuid":         "Unique identifier for the enrollment.",
					"previous_collection":     "Timestamp for the previous collection event.",
					"next_collection":         "Timestamp for the next collection event.",
					"carb_session_message_id": "ID of the most recent session message sent to the VG for this enrollment.",
					"status":                  "Current status of the collection.",
					"status_details":          "Details regarding the collection status, stored in JSON format.",
					"updated_at":              "Timestamp when the collection information was last updated.",
				},
				versionInfo: SingleVersion(1),
			},
			"carb_organizations": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Table to store information about organizations involved in CARB testing and compliance.",
				ColumnDescriptions: map[string]string{
					"org_id":     "Global identifier for the organization.",
					"created_at": "Timestamp when the organization was enrolled.",
				},
			},
			"carb_scheduling_settings_log": {
				primaryKeys:       []string{"org_id", "created_at"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Log of scheduling settings changes for CARB organizations, tracking automated scheduling configuration and retry policies over time.",
				ColumnDescriptions: map[string]string{
					"org_id":                   "Global identifier for the organization.",
					"use_automated_scheduling": "Flag indicating whether automated scheduling is enabled for this organization.",
					"retry_configurations":     "JSON configuration object defining retry policies for failed collection attempts.",
					"created_by":               "User ID of the person who created or updated this settings entry.",
					"created_at":               "Timestamp when this settings configuration was created.",
				},
				versionInfo: SingleVersion(1),
			},
		},
	},
	{
		name:                dbregistry.EncryptionKeyDB,
		schemaPath:          "go/src/samsaradev.io/infra/security/encrypt/encryptionkeyservice/encryptionkeymodels/db_schema.sql",
		passwordKeyOverride: "encryptionkey_database_password",
		Sharded:             false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"encryption_keys": {
				Description:                       "encryption_keys contains data keys used by encryptionkeyservice to encrypt/decrypt data with KMS",
				primaryKeys:                       []string{"uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.EngineActivityDB,
		schemaPath: "go/src/samsaradev.io/fleet/engineactivity/engineactivitymodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"engine_engage_event_efficiency": {
				Description:       "Stores data for enriching engine engage events with engine efficiency.",
				primaryKeys:       []string{"event_uuid", "org_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"event_uuid":      "Unique identifier for the configuration entry.",
					"org_id":          "The organization associated with the configuration.",
					"distance_meters": "The distance traveled during the engine engage event.",
					"plugin_hybrid_electric_only_distance_meters": "For plug-in hybrids, the distance traveled using only electric power.",
					"pending_distance":                            "The distance traveled during the engine engage event that is pending more data.",
					"created_at":                                  "Timestamp for when the row was created.",
					"updated_at":                                  "Timestamp for when the row was last updated.",
					"deleted_at":                                  "Timestamp for when the row was soft deleted.",
				},
			},
			"engine_engage_events": {
				Description:       "Stores the enriched engine engage events.",
				primaryKeys:       []string{"uuid", "org_id"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization associated with the engine engage event.",
					"uuid":                              "Unique identifier for the engine engage event.",
					"device_id":                         "The id of the device associated with the engine engage event.",
					"operator_id":                       "The id of the operator associated with the engine engage event.",
					"start_ms":                          "The start time of the engine engage event in milliseconds.",
					"duration_ms":                       "The duration of the engine engage event in milliseconds.",
					"cost_currency":                     "The currency of the cost stored.",
					"fuel_type":                         "The fuel type configured for the device.",
					"fuel_consumed_ml":                  "The amount of fuel consumed in milliliters during the engine engage event.",
					"fuel_cost_cents":                   "The cost of fuel consumed in cents (currency agnostic).",
					"fuel_emissions_c_o2_grams":         "The fuel emissions in grams of CO2 during the engine engage event.",
					"gaseous_fuel_consumed_grams":       "The amount of gaseous fuel consumed in grams during the engine engage event.",
					"gaseous_fuel_cost_cents":           "The cost of gaseous fuel consumed in cents (currency agnostic).",
					"gaseous_fuel_emissions_c_o2_grams": "The gaseous fuel emissions in grams of CO2 during the engine engage event.",
					"energy_consumed_kwh":               "The energy consumed in kilowatt-hours during the engine engage event.",
					"energy_cost_cents":                 "The cost of energy consumed in cents (currency agnostic).",
					"energy_emissions_c_o2_grams":       "The energy emissions in grams of CO2 during the engine engage event.",
					"pending_fuel":                      "The fuel consumed during the engine engage event that is pending more data.",
					"created_at":                        "Timestamp for when the event was created.",
					"updated_at":                        "Timestamp for when the event was last updated.",
					"deleted_at":                        "Timestamp for when the event was soft deleted.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					CronHourSchedule: "*/1",
				},
			},
			"engine_idle_events": {
				Description:       "Stores the enriched engine idle events.",
				primaryKeys:       []string{"uuid", "org_id"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization associated with the engine idle event.",
					"uuid":                              "Unique identifier for the engine idle event.",
					"device_id":                         "The id of the device associated with the engine idle event.",
					"operator_id":                       "The id of the operator associated with the engine idle event.",
					"start_ms":                          "The start time of the engine idle event in milliseconds.",
					"duration_ms":                       "The duration of the engine idle event in milliseconds.",
					"pto_state":                         "Whether power takeoff was active or not (0=unknown, 1=active, 2=inactive).",
					"pto_source":                        "If PTO active, the source of the PTO (0=invalid, 1=digio, 2=ecu).",
					"pto_digio_input_type":              "If PTO active, the digio type of the PTO if its source is digio.",
					"pto_input_count":                   "The number of PTO inputs.",
					"cost_currency":                     "The currency of the cost stored.",
					"fuel_type":                         "The fuel type configured for the device.",
					"fuel_consumed_ml":                  "The amount of fuel consumed in milliliters during the engine idle event.",
					"fuel_cost_cents":                   "The cost of fuel consumed in cents (currency agnostic).",
					"fuel_emissions_c_o2_grams":         "The fuel emissions in grams of CO2 during the engine idle event.",
					"gaseous_fuel_consumed_grams":       "The amount of gaseous fuel consumed in grams during the engine idle event.",
					"gaseous_fuel_cost_cents":           "The cost of gaseous fuel consumed in cents (currency agnostic).",
					"gaseous_fuel_emissions_c_o2_grams": "The gaseous fuel emissions in grams of CO2 during the engine idle event.",
					"ambient_temperature_milli_celsius": "The ambient temperature during the engine idle event in millicelsius.",
					"geofence_id":                       "The geofence associated with the engine idle event.",
					"geofence_type":                     "The type of geofence.",
					"latitude":                          "The location's latitude of the engine idle event.",
					"longitude":                         "The location's longitude of the engine idle event.",
					"location_house_number":             "The location's house number of the engine idle event.",
					"location_street":                   "The location's street of the engine idle event.",
					"location_city":                     "The location's city of the engine idle event.",
					"location_neighborhood":             "The location's neighborhood of the engine idle event.",
					"location_state":                    "The location's state of the engine idle event.",
					"location_country":                  "The location's country of the engine idle event.",
					"location_post_code":                "The location's post code of the engine idle event.",
					"pending_fuel":                      "The fuel consumed during the engine idle event that is pending more data.",
					"created_at":                        "Timestamp for when the event was created.",
					"updated_at":                        "Timestamp for when the event was last updated.",
					"deleted_at":                        "Timestamp for when the event was soft deleted.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					CronHourSchedule: "*/1",
				},
			},
			"unproductive_idling_config": {
				Description:       "Stores configuration for unproductive idling based on air temperature, PTO states, and duration.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"uuid":                            "Unique identifier for the configuration entry.",
					"org_id":                          "The organization associated with the configuration.",
					"minimum_duration_ms":             "The minimum duration (in milliseconds) for idling to be considered productive.",
					"air_temperature_min_milli_c":     "Minimum air temperature (in milli-Celsius) for productive idling.",
					"air_temperature_max_milli_c":     "Maximum air temperature (in milli-Celsius) for productive idling.",
					"include_unknown_air_temperature": "Include idling events with unknown air temperature.",
					"include_pto_on":                  "Include idling events with PTO on.",
					"include_pto_off":                 "Include idling events with PTO off.",
					"created_at":                      "The timestamp when the record was created.",
					"updated_at":                      "The timestamp when the record was last updated.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.EuComplianceDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/eu/eucomplianceserver/eucompliancemodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_infringements": {
				Description: "Driver infringements tracked for EU compliance with detailed violation information, including " +
					"severity levels, fines, and legislative references. Different rows with the same UUID represent different " +
					"versions of the same infringement",
				versionInfo: SingleVersion(1),
				primaryKeys: []string{"org_id", "uuid", "effective_from"},
				ColumnDescriptions: map[string]string{
					"uuid":                        "Unique identifier for the infringement record",
					"org_id":                      metadatahelpers.OrgIdDefaultDescription,
					"driver_id":                   metadatahelpers.DriverIdDefaultDescription,
					"infringement_logged_at":      "Timestamp when the infringement was logged in the system",
					"code":                        "Infringement code identifying the specific violation type",
					"activity_period_start":       "Start of the activity period related to the infringement",
					"activity_period_end":         "End of the activity period related to the infringement",
					"causative_activity_period":   "The causative activity period for the infringement",
					"auxiliary_cumulative_period": "Auxiliary cumulative period associated with the infringement",
					"severity": metadatahelpers.EnumDescription("Severity level of the infringement", map[int32]string{
						int32(0): "UNSPECIFIED",
						int32(1): "MINOR",
						int32(2): "SERIOUS",
						int32(3): "VERY_SERIOUS",
						int32(4): "MOST_SERIOUS",
					}),
					"fine_currency":               "ISO 4217 currency code for fines (e.g., EUR, GBP)",
					"fine_driver_min_minor_units": "Minimum fine amount for the driver in minor currency units",
					"fine_driver_max_minor_units": "Maximum fine amount for the driver in minor currency units",
					"fine_fleet_min_minor_units":  "Minimum fine amount for the fleet operator in minor currency units",
					"fine_fleet_max_minor_units":  "Maximum fine amount for the fleet operator in minor currency units",
					"primary_legislation":         "Primary legislation reference that defines this infringement",
					"secondary_legislation":       "Secondary legislation or regulatory reference for this infringement",
					"created_at":                  "Timestamp when the record was created in the database",
					"updated_at":                  "Timestamp when the record was last updated in the database",
					"short_description":           "A brief explanation of what this infringement is about",
					"long_description":            "A more thorough description, explaining the rules behind this infringement",
					"effective_from":              "The time from which this version of the infringement is valid",
					"effective_to":                "The time to which this version of the infringement is valid",
					"is_deleted":                  "Whether this row represents an infringement which has been deleted after having been created",
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
			"infringements": {
				Description: "Infringements of driving, resting, and working rules in European commercial fleets",
				primaryKeys: []string{"org_id", "driver_id", "timestamp_ms", "violation_id"},
				ProtoSchema: ProtobufSchema{
					"metadata": {
						Message:   &eucomplianceproto.EuComplianceInfringementMetadata{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/eu/eucomplianceproto/eucompliance.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
		},
	},
	{
		name:       dbregistry.EurofleetDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/eu/eurofleetmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
			EnableKinesisStreamDestination: true,
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_activities": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Description:       "Rows in the driver_activities table represent a period of activity for a driver. This allows orgs to have a more detailed view of what exactly a driver is doing (compared to the limited set of tachograph statuses)",
				ColumnDescriptions: map[string]string{
					"id":                     "An id identifying a driver activity",
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"driver_id":              metadatahelpers.DriverIdDefaultDescription,
					"start_time":             "The start time of the activity",
					"end_time":               "The end time of the activity",
					"activity_uuid":          "The UUID of the activity type selected",
					"activity_name_when_set": "The name of the activity when it was set (note the name can change)",
					"created_at":             "The time the activity was created by the backend",
					"updated_at":             "The time the activity was updated by the backend",
					"deleted_at":             "The time the activity was soft-deleted by the backend",
					"user_id":                "The user ID that created the row (may eg be an org admin and not a driver)",
					"entry_type":             "An integer representing the type of entry (0: created real-time by driver, 1: admin edit, 2: driver edit)",
				},
			},
			"driver_expense_config": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "driver_expense_config row represents a configuration for driver expense such as allowance amount, conditions, and other things",
				ColumnDescriptions: map[string]string{
					"uuid":                          "An id identifying an expense config",
					"org_id":                        metadatahelpers.OrgIdDefaultDescription,
					"effective_from":                "The time the expense config is effective from",
					"created_at":                    "The time the expense config was created",
					"updated_at":                    "The time the expense config was updated. This should always match created_at given we never update an existing config",
					"expense_config_uuid":           "The uuid that will link same (updated) expense configs together. This will be the same as the uuid of the original config",
					"category_uuid":                 "The uuid of the category of this expense config",
					"name":                          "The name of the expense config",
					"description":                   "The description of the expense config",
					"allowance_in_cents":            "The allowance of the expense config, currency-agnostic",
					"conditions":                    "The conditions for triggering this expense",
					"international_premium_percent": "Percentage premium added to expenses when expense is triggered in a country which is different than driver's origin country. Value 0 implies intl premium is disabled.",
					"deleted_at":                    "Deletion time of this config. Null if not deleted",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_status": {
				primaryKeys:       []string{"org_id", "driver_id", "set_at_ms", "source"},
				PartitionStrategy: DateColumnFromMilliseconds("set_at_ms"),
				Description: "driver_status stores driver activity statuses with some context for a variety of sources.\n" +
					"These statuses describe the a change in a driver's status for compliance products and are generated from " +
					"source ingestion such as tachograph live stats.",
				ColumnDescriptions: map[string]string{
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"driver_id":              "Id of the driver that set this status.",
					"status":                 metadatahelpers.EnumDescription("Status that the driver set.", euhosproto.StatusType_name),
					"set_at_ms":              "Time at which status was set from the driver's time.",
					"created_at":             "Datetime that status was set in the table.",
					"device_id":              "Id of the device that is the source of this status.",
					"location_lat":           "Latitude of where the device was when the status was created.",
					"location_long":          "Longitude of where the device was when the status was created.",
					"location_address":       "Address of where the device was when the status was created.",
					"odometer_meters":        "Odometer reading from the vehicle the driver was using when the status was created.",
					"gps_distance_meters":    "GPS Distance travelled by the VG since activation at the time the status was created.",
					"needs_fixup":            "A flag indicating whether the status needs to be updated with location, odometer and address information.",
					"source":                 metadatahelpers.EnumDescription("Source from which the status originates from.", euhosproto.Source_name),
					"deleted_at":             "Datetime that status was deleted at.",
					"tachograph_card_number": "Tachograph card number associated with this driver status. This is only relevant for statuses created from driver files or live tachogaph stats.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSEURegion: {
							MaxWorkers: pointer.IntPtr(4),
						},
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_status_edits": {
				primaryKeys:       []string{"org_id", "driver_id", "start_ms", "end_ms", "created_at"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				Description:       "driver_status_edits stores manual edits made to driver status periods by admins or drivers. Each row represents an override or correction to the automatically recorded driver statuses.",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"driver_id":  "Id of the driver whose status is being edited.",
					"status":     metadatahelpers.EnumDescription("The status type that the edit applies.", euhosproto.StatusType_name),
					"remark":     "A text comment providing context for why this edit was created. Required for all edits, max 64 characters.",
					"start_ms":   "The start time (in milliseconds) of the status period being edited.",
					"end_ms":     "The end time (in milliseconds) of the status period being edited.",
					"created_at": "Datetime when this edit was created in the system.",
					"created_by": "User ID of the person who created this edit (admin or driver).",
					"device_id":  "Id of the device associated with this status edit, defaults to 0 if not applicable.",
				},
				// Partition by org_id to ensure all status edits for the same org go to
				// the same Kinesis shard for ordered processing.
				PartitionKeyAttribute: "org_id",
			},
			"overtime_profile": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "overtime_profile row represents an overtime profile containing multiple pay rates based on the amount of working time",
				ColumnDescriptions: map[string]string{
					"uuid":       "An id identifying the profile",
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"period":     "The period that this profile applies over",
					"created_at": "The time that this profile was created",
					"updated_at": "The time that this profile was updated",
					"pay_rates":  "The pay rates based on amount of working time",
					"name":       "The name of the profile",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"time_off_policies": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "time_off_policies row represent a time off policy containing multiple time off types that drivers can take according to that specific policy",
				ColumnDescriptions: map[string]string{
					"uuid":          "An id identifying the policy",
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"hours_per_day": "The amount of paid or unpaid hours added to the timesheet per day of time off",
					"created_at":    "The time that this policy was created",
					"updated_at":    "The time that this policy was updated",
					"deleted_at":    "The time that this policy was deleted. If a policy was never deleted, the value of this is 0 (also represented as '0000-00-00' in the DB). To check for that, the built-in time.Time type has a `.IsZero()` method",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"time_off_policy_types": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "time_off_policy_types row represents a type of time off that is assigned to a policy e.g. Holiday, Unpaid Leave, Public Holiday. Each policy can have multiple types",
				ProtoSchema: ProtobufSchema{
					"config": {
						Message:   &timeoffproto.TimeOffPolicyTypeSetting{},
						ProtoPath: "go/src/samsaradev.io/fleet/compliance/eu/timeoffproto/timeoff.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"uuid":           "The organization of this policy",
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"policy_uuid":    "An id referencing the time off policy the type belongs to",
					"type":           metadatahelpers.EnumDescription("Type of a time off period", timeoffproto.TimeOffType_name),
					"effective_from": "The time that this time off type started being effective",
					"created_at":     "The time that this time off type was created",
					"updated_at":     "The time that this time off type was updated",
					"deleted_at":     "The time that this time off type was deleted. If this is null, then the time off type is not deleted",
					"config":         "A blob that stores extra config data that can be required by certain time off types",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.FinopsDB,
		schemaPath: "go/src/samsaradev.io/platform/finops/finopsmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"contracts": {
				Description:       "The Contracts table contains information about the contracts from sfdc",
				primaryKeys:       []string{"sam_number", "contract_number"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"sam_number":                  metadatahelpers.SamNumberDefaultDescription,
					"contract_number":             "The unique identifier of a contract",
					"auto_renewal":                "Whether this contract is auto renewed when it expires",
					"start_date":                  "The start date of this contract",
					"end_date":                    "The end date of this contract",
					"term_length_months":          "The length of this contract",
					"sfdc_modified_at":            "The last time sfdc modifies the data",
					"updated_at":                  "The time at which this contract row was last updated",
					"created_at":                  "The time at which this contract row was created",
					"opportunity_sfdc_id":         "Foreign key point to opportunity",
					"renewal_opportunity_sfdc_id": "Foreign key point to opportunity when a contract is renewed",
					"sfdc_id":                     "Salesforce internal ID used by other SFDC objects (like opportunity)",
					"is_active":                   "If the contract is still active and not cancelled (despite having valid dates)",
					"record_type_name":            "Determines the type of the contract",
					"compliance_enforcement_date": "Determines the compliance enforcement end date",
					"grace_period_end_date":       "Determines the grace period end date",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"customer_info": {
				primaryKeys:       []string{"sam_number"},
				PartitionStrategy: SinglePartition,
				Description: "customer_info stores customer data pulled from SalesForce. " +
					"We run a nightly job that will check for updates to a customer's account, and write that data to this table.",
				ColumnDescriptions: map[string]string{
					"hide_license_dashboard_expiration":   "This will prevent the customer from being shown a license expiration banner.",
					"segment":                             "Customers segment from SFDC with values like: (ex: Mid Market, Enterprise, etc.)",
					"current_owner_email":                 "Email of the owner of the customer's company. This field is not surfaced to the customer any longer.",
					"device_suspension_grace_period_days": "Additional grace period to prevent device suspension when a customer has unpaid invoices.",
					"renewal_ae_email":                    "Renewal AE for this account.",
					"sam_number":                          metadatahelpers.SamNumberDefaultDescription,
					"csm_tier":                            "Customer support tier.",
					"churn_payment_status":                "Tracks churn status for customer.",
					"ae_manager_email":                    "Renewal AE's manager for this account.",
					"livestream_license_count":            "This documents how many livestream licenses this customer has.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"exchanges": {
				primaryKeys:       []string{"sam_number", "exchange_number"},
				Description:       "The Exchanges table contains all exchange related returns customers sends us (RMAs), but may not include all returns. For data around individual orders, please use related tables (finopsdb.shipping_groups, finopsdb.product_details, finopsdb.product_serial_details)",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":                    metadatahelpers.SamNumberDefaultDescription,
					"exchange_number":               "ID of the exchange",
					"requested_by_email":            "The email that requested the exchange",
					"bill_contact_name":             "The name associated with the billing contact",
					"bill_contact_email":            "The email address associated with the billing contact",
					"bill_method":                   "The method for the bill",
					"bill_address_uuid":             "The UUID of the billing address",
					"created_at":                    "The time at which this exchange was created",
					"updated_at":                    "The time at which this exchange was last updated",
					"netsuite_transaction_id":       "The unique ID for netsuite's transaction table",
					"order_netsuite_transaction_id": "The key for the order related to this exchange. Join with finopsdb.order_details.netsuite_transaction_id = exchanges.order_netsuite_transaction_id",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"license_types_products": {
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description: "The License Types Products contains a mapping from license type to product type to show valid license/product pairs." +
					"This table makes the most sense after running a query like this \n \n ```SELECT hw.license_type, lic.license_type \n \n FROM license_types_products as hw \n \n " +
					"JOIN license_types_products as lic on hw.product_id = lic.product_id AND lic.license_type LIKE 'LIC-%' \n \n" +
					"WHERE hw.license_type LIKE 'HW-%'``` \n \n so you can see the valid pairs. This table is a static pull of these mappings from NetSuite.",
				ColumnDescriptions: map[string]string{
					"uuid":         "Unique identifier for the row.",
					"license_type": "Misleading name! This is the license or hardware sku for the associated product.",
					"product_id":   "Product id for the hardware itself or the product id the license applies to.",
					"created_at":   "When this row was added.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"licenses": {
				primaryKeys:       []string{"sam_number", "netsuite_transaction_id", "netsuite_transaction_line_id"},
				PartitionStrategy: SinglePartition,
				Description:       "The licenses table contains information about licenses from netsuite",
				ColumnDescriptions: map[string]string{
					"sam_number":                           metadatahelpers.SamNumberDefaultDescription,
					"netsuite_transaction_id":              "The transaction id of this license",
					"netsuite_transaction_line_id":         "Each transaction might contain multiple lines. This is the line number of a transaction.",
					"netsuite_transaction_type":            `Either "Sales Order" or "Return Authorization"`,
					"netsuite_transaction_status":          "The status of the transaction for this license",
					"order_number":                         "The order number of the transaction for this license",
					"return_number":                        `The return number of the transaction for this license. This column is only valid if the type is "Return Authorization"`,
					"created_from_netsuite_transaction_id": `The corresponding transaction_id of the original sales order for a return. This column is only valid if the type is "Return Authorization"`,
					"sku":                                  "The sku of this license",
					"quantity":                             "The quantity of this license",
					"created_at":                           "The time at which this license row was created",
					"updated_at":                           "The time at which this license row was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"netsuite_consolidated_invoices": {
				primaryKeys:       []string{"sam_number", "netsuite_internal_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table stores metadata around consolidated invoices we ingest from netsuite (No actual invoice data). We use these records to check if we have a PDF for a specified invoice.",
				ColumnDescriptions: map[string]string{
					"sam_number":           metadatahelpers.SamNumberDefaultDescription,
					"netsuite_internal_id": "The internal key for the invoice record in Netsuite.",
					"pdf_created_at":       "When we first pulled the PDF for this record.",
					"pdf_updated_at":       "When we updated the PDF for this record.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"netsuite_invoices_v2": {
				primaryKeys:       []string{"sam_number", "netsuite_internal_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table stores metadata around invoices we ingest from netsuite (No actual invoice data). We use these records to check if we have a PDF for a specified invoice.",
				ColumnDescriptions: map[string]string{
					"sam_number":           metadatahelpers.SamNumberDefaultDescription,
					"netsuite_internal_id": "The internal key for the invoice record in Netsuite.",
					"pdf_created_at":       "When we first pulled the PDF for this record.",
					"pdf_updated_at":       "When we updated the PDF for this record.",
					"created_at":           "When we created this row in the DB.",
					"updated_at":           "When we updated this row in the DB.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"opportunities": {
				Description:       "The Opportunities table contains information about the opportunities from sfdc",
				primaryKeys:       []string{"sam_number", "sfdc_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"sam_number":                       metadatahelpers.SamNumberDefaultDescription,
					"sfdc_id":                          "The unique identifier of an opportunity created from sfdc",
					"stage_name":                       "The stage of this opportunity. Helpful for determining if a contract is renewed or not",
					"netsuite_id":                      "The corresponding netsuite id to an opportunity which is related to an order",
					"agreement_link":                   "The link for customers to do the renewal themselves",
					"sub_type":                         "Useful for customer self-checkout",
					"sfdc_modified_at":                 "The last time sfdc modifies the data",
					"updated_at":                       "The time at which this opportunity row was last updated",
					"created_at":                       "The time at which this opportunity row was created",
					"amended_contract_sfdc_id":         "Contract's SFDC ID that this opportunity amends",
					"consolidated_renewal_opp_sfdc_id": "Opportunity SFDC ID that this opportunity is consolidated into",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"order_addresses_v2": {
				primaryKeys:       []string{"sam_number", "uuid"},
				Description:       "The Order Addresses table contains a list of shipping addresses from orders and exchanges",
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"sam_number":     metadatahelpers.SamNumberDefaultDescription,
					"uuid":           "UUID of the address",
					"address_line_1": "The first line of the address",
					"address_line_2": "The first line of the address",
					"locality":       "The locality of the address (City in the US)",
					"region":         "The region of the address (State in the US)",
					"country":        "The country of the address",
					"zip_code":       "The zipcode of the address",
					"created_at":     "The time at which this exchange was created",
					"updated_at":     "The time at which this exchange was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"order_details": {
				primaryKeys:       []string{"sam_number", "order_number"},
				Description:       "The Order Details table contains details about each order, including billing information. For data around individual orders, please use related tables (finopsdb.shipping_groups, finopsdb.product_details, finopsdb.product_serial_details)",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":              metadatahelpers.SamNumberDefaultDescription,
					"order_number":            "ID of the order",
					"sfdc_contract_id":        "The ID of the contract in SFDC",
					"requested_by_email":      "The email that requested the order",
					"bill_contact_name":       "The name associated with the billing contact",
					"bill_contact_email":      "The email address associated with the billing contact",
					"bill_method":             "The method for the bill",
					"bill_address_uuid":       "The UUID of the billing address",
					"created_at":              "The time at which this order detail row was created",
					"updated_at":              "The time at which this order was last updated",
					"netsuite_transaction_id": "The unique ID for netsuite's transaction table",
					"order_created_at":        "The time at which this order was created",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"order_to_exchange": {
				primaryKeys:       []string{"sam_number", "order_number", "exchange_number"},
				Description:       "This table is no longer used. Please use finopsdb.exchanges.order_netsuite_transaction_id to relate an exchange to a transaction",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":      metadatahelpers.SamNumberDefaultDescription,
					"order_number":    "ID of the order",
					"exchange_number": "ID of the exchange",
					"created_at":      "The time at which this order detail row was created",
					"updated_at":      "The time at which this order was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_details": {
				primaryKeys:       []string{"sam_number", "uuid"},
				Description:       "The Product Details table contains information about how many of each product",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":                metadatahelpers.SamNumberDefaultDescription,
					"record_number":             "ID of the order",
					"order_shipping_group_uuid": "UUID of the order shipping group",
					"uuid":                      "UUID of the product details row",
					"sku":                       "SKU of the products",
					"product_name":              "Name of the product",
					"quantity":                  "How many of the product was shipped",
					"sfdc_contract_id":          "The ID of the contract in SFDC",
					"created_at":                "The time at which this product detail row was created",
					"updated_at":                "The time at which this product was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_serial_details": {
				primaryKeys:       []string{"sam_number", "uuid"},
				Description:       "The Product Serial Details table contains information about individual serial numbers in each order",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":           metadatahelpers.SamNumberDefaultDescription,
					"record_number":        "ID of the order",
					"uuid":                 "UUID of the product serial details row",
					"product_details_uuid": "UUID of the product details row",
					"serial":               "Serial number of the device shipped",
					"created_at":           "The time at which this product serial detail row was created",
					"updated_at":           "The time at which this product was last updated",
					"sfdc_contract_id":     "The ID of the contract in SFDC",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rma_device_serials": {
				primaryKeys:       []string{"serial_number", "sam_number", "entry_created_at"},
				PartitionStrategy: DateColumnFromTimestamp("entry_created_at"),
				Description:       "This table stores serial data for devices exchanged in RMAs.",
				ColumnDescriptions: map[string]string{
					"serial_number":        "Serial number for the device being exchanged",
					"sam_number":           metadatahelpers.SamNumberDefaultDescription,
					"shipping_group_uuid":  "UUID for the related shipping group (finopsdb.rma_shipping_groups.uuid)",
					"rma_order_number":     "Order number for the related RMA (finopsdb.rmas.order_number)",
					"rma_order_created_at": "When related RMA was created",
					"entry_created_at":     "When this row was created",
					"shipping_status_enum": metadatahelpers.EnumDescription("Shipping status for this hardware", finopsproto.ShippingStatusType_name),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rma_hardware": {
				primaryKeys: []string{"sam_number", "uuid"},
				ProtoSchema: ProtobufSchema{
					"serials_proto": {
						Message:   &finopsproto.RmaHardwareSerials{},
						ProtoPath: "go/src/samsaradev.io/platform/finops/finopsproto/finops.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table stores hardware info for RMAs.",
				ColumnDescriptions: map[string]string{
					"uuid":                 "Unique id for this row.",
					"rma_uuid":             "The column you can use to join to the rmas table",
					"serial":               "This field is not used. It was from an earlier table design, and is no longer populated.",
					"sam_number":           metadatahelpers.SamNumberDefaultDescription,
					"shipping_status_enum": metadatahelpers.EnumDescription("Shipping status for this hardware", finopsproto.ShippingStatusType_name),
					"to_customer_enum":     metadatahelpers.EnumDescription("Indicates if this is the shipment of new hardware going to the customer, or the shipment of broken hardware coming back to Samsara.", finopsproto.ToCustomerType_name),
					"created_at":           "When this record was created.",
					"rma_order_number":     "The order number for this RMA. This is the customer facing order number that they will use to keep track of this order.",
					"shipping_group_uuid":  "Key to the rma_shipping_groups table.",
					"serials_proto":        "A proto with a list of serial numbers for this hardware type.",
					"quantity":             "The number of this item for this shipment.",
					"product_name":         "Customer facing product name (Ex: HW-VG34)",
					"product_description":  "Customer facing description of this product. (Ex: Vehicle tracking GPS)",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rma_shipping_groups": {
				primaryKeys: []string{"sam_number", "uuid"},
				ProtoSchema: ProtobufSchema{
					"shipping_tracking_numbers_proto": {
						Message:   &finopsproto.ShippingTrackingNumbers{},
						ProtoPath: "go/src/samsaradev.io/platform/finops/finopsproto/finops.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table stores shipping info for RMAs.",
				ColumnDescriptions: map[string]string{
					"uuid":                            "Unique id for this row.",
					"rma_order_number":                "The column you can use to join to the rmas table",
					"sam_number":                      metadatahelpers.SamNumberDefaultDescription,
					"created_at":                      "When this record was created.",
					"shipping_status_enum":            metadatahelpers.EnumDescription("Shipping status for this hardware", finopsproto.ShippingStatusType_name),
					"shipping_tracking_numbers_proto": "A proto containing a list of tracking numbers for this shipping group.",
					"shipping_carrier_enum":           metadatahelpers.EnumDescription("Shipping carrier for this shippment", finopsproto.ShippingCarrierType_name),
					"item_fulfillment_id":             "The internal netsuite id for this shipping group",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rmas": {
				primaryKeys:                       []string{"sam_number", "order_number"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"shipment_tracking_links": {
				primaryKeys:       []string{"sam_number", "uuid"},
				Description:       "The Shipment Tracking Links table contains tracking numbers for shipments",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":               metadatahelpers.SamNumberDefaultDescription,
					"record_number":            "ID of the order",
					"uuid":                     "UUID of the shipping tracking link",
					"shipping_group_uuid":      "UUID of the order shipping group",
					"delivery_tracking_number": "Tracking number for the shipment",
					"delivery_carrier":         "Which shipping company the shipment is using",
					"created_at":               "The time at which this product detail row was created",
					"updated_at":               "The time at which this product was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"shipping_groups": {
				primaryKeys:       []string{"sam_number", "uuid"},
				Description:       "The Shipping Groups table contains information about shipment groups that go out as part of an order",
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"sam_number":            metadatahelpers.SamNumberDefaultDescription,
					"record_number":         "ID of the order",
					"uuid":                  "UUID of the shipping group",
					"shipping_method":       "What type of shipping was used",
					"status":                "Status of the shipment group",
					"shipping_address_uuid": "UUID of the order shipping address",
					"shipping_email":        "Email where the shipment information was sent",
					"created_at":            "The time at which this product detail row was created",
					"updated_at":            "The time at which this product was last updated",
					"shipped_at":            "The time at which this shipment was shipped",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"trials": {
				Description:       "The Trials table contains information about the free trials from sfdc",
				primaryKeys:       []string{"sam_number", "sfdc_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"sam_number":                 metadatahelpers.SamNumberDefaultDescription,
					"sfdc_id":                    "The unique identifier of an opportunity created from sfdc",
					"start_date":                 "The start date of this free trial",
					"end_date":                   "The end date of this free trial",
					"sfdc_modified_at":           "The last time sfdc modifies the data",
					"updated_at":                 "The time at which this trial row was last updated",
					"created_at":                 "The time at which this trial row was created",
					"netsuite_transfer_order_id": "Use to map the devices/licenses that are a part of a trial",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"uat_device_allowlist": {
				Description:       "The UAT Device Allowlist table contains information about the set of devices used for User Acceptance Testing (UAT) of BizTech integrations",
				primaryKeys:       []string{"device_serial"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"device_serial": "The serial number of the device",
				},
			},
			"uat_sam_number_allowlist": {
				Description:       "The UAT SAM Number Allowlist table contains information about customers, identified by Samsara Customer Number (SAM Number), that may be used for User Acceptance Testing (UAT) of BizTech integrations.",
				primaryKeys:       []string{"sam_number"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"sam_number": "The Samsara Customer Number (SAM Number) for the customer",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.FirmwareDB,
		schemaPath: "go/src/samsaradev.io/firmware/firmwaremodels/db_schema.sql",
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"firmware_signoff_history": {
				primaryKeys:                       []string{"product_firmware_id", "user_email"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateway_config_versions": {
				primaryKeys:                       []string{"gateway_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateway_program_override_rollout_stages": {
				Description:                       "The gateway_program_override_rollout_stages stores gateways that are enrolled into non-default programs",
				primaryKeys:                       []string{"org_id", "product_id", "gateway_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateway_rollout_stages": {
				primaryKeys:                       []string{"org_id", "product_id", "gateway_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateway_ssh_audits": {
				Description: "The gateway_ssh_audits table stores the SSH attemps to remote gateways.",
				ColumnDescriptions: map[string]string{
					"id":         "auto-incremented primary key of the table to uniquely identify a row",
					"date":       "date when the SSH audit entry was inserted",
					"happend_at": "timestamp when the SSH audit entry was inserted",
					"gateway_id": metadatahelpers.GatewayIdDefaultDescription,
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
				},
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"item_firmwares": {
				primaryKeys:                       []string{"organization_id", "product_id", "item_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_program_override_rollout_stages": {
				Description:                       "The org_program_override_rollout_stages stores orgs that are enrolled into non-default programs",
				primaryKeys:                       []string{"org_id", "product_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"organization_firmwares": {
				primaryKeys:                       []string{"organization_id", "product_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_firmwares": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_group_rollout_stage_by_org": {
				primaryKeys:                       []string{"org_id", "product_group_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_programs": {
				Description:                       "The product_programs table stores non-default program information set to a product_id",
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rollout_stage_firmwares": {
				primaryKeys:                       []string{"rollout_stage", "product_id", "product_program_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"variant_ids": {
				Description:                       "The variant_ids table stores all thor gateways that have been identified as variants by the thor_variant_ingest job",
				primaryKeys:                       []string{"gateway_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.FormsDB,
		schemaPath: "go/src/samsaradev.io/forms/formsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{
			"form_templates": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "A template for a type of custom digital form.",
				ColumnDescriptions: map[string]string{
					"org_id":                               "The organization that the form template belongs to.",
					"uuid":                                 "The unique identifier of the form template.",
					"server_created_at":                    "When the form template was created on the server.",
					"created_by":                           "User who created the form template.",
					"server_updated_at":                    "When the form template or its form content was last updated on the server.",
					"updated_by":                           "User who updated the form template.",
					"title":                                "The title of the form template.",
					"description":                          "The detailed explanation of the form template.",
					"server_deleted_at":                    "The last time the form template was soft deleted, null if not deleted.",
					"product_type":                         metadatahelpers.EnumDescription("The enum representing the product area that the form belongs to.", formsproto.FormProductType_name),
					"current_template_revision_uuid":       "The unique identifier of the current template revision for this template.",
					"deleted_by":                           "User who soft deleted the form template. Null if not deleted.",
					"current_draft_template_revision_uuid": "The unique identifier of the current draft template revision for this template.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_template_revisions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "A revision of a FormTemplate that tracks historical edits to the FormTemplate's form content. The form content for the template revision, e.g. the fields definitions and sections, are stored separately in the form_template_contents table.",
				ProtoSchema: ProtobufSchema{
					"product_specific_data_proto": {
						Message:   &formsproto.FormTemplateRevisionProductSpecificData{},
						ProtoPath: "go/src/samsaradev.io/forms/formsproto/form_template_revision_product_specific_data.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":                      "The organization that the form template revision belongs to.",
					"uuid":                        "The unique identifier of the form template revision.",
					"form_template_uuid":          "The UUID of the associated form template.",
					"server_created_at":           "When the form template revision was created on the server.",
					"created_by":                  "User who created the form template revision.",
					"server_deleted_at":           "The last time the form template revision was soft deleted. Null if not deleted. A template revision should not be deleted if there are form submissions created from the template revision.",
					"product_specific_data_proto": "The product-specific data proto stores data relevant to the product type of the template, e.g. training-course-specific data.",
					"product_specific_data_proto.product_type":                    "determines which product this product-specific-data contains data for, which indicates what nested data object should be parsed.",
					"product_specific_data_proto.form_data":                       "The product-specific data proto to store data relevant to Forms Product.",
					"product_specific_data_proto.course_data":                     "The product-specific data proto to store data relevant to Training Course Product.",
					"product_specific_data_proto.asset_maintenance_settings_data": "The product-specific data proto to store data relevant to Asset Maintenance Work Order Product.",
					"draft_published_at":                                          "When the form template revision draft was published.",
					"server_updated_at":                                           "The last time the form template revision was updated on the server.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_template_revision_contents": {
				primaryKeys:       []string{`org_id`, `form_template_revision_uuid`},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:             &formsproto.FormTemplateRevisionContent{},
						ProtoPath:           "go/src/samsaradev.io/forms/formsproto/form_template_revision_content.proto",
						ExcludeProtoJsonTag: []string{"entitySchema"},
					},
				},
				Description: "Stores the form template revision's content, e.g. field definitions and sections.",
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization that the form template revision content belongs to.",
					"form_template_revision_uuid":       "The UUID of the associated form template revision that the form template revision content belongs to.",
					"proto":                             "The proto representation of the form template revision content.",
					"proto.org_id":                      "The organization that the form template revision content belongs to.",
					"proto.form_template_uuid":          "The UUID of the form template that this form content belongs to.",
					"proto.form_template_revision_uuid": "The UUID of the form template revision that this form content belongs to.",
					"proto.field_definitions":           "The definitions of the fields in the form, i.e. the form questions and types.",
					"proto.sections":                    "The sections within the form. Sections represent groupings of fields within the form that are displayed together beneath a section label.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(1),
			},
			"form_submissions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "A submitted form for a type of custom digital form. Form submissions are created from a form template. Form content, including field inputs for the submission, is stored separately in form_submission_contents table.",
				ColumnDescriptions: map[string]string{
					"org_id":                      "The organization that the form submission belongs to.",
					"uuid":                        "The unique identifier of the form submission.",
					"form_template_uuid":          "The UUID of the associated form template that the form submission was created from.",
					"form_template_revision_uuid": "The UUID of the associated form template revision that the form submission was created from.",
					"server_created_at":           "Time when the form submission was created on the server.",
					"created_by_polymorphic":      "Polymorphic user id for the user/driver who created and submitted the form submission, e.g. 'driver-12345' or 'user-67890'.",
					"client_submitted_at":         "When the form submission was submitted on the client, required to track submission times in mobile while device is offline.",
					"client_submission_timezone":  "The timezone that the submitter was in at the time of submission, in IANA format e.g. 'America/Los_Angeles'.",
					"submission_latitude":         "The location latitude the submitter was in at the time of submission.",
					"submission_longitude":        "The location longitude the submitter was in at the time of submission.",
					"server_updated_at":           "When the form submission or form submission content was last updated on the server.",
					"updated_by_polymorphic":      "Polymorphic user id for the user/driver who updated the form submission most recently, e.g. 'driver-12345' or 'user-67890'.",
					"server_deleted_at":           "The last time the form submission was soft deleted. Null if not deleted.",
					"product_type":                metadatahelpers.EnumDescription("The enum representing the product area that the form belongs to.", formsproto.FormProductType_name),
					"deleted_by_polymorphic":      "Polymorphic user id for the user or programmatic service/trigger/event that deleted the form submission.",
					"status":                      metadatahelpers.EnumDescription("The enum representing the status of the form submission.", formsproto.FormSubmissionStatus_name),
					"assigned_to_polymorphic":     "Polymorphic user id for the user/driver assigned to the task.",
					"assigned_by_polymorphic":     "Polymorphic user id for the user/driver who assigned the task.",
					"assigned_at":                 "When the form submission was assigned.",
					"due_date":                    "When the form submission is due.",
					"started_at":                  "When the form submission was started.",
					"completed_at":                "When the form submission was completed.",
					"canceled_at":                 "When the form submission was canceled.",
					"canceled_by_polymorphic":     "Polymorphic user id for the user or programmatic service/trigger/event that canceled the form submission.",
					"title":                       "The title of the form submission.",
					"is_required_assignment":      "Whether the form submission is required to be completed by its assigned user/driver",
					"submitted_by_polymorphic":    "Polymorphic user id for the user who submitted the form submission.",
					"score":                       "The score of the form submission.",
					"maximum_score_points":        "The maximum score points of the form submission.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_submission_contents": {
				primaryKeys:       []string{`org_id`, `form_submission_uuid`},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:             &formsproto.FormSubmissionContent{},
						ProtoPath:           "go/src/samsaradev.io/forms/formsproto/form_submission_content.proto",
						ExcludeProtoJsonTag: []string{"entityInstanceContent"},
					},
				},
				Description: "Stores the form submission's input, e.g. the answers to the form questions (form fields).",
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization that the form template revision belongs to.",
					"form_submission_uuid":              "The UUID of the associated form submission that the form submission content belongs to.",
					"proto":                             "The proto representation of the form content.",
					"proto.org_id":                      "The id of the org that the submission belongs to.",
					"proto.form_submission_uuid":        "The UUID of the form submission that this content belongs to.",
					"proto.form_template_uuid":          "The UUID of the form template that this submission was created from.",
					"proto.form_template_revision_uuid": "The UUID of the form template revision that this submission was created from.",
					"proto.field_inputs":                "The inputs for the fields in the form, i.e. the answers to the questions on the form.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_relationships": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Describes a relationship between two form submissions. The relationship has two sides, left and right, as well as a relationship type, e.g. Created or CreatedBy. If A was created by B, then Left=A, Right=B, RelationshipType=CreatedBy. The same relationship could also be modeled by Left=B, Right=A, RelationshipType=Created.",
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization that the form relationship belongs to.",
					"uuid":                              "The unique identifier for the form relationship.",
					"left_form_template_uuid":           "The UUID of the associated form template that the left form submission was created from.",
					"left_form_template_revision_uuid":  "The UUID of the associated form template revision that the left form submission was created from.",
					"left_form_submission_uuid":         "The UUID of the associated left form submission.",
					"left_form_field_definition_uuid":   "The UUID of the associated left form field definition, optional.",
					"left_form_product_type":            metadatahelpers.EnumDescription("The enum representing the product area that the left form belongs to.", formsproto.FormProductType_name),
					"right_form_template_uuid":          "The UUID of the associated form template that the right form submission was created from.",
					"right_form_template_revision_uuid": "The UUID of the associated form template revision that the right form submission was created from.",
					"right_form_submission_uuid":        "The UUID of the associated right form submission.",
					"right_form_field_definition_uuid":  "The UUID of the associated right form field definition, optional.",
					"right_form_product_type":           metadatahelpers.EnumDescription("The enum representing the product area that the right form belongs to.", formsproto.FormProductType_name),
					"form_relationship_type":            metadatahelpers.EnumDescription("The enum representing the form relationship type, e.g. Created or CreatedBy.", formsproto.FormRelationshipType_name),
					"server_created_at":                 "Time when the form relationship was created on the server.",
					"created_by_polymorphic":            "Polymorphic user id for the user/driver who created the form relationship, e.g. 'driver-12345' or 'user-67890'.",
					"server_updated_at":                 "When the form relationship was last updated on the server.",
					"updated_by_polymorphic":            "Polymorphic user id for the user/driver who updated the form relationship most recently, e.g. 'driver-12345' or 'user-67890'.",
					"server_deleted_at":                 "The last time the form relationship was soft deleted. Null if not deleted.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_index_person_fields": {
				primaryKeys:       []string{`org_id`, `field_key`, `polymorphic_user_id`, `form_submission_uuid`},
				PartitionStrategy: SinglePartition,
				Description:       "An index to quickly search for users and/or drivers across form submissions.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The organization that the form submission belongs to.",
					"field_key":            "The name of product-specific field (e.g. 'assignedTo').",
					"polymorphic_user_id":  "The UUID of the associated polymorphic user (e.g. 'user-{uuid}'.",
					"form_submission_uuid": "The UUID of the associated form submission.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_index_single_select_fields": {
				primaryKeys:       []string{`org_id`, `field_key`, `selected_option_uuid`, `form_submission_uuid`},
				PartitionStrategy: SinglePartition,
				Description:       "An index to quickly search for product-defined single select values.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The organization that the form submission belongs to.",
					"field_key":            "The name of product-specific field (e.g. 'priority').",
					"selected_option_uuid": "The UUID of the associated single select option.",
					"form_submission_uuid": "The UUID of the associated form submission.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"form_index_number_fields": {
				primaryKeys:       []string{`org_id`, `field_key`, `number_value`, `form_submission_uuid`},
				PartitionStrategy: SinglePartition,
				Description:       "An index to quickly search for product-defined number values.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The organization that the form submission belongs to.",
					"field_key":            "The name of product-specific field (e.g. 'priority').",
					"number_value":         "The numeric value entered for this field.",
					"form_submission_uuid": "The UUID of the associated form submission.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.FuelCardIntegrationsDB,
		schemaPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardintegrationsmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"wex_integration_configs": {
				Description:       "Represents the configuration for customers' integrations with the Wex fuel card provider.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":             "The organization that the integration configuration belongs to.",
					"uuid":               "The unique identifier of the integration configuration.",
					"created_at":         "The time at which the integration configuration was created.",
					"created_by_user_id": "The ID of the user who created the integration configuration.",
					"updated_at":         "The time at which the integration configuration was last updated.",
					"updated_by_user_id": "The ID of the user who last updated the integration configuration.",
					"auth_credentials":   "The credentials used to authenticate with Wex on behalf of the customer.",
					"last_sync_at":       "The time at which the integration last synchronized with Wex.",
					"contact_email":      "The customer's contact email address for the integration.",
					"file_names":         "Test set of file prefixes used to match files to import.",
				},
			},
		},
	},
	{
		name:       dbregistry.FuelCardsDB,
		schemaPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"fleetcor_integration_configs": {
				primaryKeys:                       []string{"org_id", "fleetcor_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"fuel_log_unverified_reasons": {
				Description:       "Reasons why a fuel log is unverified.",
				primaryKeys:       []string{"org_id", "log_key", "reason"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":  "The organization that the integration configuration belongs to.",
					"log_key": "The unique identifier of the fuel log (1-Many relationship).",
					"reason":  "The reason why the fuel log is unverified, fuelcardsproto.UnverifiedReason.",
				},
			},
			"fuel_log_vehicles": {
				Description:                       "The fuel_log_vehicles table tracks vehicles that were near the purchase location at the time of the purchase.",
				primaryKeys:                       []string{"org_id", "log_key", "vehicle_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"fuel_logs": {
				Description: "The fuel_logs table contains fuel purchase records to be displayed on the Fuel Purchase Report.",
				primaryKeys: []string{"org_id", "log_key"},
				ProtoSchema: ProtobufSchema{
					"original_data": {
						Message:              &fuelcardsproto.RawLog{},
						ProtoPath:            "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto",
						EnablePermissiveMode: true,
					},
					"error_blob": {
						Message:   &fuelcardsproto.ErrorContent{},
						ProtoPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto",
					},
					"nearby_fueling_stations": {
						Message:   &fuelcardsproto.NearbyFuelingStations{},
						ProtoPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto",
					},
					"unverified_details": {
						Message:   &fuelcardsproto.UnverifiedDetails{},
						ProtoPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto",
					},
					"verification_context": {
						Message:   &fuelcardsproto.VerificationContext{},
						ProtoPath: "go/src/samsaradev.io/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(5),
			},
			"merchant_settings": {
				Description:       "The merchant_settings table contains configurations for an organizations preferred fuel station vendors and their associated discounts.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                         "The organization that the merchant settings belongs to.",
					"uuid":                           "The unique identifier of the merchant settings.",
					"merchant_id":                    "The unique identifier of the merchant.",
					"priority":                       "The priority of the merchant settings.",
					"discount_millipercent":          "The discount percentage in millipercent.",
					"discount_price_mills_per_liter": "Deprecated. Discount_price_cents_per_unit supersedes this field as it does not convert anything when storing so precision is not lost.",
					"currency":                       "The currency of the discount price.",
					"created_at":                     "The time at which the merchant settings were created.",
					"updated_at":                     "The time at which the merchant settings were last updated.",
					"discount_price_cents_per_unit":  "The discount price in cents (according to the currency) per volume unit.",
					"volume_unit":                    "The volume unit of the discount price.",
					"fuel_type":                      "The type of fuel this discount applies to, if applicable. NULL means applicable to all fuel types.",
					"fixed_price_cents_per_unit":     "The fixed price in cents per unit for the fuel at this merchant.",
					"fuel_type_norm":                 "The normalized fuel type of the discount. Used for the unique index on the table.",
				},
			},
			"transaction_verification_rules": {
				Description:       "The transaction_verification_rules table contains the each organization's rules for verifying fuel transactions.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
			},
			"upload_events": {
				Description:                       "The upload_events table contains metadata for files that have been ingested either through CSV upload or an integration.",
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("user_uploaded_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"uploads_fuel_logs": {
				Description:                       "The uploads_fuel_logs table maps rows in fuel_logs to rows in upload_events.",
				primaryKeys:                       []string{"org_id", "log_key", "upload_event_uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"wex_integration_files": {
				Description:       "The wex_integration_files table contains metadata for files that have been ingested through a WEX integration.",
				primaryKeys:       []string{"org_id", "name"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.FuelDB,
		schemaPath: "go/src/samsaradev.io/fleet/fuel/fuelmodels/db_schema.sql",
		Sharded:    true,
		// FuelDB does not consistently seem to receive writes, so we don't monitor cdc throughput.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"custom_fuel_cost_emissions": {
				Description:                       "The custom_fuel_cost_emissions table holds custom fuel cost and emissions overrides.",
				primaryKeys:                       []string{"org_id", "fuel_id", "updated_at"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: false,
				ColumnDescriptions: map[string]string{
					"org_id":                   "The id of the org that the custom fuel cost emissions override belongs to.",
					"fuel_id":                  "The canonical fuel type flattened into its unique id.",
					"updated_at":               "The time at which this record was last updated.",
					"fuel_cost_per_unit":       "The overriding cost per unit of this fuel type, if set.",
					"updated_by":               "The id of the user that updated/created this override.",
					"emissions_grams_per_unit": "The overriding emissions grams per unit of this fuel type, if set.",
					"currency_source":          "The currency which the overriding cost is based on, if set.",
					"currency_exchange_rate":   "The exchange rate from the currency source to USD as recorded at the time when the record created, if set.",
				},
			},
			"custom_fuel_costs": {
				primaryKeys:                       []string{"org_id", "updated_at"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_efficiency_configs": {
				primaryKeys:                       []string{"org_id", "param_key", "config_option", "created_at"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_efficiency_profiles_devices": {
				primaryKeys:                       []string{"org_id", "device_id"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_efficiency_score_profiles": {
				primaryKeys:                       []string{"org_id", "profile_uuid"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"fuel_type_metadata": {
				Description:                       "The fuel_type_metadata table holds the metadata of a vehicle which is used to calculate its canonical fuel type.",
				primaryKeys:                       []string{"org_id", "device_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: false,
				ColumnDescriptions: map[string]string{
					"org_id":                "The id of the org that the fuel type metadata record belongs to.",
					"device_id":             "The id of the vehicle associated with this fuel type record.",
					"make":                  "The make of the vehicle.",
					"model":                 "The model of the vehicle.",
					"year":                  "The year of the vehicle.",
					"electrification_level": "The electrification type of the vehicle as decoded from the vin.",
					"primary_fuel_type":     "The primary fuel type of the vehicle as decoded from the vin.",
					"secondary_fuel_type":   "The secondary fuel type of the vehicle as decoded from the vin.",
					"created_at":            "The time at which this fuel type metadata record was created.",
					"updated_at":            "The time at which this fuel type metada record was last updated.",
				},
			},
			"fuel_types": {
				Description:                       "The fuel_types table holds the canonical fuel type records for vehicles as defined either automatically or manually overridden.",
				primaryKeys:                       []string{"org_id", "device_id", "created_at"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				DataModelTable:                    true,
				EmptyStringsCastToNull_DO_NOT_SET: false,
				ColumnDescriptions: map[string]string{
					"org_id":               "The id of the org that the fuel type record belongs to.",
					"device_id":            "The id of the vehicle associated with this fuel type record.",
					"uuid":                 "The unique identifier of this device fuel type record.",
					"engine_type":          metadatahelpers.EnumDescription("The enum representing the type of engine.", fueltypeproto.EngineType_name),
					"gasoline_type":        metadatahelpers.EnumDescription("The enum representing the type of gasoline.", fueltypeproto.GasolineType_name),
					"diesel_type":          metadatahelpers.EnumDescription("The enum representing the type of diesel.", fueltypeproto.DieselType_name),
					"gaseous_type":         metadatahelpers.EnumDescription("The enum representing the type of gas.", fueltypeproto.GaseousType_name),
					"configurable_fuel_id": "The configurable fuel type id that this fuel type record maps to.",
					"hydrogen_type":        metadatahelpers.EnumDescription("The enum representing the type of hydrogen.", fueltypeproto.HydrogenType_name),
					"battery_charge_type":  metadatahelpers.EnumDescription("The enum representing the charge type.", fueltypeproto.BatteryChargeType_name),
					"source":               metadatahelpers.EnumDescription("The enum representing the source of the fuel type record.", fueltypeproto.FuelTypeSource_name),
					"created_at":           "The time at which this fuel type record was created.",
					"created_by":           "The id of the user that created this record if applicable.",
				},
			},
			"ifta_detail_job_outputs": {
				primaryKeys:                       []string{"org_id", "job_uuid", "s3_key"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"ifta_detail_jobs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{
					"args_proto": {
						Message:   &iftaproto.DetailJobArgs{},
						ProtoPath: "go/src/samsaradev.io/fleet/fuel/ifta/iftaproto/ifta.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"ifta_integrations": {
				primaryKeys:                       []string{"org_id", "integration_type", "integration_customer_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"latest_fuel_types": {
				Description:       "The latest_fuel_types table holds the latest canonical fuel type record for vehicles as defined either automatically or manually overridden.",
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				DataModelTable:    true,
				ColumnDescriptions: map[string]string{
					"org_id":               "The id of the org that the fuel type record belongs to.",
					"device_id":            "The id of the vehicle associated with this fuel type record.",
					"uuid":                 "The unique identifier of this device fuel type record.",
					"engine_type":          metadatahelpers.EnumDescription("The enum representing the type of engine.", fueltypeproto.EngineType_name),
					"gasoline_type":        metadatahelpers.EnumDescription("The enum representing the type of gasoline.", fueltypeproto.GasolineType_name),
					"diesel_type":          metadatahelpers.EnumDescription("The enum representing the type of diesel.", fueltypeproto.DieselType_name),
					"gaseous_type":         metadatahelpers.EnumDescription("The enum representing the type of gas.", fueltypeproto.GaseousType_name),
					"configurable_fuel_id": "The configurable fuel type id that this fuel type record maps to.",
					"hydrogen_type":        metadatahelpers.EnumDescription("The enum representing the type of hydrogen.", fueltypeproto.HydrogenType_name),
					"battery_charge_type":  metadatahelpers.EnumDescription("The enum representing the charge type.", fueltypeproto.BatteryChargeType_name),
					"source":               metadatahelpers.EnumDescription("The enum representing the source of the fuel type record.", fueltypeproto.FuelTypeSource_name),
					"updated_at":           "The time at which this fuel type record was last updated.",
					"updated_by":           "The id of the user that last updated this record if applicable.",
				},
			},
			"power_supply_units": {
				Description:       "The power_supply_units table stores power supply unit information including fuel tank size and battery capacity for devices.",
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                              "The organization that the power supply unit belongs to.",
					"device_id":                           "The device associated with this power supply unit.",
					"primary_fuel_tank_size_milliliters":  "The size of the primary fuel tank in milliliters.",
					"original_battery_capacity_watt_hour": "The original battery capacity in watt-hours.",
					"created_at":                          "The time at which this record was created.",
					"updated_at":                          "The time at which this record was last updated.",
					"updated_by":                          "The id of the user that updated this record.",
				},
			},
			"profile_speed_thresholds": {
				primaryKeys:                       []string{"org_id", "profile_id", "created_at"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.GeoEventsDB,
		schemaPath: "go/src/samsaradev.io/platform/locationservices/geoevents/geoeventsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"events_builder_status": {
				Description:                       "Stores highwater markers for the Geo Fence Event Builder",
				primaryKeys:                       []string{"org_id", "device_id"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.InboxDB,
		schemaPath: "go/src/samsaradev.io/inbox/inboxmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"inbox_rows": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at"),
				Description:       "Stores high urgency messages displayed to users as banners.",
			},
		},
	},
	{
		name:       dbregistry.IndustrialCoreDB,
		schemaPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoremodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"alert_event_audits": {
				primaryKeys:       []string{"org_id", "alert_id", "object_ids", "output_key", "alert_started_at_ms", "happened_at_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("happened_at_ms"),
				Description:       "Represents user actions in response to an alert event like resolving/snoozing/etc.",
				ColumnDescriptions: map[string]string{
					"org_id":              "The org that the alert event audit belongs to.",
					"audit_type_id":       metadatahelpers.EnumDescription("The type of audit event.", industrialcoreproto.AlertEventAuditType_name),
					"user_id":             "The user that initiated this audit event.",
					"happened_at_ms":      "The timestamp when this audit event happened.",
					"alert_id":            "The ID of the alert this audit event belongs to (in alertsdb)",
					"object_ids":          "The stringified object type/ids to which this audit event's alert pertains to (like 'machineinput#<id>').",
					"output_key":          "The subcondition UUID to which this audit event's alert pertains to (NOTE: this only applies to industrial alerts).",
					"alert_started_at_ms": "The timestamp of the start of this audit event's alert event.",
					"summary":             "The human-readible summary text of this audit event.",
					"remote_ip_address":   "Remote IP address of the user performing the action.",
					"proxy_ip_address":    "Proxy IP address of the user performing the action.",
					"server_ip_address":   "Server IP address the user was connected to when performing the action.",
					"user_agent":          "User agent in use by the user for the action.",
					"tag_id":              "Associated tag id, if applicable.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_label_assets": {
				primaryKeys:       []string{"org_id", "asset_label_uuid", "asset_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents an association between an asset label and and an asset. This relationship can be many-to-many.",
				ColumnDescriptions: map[string]string{
					"org_id":           "The org that the asset / asset label belongs to.",
					"asset_label_uuid": "The id of the asset label that is associated with the asset.",
					"asset_uuid":       "The id of the asset that is associated with the asset label.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_label_dynamic_dashboards": {
				primaryKeys:       []string{"org_id", "asset_label_uuid", "asset_label_key_uuid", "dynamic_dashboard_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents the many-to-many association between asset labels and and a dynamic dashboards. A dynamic dashboard can also be associated with an asset label key.",
				ColumnDescriptions: map[string]string{
					"org_id":                 "The id of the org that the asset / asset label belongs to.",
					"asset_label_uuid":       "The id of the asset label that is associated with the asset.",
					"asset_label_key_uuid":   "The id of the asset label key that is associated with the asset.",
					"dynamic_dashboard_uuid": "The id of the dynamic dashboard that is associated with the asset label.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_label_keys": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "An asset label key is used in the key-value pair that makes up asset labels.",
				ColumnDescriptions: map[string]string{
					"org_id":     "The org that the asset label key belongs to.",
					"uuid":       "The id of the asset label key.",
					"name":       "The name of the asset label key.",
					"data_type":  "The data type for asset labels using this asset label key.",
					"created_at": "The time when the asset label key is created.",
					"created_by": "The id of the user who created the asset label key.",
					"updated_at": "The time when the asset label key was last updated.",
					"updated_by": "The ID of the user who last updated the asset label key.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_labels": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "An asset label is used to group together a collection of assets.",
				ColumnDescriptions: map[string]string{
					"org_id":                  "The org that the asset label belongs to.",
					"uuid":                    "The id of the asset label.",
					"parent_asset_label_uuid": "The id of the asset label's parent.",
					"key_uuid":                "The id of the asset label key for this asset label.",
					"string_value":            "The value of this asset label if it uses the String data type.",
					"created_at":              "The time when the asset label is created.",
					"created_by":              "The id of the user who created the asset label.",
					"updated_at":              "The time when the asset label was last updated.",
					"updated_by":              "The ID of the user who last updated the asset label.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_manual_entry_configs": {
				primaryKeys: []string{"org_id", "asset_uuid", "machine_input_id"},
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialcoreproto.MachineMachineInputConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/machine_input.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Manual Entry Configs configs for Asset.",
				ColumnDescriptions: map[string]string{
					"org_id":           "The Org that the Asset belongs to.",
					"asset_uuid":       "The uuid of the asset this manual entry config is associated with.",
					"machine_input_id": "The MachineInput Id.",
					"created_at":       "The time when the config was last updated.",
					"created_by":       "The id of the user who created this config.",
					"config_proto":     "Blob that stores a manual entry config.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_reports": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"config": {
						Message:   &industrialcoreproto.AssetsReportConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/asset_reports.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Description:           "Represents the configuration settings used to build an asset report preview.",
				ColumnDescriptions: map[string]string{
					"org_id":                 "The org that the assets report belongs to.",
					"uuid":                   "The id of the assets report.",
					"name":                   "The name of the assets report.",
					"config":                 "Blob that stores the config of the assets report.",
					"dashboard_id":           "The ID of the dashboard the report belongs to.",
					"dynamic_dashboard_uuid": "The UUID of the dynamic dashboard the report belongs to.",
					"is_featured":            "Boolean that represents if the asset report is the org's featured report.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},

				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"assets": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"asset_config": {
						Message:   &industrialcoreproto.AssetConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/assets.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Represents the real life assets that we want to monitor. Asset replaces machine as a logical group of inputs, outputs, and sub-assets.",
				ColumnDescriptions: map[string]string{
					"org_id":                   "The org that the asset belongs to.",
					"uuid":                     "The id of the asset.",
					"parent_id":                "The id of the asset's parent.",
					"name":                     "The name of the asset.",
					"asset_config":             "Blob that stores the config of the asset.",
					"is_template":              "Boolean that represents if the asset is a template.",
					"template_asset_uuid":      "The template asset's UUID, if this is a template child instance.",
					"device_id":                "The id of the device associated with this asset. This field is only populated for asset template instances.",
					"supported_data_sources":   metadatahelpers.EnumDescription("Indicates which data source types are supported on this asset template.", industrialcoreproto.AssetTemplateSupportedDataSources_name),
					"default_template_version": "The version of the default template this asset template was created from, if applicable.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"assets_dashboards": {
				primaryKeys:       []string{"org_id", "asset_uuid", "dashboard_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Associations between Assets and Dashboards.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The org that the asset belongs to.",
					"asset_uuid":           "The asset Uuid.",
					"dashboard_id":         "The Dashboard Id.",
					"is_default_dashboard": "True represents that the dashboard is default for this asset.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"control_programs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"code": {
						Message:   &industrialcoreproto.ControlProgramCode{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/control_programs.proto",
					},
					"libraries": {
						Message:   &industrialcoreproto.ControlProgramLibraries{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/control_programs.proto",
					},
				},
				Description: "Control Programs are program templates that store some code/logic and the metadata necessary to deploy it to devices.",
				ColumnDescriptions: map[string]string{
					"org_id":       "The org that the control program belongs to.",
					"uuid":         "The id of this control program revision.",
					"program_id":   "This program's id that remains consistent across revisions.",
					"name":         "The name of the control program.",
					"program_type": metadatahelpers.EnumDescription("This control program's type.", industrialcoreproto.ControlProgramType_name),
					"code":         "Blob that stores the code for this control program.",
					"deployments":  "Blob that stores info about the devices that this program is currently deployed to.",
					"libraries":    "Blob that stores the code for this control program.",
					"created_by":   "The id of the user that created this program.",
					"created_at":   "The time when the control program was created.",
					"updated_by":   "The id of the user that last updated this program.",
					"updated_at":   "The time when the control program was last updated.",
					"deleted":      "True represents that the control program has been deleted.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"control_variables": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores the metadata required to track data inputs and outputs in control programs.",
				ColumnDescriptions: map[string]string{
					"org_id":              "The org that the control variable belongs to.",
					"uuid":                "The id of the control variable.",
					"type":                metadatahelpers.EnumDescription("This control variable's type.", industrialcoreproto.ControlVariableType_name),
					"device_id":           "The id of the device associated with this control variable.",
					"alias_uuid":          "The uuid of the alias this control variable is associated with.",
					"machine_input_id":    "The MachineInput Id associated with reading data from this control variable.",
					"machine_output_uuid": "The MachineOutput Uuid associated with writing data to this control variable.",
					"created_at":          "The time when the control variable is created.",
					"updated_at":          "The time when the control variable was last updated.",
					"program_id":          "The ID of the program template this control variable is used in.",
					"deleted":             "True represents that the control program has been deleted.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"device_programs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"deployment": {
						Message:   &industrialcoreproto.DeviceProgramDeployment{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/device_programs.proto",
					},
				},
				Description: "Programs deployed to devices.",
				ColumnDescriptions: map[string]string{
					"org_id":       "The org that the device is in.",
					"device_id":    "The device id.",
					"program_id":   "The program id.",
					"program_uuid": "The program revision uuid.",
					"program_hash": "The hash of compiled program binary for the specified device.",
					"deployment":   "Blob that stores the info used to build and deploy this program binary.",
					"deployed":     "True represents that the this program revision is currently deployed to this device.",
					"deployed_at":  "The time when this program revision was deployed to this device.",
					"ended_at":     "The time when this program revision deployment was removed from this device.",
					"uuid":         "The id of this device program deployment model.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dynamic_dashboards": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &industrialhubproto.Dashboard{},
						ProtoPath: "go/src/samsaradev.io/hubproto/industrialhubproto/dashboard.proto",
					},
					"graphical_config_proto": {
						Message:   &industrialhubproto.GraphicalDashboard{},
						ProtoPath: "go/src/samsaradev.io/hubproto/industrialhubproto/graphical_dashboard.proto",
					},
				},
				Description: "A dynamic dashboard is a dashboard that is comprised of data groups, instead of data inputs. It applies to a collection of assets, and when viewed on a particular asset, it uses the data inputs from that asset that correspond to the configured data group.",
				ColumnDescriptions: map[string]string{
					"org_id":                       "The org that the dynamic dashboard belongs to.",
					"uuid":                         "The id of the dynamic dashboard.",
					"name":                         "The name of the dynamic dashboard.",
					"config_proto":                 "Blob that stores a dashboard's type and grid config.",
					"graphical_config_proto":       "Blob that stores a graphical dashboard config.",
					"is_default_dashboard":         "Boolean that represents if the dashboard should be rendered by default when navigating to an associated asset's dashboard tab.",
					"enable_conditional_rendering": "Boolean which controls that if a component's data group does not have an accompanying data input/output for the selected asset, it will be hidden.",
				},
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},

				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				cdcVersion_DO_NOT_SET:             0,
			},
			"machine_inputs_reports": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"data": {
						Message:   &industrialcoreproto.MachineInputsReportData{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/custom_reports.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Stores a set of machine inputs used to build a report.",
				ColumnDescriptions: map[string]string{
					"org_id": "The org that the machine inputs report belongs to.",
					"uuid":   "The id of the machine inputs report.",
					"name":   "The name of the machine inputs report.",
					"data":   "Blob that stores the machine input ids of the report. ",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machine_outputs": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"config": {
						Message:   &hubproto.MachineOutputConfig{},
						ProtoPath: "go/src/samsaradev.io/hubproto/machine_output.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Holds the configuration settings for a single physical output.",
				ColumnDescriptions: map[string]string{
					"org_id":                    "The org that the machine output belongs to.",
					"uuid":                      "The id of the machine output.",
					"name":                      "The name of the machine output.",
					"output_type":               metadatahelpers.EnumDescription("The output type of this machine output.", hubproto.DeviceConfig_StatScalerConfig_MachineOutputConfig_OutputType_name),
					"device_id":                 "The id of the device associated with this machine output.",
					"machine_input_id":          "The id of the machine input associated with this machine output.",
					"config":                    "Blob that stores a machine output config. Consists of submessages with the settings for each output type.",
					"created_by":                "The id of the user who created this machine output.",
					"asset_id":                  "The id of the asset this machine output is associate with.",
					"type_id":                   "The id of the machine input type (aka data group) that this machine output is associated with.",
					"is_template":               "Indicates whether this MachineOutput row is a virtual/template data output.",
					"template_data_output_uuid": "The id of the MachineOutput that this instance is tied to, if it is a template child.",
					"data_input_data_group_id":  "The data group that identifies the paired data input for this data output on a particular asset.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"machines_reports": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"config": {
						Message:   &industrialcoreproto.MachinesReportConfig{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/custom_reports.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Stores a set of machines and type, aka column, settings used to build a report.",
				ColumnDescriptions: map[string]string{
					"org_id": "The org that the machines report belongs to.",
					"uuid":   "The id of the machines report.",
					"name":   "The name of the machines report.",
					"config": "Blob that stores the machine ids and column metadata of the report.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"plc_binaries": {
				primaryKeys: []string{"org_id", "program_id"},
				ProtoSchema: ProtobufSchema{
					"program_variables": {
						Message:   &industrialcoreproto.ProgramVariables{},
						ProtoPath: "go/src/samsaradev.io/industrial/industrialcore/industrialcoreproto/plc_binary.proto",
					},
				},
				PartitionStrategy: SinglePartition,
				Description:       "Contains programs that can be deployed on a Programmable Logic Controller. This allows to perform some closed loop controls on a device.",
				ColumnDescriptions: map[string]string{
					"org_id":            "The organization the target device belongs to.",
					"device_id":         "The target device we want to deploy the program on.",
					"program_id":        "A unique identifier for the program.",
					"active":            "Boolean to indicate if the program is currently active (0 = inactive, 1 = active).",
					"structured_text":   "This contains the program logic in plain text, the language used is Structured Text (ST).",
					"checksum":          "The checksum of the program to ensure its integrity before execution.",
					"name":              "The program name.",
					"program_variables": "The variables that will be passed to the program.",
					"program_hash":      "The hash of the program to ensure its integrity before execution.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_assets": {
				primaryKeys:       []string{"org_id", "tag_id", "asset_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents an association between a tag and and an asset. This relationship can be many-to-many.",
				ColumnDescriptions: map[string]string{
					"org_id":     "The org that the tag asset belongs to.",
					"tag_id":     "The id of the tag that is associated with the asset.",
					"asset_uuid": "The id of the asset that is associated with the tag.",
					"created_at": "The time when the tag asset was created.",
					"created_by": "The id of the user that created the tag asset.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_dynamic_dashboards": {
				primaryKeys:       []string{"org_id", "tag_id", "dynamic_dashboard_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents an association between a tag and and a dashboard template. This relationship can be many-to-many.",
				ColumnDescriptions: map[string]string{
					"org_id":                 "The org that the tag dynamic dashboard belongs to.",
					"tag_id":                 "The id of the tag that is associated with the dynamic dashboard.",
					"dynamic_dashboard_uuid": "The id of the dynamic dashboard that is associated with the tag.",
					"created_at":             "The time when the tag dynamic dashboard was created.",
					"created_by":             "The id of the user that created the tag dynamic dashboard.",
					"updated_at":             "The time when the tag dynamic dashboard was last updated.",
					"updated_by":             "The id of the user that last updated the tag dynamic dashboard.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.IndustrialTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.InternalDB,
		schemaPath: "go/src/samsaradev.io/infra/internal/internalmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"deploymachinery_runtime_export": {
				primaryKeys:       []string{"pipeline_name"},
				PartitionStrategy: SinglePartition,
				Description:       "Keeps track of the latest time that each deploy pipeline has been run.",
				ColumnDescriptions: map[string]string{
					"pipeline_name": "Name of the deploy pipeline, ex. us-trips-canary.",
					"most_recent":   "Most recent deploy pipeline run finish time.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(1),
			},
			"pull_requests": {
				primaryKeys:           []string{"node_id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Description:           "State store for all samsara-dev/backend pull requests. Kept up to date through Github webhooks to Sambot's statuscheck plugin.",
				ColumnDescriptions: map[string]string{
					"node_id":    "Pull request's unique node ID (provided by Github).",
					"owner":      "Owner of the repo, usually `samsara-dev`.",
					"repo":       "Github repo name, usually `backend`",
					"number":     "Github pull request number.",
					"created_at": "Pull request creation time.",
					"updated_at": "Pull request last updated time.",
					"closed_at":  "Pull request closed time.",
					"author":     "Github handle of the pull request author.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"schema_version": {
				primaryKeys:       []string{"version", "completed"},
				PartitionStrategy: SinglePartition,
				Description:       metadatahelpers.SchemaVersionDefaultDescription,
				ColumnDescriptions: map[string]string{
					"version":   metadatahelpers.SchemaVersionVersionDefaultDescription,
					"completed": metadatahelpers.SchemaVersionCompletedDefaultDescription,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"simulated_orgs": {
				primaryKeys:       []string{"org_name"},
				PartitionStrategy: SinglePartition,
				Description:       metadatahelpers.SchemaVersionDefaultDescription,
				DataModelTable:    true,
				ColumnDescriptions: map[string]string{
					"org_name":       "Org name, unique across samsara. Don't change after creating.",
					"created_at":     "When the orchestrator (SOO) first operated on the org. Not necessarily when the org was created if it was made outside SOO.",
					"source":         "Creation source",
					"definition":     "Simulated org definition proto. See documentation for more info.",
					"state":          "Orchestrator state for this org.",
					"org_id":         "ID for the org, filled in after the org is first created if done by SOO.",
					"version_number": "Registry version number for this org",
				},
			},
			"url_shortener_links": {
				primaryKeys:       []string{"shortname", "version"},
				PartitionStrategy: SinglePartition,
				Description:       "Mappings for Samsara's internal URL shortener service Samsaurus.",
				ColumnDescriptions: map[string]string{
					"id":        "An autoincrement ID.",
					"shortname": "Shortened name, to be appended to https://samsaur.us/ which redirects to the actual URL.",
					"version":   "Version of the shortname mapping, starts at 0 and increments by one for each new mapping.",
					"value":     "The actual URL that the shortname maps to.",
					"clicks":    "Number of clicks on the shortened URL (specific to version).",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-supplychain-ops":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.JobSchedulesDB,
		schemaPath: "go/src/samsaradev.io/fleet/assets/jobschedules/jobschedulesmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"asset_available_hours": {
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				DataModelTable:    true,
				Description:       "Contains the availability of each asset that can be scheduled on a job.",
				ColumnDescriptions: map[string]string{
					"device_id":              "The ID of the asset the availability is defined for.",
					"weekly_availability_ms": "The asset's weekly availability in hours. This is used to calculate the % of utilization over a week (utilization_percentage = utilization_in_a_week_ms / weekly_availability_ms).",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_creation_count": {
				primaryKeys:       []string{"org_id", "asset_type"},
				PartitionStrategy: SinglePartition,
				Description:       "Model count for assets (trailers, powered/unpowered equipment, vehicles by org",
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"asset_type": metadatahelpers.EnumDescription("The type of asset being tracked by this count", assettype.AssetTypeToString),
					"count":      "The number of models of this type that have been created for this organization",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_job_items": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"job_item_proto": {
						Message:   &jobschedulesproto.AssetJobItemProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/jobschedules/jobschedulesproto/jobschedules.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "The different tasks composing a job.",
				ColumnDescriptions: map[string]string{
					"uuid":                  "The unique identifier of the job task.",
					"asset_job_uuid":        "The unique identifier of the job the task is part of.",
					"start_ms":              "The start date of the task in ms.",
					"end_ms":                "The end date of the task in ms.",
					"asset_id":              "The ID of the fleet asset assigned to this task.",
					"industrial_asset_uuid": "The uuid of the industrial asset assigned to this task.",
					"assigned_ancestor_industrial_asset_uuid": "The uuid of an associated ancestor industrial asset.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"asset_jobs": {
				primaryKeys: []string{"org_id", "uuid"},
				ProtoSchema: ProtobufSchema{
					"address": {
						Message:   &jobschedulesproto.AssetJobAddress{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/jobschedules/jobschedulesproto/jobschedules.proto",
					},
				},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "The jobs that the assets of a fleet can be scheduled for.",
				ColumnDescriptions: map[string]string{
					"uuid":          "The unique identified of the job.",
					"name":          "The name of the job.",
					"job_status":    metadatahelpers.EnumDescription("The current job status.", jobschedulesproto.JobStatus_name),
					"customer_name": "The name of the customer this job is done for.",
					"notes":         "General notes about the job.",
					"address":       "The address of the job site.",
					"start_ms":      "The start date of the job in ms.",
					"end_ms":        "The end date of the job in ms.",
					"job_number":    "The user reference for the job.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.LicenseEntityDB,
		schemaPath: "go/src/samsaradev.io/platform/licenseentity/licenseentitymodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"license_assignments": {
				Description:       "The license assignments table contains data about active and past license assignments.",
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: SinglePartition,
				DataModelTable:    true,
				ColumnDescriptions: map[string]string{
					"sam_number":       metadatahelpers.SamNumberDefaultDescription,
					"org_id":           "ID of the organization the assignment belongs to",
					"sku":              "License SKU",
					"entity_id_int":    "The ID of the entity when the entity has an integer ID",
					"entity_id_string": "The ID of the entity when the entity has a string ID",
					"start_time":       "The time the license assignment begins and is considered active",
					"end_time":         "The time the license assignment ends and is no longer considered active",
					"order_id":         "The order number the license was purchased as part of",
					"created_at":       "The time at which this license assignment row was created",
					"updated_at":       "The time at which this license assignment row was last updated",
					"entity_type":      "The type of the entity the license was assigned to",
					"order_number":     "The number of the order in which the assigned license was purchased.",
					"order_line_id":    "The line ID from the order in which the assigned license was purchased.",
					"peripheral_id":    "The ID of the peripheral device (e.g. camera, sensor) to which the license is assigned, if applicable.",
					"peripheral_type":  "The type of the peripheral device (e.g. camera, sensor) to which the license is assigned, if applicable.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.LocaleDB,
		schemaPath: "go/src/samsaradev.io/fleet/fuel/locale/localemodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// LocaleDB doesn't seem to consistently receive traffic in the EU, so we won't monitor throughput there.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: shard0Monitor(ShardMonitor{TimeframeMinutes: 24 * 60}),
			infraconsts.SamsaraAWSEURegion:      {},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"fleetcor_files": {
				primaryKeys:                       []string{"name", "source"},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"locale_fuel_costs": {
				primaryKeys:                       []string{"locale_id", "updated_at", "configurable_fuel_id"},
				PartitionStrategy:                 SinglePartition,
				Production:                        true,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.MaintenanceDB,
		schemaPath: "go/src/samsaradev.io/fleet/maintenance/maintenancemodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"service_log_attachments": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("create_time_ms"),
				Description:       "Attachments associated with service logs.",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"uuid":            "Unique identifier for the service log attachment.",
					"log_id":          "The service log id that this attachment is associated with.",
					"create_time_ms":  "The time at which the attachment was created.",
					"update_time_ms":  "The time at which the attachment was last updated.",
					"file_name":       "The name of the file.",
					"file_mime_type":  "The mime type of the file such as image, pdf etc.",
					"file_size_bytes": "The size of the file in bytes.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.MdmDB,
		schemaPath: "go/src/samsaradev.io/mobile/mdm/mdmmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"android_device_location": {
				primaryKeys:       []string{"org_id", "android_device_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"enterprise": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"home_customization_template": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Description:       "Configuration for the home screen on the Samsara Launcher",
				ColumnDescriptions: map[string]string{
					"display_name":               "The name of the template",
					"home_screen_text":           "Custom text to render on the home screen",
					"home_screen_text_hex_color": "Color to use for home screen text represented as a hex string",
					"home_screen_image_url":      "A link to a custom image. For fetching the organization logo, use the `Organization.logoUrl` fieldfunc instead.",
					"app_icon_size":              "The size that apps will be rendered on the home screen",
					"logo_display_type":          "Enum representing how logos should be displayed on the home screen. 0 = NONE, 1 = ORG_LOGO, 2 = ADMIN_UPLOADED",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"mdm_device_installed_apps": {
				primaryKeys:       []string{"org_id", "mdm_device_id", "package_name"},
				PartitionStrategy: SinglePartition,
				Description:       "Apps installed on mdm devices",
				ColumnDescriptions: map[string]string{
					"uuid":           "DEPRECATED - No longer needed with new primary key.",
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"mdm_device_id":  "ID of the MDM device this app is installed on",
					"package_name":   "Package name of the app (i.e 'com.samsara.mdm.agent')",
					"version_name":   "Version of the app installed",
					"display_name":   "Display name of the app",
					"version_code":   "The version code of the installed app",
					"install_status": metadatahelpers.EnumDescription("The install status of the app", mdmproto.MdmDeviceInstalledAppStatus_name),
					"updated_at":     "When the app status was last updated",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"mdm_devices": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Description:       "All devices enrolled into the Samsara MDM. Built to replace android_device.",
				ColumnDescriptions: map[string]string{
					"org_id":                           metadatahelpers.OrgIdDefaultDescription,
					"id":                               "ID of the device. This id generated within AMAPI and we adopt it into our data model.",
					"enterprise_id":                    "ID of the enterprise that this device belongs to",
					"device_status":                    metadatahelpers.EnumDescription("The current status of the device", mdmproto.MdmDeviceStatus_name),
					"display_name":                     "Name of the device as configured by the Samsara user",
					"manufacturer":                     "Manufacturer of the device (i.e Samsung)",
					"model":                            "Model of the device (i.e Pixel)",
					"os_version":                       "Version of the operating system the device is running",
					"agent_id":                         "ID of the Samsara Agent app on the device",
					"applied_policy_id":                "ID of the Policy that the device is currently running",
					"applied_policy_version":           "Version of the Policy that the device is currently running",
					"desired_policy_id":                "ID of the Policy that this device has been assigned to the device but might not be applied yet",
					"last_policy_sync_time":            "Timestamp of the last time a policy was sync'd onto the device",
					"auth_id":                          "ID used for this device in the MDM Auth System",
					"created_at":                       "Timestamp of when the device was created",
					"updated_at":                       "Timestamp of when the device was last updated",
					"deleted_with_behavior":            metadatahelpers.EnumDescription("The way in which the device was deleted", mdmproto.MdmDeviceDeleteBehavior_name),
					"deleted_at":                       "Timestamp of when the device was deleted",
					"imei":                             "The International Mobile Equipment Identifier (IMEI) of the device ingested from AMAPI.",
					"meid":                             "The Mobile Equipment IDentifier (MEID) of the device ingested from AMAPI.",
					"mac_address":                      "The Mac Address of the device ingested from AMAPI.",
					"phone_numbers":                    "The Phone Numbers associated with the device ingested from AMAPI. Note, this field is plural because this can be multiple phone numbers represented as a comma separated list (i.e 'phone_number_1,phone_number_2,phone_number_3').",
					"battery_charge_level":             "The most recent battery charge level of the device ingested from AMAPI.",
					"battery_charging_state":           "The most recent battery charging state of the device ingested from AMAPI.",
					"battery_charge_collected_at":      "The timestamp when the battery charge level was last reported through AMAPI.",
					"battery_temperature_celsius":      "The most recent battery temperature of the device ingested from AMAPI.",
					"battery_temperature_collected_at": "The timestamp when the battery temperature was last reported through AMAPI.",
					"brightness":                       "The most recent brightness level of the device from 0 to 100.",
					"brightness_collected_at":          "The timestamp when brightness was last reported.",
					"notification_volume":              "The most recent notification volume level of the device from 0 to 100.",
					"notification_volume_collected_at": "The timestamp when notification volume was last reported.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"mdm_policy_applications": {
				primaryKeys:       []string{"org_id", "policy_id", "package_name"},
				PartitionStrategy: SinglePartition,
				Description:       "Applications that are part of a policy",
				ColumnDescriptions: map[string]string{
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"policy_id":               "ID of the policy for this application",
					"package_name":            "Package name of the app (i.e 'com.samsara.mdm.agent')",
					"do_not_disturb_behavior": metadatahelpers.EnumDescription("The behavior of the app when Do Not Disturb is enabled", mdmproto.MdmDoNotDisturbBehavior_name),
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
			},
			"organization_launcher_config": {
				primaryKeys:           []string{"org_id", "policy_id", "id"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"policy": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_mode_template": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				Description:       "Configuration for the focus mode features on the Samsara Launcher",
				ColumnDescriptions: map[string]string{
					"app_to_launch_on_driving": "Package name of an application to launch for focus mode",
					"created_at":               "Timestamp when the template was created",
					"deeplink_on_driving":      "Android deeplink to open for focus mode",
					"deleted_at":               "Timestamp when the template was soft-deleted",
					"display_name":             "The name of the template",
					"do_not_disturb_state":     metadatahelpers.EnumDescription("Whether Do Not Disturb is enabled in focus mode", mdmproto.MdmDoNotDisturbState_name),
					"driving_app_behavior":     "The action to execute when enabling focus mode, such as launching an app.",
					"speed_threshold":          "Speed threshold (in localized units) for focus mode to be enabled",
					"speed_threshold_units":    metadatahelpers.EnumDescription("The unit localization of the speed thresholds", mdmproto.MdmFocusModeSpeedThresholdUnits_name),
					"time_threshold":           "Duration threshold (in seconds) for focus mode to be enabled",
					"toggle_application":       "Package name of an application for quick navigation in focus mode",
					"unlock_speed_threshold":   "Speed threshold (in localized units) for focus mode to be disabled",
					"unlock_time_threshold":    "Duration threshold (in seconds) for focus mode to be disabled",
					"updated_at":               "Timestamp when the template was updated",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"signup_url": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_mdm_devices": {
				primaryKeys:       []string{"org_id", "mdm_device_id", "tag_id"},
				PartitionStrategy: SinglePartition,
				Description:       "All tags for MDM devices.",
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"tag_id":        "ID of the tag a device is assigned",
					"mdm_device_id": "ID of the device a tag is assigned to. Foreign key to the id column of [mdmdb_shards.mdm_devices](https://amundsen.internal.samsara.com/table_detail/cluster/delta/mdmdb_shards/mdm_devices)",
					"created_by":    "Admin user ID of the user that assigned the tag",
					"created_at":    "Timestamp of when the tag was assigned",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.MediaCatalogDB,
		schemaPath: "go/src/samsaradev.io/mediacatalog/mediacatalogmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"retention_policies": {
				primaryKeys:       []string{"org_id", "media_source_uuid", "media_type"},
				PartitionStrategy: SinglePartition,
				Description:       "Contains the configured retention for Media Catalog data corresponding to a media source + type. If Media Catalog data doesn't have a retention policy, we implicitly retain the media forever.",
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "A randomly-generated unique identifier for this RetentionPolicy row",
					"media_source_uuid": "An identifier for the original media source. This UUID is generated by the client, and clients are responsible for converting b/w MediaSourceUUID and human-understandable IDs (devices, cameras, etc.).",
					"media_type":        metadatahelpers.EnumDescription("The type of media that this RetentionPolicy applies to", mediacatalogtypesproto.MediaType_name),
					"duration_days":     "The # of days we store the corresponding MediaReferences and assets, after which we automatically delete the media. If null, we assume a 'retain media forever' policy.",
					"created_at":        "Timestamp when the RetentionPolicy was created",
					"updated_at":        "Timestamp when the RetentionPolicy was updated",
					"deleted_at":        "Timetsamp when the RetentionPolicy was soft-deleted.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.MessagesDB,
		schemaPath: "go/src/samsaradev.io/fleet/messages/messagesmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadGroups: []components.TeamInfo{
			team.DataPlatform,
			team.MLScience,
		},
		// Skip the database from being synced to HMS as it is sensitive.
		// We share a hashed version with the ML Science team for their use.
		SkipSyncToHms: true,
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_message_channels": {
				primaryKeys:       []string{"org_id", "driver_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents a channel of messages for a driver in the org",
				ColumnDescriptions: map[string]string{
					`org_id`:                      metadatahelpers.OrgIdDefaultDescription,
					`driver_id`:                   metadatahelpers.DriverIdDefaultDescription,
					`driver_last_message_read_at`: "The last time the driver read a message in this channel",
					`driver_last_message_sent_at`: "The last time the driver sent a message in this channel",
					`org_last_message_read_at`:    "The last time the org read a message in this channel",
					`org_last_message_sent_at`:    "The last time the org sent a message in this channel",
					`driver_last_message_seen_at`: "The last time the driver saw a message in this channel",
				},
			},
			"messages": {
				primaryKeys:       []string{"org_id", "driver_message_channel_driver_id", "sent_at"},
				PartitionStrategy: SinglePartition,
				Description:       "This table stores messages sent between drivers and orgs.",
				ColumnDescriptions: map[string]string{
					"org_id":                           metadatahelpers.OrgIdDefaultDescription,
					"driver_message_channel_driver_id": metadatahelpers.DriverIdDefaultDescription,
					"sent_at":                          "The timestamp when the message was sent",
					"message_body":                     "The body of the message",
					"sender_driver_id":                 metadatahelpers.DriverIdDefaultDescription,
					"sender_user_id":                   "The user ID of the sender",
					"uuid":                             "The UUID of the message",
					"metadata":                         "The metadata of the message",
				},
			},
		},
	},
	{
		name:       dbregistry.MobileDB,
		schemaPath: "go/src/samsaradev.io/mobile/mobilemodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{
			"wizards": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "This table holds information for driver app workflows (wizards).",
				ColumnDescriptions: map[string]string{
					"uuid":        "The unique identifier for the wizard",
					"wizard_type": "Type of wizard (Sign In Wizard, End of Day Wizard, etc)",
					"wizard_name": "Name of the wizard (unique)",
					"wizard_tree": "JSON object of all the nodes in the wizard",
					"org_id":      metadatahelpers.OrgIdDefaultDescription,
					"created_at":  "Timestamp at which this wizard was created",
					"updated_at":  "Timestamp when the wizard was last updated",
					"updated_by":  "ID of the administrator that updated the wizard",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"wizard_associations": {
				primaryKeys:       []string{"org_id", "association_id", "wizard_type"},
				PartitionStrategy: SinglePartition,
				Description:       "This table holds information for the relationship between a driver app workflow (wizard) and a driver/route/etc",
				ColumnDescriptions: map[string]string{
					"association_id": "The unique identifier for the driver/route/etc that uses the wizard",
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"wizard_type":    "Type of wizard (Sign In Wizard, End of Day Wizard, etc)",
					"wizard_uuid":    "The unique identifier for the wizard",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"troy_apps_feedback": {
				primaryKeys:       []string{"org_id", "principal_id", "created_at"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table stores feedback and diagnostic information from mobile applications.",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"principal_id":       "ID of the user or driver who submitted the feedback",
					"created_at":         "Timestamp when the feedback was submitted",
					"updated_at":         "Timestamp when the feedback was last updated",
					"categories_json":    "JSON array of feedback categories selected by the user",
					"text":               "The feedback text submitted by the user",
					"total_memory_gb":    "Total memory of the device in gigabytes",
					"app_version_number": "Version number of the mobile application",
					"device_model":       "Model of the device (e.g. iPhone 12, Samsung Galaxy S21)",
					"os":                 "Operating system of the device (e.g. iOS, Android)",
					"os_version":         "Version of the operating system",
					"app_id":             "Identifier of the mobile application",
					"is_resolved":        "Flag indicating if the feedback has been resolved",
					"language":           "Language setting of the device/app when feedback was submitted",
					"feature_flags_json": "JSON object containing the feature flags active when feedback was submitted",
					"release_type":       "Type of app release (e.g. production, beta)",
				},
			},
			"custom_experience_mappings": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "This table stores custom experience mappings between attribute value ids and custom experience template ids.",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"uuid":               "UUID for the custom experience mapping row",
					"attribute_value_id": "ID of the attribute value that the custom experience mapping is associated with",
					"template_id":        "ID of the custom experience template that the custom experience mapping is associated with",
					"created_at":         "Timestamp when the custom experience mapping was created",
					"updated_at":         "Timestamp when the custom experience mapping was last updated",
				},
			},
			"custom_experience_templates": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "This table stores custom experience templates.",
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"uuid":          "UUID for the custom experience template row",
					"name":          "Name of the custom experience template",
					"template_json": "JSON object of the custom experience template",
					"created_at":    "Timestamp when the custom experience template was created",
					"updated_at":    "Timestamp when the custom experience template was last updated",
					"deleted_at":    "Timestamp when the custom experience template was soft-deleted",
				},
			},
			"custom_experience_defaults": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table stores the default custom experience templates.",
				ColumnDescriptions: map[string]string{
					"org_id":      metadatahelpers.OrgIdDefaultDescription,
					"template_id": "ID of the default custom experience template",
					"created_at":  "Timestamp when the default custom experience template was created",
					"updated_at":  "Timestamp when the default custom experience template was last updated",
				},
			},
		},
	},
	{
		name:       dbregistry.Oauth2DB,
		schemaPath: "go/src/samsaradev.io/platform/oauth2/oauth2models/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"oauth2_access_tokens": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"token_proto": {
						Message:   &apiproto.AccessTokenMetadata{},
						ProtoPath: "go/src/samsaradev.io/controllers/api/apiproto/oauth2_access_token.proto",
					},
				},
				Description: "The table that holds info about valid and expired access tokens. Access tokens usually expire after 1 hour.",
				ColumnDescriptions: map[string]string{
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"id":                      "UUID for the access token row.",
					"app_uuid":                "UUID of the app associated with this access token. This is a FK to developer_apps table in cloud db.",
					"request_uuid":            "Request UUID.",
					"service_account_user_id": "Service account user associated with the access token.",
					"created_at":              "Created at timestamp.",
					"expires_at":              "Access token expire timestamp.",
					"token_proto":             "This is a proto of type apiproto.AccessTokenMetadata. This contains metadata information about tokens.",
					"revoked_at":              "Revoked at timestamp.",
					"revoked_by":              "User Id that revoked the token.",
					"created_by":              "User Id that created this token.",
					"signature":               "Access token signature. This is the last portion of the token that the use sends us. It is typically in this format: gzUdQrDW:<orgId>:<hashValue>.<signature>.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"oauth2_refresh_tokens": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"token_proto": {
						Message:   &apiproto.AccessTokenMetadata{},
						ProtoPath: "go/src/samsaradev.io/controllers/api/apiproto/oauth2_access_token.proto",
					},
				},
				Description: "The table that holds info about valid and revoked refresh tokens.",
				ColumnDescriptions: map[string]string{
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"id":                      "UUID for the access token row.",
					"app_uuid":                "UUID of the app associated with this access token. This is a FK to developer_apps table in cloud db.",
					"request_uuid":            "Request UUID.",
					"service_account_user_id": "Service account user associated with the refresh token.",
					"signature":               "Refresh token signature. This is the last portion of the token that the use sends us. It is typically in this format: gzUdQrDW:<orgId>:<hashValue>.<signature>.",
					"created_at":              "Created at timestamp.",
					"created_by":              "User Id that created this token.",
					"expires_at":              "Refresh token expire timestamp. This is typically null. We use the revoked_at column.",
					"token_proto":             "This is a proto of type apiproto.AccessTokenMetadata. This contains metadata information about tokens.",
					"revoked_at":              "Revoked at timestamp.",
					"revoked_by":              "User Id that revoked the token.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.OemDB,
		schemaPath: "go/src/samsaradev.io/fleet/oem/oemmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"oem_integration_auth": {
				primaryKeys: []string{"org_id", "oem_type", "id"},
				ProtoSchema: ProtobufSchema{
					"auth_data": {
						Message:   &oemmodelsproto.EncryptedAuthData{},
						ProtoPath: "go/src/samsaradev.io/fleet/oem/oemmodelsproto/oem_auth_data.proto",
					},
				},
				versionInfo:                       SingleVersion(1),
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"oem_source_enrollment_status": {
				primaryKeys: []string{"org_id", "oem_type", "source_id"},
				ProtoSchema: ProtobufSchema{
					"metadata": {
						Message:   &oemmodelsproto.MetadataInfo{},
						ProtoPath: "go/src/samsaradev.io/fleet/oem/oemmodelsproto/oem_source_enrollment_status.proto",
					},
				},
				versionInfo:                       SingleVersion(1),
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"oem_sources": {
				primaryKeys:       []string{"org_id", "oem_type", "source_id"},
				PartitionStrategy: SinglePartition,
				DataModelTable:    true,
				// We're marking this table as production because the OEM team relies on it for analysis, and
				// it has historically had a lot of problems syncing so we'd like to get notified a bit more
				// urgently when it goes wrong.
				// Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.PingSchedulesDB,
		schemaPath: "go/src/samsaradev.io/fleet/assets/pingschedules/pingschedulesmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"gateway_commands": {
				primaryKeys:       []string{"org_id", "correlation_id"},
				PartitionStrategy: SinglePartition,
				Description:       "The table that holds gateway commands to push to gateways.",
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"gateway_id":     "The gateway id of the gateway command.",
					"correlation_id": "The id of the gateway command. Used to correlate when a gateway checks in due to a command.",
					"command_type":   "The type of command that was issued to the gateway.",
					"status":         "The status of the command.",
					"expires_at":     "Timestamp that indicates when a gateway command expires",
					"deleted_at":     "Timestamp that indicates when a gateway command was deleted",
					"checked_in_at":  "Timestamp that indicates when a gateway checked in.",
					"created_at":     "Created at timestamp",
					"updated_at":     "Updated at timestamp",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"ping_schedules": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "The table that holds ping schedule setting in the org",
				ProtoSchema: ProtobufSchema{
					"customizable_ping_schedule_proto": {
						Message:   &pingschedulesproto.CustomizablePingSchedule{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/pingschedules/pingschedulesproto/pingschedules.proto",
					},
					"wake_on_motion_override_proto": {
						Message:   &pingschedulesproto.WakeOnMotionOverrideProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/assets/pingschedules/pingschedulesproto/pingschedules.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":                             metadatahelpers.OrgIdDefaultDescription,
					"wake_on_motion_enabled":             "Stores whether wake on motion is enabled",
					"wake_on_motion_interval_sec":        "Stores how often an asset should check-in during motion events",
					"ping_interval_sec":                  "Stores how often an asset should check-in",
					"customizable_ping_schedule_enabled": "Stores whether there are scheduled pings or if specific devices/tags are targeted",
					"customizable_ping_schedule_proto":   "Stores scheduled ping information and targeted device/tag ids",
					"wake_on_motion_override_proto":      "Stores targeted device/tag ids and it's check in rate",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"recovery_mode_config": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "The table that holds recovery mode configs for the Wake on Demand feature.",
				ColumnDescriptions: map[string]string{
					"device_id":                       "The device id of the recovery mode config",
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"created_by_user_id":              "The user id that created the recovery mode config",
					"alert_user_upon_expiry":          "Informs whether we should send an email to a user when the config expires",
					"polling_frequency_ms":            "Rate at which a device should check in while the recovery mode config is active",
					"activation_received_by_device":   "Informs whether a recovery mode config has been sent and is active on the device",
					"deactivation_received_by_device": "Informs whether a recovery mode config is no longer on a device after it has expired or been deleted",
					"created_at":                      "Created at timestamp",
					"updated_at":                      "Updated at timestamp",
					"expires_at":                      "Timestamp that indicates when a recovery mode config expires",
					"deleted_at":                      "Timestamp that indicates when a recovery mode config was deleted",
					"activation_config_version":       "The device config version after the version is bumped by the creation of a recovery mode config",
					"deactivation_config_version":     "The device config version after the version is bumped by the expiration/deletion of a recovery mode config",
					"uuid":                            "Recovery mode config uuid",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.PlaceDB,
		schemaPath: "go/src/samsaradev.io/platform/mapping/places/placemodels/db_schema.sql",
		Sharded:    true,
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
			infraconsts.SamsaraAWSEURegion:      euShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
			AuroraDatabaseEngine: infraconsts.AuroraDatabaseEnginePostgres17,
		},
		Tables: map[string]tableDefinition{
			"place": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "Main table for storing places.",
				Production:        false,
				DataModelTable:    false,
				ExcludeColumns:    []string{"geometry", "address_point"},
			},
			"place_contacts": {
				primaryKeys:       []string{"place_id", "contact_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Transitional table for storing place contacts.",
				Production:        false,
				DataModelTable:    false,
			},
			"place_fleet_viewer_tokens": {
				primaryKeys:       []string{"fleet_viewer_token"},
				PartitionStrategy: SinglePartition,
				Description:       "Fleet viewer tokens for places.",
				Production:        false,
				DataModelTable:    false,
			},
			"place_metadata": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "Metadata table for places.",
				Production:        false,
				DataModelTable:    false,
			},
			"tag_places": {
				primaryKeys:       []string{"tag_id", "place_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Tag places table.",
				Production:        false,
				DataModelTable:    false,
			},
		},
	},
	{
		name:            dbregistry.ProductsDB,
		schemaPath:      "go/src/samsaradev.io/models/productsmodels/db_schema.sql",
		mysqlDbOverride: "prod_db",
		Sharded:         false,
		// Productsdb receives consistent traffic in the US, but more sporadic in the EU.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			EnableKinesisStreamDestination: true,
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"core_external_keys": {
				primaryKeys:       []string{"org_id", "uuid"},
				Description:       "Store external id keys for core data entities",
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"uuid":       "Unique key for this external key.",
					"name":       "The customer set name for this external key.",
					"created_at": "When this external key was first created.",
					"updated_at": "When this external key was last updated.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"core_external_values": {
				primaryKeys:       []string{"org_id", "object_type", "object_id", "external_uuid"},
				Description:       "Store external id values for core data entities",
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"object_type":    "The enum type of the object represented by this external value",
					"object_id":      "The id of the object represented by this external value",
					"external_uuid":  "Foreign key to the external key (in core_external_keys) this value relates to.",
					"external_value": "The external id value for this object+key combination.",
					"created_at":     "When this external value was first created.",
					"updated_at":     "When this external value was last updated.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"devices": {
				Description: "This table contains information about devices, which represents a customer's device (e.g a vehicle, or a trailer). The [productsdb.gateways](https://amundsen.internal.samsara.com/table_detail/cluster/delta/productsdb/gateways) table represents the actual Samsara hardware device",
				primaryKeys: []string{"id"},
				ProtoSchema: ProtobufSchema{
					"device_settings_proto": {
						Message:   &deviceproto.DeviceSettings{},
						ProtoPath: "go/src/samsaradev.io/settings/deviceproto/device_settings.proto",
					},
					"proto": {
						Message:              &deviceproto.DeviceProto{},
						ProtoPath:            "go/src/samsaradev.io/settings/deviceproto/devices.proto",
						EnablePermissiveMode: true,
					},
				},
				PartitionStrategy: SinglePartition,
				Production:        true,
				DataModelTable:    true,
				InternalOverrides: RdsTableInternalOverrides{
					LegacyUint64AsBigint: true,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvir_templates_devices": {
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Joins DVIR templates to the devices assigned to those templates",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"dvir_template_uuid": "The UUID of the DVIR template the device is assigned to",
					"device_id":          metadatahelpers.DeviceIdDefaultDescription,
					"created_at":         "The timestamp for when this record was created",
					"created_by":         "The user who created the assignment",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"enrollment_logs": {
				primaryKeys:       []string{"device_id", "created_at", "enrollment_type"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table contains the history of hubserver key enrollment attempts",
				ColumnDescriptions: map[string]string{
					"device_id":       metadatahelpers.DeviceIdDefaultDescription,
					"created_at":      "The timestamp for when this record was created",
					"remote_ip":       "The remote IP address that this enrollment attempt originated from",
					"succeeded":       "Whether the enrollment attempt succeeded",
					"enrollment_type": metadatahelpers.EnumDescription("The type of enrollment attempt: ", models.EnrollmentTypeMap),
					"gateway_id":      metadatahelpers.GatewayIdDefaultDescription,
					"gateway_serial":  "The serial of the gateway",
				},
			},
			"gateway_device_history": {
				primaryKeys:       []string{"gateway_id", "timestamp"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains the history of gateway and device associations",
				ColumnDescriptions: map[string]string{
					"gateway_id": metadatahelpers.GatewayIdDefaultDescription,
					"device_id":  metadatahelpers.DeviceIdDefaultDescription,
					"timestamp":  "The timestamp at which the gateway and device were linked.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateways": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Production:        true,
				DataModelTable:    true,
				Description:       "This table contains information associated with a Samsara hardware gateway.",
				ColumnDescriptions: map[string]string{
					"id":                   "The unique ID of the gateway",
					"device_id":            "The id of the device associated with the gateway",
					"created_at":           "Timestamp when the gateway was created.",
					"updated_at":           "Timestamp When the gateway was last updated.",
					"enrolled_at":          "Timestamp when the gateway was enrolled.",
					"enrolled":             "Whether the gateway is enrolled.",
					"hashed_hubserver_key": "The hashed key used to authenticate to hubserver.",
					"product_id":           "The product type of the gateway.",
					"serial":               "The serial number of the gateway.",
					"org_id":               "The organization id that the gateway is assigned to.",
					"group_id":             "The group id that the gateway is assigned to.",
					"variant_id":           "The product variant of the gateway.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"id_cards": {
				primaryKeys:                       []string{"id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"monitor_widget_history": {
				primaryKeys:       []string{"monitor_id", "updated_at"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains the history of monitor and widget associations",
				ColumnDescriptions: map[string]string{
					"monitor_id": "The monitor id associated with the monitor.",
					"widget_id":  "The widget id associated with the monitor.",
					"updated_at": "The timestamp at which the monitor and widget were linked.",
				},
			},
			"monitors": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains information on monitors, representing the physical hardware associated with a sensor.",
				ColumnDescriptions: map[string]string{
					"id":     "The unique ID for a montior.",
					"org_id": metadatahelpers.OrgIdDefaultDescription,
					"widget_id": "The widget id associated with the monitor. While the monitor represents the physical hardware, " +
						"the widget represents the customer data associated with the sensor.",
					"serial":                 "The serial of the monitor.",
					"product_id":             "The product id of the monitor.",
					"order_id":               "The order id of the monitor.",
					"order_uuid":             "The order uuid of the monitor.",
					"region_id":              "The region id associated with the monitor. Used to determine regions for a given monitor.",
					"region_id_to_propagate": "Region id that should be propagated to other regions.",
					"queued_propagate_at":    "Timestamp a monitor was queued to be propagated.",
					"propagate_upsert":       "Column used for region synchronization, along with the two above.",
					"created_at":             "The timestamp of when the monitor was created",
					"updated_at":             "The timestamp of when the monitor was last updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"product_oui_batches": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table tracks product batches, which are used to mass provision serial numbers/ids for newly created products.",
				ColumnDescriptions: map[string]string{
					"product_id":  "The enum ID of the product (ProductCm32, ProductVg34, etc).",
					"start":       "The id of the first product in the batch.",
					"count":       "The number of products in the batch.",
					"created_at":  "When the batch was created.",
					"created_by":  "The user id that created the batch.",
					"description": "A description for the batch.",
					"notes":       "Any additional notes for the batch.",
					"variant_id":  "The enum variant type of the product (i.e. VariantVg34M)",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_devices": {
				primaryKeys:                       []string{"device_id", "tag_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tag_widgets": {
				primaryKeys:                       []string{"widget_id", "tag_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"widgets": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "All sensor devices produced by Samsara (activated by a client or not).",
				ColumnDescriptions: map[string]string{
					"id":               "The internal unique identifier (int) of the sensor device.",
					"name":             "The name of the sensor device (default=Device serial number).",
					"product_id":       "The unique identifier of the type of sensor (product).",
					"serial":           "The serial number of the sensor device.",
					"pinned_device_id": "The device id of the asset the sensor device is installed on.",
				},
				Tags:                              []amundsentags.Tag{amundsentags.AssetsTag},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-supplychain-ops":          {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.RecognitionDB,
		schemaPath: "go/src/samsaradev.io/safety/safetyvision/facial_recognition/recognitionmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"autoassignment_groups": {
				primaryKeys:       []string{"org_id", "uuid"},
				Description:       "Metadata and settings of the auto assignment groups for a given org.",
				PartitionStrategy: SinglePartition,
			},
			"autoassignment_groups_tags": {
				primaryKeys:       []string{"org_id", "autoassignment_group_uuid", "tag_id"},
				Description:       "One-to-many mapping of auto assignment groups to tags for a given org.",
				PartitionStrategy: SinglePartition,
			},
			"camera_id_org_metadata": {
				primaryKeys:       []string{"org_id"},
				Description:       "This table includes usage data of Camera ID Feature for a given org.",
				PartitionStrategy: SinglePartition,
			},
			"dark_launch_faces": {
				primaryKeys: []string{"org_id", "device_id", "asset_ms", "experiment_tag"},
				ProtoSchema: ProtobufSchema{
					"face_detail_proto": {
						Message:   &recognitionhelpersproto.DarkLaunchFaceDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyvision/facial_recognition/recognitionhelpersproto/shared_types.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("asset_ms"),
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"face_assignment_suggestions": {
				primaryKeys:       []string{"org_id", "asset_ms", "device_id", "match_asset_ms", "match_device_id"},
				Description:       "This table includes all the suggested driver face matches for a given org that are pending manual confirmation by the customers.",
				PartitionStrategy: DateColumnFromMilliseconds("asset_ms"),
			},
			"face_similarities": {
				primaryKeys:       []string{"org_id", "rekognition_id", "match_rekognition_id"},
				Description:       "The similarity score (0-1) between a face vector and an existing face vector stored in AWS Rekognition Whether a feature is for a given org.",
				PartitionStrategy: SinglePartition,
			},
			"faces": {
				primaryKeys: []string{"org_id", "device_id", "asset_ms"},
				ProtoSchema: ProtobufSchema{
					"face_detail_proto": {
						Message:   &recognitionhelpersproto.RekognitionFaceDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyvision/facial_recognition/recognitionhelpersproto/shared_types.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("asset_ms"),
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"identities": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.ReleaseManagementDB,
		schemaPath: "go/src/samsaradev.io/infra/releasemanagement/releasemanagementmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"categories": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "Feature categories for organizing and filtering features in the release management system.",
				ColumnDescriptions: map[string]string{
					"org_id":      "The organization that this category belongs to (just a placeholder for sharding, always 1).",
					"id":          "Auto-incrementing primary key for the category.",
					"name":        "Unique name of the category (e.g., 'Routing', 'Safety', 'Compliance').",
					"description": "Optional description of what this category represents.",
					"created_at":  "Timestamp when the category was created.",
					"updated_at":  "Timestamp when the category was last updated.",
				},
			},
			"feature_categories": {
				primaryKeys:       []string{"org_id", "feature_package_uuid", "category_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Many-to-many relationship between features and categories.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The organization that this relationship belongs to (just a placeholder for sharding, always 1).",
					"feature_package_uuid": "UUID of the feature package this category applies to.",
					"category_id":          "ID of the category this feature belongs to.",
					"created_at":           "Timestamp when this feature-category relationship was created.",
				},
			},
			"feature_package_self_serve": {
				primaryKeys:                       []string{"org_id", "user_id", "scope", "feature_package_uuid"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "Whether a feature is enabled for a given org or user.",
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"feature_packages": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "A LaunchDarkly feature flag that can be released to and self-served by orgs and users.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"flag_metadata": {
				primaryKeys:       []string{"org_id", "flag_key"},
				PartitionStrategy: SinglePartition,
				Description:       "Metadata around the current state of a LaunchDarkly flag.",
				ColumnDescriptions: map[string]string{
					"flag_key":   "The key of the flag in LaunchDarkly.",
					"created_by": "The user_id of who created the flag, or the most first known maintainer for flags created before Nov 2022.",
					"org_id":     "Dummy org_id required for sharding.",
					"created_at": "When this entry was created in this table. Not when the flag itself was created. It should be close to when the flag was created for flags created after Nov 2022.",
					"updated_at": "When the flag was most recently updated.",
					"maintainer": "The user_id of the current maintainer of the flag.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"gateway_feature_flag_states": {
				primaryKeys:       []string{"org_id", "gateway_id"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"enabled_feature_flag_values": {
						Message:   &releasemanagementmodels.FeatureFlagValues{},
						ProtoPath: "go/src/samsaradev.io/infra/releasemanagement/releasemanagementmodels/feature_flag_values.proto",
					},
				},
				Description:                       "The most recent set of enabled firmware FFs a given gateway.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"quarterly_release_self_serve": {
				primaryKeys:                       []string{"org_id", "user_id", "scope", "quarterly_release_uuid"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "Whether a batched release is enabled for a given org or user.",
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"quarterly_releases": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "A set of LaunchDarkly feature flags that can be released to and self-served by orgs and users.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.RemoteSupportDB,
		schemaPath: "go/src/samsaradev.io/mobile/remotesupport/remotesupportmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"channel_messages": {
				primaryKeys:       []string{"org_id", "channel_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Messages used for logging and signaling during Remote Support sessions.",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"channel_uuid":       "Channel uuid",
					"uuid":               "Message uuid",
					"message_type":       metadatahelpers.EnumDescription("Type of the message: ", remotesupportconsts.RemoteSupportMessageTypeLabels),
					"source":             metadatahelpers.EnumDescription("Source of the message: ", remotesupportconsts.RemoteSupportMessageSourceLabels),
					"message":            "Content of the message",
					"created_at":         "Timestamp of message creation",
					"created_by_user_id": "ID of the admin that started the Remote Support session (if the message was sent from the dashboard), or 0 otherwise",
					"updated_at":         "Timestamp of the last update to the message record",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"channels": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Remote Support channels.",
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"uuid":                  "Channel uuid",
					"pin":                   "6-digit PIN used by Remote Support to auth to the backend",
					"status":                metadatahelpers.EnumDescription("Channel status: ", remotesupportproto.RemoteSupportChannelStatus_name),
					"created_by_user_id":    "ID of the admin that started the Remote Support session",
					"created_by_user_email": "Email of the admin that started the Remote Support session",
					"emm_device_id":         "ID of the enrolled MEM device that the admin created the session for",
					"driver_id":             "ID of the driver signed into the Driver app on the enrolled device, if present, when the device authenticated with a PIN",
					"created_at":            "Timestamp of channel creation",
					"ended_at":              "Timestamp when the channel transitioned to a terminal status (CLOSED, TIMEOUT)",
					"updated_at":            "Timestamp of the last update to the channel record",
					"terminated_by_source":  metadatahelpers.EnumDescription("Source of the termination of the channel: ", remotesupportconsts.RemoteSupportChannelTerminatedBySourceLabels),
					"ice_servers":           "List of available ice servers for WebRTC",
					"channel_type":          "The type of the Remote Support session - currently Attended or Unattended",
				},
				Tags: []amundsentags.Tag{
					amundsentags.ConnectedAppsTag,
					amundsentags.MemTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.ReportConfigDB,
		schemaPath: "go/src/samsaradev.io/reports/reportconfigmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"custom_report_configs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"report_definition": {
						Message:   &reportconfigproto.CustomReportDefinition{},
						ProtoPath: "go/src/samsaradev.io/reports/proto/reportconfigproto/custom_report.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"report_runs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "This table stores custom report runs.",
				ProtoSchema: ProtobufSchema{
					"report_run_metadata": {
						Message:             &reportconfigproto.ReportRunMetadata{},
						ExcludeProtoJsonTag: []string{"run_options_tag_filters", "report_config_tag_filters", "time_range_filter,omitempty"},
						ProtoPath:           "go/src/samsaradev.io/reports/proto/reportconfigproto/report_run.proto",
					},
				},
			},
			"scheduled_report_plans": {
				primaryKeys: []string{"org_id", "id"},
				ProtoSchema: ProtobufSchema{
					"marshaled_proto": {
						Message:   &reportconfigproto.ScheduledReportPlan{},
						ProtoPath: "go/src/samsaradev.io/reports/proto/reportconfigproto/report_config.proto",
					},
					"extra_args": {
						Message:   &reportconfigproto.ScheduledReportExtraArgs{},
						ProtoPath: "go/src/samsaradev.io/reports/proto/reportconfigproto/report_config.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"scheduled_runs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				Description:       "Stores scheduled report run records with their execution status and timing information.",
			},
			"year_in_review": {
				primaryKeys:       []string{"org_id", "year", "created_at_ms"},
				PartitionStrategy: SinglePartition,
				Description:       "Year in review summary data for organizations including safety scores, fuel efficiency, and top drivers.",
			},
		},
	},
	{
		name:       dbregistry.RetentionDB,
		schemaPath: "go/src/samsaradev.io/infra/retention/retentionmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"retention_config": {
				primaryKeys:                       []string{"org_id", "data_type"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"retention_state": {
				Description:                       "This table stores the current progress of data retention for a given data_type and system_type.",
				primaryKeys:                       []string{"org_id", "data_type", "system_type"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.RewardsDB,
		schemaPath: "go/src/samsaradev.io/safety/rewards/rewardsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"rewards": {
				Description:       "Stores individual rewards for the rewards service",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"uuid":          "Unique identifier for the reward",
					"recipient_id":  "The id of the recipient of the reward",
					"sender_id":     "The id of the sender of the reward",
					"type":          "The type of the reward",
					"status":        "The status of the reward",
					"reason_type":   "The reason type for the reward",
					"reason_uuid":   "The uuid of a specific instance of the reason type for the reward, optional",
					"trigger_type":  "The type of the trigger for the reward",
					"trigger_uuid":  "The uuid of a specific instance of the trigger type for the reward, optional",
					"message":       "The message of the reward",
					"sort_value":    "The sort value of the reward",
					"reward_detail": "The detail of the reward as a JSON object",
					"created_at":    "The timestamp when the reward was created",
					"updated_at":    "The timestamp when the reward was last updated",
				},
			},
		},
	},
	{
		name:       dbregistry.RoutePlanningDB,
		schemaPath: "go/src/samsaradev.io/fleet/routingplatform/routeplanning/routeplanningmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"planner_capacities": {
				Description:       "Stores capacity restraints for route planners.",
				primaryKeys:       []string{"org_id", "planner_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier for the capacity",
					"planner_uuid":      "Reference to the parent planner",
					"created_at":        "Timestamp when the record was created",
					"created_by":        "User ID who created the record",
					"updated_by":        "User ID who last updated the record",
					"updated_at":        "Timestamp when the record was last updated",
					"server_deleted_at": "Timestamp when the record was soft deleted",
					"server_deleted_by": "User ID who soft deleted the record",
					"name":              "Name of the capacity",
					"capacity_units":    "Number of capacity units",
					"csv_columns":       "CSV column mappings for capacity",
				},
			},
			"planner_equipment_profile_capacity_limits": {
				Description:       "Links equipment profiles to specific capacity constraints, defining maximum capacity values for different types of equipment.",
				primaryKeys:       []string{"org_id", "planner_equipment_profile_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                         metadatahelpers.OrgIdDefaultDescription,
					"uuid":                           "Unique identifier for the capacity limit",
					"planner_uuid":                   "Reference to the parent planner",
					"planner_equipment_profile_uuid": "Reference to the equipment profile",
					"planner_capacity_uuid":          "Reference to the capacity definition",
					"created_at":                     "Timestamp when the record was created",
					"created_by":                     "User ID who created the record",
					"updated_by":                     "User ID who last updated the record",
					"updated_at":                     "Timestamp when the record was last updated",
					"server_deleted_at":              "Timestamp when the record was soft deleted",
					"server_deleted_by":              "User ID who soft deleted the record",
					"capacity_limit":                 "Maximum capacity limit value",
				},
			},
			"planner_equipment_profiles": {
				Description:       "Contains equipment profile configurations including vehicle specifications, restrictions (like road type limitations), and capabilities that affect routing decisions.",
				primaryKeys:       []string{"org_id", "planner_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"protos": {
						Message:   &routeplanningproto.EquipmentProfileProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/routingplatform/routeplanning/routeplanningproto/equipment_profile.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"uuid":                            "Unique identifier for the equipment profile",
					"planner_uuid":                    "Reference to the parent planner",
					"created_at":                      "Timestamp when the record was created",
					"created_by":                      "User ID who created the record",
					"updated_by":                      "User ID who last updated the record",
					"updated_at":                      "Timestamp when the record was last updated",
					"server_deleted_at":               "Timestamp when the record was soft deleted",
					"server_deleted_by":               "User ID who soft deleted the record",
					"name":                            "Name of the equipment profile",
					"time_of_day_start_seconds":       "Start time of day in seconds from midnight",
					"additional_service_time_enabled": "Whether additional service time is enabled",
					"additional_service_time_seconds": "Additional service time in seconds",
					"restriction_commercial_roads_only_enabled":         "Whether commercial roads only restriction is enabled",
					"restriction_vehicle_weight_enabled":                "Whether vehicle weight restriction is enabled",
					"restriction_vehicle_weight_kilograms":              "Maximum vehicle weight in kilograms",
					"restriction_vehicle_height_enabled":                "Whether vehicle height restriction is enabled",
					"restriction_vehicle_height_meters":                 "Maximum vehicle height in meters",
					"restriction_vehicle_width_enabled":                 "Whether vehicle width restriction is enabled",
					"restriction_vehicle_width_meters":                  "Maximum vehicle width in meters",
					"restriction_vehicle_length_enabled":                "Whether vehicle length restriction is enabled",
					"restriction_vehicle_length_meters":                 "Maximum vehicle length in meters",
					"restriction_speed_cap_enabled":                     "Whether speed cap restriction is enabled",
					"restriction_speed_cap_kmph":                        "Maximum speed in kilometers per hour",
					"restriction_speed_factor_enabled":                  "Whether speed factor restriction is enabled",
					"restriction_speed_factor":                          "Speed factor multiplier",
					"restriction_max_shift_time_enabled":                "Whether maximum shift time restriction is enabled",
					"restriction_max_shift_time_seconds":                "Maximum shift time in seconds",
					"restriction_min_stop_count_enabled":                "Whether minimum stop count restriction is enabled",
					"restriction_min_stop_count":                        "Minimum number of stops required",
					"restriction_max_stop_count_enabled":                "Whether maximum stop count restriction is enabled",
					"restriction_max_stop_count":                        "Maximum number of stops allowed",
					"restriction_max_distance_enabled":                  "Whether maximum distance restriction is enabled",
					"restriction_max_distance_meters":                   "Maximum distance in meters",
					"restriction_min_distance_enabled":                  "Whether minimum distance restriction is enabled",
					"restriction_min_distance_meters":                   "Minimum distance in meters",
					"capacity0":                                         "Capacity value for dimension 0",
					"capacity1":                                         "Capacity value for dimension 1",
					"capacity2":                                         "Capacity value for dimension 2",
					"capacity3":                                         "Capacity value for dimension 3",
					"capacity4":                                         "Capacity value for dimension 4",
					"restriction_no_uturns_enabled":                     "Whether U-turns are prohibited",
					"restriction_no_difficult_turns_enabled":            "Whether difficult turns are prohibited",
					"restriction_no_toll_roads_enabled":                 "Whether toll roads are prohibited",
					"restriction_no_ferries_enabled":                    "Whether ferries are prohibited",
					"restriction_no_highways_enabled":                   "Whether highways are prohibited",
					"restriction_no_controlled_access_highways_enabled": "Whether controlled access highways are prohibited",
					"cost_fixed":                                        "Fixed cost component",
					"cost_per_meter":                                    "Cost per meter traveled",
					"cost_per_second":                                   "Cost per second of travel time",
				},
			},
			"planner_locations": {
				Description:       "Stores location data for route planning, including depots, delivery points, and other significant locations with their coordinates and attributes.",
				primaryKeys:       []string{"org_id", "planner_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"protos": {
						Message:   &routeplanningproto.PlannerLocationProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/routingplatform/routeplanning/routeplanningproto/planner_location.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"uuid":                            "Unique identifier for the location",
					"planner_uuid":                    "Reference to the parent planner",
					"created_at":                      "Timestamp when the record was created",
					"created_by":                      "User ID who created the record",
					"updated_by":                      "User ID who last updated the record",
					"updated_at":                      "Timestamp when the record was last updated",
					"server_deleted_at":               "Timestamp when the record was soft deleted",
					"server_deleted_by":               "User ID who soft deleted the record",
					"name":                            "Name of the location",
					"address":                         "Physical address of the location",
					"planner_notes":                   "Additional notes about the location",
					"standard_instructions":           "Standard instructions for this location",
					"lat":                             "Latitude coordinate",
					"lng":                             "Longitude coordinate",
					"is_depot":                        "Whether this location is a depot",
					"additional_service_time_enabled": "Whether additional service time is enabled",
					"additional_service_time_seconds": "Additional service time in seconds",
					"depot_shift_start_time_seconds":  "Depot shift start time in seconds from midnight",
					"depot_shift_end_time_seconds":    "Depot shift end time in seconds from midnight",
					"external_id":                     "External identifier for the location",
				},
			},
			"planner_session_order_quantities": {
				Description:       "Tracks quantity-related information for orders within a planning session.",
				primaryKeys:       []string{"org_id", "planner_session_order_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                     metadatahelpers.OrgIdDefaultDescription,
					"uuid":                       "Unique identifier for the order quantity",
					"planner_uuid":               "Reference to the parent planner",
					"planner_session_order_uuid": "Reference to the order",
					"planner_capacity_uuid":      "Reference to the capacity definition",
					"created_at":                 "Timestamp when the record was created",
					"created_by":                 "User ID who created the record",
					"updated_by":                 "User ID who last updated the record",
					"updated_at":                 "Timestamp when the record was last updated",
					"server_deleted_at":          "Timestamp when the record was soft deleted",
					"server_deleted_by":          "User ID who soft deleted the record",
					"quantity":                   "Quantity value for this capacity dimension",
					"planner_session_uuid":       "Reference to the parent session",
				},
			},
			"planner_session_orders": {
				Description:       "Maintains order information within planning sessions, tracking delivery/pickup requests that need to be routed.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"protos": {
						Message:   &routeplanningproto.PlannerSessionOrderProto{},
						ProtoPath: "go/src/samsaradev.io/fleet/routingplatform/routeplanning/routeplanningproto/planner_session_order.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"uuid":                            "Unique identifier for the order",
					"planner_uuid":                    "Reference to the parent planner",
					"planner_session_uuid":            "Reference to the parent session",
					"planner_session_route_uuid":      "Reference to the assigned route",
					"planner_session_route_stop_uuid": "Reference to the route stop",
					"created_at":                      "Timestamp when the record was created",
					"created_by":                      "User ID who created the record",
					"updated_by":                      "User ID who last updated the record",
					"updated_at":                      "Timestamp when the record was last updated",
					"server_deleted_at":               "Timestamp when the record was soft deleted",
					"server_deleted_by":               "User ID who soft deleted the record",
					"id":                              "External identifier for the order",
					"special_instructions":            "Special instructions for this order",
					"service_window_start":            "Start of the service time window",
					"service_window_end":              "End of the service time window",
					"additional_service_time_seconds": "Additional service time in seconds",
					"address":                         "Address for the order",
					"lat":                             "Latitude coordinate",
					"lng":                             "Longitude coordinate",
					"planner_location_uuid":           "Reference to a known location if applicable",
					"location_external_id":            "External identifier for the location",
					"error_messages_json":             "Error messages for the order",
				},
			},
			"planner_session_route_stops": {
				Description:       "Defines individual stops within a route, including sequence, timing, and service details for each location visit.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                     metadatahelpers.OrgIdDefaultDescription,
					"uuid":                       "Unique identifier for the stop",
					"planner_uuid":               "Reference to the parent planner",
					"planner_session_uuid":       "Reference to the parent session",
					"planner_session_route_uuid": "Reference to the parent route",
					"created_at":                 "Timestamp when the record was created",
					"created_by":                 "User ID who created the record",
					"updated_by":                 "User ID who last updated the record",
					"updated_at":                 "Timestamp when the record was last updated",
					"server_deleted_at":          "Timestamp when the record was soft deleted",
					"server_deleted_by":          "User ID who soft deleted the record",
					"stop_order":                 "Order of this stop in the route",
					"name":                       "Name of the stop",
					"description":                "Description of the stop",
					"address":                    "Address of the stop",
					"lat":                        "Latitude coordinate",
					"lng":                        "Longitude coordinate",
					"arrival_time":               "Estimated arrival time",
					"departure_time":             "Estimated departure time",
					"service_time_seconds":       "Service time at this stop in seconds",
					"planner_location_uuid":      "Reference to the location if this is a known location",
					"stop_type":                  metadatahelpers.EnumDescription("Type of stop", routeplanningproto.RouteStopType_name),
				},
			},
			"planner_session_routes": {
				Description:       "Stores generated routes for a planning session.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                            metadatahelpers.OrgIdDefaultDescription,
					"uuid":                              "Unique identifier for the route",
					"planner_uuid":                      "Reference to the parent planner",
					"planner_session_uuid":              "Reference to the parent session",
					"route_template_uuid":               "Reference to the route template if any",
					"created_at":                        "Timestamp when the record was created",
					"created_by":                        "User ID who created the record",
					"updated_by":                        "User ID who last updated the record",
					"updated_at":                        "Timestamp when the record was last updated",
					"server_deleted_at":                 "Timestamp when the record was soft deleted",
					"server_deleted_by":                 "User ID who soft deleted the record",
					"name":                              "Name of the route",
					"distance_meters":                   "Total distance of route in meters",
					"duration_seconds":                  "Total duration of route in seconds",
					"route_construction_uuid":           "Reference to the route construction",
					"planner_equipment_profile_uuid":    "Reference to the equipment profile",
					"cost":                              "Total cost of the route",
					"dispatch_route_id":                 "ID of the dispatched route if exported",
					"edited":                            "Whether the route has been manually edited",
					"planner_strategic_route_uuid":      "Reference to the strategic route if any",
					"planner_territory_uuid":            "Reference to the territory if any",
					"depot_start_planner_location_uuid": "Reference to the starting depot location for this route",
					"depot_end_planner_location_uuid":   "Reference to the ending depot location for this route",
					"shift_start_time_override":         "Override for the shift start time",
				},
			},
			"planner_sessions": {
				Description:       "Stores planning session metadata and configurations, representing distinct route planning activities or scenarios.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                             metadatahelpers.OrgIdDefaultDescription,
					"uuid":                               "Unique identifier for the session",
					"planner_uuid":                       "Reference to the parent planner",
					"created_at":                         "Timestamp when the record was created",
					"created_by":                         "User ID who created the record",
					"updated_by":                         "User ID who last updated the record",
					"updated_at":                         "Timestamp when the record was last updated",
					"server_deleted_at":                  "Timestamp when the record was soft deleted",
					"server_deleted_by":                  "User ID who soft deleted the record",
					"name":                               "Name of the session",
					"optimization_goal":                  metadatahelpers.EnumDescription("Goal for the optimization", routeplanningproto.OptimizationGoal_name),
					"order_time_windows_enabled":         "Whether time windows are enabled for orders",
					"shift_start_time":                   "Start time for the planning shift",
					"max_shift_duration_seconds":         "Maximum duration of a shift in seconds",
					"default_order_service_time_seconds": "Default service time for orders in seconds",
					"default_order_time_window_start":    "Default start of time window in seconds from shift start",
					"default_order_time_window_end":      "Default end of time window in seconds from shift start",
					"optimization_time_limit_seconds":    "Time limit for optimization in seconds",
					"optimization_stagnation_time_limit_seconds": "Time limit for optimization stagnation in seconds",
					"last_session_optimization_uuid":             "Reference to the last optimization run",
					"status":                                     metadatahelpers.EnumDescription("Current status of the session", routeplanningproto.PlannerSessionStatus_name),
					"locked_status":                              metadatahelpers.EnumDescription("Lock status of the session", routeplanningproto.PlannerSessionLockedStatus_name),
					"locked_status_updated_at":                   "Timestamp when lock status was last updated",
					"locked_status_updated_by":                   "User ID who last updated lock status",
					"export_to_dispatch_status":                  metadatahelpers.EnumDescription("Status of dispatch export", routeplanningproto.PlannerSessionExportToDispatchStatus_name),
					"export_to_dispatch_status_updated_at":       "Timestamp when dispatch export status was last updated",
					"export_to_dispatch_status_updated_by":       "User ID who last updated dispatch export status",
					"is_planner_default_session":                 "Flag indicating if this is the default session for the planner",
				},
			},
			"planner_skills": {
				Description:       "Defines skills or qualifications that can be required for specific routes or locations",
				primaryKeys:       []string{"org_id", "planner_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier for the skill",
					"planner_uuid":      "Reference to the parent planner",
					"created_at":        "Timestamp when the record was created",
					"created_by":        "User ID who created the record",
					"updated_by":        "User ID who last updated the record",
					"updated_at":        "Timestamp when the record was last updated",
					"server_deleted_at": "Timestamp when the record was soft deleted",
					"server_deleted_by": "User ID who soft deleted the record",
					"name":              "Name of the skill",
					"csv_columns":       "CSV column mappings for skill requirements",
				},
			},
			"planner_strategic_routes": {
				Description:       "Stores strategic routes that are predefined or pre-optimized for specific planning scenarios.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier for the strategic route",
					"planner_uuid":      "Reference to the parent planner",
					"name":              "Name of the strategic route",
					"created_at":        "Timestamp when the record was created",
					"created_by":        "User ID who created the record",
					"updated_by":        "User ID who last updated the record",
					"updated_at":        "Timestamp when the record was last updated",
					"server_deleted_at": "Timestamp when the record was soft deleted",
					"server_deleted_by": "User ID who soft deleted the record",
					"configs":           "Configuration data for the strategic route",
				},
			},
			"planner_territories": {
				Description:       "Defines geographic areas or regions that are used for route planning.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier for the territory",
					"planner_uuid":      "Reference to the parent planner",
					"name":              "Name of the territory",
					"polygon":           "Geographic polygon defining the territory",
					"latitude":          "Center latitude of the territory",
					"longitude":         "Center longitude of the territory",
					"radius":            "Radius of the territory in meters",
					"created_at":        "Timestamp when the record was created",
					"created_by":        "User ID who created the record",
					"updated_by":        "User ID who last updated the record",
					"updated_at":        "Timestamp when the record was last updated",
					"server_deleted_at": "Timestamp when the record was soft deleted",
					"server_deleted_by": "User ID who soft deleted the record",
				},
			},
			"planners": {
				Description:       "Stores route planner configurations and settings.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                             metadatahelpers.OrgIdDefaultDescription,
					"uuid":                               "Unique identifier for the planner",
					"created_at":                         "Timestamp when the record was created",
					"created_by":                         "User ID who created the record",
					"updated_by":                         "User ID who last updated the record",
					"updated_at":                         "Timestamp when the record was last updated",
					"server_deleted_at":                  "Timestamp when the record was soft deleted",
					"server_deleted_by":                  "User ID who soft deleted the record",
					"name":                               "Name of the planner",
					"description":                        "Description of the planner",
					"capacity_label0":                    "Label for capacity dimension 0",
					"capacity_label1":                    "Label for capacity dimension 1",
					"capacity_label2":                    "Label for capacity dimension 2",
					"capacity_label3":                    "Label for capacity dimension 3",
					"capacity_label4":                    "Label for capacity dimension 4",
					"timezone":                           "Timezone for the planner",
					"enable_order_location_gps_matching": "Whether GPS location matching is enabled for orders",
					"order_location_gps_matching_radius_meters": "Radius in meters for GPS location matching",
					"require_order_location":                    "Whether orders must have a location specified",
				},
			},
			"route_constructions": {
				Description:       "Stores rules and parameters for how routes should be built, including constraints and optimization preferences.",
				primaryKeys:       []string{"org_id", "planner_session_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				DisableServerless: true,
				versionInfo:       SingleVersion(2),
				ColumnDescriptions: map[string]string{
					"org_id":                            metadatahelpers.OrgIdDefaultDescription,
					"uuid":                              "Unique identifier for the route construction",
					"planner_uuid":                      "Reference to the parent planner",
					"planner_session_uuid":              "Reference to the parent session",
					"planner_equipment_profile_uuid":    "Reference to the equipment profile",
					"depot_start_planner_location_uuid": "Reference to the starting depot location",
					"depot_end_planner_location_uuid":   "Reference to the ending depot location",
					"created_at":                        "Timestamp when the record was created",
					"created_by":                        "User ID who created the record",
					"updated_by":                        "User ID who last updated the record",
					"updated_at":                        "Timestamp when the record was last updated",
					"server_deleted_at":                 "Timestamp when the record was soft deleted",
					"server_deleted_by":                 "User ID who soft deleted the record",
					"count":                             "Number of routes to construct",
					"additional_configs":                "Additional configuration data",
					"optimization_mode":                 metadatahelpers.EnumDescription("Mode of optimization", routeplanningproto.RouteConstructionMode_name),
					"planner_territory_uuid":            "Reference to the territory if any",
					"planner_strategic_route_uuid":      "Reference to the strategic route if any",
				},
			},
			"route_templates": {
				Description:       "Route templates are a predefined collection of locations and metadata used to create routes in plans.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier for the route template",
					"planner_uuid":      "Reference to the parent planner",
					"name":              "Name of the route template",
					"created_at":        "Timestamp when the record was created",
					"created_by":        "User ID who created the record",
					"updated_by":        "User ID who last updated the record",
					"updated_at":        "Timestamp when the record was last updated",
					"server_deleted_at": "Timestamp when the record was soft deleted",
					"server_deleted_by": "User ID who soft deleted the record",
				},
			},
			"route_templates_locations": {
				Description:       "A relation table enacting the 1:many relationship between route templates and Locations.",
				primaryKeys:       []string{"org_id", "route_template_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"uuid":                "Unique identifier for the location in the route template",
					"org_id":              metadatahelpers.OrgIdDefaultDescription,
					"route_template_uuid": "Reference to the parent route template",
					"location_uuid":       "Reference to the parent location",
					"position":            "Position of the location in the route template",
				},
			},
			"session_optimizations": {
				Description:       "Tracks optimization runs within planning sessions, including status and results.",
				primaryKeys:       []string{"org_id", "planner_session_uuid", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"status_history_proto": {
						Message:   &routeplanningproto.SessionOptimizationStatusHistory{},
						ProtoPath: "go/src/samsaradev.io/fleet/routingplatform/routeplanning/routeplanningproto/route_planning_worker.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"org_id":               metadatahelpers.OrgIdDefaultDescription,
					"uuid":                 "Unique identifier for the optimization",
					"planner_uuid":         "Reference to the parent planner",
					"planner_session_uuid": "Reference to the parent session",
					"created_at":           "Timestamp when the record was created",
					"created_by":           "User ID who created the record",
					"updated_by":           "User ID who last updated the record",
					"updated_at":           "Timestamp when the record was last updated",
					"server_deleted_at":    "Timestamp when the record was soft deleted",
					"server_deleted_by":    "User ID who soft deleted the record",
					"completed_at":         "When the optimization completed",
					"cost":                 "Final cost achieved by optimization",
					"status":               metadatahelpers.EnumDescription("Current status of optimization", routeplanningproto.SessionOptimizationStatus_name),
					"error_message":        "Error message if optimization failed",
					"raw_error_message":    "Unsanitized error message if optimization failed",
					"async_job_id":         "ID of the asynchronous optimization job",
				},
			},
			"tag_planners": {
				Description:       "Associates tags with planners, allowing for grouping and filtering of planners based on tags.",
				primaryKeys:       []string{"org_id", "planner_uuid", "tag_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":       metadatahelpers.OrgIdDefaultDescription,
					"tag_id":       "ID of the tag",
					"planner_uuid": "Reference to the planner being tagged",
					"created_at":   "Timestamp when the record was created",
					"created_by":   "User ID who created the record",
					"updated_by":   "User ID who last updated the record",
					"updated_at":   "Timestamp when the record was last updated",
				},
			},
		},
	},
	{
		name:       dbregistry.SafetyDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetymodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// Safetydb consistently receives traffic across all shards in both regions.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
			infraconsts.SamsaraAWSEURegion:      euShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"activity_events": {
				primaryKeys: []string{"org_id", "device_id", "event_ms", "created_at", "activity_type"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &safetyproto.SafetyActivityDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/safetyactivity.proto",
					},
				},
				DataModelTable:                    true,
				PartitionStrategy:                 DateColumnFromMilliseconds("event_ms"),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"device_safety_settings": {
				Description:    "Stores device-specific safety speeding configuration settings.",
				primaryKeys:    []string{"org_id", "device_id", "created_at_ms"},
				DataModelTable: true,
				ProtoSchema: ProtobufSchema{
					"settings": {
						Message:   &safetyproto.DeviceSafetySettingsHistory{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/device_settings.proto",
					},
				},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"device_id":     metadatahelpers.DeviceIdDefaultDescription,
					"created_at_ms": "Unix timestamp (in milliseconds) when the device safety speeding setting was created",
					"created_by":    "User ID who created the device safety speeding setting",
					"settings":      "Proto blob containing device-specific safety speeding configuration settings",
				},
			},
			"device_settings": {
				Description: "Device settings stores settings for our safety features such as Distracted Driving Detection.",
				primaryKeys: []string{"org_id", "device_id"},
				ProtoSchema: ProtobufSchema{
					"device_setting": {
						Message:   &safetyproto.DeviceSetting{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/device_settings.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"device_id":      metadatahelpers.DeviceIdDefaultDescription,
					"device_setting": "Device safety settings proto blob",
					"created_at":     "Time when the device safety setting was created",
					"updated_at":     "Time when the device safety setting was updated",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_audio_alerts": {
				Description:       "Stores in-cab audio alerts as driver_audio_alerts before they reach a critical threshold to be promoted into a safety event",
				primaryKeys:       []string{"org_id", "device_id", "event_ms", "accel_type"},
				PartitionStrategy: DateColumnFromMilliseconds("event_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"device_id":                metadatahelpers.DeviceIdDefaultDescription,
					"event_ms":                 "Unixtime (in milliseconds) that the event was triggered",
					"accel_type":               "harsh accel type of in-cab audio alert event",
					"alerted_at_ms":            "Unixtime (in milliseconds) that the in-cab audio alert was triggered",
					"driver_id":                "driver Id assigned to in-cab audio alert",
					"trip_start_ms":            "Unixtime (in milliseconds) that the trip associated with in-cab audio alert started",
					"asset_ms":                 "Unixtime (in milliseconds) that the video associated with event starts",
					"promoted_to_safety_event": "whether driver audio alert has been promoted to safety event already",
					"safety_event_id":          "unique event id of harsh event associated with in-cab audio alert",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"excused_speeding_intervals": {
				Description:       "We automatically detect speeding events. We allow customers to excuse a time range (which may include several speeding events), which prevents the speeding from impacting driver safety score. Excusals are most often due to incorrect speeding detection.",
				primaryKeys:       []string{"org_id", "device_id", "start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"device_id":     metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":      "Unixtime (in milliseconds) that the excusal starts",
					"end_ms":        "Unixtime (in milliseconds) that the excusal ends",
					"created_at_ms": "Unixtime (in milliseconds) when dismissed interval was created",
					"updated_at_ms": "Unixtime (in milliseconds) when dismissed interval was updated or deleted",
					"is_deleted":    "Boolean value to indicate if dismissed interval is deleted",
					"trip_start_ms": "Unixtime (in milliseconds) start time of the trip that the dismissed interval occurred on",
				},
			},
			"harsh_event_surveys": {
				primaryKeys: []string{"org_id", "device_id", "event_ms", "user_id", "created_at"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &safetyproto.HarshEventSurveyDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/harsheventsurvey.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("created_at"),
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				DataModelTable:                    true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"harsh_events_v2_org_settings": {
				Description: "Org settings for detecting harsh driving events.",
				primaryKeys: []string{"org_id"},
				ProtoSchema: ProtobufSchema{
					"setting": {
						Message:   &safetyproto.HarshEventV2OrgSetting{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/harsh_event_v2_settings.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ColumnDescriptions: map[string]string{
					"org_id":                                metadatahelpers.OrgIdDefaultDescription,
					"setting":                               "Proto blob containing sensitivity settings.",
					"setting.harsh_accel_sensitivity":       "UNUSED. Was used for 'basic settings', which we dropped before entering beta.",
					"setting.harsh_brake_sensitivity":       "UNUSED. Was used for 'basic settings', which we dropped before entering beta.",
					"setting.harsh_turn_sensitivity":        "UNUSED. Was used for 'basic settings', which we dropped before entering beta.",
					"setting.advanced_settings":             "Contains detection settings for each kind of harsh driving event.",
					"setting.advanced_settings.harsh_accel": "Contains HarshEventV2SensitivityEnum for each vehicle type (0 invalid, 1 low, 2 normal, 3 high, 4 off, 5 very low)",
					"setting.advanced_settings.harsh_turn":  "Contains HarshEventV2SensitivityEnum for each vehicle type (0 invalid, 1 low, 2 normal, 3 high, 4 off, 5 very low)",
					"setting.advanced_settings.harsh_brake": "Contains HarshEventV2SensitivityEnum for each vehicle type (0 invalid, 1 low, 2 normal, 3 high, 4 off, 5 very low)",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"imu_orienter_metadata": {
				Description: "Metadata gathered for an accelerometer based harsh event after running backend orientation. This metadata is not associated with the harsh event v2 orienter.",
				primaryKeys: []string{"org_id", "event_ms", "device_id"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &safetyproto.IMUOrienterMetadataDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/harsheventclassifier.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("event_ms"),
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				ColumnDescriptions: map[string]string{
					"org_id":                       metadatahelpers.OrgIdDefaultDescription,
					"device_id":                    metadatahelpers.DeviceIdDefaultDescription,
					"event_ms":                     "Unixtime (in milliseconds) that the harsh event was triggered",
					"has_multiple_orientations":    "Boolean that indicates that multiple valid VG orientations were detected around the harsh event. Used to determine whether the VG potentially has a loose installation",
					"classifier_version":           "The version of the backend orienter that generated this metadata for the harsh event",
					"detail_proto":                 "Proto blob containing additional useful metadata.",
					"detail_proto.max_mag_accel_x": "The max G value detected on the X axis around the harsh event. Used to determine whether the event should be filtered out based on organization threshold settings",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"manual_driver_assignments": {
				Description:           "Manually-created assignments of a driver to a vehicle for a given time period. These are created by fleet admins using the Safety Inbox.",
				primaryKeys:           []string{"org_id", "device_id", "start_at_ms", "end_at_ms", "created_at"},
				PartitionStrategy:     DateColumnFromMilliseconds("start_at_ms"),
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"device_id":          "ID of the vehicle for this assignment",
					"driver_id":          "ID of the driver for this assignment",
					"start_at_ms":        "Start time (in milliseconds) of the assignment",
					"end_at_ms":          "End time (in milliseconds) of the assignment",
					"created_at":         "The time at which this assignment was created",
					"created_by_user_id": "ID of the fleet administrator that created this assignment",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_safety_speeding_settings": {
				Description:           "Audit log of changes to organization speeding settings.",
				primaryKeys:           []string{"org_id", "created_at_ms", "uuid"},
				PartitionStrategy:     DateColumnFromMilliseconds("created_at_ms"),
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ProtoSchema: ProtobufSchema{
					"speeding_settings_proto": {
						Message:   &orgproto.OrganizationSettings_OrganizationSafetySettings_SpeedingSettings{},
						ProtoPath: "go/src/samsaradev.io/settings/orgproto/organization_settings.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"uuid":                    "The unique identifier for this version of the speeding settings",
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"created_at_ms":           "Unix time ms at which the speeding settings were updated",
					"created_by":              "ID of the fleet administrator that created this assignment",
					"speeding_settings_proto": "A snapshot of the organization's speeding settings which were active at created_at_ms",
				},
				DataModelTable:                    true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_threshold_audits": {
				Description: "Audit log of changes to organization harsh event threshold settings.",
				primaryKeys: []string{"org_id", "created_at"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &safetyproto.OrgThresholdAuditDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/harsh_event_thresholds.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("created_at"),
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				ColumnDescriptions: map[string]string{
					"org_id":       metadatahelpers.OrgIdDefaultDescription,
					"created_at":   "Unixtime (in milliseconds) that the threshold settings were updated",
					"author_id":    "ID of the admin that updated the threshold settings",
					"detail_proto": "Proto blob containing the threshold settings that were applied for the org. The units for the fields are multipliers that are applied against the Samsara recommended G force thresholds for each event type (accel, brake, turn)",
					"detail_proto.org_threshold_scalars.harsh_accel_x_threshold_scalar":                              "The simple multiplier for harsh accel threshold settings",
					"detail_proto.org_threshold_scalars.harsh_brake_x_threshold_scalar":                              "The simple multiplier for harsh brake threshold settings",
					"detail_proto.org_threshold_scalars.harsh_turn_x_threshold_scalar":                               "The simple multiplier for harsh turn threshold settings",
					"detail_proto.org_threshold_scalars.use_advanced_scalars":                                        "Boolean that indicates when the organization is using the advanced (per vehicle type) harsh event threshold settings",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_accel_x_threshold_scalars.passenger":  "The advanced multiplier for harsh accel threshold settings for passenger vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_accel_x_threshold_scalars.light_duty": "The advanced multiplier for harsh accel threshold settings for light duty vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_accel_x_threshold_scalars.heavy_duty": "The advanced multiplier for harsh accel threshold settings for heavy duty vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_brake_x_threshold_scalars.passenger":  "The advanced multiplier for harsh brake threshold settings for passenger vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_brake_x_threshold_scalars.light_duty": "The advanced multiplier for harsh brake threshold settings for light duty vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_brake_x_threshold_scalars.heavy_duty": "The advanced multiplier for harsh brake threshold settings for heavy duty vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_turn_x_threshold_scalars.passenger":   "The advanced multiplier for harsh turn threshold settings for passenger vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_turn_x_threshold_scalars.light_duty":  "The advanced multiplier for harsh turn threshold settings for light duty vehicles",
					"detail_proto.org_threshold_scalars.advanced_scalars.harsh_turn_x_threshold_scalars.heavy_duty":  "The advanced multiplier for harsh turn threshold settings for heavy duty vehicles",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_threshold_scalars": {
				Description: "Harsh event threshold settings for the organization that determine the sensitivity of accels, brakes, and turns that are triggered on the VG and are surfaced in the safety product to the user. The units for the fields are multipliers that are applied against the Samsara recommended G force thresholds for each event type.",
				primaryKeys: []string{"org_id"},
				ProtoSchema: ProtobufSchema{
					"advanced_scalars": {
						Message:   &safetyproto.OrgThresholdAdvancedScalars{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/harsh_event_thresholds.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ColumnDescriptions: map[string]string{
					"org_id":                       metadatahelpers.OrgIdDefaultDescription,
					"harsh_accel_threshold_scalar": "The simple multiplier for harsh accel threshold settings",
					"harsh_brake_threshold_scalar": "The simple multiplier for harsh brake threshold settings",
					"harsh_turn_threshold_scalar":  "The simple multiplier for harsh turn threshold settings",
					"use_advanced_scalars":         "Boolean that indicates when the organization is using the advanced (per vehicle type) harsh event threshold settings",
					"advanced_scalars":             "Proto blob containing the advanced threshold settings that are used for the org. These are used when use_advanced_scalars is true",
					"advanced_scalars.harsh_accel_x_threshold_scalars.passenger":  "The advanced multiplier for harsh accel threshold settings for passenger vehicles",
					"advanced_scalars.harsh_accel_x_threshold_scalars.light_duty": "The advanced multiplier for harsh accel threshold settings for light duty vehicles",
					"advanced_scalars.harsh_accel_x_threshold_scalars.heavy_duty": "The advanced multiplier for harsh accel threshold settings for heavy duty vehicles",
					"advanced_scalars.harsh_brake_x_threshold_scalars.passenger":  "The advanced multiplier for harsh brake threshold settings for passenger vehicles",
					"advanced_scalars.harsh_brake_x_threshold_scalars.light_duty": "The advanced multiplier for harsh brake threshold settings for light duty vehicles",
					"advanced_scalars.harsh_brake_x_threshold_scalars.heavy_duty": "The advanced multiplier for harsh brake threshold settings for heavy duty vehicles",
					"advanced_scalars.harsh_turn_x_threshold_scalars.passenger":   "The advanced multiplier for harsh turn threshold settings for passenger vehicles",
					"advanced_scalars.harsh_turn_x_threshold_scalars.light_duty":  "The advanced multiplier for harsh turn threshold settings for light duty vehicles",
					"advanced_scalars.harsh_turn_x_threshold_scalars.heavy_duty":  "The advanced multiplier for harsh turn threshold settings for heavy duty vehicles",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"organization_settings": {
				Description: "Organization safety settings stores settings for our safety features such as Distracted Driving Detection and Following Distance Detection.",
				primaryKeys: []string{"org_id"},
				ProtoSchema: ProtobufSchema{
					"organization_setting": {
						Message:   &safetyproto.OrganizationSetting{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/organization_settings.proto",
					},
				},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ColumnDescriptions: map[string]string{
					"org_id":               metadatahelpers.OrgIdDefaultDescription,
					"organization_setting": "Organization safety settings proto blob",
					"created_at":           "Time when the organization safety setting was created",
					"updated_at":           "Time when the organization safety setting was updated",
				},
				DataModelTable:                    true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"qr_code_lookup": {
				Description:       "Maps a lookup_key UUID to a driver id, for use in QR Code driver assignment.",
				primaryKeys:       []string{"lookup_key"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"org_id":     metadatahelpers.OrgIdDefaultDescription,
					"driver_id":  metadatahelpers.DriverIdDefaultDescription,
					"lookup_key": "UUID stored in the QR code itself.",
					"created_at": "Time when the qr code lookup was created.",
					"updated_at": "Time when the qr code lookup was updated.",
					"status":     "Status of QR Code. Can be revoked (0) or active (1)",
				},
				Tags: []amundsentags.Tag{
					amundsentags.SafetyTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"qr_code_sent_metadata": {
				Description:       "Captures metadata about when QR codes were distributed to drivers.",
				primaryKeys:       []string{"org_id", "driver_id", "sent_method"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"driver_id":                       metadatahelpers.DriverIdDefaultDescription,
					"sent_method":                     "Method by which a QR code was shared (e.g., texting or printing).",
					"last_qr_code_sent_method_detail": "Additional details regarding how the QR code was sent.",
					"last_lookup_key":                 "Latest lookup key used when sharing the QR code.",
					"last_qr_code_sent_timestamp":     "When we last sent the QR code.",
					"created_at":                      "Time when the qr code lookup was created.",
					"updated_at":                      "Time when the qr code lookup was updated.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.SafetyTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"qrcode_pending_driver_assignments": {
				Description:       "Tracks open driver assignments.",
				primaryKeys:       []string{"org_id", "device_id", "driver_id", "assignment_start"},
				PartitionStrategy: DateColumnFromMilliseconds("assignment_start"),
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"device_id":             metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":             metadatahelpers.DriverIdDefaultDescription,
					"assignment_start":      "Time in milliseconds when the assignment period should begin.",
					"assignment_end":        "Time in milliseconds when the assignment period should end.",
					"assignment_status":     "Current status of this assignment record (e.g., PENDING or FINALIZED).",
					"assignment_end_reason": "Reason for the assignment ending (e.g., VEHICLE_REASSIGNED or DRIVER_REASSIGNED).",
					"last_poll_time":        "Time in milliseconds of the last poll cycle when this pending assignment record was checked.",
					"next_poll_time":        "Time in milliseconds when this pending assignment record should next be checked.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.SafetyTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_event_geofences": {
				primaryKeys: []string{"org_id", "address_id"},
				ProtoSchema: ProtobufSchema{
					"safety_event_settings_proto": {
						Message:   &safetyproto.SafetyEventGeofenceSettings{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/safetyeventgeofence.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_event_metadata": {
				Description:           "Additional metadata associated with safety_events. This table has a 1:1 mapping with the safety_events table joined on the safety event identifier (org_id, device_id, event_ms).",
				primaryKeys:           []string{"org_id", "device_id", "event_ms"},
				PartitionStrategy:     DateColumnFromMilliseconds("event_ms"),
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Production:            true,
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"device_id":         metadatahelpers.DeviceIdDefaultDescription,
					"event_ms":          "Unixtime (in milliseconds) that the event was triggered",
					"inbox_state":       metadatahelpers.EnumDescription("Previously tracked the workflow state of the safety event but has been deprecated for coaching_state. Currently, this field is still used to determine whether the event is starred", safetyproto.SafetyEventMetadataInboxState_name),
					"safety_manager_id": "ID of the admin assigned to the safety event",
					"coaching_state":    metadatahelpers.EnumDescription("The current workflow state of the safety event.", safetyproto.CoachingWorkflow_State_name),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_event_watermarks": {
				Description:       "The safety_event_watermarks table guarantees that all safety events before the last_polled_ms for the org_id and device_id is processed and persisted in the safety_events table.",
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"device_id":      metadatahelpers.DeviceIdDefaultDescription,
					"last_polled_ms": "Last time the event was polled for in milliseconds.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_events": {
				Description: "Represents an event that we detect in a vehicle through the VG or CM that we surface to customers in the safety inbox and the safety dashboard. A row in this table represents a behavior committed by a driver that affects their safety score.",
				primaryKeys: []string{"org_id", "device_id", "event_ms"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &tripsproto.HarshEvent{},
						ProtoPath: "go/src/samsaradev.io/stats/trips/tripsproto/trips.proto",
					},
					"additional_labels": {
						Message:   &safetyproto.SafetyEventAdditionalLabels{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/behavior_labels.proto",
					},
					"event_metadata": {
						Message:   &safetyproto.EventMetadata{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/context_labels.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("event_ms"),
				versionInfo:           SingleVersion(6),
				cdcVersion_DO_NOT_SET: TableVersion(4),
				Production:            true,
				DataModelTable:        true,
				ColumnDescriptions: map[string]string{
					"org_id":                                      metadatahelpers.OrgIdDefaultDescription,
					"device_id":                                   metadatahelpers.DeviceIdDefaultDescription,
					"event_ms":                                    "Unixtime (in milliseconds) that the event was triggered",
					"detail_proto":                                "Proto blob containing additional data associated with the triggered event",
					"detail_proto.start.latitude":                 "Gps latitude reported by the device at the start of the event",
					"detail_proto.start.longitude":                "Gps longitude reported by the device at the start of the event",
					"detail_proto.start.speed_milliknots":         "Gps speed in milliknots reported by the device at the start of the event. Used to determine whether the event is considered low speed (speeds under 4344.8812095 milliknots for events that are not crashes or rolling stops are filtered out)",
					"detail_proto.stop.latitude":                  "Gps latitude reported by the device at the end of the event",
					"detail_proto.stop.longitude":                 "Gps longitude reported by the device at the end of the event",
					"detail_proto.stop.speed_milliknots":          "Gps speed in milliknots reported by the device at the end of the event. Used to determine whether the event is considered low speed (speeds under 4344.8812095 milliknots for events that are not crashes or rolling stops are filtered out)",
					"detail_proto.duration_ms":                    "The duration of the event for applicable event types (ex. the duration of the detected driver distraction)",
					"detail_proto.accel_type":                     metadatahelpers.EnumDescription("The type of event that was triggered by the device", hubproto.HarshAccelTypeEnum_name),
					"detail_proto.target_gateway_id":              "For CM generated events, this is the gateway ID of the associated VG when the event was triggered",
					"detail_proto.brake_thresh_gs":                "The G force threshold used to trigger the event on the VG for non-crash accelerometer based events",
					"detail_proto.crash_thresh_gs":                "The G force threshold used to trigger the event on the VG for crashes",
					"detail_proto.ingestion_tag":                  "The version of harsh event v2 that this event was generated from the device",
					"detail_proto.customer_visible_ingestion_tag": "The version of harsh event v2 events that the customer should see in the safety inbox and dashboard",
					"detail_proto.hidden_to_customer":             "For harsh event v2 events, if true, this event should not be surfaced to the user",
					"trigger_reason":                              metadatahelpers.EnumDescription("The reason the safety event was created", safetyproto.SafetyEventSource_name),
					"additional_labels":                           "Proto blob that contains a list of labels that have been applied (either automatically by the system or by a user) to this event. The labels are represented by an enum. " + metadatahelpers.EnumDescription("Behavior labels", safetyproto.Behavior_Label_name),
					"release_stage":                               metadatahelpers.EnumDescription("The release stage of the safety event. Controls visibility to end users", safetyproto.SafetyEventReleaseStage_name),
					"generated_in_release_stage":                  metadatahelpers.EnumDescription("The release stage that the safety event was set to when it was initially generated", safetyproto.SafetyEventReleaseStage_name),
					"reviewed_by_samsara_at_ms":                   "Unixtime (in milliseconds) that a member of the safety event review (SER) team reviewed the event. Represents when the event became visible to customers with SER services. This is also the time the event triggers an alert for the customer if the customer has alerts enabled for events of this type",
					"driver_id":                                   "ID of the driver assigned to the safety event",
					"trip_start_ms":                               "Unixtime (in milliseconds) of the start of the trip that the event occurred on. 0 or -1 if the event happened off-trip",
					"event_metadata":                              "Proto blob containing additional metadata associated with the triggered event, such as context labels.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safety_score_settings": {
				primaryKeys: []string{"org_id", "created_at"},
				ProtoSchema: ProtobufSchema{
					"detail_proto": {
						Message:   &safetyproto.FlattenedSafetyScoreSettingsDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyproto/safetyscore.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("created_at"),
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"safetybackfill_watermark": {
				primaryKeys:                       []string{"org_id", "job_id"},
				PartitionStrategy:                 SinglePartition,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.SafetyEventIngestionDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetyeventingestionmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// Safetyeventingestiondb consistently receives traffic across all shards in both regions,
		// except shard 1 in the EU which appears unused.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"safety_events_v2": {
				primaryKeys: []string{"org_id", "device_id", "start_ms", "detection_label"},
				ProtoSchema: ProtobufSchema{
					"customer_facing_labels": {
						Message:   &safetyeventingestionmodels.CustomerFacingLabels{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/safetyeventingestionmodels/safety_events_v2.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("start_ms"),
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Description:           "Table for detected safety events. V2 of safety_events table, to replace the one in safetyDB in the future.",
				ColumnDescriptions: map[string]string{
					"org_id":                        metadatahelpers.OrgIdDefaultDescription,
					"device_id":                     metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":                      "Start time of the event in milliseconds",
					"end_ms":                        "End time of the event in milliseconds",
					"detection_label":               "The detected behavior label of the event.",
					"uuid":                          "Unique identifier of the event",
					"customer_facing_labels":        "Behavior labels of the events shown to customers",
					"customer_launch_release_stage": "Release stage for the event",
					"event_id":                      "Id of the event",
					"event_id_source":               "The source of the event id",
					"driver_id":                     "The driver the event belongs to",
					"trip_start_ms":                 "Start time of the trip that the event belongs to in milliseconds",
					"created_at_ms":                 "Time when the event was created in milliseconds",
					"updated_at_ms":                 "Last time the event was updated in milliseconds",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				Production:                        true,
			},
			"speeding_events_watermark": {
				primaryKeys:           []string{"org_id", "device_id", "detection_label"},
				PartitionStrategy:     SinglePartition,
				versionInfo:           SingleVersion(2),
				cdcVersion_DO_NOT_SET: TableVersion(2),
				Description:           "For tracking polling cycle and ongoing events, each event type will have at most one ongoing event indicated by event_start_ms",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"device_id":       metadatahelpers.DeviceIdDefaultDescription,
					"detection_label": "The detected behavior label of the event.",
					"event_start_ms":  "Time when the event started in milliseconds",
					"last_polled_ms":  "Last time the event was polled for in milliseconds",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.SafetyEventReviewDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetyeventreviewmodels/db_schema.sql",
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"firmware_ai_event_dataset_orgs": {
				primaryKeys:       []string{"org_id", "firmware_ai_event_dataset_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Represents an organization that is included in an AI event dataset.",
				ColumnDescriptions: map[string]string{
					"org_id":                         metadatahelpers.OrgIdDefaultDescription,
					"firmware_ai_event_dataset_uuid": "The unique identifer for the dataset this organization is included in.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"firmware_ai_event_datasets": {
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"config_proto": {
						Message:   &safetyeventreviewproto.DatasetConfiguration{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyeventreview/safetyeventreviewproto/firmware_ai_event_dataset_management.proto",
					},
				},
				Description: "Represents a dataset of firmware-triggered AI events stored as a CSV file in S3.",
				ColumnDescriptions: map[string]string{
					"uuid":             "The unique identifer for this dataset.",
					"start_ms":         "Timestamp in milliseconds representing the earliest possible event timestamp in the dataset.",
					"end_ms":           "Timestamp in milliseconds representing the latest possible event timestamp in the dataset.",
					"harsh_accel_type": metadatahelpers.EnumDescription("The trigger event type of events in this dataset.", hubproto.HarshAccelTypeEnum_name),
					"model_version":    "The version of the firmware model used to trigger firmware events in this dataset.",
					"cm_build":         "The build of CMs that triggered firmware events in this dataset.",
					"vg_build":         "The build of VGs that triggered firmware events in this dataset.",
					"video_length":     metadatahelpers.EnumDescription("Length of videos retrieved for events in this dataset.", safetyeventreviewproto.AIEventVideoLength_name),
					"dataset_s3_url":   "The S3 url of the dataset CSV file.",
					"config_proto":     "Additional metadata related to this dataset.",
					"config_proto.firmware_confidence_threshold":  "The firmware confidence threshold used to detect events in this dataset.",
					"config_proto.backend_confidence_threshold":   "The backend confidence threshold used to detect events in this dataset.",
					"config_proto.confidence_analysis_job_status": metadatahelpers.EnumDescription("The current state of the confidence analysis job for this dataset.", safetyeventreviewproto.DatasetAdditionalJobStatus_name),
					"description": "A description of this dataset.",
					"status":      metadatahelpers.EnumDescription("The current state of this dataset job.", safetyeventreviewproto.DatasetStatus_name),
					"event_count": "The number of events in this dataset.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"job_groups": {
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Identifies a collection of Jobs.  Jobs reference the optional collection they belong to through their job_group_uuid column.",
				ColumnDescriptions: map[string]string{
					"uuid": "The unique identifer for this job group.",
					"name": "A descriptive name for this collection.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"jobs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Represents the processing of a single entity.",
				ColumnDescriptions: map[string]string{
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"uuid":                   "The unique identifer for this job.",
					"status":                 metadatahelpers.EnumDescription("The current state of this job.", safetyeventreviewproto.JobStatus_name),
					"job_type":               metadatahelpers.EnumDescription("The type of job being processed.", safetyeventreviewproto.JobType_name),
					"due_at":                 "When (ms) the job needs to be completed by to achieve SLA.",
					"completed_at":           "When (ms) the job was completed.",
					"escape_time":            "The time (datetime) at which we should escape the job if it has been reviewed at least once.",
					"hit_sla":                "Indicates if the job was completed after its SLA.",
					"result_type":            metadatahelpers.EnumDescription("The final result type calculated from all of the job's reviews", safetyeventreviewproto.ResultType_name),
					"applied_trigger_label":  metadatahelpers.EnumDescription("Currently has no impact, trigger labels aren't changed and this should always match the initial trigger label", safetyproto.Behavior_Label_name),
					"applied_coaching_state": metadatahelpers.EnumDescription("The coaching state applied to the SafetyEvent", safetyproto.CoachingWorkflow_State_name),
					"applied_reviewer_id":    "The reviewer identified in the audit trail of changes made",
					"job_group_uuid":         "An optional reference to the uuid column of a job_groups row indicating this job belongs to a group.",
					"source_id":              "ID representing the event this request was created for. Three comma-separated values representing the organization ID, device ID and event ms respectively.",
					"event_id":               "Id of the event that is linked to the safety event of the job",
					"policy_version_id":      "The ID of the policy version used for this job.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"jobs_applied_behavior_labels": {
				primaryKeys:       []string{"org_id", "job_uuid", "behavior_label"},
				PartitionStrategy: SinglePartition,
				Description:       "Many-to-many mapping of jobs to applied behavior labels, created when a job is completed",
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"job_uuid":       "The unique identifer for the associated job",
					"behavior_label": metadatahelpers.EnumDescription("The behavior label that was applied to the associated job", safetyproto.Behavior_Label_name),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"review_policies": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition, // Configuration table typically queried by org/device/entity, not by date ranges.
				Description:       "Configuration table for review workflow policies by domain, entity type, organization, and device.",
				ColumnDescriptions: map[string]string{
					"id":                 "Auto-incrementing unique identifier for the policy.",
					"domain_id":          "The domain this policy applies to.",
					"entity_type":        "The type of entity this policy applies to.",
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"device_id":          "The device ID this policy applies to, or 0 for all devices.",
					"workflow_config":    "JSON configuration defining the review workflow.",
					"created_at":         "Timestamp when the policy was created.",
					"updated_at":         "Timestamp when the policy was last updated.",
					"created_by_user_id": "The user ID who created this policy.",
					"description":        "Human-readable description of the policy.",
					"is_deleted":         "Soft delete flag. When true, the policy is considered deleted and ignored in lookups.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"review_request_metadata": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("review_requested_at"),
				Description:       "Represents a request for a review.  Will be persisted permanently.",
				ColumnDescriptions: map[string]string{
					"org_id":              metadatahelpers.OrgIdDefaultDescription,
					"uuid":                "The unique identifier for this review request.",
					"source_id":           "The ID representing the event this request was created for.  Three comma-separated values representing the organization ID, device ID and event ms respectively.",
					"queue_name":          "The name of the queue this request will be surfaced within.  Standard event processing happens in the 'CUSTOMER' queue but customizations can be made for testing and development.",
					"item_type":           metadatahelpers.EnumDescription("The type of item being reviewed.", safetyeventreviewproto.ItemType_name),
					"review_status":       metadatahelpers.EnumDescription("The current state of this request.", safetyeventreviewproto.Review_State_name),
					"review_requested_at": "When (ms) this review was requested.",
					"review_due_at":       "When (ms) this review is due for completion to comply with SLA.",
					"review_claimed_at":   "When (ms) this review was last claimed by a reviewer.",
					"result_created_at":   "When (ms) a result was last created for this request.",
					"result_applied_at":   "When (ms) a result was last applied to the source entity for this request.",
					"reviewer_id":         "The last reviewer who claimed this request.",
					"review_type":         metadatahelpers.EnumDescription("The type of review to be performed.", safetyeventreviewproto.ReviewType_name),
					"job_uuid":            "The unique identifer for the job this request lives within.",
					"tiebreak":            "Indicates if a tiebreak review is required for this review.",
					"accel_type":          "The accel type of the harsh event associated to this request.",
					"review_type_details": "The metadata fields specific to an event of that particular review type.",
					"policy_version_id":   "The ID of the policy version used for this review request.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"review_requests": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Effectively constitutes a queue of events to be reviewed by SER agents. Records in this table are deleted once the event has been reviewed. A historical record of the review of the event is stored in review_request_metadata.",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"uuid":            "The unique identifier for this review request.",
					"source_id":       "The ID representing the event this request was created for.  Three comma-separated values representing the organization ID, device ID and event ms respectively.",
					"item_type":       metadatahelpers.EnumDescription("The type of item being reviewed.", safetyeventreviewproto.ItemType_name),
					"status":          metadatahelpers.EnumDescription("The current state of this request.", safetyeventreviewproto.Review_State_name),
					"due_date":        "When (ms) this review request is due to comply with SLA.",
					"reviewer_id":     "The last reviewer who claimed this request.",
					"review_type":     metadatahelpers.EnumDescription("The type of review to be performed.", safetyeventreviewproto.ReviewType_name),
					"priority":        "The priority field partly dictates an individual event's position in the queue. Position is determined as the highest priority review with the earliest due date. A priority of 1 is reviewed first.",
					"qa_priority":     "This serves the same function as the `priority` field, except it determines the priority for review by quality assurance (QA) agents. QA agents prioritize events slightly differently than normal agents",
					"tiebreak":        "Indicates if a tiebreak review is required for this review.",
					"queue_name":      "Provides a mechanism through which the `review_request` table can be effectively 'multiplex' several queues. By filtering the table for only rows with a specific `queue_name`, you can effectively treat the table as a queue of only items with that `queue_name`.",
					"job_uuid":        "The unique ID of the job associated with this review request.",
					"claimed_at":      "When reviewers begin reviewing an event, they acquire an exclusive claim on that event so no other agents can review it. This field records when that claim began.",
					"prior_reviewers": "Comma-separated list of reviewer ID's of the reviewers who previous reviewed the event to which this review request pertains. This is used to prevent the same agent reviewing an event twice.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"review_results": {
				primaryKeys:       []string{"org_id", "review_request_metadata_uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"detail_proto": {
						Message:   &safetyeventreviewproto.ReviewResultDetail{},
						ProtoPath: "go/src/samsaradev.io/safety/safetyeventreview/safetyeventreviewproto/review_results.proto",
					},
				},
				Description: "Represents results submitted for review requests.",
				ColumnDescriptions: map[string]string{
					"org_id":                       metadatahelpers.OrgIdDefaultDescription,
					"review_request_metadata_uuid": "The unique identifier for the review request this result is associated with.",
					"job_uuid":                     "The unique identifier for the job this result is associated with.",
					"source_id":                    "The ID representing the event this request was created for.  Three comma-separated values representing the organization ID, device ID and event ms respectively.",
					"item_type":                    metadatahelpers.EnumDescription("The type of item being reviewed.", safetyeventreviewproto.ItemType_name),
					"review_type":                  metadatahelpers.EnumDescription("The type of review to be performed.", safetyeventreviewproto.ReviewType_name),
					"reviewer_id":                  "The reviewer who submitted this result.",
					"result_type":                  metadatahelpers.EnumDescription("The type of result submitted.", safetyeventreviewproto.ResultType_name),
					"detail_proto":                 "Additional metadata.",
					"detail_proto.safety_event_result_detail":                         "Additional metadata for safety events.",
					"detail_proto.safety_event_result_detail.behavior_labels":         "The behavior labels selected for the safety event.",
					"detail_proto.safety_event_result_detail.initial_trigger_label":   "The initial trigger label on the safety event.",
					"detail_proto.safety_event_result_detail.result_type":             metadatahelpers.EnumDescription("The type of result submitted.", safetyeventreviewproto.ResultType_name),
					"detail_proto.safety_event_result_detail.reviewed_by_qa_reviewer": "Determines if the agent that submitted this review result was considered highly accurate by the SER management team at the time of submission.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vlm_configs": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores VLM prompt and inference configuration per accel_type. Highest ID for an accel_type represents the latest config version.",
				ColumnDescriptions: map[string]string{
					"id":          "Auto-increment ID that also serves as the version number.",
					"accel_type":  metadatahelpers.EnumDescription("The harsh acceleration type this config applies to.", hubproto.HarshAccelTypeEnum_name),
					"prompt_text": "The actual prompt template text sent to the VLM.",
					"model_name":  "The name of the VLM model to use (e.g., 'vertex_ai/gemini-3.0-flash').",
					"input_mode":  "How video is sent to the VLM: 1=FRAMES (extract N frames as images), 2=VIDEO (send video URL directly).",
					"frame_count": "Number of frames to extract when input_mode is FRAMES. Null when input_mode is VIDEO.",
					"created_at":  "Timestamp when the config was created.",
					"updated_at":  "Timestamp when the config was last updated.",
					"created_by":  "The identifier of the user who created this config.",
				},
			},
			"vlm_predictions": {
				primaryKeys:       []string{"event_id", "org_id", "device_id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Stores VLM predictions for safety events. Used for shadow mode POC to compare VLM verdicts with human review results.",
				ColumnDescriptions: map[string]string{
					"org_id":               metadatahelpers.OrgIdDefaultDescription,
					"device_id":            "The device ID associated with this prediction.",
					"event_id":             "The safety event ID this prediction is for.",
					"vlm_verdict":          metadatahelpers.EnumDescription("The VLM's determination of whether the safety event trigger was correct.", safetyeventreviewproto.VlmVerdict_name),
					"vlm_confidence_level": metadatahelpers.EnumDescription("The VLM's confidence in its verdict (1-5 scale).", safetyeventreviewproto.VlmConfidenceLevel_name),
					"vlm_reasoning":        "The reasoning provided by the VLM for its verdict.",
					"model_version":        "The version of the VLM model used (e.g., 'gemini-2.0-flash').",
					"prompt_version":       "The version number of the prompt used for the prediction.",
					"accel_type":           metadatahelpers.EnumDescription("The harsh acceleration type of the safety event.", hubproto.HarshAccelTypeEnum_name),
					"created_at":           "Timestamp when the prediction was created.",
					"updated_at":           "Timestamp when the prediction was last updated.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.SafetyEventTriageDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetyeventtriagemodels/db_schema.sql",
		Sharded:    true,
		// Safetyeventtriage database receives events somewhat sporadically and inconsistently across shards.
		// It's hard to measure throughput on this database.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"aggregated_event_metadata_v2": {
				Description:       "Metadata associated with each aggregation.",
				primaryKeys:       []string{"org_id", "aggregated_event_uuid", "metadata_type"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"aggregated_event_uuid": "Unique identifier of the aggregation",
					"metadata_type":         "The value type of the search field, such as behavior_label or event_total",
					"search":                "The value of a searchable value",
					"data":                  "The compilation of data of the underlying TriageEventV2, like as a string of concatenated Behavior Labels",
					"created_at_ms":         "Time when the metadata was created in milliseconds",
					"updated_at_ms":         "Last time the metadata was updated in milliseconds",
				},
			},
			"aggregated_event_triage_events_v2": {
				Description:       "Stores associations between aggregated events and triage events, in a many-to-many relationship.",
				primaryKeys:       []string{"org_id", "aggregated_event_uuid", "triage_event_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"aggregated_event_uuid": "Unique identifier of the aggregation",
					"triage_event_uuid":     "Unique identifier of the triage event",
					"created_at_ms":         "Time when the association was created in milliseconds",
				},
			},
			"aggregated_events": {
				primaryKeys:       []string{"org_id", "device_id", "start_ms", "aggregation_type"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				Description:       "Stores one row for each aggregation of any number of triage events with different trigger labels (safetyeventtriagemodels.AggregatedEvent)",
				ColumnDescriptions: map[string]string{
					"uuid":                     "The unique identifier for this aggregated event",
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"device_id":                metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":                 "Start time of the aggregated event in milliseconds",
					"end_ms":                   "End time of the aggregated event in milliseconds",
					"aggregation_type":         "Aggregation type of the event (safetyeventtriagemodels.AggregatedEventType)",
					"driver_id":                metadatahelpers.DriverIdDefaultDescription,
					"event_count":              "Total number of triage events under this aggregated event",
					"dismissed_event_count":    "Number of `Dismissed` triage events under this aggregated event",
					"needs_review_event_count": "Number of `Needs Review` triage events under this aggregated event",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"aggregated_events_v2": {
				Description:       "Aggregations for the aggregated safety inbox. Each aggregation will be associated with one or more triage events.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":           metadatahelpers.OrgIdDefaultDescription,
					"uuid":             "Unique identifier of the aggregation",
					"start_ms":         "Start time of the earliest event in the aggregation in milliseconds",
					"end_ms":           "End time of the latest event in the aggregation in milliseconds",
					"aggregation_type": "Type of the aggregation (like TripByDeviceIdDriverId)",
					"dimension_1":      "Primary dimension we aggregate across (like DeviceId)",
					"dimension_2":      "Secondary dimension we aggregate across (like DeviceId)",
					"priority":         "Priority of the aggreation in the inbox. Higher priority aggregations will be shown to the user first. ",
					"created_at_ms":    "Time when the aggregation was created in milliseconds",
					"updated_at_ms":    "Last time the aggregation was updated in milliseconds",
				},
			},
			"detections": {
				Description:       "Detection events captured from devices, including various safety and behavior detections",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":                    metadatahelpers.OrgIdDefaultDescription,
					"uuid":                      "Unique identifier for the detection",
					"device_id":                 metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":                  "Start time of the detection in milliseconds",
					"end_ms":                    "End time of the detection in milliseconds",
					"detection_type":            "Type of detection (e.g., harsh braking, acceleration)",
					"event_id":                  "Associated safety event ID if applicable",
					"driver_id":                 metadatahelpers.DriverIdDefaultDescription,
					"triage_event_uuid":         "UUID of associated triage event if applicable",
					"metadata":                  "JSON metadata containing additional detection details",
					"created_at_ms":             "Time when the detection was created in milliseconds",
					"updated_at_ms":             "Last time the detection was updated in milliseconds",
					"release_stage":             "Release stage of the detection feature",
					"in_cab_alert_played_at_ms": "Time when in-cab alert was played in milliseconds, if applicable",
				},
				Production: false,
			},
			"triage_events": {
				primaryKeys:       []string{"org_id", "device_id", "start_ms", "source_event_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				Description:       "Contains events that safety admins can triage in the aggregated safety inbox (safetyeventtriagemodels.TriageEvent)",
				ColumnDescriptions: map[string]string{
					"uuid":                  "The unique identifier for this triage event",
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"device_id":             metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":              "Start time of the event in milliseconds",
					"source_event_uuid":     "The unique identifier of the source SafetyEventV2",
					"end_ms":                "End time of the event in milliseconds",
					"trigger_label":         "Behavior label of the event (safetyproto.Behavior_Label)",
					"aggregated_event_uuid": "The unique identifier for the aggregated event that this triage event belongs to",
					"triage_state":          "Triage state of the event (safetyeventtriagemodels.TriageState)",
					"is_dismissed":          "Whether the event has been dismissed",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				Production:                        true,
			},
			"triage_events_v2": {
				Description:       "Individual triage events. Each triage event will be associated with one or more aggregations.",
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("start_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"uuid":              "Unique identifier of the triage event",
					"version":           "Version of the triage event to assist migration strategy. (Safety Event V1 or V2)",
					"device_id":         metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":         metadatahelpers.DriverIdDefaultDescription,
					"start_ms":          "Start time of the event in milliseconds",
					"end_ms":            "End time of the event in milliseconds",
					"source_event_uuid": "Uuid from the equivalent SafetyEventV2 instance. Aka, this is SafetyEventV2.Uuid",
					"trigger_label":     "The BehaviorLabel that was identified by our ML processing to be a valid event",
					"behavior_labels":   "A list of Behavior Labels that add to or override the originally detected trigger_label. Can be modified by SER or by the customer",
					"release_stage":     metadatahelpers.EnumDescription("The release stage of the safety event. Controls visibility to end users", safetyproto.SafetyEventReleaseStage_name),
					"triage_state":      "Triage state of the event (safetyeventtriagemodels.TriageState)",
					"is_dismissed":      "Whether the event has been dismissed",
					"coach_id":          "ID of the coach assigned to the safety event",
					"coaching_state":    metadatahelpers.EnumDescription("The current workflow state of the safety event.", safetyproto.CoachingWorkflow_State_name),
					"priority":          "Priority of the triage event in the inbox. Higher priority events will be shown to the user first.",
					"created_at_ms":     "Time when the triage event was created in milliseconds",
					"updated_at_ms":     "Last time the triage event was updated in milliseconds",
				},
				Production: true,
			},
			"triage_labels": {
				Description:       "Triage labels, linked by uuid to a parent object (triage event or aggregated event)",
				primaryKeys:       []string{"org_id", "uuid", "label_id"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				ColumnDescriptions: map[string]string{
					"org_id":        metadatahelpers.OrgIdDefaultDescription,
					"uuid":          "Unique identifier of the parent object",
					"label_id":      "unique identifier of the label",
					"created_at_ms": "Time when the triage label was created in milliseconds",
					"label_source":  "source for the label",
					"label_author":  "author of the label",
				},
				Production: false,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.SafetyReportingDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetyreportingmodels/db_schema.sql",
		Sharded:    true,
		// safetyreportingdb is a new low traffic DB, but as time passes and traffic flows we can update this.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"triage_events_reporting_release_stage": {
				primaryKeys:       []string{"org_id", "event_uuid"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Production:        true,
				Description:       "Stores the reporting release stage for a triage event.",
				ColumnDescriptions: map[string]string{
					"org_id":        "ID of the org that owns this event.",
					"event_uuid":    "UUID of the triage event.",
					"release_stage": "Release stage of the event.",
					"created_at":    "Timestamp when the release stage was created.",
					"updated_at":    "Timestamp when the release stage was last updated.",
				},
			},
		},
	},
	{
		name:       dbregistry.SafetyRiskDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/safetyriskmodels/db_schema.sql",
		Sharded:    true,
		CdcThroughputMonitors: map[string]ShardMonitors{
			// SafetyRiskDB should receive updates multiple times per day. Monitor a 24-hour window.
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 1440}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_risk": {
				primaryKeys:       []string{"org_id", "use_date", "driver_id"},
				PartitionStrategy: DateColumnFromDate("use_date"),
				Production:        false,
				Description:       "Driver-level daily risk classification, score, and explainability from Safety Risk.",
				ColumnDescriptions: map[string]string{
					"org_id":                     metadatahelpers.OrgIdDefaultDescription,
					"use_date":                   "The date the risk score applies to (UTC). This field represents the date the risk score applies to ('YYYY-MM-DD' format). It's used to display the latest available date's data for an org. The created_at field records when the entry was written to the database, which may differ from use_date if the data was backfilled.",
					"driver_id":                  metadatahelpers.DriverIdDefaultDescription,
					"risk_classification":        "Discrete risk class label assigned to the driver for the use_date.",
					"risk_score":                 "Continuous risk score computed by the Safety Risk model.",
					"explainability_features_v1": "Model features used in v1 explainability output (JSON).",
					"explainability_features_v2": "Model features used in v2 explainability output (JSON).",
					"explanation":                "Model explanation payload for the assigned risk (JSON).",
					"risk_score_computed_at":     "Timestamp when the risk score was computed (UTC).",
					"created_at":                 "Timestamp when this record was created (UTC).",
					"updated_at":                 "Timestamp when this record was last updated (UTC).",
				},
			},
			"driver_risk_v2": {
				primaryKeys:       []string{"org_id", "use_date", "driver_id", "version"},
				PartitionStrategy: DateColumnFromDate("use_date"),
				Production:        false,
				Description:       "Driver-level daily risk classification, score, and explainability from Safety Risk (v2).",
				ColumnDescriptions: map[string]string{
					"org_id":              metadatahelpers.OrgIdDefaultDescription,
					"use_date":            "The date the risk score applies to (UTC). This field represents the date the risk score applies to ('YYYY-MM-DD' format). It's used to display the latest available date's data for an org. The created_at field records when the entry was written to the database, which may differ from use_date if the data was backfilled.",
					"driver_id":           metadatahelpers.DriverIdDefaultDescription,
					"version":             "Model version identifier used for the risk computation.",
					"risk_classification": "Discrete risk class label assigned to the driver for the use_date.",
					"risk_score":          "Continuous risk score computed by the Safety Risk model.",
					"explanation":         "Model explanation payload for the assigned risk (JSON).",
					"created_at":          "Timestamp when this record was created (UTC).",
					"updated_at":          "Timestamp when this record was last updated (UTC).",
				},
			},
		},
	},
	{
		name:       dbregistry.ScoringDB,
		schemaPath: "go/src/samsaradev.io/safety/infra/scoringmodels/db_schema.sql",
		Sharded:    true,
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usNonProdShardMonitors(ShardMonitor{TimeframeMinutes: 24 * 60}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 24 * 60}),
		},
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_aggregate_scores": {
				primaryKeys:       []string{"org_id", "agg_window", "driver_id", "start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("end_ms"),
				ProtoSchema: map[string]ProtoDetail{
					"trip_data": {
						Message:   &scoringmodels.TripData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"config_snapshot": {
						Message:   &scoringmodels.ConfigSnapshot{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"triage_events": {
						Message:   &scoringmodels.AggregateTriageEventData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
				},
				Description: "An aggregation of trip-scores for a specific driver and interval",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"agg_window":      "The aggregation window",
					"driver_id":       metadatahelpers.DriverIdDefaultDescription,
					"start_ms":        "The aggregation start ms",
					"end_ms":          "The aggregation end ms",
					"trip_data":       "Aggregate data for trips within the time window",
					"config_snapshot": "A aggregate snapshot of relevant config",
					"triage_events":   "Aggregated triage event score information",
				},
			},
			"triage_event_state": {
				primaryKeys:       []string{"org_id", "triage_event_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("trip_start_ms"),
				ProtoSchema: map[string]ProtoDetail{
					"triage_event_data": {
						Message:   &scoringmodels.TriageEventData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_state.proto",
					},
				},
				Description: "Scoring's most recent understanding of a triage event, for use in synchronization",
				ColumnDescriptions: map[string]string{
					"org_id":            metadatahelpers.OrgIdDefaultDescription,
					"triage_event_uuid": "The triage event uuid",
					"trip_start_ms":     "The trip this event occurred on",
					"triage_event_data": "Scoring's latest understanding of this triage event",
				},
			},
			"trip_scores": {
				primaryKeys:       []string{"org_id", "device_id", "trip_start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("trip_end_ms"),
				ProtoSchema: map[string]ProtoDetail{
					"trip_data": {
						Message:   &scoringmodels.TripData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"config_snapshot": {
						Message:   &scoringmodels.ConfigSnapshot{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"triage_events": {
						Message:   &scoringmodels.AggregateTriageEventData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
				},
				Description: "Scoring's understanding of a trip, including all data necessary to compute scores",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"device_id":       metadatahelpers.DeviceIdDefaultDescription,
					"trip_start_ms":   "The trip start ms",
					"trip_end_ms":     "The trip end ms, null for in-progress trips",
					"driver_id":       metadatahelpers.DriverIdDefaultDescription,
					"trip_data":       "Snapshotted data about a trip for scoring",
					"config_snapshot": "A trip-level snapshot of relevant config",
					"triage_events":   "Aggregated triage event score information",
					"created_at_ms":   "The row creation time",
					"updated_at_ms":   "The latest row update time",
				},
			},
			"vehicle_aggregate_scores": {
				primaryKeys:       []string{"org_id", "agg_window", "device_id", "start_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("end_ms"),
				ProtoSchema: map[string]ProtoDetail{
					"trip_data": {
						Message:   &scoringmodels.TripData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"config_snapshot": {
						Message:   &scoringmodels.ConfigSnapshot{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
					"triage_events": {
						Message:   &scoringmodels.AggregateTriageEventData{},
						ProtoPath: "go/src/samsaradev.io/safety/infra/scoringmodels/triage_event_score.proto",
					},
				},
				Description: "An aggregation of trip-scores for a specific vehicle and interval",
				ColumnDescriptions: map[string]string{
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"agg_window":      "The aggregation window",
					"device_id":       metadatahelpers.DeviceIdDefaultDescription,
					"start_ms":        "The aggregation start ms",
					"end_ms":          "The aggregation end ms",
					"trip_data":       "Aggregate data for trips within the time window",
					"config_snapshot": "A aggregate snapshot of relevant config",
					"triage_events":   "Aggregated triage event score information",
				},
			},
		},
	},
	{
		name:       dbregistry.SensorConfigDB,
		schemaPath: "go/src/samsaradev.io/fleet/assets/sensorconfiguration/sensorconfigmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"device_tire_pressure_gateway_configs": {
				primaryKeys:       []string{"org_id", "integration_type", "tire_pressure_gateway_serial"},
				PartitionStrategy: SinglePartition,
				Description:       "Defines the assocations between a device and tire pressure gateway.",
				ColumnDescriptions: map[string]string{
					"org_id":                       "ID of the org that owns the config.",
					"integration_type":             "Tire Pressure integration type.",
					"tire_pressure_gateway_serial": "Serial of the tire pressure gateway.",
					"device_id":                    "Device Id the Tire Pressure Config applies to.",
					"created_at":                   "Timestamp when the config was created.",
					"updated_at":                   "Timestamp when the config was last updated.",
				},
			},
		},
	},
	{
		name:       dbregistry.SessionsDB,
		schemaPath: "go/src/samsaradev.io/platform/sessions/sessionsmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
		},
		Tables: map[string]tableDefinition{
			"session_principals": {
				Description:       "This table contains a mapping of session UUIDs to principals.",
				primaryKeys:       []string{"uuid", "principal_type", "principal_id"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"uuid":           "The UUID of the session.",
					"principal_id":   "The ID of the principal (e.g. user ID, driver ID, ...).",
					"principal_type": "The type of the principal (0=user, 1=driver, ...).",
				},
			},
			"sessions": {
				Description:       "Tracks information for every session.",
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: DateColumnFromTimestamp("expires_at"),
				ColumnDescriptions: map[string]string{
					"uuid":           "The UUID of the session.",
					"expires_at":     "The time at which the session expires.",
					"invalidated_at": "The time at which the session was invalidated.",
				},
			},
		},
	},
	{
		name:       dbregistry.SignalPromotionDB,
		schemaPath: "go/src/samsaradev.io/fleet/diagnostics/signalpromotion/signalpromotionmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"activity_history": {
				primaryKeys:       []string{"promotion_uuid", "created_at_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				Description:       "Stores the history of activities performed on a promotion.",
				ColumnDescriptions: map[string]string{
					"promotion_uuid": "The unique identifier of the promotion this activity is associated with.",
					"created_at_ms":  "Unix timestamp in milliseconds of when the record was created.",
					"activity":       "The activity performed on the promotion.",
					"stage":          metadatahelpers.EnumDescription("Stage of the promotion when ativity was performed", signalpromotionproto.PromotionStage_name),
					"user_id":        "ID of the user who performed the activity.",
					"comment":        "Comment provided by the user who performed the activity.",
				},
			},
			"alpha_internal_obd_mappings": {
				primaryKeys:       []string{"population_uuid", "promotion_uuid", "started_at_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("started_at_ms"),
				versionInfo:       SingleVersion(1),
				Description: `Stores the mappings between internal OBD value and actual promotion's OBD value.

This table is used only at the Alpha stage of the promotion and allows simultaneous evaluation of multiple singal's definitions.`,
				ColumnDescriptions: map[string]string{
					"population_uuid":    "The unique identifier of the population this mapping is associated with.",
					"promotion_uuid":     "The unique identifier of the promotion this mapping is associated with.",
					"internal_obd_value": metadatahelpers.EnumDescription("Internal OBD value for the signal under promotion.", hubproto.ObdValue_name),
					"started_at_ms":      "Unix timestamp in milliseconds of when the mapping started.",
					"ended_at_ms":        "Unix timestamp in milliseconds of when the mapping ended.",
				},
			},
			"populations": {
				primaryKeys:       []string{"population_uuid"},
				PartitionStrategy: SinglePartition,
				versionInfo:       SingleVersion(3),
				Description:       "Stores unique populations of vehicles for which signals are promoted.",
				ColumnDescriptions: map[string]string{
					"population_uuid":       "The unique identifier for this population.",
					"make":                  "Make of the vehicle.",
					"model":                 "Model of the vehicle.",
					"year":                  "Production year of the vehicle.",
					"engine_model":          "Engine model",
					"fuel_type_primary":     "Primary fuel type",
					"fuel_type_secondary":   "Secondary fuel type",
					"trim_primary":          "Primary trim",
					"trim_secondary":        "Secondary trim",
					"ignore_vin":            "Indicates whether the VIN should be ignored when matching vehicles to this population.",
					"electrification_level": "Electrification level",
					"created_at_ms":         "Unix timestamp in milliseconds of when the record was created.",
					"powertrain":            metadatahelpers.EnumDescription("Powertrain", vehiclepropertyproto.Properties_Fuel_Powertrain_name),
					"fuel_group":            metadatahelpers.EnumDescription("Fuel group", vehiclepropertyproto.Properties_Fuel_FuelGroup_name),
					"trim":                  "Trim is the concatenation of primary trim and secondary trim.",
					"type":                  metadatahelpers.EnumDescription("Type of the population", signalpromotionproto.PopulationType_name),
					"country":               "The country that the vehicle operates in for diagnostic signal targeting (user specified).",
				},
			},
			"pre_alpha_checks": {
				primaryKeys:       []string{"promotion_uuid", "device_id", "pre_alpha_check_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				Description:       "Stores the results of the pre-alpha checks performed on a promotion.",
				ColumnDescriptions: map[string]string{
					"pre_alpha_check_uuid": "Unique identifier for this pre-alpha check.",
					"promotion_uuid":       "Unique identifier of the promotion this pre-alpha check is associated with.",
					"device_id":            "ID of the device that was checked.",
					"created_at_ms":        "Unix timestamp in milliseconds of when the record was created.",
					"updated_at_ms":        "Unix timestamp in milliseconds of when the record was updated.",
					"result":               "Result of the pre-alpha check.",
					"result_details":       "Detailed output of the command(s) performed during pre-alpha check.",
				},
			},
			"promotions": {
				primaryKeys:       []string{"population_uuid", "signal_uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				versionInfo:       SingleVersion(1),
				Description:       "Stores the promotions of signals for a specific populations.",
				ColumnDescriptions: map[string]string{
					"promotion_uuid":  "The unique identifier for this promotion.",
					"population_uuid": "The unique identifier of the population this promotion is targeting.",
					"signal_uuid":     "The unique identifier of the signal being promoted.",
					"created_by":      "ID of the user who created the promotion.",
					"created_at_ms":   "Unix timestamp in milliseconds of when the record was created.",
					"updated_at_ms":   "Unix timestamp in milliseconds of when the record was updated i.e. when state/status changed.",
					"stage":           metadatahelpers.EnumDescription("Current stage of the promotion.", signalpromotionproto.PromotionStage_name),
					"status":          metadatahelpers.EnumDescription("Current status of the promotion.", signalpromotionproto.PromotionStatus_name),
				},
			},
			"signal_mappings": {
				primaryKeys:       []string{"signal_uuid", "input_state"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores the mappings between input and output states of a signal.",
				ColumnDescriptions: map[string]string{
					"signal_uuid":  "The unique identifier of the signal this mapping is associated with.",
					"input_state":  "The value of the signal that will be mapped to output_state.",
					"output_state": "The final value that will be reported to obd_value for given input_state.",
				},
			},
			"signals": {
				primaryKeys:       []string{"obd_value", "response_id", "signal_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores unique signal definitions that can be promoted.",
				ColumnDescriptions: map[string]string{
					"signal_uuid":            "The unique identifier for this signal definition.",
					"created_at_ms":          "Unix timestamp in milliseconds of when the record was created.",
					"data_source":            "One of `DataSource` enum values defined in `global_obd_configuration`.",
					"network_interface":      metadatahelpers.EnumDescription("Network interface of the signal.", objectstatproto.VehicleDiagnosticBus_name),
					"obd_value":              metadatahelpers.EnumDescription("OBD value of the signal.", hubproto.ObdValue_name),
					"protocol":               metadatahelpers.EnumDescription("Protocol of the signal.", hubproto.ObdProtocol_name),
					"transmission_type":      metadatahelpers.EnumDescription("Type of the signal. Can be either 'active' or 'broadcast'.", signalpromotionproto.TransmissionType_name),
					"data_identifier":        "Maps to `ModePid` in `PassengerObdConfig` or `PGN` in `ObdJ1939ConfigInfo`.",
					"request_period_ms":      "Number of milliseconds between consecutive requests if the signal is active.",
					"passive_response_mask":  "Broadcast source mask if the signal is broadcast. Only applies to passenger protocols.",
					"request_id":             "ECU request address",
					"response_id":            "ECU source address",
					"bit_start":              "Bit offset of the signal value in the response.",
					"bit_length":             "Bit length of the signal value in the response.",
					"sign":                   metadatahelpers.EnumDescription("Sign of the signal.", hubproto.Sign_name),
					"endianness":             metadatahelpers.EnumDescription("Endianness of the signal.", hubproto.Endian_name),
					"scale":                  "Scale factor to apply for the signal readings.",
					"offset":                 "Offset to apply to the signal readings.",
					"minimum":                "Minimal value of the signal readings.",
					"maximum":                "Maximal value of the signal readings.",
					"internal_scaling":       "Scale factor for OEM to Samsara units conversion.",
					"comment":                "Comment describing the signal.",
					"state_encoding_enabled": "Indicates if state encoding is enabled for the signal.",
					"state_default_value":    "Default output reported to obd_value when no mapping exists for given input_state. Only applied when is no null.",
				},
			},
		},
	},
	{
		name:       dbregistry.SpeedlimitsDB,
		schemaPath: "go/src/samsaradev.io/infra/speedlimit/speedlimitsmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"commercial_speed_limit_overrides": {
				primaryKeys:       []string{"org_id", "way_id", "vehicle_type", "version_id", "dataset_version"},
				PartitionStrategy: SinglePartition,
				Description:       "User created overrides for the commercial speed limit of a given way (road segment) and vehicle type",
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"way_id":                          "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"original_speed_limit_milliknots": "The default speed limit (as if no overrides existed in our system) for the way id and vehicle type at the time of override creation in milliknots",
					"override_speed_limit_milliknots": "The override for the speed limit for the way id and vehicle type in milliknots",
					"created_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was created",
					"updated_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was last updated",
					"data_source":                     metadatahelpers.EnumDescription("Source of the speed limit data that is being overridden", speedlimitproto.SpeedLimitDataSource_name),
					"vehicle_type":                    metadatahelpers.EnumDescription("Type of vehicle that the speed limit is being overridden for", maptileproto.VehicleTypeTag_name),
					"user_id":                         "Identifier of the user that last updated or created the override",
					"version_id":                      "Version of the map tile that this override was generated using",
					"dataset_version":                 "The relative version of the dataset used to generate the speed limit",
					"slippy_x":                        "The horizontal position of the way_id used to identify the relevant map tile",
					"slippy_y":                        "The vertical position of the way_id used to identify the relevant map tile",
				},
				versionInfo: SingleVersion(1),
			},
			"global_speed_limit_overrides": {
				primaryKeys:       []string{"way_id", "vehicle_type"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores speed limit overrides where the overrides have been derived from customers in agreement on an override for a given way id.",
				ColumnDescriptions: map[string]string{
					"way_id":                          "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"vehicle_type":                    metadatahelpers.EnumDescription("Type of vehicle that the speed limit is being overridden for", maptileproto.VehicleTypeTag_name),
					"original_speed_limit_milliknots": "The maximum speed limit for the way id at the time of override creation that is being overridden in milliknots",
					"override_speed_limit_milliknots": "The override for the maximum speed limit for the way id and vehicle type in milliknots",
					"created_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was created",
					"updated_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was last updated",
					"orgs_in_agreement_count":         "Number of unique organizations that agree with the override value for the way id",
					"slippy_x":                        "The horizontal position of the way_id used to identify the relevant map tile",
					"slippy_y":                        "The vertical position of the way_id used to identify the relevant map tile",
				},
			},
			"speed_limit_overrides": {
				primaryKeys:       []string{"org_id", "way_id", "version_id", "dataset_version"},
				PartitionStrategy: SinglePartition,
				Description:       "User created overrides for the maximum speed limit of a given way (road segment)",
				ColumnDescriptions: map[string]string{
					"org_id":                          metadatahelpers.OrgIdDefaultDescription,
					"way_id":                          "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"original_speed_limit_milliknots": "The maximum speed limit for the way id at the time of override creation that is being overridden in milliknots",
					"override_speed_limit_milliknots": "The override for the maximum speed limit for the way id and vehicle type in milliknots",
					"created_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was created",
					"updated_at_ms":                   "Unixtime (in milliseconds) that the speed limit override was last updated",
					"data_source":                     metadatahelpers.EnumDescription("Source of the speed limit data that is being overridden", speedlimitproto.SpeedLimitDataSource_name),
					"user_id":                         "Identifier of the user that last updated or created the override",
					"version_id":                      "Version of the map tile that this override was generated using",
					"slippy_x":                        "The horizontal position of the way_id used to identify the relevant map tile",
					"slippy_y":                        "The vertical position of the way_id used to identify the relevant map tile",
					"dataset_version":                 "The relative version of the dataset used to generate the speed limit",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"speed_limit_signs_metadata": {
				primaryKeys:       []string{"org_id", "way_id", "captured_timestamp"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores metadata for speed limit signs",
				ColumnDescriptions: map[string]string{
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"way_id":                 "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"captured_timestamp":     "The timestamp of the capture of the speed limit sign",
					"heading":                "The heading of the vehicle",
					"s3_url":                 "The S3 URL of the speed limit sign",
					"speed_limit_milliknots": "The speed limit of the speed limit sign",
					"latitude":               "The latitude of the speed limit sign",
					"longitude":              "The longitude of the speed limit sign",
					"image_quality":          "The quality of the image of the speed limit sign",
					"is_deleted":             "Indicates if the speed limit sign is deleted",
				},
				versionInfo: SingleVersion(1),
			},
			"speed_limits": {
				primaryKeys:       []string{"way_id", "speed_limit_type", "seen_at_ms"},
				PartitionStrategy: SinglePartition,
				Description:       "Deprecated.",
			},
			"way_speed_histograms": {
				primaryKeys:       []string{"way_id", "day_of_week", "hour_of_day"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores speed distribution histogram data for specific road segments (way_id) partitioned by day of week and hour of day, enabling time-aware speed analytics.",
				ProtoSchema: map[string]ProtoDetail{
					"speed_distribution_json": {
						Message:   &noningestionspeedlimitproto.WaySpeedDistribution{},
						ProtoPath: "go/src/samsaradev.io/safety/noningestionspeedlimitproto/noningestionspeedlimitservice.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"way_id":                  "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"day_of_week":             metadatahelpers.EnumDescription("Day of the week for the speed distribution data", pingschedulesproto.DayOfWeek_name),
					"hour_of_day":             "Hour of the day (0-23) for the speed distribution data",
					"speed_distribution_json": "JSON object containing speed distribution histogram data including metadata (bin_size, lower_bound, upper_bound, total_samples) and probabilities array for rendering histograms",
				},
				versionInfo: SingleVersion(1),
				Tags: []amundsentags.Tag{
					amundsentags.SafetyTag,
					amundsentags.PerformanceTag,
				},
			},
			"way_to_slippy_x_y": {
				primaryKeys:       []string{"slippy_x", "slippy_y", "way_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Stores the mapping of a way_id to the corresponding slippy X and Y co-ordinates. Used for mapping between way ids and FW tiles.",
				ColumnDescriptions: map[string]string{
					"way_id":   "The OSM (Open Street Map) identifier for a specific way (road segment)",
					"slippy_x": "The horizontal position of the way_id used to identify the relevant map tile",
					"slippy_y": "The vertical position of the way_id used to identify the relevant map tile",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:            dbregistry.StatsDB,
		schemaPath:      "go/src/samsaradev.io/stats_models/db_schema.sql",
		mysqlDbOverride: "stats_db",
		Sharded:         false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"driver_media_uploads": {
				primaryKeys:       []string{"uuid"},
				PartitionStrategy: DateColumnFromMilliseconds("created_at_ms"),
				Description:       "Metadata table for driver media uploads used for documents and DVIRs",
				ColumnDescriptions: map[string]string{
					"uuid":           "The unique identifier for the upload",
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"driver_id":      metadatahelpers.DriverIdDefaultDescription,
					"type":           "Enum value of type DriverMediaUploadType",
					"created_at_ms":  "Millisecond timestamp of when the record was created",
					"content_type":   "The type of content stored in the media file",
					"content_length": "The length in bytes of the content",
					"content_md5":    "The MD5 hash associated with the content",
					"metadata_json":  "Not used",
					"s3_bucket_name": "The name of the S3 bucket where the media is stored",
					"s3_object_key":  "The object key of the file",
					"state":          "Enum value of type DriverMediaUploadState",
					"name":           "The file name of the media upload",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_trailer_assignments": {
				primaryKeys:       []string{"org_id", "session_uuid"},
				PartitionStrategy: DateColumnFromTimestamp("start_at"),
				Description:       "Indicates when a driver gets a trailer assigned for a trip.",
				ColumnDescriptions: map[string]string{
					"session_uuid": "The uuid identifying the drive session",
					"org_id":       metadatahelpers.OrgIdDefaultDescription,
					"driver_id":    metadatahelpers.DriverIdDefaultDescription,
					"trailer_id":   "The ID of the trailer that is being assigned.",
					"start_at":     "Time when the trailer was assigned to the driver.",
					"end_at":       "Time when the trailer was unassigned.",
					"created_at":   "Time when the assignment was created.",
					"updated_at":   "Time when the assignment was updated.",
					"is_passenger": "Indicates if the assignment is for a driver or a passenger.",
					"hard_trailer": "Indicates if the assigned trailer has samsara hardware equipped.",
					"trailer_name": "The name of the trailer that is being assigned.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvir_defect_types": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "The types of DVIR defects an org can add to a template",
				ColumnDescriptions: map[string]string{
					"uuid":             "The unique identifier for the defect type",
					"org_id":           metadatahelpers.OrgIdDefaultDescription,
					"label":            "A brief text description of the defect type",
					"section":          metadatahelpers.EnumDescription("The section of the DVIR the defect type should appear in", dvirsmodels.DvirTemplateSections),
					"created_by":       "The ID of the user that created the defect type. 0 if the defect type was automatically created",
					"created_at":       "The timestamp of the creation of the defect type",
					"deleted":          "A number that is 1 if the defect type has been deleted, otherwise 0",
					"deleted_by":       "The user that deleted the defect type. If the defect type is not deleted or was deleted automatically, this is 0",
					"deleted_at":       "The timestamp of the deletion of the defect type. May be 0001-01-01T00:00:00Z if the defect is not deleted",
					"requires_comment": "1 if a comment is required when adding a defect of this type, otherwise 0 or null",
					"requires_photo":   "1 if a photo is required when adding a defect of this type, otherwise 0 or null",
					"updated_by":       "The ID of the user that updated the defect type last",
					"updated_at":       "The timestamp of the last update of the defect type",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvir_defects": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "The types of DVIR defects an org can add to a template",
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &dvirsproto.DvirDefect{},
						ProtoPath: "go/src/samsaradev.io/dvirs/dvirsproto/dvir.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"id":                    "The ID for the defect. Autoincrementing",
					"dvir_id":               "The ID of the DVIR this defect is on",
					"compartment_type":      "The compartment of the DVIR the defect refers to. Deprecated in favor of the section column on dvir_defect_types",
					"defect_type":           metadatahelpers.EnumDescription("The type of the defect. Deprecated in favor of the section column on dvir_defect_types", dvirsmodels.DefaultDvirDefectTypes),
					"comment":               "The comment left by the creator of the defect",
					"proto":                 "A proto that may contain the defect photo",
					"dvir_defect_type_uuid": "The UUID of the defect type for this defect",
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"vehicle_id":            "The device ID of the vehicle, if the DVIR is for a vehicle",
					"deprecated_trailer_id": "The trailer ID of the trailer, if the DVIR is for a trailer. Deprecated in favor of trailer_device_id",
					"trailer_name":          "The name of the trailer, if the DVIR is for a trailer",
					"resolved":              metadatahelpers.EnumDescription("The DVIR's resolution status", dvirsproto.DvirDefectFields_ResolutionStatus_name),
					"resolved_at":           "Timestamp for when the resolved column was set",
					"resolved_by_driver_id": "Driver ID of the driver who resolved this defect",
					"resolved_by_user_id":   "User ID of the user who resolved this defect",
					"created_at":            "Timestamp for when the defect was created",
					"trailer_device_id":     "The device ID of the trailer, if the DVIR is for a trailer",
					"mechanic_notes":        "Nodes added by the mechanic when updating the resolution status",
					"mechanic_notes_ms":     "Timestamp for when the mechanic added notes",
					"updated_at":            "Timestamp for when the defect was updated",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvir_templates": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "The templates that can be used to create a DVIR",
				ColumnDescriptions: map[string]string{
					"uuid":            "The unique identifier for this DVIR template",
					"name":            "The template name",
					"org_id":          metadatahelpers.OrgIdDefaultDescription,
					"is_org_default":  "1 if the template is the default one used for this org, 0 otherwise",
					"current_version": "The version of this template. Updated whenever the template is modified",
					"created_by":      "The user who created the template. 0 if it was auto-generated",
					"created_at":      "Timestamp for when the template was created",
					"updated_by":      "The user who last updated the template. 0 if it was auto-generated or auto-updated",
					"updated_at":      "Timestamp for when the template was last updated",
					"deleted":         "True if the template has been deleted",
					"deleted_by":      "The user who deleted the template",
					"deleted_at":      "Timestamp for when the template was deleted",
					"proto":           "A proto containing the DVIR defects associated with past versions of the template",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvir_templates_dvir_defect_types": {
				primaryKeys:       []string{"org_id", "dvir_template_uuid", "dvir_defect_type_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Joins DVIR templates to the DVIR defect types assigned to those templates",
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"dvir_template_uuid":    "The UUID of the DVIR template the defect type is assigned to",
					"dvir_defect_type_uuid": "The UUID of the DVIR defect type",
					"created_at":            "Timestamp for when the record was created",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvirs": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "Driver-Vehicle Inspection Reports that are created by drivers based on predefined templates and may have one or more defects",
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &dvirsproto.Dvir{},
						ProtoPath: "go/src/samsaradev.io/dvirs/dvirsproto/dvir.proto",
					},
				},
				DataModelTable: true,
				ColumnDescriptions: map[string]string{
					"id":                       "The ID for the DVIR. Autoincrementing",
					"org_id":                   metadatahelpers.OrgIdDefaultDescription,
					"group_id":                 metadatahelpers.GroupIdDefaultDescription,
					"vehicle_id":               "The device ID of the vehicle, if the DVIR is for a vehicle",
					"driver_id":                "The ID of the driver who created the DVIR",
					"log_at":                   "Timestamp for when the DVIR was uploaded to the backend",
					"created_at":               "Timestamp for when the driver selected the \"Create DVIR\" button",
					"updated_at":               "Timestamp for when the DVIR was most recently updated",
					"odometer_meters":          "The odometer reading when the DVIR was created",
					"trailer_name":             "The name of the trailer, if the DVIR is for a trailer",
					"defects_need_correction":  "Used in combination with defects_corrected to determine resolution status. If 1, the DVIR is either unsafe or resolved. If 0, the DVIR is safe",
					"driver_signed_at":         "Timestamp for when the driver selected the \"Certify & Submit\" button",
					"mechanic_signed_at":       "Timestamp for when a cloud app or API user resolved the DVIR or marked it as safe",
					"inspection_type":          metadatahelpers.EnumDescription("The type of the DVIR", dvirsmodels.InspectionTypes),
					"defects_corrected":        "Used in combination with defects_need_correction to determine resolution status. If 1, the DVIR is resolved. If 0, the DVIR is either safe or unsafe",
					"defects_ignored":          "Legacy field for noting that the defects were ignored. Deprecated",
					"mechanic_notes":           "Notes added by the mechanic when marking the DVIR safe. Not used if the mechanic updates the DVIR via its defects",
					"author_user_id":           "User ID of the DVIR if it wasn't created by a driver",
					"next_safe_dvir_driver_id": "Driver ID of the driver who next marked the asset as safe, if applicable",
					"next_safe_dvir_user_id":   "User ID of the mechanic to next marked the asset as safe, if applicable",
					"next_safe_dvir_signed_at": "Timestamp for when the asset was next marked as safe",
					"next_safe_dvir_id":        "ID of the next DVIR for this asset that was marked safe",
					"next_driver_id":           "Driver ID of the next driver to create a DVIR for this asset",
					"next_driver_signed_at":    "Timestamp for when the next DVIR was created for this asset",
					"next_driver_dvir_id":      "ID of the next DVIR for this asset",
					"proto":                    "A proto containing an array of walkaround photos",
					"dvir_type":                "The type of the DVIR. One of \"truck\", \"trailer\" and \"truck+trailer\"",
					"deprecated_trailer_id":    "The trailer ID if the DVIR is for a trailer. Deprecated",
					"vin":                      "The vehicle VIN as entered on the DVIR",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"dvirs_to_dvir_defects": {
				primaryKeys:       []string{"org_id", "dvir_id", "dvir_defect_id"},
				PartitionStrategy: SinglePartition,
				Description:       "Joins DVIRs to the DVIR defects assigned to those DVIRs",
				ColumnDescriptions: map[string]string{
					"org_id":         metadatahelpers.OrgIdDefaultDescription,
					"dvir_id":        "The ID of the DVIR the defect is assigned to",
					"dvir_defect_id": "The ID of the DVIR defect",
				},
				Tags: []amundsentags.Tag{
					amundsentags.MaintenanceTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"rfid_card_holder_assignments": {
				primaryKeys:       []string{"id"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "An RFID Card (aka NFC Card aka ID Card) scan from a driver or student.",
				ColumnDescriptions: map[string]string{
					"id":                      "Auto-incrementing ID.",
					"org_id":                  metadatahelpers.OrgIdDefaultDescription,
					"rfid_card_code":          "Identifiier of the physical card.",
					"rfid_card_holder_id":     "Identifier of the card_holder_object_type.",
					"start_at":                "Datetime when the assignment mapping started.",
					"end_at":                  "Datetime when the assignment mapping ended (nullable).",
					"created_at":              "Datetime of creation.",
					"modified_at":             "Datetime of last modification.",
					"card_holder_object_type": metadatahelpers.EnumDescription("Entity type that this card belongs to.", statsproto.CardHolderObjectTypeEnum_name),
				},
				Tags: []amundsentags.Tag{
					amundsentags.SafetyTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.TachographDB,
		schemaPath: "go/src/samsaradev.io/fleet/compliance/eu/tachographmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"company_card_assignments": {
				primaryKeys:       []string{"org_id", "card_serial_number"},
				PartitionStrategy: DateColumnFromTimestamp("created_at"),
				Description:       "The company card assignments table shows the latest connection state for a given company card. This mapping could show that the card was most recently connected to a VG X, or instead most recently disconnected from VG Y",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"card_serial_number": "The serial number for the company card",
					"gateway_id":         "Id of the gateway (see the gateways table in the products DB)",
					"card_number":        "First 13 digits (out of 16) of the company card number",
					"card_connected":     "Whether the latest connection state of this card was connected or disconnected",
					"created_at":         "Timestamp at which the assignment was created",
					"updated_at":         "Timestamp at which the assignment was updated",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
			},
			"driver_device_assignments": {
				primaryKeys:       []string{"org_id", "started_at", "device_id", "is_primary"},
				PartitionStrategy: DateColumnFromTimestamp("started_at"),
				Description:       "The driver device assignments table holds all periods for which a specific driver was assigned to a device. Assignments are sourced using live tachograph stats (ObjectStatEnum_osDTachographDriverState)",
				ColumnDescriptions: map[string]string{
					"org_id":      metadatahelpers.OrgIdDefaultDescription,
					"uuid":        "The generated id for this assignment",
					"device_id":   "Id of the device that the assignment is against",
					"driver_id":   "Id of the driver that the assignment is for",
					"card_number": "Driver tachograph card number",
					"is_primary": "Indicates whether this driver was the primary driver or not. \n\n " +
						"The primary driver will have his card inserted into the first tachograph card slot, whilst the secondary will have their card inserted into the secondary tachograph card slot",
					"started_at": "Timestamp at which the assignment started",
					"ended_at":   "Timestamp at which the assignment ended",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"driver_file_downloads": {
				primaryKeys:       []string{"org_id", "card_number_id", "os_changed_at_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("os_changed_at_ms"),
				Description:       "A DriverFileDownload represents an individual driver file download.",
				ColumnDescriptions: map[string]string{
					"org_id":                         metadatahelpers.OrgIdDefaultDescription,
					"card_number_id":                 "The 14 digit card number uniquely identifying a driver associated with the driver file download.",
					"card_number_seq":                "The remaining 2 digit card sequence number.",
					"os_changed_at_ms":               "The changed at timestamp associated with the driver file object stat or the time at which manually uploaded file got stored.",
					"device_id":                      "The device id associated with the driver file object stat.",
					"object_stat":                    "The object stat proto associated with the driver file download.",
					"s3_bucket":                      "Name of S3 bucket where file is stored.",
					"s3_key":                         "S3 Key corresponding to file in bucket.",
					"uuid":                           "UUID of the file.",
					"file_processing_status":         "The processing status of the file, tracking whether the file has been processed and the associated logs stored 'tachoproto.DriverFileProcessingStatus'.",
					"earliest_activity_timestamp_ms": "Timestamp of the first activity entry in the driver file.",
					"latest_activity_timestamp_ms":   "Timestamp of the latest activity entry in the driver file.",
					"is_manually_uploaded":           "True if the file was manually uploaded.",
					"issuing_country":                "Country which issued the tachograph card.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"tachograph_historical_logs": {
				primaryKeys:       []string{"org_id", "card_number_id", "daily_presence_counter", "daily_sequence_id"},
				PartitionStrategy: DateColumnFromMilliseconds("timestamp_ms"),
				Description:       "A TachographHistoricalLog represents a single timestamped tachograph log as parsed from a driver file. Note that this table does not contain a complete history of tachograph logs - some get overwritten (eg if a new log comes in with the same PK but with a different card number sequence to what is already persisted).",
				ColumnDescriptions: map[string]string{
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"card_number_id":         "The 14 digit card number uniquely identifying a driver associated with the tachograph log.",
					"card_number_seq":        "The remaining 2 digit card sequence number on the driver card.",
					"daily_presence_counter": "The daily presence counter for the driver, incrementing by one for each daily insertion of the driver card.",
					"daily_sequence_id":      "The daily sequence id for the log, incrementing by one for each log for the given daily presence counter.",
					"vehicle_reg_country":    "The 3-letter country code of the country the vehicle's registration number was issued by.",
					"vehicle_reg_number":     "The vehicle registration number uniquely identifying the vehicle within the issuing country.",
					"timestamp_ms":           "The timestamp in ms associated with the beginning of the time period covered by the tachograph log.",
					"manual_entry":           "Whether or not the entry constitutes a manual entry as made by the driver after inserting a driver card.",
					"duration_ms":            "The duration in ms of the time period covered by the tachograph log.",
					"state":                  "The tachograph state associated with the time period covered by the logs, such as driving or resting.",
					"is_primary":             "Boolean indicating the slot (primary or not) of the driver card inserted during the driver device assignment.",
					"is_crew":                "Boolean indicating the driving status of the log (single or crew) of the non-manual tachograph log.",
					"file_start_ms":          "The timestamp in ms of the first log in the driver file this log originates from.",
					"file_end_ms":            "The timestamp in ms of the last log in the driver file this log originates from.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSEURegion: {
							MaxWorkers: pointer.IntPtr(4),
						},
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"unassigned_tacho_trip_segments": {
				primaryKeys:       []string{"org_id", "device_id", "start_time"},
				PartitionStrategy: DateColumnFromTimestamp("start_time"),
				Description:       "A UnassignedTachoTripSegment represents a segment of a trip where driver card was not inserted in the tachograph.",
				ColumnDescriptions: map[string]string{
					"org_id":                        metadatahelpers.OrgIdDefaultDescription,
					"device_id":                     "The device id associated with the vehicle that the trip segment came from.",
					"vehicle_file_uploaded_at_ms":   "The uploaded at timestamp associated with the vehicle file from which trip segment was extracted.",
					"start_time":                    "Start time of the unassigned segment.",
					"end_time":                      "End time of the unassigned segment.",
					"is_derived_from_activity_info": "Whether the unassigned segment is derived from ActivityChangeInfo.",
					"annotation":                    "User annotation for the unassigned trip segment.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"vehicle_file_downloads": {
				primaryKeys:       []string{"org_id", "vehicle_id_number", "uploaded_at_ms"},
				PartitionStrategy: DateColumnFromMilliseconds("uploaded_at_ms"),
				Description:       "A VehicleFileDownload represents an individual vehicle file download as well as the corresponding object stat.",
				ColumnDescriptions: map[string]string{
					"org_id":             metadatahelpers.OrgIdDefaultDescription,
					"uploaded_at_ms":     "The uploaded at timestamp associated with the vehicle file.",
					"device_id":          "The device id associated with the vehicle file if it came from an object stat.",
					"s3_key":             "S3 Key corresponding to file in bucket.",
					"uuid":               "UUID of the file.",
					"created_at":         "Time that the download was created.",
					"vehicle_id_number":  "The vehicle identification number (VIN) that uniquely identifies the vehicle that the file came from.",
					"vehicle_reg_number": "The vehicle registration number uniquely identifying the vehicle within the issuing country/state/province.",
				},
				Tags: []amundsentags.Tag{
					amundsentags.TachographTag,
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.TimecardsDB,
		schemaPath: "go/src/samsaradev.io/fleet/sled/timecards/timecardsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"job_logs": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromTimestamp("created_at"),
				Description:                       "A job log represents an entry recording a job performed by a user.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"job_types": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "A job type represents a configured job type that can be used to label job logs.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"organization_settings": {
				primaryKeys: []string{"org_id"},
				ProtoSchema: ProtobufSchema{
					"settings": {
						Message:   &timecardsmodels.Settings{},
						ProtoPath: "go/src/samsaradev.io/fleet/sled/timecards/timecardsmodels/settings.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				Description:                       "Stores the timecard-related settings of an organization.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.TrainingDB,
		schemaPath: "go/src/samsaradev.io/training/trainingmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"avatars": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Custom Avatars for generating AI avatar videos for that are trained and consented by the user.",
				ColumnDescriptions: map[string]string{
					"org_id":        "The organization that this avatar belongs to.",
					"uuid":          "UUID of the avatar. Also used as the MSP asset.",
					"name":          "Name of the avatar.",
					"is_default":    "Whether this is the default avatar for the organization, ie in AI Course Builder.",
					"vendor":        "The vendor that the avatar can be used to generate AI avatar videos with. Tavus, Synthesia, Heygen.",
					"external_id":   "The ID of the avatar from the vendor, used to make AI avatar generation calls.",
					"created_at_ms": "The time in milliseconds when the avatar was created.",
					"updated_at_ms": "The time in milliseconds when the avatar was updated.",
					"deleted_at_ms": "The time in milliseconds when the avatar was deleted.",
					"created_by":    "ID corresponding to the user that created the avatar.",
					"updated_by":    "ID corresponding to the user that updated the avatar.",
					"deleted_by":    "ID corresponding to the user that deleted the avatar.",
				},
			},
			"categories": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Category groupings for courses that give structure to the training library.",
				ColumnDescriptions: map[string]string{
					"org_id":               "The organization that this category belongs to.",
					"uuid":                 "UUID of the category.",
					"global_category_uuid": "UUID for the global, Samsara-controlled categories; only present if this category maps to a global category (e.g., Driver App and Safety categories).",
					"label":                "Human-readable label for the category (e.g., Safety).",
					"last_updated_at_ms":   "Time in milliseconds when the category was last updated.",
					"last_updated_by":      "ID corresponding to the user that last updated the category.",
					"deleted_at_ms":        "Time in milliseconds when the category was deleted.",
					"deleted_by":           "ID corresponding to the user that deleted the category.",
				},
			},
			"course_behavior_mappings": {
				primaryKeys:       []string{"org_id", "behavior_type", "behavior_value", "course_uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Course behavior mapping maps a course to a learner behavior.",
				ColumnDescriptions: map[string]string{
					"org_id":         "The organization that this course behavior mapping belongs to.",
					"behavior_type":  "The behavior type that a course is being mapped to. Default is SAFETY.",
					"behavior_value": "The behavior value that a course is being mapped to.",
					"course_uuid":    "UUID of the associated course that is being mapped to a behavior.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"course_revisions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Course revision is a version of a Training product learning course. It is a child object of Course.",
				ColumnDescriptions: map[string]string{
					"org_id":                "The organization that this course revision belongs to.",
					"uuid":                  "A unique identifier of this course revision.",
					"course_uuid":           "UUID of the associated parent course object.",
					"global_revision_uuid":  "If not null, indicates it's a copied course, and value is the UUID the global course revision that this was copied from.",
					"rustici_version":       "Rustici's version number for this course revision.  Rustici version is auto incremented and read only, so we need to capture this and store on our end. Set internally after Rustici callback (based on data provided by Rustici).",
					"content_location":      "Source path of the content file for this course revision.",
					"content_type":          "The source type of the content file for this course revision, ie \"S3\".",
					"content_import_status": "Status of this course revision's file import process.",
					"created_at_ms":         "When this course revision was created on the server.",
					"updated_at_ms":         "When this course revision was updated on the server.",
					"deleted_at_ms":         "When this course revision was soft deleted on the server. Null if not deleted. This indicates whether this course revision is active.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"courses": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "Course is a parent object for a Training product learning course.",
				ColumnDescriptions: map[string]string{
					"org_id":                            "The organization that this course belongs to.",
					"uuid":                              "A unique identifier of this course. When creating a course in Rustici, we will use this for setting course id. This value is reused for the UUID of the associated form_template.",
					"global_course_uuid":                "If not null, indicates it's a copied course, and value is the UUID the global course that this was copied from.",
					"current_revision_uuid":             "UUID of the associated child course revision that is currently active for this course.",
					"title":                             "Title of this course, duplicate field from associated form_template.",
					"description":                       "Description of this course, duplicate field from associated form_template.",
					"thumbnail_s3_key":                  "Unique key corresponding to the thumbnail file of this course.",
					"thumbnail_upload_status":           "Status of this course's thumbnail upload process.",
					"estimated_duration_seconds":        "Estimated number of seconds required to complete this course.",
					"learning_standard":                 "The learning standard for this course. If imported, this is set internally after Rustici callback (based on data provided by Rustici).",
					"categories":                        "Delimited string of categories corresponding to this course.",
					"locales":                           "Delimited string of locales corresponding to this course.",
					"created_at_ms":                     "When this course was created on the server.",
					"updated_at_ms":                     "When this course was updated on the server.",
					"deleted_at_ms":                     "When this course was soft deleted on the server. Null if not deleted. This indicates whether this course is active.",
					"feature_config":                    "Feature config corresponding to this global course.",
					"curr_draft_template_revision_uuid": "UUID of the current draft template revision associated with this course.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_sync_status": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "OrgSyncStatus track global course sync job status for each org.",
				ColumnDescriptions: map[string]string{
					"org_id":           "Org for which the sync status is being tracked.",
					"version_id":       "Last successful version that was synced to the org. -1 if there was never a successful sync.",
					"status":           metadatahelpers.EnumDescription("Status of the last sync job run for this org.", trainingproto.JobStatus_name),
					"last_success_ms":  "When was the last sync was successful for this org.",
					"last_run_ms":      "When was the last time sync job ran for this org - irrespective of success.",
					"errored_attempts": "Incremental counter for failed sync attempts. Successful sync resets this to 0.",
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.Trips2DB,
		schemaPath: "go/src/samsaradev.io/stats/trips/trips2models/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// Trips receives consistent writes across all shards in all regions.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: usShardMonitors(ShardMonitor{TimeframeMinutes: 120}),
			infraconsts.SamsaraAWSEURegion:      shard0Monitor(ShardMonitor{TimeframeMinutes: 120}),
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"device_trip_settings": {
				primaryKeys: []string{"org_id", "device_id", "version"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &tripsproto.TripSettings{},
						ProtoPath: "go/src/samsaradev.io/stats/trips/tripsproto/trips.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"org_trip_settings": {
				primaryKeys: []string{"org_id", "version"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &tripsproto.TripSettings{},
						ProtoPath: "go/src/samsaradev.io/stats/trips/tripsproto/trips.proto",
					},
				},
				PartitionStrategy:                 SinglePartition,
				versionInfo:                       SingleVersion(1),
				cdcVersion_DO_NOT_SET:             TableVersion(1),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"trips": {
				primaryKeys: []string{"org_id", "device_id", "version", "start_ms"},
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &tripsproto.Trip{},
						ProtoPath: "go/src/samsaradev.io/stats/trips/tripsproto/trips.proto",
					},
				},
				PartitionStrategy:     DateColumnFromMilliseconds("start_ms"),
				Production:            true,
				customCombineShards:   true,
				versionInfo:           SingleVersion(4),
				cdcVersion_DO_NOT_SET: TableVersion(4),
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							MaxWorkers: pointer.IntPtr(24),
						},
						infraconsts.SamsaraAWSEURegion: {
							MaxWorkers: pointer.IntPtr(8),
						},
					},
				},
				Description: `Trips are segments of device activity in a specific time range.

* A location based trip's time range is determined based on location data. It begins when a vehicle achieves a speed of at least 5mph, and ends when the vehicle either remains below 5mph for more than 5 minutes or when the vehicle crosses a state or national border.
* An engine based trip's time range is determined based on osDEquipmentActivity data. It begins when ON state is reported (after preceding OFF states), and ends when OFF state is reported (after preceding ON states).

**NOTE**
- When querying trips, be sure to filter on version to fetch only valid trips. **Use version=101 by default.**
- Engine based trips are currently only available for Heavy Equipment (HE) devices and their usage should be limited ONLY to HE enabled features/reports.
- Trailers have their own separate trips from vehicles pulling them. You might want to filter them out (e.g. not to count doubled trips in some cases).
				`,
				ColumnDescriptions: map[string]string{
					"org_id":                            metadatahelpers.OrgIdDefaultDescription,
					"device_id":                         metadatahelpers.DeviceIdDefaultDescription,
					"driver_id":                         metadatahelpers.DriverIdDefaultDescription,
					"end_ms":                            "The timestamp (in milliseconds) marking the end of the trip.",
					"proto.end.address":                 metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.address.id":              metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.address.name":            metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.address.address":         metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.address.latitude":        metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.address.longitude":       metadatahelpers.DeprecatedDefaultDescription,
					"proto.end.place":                   "Ending location information from reverse geocoding.",
					"proto.end.place.street":            "Street of the ending location.",
					"proto.end.place.city":              "City of the ending location.",
					"proto.end.place.state":             "State/Province of the ending location.",
					"proto.end.place.house_number":      "Street number of the ending location.",
					"proto.end.place.poi":               "Point of interest at the ending location.",
					"proto.end.place.neighborhood":      "Neighborhood name of the ending location.",
					"proto.end.place.postcode":          "Postal/zip code of the ending location.",
					"proto.end.place.country":           "Country of the ending location (EU only).",
					"proto.hos_unassigned.org_id":       "Prefer using base level org_id; it is guaranteed to be populated and accurate.",
					"proto.driver_face_id.org_id":       "Prefer using base level org_id; it is guaranteed to be populated and accurate.",
					"proto.org_id":                      "Prefer using base level org_id; it is guaranteed to be populated and accurate.",
					"proto.prev_end.address":            metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.address.id":         metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.address.name":       metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.address.address":    metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.address.latitude":   metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.address.longitude":  metadatahelpers.DeprecatedDefaultDescription,
					"proto.prev_end.place":              "Location information of the previous trip's end point, retrieved from reverse geocoding.",
					"proto.prev_end.place.street":       "Street of the previous trip's end point.",
					"proto.prev_end.place.city":         "City of the previous trip's end point.",
					"proto.prev_end.place.state":        "State/Province of the previous trip's end point.",
					"proto.prev_end.place.house_number": "Street number of the previous trip's end point.",
					"proto.prev_end.place.poi":          "Point of interest of the previous trip's end point.",
					"proto.prev_end.place.neighborhood": "Neighborhood name of the previous trip's end point.",
					"proto.prev_end.place.postcode":     "Postal/zip code of the previous trip's end point.",
					"proto.prev_end.place.country":      "Country of the previous trip's end point (EU only).",
					"proto.start.address":               metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.address.id":            metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.address.name":          metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.address.address":       metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.address.latitude":      metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.address.longitude":     metadatahelpers.DeprecatedDefaultDescription,
					"proto.start.place":                 "Starting location information from reverse geocoding.",
					"proto.start.place.street":          "Street of the starting location.",
					"proto.start.place.city":            "City of the starting location.",
					"proto.start.place.state":           "State/Province of the starting location.",
					"proto.start.place.house_number":    "Street number of the starting location.",
					"proto.start.place.poi":             "Point of interest at the starting location.",
					"proto.start.place.neighborhood":    "Neighborhood name of the starting location.",
					"proto.start.place.postcode":        "Postal/zip code of the starting location.",
					"proto.start.place.country":         "Country of the starting location (EU only).",
					"start_ms":                          "The timestamp (in milliseconds) marking the start of the trip.",
					"version": `Indicates version of a trip in two ways:
* Version of a trip computation algorithm that would otherwise be breaking changes (x01, x02, etc., e.g. 101),
* Type of trip (1xx - LocationBasedTrip, 2xx - EngineBasedTrip, etc., e.g. 101, 201). Type of trip is also explicitly set in proto.trip_type.

Trips can be also built as "test trips" with negative versions (e.g. -1). They should be filtered out of most results.`,
					"proto.trip_type": metadatahelpers.EnumDescription("Type of trip.", tripsproto.TripType_name),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.TripsDB,
		schemaPath: "go/src/samsaradev.io/stats/trips/tripsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		// This is the old trips database and it doesn't receive much writes so we won't monitor it.
		CdcThroughputMonitors: map[string]ShardMonitors{
			infraconsts.SamsaraAWSDefaultRegion: {},
			infraconsts.SamsaraAWSEURegion:      {},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"trip_purpose_assignments": {
				primaryKeys: []string{"org_id", "device_id", "assignment_start_ms"},
				ProtoSchema: ProtobufSchema{
					"edit_history": {
						Message:   &tripsproto.EditHistory{},
						ProtoPath: "go/src/samsaradev.io/stats/trips/tripsproto/trip_purpose.proto",
					},
				},
				PartitionStrategy:                 DateColumnFromMilliseconds("assignment_start_ms"),
				Production:                        true,
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.UserOrgPreferencesDB,
		schemaPath: "go/src/samsaradev.io/platform/userorgpreferences/userorgpreferencesmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"user_org_preferences": {
				primaryKeys:                       []string{"org_id", "user_id", "key_enum"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.UserPreferencesDB,
		schemaPath: "go/src/samsaradev.io/userpreferences/userpreferencesmodels/db_schema.sql",
		Sharded:    false,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"fleet_list_customizations": {
				primaryKeys:                       []string{"org_id", "user_id"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"onboarding_progresses": {
				primaryKeys:       []string{"user_id"},
				PartitionStrategy: SinglePartition,
				allowedRegions:    []string{infraconsts.SamsaraAWSDefaultRegion},
				ProtoSchema: ProtobufSchema{
					"progress": {
						Message:   &userpreferencesproto.OnboardingProgress{},
						ProtoPath: "go/src/samsaradev.io/userpreferences/userpreferencesproto/onboardingprogress.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
	},
	{
		name:       dbregistry.VinDB,
		schemaPath: "go/src/samsaradev.io/fleet/vin/vinmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"device_vin_metadata": {
				primaryKeys:       []string{"org_id", "device_id"},
				PartitionStrategy: SinglePartition,
				DataModelTable:    true,
				ColumnDescriptions: map[string]string{
					"org_id":                      metadatahelpers.OrgIdDefaultDescription,
					"device_id":                   metadatahelpers.DeviceIdDefaultDescription,
					"cable_id":                    "The cable id reported by the device.",
					"product_id":                  "The product ID of the device.",
					"vin":                         "The vin of the vehicle.",
					"is_ecm_vin":                  "Whether this VIN is reported by the ECM or not.  If not, it's a manually entered VIN.",
					"make":                        "The make of the vehicle (from NHTSA).",
					"model":                       "The model of the vehicle (from NTHSA).",
					"year":                        "The year of the vehicle (from NHTSA).",
					"engine_model":                "The engine model of the vehicle (from NHTSA).",
					"electrification_level":       "The electrification level of the vehicle (from NHTSA).",
					"engine_manufacturer":         "The engine manufacturer of the vehicle (from NHTSA).",
					"engine_displacement_cc":      "The engine displacement in cc of the vehicle (from NTHSA).",
					"gross_vehicle_weight_rating": "The gross vehicle weight rating of the vehicle (from NHTSA).",
					"max_weight_lbs":              "The max weight in lbs of the vehicle (from NHTSA).",
					"plant_information":           "The plant that the vehicle was manufactured at (from NHTSA).",
					"primary_fuel_type":           "The primary fuel type of the vehicle (from NHTSA).",
					"secondary_fuel_type":         "The secondary fuel type of the vehicle (from NHTSA).",
					"updated_at":                  "The timestamp when this entry was last updated.",
					"decoded_at":                  "The timestamp when this device's VIN was last decoded.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(1),
				DisableServerless:                 true,
			},
			"device_vin_metadata_overrides": {
				primaryKeys:       []string{"org_id", "device_id", "vin"},
				PartitionStrategy: SinglePartition,
				ColumnDescriptions: map[string]string{
					"org_id":                 metadatahelpers.OrgIdDefaultDescription,
					"device_id":              metadatahelpers.DeviceIdDefaultDescription,
					"vin":                    "The vin of the vehicle.",
					"make":                   "The make of the vehicle (user specified).",
					"model":                  "The model of the vehicle (user specified).",
					"year":                   "The year of the vehicle (user specified).",
					"primary_fuel_type":      "The primary fuel type of the vehicle (user specified).",
					"secondary_fuel_type":    "The secondary fuel type of the vehicle (user specified).",
					"electrification_level":  "The electrification level of the vehicle (user specified).",
					"plant_information":      "The plant that the vehicle was manufactured at (user specified).",
					"engine_model":           "The engine model of the vehicle (user specified).",
					"engine_manufacturer":    "The engine manufacturer of the vehicle (user specified).",
					"engine_displacement_cc": "The engine displacement in cc of the vehicle (user specified).",
					"max_weight_lbs":         "The max weight in lbs of the vehicle (user specified).",
					"updated_at":             "The timestamp when this entry was last updated.",
					"asset_subtype":          "The asset subtype representing different types of asset body / shape.",
					"country":                "The country that the vehicle operates in for diagnostic signal targeting (user specified).",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
				versionInfo:                       SingleVersion(1),
				DisableServerless:                 true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.WorkflowsDB,
		schemaPath: "go/src/samsaradev.io/platform/workflows/workflowsmodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"actions": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto_data": {
						Message:   &workflowsproto.ActionConfig{},
						ProtoPath: "go/src/samsaradev.io/platform/workflows/workflowsproto/action.proto",
					},
				},
				Description:                       "This table contains all action associated with a workflow configuration.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
			},
			"alert_migration_statuses": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains information associated with org migration status to workflows.",
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"configs_migrated_at":   "The timestamp at which alert configs were migrated to workflow configs for the org.",
					"processing_visible_at": "The timestamp at which migrated workflow configs could start generating incidents viewable for the org.",
					"workflows_enabled_at":  "The timestamp at which the org opted into workflows.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"integrated_ux_migration_statuses": {
				primaryKeys:       []string{"org_id"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains information associated with integrated ux migration status.",
				ColumnDescriptions: map[string]string{
					"org_id":                metadatahelpers.OrgIdDefaultDescription,
					"configs_migrated_at":   "The timestamp at which alert configs were migrated to workflow configs for the org.",
					"processing_visible_at": "The timestamp at which migrated workflow configs could start generating incidents viewable for the org.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"notification_role_recipients": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				Description:       "This table contains information associated with notification role recipients configured on a workflow config.",
				ColumnDescriptions: map[string]string{
					"uuid":             "The distinct identifier for a notification role recipient row.",
					"org_id":           metadatahelpers.OrgIdDefaultDescription,
					"action_uuid":      "The uuid of the action configuration that this role recipient is configured on.",
					"role_id":          "The built-in role id that is configured.",
					"custom_role_uuid": "The custom role uuid that is configured.",
					"has_text":         "Determines if role recipient should receive text notifications.",
					"has_push":         "Determines if role recipient should receive push notifications.",
					"has_email":        "Determines if role recipient should receive email notifications.",
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"trigger_targets": {
				primaryKeys:                       []string{"org_id", "trigger_uuid", "object_id", "object_type"},
				PartitionStrategy:                 SinglePartition,
				Description:                       "This table contains all the trigger targets associated with a workflow trigger.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
			},
			"triggers": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto_data": {
						Message:   &workflowsproto.TriggerProto{},
						ProtoPath: "go/src/samsaradev.io/platform/workflows/workflowsproto/trigger.proto",
					},
				},
				Description:                       "This table contains all the trigger information associated with a workflow configuration.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
			},
			"workflow_configs": {
				primaryKeys:       []string{"org_id", "uuid"},
				PartitionStrategy: SinglePartition,
				ProtoSchema: ProtobufSchema{
					"proto_data": {
						Message:   &workflowsproto.WorkflowProto{},
						ProtoPath: "go/src/samsaradev.io/platform/workflows/workflowsproto/workflow.proto",
					},
				},
				Description:                       "This table contains all the workflow configurations used to match workflow incidents.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
			},
			"workflow_incidents": {
				primaryKeys:       []string{"org_id", "workflow_id", "occurred_at_ms", "object_ids"},
				PartitionStrategy: DateColumnFromMilliseconds("occurred_at_ms"),
				ProtoSchema: ProtobufSchema{
					"proto": {
						Message:   &workflowsproto.IncidentProto{},
						ProtoPath: "go/src/samsaradev.io/platform/workflows/workflowsproto/workflows_server.proto",
					},
				},
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				Description:                       "This table contains all the workflow incidents that match a certain workflow configuration.",
				EmptyStringsCastToNull_DO_NOT_SET: true,
				DataModelTable:                    true,
				InternalOverrides: RdsTableInternalOverrides{
					RegionToJobOverrides: map[string]RdsJobOverrides{
						infraconsts.SamsaraAWSDefaultRegion: {
							// Disable serverless for this table because it frequently OOMs and we have no way of fixing it via the UI
							// when the job is set to serverless.
							DisableServerless: true,
						},
					},
				},
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		name:       dbregistry.WorkforceVideoDB,
		schemaPath: "go/src/samsaradev.io/connectedworker/workforce/workforcevideomodels/db_schema.sql",
		Sharded:    true,
		InternalOverrides: RdsDatabaseInternalOverrides{
			ServerlessConfig: &databricks.ServerlessConfig{
				PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			},
		},
		Tables: map[string]tableDefinition{ // lint: +sorted
			"areas_of_interest": {
				primaryKeys:           []string{"org_id", "uuid"},
				PartitionStrategy:     SinglePartition,
				allowedRegions:        []string{infraconsts.SamsaraAWSDefaultRegion},
				versionInfo:           SingleVersion(1),
				cdcVersion_DO_NOT_SET: TableVersion(1),
				ProtoSchema: ProtobufSchema{
					"polygon": {
						Message:   &workforcevideomodels.Polygon{},
						ProtoPath: "go/src/samsaradev.io/connectedworker/workforce/workforcevideomodels/types.proto",
					},
				},
				ColumnDescriptions: map[string]string{
					"uuid":                  "Unique identifier for the Area of Interest, within its organization; primary key = (org_id, uuid)",
					"name":                  "Name of the Area of Interest",
					"stream_id":             "The ID of the CameraStream, on which this Area of Interest was drawn.",
					"org_id":                "Organization for this Area of Interest",
					"polygon":               "The vertices representing the Area's drawn polygon shape.",
					"polygon_type":          metadatahelpers.EnumDescription("The current polygon 'type' (legacy 10x10 rectangles, or arbitrary shapes)", workforcevideomodels.PolygonType_name),
					"original_polygon_type": metadatahelpers.EnumDescription("The original polygon 'type', upon initial AOI creation", workforcevideomodels.PolygonType_name),
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"camera_devices": {
				primaryKeys:                       []string{"org_id", "id"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"camera_streams": {
				primaryKeys:       []string{"org_id", "id"},
				PartitionStrategy: SinglePartition,
				allowedRegions:    []string{infraconsts.SamsaraAWSDefaultRegion},
				ProtoSchema: ProtobufSchema{ // Name to proto.Message mapping for all protobuf columns
					"extra_info": {
						Message:   &workforcevideomodels.CameraStreamExtraInfo{},
						ProtoPath: "go/src/samsaradev.io/connectedworker/workforce/workforcevideomodels/camera_stream_extra.proto",
					},
				},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"library_video_info": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromMilliseconds("start_ms"),
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"media_sources": {
				Description:                       "Assocations between {Stream, Channel} sources to a representative UUID.",
				primaryKeys:                       []string{"org_id", "stream_id", "channel_id"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"media_upload_recovery": {
				Description:       "Media storage recovery state for media sources.",
				primaryKeys:       []string{"org_id", "media_source_uuid"},
				PartitionStrategy: SinglePartition,
				allowedRegions:    []string{infraconsts.SamsaraAWSDefaultRegion},
			},
			"object_detection_feedback": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromMilliseconds("video_ms"),
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				versionInfo:                       SingleVersion(2),
				cdcVersion_DO_NOT_SET:             TableVersion(2),
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"scene_metadata": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"sites": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"sites_camera_devices": {
				primaryKeys:                       []string{"org_id", "site_uuid", "camera_device_id"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"sites_floor_plans": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"video_segments": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 DateColumnFromMilliseconds("start_ms"),
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"views": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
			"views_streams": {
				primaryKeys:                       []string{"org_id", "uuid"},
				PartitionStrategy:                 SinglePartition,
				allowedRegions:                    []string{infraconsts.SamsaraAWSDefaultRegion},
				EmptyStringsCastToNull_DO_NOT_SET: true,
			},
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
}

func (db *databaseDefinition) registryTables(productionOnly bool) ([]*Table, error) {
	rdsDbNames, err := dbregistry.GetRdsDbNames(db.name, db.Sharded)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to get rds names for %s", db.name)
	}

	var tables []*Table
	for tablename, unregTable := range db.Tables {
		for _, rdsDb := range rdsDbNames {
			t := Table{
				DbName:                            db.Name(),
				RdsDb:                             rdsDb,
				RdsPasswordKey:                    db.PasswordKey(),
				MySqlDb:                           db.MySqlDb(),
				TableName:                         tablename,
				PrimaryKeys:                       unregTable.primaryKeys,
				PartitionStrategy:                 unregTable.PartitionStrategy,
				ProtoSchema:                       unregTable.ProtoSchema,
				VersionInfo:                       unregTable.versionInfo,
				AllowedRegions:                    unregTable.allowedRegions,
				DBSharded:                         db.Sharded,
				CustomCombineShardsSelect:         unregTable.customCombineShards,
				Production:                        unregTable.Production,
				Description:                       unregTable.Description,
				ColumnDescriptions:                unregTable.ColumnDescriptions,
				Tags:                              unregTable.Tags,
				EmptyStringsCastToNull_DO_NOT_SET: unregTable.EmptyStringsCastToNull_DO_NOT_SET,
				ExcludeColumns:                    unregTable.ExcludeColumns,
				ServerlessConfig:                  unregTable.ServerlessConfig,
				DisableServerless:                 unregTable.DisableServerless,
				PostgreSchemaName:                 unregTable.InternalOverrides.DatabaseSchemaOverride,
			}
			if !productionOnly {
				tables = append(tables, &t)
			} else if productionOnly && t.Production {
				tables = append(tables, &t)
			}

			// If the table doesn't have a serverless config, use the database level config.
			if t.ServerlessConfig == nil {
				t.ServerlessConfig = db.InternalOverrides.ServerlessConfig
			}

			if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				if unregTable.InternalOverrides.DatabaseSchemaOverride == "" {
					unregTable.InternalOverrides.DatabaseSchemaOverride = "public"
				}
				t.PostgreSchemaName = unregTable.InternalOverrides.DatabaseSchemaOverride
				t.TableName = unregTable.InternalOverrides.DatabaseSchemaOverride + "__" + t.TableName
			}

		}
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].S3PathName() < tables[j].S3PathName()
	})
	return tables, nil
}

func registryTables(databases []databaseDefinition, productionOnly bool) []*Table {
	var tables []*Table
	for _, db := range databases {
		dbTables, err := db.registryTables(productionOnly)
		if err != nil {
			panic(oops.Wrapf(err, "unable to register db %s", db.name))
		}
		tables = append(tables, dbTables...)
	}
	return tables
}

// == V2 of exported registry models ==
//
// Currently, we had an interface that exported an array of []Table objects that
// had denormalized the database info in them.
// This interface was sometimes confusing to work with, and so this is a new interface
// that will expose database objects, and within them table objects.
// Any shared information (e.g. rds name, mysql db name, etc). will be
// at the top level db, with table specific info inside each table.
type RegistryDatabase struct {
	// This is the registry name of the database, from dbregistry.
	// e.g. dbregistry.Workflowsdb (workflows)
	RegistryName string

	// This is the name that we have historically used for the db,
	// which is the registry name + 'db'
	// e.g. workflowsdb
	Name string

	// This is the name of the actual database in MySql. This is
	// almost always the same as the Name field above, but in some
	// rare cases will be something different.
	MySqlDb string

	// Map from region to the existing shards. These will look like prod-workflowsdb,
	// prod-workflow-shard-1db, etc.
	// For unsharded dbs there will be only 1 entry per region, like prod-workflowsdb.
	RegionToShards map[string][]string
	Sharded        bool

	// Key for the password for the RDS table.
	RdsPasswordKey string

	// Absolute path to the schema file (usually something ending in db_schema.sql)
	SchemaPath string

	// Cdcthroughput monitors.
	CdcThroughputMonitors map[string]ShardMonitors

	// Dataplatform overrides
	InternalOverrides RdsDatabaseInternalOverrides

	// Mapping of table names to their configuration.
	// Table names are exactly as they would be in mysql
	// e.g. `organizations` or `devices`.
	Tables map[string]RegistryTable

	// Override for the database to be read only by the given groups.
	// If this is set, the database will be read only by the given groups.
	// If this is not set, the database will be read by all teams with DBX clusters.
	CanReadGroups []components.TeamInfo

	// Skip the database from being synced to HMS.
	SkipSyncToHms bool

	// CanReadBiztechGroups is a list of biztech groups that can read the table in the default catalog
	// These groups are not managed by dataplatform like R&D teams in CanReadGroups, but are managed by the biztech team
	// and represent Non-R&D teams that use the biztech workspace.
	// The key is the group name and the value is the list of regions the group has access to.
	CanReadBiztechGroups map[string][]string
}

// Specify the number of minutes in which you would definitely expect there to be at least 1
// insertion/update/deletion to this shard. We'll use this to generate monitors
// that will alert dataplatform when this condition isn't met, which has historically
// indicated Amazon DMS problems.
// Minimum value is 2 hours, as we may need to occasionally pause tasks for maintenance reasons.
type ShardMonitor struct {
	TimeframeMinutes int64
}

type ShardMonitors map[shardconsts.ShardNumber]ShardMonitor

func (db *databaseDefinition) registryDatabase() (RegistryDatabase, error) {
	// Get the shards
	shards := map[string][]string{
		infraconsts.SamsaraAWSDefaultRegion: {},
		infraconsts.SamsaraAWSEURegion:      {},
		infraconsts.SamsaraAWSCARegion:      {},
	}

	tables := make(map[string]RegistryTable)
	rdsDbNames, err := dbregistry.GetRdsDbNames(db.name, db.Sharded)
	if err != nil {
		return RegistryDatabase{}, oops.Wrapf(err, "unable to get rds names for %s", db.name)
	}

	// The list of db names is sorted, but this puts shard 0 at the end because for example, prod-compliance-shard-Ndb comes
	// before prod-compliancedb. This makes it confusing to use this value, since the index -> shard mapping is off.
	// We sort it in correct order before saving it.
	for region, _ := range shards {
		for _, shard := range rdsDbNames {
			if dbregistry.ShardInRegion(shard, region) {
				shards[region] = append(shards[region], shard)
			}
		}
		SortShards(shards[region])
	}

	for tableName, spec := range db.Tables {

		sc := spec.ServerlessConfig
		// If the table doesn't have a serverless config, use the database level config.
		if sc == nil {
			sc = db.InternalOverrides.ServerlessConfig
		}

		// If the database engine is Postgres, we need to override the table name to include the schema name.
		if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
			if spec.InternalOverrides.DatabaseSchemaOverride == "" {
				spec.InternalOverrides.DatabaseSchemaOverride = "public"
			}
			tableName = spec.InternalOverrides.DatabaseSchemaOverride + "__" + tableName // __ is used to separate the schema name from the table name
		}

		tables[tableName] = RegistryTable{
			TableName:                         tableName,
			VersionInfo:                       spec.versionInfo,
			ProtoSchema:                       spec.ProtoSchema,
			PartitionStrategy:                 spec.PartitionStrategy,
			PrimaryKeys:                       spec.primaryKeys,
			CustomCombineShardsSelect:         spec.customCombineShards,
			Production:                        spec.Production,
			DataModelTable:                    spec.DataModelTable,
			description:                       spec.Description,
			ColumnDescriptions:                spec.ColumnDescriptions,
			Tags:                              spec.Tags,
			AllowedRegions:                    spec.allowedRegions,
			InternalOverrides:                 spec.InternalOverrides,
			EmptyStringsCastToNull_DO_NOT_SET: spec.EmptyStringsCastToNull_DO_NOT_SET,
			CdcVersion_DO_NOT_SET:             spec.cdcVersion_DO_NOT_SET,
			ExcludeColumns:                    spec.ExcludeColumns,
			ServerlessConfig:                  sc,
			DisableServerless:                 spec.DisableServerless,
			PartitionKeyAttribute:             spec.PartitionKeyAttribute,
		}

	}
	return RegistryDatabase{
		RegistryName:          db.name,
		Name:                  db.Name(),
		MySqlDb:               db.MySqlDb(),
		RdsPasswordKey:        db.PasswordKey(),
		RegionToShards:        shards,
		Sharded:               db.Sharded,
		Tables:                tables,
		SchemaPath:            db.SchemaPath(),
		InternalOverrides:     db.InternalOverrides,
		CdcThroughputMonitors: db.CdcThroughputMonitors,
		CanReadGroups:         db.CanReadGroups,
		SkipSyncToHms:         db.SkipSyncToHms,
		CanReadBiztechGroups:  db.CanReadBiztechGroups,
	}, nil
}

func usShardMonitors(t ShardMonitor) ShardMonitors {
	return map[shardconsts.ShardNumber]ShardMonitor{
		shardconsts.Shard0: t,
		shardconsts.Shard1: t,
		shardconsts.Shard2: t,
		shardconsts.Shard3: t,
		shardconsts.Shard4: t,
		shardconsts.Shard5: t,
	}
}

func usNonProdShardMonitors(t ShardMonitor) ShardMonitors {
	return map[shardconsts.ShardNumber]ShardMonitor{
		shardconsts.Shard1: t,
		shardconsts.Shard2: t,
		shardconsts.Shard3: t,
		shardconsts.Shard4: t,
		shardconsts.Shard5: t,
	}
}

func euShardMonitors(t ShardMonitor) ShardMonitors {
	return map[shardconsts.ShardNumber]ShardMonitor{
		shardconsts.Shard0: t,
		shardconsts.Shard1: t,
	}
}

func shard0Monitor(t ShardMonitor) ShardMonitors {
	return map[shardconsts.ShardNumber]ShardMonitor{
		shardconsts.Shard0: t,
	}
}

type RegistryTable struct {
	TableName                 string
	VersionInfo               TableVersionInfo
	ProtoSchema               ProtobufSchema
	PartitionStrategy         PartitionStrategy
	PrimaryKeys               []string
	CustomCombineShardsSelect bool
	Production                bool
	// DataModelTable indicates whether if a non-prod table is consumed in downstream data model pipeline or equivalent.
	// Setting this will result in the table getting a smaller SLO window and faster replication (6H) over non-production tables (12h).
	// The priority of the tables is as follows: Production (3h freshness) > DataModelTable (6h freshness) > NonProduction (12h freshness).
	DataModelTable     bool
	description        string
	ColumnDescriptions map[string]string
	Tags               []amundsentags.Tag
	AllowedRegions     []string
	InternalOverrides  RdsTableInternalOverrides

	// EmptyStringsCastToNull_DO_NOT_SET indicates whether empty strings should be overwritten with null to maintain parity
	// with the previous replication system. This should only be set for tables that existed in the old replication pipeline.
	// Do not set this field for newly added tables.
	EmptyStringsCastToNull_DO_NOT_SET bool

	// The pinned version for the CDC task which should never change. For any new tables,
	// the version is automatically set to 0 and this field should not be set.
	CdcVersion_DO_NOT_SET TableVersion

	// List of column names to be excluded from replication to the data lake.
	ExcludeColumns []string

	// UseServerlessCompute indicates whether to use serverless compute for the table.
	ServerlessConfig *databricks.ServerlessConfig

	// DisableServerless indicates whether to disable serverless compute for the table.
	DisableServerless bool

	// PartitionKeyAttribute specifies the column to use as the Kinesis partition key
	// for DMS CDC records. When set, DMS uses an object-mapping rule to partition
	// records by this column's value.
	//
	// This ensures that records with the same partition key are processed in order.
	// Note: There's no strict guarantee records go to a single shard, as shards can
	// split or merge. However, a well-implemented KCL consumer should use the shard
	// lineage features to avoid processing records out of order even during resharding.
	PartitionKeyAttribute string
}

func (tb *RegistryTable) GetReplicationFrequencyHour() string {
	if tb.Production {
		return "3"
	} else if tb.DataModelTable {
		return "6"
	} else {
		// Non Production Table.
		return "12"
	}
}

var getAllRegistryDatabases = sync.OnceValues(func() ([]RegistryDatabase, error) {
	var dbs []RegistryDatabase

	for _, db := range databaseDefinitions {
		registryDb, err := db.registryDatabase()
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get registry DB for %s", db.name)
		}

		dbs = append(dbs, registryDb)

	}

	return dbs, nil
})

func (d *RegistryDatabase) IsInRegion(region string) bool {
	hasTableInRegion := false
	for _, table := range d.Tables {
		if table.IsInRegion(region) {
			hasTableInRegion = true
			break
		}
	}

	return hasTableInRegion
}

func (d *RegistryDatabase) TablesInRegion(region string) []RegistryTable {
	var tables []RegistryTable
	for _, table := range d.Tables {
		if table.IsInRegion(region) {
			tables = append(tables, table)
		}
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})
	return tables
}

func (d *RegistryDatabase) HasProductionTable() bool {
	for _, table := range d.Tables {
		if table.Production {
			return true
		}
	}
	return false
}

func (d *RegistryDatabase) DatabaseS3Path(shard string) string {
	pathComponents := []string{shard, d.MySqlDb}
	return strings.Join(pathComponents, "/")
}

func (d *RegistryDatabase) TableS3PathName(shard string, tableName string) string {
	pathComponents := []string{d.DatabaseS3Path(shard), tableName}
	return strings.Join(pathComponents, "/")
}

func (d *RegistryDatabase) TableS3PathNameWithVersion(shard string, tableName string, version TableVersion) string {
	return fmt.Sprintf("%s_%s", d.TableS3PathName(shard, tableName), version.Name())
}

func (d *RegistryDatabase) QueueName(shard string, tableName string, version TableVersion) string {
	queueSuffix := strings.ToLower(strings.Join([]string{shard, d.MySqlDb, tableName, version.Name()}, "_"))
	return fmt.Sprintf("samsara_delta_lake_rds_%s", queueSuffix)
}

func (d *RegistryDatabase) GetAllColumnDescriptions(table RegistryTable) map[string]string {
	columnDescriptions := make(map[string]string)
	for col, description := range defaultColumns {
		columnDescriptions[col] = description
	}

	for col, description := range table.ColumnDescriptions {
		columnDescriptions[col] = description
	}

	if d.Sharded {
		for col, description := range shardedDBExtraDefaultColumns {
			columnDescriptions[col] = description
		}
	}

	if table.PartitionStrategy != SinglePartition {
		columnDescriptions[table.PartitionStrategy.Key] = fmt.Sprintf("This partition column is derived from the %s column", table.PartitionStrategy.SourceColumnName)
	}

	return columnDescriptions
}

func (d *RegistryDatabase) IsProduction() bool {
	for _, spec := range d.Tables {
		if spec.Production {
			return true
		}
	}
	return false
}

func (t *RegistryTable) IsInRegion(region string) bool {
	if len(t.AllowedRegions) == 0 {
		return true
	}
	for _, elem := range t.AllowedRegions {
		if elem == region {
			return true
		}
	}
	return false
}

func (t *RegistryTable) SloConfig() dataplatformconsts.JobSlo {
	// Nonproduction jobs by default run every 12 hours. We page low urgency after 25,
	// and high urgency business after 37. We set high urgency
	// for 4 days so that we definitely get paged even during extended non-business-hours
	// once we start to approach the dms export retention window.
	sloConfig := dataplatformconsts.JobSlo{
		SloTargetHours:              36,
		Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
		LowUrgencyThresholdHours:    25,
		BusinessHoursThresholdHours: 37,
		HighUrgencyThresholdHours:   96,
	}

	// Create a separate SLO for Datamodel used tables. It is currently same as
	// the non-production tables, but once we have SLA with data tools team, we shall
	// populate the correct numbers for SLO here.
	if t.DataModelTable {
		sloConfig = dataplatformconsts.JobSlo{
			SloTargetHours:              24,
			Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
			LowUrgencyThresholdHours:    13,
			BusinessHoursThresholdHours: 24,
			HighUrgencyThresholdHours:   96,
		}
	}

	// For production jobs, we page high urgency after 2 failures.
	if t.Production {
		sloConfig = dataplatformconsts.JobSlo{
			SloTargetHours:            8,
			Urgency:                   dataplatformconsts.JobSloHigh,
			HighUrgencyThresholdHours: 8,
		}
	}

	return sloConfig
}

func (t *RegistryTable) BuildDescription() string {
	var parts []string
	if t.description != "" {
		parts = append(parts, t.description)
	}
	parts = append(parts, t.SloConfig().SloDescription())
	return strings.Join(parts, "\n\n")
}

func SortShards(shards []string) {
	// shards look like:
	// `prod-<dbname>db` for shard 0 or
	// `prod-<dbname>-shard-<num>db` for shards 1-N
	sort.Slice(shards, func(i, j int) bool {
		iParts := strings.Split(shards[i], "-")
		jParts := strings.Split(shards[j], "-")
		if len(iParts) == 2 {
			return true
		} else if len(jParts) == 2 {
			return false
		} else {
			return shards[i] < shards[j]
		}
	})
}

// GetSparkFriendlyRdsDBName gets a version of the rds db name that is compatible with Spark
// Ex: prod-workforcevideo-shard-1db --> workforcevideo_shard_1db
//
// shard0MainShard variable is a boolean that dictates whether you want the main shard name
// to be returned as it is in AWS RDS (i.e workforcevideodb) or you would like the main shard
// name to be returned with `shard_0` piped in (i.e workforcevideo_shard_0db)
func GetSparkFriendlyRdsDBName(prodDbName string, dbSharded bool, shard0MainShard bool) string {
	sparkFriendlyDBName := strings.Replace(prodDbName, "prod-", "", 1)

	// The main shard of a sharded databases will have a sharded table but the db name will not contain
	// the shard identifier because it is the main shard
	// Example: main shard of workforcevideo is workforcevideodb
	// We want to catch these and change the db name by adding the shard_0 identifier to match the toolshed API
	if shard0MainShard && dbSharded && !strings.Contains(sparkFriendlyDBName, "shard") {
		splitName := strings.Split(sparkFriendlyDBName, "db")
		return splitName[0] + "_shard_0db"
	}

	return strings.Replace(sparkFriendlyDBName, "-", "_", -1)
}

// SparkDbNameToRdsDbRegistryName takes the "Spark friendly" DB name and returns the RDS database registry name.
// i.e. Individual shard schema: associations_shard_0db -> associations
// i.e. Combined Shards schema: associationsdb_shards or associationsdb (unsharded) -> associations
func SparkDbNameToRdsDbRegistryName(sparkDbName string) string {
	// Handle the individual shard case, which ends in "_shard_#db".
	rdsRegistryName := regexp.MustCompile(`_shard_\ddb$`).ReplaceAllString(sparkDbName, "")

	// Handle the combine shards case. Trim "_shards" off of the schema name if it is
	// present (true for sharded DBs). Then trim off the "db" suffix, which is present for
	// all combine shards schemas.
	rdsRegistryName = strings.TrimSuffix(rdsRegistryName, "_shards")
	rdsRegistryName = strings.TrimSuffix(rdsRegistryName, "db")
	return rdsRegistryName
}

// ShardedSparkDbNameToProdDbName takes the Spark schema name of a sharded RDS DB,and
// returns the "prod" AWS shard name.
// Ex: associations_shard_0db -> prod-associationsdb
// Ex: associations_shard_1db -> prod-associations-shard-1db
func ShardedSparkDbNameToProdDbName(sparkDbName string) (string, error) {
	rdsRegistryName := SparkDbNameToRdsDbRegistryName(sparkDbName)
	if strings.HasSuffix(sparkDbName, "_0db") {
		return dbregistry.GetProdRdsDbName(rdsRegistryName), nil
	}
	// Replace underscores with hyphens, and add a "prod-" prefix.
	return fmt.Sprintf("prod-%s", strings.ReplaceAll(sparkDbName, "_", "-")), nil
}
