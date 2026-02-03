package ksdeltalake

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/sparktypes"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/stats/kinesisstats/bigstatsjsonexporter/bigstatsjsonschema"
	"samsaradev.io/stats/kinesisstats/jsonexporter/cf2jsonlschema"
	"samsaradev.io/team/components"
)

const ksDatabaseName = "kinesisstats"
const ksBigStatsDatabaseName = "s3bigstats"

type StatKind string

const (
	StatKindLocation   = StatKind("location")
	StatKindObjectStat = StatKind("objectstat")
)

type RegionOverrides struct {
	MinWorkersOverride *int
	MaxWorkersOverride *int

	// Set custom partition chunk size for the hourly merge operation to chunk the input batch of data
	PartitionChunkSize *int

	// Set custom partition chunk size for the daily merge operation to chunk the input batch of data
	// Note: This should only be set for stats with tiered schedules
	DailyPartitionChunkSize *int

	// Set custom number of s3files to process in each chunk for the merge operation
	S3FilesPerChunk *int

	// Whether or not to run the ks merge job in a unity catalog enabled cluster.
	EnableUcKinesisStats bool

	// Enable queueing for specific job schedules
	QueueEnabledSchedules []dataplatformconsts.KsJobType
}

type DataplatformOverrides struct {
	PrimaryKeysOverride *string

	// Optionally set overrides per region for specific attributes; see the RegionOverrides
	// struct for what values are configurable.
	RegionOverrides map[string]RegionOverrides

	// Merge performance is impacted by the number of partitions that we merge at once.
	// For some very large stats, or stats where older data is routinely reported, we create
	// a tiered schedule that merges new data routinely, and older data once a day.
	// NOTE: This is unsafe to set on existing stats, and can only be added to new stats.
	UseTieredSchedule bool

	MirrorToK8sSqSQueue bool

	// Whether to disable the serverless config for the job.
	DisableServerless bool

	// Whether to disable the delete task in the KS merge job.
	DisableDeletionTask bool

	// Optionally set to true to run the merge job every hour instead of default (3 hours).
	HourlyCronSchedule bool
}

type Stat struct {
	Kind                 StatKind
	StatType             objectstatproto.ObjectStatEnum
	BinaryMessageField   string
	S3BinaryMessageField string

	// Production indicates if an ObjectStat is used in customer facing feature, e.g. a Spark Report.
	Production bool
	// BigStatProduction indicates if a the S3 Big Stat portion of a stat is used in customer facing feature, e.g. a Spark Report.
	// For KS stats that have 2 parts (the regular base stat and an S3 big stat), seprately specify whether each part is used in
	// production. Big stat jobs often take a long time to run, so we don't want to page on overruns unless this portion is actually
	// used in production.
	BigStatProduction bool

	// DataModelStat indicates whether if a non-prod table is consumed in downstream data model pipeline or equivalent.
	// Setting this will result in the table getting a smaller SLO window and faster replication (3H) over non-production tables (6h).
	// The priority of the tables is as follows: Production (3h freshness) > DataModelTable (3h freshness) > NonProduction (6h freshness).
	DataModelStat bool

	// DoNotSetConversionParams specify parameters for schema generation. This is
	// a DataPlatform only flag and there is no need to set this.
	DoNotSetConversionParams sparktypes.ConversionParams

	// ContinuousIngestion is a boolean value that should be set to True if you would like your stat to replicate
	// to the Data Lake in a continuous fashion (5-15 min) instead of in a batch fashion (every 3 hours). This is False by
	// default.
	//
	// NOTE: continuous ingestion only applies to S3BigStats at the moment.
	ContinuousIngestion bool

	// Tags are a list of amundsentags.Tag that will enhance your stats discoverability within Amunden
	// View all available tags at infra/dataplatform/amundsen/amundsentags/tags.go
	// Feel free to add new tags to enhance your stat
	Tags []amundsentags.Tag

	// In some cases, we may want to prevent ingestion of certain fields into the datalake (e.g. uint64 fields likely
	// to overflow and break our parsing). To do that, add the full path of the field here (e.g.
	// "value.proto_value.modi_device_info.manufacturing_data.serial_number") and it will be excluded.
	// In general these should not be set; please validate any uses of this with data platform.
	ksFieldsToExclude        []string
	s3BigStatFieldsToExclude []string

	// MetadataInfo describes the table, columns, and how often its logged
	MetadataInfo *MetadataInfo

	// AdditionalAdminTeams are teams (besides the owners/dataplat) that can manage this job.
	// This should generally not be used but can be used in some cases where non-owning teams
	// need to manage the runs of a stat.
	AdditionalAdminTeams []components.TeamInfo

	// Dataplatform Overrides
	// These are additional features that control how the job is executed/run.
	// These should only be set by dataplatform team members.
	InternalOverrides *DataplatformOverrides

	// ServerlessConfig is the serverless config for the job.
	ServerlessConfig *databricks.ServerlessConfig
}

type Frequency interface {
	GetFrequencyDescription() string
}

type FrequencyEveryXSeconds int64

func (f FrequencyEveryXSeconds) GetFrequencyDescription() string {
	return fmt.Sprintf("Sent to samsara cloud every %s.", samtime.DurationToHuman(time.Second*time.Duration(f)))
}

type FrequencyOnChange string

func (f FrequencyOnChange) GetFrequencyDescription() string {
	return fmt.Sprintf("When the event %s triggers a stat to be sent to samsara cloud.", f)
}

type FrequencyCustom string

func (f FrequencyCustom) GetFrequencyDescription() string {
	return string(f)
}

type MetadataInfo struct {
	Description            string            // Description is a human-readable explanation of the data in the stat
	Frequency              Frequency         // Frequency describes how often the stat is logged
	IntValueDescription    string            // IntValueDescription describes what the int value field means
	DoubleValueDescription string            // DoubleValueDescription describes what the double value field means
	ColumnDescriptions     map[string]string // ColumnDescriptions is a map where the key is the column name (separated by "."s if the column is nested) and the value is a description of the column
}

const (
	IntValueColumnName    string = "value.int_value"
	DoubleValueColumnName string = "value.double_value"
	TimeColumnName        string = "value.time"
	DateColumnName        string = "value.date"
)

var ValueDefaultColumnNames = map[string]struct{}{
	IntValueColumnName:    {},
	DoubleValueColumnName: {},
	TimeColumnName:        {},
	DateColumnName:        {},
}

var DefaultColumns = map[string]string{
	"org_id":                       metadatahelpers.OrgIdDefaultDescription,
	"object_id":                    "A unique ID that provides a way to specify what this data message is for. Example: DeviceId for otDevice, WidgetId for otWidget, etc.",
	"object_type":                  metadatahelpers.EnumDescription("Enum value representing the type of stream.", objectstatproto.ObjectTypeEnum_name),
	"stat_type":                    "An identifier of the type of data being sent up.",
	"date":                         "The date the event was logged (yyyy-mm-dd).",
	"time":                         "The time the event was logged in milliseconds since the unix epoch (UTC).",
	"value.received_delta_seconds": "The difference, rounded to seconds, between when this event happened and when it was received by the backend. Note that this difference is between a device's clock (event timestamp) and the server clock (received at timestamp), so there may be cases where clock drift affects the result (even resulting in negative numbers indicating the device's clock is ahead of the server's).",
}

func (metadataInfo *MetadataInfo) GetAllColumnDescriptions() map[string]string {
	statColumnDescriptions := make(map[string]string)
	if metadataInfo != nil {
		for colName, colDesccription := range metadataInfo.ColumnDescriptions {
			statColumnDescriptions[colName] = colDesccription
		}

		if metadataInfo.IntValueDescription != "" {
			statColumnDescriptions[IntValueColumnName] = metadataInfo.IntValueDescription
		}
		if metadataInfo.DoubleValueDescription != "" {
			statColumnDescriptions[DoubleValueColumnName] = metadataInfo.DoubleValueDescription
		}
	}
	statColumnDescriptions[TimeColumnName] = DefaultColumns["time"]
	statColumnDescriptions[DateColumnName] = DefaultColumns["date"]

	for colName, colDescription := range DefaultColumns {
		// Only use the default column description if there is not a custom column description.
		if _, ok := statColumnDescriptions[colName]; !ok {
			statColumnDescriptions[colName] = colDescription
		}
	}
	return statColumnDescriptions
}

func fieldByName(typ *sparktypes.Type, name string) (*sparktypes.Field, error) {
	if typ.Type != sparktypes.TypeEnumStruct {
		return nil, oops.Errorf("not struct")
	}
	for _, field := range typ.Fields {
		if field.Name == name {
			return field, nil
		}
	}
	return nil, oops.Errorf("not found")
}

type Job struct {
	Name    dataplatformconsts.KsJobType
	Filter  string
	Timeout time.Duration
	Queue   bool // Whether to enable job queueing

	// Only run json2parquet step in the most frequently scheduled job.
	// It's not safe to run json2parquet concurrently.
	Json2Parquet bool
}

// Table holds storage and schema information about a KinesisStats Delta Lake table.
type Table struct {
	Name      string
	Partition string
	Schema    *sparktypes.Type
	Jobs      []Job
	// Production flag to indicate if a KS Delta Lake table is used in production reports
	Production bool
	// BigStatProduction flag to indicate if the big stat portion of a KS Delta Lake table is used in production reports.
	// These tables are a subset of Production tables (all tables with BigStatProduction should also have Production set).
	BigStatProduction bool

	// DataModelStat indicates whether if a non-prod table is consumed in downstream data model pipeline or equivalent.
	// Setting this will result in the table getting a smaller SLO window and faster replication (3H) over non-production tables (6h).
	// The priority of the tables is as follows: Production (3h freshness) > DataModelTable (3h freshness) > NonProduction (6h freshness).
	DataModelStat       bool
	S3BigStatSchema     *sparktypes.Type
	ContinuousIngestion bool
	Tags                []amundsentags.Tag

	MetadataInfo         *MetadataInfo
	S3BinaryMessageField string

	AdditionalAdminTeams []components.TeamInfo

	StatType objectstatproto.ObjectStatEnum
	StatKind StatKind

	InternalOverrides *DataplatformOverrides
	ServerlessConfig  *databricks.ServerlessConfig
}

func (tb *Table) GetReplicationFrequencyHour() string {
	if tb.InternalOverrides != nil && tb.InternalOverrides.HourlyCronSchedule {
		return "1"
	}
	if tb.Production || tb.DataModelStat {
		return "3"
	}
	// Non production stat.
	return "12"
}

// GetS3BBigStatCombinedViewSchema creates the combined view for the s3bigstat combined with the ks stat in sparktypes.Type
// This view is created when a ks stat has an s3bigstat paired with it in the databricks merge job and we
// need to emulate it in schema files in the backend repo
func (tb *Table) GetS3BBigStatCombinedViewSchema() *sparktypes.Type {
	for _, field := range tb.Schema.Fields {
		if field.Name == "value" {
			valueFields := make([]*sparktypes.Field, len(field.Type.Fields))
			idx := 0
			var recievedDeltaSecondsField *sparktypes.Field
			for _, field := range field.Type.Fields {
				// HACK: For some reason the received_delta_seconds in Glue for the combined schema is at the end
				// of the schema while here it is at the beginning. This iteration loop moves this field to the end
				// of the schema to match what is in Glue. Since this view is created with SparkSQL statements in a
				// Python job we don't have much control over it but want to emulate it here for our test environment
				if field.Name == "received_delta_seconds" {
					recievedDeltaSecondsField = field
					continue
				}

				valueFields[idx] = field
				idx++
			}

			valueFields[idx] = recievedDeltaSecondsField
			field.Type.Fields = valueFields
			tb.S3BigStatSchema.Fields = append(tb.S3BigStatSchema.Fields, field)
			break
		}
	}
	return tb.S3BigStatSchema
}

func NewTable(stat Stat) Table {
	var name string
	var typ *sparktypes.Type
	var s3BigStatTyp *sparktypes.Type
	var err error
	switch stat.Kind {
	case StatKindLocation:
		name = "location"
		typ, err = sparktypes.JsonTypeToSparkType(reflect.TypeOf(cf2jsonlschema.LocationOutput{}), stat.DoNotSetConversionParams)
		if err != nil {
			log.Fatalln(err)
		}
	case StatKindObjectStat:
		name = stat.StatType.String()
		typ, err = sparktypes.JsonTypeToSparkType(reflect.TypeOf(cf2jsonlschema.ObjectStatOutputWithFilenameMetadata{}), stat.DoNotSetConversionParams)

		if err != nil {
			log.Fatalln(err)
		}

		if stat.BinaryMessageField != "" {
			// We don't want to include every possible field in ObjectStatBinaryMessage
			// in schema definition, so we cherry-pick the field specified in stat.BinaryMessageField.
			var binaryMessage hubproto.ObjectStatBinaryMessage
			field, ok := reflect.TypeOf(binaryMessage).FieldByName(stat.BinaryMessageField)
			if !ok {
				log.Fatalln("hubproto.ObjectStatBinaryMessage does not contain field:", stat.BinaryMessageField)
			}

			name := strings.Split(field.Tag.Get("json"), ",")[0]
			protoValueSchema, err := sparktypes.JsonTypeToSparkType(field.Type, stat.DoNotSetConversionParams)
			if err != nil {
				log.Fatalln(err)
			}

			objectStatValueField, err := fieldByName(typ, "value")
			if err != nil {
				log.Fatalln(err)
			}

			// Add binary message type to value.proto_value.<name>.
			objectStatValueField.Type.Fields = append(objectStatValueField.Type.Fields,
				sparktypes.StructField("proto_value", sparktypes.StructType(
					sparktypes.StructField(name, protoValueSchema),
				)),
			)
		}

		if stat.S3BinaryMessageField != "" {
			s3BigStatTyp, err = sparktypes.JsonTypeToSparkType(reflect.TypeOf(bigstatsjsonschema.S3BigStatOutput{}), stat.DoNotSetConversionParams)
			if err != nil {
				log.Fatalln(err)
			}

			var s3BinaryMessage hubproto.ObjectStatS3BinaryMessage
			field, ok := reflect.TypeOf(s3BinaryMessage).FieldByName(stat.S3BinaryMessageField)
			if !ok {
				log.Fatalln("hubproto.ObjectStatS3BinaryMessage does not contain field: ", stat.S3BinaryMessageField)
			}

			name := strings.Split(field.Tag.Get("json"), ",")[0]
			s3ProtoValueSchema, err := sparktypes.JsonTypeToSparkType(field.Type, stat.DoNotSetConversionParams)
			if err != nil {
				log.Fatalln(err)
			}

			s3BigStatTyp.Fields = append(s3BigStatTyp.Fields,
				// Add s3 binary message type to value.s3_proto_value.<name>.
				sparktypes.StructField("s3_proto_value", sparktypes.StructType(
					sparktypes.StructField(name, s3ProtoValueSchema),
				)),
				// _synced_at represents the time the stat was written to the data lake
				sparktypes.StructField("_synced_at", sparktypes.TimestampType()),
			)
		}

	}

	jobType := dataplatformconsts.KsJobTypeEvery3Hr
	if stat.InternalOverrides != nil && stat.InternalOverrides.HourlyCronSchedule {
		jobType = dataplatformconsts.KsJobTypeEvery1Hr
	}

	defaultJobs := []Job{
		{
			Name:         dataplatformconsts.KsJobTypeAll,
			Timeout:      12 * time.Hour,
			Json2Parquet: true,
		},
	}

	// Input KinesisStats data can be merged into deduplicated data lake
	// at different interval depending on their event time. For example,
	// we may merge very recent data more frequently than one month old
	// data. We need to make sure these schedules do not process ranges
	// of data overlapping with each other.
	tieredSchedules := []Job{
		{
			// Every 3 hours, merge data in last 4 days.
			Name:         jobType,
			Filter:       `date >= date_add(current_date(), -4)`,
			Timeout:      12 * time.Hour,
			Json2Parquet: true,
		},
		{
			// Every day at 1am, merge data older than 4 days.
			Name:         dataplatformconsts.KsJobTypeDaily,
			Filter:       `date < date_add(current_date(), -4)`,
			Timeout:      23 * time.Hour,
			Json2Parquet: false,
		},
	}

	jobs := defaultJobs
	if stat.InternalOverrides != nil && stat.InternalOverrides.UseTieredSchedule {
		jobs = tieredSchedules
	}

	if len(stat.ksFieldsToExclude) != 0 {
		for _, path := range stat.ksFieldsToExclude {
			err = typ.RemoveFieldAtPath(path)
			if err != nil {
				log.Fatalf("Couldn't remove field at path %s for stat %s: %v\n", path, name, err)
			}
		}
	}

	if len(stat.s3BigStatFieldsToExclude) != 0 {
		for _, path := range stat.s3BigStatFieldsToExclude {
			err = s3BigStatTyp.RemoveFieldAtPath(path)
			if err != nil {
				log.Fatalf("Couldn't remove field at path %s for s3 big stat %s: %v\n", path, name, err)
			}
		}
	}

	// After constructing the schemas, validate that they are safe for json output.
	if typ != nil {
		if err = typ.ValidateSafeForJson(); err != nil {
			log.Fatalf("Schema for ks table %s is not safe for json. Please exclude the following fields in the ks registry (`ksFieldsToExclude`), or post in #ask-data-platform if you have any questions. Errors:\nx%v\n", name, err)
		}
	}
	if s3BigStatTyp != nil {
		if err = s3BigStatTyp.ValidateSafeForJson(); err != nil {
			log.Fatalf("Schema for s3bigstats table %s is not safe for json. Please exclude the following fields in the ks registry (`s3BigStatFieldsToExclude`), or post in #ask-data-platform if you have any questions. Errors:\nx%v\n", name, err)
		}
	}

	serverlessConfig := stat.ServerlessConfig

	// If no serverless config is provided, use the default serverless config.
	if serverlessConfig == nil {
		serverlessConfig = &databricks.ServerlessConfig{
			PerformanceTarget: databricks.StandardSeverlessJobPerformanceTarget,
			BudgetPolicyId:    dataplatformterraformconsts.BudgetPolicyIdForDataplatformAutomatedJobs,
		}
	}

	return Table{
		Name: name,

		// Partition by `date` field so that partitions are in similar size,
		// and filtering by date is fast.
		Partition:            "date",
		Schema:               typ,
		Jobs:                 jobs,
		Production:           stat.Production,
		BigStatProduction:    stat.BigStatProduction,
		DataModelStat:        stat.DataModelStat,
		S3BigStatSchema:      s3BigStatTyp,
		ContinuousIngestion:  stat.ContinuousIngestion,
		MetadataInfo:         stat.MetadataInfo,
		Tags:                 stat.Tags,
		S3BinaryMessageField: stat.S3BinaryMessageField,
		StatType:             stat.StatType,
		StatKind:             stat.Kind,
		AdditionalAdminTeams: stat.AdditionalAdminTeams,
		InternalOverrides:    stat.InternalOverrides,
		ServerlessConfig:     serverlessConfig,
	}
}

func (tb Table) QualifiedName() string {
	return fmt.Sprintf("%s.%s", ksDatabaseName, tb.Name)
}

func (tb Table) S3BigStatsName() string {
	return fmt.Sprintf("%s.%s", ksBigStatsDatabaseName, tb.Name)
}

func (tb Table) S3BigStatCombinedViewName() string {
	return fmt.Sprintf("%s.%s_with_s3_big_stat", ksDatabaseName, tb.Name)
}

func (tb Table) GetK8sTableName() string {
	return fmt.Sprintf("k8s_%s", tb.Name)
}

func (tb Table) KsSloConfig(sched dataplatformconsts.KsJobType) dataplatformconsts.JobSlo {
	sloConfig := dataplatformconsts.JobSlo{}
	if sched == dataplatformconsts.KsJobTypeDaily {
		// Daily jobs are confusingly named and run 2x a day.
		// So we'll page low urgency after 2 failures and high urgency after 4 days
		// to ensure we act before retention periods are hit.
		sloConfig = dataplatformconsts.JobSlo{
			SloTargetHours:              24,
			Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
			BusinessHoursThresholdHours: 24,
			HighUrgencyThresholdHours:   96,
		}

		if tb.Production {
			sloConfig = dataplatformconsts.JobSlo{
				SloTargetHours:            24,
				Urgency:                   dataplatformconsts.JobSloHigh,
				HighUrgencyThresholdHours: 24,
			}
		}
	} else {
		// By default, nonproduction jobs will page low urgency after 2 failures
		// and high urgency business hours after 1 day.
		// To make sure we don't hit retention policies we'll definitely page high urgency after 4 days.
		sloConfig = dataplatformconsts.JobSlo{
			SloTargetHours:              24,
			Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
			LowUrgencyThresholdHours:    13,
			BusinessHoursThresholdHours: 24,
			HighUrgencyThresholdHours:   96,
		}

		// Create a separate SLO for Datamodel used tables. It is currently same as
		// the non-production tables, but once we have SLA with data tools team, we shall
		// populate the correct numbers for SLO here.
		if tb.DataModelStat {
			sloConfig = dataplatformconsts.JobSlo{
				SloTargetHours:              24,
				Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
				LowUrgencyThresholdHours:    13,
				BusinessHoursThresholdHours: 24,
				HighUrgencyThresholdHours:   96,
			}
		}

		if tb.Production {
			// Production jobs page high urgency after 2 failures.
			sloConfig = dataplatformconsts.JobSlo{
				SloTargetHours:            8,
				Urgency:                   dataplatformconsts.JobSloHigh,
				HighUrgencyThresholdHours: 8,
			}
		}

	}

	return sloConfig
}

func (tb Table) BigStatsSloConfig() dataplatformconsts.JobSlo {
	if tb.S3BigStatSchema == nil {
		return dataplatformconsts.JobSlo{}
	}
	// Page high urgency business hours after 25 hours (our slo). And make sure to page high
	// urgency after 4 days of no success so we don't hit retention windows.
	sloConfig := dataplatformconsts.JobSlo{
		SloTargetHours:              24,
		Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
		LowUrgencyThresholdHours:    13,
		BusinessHoursThresholdHours: 24,
		HighUrgencyThresholdHours:   96,
	}

	if tb.BigStatProduction {
		// For production jobs we page after 2 failures.
		sloConfig = dataplatformconsts.JobSlo{
			SloTargetHours:            8,
			Urgency:                   dataplatformconsts.JobSloHigh,
			HighUrgencyThresholdHours: 8,
		}
	}

	return sloConfig
}

// Build descriptions for the kinesisstat/bigstat, including the description/frequency and slo information.
// Separate them by double newlines so that they display nicely in amundsen/datahub.
func (tb Table) KsDescription() string {
	var parts []string
	if tb.MetadataInfo != nil {
		if tb.MetadataInfo.Description != "" {
			parts = append(parts, tb.MetadataInfo.Description)
		}
		if tb.MetadataInfo.Frequency != nil {
			parts = append(parts, tb.MetadataInfo.Frequency.GetFrequencyDescription())
		}
	}
	parts = append(parts, tb.KsSloConfig(dataplatformconsts.KsJobTypeAll).SloDescription())
	return strings.Join(parts, "\n\n")
}

func (tb Table) BigStatsDescription() string {
	var parts []string
	if tb.MetadataInfo != nil {
		if tb.MetadataInfo.Description != "" {
			parts = append(parts, tb.MetadataInfo.Description)
		}
		if tb.MetadataInfo.Frequency != nil {
			parts = append(parts, tb.MetadataInfo.Frequency.GetFrequencyDescription())
		}
	}
	parts = append(parts, tb.BigStatsSloConfig().SloDescription())
	return strings.Join(parts, "\n\n")
}

func ParseFieldNamesOnType(typ *sparktypes.Type) []string {
	names := make([]string, 0)
	for _, f := range typ.Fields {
		if (*f.Type).Type == sparktypes.TypeEnumStruct {
			names = append(names, f.Name)
			for _, subTypeName := range ParseFieldNamesOnType(f.Type) {
				names = append(names, fmt.Sprintf("%s.%s", f.Name, subTypeName))
			}
		} else if (*f.Type).Type == sparktypes.TypeEnumArray {
			names = append(names, f.Name)
			for _, subTypeName := range ParseFieldNamesOnType((*f.Type).ElementType) {
				names = append(names, fmt.Sprintf("%s.%s", f.Name, subTypeName))
			}
		} else {
			names = append(names, f.Name)
		}
	}
	return names
}

func (d *DataplatformOverrides) IsKinesisStatsUcEnabled(region string) bool {
	if d.RegionOverrides != nil {
		if regionOverrides, ok := d.RegionOverrides[region]; ok {
			return regionOverrides.EnableUcKinesisStats
		}
	}
	return false
}
