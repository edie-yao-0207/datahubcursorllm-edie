package datastreamlake

import (
	"log"
	"os"
	"path/filepath"
	"regexp"

	dataconnectordatastreamtypes "samsaradev.io/api/dataconnectors/datastreamtypes"
	"samsaradev.io/connectedworker/workforce/workforcedatastreamtypes"
	"samsaradev.io/deviceconfig/safety/configvalidator/configvalidatordatastream"
	"samsaradev.io/deviceconfig/safety/configvalidator/featuresturnoffbysafeguarddatastream"
	"samsaradev.io/firmware/firmwaredatastreamtypes"
	"samsaradev.io/fleet/assets/assetdatastreamtypes"
	"samsaradev.io/fleet/connectedequipment/stcedatastreamtypes"
	"samsaradev.io/fleet/oem/oemfirehoseutils"
	"samsaradev.io/fleet/routingplatform/infra/routingdatastreamtypes"
	"samsaradev.io/fleet/routingplatform/routeplanning/routeplanningdatastreamtypes"
	"samsaradev.io/fleet/security/anomalydetection/anomalydatastreamtypes"
	"samsaradev.io/fleet/security/fuelactivity/fueldatastreamtypes"
	"samsaradev.io/fleet/telematics/internalrma/internalrmadatastreams"
	"samsaradev.io/fleet/vin/vindecoding/vindecodingdatastreamtypes"
	"samsaradev.io/fleet/vin/vinserver/cableselectiondatastreamtypes"
	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/helpers/scientist/scientistdatastream"
	"samsaradev.io/infra/api/devecodatastreamtypes"
	streampublisherdatastreamtypes "samsaradev.io/infra/api/streaming/apipublisher/datastreamtypes"
	buildkitepipelinemetrics "samsaradev.io/infra/buildkite/cmd/buildkitepipelinemetrics/metric"
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
	"samsaradev.io/infra/devtools/goplslogger/goplsloggermetrics"
	"samsaradev.io/infra/monitoring/buildkitemetrics/buildkitedatastreamtypes"
	gotestmetrics "samsaradev.io/infra/monitoring/cmd/gotestmetrics/metric"
	"samsaradev.io/infra/monitoring/tracingtypes"
	workflowsplatformdatastreams "samsaradev.io/infra/workflows/dynamicworkflows/documents/datastreams"
	"samsaradev.io/ml/accessors/cohortconfigmanagerapi/cohortconfigmanagerdatastream"
	"samsaradev.io/ml/models/mldatastreams"
	driverappproxydatastream "samsaradev.io/mobile/app/driverappproxyingestionworker/datastream"
	"samsaradev.io/mobile/mdm/amapipubsubreader/amapipubsubdatastream"
	"samsaradev.io/mobile/mobileappmetrics"
	"samsaradev.io/mobile/remotesupport/remotesupportfeedbackdatastream"
	"samsaradev.io/models/orgsammappinglog"
	"samsaradev.io/platform/alerts/alertdatastreamtypes"
	"samsaradev.io/platform/deviceservices/assetphotos/assetphotodatastreams"
	"samsaradev.io/platform/locationservices/driverassignment/driverassignmentauditlog"
	"samsaradev.io/platform/locationservices/driverassignment/driverassignmentsmodels"
	driverassignmentmanualunassignwriter "samsaradev.io/platform/locationservices/driverassignment/manualunassignwriter"
	"samsaradev.io/platform/workflows/workflowsdatastreamtypes"
	"samsaradev.io/safety/eventdetection/checkpointers/records"
	"samsaradev.io/safety/geospatial/smartmapsdatastreamtypes"
	"samsaradev.io/safety/harshevents/harsheventsmetricstypes"
	"samsaradev.io/safety/infra/safetydatastreamtypes"
	"samsaradev.io/safety/owlgraph/tracing"
	"samsaradev.io/safety/risk/models/riskdatastreams"
	"samsaradev.io/safety/safetyvision/facial_recognition/recognitiondatastreams"
	sertracerecords "samsaradev.io/safety/sertrace/records"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	migratetrainingtypes "samsaradev.io/training/trainingmodels/app/migratetraining/types"
	"samsaradev.io/workersafety/vapidatastreamtypes"
)

const (
	// maxDatastreamLength is the maximum length of the firehose datastream that AWS accepts.
	maxDatastreamLength = 64

	// maxSamsaraDefinedNameLength is the maximum length of the samsara defined name.
	// We prefix the `datastream_firehose_` string, so the max internally defined name
	// length is maxDatastreamLength - len(prefix).
	maxSamsaraDefinedNameLength = maxDatastreamLength - len("datastream_firehose_")
)

type TestRecord struct {
	IntegerValue int64               `json:"int_val"`
	StringValue  string              `json:"string_val"`
	TimeValue    datastreamtime.Time `json:"time_val"`
	StringArray  []string            `json:"string_arr"`
	ByteArray    []byte              `json:"byte_arr"`
	Map          map[string]string   `json:"map_type"`
	// This is a new field that will be added to the record
	// in order to test the propagation of the new field to
	// the final delta table.
	NewField string `json:"new_field"`
}

// DataStream defines a Data Stream to be ingested into the Data Lake
type DataStream struct {
	StreamName string              // StreamName is the name of the stream. Stream names should be snake_case with lowercase letters & digits, not start with a digit, and at least two characters. (i.e random_test_stream)
	Owner      components.TeamInfo // Owner is the team owning the stream of cost attribution purposes

	// Record is the Go Struct that will be sent onto the Data Stream, converted to Parquet,
	// and ingested into the Data Lake.
	//
	// To successfully complete this path, the struct must satisfy a few requirements:
	// 1. All fields must have a JSON tag (i.e `json:"column_name"`)
	// 2. All fields must be exported for proper JSON marshalling (MyField not myField)
	// 3. All field types must be Glue/Parquet Compatible. Supported types can be found at datastreamlake/go2glue.go L37-46
	// 4. For timestamp columns no not use time.Time. Use datatstreamlake.DataStreamTime. This is needed for record conversion reason.
	Record interface{}

	// GeneratedCodePath is the path (relative to samsaradev.io) where the Stream Writers for this stream will be generated
	GeneratedWriterPath string
	// Description is a human readable string that describes what type of data is in the stream.
	Description string

	// Tags are a list of amundsentags.Tag that will enhance your data stream tables discoverability within Amundsen
	// View all available tags at infra/dataplatform/amundsen/amundsentags/tags.go
	// Feel free to add new tags to enhance your data stream table
	Tags []amundsentags.Tag

	// WriteOutsideECSEnv determines whether the generated writer performs writes
	// out of our production ECS environment. Set this to true if writing from dev
	// or buildkite env is intentional, and this only works if the caller role has
	// the right IAM permissions to write to Firehose. The default behavior
	// (false) prevents us from spewing unnecessary Firehose write permission
	// errors.
	EnableWriteOutsideECSEnv bool

	// Partition Strategy defines how to partition the delta table.
	PartitionStrategy *PartitionStrategy
}

type PartitionStrategy struct {
	PartitionColumns             []string
	DateColumnOverride           string
	DateColumnOverrideExpression string
}

var Registry = []DataStream{ // lint: +sorted
	{
		StreamName:          "alert_sms_logs",
		Owner:               team.PlatformAlertsWorkflows,
		Record:              alertdatastreamtypes.AlertSMSLogDataLakeMetric{},
		GeneratedWriterPath: "platform/alerts",
		Description:         "Contains a log for each alert SMS or Whatsapp notification sent",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "amapi_pubsub_messages",
		Owner:               team.MdmSdk,
		Record:              amapipubsubdatastream.AMAPIPubSubMessageDataLakeRecord{},
		GeneratedWriterPath: "mobile/mdm/amapipubsubreader",
		Description:         "Messages from AMAPI PubSub",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "anomalous_stop_results",
		Owner:               team.FleetSec,
		Record:              anomalydatastreamtypes.AnomalousStopResult{},
		GeneratedWriterPath: "fleet/security/anomalydetection/anomalydatastreamtypes",
		Description:         "Contains analysis results from anomalous stop detection.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "api_logs",
		Owner:               team.DevEcosystem,
		Record:              devecodatastreamtypes.ApiAccessLogDataLakeMetric{},
		GeneratedWriterPath: "controllers/api/middleware",
		Description:         "Contains a log for each request made to the public facing API.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "asset_characteristics_parses",
		Owner:               team.STCEUnpowered,
		Record:              assetphotodatastreams.AssetCharacteristicsParse{},
		GeneratedWriterPath: "platform/deviceservices/assetphotos/assetphotodatastreams",
		Description:         "Contains a log for each time we attempt to parse asset photo characteristics.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "backend_model_registry_ddb",
		Owner:               team.MLInfra,
		Record:              mldatastreams.BackendModelRegistryDDB{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Logs changes to the backend model registry in DDB.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "battery_prediction_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.BatteryChargePredictionInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Inference results from the battery prediction model.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:               "buildkite_job_error_metrics",
		Owner:                    team.DeveloperExperience,
		Record:                   buildkitedatastreamtypes.BuildkiteJobErrorMetric{},
		GeneratedWriterPath:      "infra/monitoring/buildkitemetrics",
		Description:              "Collects error metrics and data from buildkite jobs.",
		EnableWriteOutsideECSEnv: true, // This assumes AppConfig.IsDevEnv() is true in Buildkite, which has not been validated.
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:               "buildkite_pipeline_metrics",
		Owner:                    team.DeveloperExperience,
		Record:                   buildkitepipelinemetrics.BuildkitePipelineMetric{},
		GeneratedWriterPath:      "infra/buildkite/cmd/buildkitepipelinemetrics/internal",
		Description:              "Collects metrics from within buildkite pipeline scripts for pipeline and step-level metrics",
		EnableWriteOutsideECSEnv: true, // This assumes AppConfig.IsDevEnv() is true in Buildkite, which has not been validated.
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "cable_selection_logs",
		Owner:               team.DiagnosticsTools,
		Record:              cableselectiondatastreamtypes.CableSelectionLogDataLakeMetric{},
		GeneratedWriterPath: "fleet/vin/vinserver/cableselection",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "ce_disassociation_evaluations",
		Owner:               team.ConnectedEquipment,
		Record:              assetdatastreamtypes.DisassociationEvaluation{},
		GeneratedWriterPath: "fleet/assets/assetdatastreamtypes",
		Description:         "Logs data used to evaluate if a Vehicle and Asset are disassociated (dropped).",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "ce_parsed_proxied_advertisements",
		Owner:               team.ConnectedEquipment,
		Record:              stcedatastreamtypes.ParsedProxiedAdvertisement{},
		GeneratedWriterPath: "fleet/connectedequipment/stcedatastreamtypes",
		Description:         "Parsed proxied advertisements from 3rd party devices.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "cloud_app_user_interactions",
		Owner:               team.SRE,
		Record:              tracingtypes.CloudAppUserInteractionDataLakeMetric{},
		GeneratedWriterPath: "infra/monitoring/internal/remotetrace",
		Description:         "Pages can be instrumented to time specific user interactions. Each record is a specific timed user interaction.",
		Tags:                []amundsentags.Tag{amundsentags.PerformanceTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "cm_outward_obstruction_v2_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.CmOutwardObstructionV2InferenceMetrics{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of ML model inference and associated metadata to analyze performance.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "cohort_profile_config_data",
		Owner:               team.MLInfra,
		Record:              cohortconfigmanagerdatastream.CohortProfileConfigDataMetric{},
		GeneratedWriterPath: "ml/accessors/cohortconfigmanagerapi/cohortconfigmanagerdatastream",
		Description:         "Emit the profile_config_data for a cohort profile to visualize on datalake",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "config_validation_output",
		Owner:               team.MLInfra,
		Record:              configvalidatordatastream.ConfigRuleValidationOutputMetric{},
		GeneratedWriterPath: "deviceconfig/safety/configvalidator/configvalidatordatastream",
		Description:         "Config builder validation results. This is used to confirm the populated device config passes validation criteria",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "config_version_history",
		Owner:               team.CoreServices,
		Record:              firmwaredatastreamtypes.ConfigVersionHistory{},
		GeneratedWriterPath: "firmware/firmwaredatastreamtypes",
		Description:         "Logs data for when and why gateway config versions are incremented.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "connected_equipment_association_evaluations",
		Owner:               team.ConnectedEquipment,
		Record:              assetdatastreamtypes.AssociationEvaluation{},
		GeneratedWriterPath: "fleet/assets/assetdatastreamtypes",
		Description:         "Logs data used to evaluate if a Vehicle and Asset are associated.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "connected_equipment_fma_deliverability_logs",
		Owner:               team.STCEUnpowered,
		Record:              assetdatastreamtypes.FmaDeliverabilityLogs{},
		GeneratedWriterPath: "fleet/assets/assetdatastreamtypes",
		Description:         "Logs info around the Find My Asset SMS send flow for a deliverability study.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "connected_equipment_reefer_command_logs",
		Owner:               team.ConnectedEquipment,
		Record:              assetdatastreamtypes.ReeferCommandLog{},
		GeneratedWriterPath: "fleet/assets/assetdatastreamtypes",
		Description:         "Logs device and command data when a command is sent to a reefer.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "connected_sites_camera_device_auth_logs",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.ConnectedSitesCameraDeviceAuthLog{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Contains a log for a camera's authentication metadata and user interaction, for cameras that failed or succeeded authentication attempts.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "connected_sites_camera_stream_auth_logs",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.ConnectedSitesCameraStreamAuthLog{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Contains a log for a camera streams's authentication metadata and user interaction, for cameras that failed or succeeded authentication attempts.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "construction_zone_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.ConstructionZoneInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from construction zone classifier inferences.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "crash_detector_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.CrashDetectorInferenceMetrics{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Metrics to log results of ML model inference and associated metadata to analyze performance.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "crash_filter_transformed_events",
		Owner:               team.SafetyPlatform,
		Record:              safetydatastreamtypes.CrashFilterTransformedEvent{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains entries for safety events where the event was transformed by the backend crash filter",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(event_ms / 1e3))",
		},
	},
	{
		StreamName:          "crux_aggregated_locations",
		Owner:               team.STCEUnpowered,
		Record:              stcedatastreamtypes.CruxAggregatedLocations{},
		GeneratedWriterPath: "fleet/connectedequipment/stcedatastreamtypes",
		Description:         "Aggregated locations of crux devices. Used for experimentation and analysis of new aggregation techniques.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "crux_observations",
		Owner:               team.STCEUnpowered,
		Record:              stcedatastreamtypes.CruxObservations{},
		GeneratedWriterPath: "fleet/connectedequipment/stcedatastreamtypes",
		Description:         "Observations of crux devices. Used for monitoring device health before and after activation in customer orgs, and observing the health of the observation processor.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "crux_unaggregated_locations",
		Owner:               team.STCEUnpowered,
		Record:              stcedatastreamtypes.CruxUnaggregatedLocations{},
		GeneratedWriterPath: "fleet/connectedequipment/stcedatastreamtypes",
		Description:         "Unaggregated location observations of crux devices. Used for experimentation and analysis of new aggregation techniques.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "dashcam_drowsiness_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamDrowsinessInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from dashcam drowsiness backend ML model inferences.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "dashcam_inward_mtl_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamInwardMtlInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from dashcam inward mtl inferences, including but not limited to mobile usage and inattentive driving.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "dashcam_rlv_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamRedLightViolationInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from dashcam red light violation inferences.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "dashcam_rolling_stop_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamRollingStopInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the dashcam rolling stop backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "dashcam_vru_cw_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamVulnerableRoadUserCollisionWarningInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the dashcam vulnerable road user collision warning backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "data_connector_client_log_events",
		Owner:               team.DevEcosystem,
		Record:              dataconnectordatastreamtypes.ClientLogEvent{},
		GeneratedWriterPath: "api/dataconnectors/datastreamtypes",
		Description:         "Data Connector Client Log Events",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "delete_s3_video_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.DeleteS3FileMetrics{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains an entry for video deletions requested by super users.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "device_backfilled_cargo_state",
		Owner:               team.ConnectedEquipment,
		Record:              stcedatastreamtypes.CargoBackfilledState{},
		GeneratedWriterPath: "fleet/connectedequipment/stcedatastreamtypes",
		Description:         "Contains a log for each time we write a backfilled derived cargo state for a device.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(timestamp)",
		},
	},
	{
		StreamName:          "dpf_failure_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DPFFailureInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Inference results from the dpf failure model.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "driver_assignment_audit_log",
		Owner:               team.DriverManagement,
		Record:              driverassignmentauditlog.AuditLog{},
		GeneratedWriterPath: "platform/locationservices/driverassignment/driverassignmentauditlog",
		Description:         "Contains an entry for each message that was received by the Driver Assignments Worker process",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "driver_assignment_dropped_backfill_messages",
		Owner:               team.DriverManagement,
		Record:              driverassignmentsmodels.DroppedBackfillMessage{},
		GeneratedWriterPath: "platform/locationservices/backfill",
		Description:         "Contains a record of each unprocessable driver assignment message that was dropped by the backfill worker",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "driver_assignment_manual_unassign",
		Owner:               team.DriverManagement,
		Record:              driverassignmentmanualunassignwriter.ManualUnassignLog{},
		GeneratedWriterPath: "platform/locationservices/driverassignment/manualunassignwriter",
		Description:         "Contains an entry for each manual unassignment of a driver from a vehicle",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "driver_assignment_settings_audit_log",
		Owner:               team.DriverManagement,
		Record:              driverassignmentauditlog.SettingsAuditLog{},
		GeneratedWriterPath: "platform/locationservices/driverassignment/driverassignmentauditlog",
		Description:         "Contains an entry for each driver assignment setting whenever it is created or updated",
		Tags:                []amundsentags.Tag{amundsentags.FleetTag, amundsentags.TelematicsTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "driver_risk_score_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DriverRiskScoreInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the driver risk score backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "driver_trip_risk_score_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DriverTripRiskScoreInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the driver trip risk score backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "driver_trip_risk_score_v2_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DriverTripRiskScoreV2InferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the driver trip risk score v2 backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "driverappproxy_http_logs",
		Owner:               team.Mobile,
		Record:              driverappproxydatastream.DriverAppProxyHttpLog{},
		GeneratedWriterPath: "mobile/app/driverappproxyingestionworker/datastream",
		Description:         "Logs HTTP requests made to the driver app proxy.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(timestamp)",
		},
	},
	{
		StreamName:          "dynamic_workflow_runs",
		Owner:               team.WorkflowsPlatform,
		Record:              workflowsplatformdatastreams.WorkflowRunDataStreamRecord{},
		GeneratedWriterPath: "infra/workflows/dynamicworkflows/documents/datastreams",
		Description:         "Contains an entry for each workflow run that occurs on the dynamic workflows platform.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "event_detection_checkpoint_status",
		Owner:               team.SafetyPlatform,
		Record:              records.CheckpointRecord{},
		GeneratedWriterPath: "safety/eventdetection/checkpointers",
		Description:         "Records the checkpoint status of an event in the event detection pipeline, including the location of a checkpoint in the pipeline and whether an event passes this checkpoint.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "facial_recognition_processing_audit_log",
		Owner:               team.DriverManagement,
		Record:              recognitiondatastreams.FacialRecognitionProcessingAuditLog{},
		GeneratedWriterPath: "safety/safetyvision/facial_recognition/recognitiondatastreams",
		Description:         "An audit log of each time facial recognition processing occurs",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "image_timestamp",
		},
	},
	{
		StreamName:          "features_turnoff_by_safeguard",
		Owner:               team.MLInfra,
		Record:              featuresturnoffbysafeguarddatastream.FeaturesTurnoffBySafeguardDatastreamMetric{},
		GeneratedWriterPath: "deviceconfig/safety/configvalidator/featuresturnoffbysafeguarddatastream",
		Description:         "Logs the features that are turned off by the safeguard",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "firmware_model_registry_ddb",
		Owner:               team.MLInfra,
		Record:              mldatastreams.FirmwareModelRegistryDDB{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Logs changes to the firmware model registry in DDB.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "forward_collision_warning_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.ForwardCollisionWarningInferenceMetrics{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Metrics to log results of ML model inference and associated metadata to analyze performance.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "forward_distance_v2_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DashcamFollowingDistanceMetricsV2{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Metrics to log results of ML model inference and associated metadata to analyze performance for FDv2.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "frontend_routeload",
		Owner:               team.SRE,
		Record:              tracingtypes.RouteLoadDataLakeMetric{},
		Description:         "Contains an entry about every page visit to the cloud app",
		GeneratedWriterPath: "infra/monitoring/internal/remotetrace",
		Tags:                []amundsentags.Tag{amundsentags.PerformanceTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "fuel_anomaly_events",
		Owner:               team.FleetSec,
		Record:              fueldatastreamtypes.FuelAnomalyEvent{},
		GeneratedWriterPath: "fleet/security/fuelactivity/fueldatastreamtypes",
		Description:         "Fuel anomaly events detected by different fuel anomaly detection models.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "fuel_change_estimator_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.FuelChangeEstimatorInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from fuel change estimator backend ML model inferences.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "fuel_level_denoising_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.FuelLevelDenoisingInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Inference results from the fuel level denoising model.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:               "go_test_metrics",
		Owner:                    team.DeveloperExperience,
		Record:                   gotestmetrics.TestMetric{},
		Description:              "Collects metrics on go tests.",
		GeneratedWriterPath:      "infra/monitoring/cmd/gotestmetrics",
		EnableWriteOutsideECSEnv: true, // We're writing from a buildkite environment.
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:               "gopls_metrics",
		Owner:                    team.DeveloperExperience,
		Record:                   goplsloggermetrics.GoplsTraceMetric{},
		Description:              "Collects metrics on gopls (official Go language server) actions.",
		GeneratedWriterPath:      "infra/devtools/goplslogger",
		EnableWriteOutsideECSEnv: true,
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "gql_query_load",
		Owner:               team.SRE,
		Record:              tracingtypes.GQLQueryLoadDataLakeMetric{},
		Description:         "Contains an entry about every GQL query that executed when loading a page on the cloud app.",
		GeneratedWriterPath: "infra/monitoring/internal/remotetrace",
		Tags:                []amundsentags.Tag{amundsentags.PerformanceTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "harsh_event_write_path_dropoff_metrics",
		Owner:               team.SafetyPlatform,
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		Record:              harsheventsmetricstypes.HarshEventWritePathDropoffMetric{},
		GeneratedWriterPath: "safety/harshevents/harsheventmetrics",
		Description:         "Metrics to track dropoff at each filter in the write path for Harsh Events. For each filter evaluated for each harsh event, we record whether it was NotCaught, CaughtAndKept, or CaughtAndDropped.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(event_ms / 1e3))",
		},
	},
	{
		StreamName:          "internal_rma_audit_log",
		Owner:               team.DeviceServices,
		Record:              internalrmadatastreams.InternalRmaAuditLog{},
		GeneratedWriterPath: "fleet/telematics/internalrma/internalrmadatastreams",
		Description:         "This DataStream contains the history for each Internal RMA request. Data is written to this data stream each time an RMA request is created or updated",
		Tags:                []amundsentags.Tag{amundsentags.FleetTag, amundsentags.TelematicsTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:               "jack_test",
		Owner:                    team.DataPlatform,
		Record:                   TestRecord{},
		GeneratedWriterPath:      "infra/dataplatform",
		Description:              "This is a test data stream",
		EnableWriteOutsideECSEnv: true,
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "lane_departure_warning_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.LaneDepartureWarningInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Metrics to log results of LDW model inference and associated metadata to analyze performance.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "messages_metadata",
		Owner:               team.Mobile,
		Record:              mobileappmetrics.MessageMetadataDataLakeMetric{},
		GeneratedWriterPath: "mobile/mobileappmetrics",
		Description:         "Contains metadata about driver messages. Message bodies are not included.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "sent_at",
		},
	},
	{
		StreamName:          "ml_data_collection_requests",
		Owner:               team.MLInfra,
		Record:              mldatastreams.DataCollectionRequest{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of ML data collection requests.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "ml_device_configuration_record",
		Owner:               team.MLInfra,
		Record:              mldatastreams.MLDeviceConfigRecord{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Records ML model config values when pushing device config to firmware devices",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "ml_test_datastream",
		Owner:               team.MLInfra,
		Record:              mldatastreams.MLTestDatastream{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Test data stream for ML",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "mobile_device_info",
		Owner:               team.Mobile,
		Record:              mobileappmetrics.MobileTroyDeviceInfoDataLakeMetric{},
		GeneratedWriterPath: "mobile/mobileappmetrics",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
		Description: "Mobile device information for all troy apps. New entries are populated for the same device on un/reinstallation of a native app or when a team driver passenger signs in.",
	},
	{
		StreamName:          "mobile_logs",
		Owner:               team.Mobile,
		Record:              mobileappmetrics.MobileTroyLogEventDataLakeMetric{},
		GeneratedWriterPath: "mobile/mobileappmetrics",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
		Description: "Mobile log event data from all troy apps (see: [Logs in Troy](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4321894/Logs+in+Troy)). New log events are continuously generated on the mobile app and bulk uploaded on an interval which then populates new entries to this table.",
	},
	{
		StreamName:          "mobile_nav_routing_events",
		Owner:               team.Navigation,
		Record:              routingdatastreamtypes.MobileNavRoutingDataLakeEvent{},
		GeneratedWriterPath: "fleet/routingplatform/infra/routingdatastreamtypes",
		Description:         "Contains commercial navigation routing events from the mobile app.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "mobile_sentry_events",
		Owner:               team.Mobile,
		Record:              mobileappmetrics.MobileTroySentryEventDataLakeMetric{},
		GeneratedWriterPath: "mobile/mobileappmetrics",
		Description:         "Contains Sentry troy event API data",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "date_created",
		},
	},
	{
		StreamName:          "oauth_logs",
		Owner:               team.DevEcosystem,
		Record:              devecodatastreamtypes.OauthLogDataLakeMetric{},
		GeneratedWriterPath: "controllers/api/middleware",
		Description:         "Contains a log for each oauth request through our API.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "oem_api_data_logs",
		Owner:               team.TelematicsData,
		Record:              oemfirehoseutils.OemDataRecord{},
		GeneratedWriterPath: "fleet/oem/oemfirehoseutils",
		Description:         "Contains raw OEM API data",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "to_date(received_at)",
		},
	},
	{
		StreamName:          "open_weather_severe_weather_alerts",
		Owner:               team.SafetyPlatform,
		Record:              riskdatastreams.OpenWeatherSevereWeatherAlert{},
		GeneratedWriterPath: "safety/risk/models/riskdatastreams",
		Description:         "Contains severe weather alerts received from OpenWeather",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "org_sam_mapping_events",
		Owner:               team.PlatformOperations,
		Record:              orgsammappinglog.Event{},
		GeneratedWriterPath: "models/orgsammappinglog",
		Description:         "Contains an internal audit log of events that lead to OrgID to SAM number mapping changes",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "to_date(timestamp)",
		},
	},
	{
		StreamName:          "osm_way_metadata_refresh",
		Owner:               team.SmartMaps,
		Record:              smartmapsdatastreamtypes.OsmWayMetadataRefreshRecord{},
		GeneratedWriterPath: "safety/geospatial/smartmapsdatastreamtypes",
		Description:         "internal audit log for OSM way metadata refresh jobs",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "owlgraph_application_logs",
		Owner:               team.SafetyInsights,
		Record:              tracing.OwlGraphLogEvent{},
		GeneratedWriterPath: "safety/owlgraph/tracing",
		Description:         "Logs OwlGraph/Eino application execution events including function calls, errors, and metrics with hierarchical workflow tracking.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "predictive_maintenance_alert_metrics",
		Owner:               team.MLInfra,
		Record:              mldatastreams.PredictiveMaintenanceAlertMetrics{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Metrics to track how many predictive maintenance alerts are generated",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "to_date(timestamp)",
		},
	},
	{
		StreamName:          "qualifications_jjkeller_migration",
		Owner:               team.Training,
		Record:              migratetrainingtypes.JjkellerMigrationLog{},
		GeneratedWriterPath: "training/trainingmodels/app/migratetraining",
		Description:         "Contains logs for training form migration events, tracking the migration of training forms and their associated metadata.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "created_at",
		},
	},
	{
		StreamName:          "rear_collision_warning_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.RearCollisionWarningInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the rear collision warning backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "remote_support_feedback_log",
		Owner:               team.MdmSdk,
		Record:              remotesupportfeedbackdatastream.RemoteSupportFeedbackDataLakeRecord{},
		GeneratedWriterPath: "mobile/remotesupport",
		Description:         "Logs from remote support feedback loop",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "routeplanning_cost_metrics",
		Owner:               team.RoutingPlatform,
		Record:              routeplanningdatastreamtypes.RoutePlanningCostMetrics{},
		GeneratedWriterPath: "fleet/routingplatform/routeplanning/routeplanningdatastreamtypes",
		Description:         "Metrics for calculating the cost of route planning.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "safety_asset_processing_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyAssetProcessingMetrics{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Metrics we want to track for assets that flow through our backend processing pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(timestamp_ms / 1e3))",
		},
	},
	{
		StreamName:          "safety_asset_url_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyAssetUrlMetrics{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains an entry for asset urls requested by users.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:               "safety_assetblurringserver_metrics",
		Owner:                    team.SafetyCameraServices,
		Record:                   safetydatastreamtypes.AssetBlurringServerMetric{},
		GeneratedWriterPath:      "safety/infra/safetydatastreamtypes",
		Description:              "Contains an entry with metadata about blurred assets generated by assetblurringserver.",
		EnableWriteOutsideECSEnv: true,
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "safety_cell_data_usage_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyCellDataUsageMetric{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains an entry for cell usage and associated metadata with that asset uploaded into our backend.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(asset_ms / 1e3))",
		},
	},
	{
		StreamName:          "safety_cvworker_audio_deprecation_logs",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyCvWorkerDeprecationLog{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs the cvworker and dashcamprocessor assets consumed to see whether cvworker audio attachment can be moved.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(timestamp_ms / 1e3))",
		},
	},
	{
		StreamName:          "safety_ser_sla_bucket_metrics",
		Owner:               team.SafetyWorkflow,
		Record:              safetydatastreamtypes.SafetySerSlaBucketMetrics{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs request volumes in Safety Event Review (SER) SLA buckets.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(timestamp_ms / 1e3))",
		},
	},
	{
		StreamName:          "safety_webrtc_livestream_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyVideoWebrtcLivestreamMetric{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs data about the lifespan (start->stop) of a Fleet WebRTC livestream after it is stopped. It contains information about the Webrtc request, the client, and connection events",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(start_unix_ms / 1e3))",
		},
	},
	{
		StreamName:          "safety_workflow_metrics",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.SafetyWorkflowMetrics{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Metrics we want to track in our safety workflows pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(timestamp_ms / 1e3))",
		},
	},
	{
		StreamName:          "scientist_experiment_logs",
		Owner:               team.DeveloperExperience,
		Record:              scientistdatastream.ExperimentResultLog{},
		GeneratedWriterPath: "helpers/scientist/scientistdatastream",
		Description:         "Logs scientist experiment output",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"experiment_name", "date"},
		},
	},
	{
		StreamName:          "ser_event_trace",
		Owner:               team.SafetyDetectionPrecision,
		Record:              sertracerecords.SERCheckpointRecord{},
		GeneratedWriterPath: "safety/sertrace/records",
		Description:         "Records checkpoints for events flowing through the Safety Event Review pipeline, including policy decisions and processing status.",
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "recorded_at",
		},
	},
	{
		StreamName:          "smart_maps_ingested_images",
		Owner:               team.SmartMaps,
		Record:              smartmapsdatastreamtypes.SmartMapsIngestedImageMetric{},
		GeneratedWriterPath: "safety/geospatial/smartmapsdatastreamtypes",
		Description:         "logs data about images ingested by the image ingestion worker",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "smart_maps_road_condition_notifications",
		Owner:               team.SmartMaps,
		Record:              smartmapsdatastreamtypes.SmartMapsRoadConditionsNotificationMetric{},
		GeneratedWriterPath: "safety/geospatial/smartmapsdatastreamtypes",
		Description:         "logs data about road condition changes for devices",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "smart_maps_vehicle_settings_changes",
		Owner:               team.SmartMaps,
		Record:              smartmapsdatastreamtypes.SmartMapsDynamicSettingsChangeMetric{},
		GeneratedWriterPath: "safety/geospatial/smartmapsdatastreamtypes",
		Description:         "logs data about vehicle settings upgrades and downgrades in the dynamicsettingsworker when vehicles enter or exit severe weather conditions",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "speed_limit_locations",
		Owner:               team.SafetyGeoDetection,
		Record:              safetydatastreamtypes.SpeedLimitLocation{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs location data read from SpeedLimitLocationsReader",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(time / 1e3))",
		},
	},
	{
		StreamName:          "speed_spike_detections",
		Owner:               team.SafetyGeoDetection,
		Record:              safetydatastreamtypes.SpeedSpikeDetection{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains data about each detected speed spike",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "statsprocessor_future_timestamp_logs_dropped",
		Owner:               team.Firmware,
		Record:              firmwaredatastreamtypes.EncodedLog{},
		GeneratedWriterPath: "firmware/firmwaredatastreamtypes",
		Description:         "Firmware logs (i.e. object stats and other log types) that are dropped by statsprocessor on the ingestion write path because their timestamp is in the future by more than the configured threshold.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "stream_publish_logs",
		Owner:               team.DevEcosystem,
		Record:              streampublisherdatastreamtypes.StreamPublishLogDataLakeMetric{},
		GeneratedWriterPath: "infra/api/streaming/apipublisher",
		Description:         "Contains a log for each message published to Kafka via the streaming API.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "tile_generation_logs",
		Owner:               team.SafetyGeoDetection,
		Record:              safetydatastreamtypes.TileGenerationDataLakeMetric{},
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Contains an entry for every firmware, speed limit, and stop location tile generated. Entries include data surrounding tile size and osm features present.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "transcodeworker_processing_duration_metrics",
		Owner:               team.SafetyPlatform,
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		Record:              safetydatastreamtypes.TranscodeworkerProcessingDurationMetric{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Reports durations for backend transcoding processing, and provides other fields to help slice the data. Contains a record for each message that goes through the transcoding pipeline. Each message is an mp4, but there may be multiple records for a single mp4 if it gets enqueued multiple times.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(from_unixtime(asset_ms / 1e3))",
		},
	},
	{
		StreamName:          "triage_event_filter_checkpoint_status",
		Owner:               team.SafetyPlatform,
		Tags:                []amundsentags.Tag{amundsentags.SafetyTag},
		Record:              safetydatastreamtypes.TriageEventFilterCheckpoint{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs checkpoints for incoming safety events that are passing through triage event filters. Helps with identifying why events were dropped by filters based on metadata available at point of ingestion.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "vapi_webhook_messages",
		Owner:               team.WorkerSafety,
		Record:              vapidatastreamtypes.VapiWebhookMessageDataLakeRecord{},
		GeneratedWriterPath: "workersafety/vapidatastreamtypes/webhookmessageswriter",
		Description:         "Logs messages received from Vapi Webhook",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "vbs_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.VehicleBlindSpotInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Log results of inference from the vehicle blind spot backend pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "vehicle_misuse_anomalies",
		Owner:               team.FleetSec,
		Record:              anomalydatastreamtypes.VehicleMisuseAnomaly{},
		GeneratedWriterPath: "fleet/security/anomalydetection/anomalydatastreamtypes",
		Description:         "Contains analysis of the anomalous trips that happened outside of the usual operating hours.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "vehicle_positioning_inference_results",
		Owner:               team.MLInfra,
		Record:              mldatastreams.VehiclePositioningInferenceResults{},
		GeneratedWriterPath: "ml/models/mldatastreams",
		Description:         "Inference results from the vehicle positioning model pipeline.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "video_retrieval_cancelled_log",
		Owner:               team.SafetyCameraServices,
		Record:              safetydatastreamtypes.VideoRetrievalCancelledLog{},
		GeneratedWriterPath: "safety/infra/safetydatastreamtypes",
		Description:         "Logs timestamps when user cancelled a safety video retrieval.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "vin_decoding_logs",
		Owner:               team.DiagnosticsTools,
		Record:              vindecodingdatastreamtypes.VinDecodingResultLog{},
		GeneratedWriterPath: "fleet/vin/vindecoding/vindecodingdatastreamtypes",
		Description:         "Logs the results of each VIN decoding request.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "webhook_logs",
		Owner:               team.DevEcosystem,
		Record:              devecodatastreamtypes.WebhookAccessLogDataLakeMetric{},
		GeneratedWriterPath: "infra/api/webhooks",
		Description:         "Contains a log for each webhook sent out.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "workflow_alert_incident_comparison_logs",
		Owner:               team.PlatformAlertsWorkflows,
		Record:              workflowsdatastreamtypes.WorkflowAlertIncidentComparison{},
		GeneratedWriterPath: "platform/workflows/validator",
		Description:         "Contains a log for each workflow-alert comparison performed by the workflows validator.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:   []string{"date"},
			DateColumnOverride: "timestamp",
		},
	},
	{
		StreamName:          "workforce_camera_device_add_metrics",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceCameraDeviceAddMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs when a camera device has been added to a gateway. It contains the information saved on a camera device, additional information about the camera device that was gathered during discovery, and information about the camera addition transaction.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(created_at)",
		},
	},
	{
		StreamName:          "workforce_camera_stream_add_metrics",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceCameraStreamAddMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs information about each camera stream when its camera device has been added to a gateway. It contains information saved on the camera stream, as well as information about how it was discovered and configured, and information about the camera addition transaction.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns:             []string{"date"},
			DateColumnOverrideExpression: "date(created_at)",
		},
	},
	{
		StreamName:          "workforce_local_storage_and_retention_logs",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.LocalStorageAndRetentionMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs site gateway local storage and retention metrics on page load, including projected retention and actual oldest footage.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_media_upload_recovery_attempts",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.SitesMediaUploadRecoveryEvent{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs Media Upload Recovery cron job's recovery attempts for each media_source (camera stream, channel).",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_hls_client_fetch_segment",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoHlsClientFetchSegmentMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs when an HLS video segment is fetched by the browser, how long it took to fetch the segment, additional error information when fetching, and segment metadata.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_hls_server_fetch_segment",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoHlsServerFetchSegmentMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs when an HLS video segment is fetched on the server side, how long it took to fetch the segment, additional error information, and segment metadata.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_hls_stream_error_end",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoHlsStreamErrorSessionEnd{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logged when an HLS stream error state ends. This can be logged when the stream is no longer in an error state, if it enters a different kind of error state, or if the stream page is exited in some way.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_hls_stream_error_start",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoHlsStreamErrorSessionStart{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logged when an HLS stream error state is entered meaning when a new HLS error is encountered. For example, if there is no current error then a buffer stalled error occurs, this metric would be logged. If later a manifest parsed error occurs, the current error session would end and another metric would be logged for a new error session starting.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_stream_end_logs",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoStreamEndLog{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logged when an HLS streaming session ends. For example, when the user navigates away from the page. Its uuid can be correlated with WorkforceVideoStreamStart in bigquery.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_stream_metric_intervals",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoStreamMetricInterval{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs an interval of quality for HLS streaming. Its uuid can be correlated with WorkforceVideoStreamStart in bigquery.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_streaming_page_states",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.SitesVideoStreamingPageState{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs the different streaming states (HLS, webRTC, empty state X) of cameras within streaming page loads",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_unhealthy_webrtc_intervals",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.SitesVideoUnhealthyWebrtcInterval{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs unhealthy Sites WebRTC livestream intervals.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_webrtc_fallback_events",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.SitesVideoWebrtcFallbackEvent{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs data about Sites WebRTC fallback events (rendering different MediaStreams based on performance) within the video player.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_webrtc_livestream_metrics",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.WorkforceVideoWebrtcLivestreamMetric{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs data about the lifespan (start->stop) of a Sites WebRTC livestream after it is stopped. It contains information about the Webrtc request, the client, and connection events",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
	{
		StreamName:          "workforce_video_webrtc_sample_interval",
		Owner:               team.ConnectedWorker,
		Record:              workforcedatastreamtypes.SitesVideoWebrtcSampleInterval{},
		GeneratedWriterPath: "connectedworker/workforce/workforcedatastreamtypes",
		Description:         "Logs Sites WebRTC livestream metadata per stream at a set interval, including the inbound-rtp stat and average FPS over that timeframe.",
		PartitionStrategy: &PartitionStrategy{
			PartitionColumns: []string{"date"},
		},
	},
}

func validateRegistry() error {
	for _, entry := range Registry {
		if !IsValidStreamName(entry.StreamName) {
			log.Panicf("StreamName %s is not valid. Stream names should be snake_case with lowercase letters & digits, not start with a digit, at least two characters, but no more than %d characters.", entry.StreamName, maxSamsaraDefinedNameLength)
		}

		if _, err := os.Stat(filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io", entry.GeneratedWriterPath)); os.IsNotExist(err) {
			log.Panicf("StreamName %s GeneratedWriterPath does not exist. Please make sure the value for GeneratedWriterPath exists relative to go/src/samsaradev.io", entry.StreamName)
		}
	}

	return nil
}

// Regex for a valid stream name
// Stream names should be snake_case with lowercase letters & digits, not start with a digit, and at least two characters
var streamNameRe = regexp.MustCompile("^[a-z]+([a-z0-9]*_[a-z0-9]*)*[a-z0-9]+$")

func IsValidStreamName(s string) bool {
	if len(s) > maxSamsaraDefinedNameLength {
		return false
	}
	return streamNameRe.MatchString(s)
}
