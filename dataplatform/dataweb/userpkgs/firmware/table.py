from enum import Enum
from typing import Union

from ..constants import Database


class ProductAnalytics(str, Enum):
    DIM_POPULATIONS = "dim_populations"
    AGG_MMYEF_PROMOTIONS = "agg_mmyef_promotions"
    AGG_DEVICE_STATS_PRIMARY = "agg_device_stats_primary"
    DIM_DEVICE_DIMENSIONS = "dim_device_dimensions"
    DIM_ATTACHED_USB_DEVICES = "dim_attached_usb_devices"
    DIM_DIAGNOSTIC_CABLE = "dim_diagnostic_cable"
    DIM_FIRMWARE_BUILDS = "dim_firmware_builds"
    AGG_DEVICE_STATS_SECONDARY_CONSISTENCY = "agg_device_stats_secondary_consistency"
    AGG_DEVICE_STATS_SECONDARY_COVERAGE = "agg_device_stats_secondary_coverage"
    AGG_DEVICE_STATS_SECONDARY_HISTOGRAM_REPORTED = (
        "agg_device_stats_secondary_histogram_reported"
    )
    AGG_DEVICE_STATS_SECONDARY_NONZERO = "agg_device_stats_secondary_nonzero"
    AGG_DEVICE_STATS_SECONDARY_RATE = "agg_device_stats_secondary_rate"
    AGG_DEVICE_STATS_SECONDARY_RATIO = "agg_device_stats_secondary_ratio"
    AGG_DEVICE_STATS_SECONDARY_RESPONSE_RATIO = (
        "agg_device_stats_secondary_response_ratio"
    )
    AGG_DEVICE_STATS_SECONDARY_ANOMALY_COUNT = (
        "agg_device_stats_secondary_anomaly_count"
    )
    AGG_POPULATION_METRICS = "agg_population_metrics"
    AGG_POPULATION_ROLLING_METRICS = "agg_population_rolling_metrics"

    def __str__(self):
        return f"{Database.PRODUCT_ANALYTICS}.{self.value}"


class ProductAnalyticsStaging(str, Enum):
    AGG_STAT_COVERAGE_ON_DRIVE = "agg_stat_coverage_on_drive"
    STG_SEGMENT_CANONICAL_FUEL_TYPES = "stg_segment_canonical_fuel_types"
    STG_DAILY_CAN_CONNECTED = "stg_daily_can_connected"
    FCT_TELEMATICS_MMYEF_FEATURE_FLAG_TARGETING = (
        "fct_telematics_mmyef_feature_flag_targeting"
    )
    FCT_TELEMATICS_STAT_METADATA = "fct_telematics_stat_metadata"
    FCT_TELEMATICS_MARKET_METADATA = "fct_telematics_market_metadata"
    STG_VEHICLE_PROMOTIONS = "stg_vehicle_promotions"
    STG_DAILY_FUEL_TYPE = "stg_daily_fuel_type"
    FCT_TELEMATICS_PRODUCTS = "fct_telematics_products"
    AGG_OSD_COMMAND_SCHEDULER_STATS = "agg_osdcommandschedulerstats"
    AGG_DAILY_OSD_COMMAND_SCHEDULER_STATS = "agg_daily_osdcommandschedulerstats"
    FCT_DERIVED_STAT_COVERAGE_ON_DRIVE = "fct_derivedstatcoverageondrive"
    FCT_OSD_CAN_BITRATE_DETECTION_V2 = "fct_osdcanbitratedetectionv2"
    FCT_CANINTERFACE_BUSID = "fct_caninterface_busid"
    FCT_OSD_CAN_PROTOCOLS_DETECTED = "fct_osdcanprotocolsdetected"
    FCT_OSD_COMMAND_SCHEDULER_STATS = "fct_osdcommandschedulerstats"
    FCT_COMMAND_SCHEDULER_STATS = "fct_command_scheduler_stats"
    FCT_OSD_ENGINE_FAULT = "fct_osdenginefault"
    FCT_OSD_J1708_CAN3_BUS_AUTO_DETECT_RESULT = "fct_osdj1708can3busautodetectresult"
    FCT_OSD_J1939_CLAIMED_ADDRESS = "fct_osdj1939claimedaddress"
    FCT_OSD_OBD_VALUE_LOCK_ADDED = "fct_osdobdvaluelockadded"
    FCT_STATE_CHANGE_EVENTS = "fct_statechangeevents"
    STG_DAILY_FAULTS = "stg_daily_faults"
    FCT_OSD_VIN = "fct_osdvin"
    FCT_OSD_FIRMWARE_METRICS_VDP = "fct_osdfirmwaremetrics_vdp"
    FCT_HOURLY_FUEL_CONSUMPTION = "fct_hourlyfuelconsumption"
    FCT_ENGINE_SEGMENT_METRICS_PROD = "fct_enginestate_segment_metrics_prod"
    FCT_ENGINE_SEGMENT_METRICS_LOG_ONLY = "fct_enginestate_segment_metrics_log_only"
    FCT_ENGINE_STATE_ERRORS = "fct_enginestateerrors"
    FCT_ENGINE_STATE_COMPARISON = "fct_enginestate_comparison"
    FCT_ENGINE_STATE_COMPARISON_STATS = "fct_enginestate_comparison_stats"
    FCT_ENGINE_SEGMENT_STATS = "fct_engine_segment_stats"
    FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN = "fct_osddiagnosticmessagesseen"
    FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN_CLASSIFIED = (
        "fct_osddiagnosticmessagesseen_classified"
    )
    FCT_DIAGNOSTIC_MESSAGES_CONSOLIDATED = "fct_diagnostic_messages_consolidated"
    AGG_SIGNAL_CACHE_SOURCES_BY_OBD_VALUE = "agg_signal_cache_sources_by_obd_value"
    AGG_SIGNAL_SOURCE_COMPARISON_DAILY = "agg_signal_source_comparison_daily"
    AGG_SIGNAL_POPULATION_BEHAVIOR_DAILY = "agg_signal_population_behavior_daily"
    AGG_CAN_PROTOCOLS_DAILY = "agg_can_protocols_daily"
    STG_DEVICE_HWINFO = "stg_device_hwinfo"
    AGG_QUALIFIED_DEVICE_TRIPS = "agg_qualified_device_trips"
    FCT_TELEMATICS_FF_IMPACTED_DEVICES = "fct_telematics_ff_impacted_devices"
    FCT_TELEMATICS_COVERAGE_ROLLUP = "fct_telematics_coverage_rollup"
    FCT_TELEMATICS_COVERAGE_DIMENSIONS = "fct_telematics_coverage_dimensions"
    AGG_TELEMATICS_POPULATIONS = "agg_telematics_populations"
    AGG_TELEMATICS_ACTUAL_COVERAGE = "agg_telematics_actual_coverage"
    AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED = (
        "agg_telematics_actual_coverage_normalized"
    )
    FCT_TELEMATICS_INSTALL_RECOMMENDATIONS = "fct_telematics_install_recommendations"
    AGG_TELEMATICS_INSTALL_RECOMMENDATIONS = "agg_telematics_install_recommendations"
    FCT_TELEMATICS_COVERAGE_SUMMARY = "fct_telematics_coverage_summary"
    AGG_TELEMATICS_COVERAGE_SUMMARY = "agg_telematics_coverage_summary"
    AGG_TELEMATICS_MMYEF_PROMOTION_GAPS = "agg_telematics_mmyef_promotion_gaps"
    AGG_TELEMATICS_PREFERRED_INSTALL = "agg_telematics_preferred_install"
    FCT_TELEMATICS_ISSUE_CLASSIFIER = "fct_telematics_issue_classifier"
    FCT_TELEMATICS_ISSUE_CLASSIFIER_SMOOTHED = (
        "fct_telematics_issue_classifier_smoothed"
    )
    AGG_TELEMATICS_POPULATION_ISSUES = "agg_telematics_population_issues"
    AGG_TELEMATICS_ISSUE_RANKING = "agg_telematics_issue_ranking"
    FCT_VEHICLE_ARCHITECTURE = "fct_vehicle_architecture"
    AGG_TELEMATICS_PRIORITIES = "agg_telematics_priorities"
    FCT_TELEMATICS_VIN_ISSUES = "fct_telematics_vin_issues"
    FCT_TELEMATICS_VIN_GAPS = "fct_telematics_vin_gaps"
    FCT_ENGINE_STATE_METHOD = "fct_enginestate_method"
    AGG_DIAGNOSTICS_COVERAGE_V3_LATEST = "agg_diagnostics_coverage_v3_latest"
    FCT_DIAGNOSTICS_COVERAGE_BY_VIN_LATEST = "fct_diagnostics_coverage_by_vin_latest"
    DIM_DEVICE_POPULATION_MATCHES = "dim_device_population_matches"
    DIM_TELEMATICS_COVERAGE_FULL = "dim_telematics_coverage_full"
    FCT_TELEMATICS_COVERAGE_ROLLUP_FULL = "fct_telematics_coverage_rollup_full"
    FCT_TELEMATICS_COVERAGE_QOQ = "fct_telematics_coverage_qoq"
    AGG_TELEMATICS_COVERAGE_QOQ = "agg_telematics_coverage_qoq"
    AGG_TELEMATICS_COVERAGE_QOQ_NORMALIZED = "agg_telematics_coverage_qoq_normalized"
    FCT_CAN_RECORDING_WINDOWS = "fct_can_recording_windows"
    FCT_ELIGIBLE_CAN_RECORDING_WINDOWS = "fct_eligible_can_recording_windows"
    FCT_CAN_TRACE_STATUS = "fct_can_trace_status"
    FCT_REVERSE_ENGINEERING_TRACE_CANDIDATES = (
        "fct_reverse_engineering_trace_candidates"
    )
    AGG_DEVICE_STATS_QUANTILES = "agg_device_stats_quantiles"
    AGG_POPULATION_QUANTILES = "agg_population_quantiles"
    AGG_POPULATION_QUANTILE_ROLLUP = "agg_population_quantile_rollup"
    AGG_CAN_TRACE_COLLECTION_COVERAGE = "agg_can_trace_collection_coverage"
    FCT_SAMPLED_CAN_FRAMES = "fct_sampled_can_frames"
    FCT_CAN_TRACE_RECOMPILED = "fct_can_trace_recompiled"
    FCT_CAN_TRACE_DECODED = "fct_can_trace_decoded"
    FCT_CAN_TRACE_ASC_CHUNKS = "fct_can_trace_asc_chunks"
    FCT_CAN_TRACE_EXPORTS = "fct_can_trace_exports"
    DIM_J1939_SIGNAL_DEFINITIONS = "dim_j1939_signal_definitions"
    FCT_TELEMATICS_DATA_QUALITY_CUTOFFS = "fct_telematics_data_quality_cutoffs"
    FCT_TELEMATICS_DATA_QUALITY_OUTLIERS = "fct_telematics_data_quality_outliers"
    DIM_COMBINED_SIGNAL_DEFINITIONS = "dim_combined_signal_definitions"
    DIM_DEVICE_VEHICLE_PROPERTIES = "dim_device_vehicle_properties"
    DIM_MMYEF_VEHICLE_CHARACTERISTICS = "dim_mmyef_vehicle_characteristics"
    STG_DAILY_FUEL_PROPERTIES = "stg_daily_fuel_properties"
    DIM_TRACE_CHARACTERISTICS = "dim_trace_characteristics"
    MAP_CAN_CLASSIFICATIONS = "map_can_classifications"
    DIM_COMBINED_PASSENGER_SIGNAL_DEFINITIONS = (
        "dim_combined_passenger_signal_definitions"
    )
    DIM_SIGNAL_CATALOG_DEFINITIONS = "dim_signal_catalog_definitions"
    FCT_TELEMATICS_PROMOTION_GAPS = "fct_telematics_promotion_gaps"
    FCT_DTC_EVENTS_DAILY = "fct_dtc_events_daily"
    AGG_MONTHLY_DTC_EVENTS_BY_MMYEF = "agg_monthly_dtc_events_by_mmyef"
    FCT_SIGNAL_PROMOTION_TRANSITIONS = "fct_signal_promotion_transitions"
    FCT_SIGNAL_PROMOTION_LATEST_STATE = "fct_signal_promotion_latest_state"
    AGG_SIGNAL_PROMOTION_DAILY_METRICS = "agg_signal_promotion_daily_metrics"
    AGG_SIGNAL_PROMOTION_TRANSITION_METRICS = "agg_signal_promotion_transition_metrics"
    DIM_PROMOTION_POPULATION_MAPPING = "dim_promotion_population_mapping"
    DIM_TABLE_OWNERSHIP = "dim_table_ownership"
    FCT_TABLE_COST_DAILY = "fct_table_cost_daily"
    AGG_COSTS_ROLLING = "agg_costs_rolling"
    FCT_TABLE_COST_ANOMALIES = "fct_table_cost_anomalies"
    DIM_COST_ALERTS = "dim_cost_alerts"
    FCT_TABLE_USER_QUERIES = "fct_table_user_queries"
    AGG_TABLE_USAGE_ROLLING = "agg_table_usage_rolling"
    AGG_USER_ACTIVITY_ROLLING = "agg_user_activity_rolling"
    AGG_DASHBOARD_ENGAGEMENT_ROLLING = "agg_dashboard_engagement_rolling"
    FCT_DASHBOARD_USAGE_DAILY = "fct_dashboard_usage_daily"
    DIM_DASHBOARDS = "dim_dashboards"
    DIM_DASHBOARD_HIERARCHIES = "dim_dashboard_hierarchies"
    AGG_DASHBOARD_ACTIVITY_ROLLING = "agg_dashboard_activity_rolling"
    DIM_USER_HIERARCHIES = "dim_user_hierarchies"
    AGG_PLATFORM_USAGE_ROLLING = "agg_platform_usage_rolling"
    DIM_BILLING_SERVICES = "dim_billing_services"
    DIM_COST_HIERARCHIES = "dim_cost_hierarchies"
    DIM_TABLES = "dim_tables"
    FCT_PARTITION_LANDINGS = "fct_partition_landings"
    AGG_LANDING_RELIABILITY_ROLLING = "agg_landing_reliability_rolling"
    # CAN trace collection pipeline tables
    FCT_CAN_TRACE_REVERSE_ENGINEERING_CANDIDATES = "fct_can_trace_reverse_engineering_candidates"
    FCT_ELIGIBLE_TRACES_BASE = "fct_eligible_traces_base"
    FCT_CAN_TRACE_REPRESENTATIVE_CANDIDATES = "fct_can_trace_representative_candidates"
    AGG_TAGS_PER_MMYEF = "agg_tags_per_mmyef"
    AGG_TAGS_PER_DEVICE = "agg_tags_per_device"
    AGG_MMYEF_STREAM_IDS_DAILY = "agg_mmyef_stream_ids_daily"
    AGG_MMYEF_STREAM_IDS = "agg_mmyef_stream_ids"
    FCT_CAN_TRACE_TRAINING_BY_STREAM_ID = "fct_can_trace_training_by_stream_id"
    FCT_CAN_TRACE_SEATBELT_TRIP_START_CANDIDATES_BASE = "fct_can_trace_seatbelt_trip_start_candidates_base"
    FCT_CAN_TRACE_SEATBELT_TRIP_START_CANDIDATES = "fct_can_trace_seatbelt_trip_start_candidates"

    def __str__(self):
        return f"{Database.PRODUCT_ANALYTICS_STAGING}.{self.value}"


class DataModelCore(str, Enum):
    DIM_DEVICES = "dim_devices"
    DIM_ORGANIZATIONS = "dim_organizations"
    DIM_PRODUCT_VARIANTS = "dim_product_variants"

    def __str__(self):
        return f"{Database.DATAMODEL_CORE}.{self.value}"


class DataModelTelematics(str, Enum):
    FCT_TRIPS_DAILY = "fct_trips_daily"
    FCT_TRIPS = "fct_trips"

    def __str__(self):
        return f"{Database.DATAMODEL_TELEMATICS}.{self.value}"


class DataPrep(str, Enum):
    FUEL_PROPERTIES = "fuel_properties"

    def __str__(self):
        return f"{Database.DATAPREP}.{self.value}"


class DataPrepFirmware(str, Enum):
    GATEWAY_DAILY_ROLLOUT_STAGES = "gateway_daily_rollout_stages"

    def __str__(self):
        return f"{Database.DATAPREP_FIRMWARE}.{self.value}"


class FuelDbShards(str, Enum):
    FUEL_TYPES = "fuel_types"

    def __str__(self):
        return f"{Database.FUELDB_SHARDS}.{self.value}"


class DataModelCoreBronze(str, Enum):
    RAW_VINDB_SHARDS_DEVICE_VIN_METADATA = "raw_vindb_shards_device_vin_metadata"

    def __str__(self):
        return f"{Database.DATAMODEL_CORE_BRONZE}.{self.value}"


class Definitions(str, Enum):
    DIAGNOSTIC_BUS_TO_BUS_ID_MAPPING = "diagnostic_bus_to_bus_id_mapping"
    ORGANIZATION_INTERNAL_TYPES = "organization_internal_types"
    PRODUCTS = "products"
    CABLE_TYPE_MAPPINGS = "cable_type_mappings"
    CAN_BUS_EVENTS = "can_bus_events"
    CANONICAL_FUEL_GASOLINE_TYPE = "canonical_fuel_gasoline_type"
    CANONICAL_FUEL_DIESEL_TYPE = "canonical_fuel_diesel_type"
    CANONICAL_FUEL_GASEOUS_TYPE = "canonical_fuel_gaseous_type"
    CANONICAL_FUEL_HYDROGEN_TYPE = "canonical_fuel_hydrogen_type"
    CANONICAL_FUEL_ENGINE_TYPE = "canonical_fuel_engine_type"
    CANONICAL_FUEL_BATTERY_CHARGE_TYPE = "canonical_fuel_battery_charge_type"
    PROMOTIONS = "promotions"
    OBD_VALUES = "obd_values"
    VEHICLE_DIAGNOSTIC_BUS = "vehicle_diagnostic_bus"
    TELEMATICS_LD_TARGETS = "telematics_ld_targets"
    TELEMATICS_LD_METADATA = "telematics_ld_metadata"
    TELEMATICS_MARKET_PRIORITY = "telematics_market_priority"
    TELEMATICS_PRODUCT_RANK = "telematics_product_rank"
    TELEMATICS_ISSUE_RANK = "telematics_issue_rank"
    TELEMATICS_ISSUE_IMPACTS = "telematics_issue_impacts"
    TELEMATICS_STAT_METADATA = "telematics_stat_metadata"
    TELEMATICS_OBD_VALUE_MAPPING = "telematics_obd_value_mapping"
    TELEMATICS_ISSUES = "telematics_issues"
    J1939_DA_2023 = "j1939_da_2023"
    J1939_TXID_TO_SOURCE = "j1939_txid_to_source"
    CAN_BUS_TYPES = "can_bus_types"
    TELEMATICS_WMI_ISSUES = "telematics_wmi_issues"
    TELEMATICS_APPLICABLE_SIGNALS = "telematics_applicable_signals"
    TELEMATICS_PROMOTION_GAP_REVIEW = "telematics_promotion_gap_review"
    OBD_PROTOCOL = "obd_protocol"
    PASSENGER_STANDARD_EMITTER_SIGNALS = "passenger_standard_emitter_signals"
    J1979_DA_ANNEX_B_2025_04 = "j1979_da_annex_b_2025_04"
    GLOBAL_OBD_CONFIG_PASSENGER_SIGNAL_DEFS = "global_obd_config_passenger_signal_defs"
    PROMOTION_STAGE = "promotion_stage"
    PROMOTION_STATUS = "promotion_status"
    ACTIVITY_TYPE = "activity_type"
    PROPERTIES_FUEL_FUELGROUP = "properties_fuel_fuelgroup"
    PROPERTIES_FUEL_POWERTRAIN = "properties_fuel_powertrain"

    def __str__(self):
        return f"{Database.DEFINITIONS}.{self.value}"


class Dojo(str, Enum):
    SIGNAL_REVERSE_ENGINEERING_PREDICTIONS = "signal_reverse_engineering_predictions"
    SIGNAL_REVERSE_ENGINEERING_PREDICTIONS_V2 = (
        "signal_reverse_engineering_predictions_v2"
    )

    def __str__(self):
        return f"{Database.DOJO}.{self.value}"


class KinesisStatsHistory(str, Enum):
    OSD_DIAGNOSTIC_MESSAGES_SEEN = "osddiagnosticmessagesseen"
    OSD_ENGINE_STATE = "osdenginestate"
    OSD_ENGINE_STATE_LOG_ONLY = "osdenginestatelogonly"
    OSD_CAN_CONNECTED = "osdcanconnected"
    OSD_CAN_RECORDER_INFO = "osdcanrecorderinfo"
    OSD_J1939_DD_FUEL_LEVEL_2_DECI_PERCENT = "osdj1939ddfuellevel2decipercent"
    OSD_ENGINE_GAUGE = "osdenginegauge"
    OSD_ENGINE_SECONDS = "osdengineseconds"
    OSD_ENGINE_MILLI_KNOTS = "osdenginemilliknots"
    OSD_ENGINE_ACTIVITY_METHOD = "osdengineactivitymethod"
    OSD_POWER_STATE = "osdpowerstate"
    LOCATION = "location"
    OSD_ENGINE_FAULT = "osdenginefault"
    OSD_CAN_PROTOCOLS_DETECTED = "osdcanprotocolsdetected"
    OSD_OBD_PROTOCOL = "osdobdprotocol"
    OSD_VIN = "osdvin"

    def __str__(self):
        return f"{Database.KINESISSTATS_HISTORY}.{self.value}"


class KinesisStats(str, Enum):
    OSD_MODI_DEVICE_INFO = "osdmodideviceinfo"
    OSD_J1708_CAN3_BUS_AUTO_DETECT_RESULT = "osdj1708can3busautodetectresult"
    OSD_DIAGNOSTIC_MESSAGES_SEEN = "osddiagnosticmessagesseen"
    OSD_OBD_CABLE_ID = "osdobdcableid"
    OSD_ENGINE_GAUGE = "osdenginegauge"
    OSD_HUB_SERVER_DEVICE_HEARTBEAT = "osdhubserverdeviceheartbeat"
    OSD_AT1_DPF_SOOT_LOAD_PERCENT = "osdat1dpfsootloadpercent"
    OSD_COASTING_TIME_TORQUE_BASED_IGNORING_BRAKE_MS = (
        "osdcoastingtimetorquebasedignoringbrakems"
    )
    OSD_DEF_TANK_LEVEL_MILLI_PERCENT = "osddeftanklevelmillipercent"
    OSD_DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_MS = "osddeltaaccelenginetorqueoverlimitms"
    OSD_DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_WHILE_NOT_ON_CRUISE_CONTROL_MS = (
        "osddeltaaccelenginetorqueoverlimitwhilenotoncruisecontrolms"
    )
    OSD_DELTA_ACCELERATOR_PEDAL_TIME_GREATER_THAN_95_PERCENT_MS = (
        "osddeltaacceleratorpedaltimegreaterthan95percentms"
    )
    OSD_DELTA_ANTICIPATION_BRAKE_EVENTS = "osddeltaanticipationbrakeevents"
    OSD_DELTA_BRAKE_EVENTS = "osddeltabrakeevents"
    OSD_DELTA_COASTING_TIME_MS = "osddeltacoastingtimems"
    OSD_DELTA_COASTING_TIME_TORQUE_BASED_WHILE_NOT_ON_CRUISE_CONTROL_MS = (
        "osddeltacoastingtimetorquebasedwhilenotoncruisecontrolms"
    )
    OSD_DELTA_COASTING_TIME_WHILE_NOT_ON_CRUISE_CONTROL_MS = (
        "osddeltacoastingtimewhilenotoncruisecontrolms"
    )
    OSD_DELTA_CRUISE_CONTROL_MS = "osddeltacruisecontrolms"
    OSD_DELTA_EV_CHARGING_ENERGY_MICRO_WH = "osddeltaevchargingenergymicrowh"
    OSD_DELTA_EV_DISTANCE_DRIVEN_ON_ELECTRIC_POWER_METERS = (
        "osddeltaevdistancedrivenonelectricpowermeters"
    )
    OSD_DELTA_EV_ENERGY_CONSUMED_MICRO_WH = "osddeltaevenergyconsumedmicrowh"
    OSD_DELTA_EV_TOTAL_ENERGY_REGENERATED_WHILE_DRIVING_MICRO_WH = (
        "osddeltaevtotalenergyregeneratedwhiledrivingmicrowh"
    )
    OSD_DELTA_FUEL_CONSUMED = "osddeltafuelconsumed"
    OSD_DELTA_GPS_DISTANCE = "osddeltagpsdistance"
    OSD_DELTA_RPM_GREEN_BAND_MS = "osddeltarpmgreenbandms"
    OSD_DERIVED_ENGINE_SECONDS_FROM_IGNITION_CYCLE = (
        "osdderivedenginesecondsfromignitioncycle"
    )
    OSD_ENGINE_MILLI_KNOTS = "osdenginemilliknots"
    OSD_ENGINE_SECONDS = "osdengineseconds"
    OSD_ENGINE_STATE = "osdenginestate"
    OSD_EV_AVERAGE_CELL_TEMPERATURE_MILLI_CELSIUS = (
        "osdevaveragecelltemperaturemillicelsius"
    )
    OSD_EV_CHARGER_MILLI_AMP = "osdevchargermilliamp"
    OSD_EV_CHARGER_MILLI_VOLT = "osdevchargermillivolt"
    OSD_EV_CHARGING_STATUS = "osdevchargingstatus"
    OSD_EV_CHARGING_TYPE = "osdevchargingtype"
    OSD_EV_HIGH_CAPACITY_BATTERY_MILLI_AMP = "osdevhighcapacitybatterymilliamp"
    OSD_EV_HIGH_CAPACITY_BATTERY_MILLI_VOLT = "osdevhighcapacitybatterymillivolt"
    OSD_EV_HIGH_CAPACITY_BATTERY_STATE_OF_HEALTH_MILLI_PERCENT = (
        "osdevhighcapacitybatterystateofhealthmillipercent"
    )
    OSD_EV_LAST_CHARGE_AC_WALL_ENERGY_CONSUMED_WH = (
        "osdevlastchargeacwallenergyconsumedwh"
    )
    OSD_EV_USABLE_STATE_OF_CHARGE_MILLI_PERCENT = "osdevusablestateofchargemillipercent"
    OSD_FUEL_TYPE = "osdfueltype"
    OSD_J1939_CVW_GROSS_COMBINATION_VEHICLE_WEIGHT_KILOGRAMS = (
        "osdj1939cvwgrosscombinationvehicleweightkilograms"
    )
    OSD_ODOMETER = "osdodometer"
    OSD_POWER_TAKE_OFF = "osdpowertakeoff"
    OSD_POWER_STATE = "osdpowerstate"
    OSD_SEAT_BELT_DRIVER = "osdseatbeltdriver"
    OSD_DPF_LAMP_STATUS = "osddpflampstatus"
    OSD_DOOR_LOCK_STATUS = "osddoorlockstatus"
    OSD_DOOR_OPEN_STATUS = "osddooropenstatus"
    OSD_VEHICLE_CURRENT_GEAR = "osdvehiclecurrentgear"
    OSD_TELL_TALE_STATUS = "osdtelltalestatus"
    OSD_J1939_OEL_TURN_SIGNAL_SWITCH_STATE = "osdj1939oelturnsignalswitchstate"
    OSD_J1939_LCMD_LEFT_TURN_SIGNAL_LIGHTS_COMMAND_STATE = (
        "osdj1939lcmdleftturnsignallightscommandstate"
    )
    OSD_J1939_LCMD_RIGHT_TURN_SIGNAL_LIGHTS_COMMAND_STATE = (
        "osdj1939lcmdrightturnsignallightscommandstate"
    )
    OSD_J1939_ETC5_TRANSMISSION_REVERSE_DIRECTION_SWITCH_STATE = (
        "osdj1939etc5transmissionreversedirectionswitchstate"
    )
    OSD_J1939_TCO1_DIRECTION_INDICATOR_STATE = "osdj1939tco1directionindicatorstate"
    OSD_J1939_VDC2_STEERING_WHEEL_ANGLE_PICO_RAD = (
        "osdj1939vdc2steeringwheelanglepicorad"
    )
    OSD_J1939_VDC2_YAW_RATE_FEMTO_RAD_PER_SECONDS = (
        "osdj1939vdc2yawratefemtoradperseconds"
    )
    OSD_REEFER_ACTIVE_ZONES = "osdreeferactivezones"
    OSD_SALT_SPREADER_ACTUAL_SPREAD_RATE_DRY_MILLIGRAMS_PER_METER = (
        "osdsaltspreaderactualspreadratedrymilligramspermeter"
    )
    OSD_SALT_SPREDER_ACTUAL_SPREAD_RATE_PREWET_MILLILITERS_PER_METER = (
        "osdsaltspreaderactualspreadrateprewetmilliliterspermeter"
    )
    OSD_SALT_SPREADER_ACTUAL_SPREAD_RATE_WET_MILLILITERS_PER_METER = (
        "osdsaltspreaderactualspreadratewetmilliliterspermeter"
    )
    OSD_AIR_INLET_PRESSURE_PA = "osdairinletpressurepa"
    OSD_AIR_INLET_TEMP_MILLI_C = "osdairinlettempmillic"
    OSD_SALT_SPREADER_AIR_TEMP_MILLI_C = "osdsaltspreaderairtempmillic"
    OSD_REEFER_RETURN_AIR_TEMP_MILLI_C = "osdreeferreturnairtempmillic"
    OSD_REEFER_RETURN_AIR_TEMP_MILLI_C_ZONE_2 = "osdreeferreturnairtempmilliczone2"
    OSD_REEFER_RETURN_AIR_TEMP_MILLI_C_ZONE_3 = "osdreeferreturnairtempmilliczone3"
    OSD_REEFER_SUPPLY_AIR_TEMP_MILLI_C = "osdreefersupplyairtempmillic"
    OSD_REEFER_SUPPLY_AIR_TEMP_MILLI_C_ZONE_2 = "osdreefersupplyairtempmilliczone2"
    OSD_REEFER_SUPPLY_AIR_TEMP_MILLI_C_ZONE_3 = "osdreefersupplyairtempmilliczone3"
    OSD_INTAKE_MANIFOLD_TEMPERATURE_MICRO_CELSIUS = (
        "osdintakemanifoldtemperaturemicrocelsius"
    )
    OSD_OBD_INTERNAL_START = "osdobdinternalstart"
    OSD_OBD_INTERNAL_END = "osdobdinternalend"
    OSD_J1939_COMPONENT_ID = "osdj1939componentid"
    OSD_LIFETIME_FUEL_CONSUMED = "osdlifetimefuelconsumed"
    OSD_AVERAGE_LINE_TO_NEUTRAL_AC_RMS_MILLI_VOLTS = (
        "osdaveragelinetoneutralacrmsmillivolts"
    )
    OSD_SALT_SPREADER_MAJOR_EVENT = "osdsaltspreadermajorevent"
    OSD_REEFER_COMMAND_METADATA = "osdreefercommandmetadata"
    OSD_OBD_COMPLIANCE_STANDARD = "osdobdcompliancestandard"
    OSD_REEFER_ON_HOURS = "osdreeferonhours"
    OSD_PHASE_A_AMPS_RMS = "osdphaseaampsrms"
    OSD_PHASE_A_AC_FREQ_MICRO_HERTZ = "osdphaseaacfreqmicrohertz"
    OSD_PHASE_A_LL_VOLTS_RMS = "osdphaseallvolts"
    OSD_PHASE_A_LN_VOLTS = "osdphasealnvolts"
    OSD_PHASE_B_AMPS_RMS = "osdphasebampsrms"
    OSD_PHASE_B_LL_VOLTS = "osdphasebllvolts"
    OSD_PHASE_B_LN_VOLTS = "osdphaseblnvolts"
    OSD_PHASE_C_AMPS_RMS = "osdphasecamprms"
    OSD_PHASE_C_LL_VOLTS = "osdphasecllvolts"
    OSD_PHASE_C_LN_VOLTS = "osdphaseclnvolts"
    OSD_POWER_FACTOR_RATIO = "osdpowerfactorratio"
    OSD_TACHOGRAPH_VU_DOWNLOAD = "osdtachographvudownload"
    OSD_SALT_SPREADER_QUANTITY_SPREAD_DRY_MILLIGRAMS = (
        "osdsaltspreaderquantityspreaddrymilligrams"
    )
    OSD_SALT_SPREADER_SPREAD_RATE_WET_MILLILITERS_PER_METER = (
        "osdsaltspreaderspreadratewetmilliliterspermeter"
    )
    OSD_COMMAND_SCHEDULER_STATS = "osdcommandschedulerstats"
    OSD_ACCELERATOR_PEDAL_POSITION_DECI_PERCENT = (
        "osdacceleratorpedalpositiondecipercent"
    )
    OSD_ACTUAL_IGNITION_TIMING_NANO_DEGREES = "osdactualignitiontimingnanodegrees"
    OSD_J1939_AEBS1_ADVANCED_EMERGENCY_BRAKING_SYSTEM_STATE_STATE = (
        "osdj1939aebs1advancedemergencybrakingsystemstatestate"
    )
    OSD_AFTERTREATMENT_1_DIESEL_EXHAUST_FLUID_TANK_LEVEL_MICROMETER = (
        "osdaftertreatment1dieselexhaustfluidtanklevelmicrometer"
    )
    OSD_AFTERTREATMENT_1_DIESEL_EXHAUST_FLUID_TANK_VOLUME_DECIPERCENT = (
        "osdaftertreatment1dieselexhaustfluidtankvolumedecipercent"
    )
    OSD_AFTERTREATMENT_REGENERATION_INHIBIT_SWITCH_STATE = (
        "osdaftertreatmentregenerationinhibitswitchstate"
    )
    OSD_REEFER_ALARMS = "osdreeferalarms"
    OSD_REEFER_AMBIENT_AIR_TEMP_MILLI_C = "osdreeferambientairtempmillic"
    OSD_APPARENT_POWER_VOLT_AMPERE = "osdapparentpowervoltampere"
    OSD_AVERAGE_AC_CURRENT_MILLI_AMPS = "osdaverageaccurrentmilliamps"
    OSD_AVERAGE_AC_FREQUENCY_MICRO_HERTZ = "osdaverageacfrequencymicrohertz"
    OSD_BATTERY_POTENTIAL_SWITCHED_MILLI_V = "osdbatterypotentialswitchedmilliv"
    OSD_REEFER_BATT_VOLTAGE_MILLI_V = "osdreeferbattvoltagemilliv"
    OSD_BOOST_PRESSURE_PA = "osdboostpressurepa"
    OSD_BOOST_PRESSURE_TURBOCHARGER_1_PA = "osdboostpressureturbocharger1pa"
    OSD_BOOST_PRESSURE_TURBOCHARGER_2_PA = "osdboostpressureturbocharger2pa"
    OSD_FUEL_CONSUMPTION_RATE_ML_PER_HOUR = "osdfuelconsumptionratemlperhour"
    OSD_REEFER_ATTACHED_CONTROLLER = "osdreeferattachedcontroller"
    OSD_FUEL_DELIVERY_PRESSURE_K_PA = "osdfueldeliverypressurekpa"
    OSD_REEFER_DISPLAY_AIR_TEMP_MILLI_C = "osdreeferdisplayairtempmillic"
    OSD_REEFER_DOOR_OPEN = "osdreeferdooropen"
    OSD_REEFER_DOOR_OPEN_ZONE_1 = "osdreeferdooropenzone1"
    OSD_REEFER_DOOR_OPEN_ZONE_2 = "osdreeferdooropenzone2"
    OSD_REEFER_DOOR_OPEN_ZONE_3 = "osdreeferdooropenzone3"
    OSD_TACHOGRAPH_DOWNLOAD_ERROR = "osdtachographdownloaderror"
    OSD_TACHOGRAPH_DRIVER_INFO = "osdtachographdriverinfo"
    OSD_TACHOGRAPH_DRIVER_STATE = "osdtachographdriverstate"
    OSD_SALT_SPREADER_RATE_DRY_MILLIGRAMS_PER_METER = (
        "osdsaltspreaderratedrymilligramspermeter"
    )
    OSD_ECU_HISTORY_TOTAL_RUN_TIME_SEC = "osdecuhistorytotalruntime"
    OSD_REEFER_ELECTRIC_HOURS = "osdreeferelectrichours"
    OSD_CABLE_VOLTAGE = "osdcablevoltage"
    OSD_J1939_D1_STATUS = "osdj1939d1status"
    OSD_J1939_D2_STATUS = "osdj1939d2status"
    OSD_REEEFER_ENGINE_HOURS_ZONE_2 = "osdreeferenginehourszone2"
    OSD_ENGINE_OIL_TEMP_MICRO_C = "osdengineoiltempmicroc"
    OSD_REEFER_FUEL_LEVEL_PERCENT = "osdreeferfuellevelpercent"
    OSD_SALT_SPREADER_GROUND_SPEED_MILLI_KNOTS = "osdsaltspreadergroundspeedmilliknots"
    OSD_SALT_SPREADER_GROUND_TEMP_MILLI_C = "osdsaltspreadergroundtempmillic"
    OSD_REEFER_HEARTBEAT = "osdreeferheartbeat"
    OSD_REEFER_POWER_STATUS = "osdreeferpowerstatus"
    OSD_REEFER_PRETRIP_STATUS = "osdreeferpretripstatus"
    OSD_SALT_SPREADER_QUANTITY_SPREAD_PREWET_MILLILITERS = (
        "osdsaltspreaderquantityspreadprewetmilliliters"
    )
    OSD_SALT_SPREADER_SPREAD_RATE_PREWET_LITERS_PER_TONNE = (
        "osdsaltspreaderspreadrateprewetlitersperTonne"
    )
    OSD_SALT_SPREADER_SPREAD_RATE_PREWET_MILLILITERS_PER_METER = (
        "osdsaltspreaderspreadrateprewetmilliliterspermeter"
    )
    OSD_EV_RAW_STATE_OF_CHARGE_MILLI_PERCENT = "osdevrawstateofchargemillipercent"
    OSD_REEFER_RUN_MODE = "osdreeferrunmode"
    OSD_REEFER_ROAD_HOURS_ZONE_1 = "osdreeferroadhourszone1"
    OSD_REEFER_ROAD_HOURS_ZONE_2 = "osdreeferroadhourszone2"
    OSD_DISTANCE_SERVICE_METERS = "osddistanceservicemeters"
    OSD_SET_POINT_TEMP_MILLI_C = "osdsetpointtempmillic"
    OSD_SET_POINT_TEMP_MILLI_C_ZONE_2 = "osdsetpointtempmilliczone2"
    OSD_SET_POINT_TEMP_MILLI_C_ZONE_3 = "osdsetpointtempmilliczone3"
    OSD_SMOG_CHECK_RESULTS = "osdsmogcheckresults"
    OSD_SALT_SPREADER_STATE = "osdsaltspreaderstate"
    OSD_REEFER_STATE = "osdreeferstate"
    OSD_REEFER_STATE_ZONE_1 = "osdreeferstatezone1"
    OSD_REEFER_STATE_ZONE_2 = "osdreeferstatezone2"
    OSD_REEFER_STATE_ZONE_3 = "osdreeferstatezone3"
    OSD_TORQUE_PERCENT = "osdtorquepercent"
    OSD_TOTAL_APPARENT_POWER_VOLT_AMPERE = "osdtotalapparentpowervoltampere"
    OSD_TOTAL_KWH_EXPORTED = "osdtotalkwhexported"
    OSD_TOTAL_POWER_KW = "osdtotalpowerkw"
    OSD_TOTAL_REACTIVE_POWER_VOLT_AMPERE_REACTIVE = (
        "osdtotalreactivepowervoltampereactive"
    )
    OSD_TOTAL_REAL_POWER_WATT = "osdtotalrealpowerwatt"
    OSD_SALT_SPREADER_TYPE = "osdsaltspreadertype"
    OSD_CAN_CONNECTED = "osdcanconnected"
    OSD_AIR_FILTER_DIFF_PRESSURE_PA = "osdairfilterdiffpressurepa"
    OSD_J1939_PROP_ANALOG_INPUT_CURRENT_1_MICRO_AMPS = (
        "osdj1939propanaloginputcurrent1microamps"
    )
    OSD_J1939_PROP_ANALOG_INPUT_CURRENT_2_MICRO_AMPS = (
        "osdj1939propanaloginputcurrent2microamps"
    )
    OSD_J1939_PROP_ANALOG_INPUT_CURRENT_3_MICRO_AMPS = (
        "osdj1939propanaloginputcurrent3microamps"
    )
    OSD_J1939_PROP_ANALOG_INPUT_VOLTAGE_1_MILLI_VOLTS = (
        "osdj1939propanaloginputvoltage1millivolts"
    )
    OSD_J1939_PROP_ANALOG_INPUT_VOLTAGE_2_MILLI_VOLTS = (
        "osdj1939propanaloginputvoltage2millivolts"
    )
    OSD_J1939_PROP_ANALOG_INPUT_VOLTAGE_3_MILLI_VOLTS = (
        "osdj1939propanaloginputvoltage3millivolts"
    )
    OSD_J1939_AEBS1_COLLISION_WARNING_LEVEL_STATE = (
        "osdj1939aebs1collisionwarninglevelstate"
    )
    OSD_SULLAIR_COMPRESSOR_DISCHARGE_TEMP_MICRO_CELSIUS = (
        "osdsullaircompressordischargetempmicrocelsius"
    )
    OSD_SULLAIR_COMPRESSOR_FUEL_LEVEL_DECI_PERCENT = (
        "osdsullaircompressorfuelleveldecipercent"
    )
    OSD_SULLAIR_COMPRESSOR_P1_PRESSURE_PA = "osdsullaircompressorp1pressurepa"
    OSD_SULLAIR_COMPRESSOR_P2_PRESSURE_PA = "osdsullaircompressorp2pressurepa"
    OSD_SULLAIR_COMPRESSOR_P3_PRESSURE_PA = "osdsullaircompressorp3pressurepa"
    OSD_CUSTOM_OBD_START = "osdcustomobdstart"
    OSD_DIESEL_PARTICULATE_FILTER_ACTIVE_REGENERATION_INHIBITED_DUE_TO_INHIBIT_SWITCH_STATE = (
        "osddieselparticulatefilteractiveregenerationinhibitedduetoinhibitswitchstate"
    )
    OSD_DIESEL_PARTICULATE_FILTER_ACTIVE_REGENERATION_INHIBITED_STATUS_STATE = (
        "osddieselparticulatefilteractiveregenerationinhibitedstatusstate"
    )
    OSD_ENGINE_DESIRED_IGNITION_TIMING_NANO_DEGREES = (
        "osdenginedesiredignitiontimingnanodegrees"
    )
    OSD_EV_HIGH_CAPACITY_BATTERY_DISPLAY_STATE_OF_CHARGE_MILLI_PERCENT = (
        "osdevhighcapacitybatterydisplaystateofchargemillipercent"
    )
    OSD_J1939_DPF_REGEN_ACTIVE_STATUS = "osdj1939dpfregenactivestatus"
    OSD_J1939_DPF_REGEN_FORCED_STATUS = "osdj1939dpfregenforcedstatus"
    OSD_J1939_DPF_REGEN_NEEDED_STATUS = "osdj1939dpfregenneededstatus"
    OSD_J1939_DPF_REGEN_PASSIVE_STATUS = "osdj1939dpfregenpassivestatus"
    OSD_J1939_FLI1_DRIVER_ALERTNESS_WARNING = "osdj1939fli1driveralertnesswarning"
    OSD_ELD_ODOMETER_METERS = "osdeldodometermeters"
    OSD_EV_HIGH_CAPACITY_BATTERY_REMAINING_ENERGY_WH = (
        "osdevhighcapacitybatteryremainingenergywh"
    )
    OSD_ENGINE_OIL_LEVEL_DECI_PERCENT = "osdengineoilleveldecipercent"
    OSD_ENGINE_THROTTLE_ACTUATOR_1_CONTROL_COMMAND_MICRO_PERCENT = (
        "osdenginethrottleactuator1controlcommandmicropercent"
    )
    OSD_ENGINE_TOTAL_FUEL_USED_MILLI_LITERS = "osdenginetotalfuelusedmilliliters"
    OSD_ENGINE_TOTAL_IDLE_FUEL_USED_MILLI_LITERS = (
        "osdenginetotalidlefuelusedmilliliters"
    )
    OSD_ENGINE_TOTAL_IDLE_TIME_MINUTES = "osdenginetotalidletimeminutes"
    OSD_ENGINE_EXHAUST_MANIFOLD_BANK_1_TEMP_1_MICRO_CELSIUS = (
        "osdengineexhaustmanifoldbank1temp1microcelsius"
    )
    OSD_ENGINE_EXHAUST_MANIFOLD_BANK_2_TEMP_1_MICRO_CELSIUS = (
        "osdengineexhaustmanifoldbank2temp1microcelsius"
    )
    OSD_ENGINE_EXHAUST_TEMP_MICRO_C = "osdengineexhausttempmicroc"
    OSD_FUEL_LEVEL_1_DECI_PERCENT = "osdfuellevel1decipercent"
    OSD_FUEL_LEVEL_2_DECI_PERCENT = "osdfuellevel2decipercent"
    OSD_ENGINE_INTERCOOLER_TEMP_C = "osdengineintercoolertempc"
    OSD_J1939_FLI1_LANE_DEPARTURE_IMMINENT_LEFT_SIDE_STATE = (
        "osdj1939fli1lanedepartureimminentleftsidestate"
    )
    OSD_J1939_FLI1_LANE_DEPARTURE_IMMINENT_RIGHT_SIDE_STATE = (
        "osdj1939fli1lanedepartureimminentrightsidestate"
    )
    OSD_J1939_FLI1_LANE_DEPARTURE_LEFT_SIDE = "osdj1939fli1lanedepartureleftside"
    OSD_J1939_FLI1_LANE_DEPARTURE_RIGHT_SIDE = "osdj1939fli1lanedeparturerightside"
    OSD_J1939_ASSC1_LANE_KEEPING_ASSIST_SYSTEM_STATE = (
        "osdj1939assc1lanekeepingassistsystemstate"
    )
    OSD_OVERALL_POWER_FACTOR_LAGGING_ENUM = "osdoverallpowerfactorlaggingenum"
    OSD_EV_HIGH_CAPACITY_BATTERY_REAL_STATE_OF_CHARGE_MILLI_PERCENT = (
        "osdevhighcapacitybatteryrealstateofchargemillipercent"
    )
    OSD_SCR_SYSTEM_CLEANING_INHIBITED_STATUS_STATE = (
        "osdscrsystemcleaninginhibitedstatusstate"
    )
    OSD_SCR_SYSTEM_CLEANING_INHIBITED_DUE_TO_INHIBIT_SWITCH_STATE = (
        "osdscrsystemcleaninginhibitedduetoinhibitswitchstate"
    )
    OSD_THROTTLE_VALUE_1_POSITION_1_DECI_PERCENT = (
        "osdthrottlevalue1position1decipercent"
    )
    OSD_J1939_AEBS1_TIME_TO_COLLISION_WITH_RELEVANT_OBJECT_CENTI_SECONDS = (
        "osdj1939aebs1timetocollisionwithrelevantobjectcentiseconds"
    )
    OSD_PASSENGER_DTCS = "osdpassengerdtcs"
    OSD_ENGINE_FAULT = "osdenginefault"
    OSD_FUEL_CONSUMED_GASEOUS = "osdfuelconsumedgaseous"
    OSD_AVERAGE_LINE_TO_LINE_ACRMS_MILLI_VOLTS = "osdaveragelinetolineacrmsmillivolts"
    OSD_EXHAUST_GAS_PRESSURE_PA = "osdexhaustgaspressurepa"
    OSD_ATTACHED_USB_DEVICES = "osdattachedusbdevices"
    LOCATION = "location"
    OSD_CAN_BITRATE_DETECTION = "osdcanbitratedetection"
    OSD_CAN_BITRATE_DETECTION_V2 = "osdcanbitratedetectionv2"
    OSD_CAN_PROTOCOLS_DETECTED = "osdcanprotocolsdetected"
    OSD_FIRMWARE_METRICS = "osdfirmwaremetrics"
    OSD_J1939_CLAIMED_ADDRESS = "osdj1939claimedaddress"
    OSD_OBD_VALUE_LOCK_ADDED = "osdobdvaluelockadded"
    OSD_VIN = "osdvin"
    OSD_VDP_SIGNAL_CACHE_SNAPSHOT = "osdvdpsignalcachesnapshot"
    OSG_HARDWARE_INFO = "osghardwareinfo"

    def __str__(self):
        return f"{Database.KINESISSTATS}.{self.value}"


class ProductsDb(str, Enum):
    DEVICES = "devices"

    def __str__(self):
        return f"{Database.PRODUCTS_DB}.{self.value}"


class CloudDb(str, Enum):
    ORGANIZATIONS = "organizations"
    ORG_SFDX_ACCOUNTS = "org_sfdc_accounts"

    def __str__(self):
        return f"{Database.CLOUD_DB}.{self.value}"


class SignalPromotionDb(str, Enum):
    PROMOTIONS = "promotions"
    ACTIVITY_HISTORY = "activity_history"
    POPULATIONS = "populations"
    SIGNALS = "signals"

    def __str__(self):
        return f"{Database.SIGNALPROMOTIONDB}.{self.value}"


class AttributedbShards(str, Enum):
    ATTRIBUTES = "attributes"
    ATTRIBUTE_CLOUD_ENTITIES = "attribute_cloud_entities"
    ATTRIBUTE_VALUES = "attribute_values"

    def __str__(self):
        return f"{Database.ATTRIBUTEDB_SHARDS}.{self.value}"


class Auditlog(str, Enum):
    DIM_DAGSTER_ASSETS = "dim_dagster_assets"
    DATAHUB_SCRAPES = "datahub_scrapes"
    DATABRICKS_TABLES_QUERIED = "databricks_tables_queried"
    LANDED_PARTITIONS = "landed_partitions"
    DQ_CHECK_LOG = "dq_check_log"
    FCT_DATABRICKS_DASHBOARD_EVENTS = "fct_databricks_dashboard_events"

    def __str__(self):
        return f"{Database.AUDITLOG}.{self.value}"


class Billing(str, Enum):
    DATAPLATFORM_COSTS = "dataplatform_costs"

    def __str__(self):
        return f"billing.{self.value}"


DatabaseTable = Union[
    ProductAnalytics,
    ProductAnalyticsStaging,
    DataModelCore,
    DataModelTelematics,
    DataModelCoreBronze,
    DataPrep,
    DataPrepFirmware,
    Definitions,
    KinesisStats,
    CloudDb,
    ProductsDb,
    FuelDbShards,
    Auditlog,
    Billing,
]


class DatamodelCore(str, Enum):
    DIM_DEVICES = "dim_devices"

    def __str__(self):
        return f"{Database.DATAMODEL_CORE}.{self.value}"


class DatamodelCoreBronze(str, Enum):
    RAW_PRODUCTDB_DEVICES = "raw_productdb_devices"
    RAW_VINDB_SHARDS_DEVICE_VIN_METADATA = "raw_vindb_shards_device_vin_metadata"

    def __str__(self):
        return f"{Database.DATAMODEL_CORE_BRONZE}.{self.value}"


class Dynamodb(str, Enum):
    SENSOR_REPLAY_TRACES = "sensor_replay_traces"

    def __str__(self):
        return f"{Database.DYNAMODB}.{self.value}"
