from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_keys_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "driver_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID,
        },
    },
    {
        "name": "last_trip_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Date of the most recent trip for the driver",
        },
    },
    {
        "name": "trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_count_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_distance_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_distance_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_distance_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_count_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_distance_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_distance_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "outward_only_facing_camera_trip_distance_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_count_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_distance_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_distance_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_distance_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_duration_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_duration_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_duration_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_distance_miles_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_distance_miles_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_distance_miles_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_speed_kmph_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_speed_kmph_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_speed_kmph_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "over_speed_limit_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "over_speed_limit_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "over_speed_limit_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_ratio_over_limit_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_ratio_over_limit_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_ratio_over_limit_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_mins_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_mins_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_ratio_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_ratio_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_ratio_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_in_morning_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_in_morning_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_in_morning_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_at_night_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_at_night_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_at_night_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_below_freezing_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_below_freezing_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_below_freezing_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_over_35_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_over_35_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_over_35_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crashes_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crashes_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crashes_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_violations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_violations_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_violations_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_braking_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_braking_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_braking_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_turns_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_turns_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_turns_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "no_seatbelt_violations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "no_seatbelt_violations_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "no_seatbelt_violations_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "distracted_driving_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "distracted_driving_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "distracted_driving_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "rolling_stop_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "rolling_stop_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "rolling_stop_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_accelerations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_accelerations_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_accelerations_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "lane_departure_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "lane_departure_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "lane_departure_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "hos_violation_count_1d",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "hos_violation_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "hos_violation_count_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "total_events_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "total_events_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "total_events_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "coached_events_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "coached_events_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "coached_events_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "virtual_coaching_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "virtual_coaching_30d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "virtual_coaching_60d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_rural_driving_environment_locations_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_rural_driving_environment_locations_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_rural_driving_environment_locations_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_urban_driving_environment_locations_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_urban_driving_environment_locations_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_urban_driving_environment_locations_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_mixed_driving_environment_locations_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_mixed_driving_environment_locations_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_mixed_driving_environment_locations_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_living_street_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_living_street_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_living_street_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_motorway_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_motorway_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_motorway_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_primary_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_primary_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_primary_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_residential_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_residential_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_residential_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_secondary_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_secondary_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_secondary_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_service_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_service_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_service_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_tertiary_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_tertiary_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_tertiary_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_trunk_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_trunk_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_trunk_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_unclassified_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_unclassified_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_unclassified_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_null_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_null_30d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_null_60d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "is_eld_relevant_device",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_vehicle_model",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_vehicle_make",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_max_weight_lbs",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_trip_end_time_ms_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofweek_sin_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofweek_cos_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofyear_sin_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofyear_cos_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "n_org_vg_devices",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "n_org_cm_devices",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_next_1_day",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_next_7_days",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "seatbelt_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "seatbelt_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "inattentive_driving_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "inattentive_driving_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_detection_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_detection_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "rolling_stop_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "in_cab_stop_sign_violation_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "forward_collision_warning_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "forward_collision_warning_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_audio_alerts_enabled_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "driver_tenure_days",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_prev_1_year",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "org_30d_coaching_history",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "org_30d_virtual_coaching_history",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_next_1_day_source",
        "type": {"type": "array", "elementType": "string"},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_next_7_days_source",
        "type": {"type": "array", "elementType": "string"},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "labelled_crash_count_prev_1_year_source",
        "type": {"type": "array", "elementType": "string"},
        "nullable": False,
        "metadata": {},
    },
]

QUERY = """
WITH base AS (
SELECT *,
      RANK() OVER (PARTITION BY driver_id, org_id ORDER BY date desc) AS nth_driving_day
FROM product_analytics_staging.stg_driver_features
WHERE date BETWEEN DATE_SUB({PARTITION_START}, 119) AND {PARTITION_START}),
org_size AS (
SELECT org_id,
       COUNT(DISTINCT CASE WHEN device_type = 'VG - Vehicle Gateway' THEN device_id END) AS n_org_vg_devices,
       COUNT(DISTINCT CASE WHEN device_type = 'CM - AI Dash Cam' THEN device_id END) AS n_org_cm_devices
FROM datamodel_core.lifetime_device_activity
WHERE date = (
    SELECT COALESCE(
        MAX(CASE WHEN date = {PARTITION_START} THEN date END),
        MAX(date)
    )
    FROM datamodel_core.lifetime_device_activity
    WHERE date <= {PARTITION_START}
)
     AND device_type IN ('VG - Vehicle Gateway', 'CM - AI Dash Cam')
     AND l28 > 0
GROUP BY 1),
dim_organizations AS (
SELECT
        org_id,
        account_cs_tier,
        account_size_segment,
        account_arr_segment
FROM datamodel_core.dim_organizations
WHERE date = (
    SELECT COALESCE(
        MAX(CASE WHEN date = {PARTITION_START} THEN date END),
        MAX(date)
    )
    FROM datamodel_core.dim_organizations
    WHERE date <= {PARTITION_START}
)),
driver_crash_counts AS (
SELECT
       org_id,
       driver_id,
       SUM(CASE WHEN date = DATE_ADD({PARTITION_START}, 2) THEN 1 ELSE 0 END) AS labelled_crash_count_next_1_day,
       ARRAY_AGG(DISTINCT CASE WHEN date = DATE_ADD({PARTITION_START}, 2) THEN crash_source ELSE NULL END) AS labelled_crash_count_next_1_day_source,
       SUM(CASE WHEN date BETWEEN DATE_ADD({PARTITION_START}, 2) AND DATE_ADD({PARTITION_START}, 9) THEN 1 ELSE 0 END) AS labelled_crash_count_next_7_days,
       ARRAY_AGG(DISTINCT CASE WHEN date BETWEEN DATE_ADD({PARTITION_START}, 2) AND DATE_ADD({PARTITION_START}, 9) THEN crash_source ELSE NULL END) AS labelled_crash_count_next_7_days_source,
       SUM(CASE WHEN date BETWEEN DATE_SUB({PARTITION_START}, 364) AND {PARTITION_START} THEN 1 ELSE 0 END) AS labelled_crash_count_prev_1_year,
       ARRAY_AGG(DISTINCT CASE WHEN date BETWEEN DATE_SUB({PARTITION_START}, 364) AND {PARTITION_START} THEN crash_source ELSE NULL END) AS labelled_crash_count_prev_1_year_source
FROM product_analytics_staging.stg_crash_trips
WHERE date BETWEEN DATE_SUB({PARTITION_START}, 364) AND DATE_ADD({PARTITION_START}, 9)
GROUP BY 1,2),
safety_org_settings AS (
SELECT
       org_id,
       mobile_usage_enabled,
       CASE WHEN COALESCE(mobile_usage_enabled, FALSE) = FALSE
            THEN FALSE ELSE mobile_usage_audio_alerts_enabled END AS mobile_usage_audio_alerts_enabled,
       seatbelt_enabled,
       CASE WHEN COALESCE(seatbelt_enabled, FALSE) = FALSE
            THEN FALSE ELSE seatbelt_audio_alerts_enabled END AS seatbelt_audio_alerts_enabled,
       inattentive_driving_enabled,
       CASE WHEN COALESCE(inattentive_driving_enabled, FALSE) = FALSE
            THEN FALSE ELSE inattentive_driving_audio_alerts_enabled END AS inattentive_driving_audio_alerts_enabled,
       drowsiness_detection_enabled,
       CASE WHEN COALESCE(drowsiness_detection_enabled, FALSE) = FALSE
            THEN FALSE ELSE drowsiness_detection_audio_alerts_enabled END AS drowsiness_detection_audio_alerts_enabled,
       rolling_stop_enabled,
       CASE WHEN COALESCE(rolling_stop_enabled, FALSE) = FALSE
            THEN FALSE ELSE in_cab_stop_sign_violation_enabled END AS in_cab_stop_sign_violation_enabled,
       lane_departure_warning_enabled,
       CASE WHEN COALESCE(lane_departure_warning_enabled, FALSE) = FALSE
            THEN FALSE ELSE lane_departure_warning_audio_alerts_enabled END AS lane_departure_warning_audio_alerts_enabled,
       forward_collision_warning_enabled,
       CASE WHEN COALESCE(forward_collision_warning_enabled, FALSE) = FALSE
            THEN FALSE ELSE forward_collision_warning_audio_alerts_enabled END AS forward_collision_warning_audio_alerts_enabled,
       following_distance_enabled,
       CASE WHEN COALESCE(following_distance_enabled, FALSE) = FALSE
            THEN FALSE ELSE following_distance_audio_alerts_enabled END AS following_distance_audio_alerts_enabled
FROM product_analytics.dim_organizations_safety_settings
WHERE date = (
    SELECT COALESCE(
        MAX(CASE WHEN date = {PARTITION_START} THEN date END),
        MAX(date)
    )
    FROM product_analytics.dim_organizations_safety_settings
    WHERE date <= {PARTITION_START}
)),
driver_tenure AS (
SELECT driver_id,
       org_id,
       DATEDIFF({PARTITION_START}, DATE(created_at)) AS driver_tenure_days
FROM datamodel_platform.dim_drivers
WHERE date = (
    SELECT COALESCE(
        MAX(CASE WHEN date = {PARTITION_START} THEN date END),
        MAX(date)
    )
    FROM datamodel_platform.dim_drivers
    WHERE date <= {PARTITION_START}
)),
org_coaching_history AS (
SELECT org_id,
       SUM(coached_events_1d) AS org_30d_coaching_history,
       SUM(virtual_coaching_1d) AS org_30d_virtual_coaching_history
FROM base
WHERE date BETWEEN DATE_SUB({PARTITION_START}, 29) AND {PARTITION_START}
GROUP BY org_id
),
driver_features AS (
SELECT {PARTITION_START} AS date,
       org_id,
       driver_id,
       FIRST(CASE WHEN nth_driving_day = 1 THEN date ELSE NULL END) AS last_trip_date,
       MAX(CASE WHEN nth_driving_day = 1 THEN trip_count_1d ELSE 0 END) AS trip_count_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END) AS trip_count_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END) AS trip_count_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_count_1d ELSE 0 END) AS camera_trip_count_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_count_1d ELSE 0 END) AS camera_trip_count_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_count_1d ELSE 0 END) AS camera_trip_count_60d,
       CASE
            WHEN FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_1d ELSE 0 END) > 0
            THEN FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_count_1d ELSE 0 END) /
                 FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_ratio_60d,
       CASE
            WHEN FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END) > 0
            THEN FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_distance_1d ELSE 0 END) /
                 FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_distance_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_distance_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_distance_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_distance_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS camera_trip_distance_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) AS outward_only_facing_camera_trip_count_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) AS outward_only_facing_camera_trip_count_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) AS outward_only_facing_camera_trip_count_60d,
       CASE
            WHEN  FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END)  > 0
            THEN  FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) /
                  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_count_1d - dual_facing_camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_ratio_60d,
       CASE
            WHEN  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN  FIRST(CASE WHEN nth_driving_day = 1 THEN camera_trip_distance_1d - dual_facing_camera_trip_distance_1d ELSE 0 END) /
                  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_distance_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN camera_trip_distance_1d - dual_facing_camera_trip_distance_1d  END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_distance_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN camera_trip_distance_1d - dual_facing_camera_trip_distance_1d  ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS outward_only_facing_camera_trip_distance_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN dual_facing_camera_trip_count_1d ELSE 0 END) AS dual_facing_camera_trip_count_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN dual_facing_camera_trip_count_1d ELSE 0 END) AS dual_facing_camera_trip_count_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN dual_facing_camera_trip_count_1d ELSE 0 END) AS dual_facing_camera_trip_count_60d,
       CASE
            WHEN  FIRST(CASE WHEN nth_driving_day = 1 THEN dual_facing_camera_trip_count_1d ELSE 0 END)  > 0
            THEN  FIRST(CASE WHEN nth_driving_day = 1 THEN dual_facing_camera_trip_count_1d ELSE 0 END) /
                  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN dual_facing_camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN dual_facing_camera_trip_count_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_ratio_60d,
       CASE
            WHEN  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN  FIRST(CASE WHEN nth_driving_day = 1 THEN dual_facing_camera_trip_distance_1d ELSE 0 END) /
                  FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_distance_ratio_1d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN dual_facing_camera_trip_distance_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_distance_ratio_30d,
       CASE
            WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)  > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN dual_facing_camera_trip_distance_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END)
            ELSE 0
       END AS dual_facing_camera_trip_distance_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) AS trip_duration_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) AS trip_duration_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) AS trip_duration_mins_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_distance_miles_1d ELSE 0 END) AS trip_distance_miles_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_distance_miles_1d ELSE 0 END) AS trip_distance_miles_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_distance_miles_1d ELSE 0 END) AS trip_distance_miles_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN max_speed_kmph_1d ELSE 0 END) AS max_speed_kmph_1d,
       MAX(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN max_speed_kmph_1d ELSE 0 END) AS max_speed_kmph_30d,
       MAX(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN max_speed_kmph_1d ELSE 0 END) AS max_speed_kmph_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN over_speed_limit_mins_1d ELSE 0 END) AS over_speed_limit_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN over_speed_limit_mins_1d ELSE 0 END) AS over_speed_limit_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN over_speed_limit_mins_1d ELSE 0 END) AS over_speed_limit_mins_60d,
       CASE WHEN SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day = 1 THEN over_speed_limit_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END)
        ELSE 0 END AS speeding_ratio_over_limit_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN over_speed_limit_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS speeding_ratio_over_limit_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN over_speed_limit_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS speeding_ratio_over_limit_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN light_speeding_mins_1d ELSE 0 END) AS light_speeding_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN light_speeding_mins_1d ELSE 0 END) AS light_speeding_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN light_speeding_mins_1d ELSE 0 END) AS light_speeding_mins_60d,
       CASE WHEN SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day = 1 THEN light_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END)
        ELSE 0 END AS light_speeding_ratio_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN light_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS light_speeding_ratio_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN light_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS light_speeding_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN moderate_speeding_mins_1d ELSE 0 END) AS moderate_speeding_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN moderate_speeding_mins_1d ELSE 0 END) AS moderate_speeding_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN moderate_speeding_mins_1d ELSE 0 END) AS moderate_speeding_mins_60d,
       CASE WHEN SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day = 1 THEN moderate_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END)
        ELSE 0 END AS moderate_speeding_ratio_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN moderate_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS moderate_speeding_ratio_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN moderate_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS moderate_speeding_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN heavy_speeding_mins_1d ELSE 0 END) AS heavy_speeding_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN heavy_speeding_mins_1d ELSE 0 END) AS heavy_speeding_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN heavy_speeding_mins_1d ELSE 0 END) AS heavy_speeding_mins_60d,
       CASE WHEN SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day = 1 THEN heavy_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END)
        ELSE 0 END AS heavy_speeding_ratio_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN heavy_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS heavy_speeding_ratio_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN heavy_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS heavy_speeding_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN severe_speeding_mins_1d ELSE 0 END) AS severe_speeding_mins_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN severe_speeding_mins_1d ELSE 0 END) AS severe_speeding_mins_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN severe_speeding_mins_1d ELSE 0 END) AS severe_speeding_mins_60d,
       CASE WHEN SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day = 1 THEN severe_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day = 1 THEN trip_duration_mins_1d ELSE 0 END)
        ELSE 0 END AS severe_speeding_ratio_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN severe_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS severe_speeding_ratio_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN severe_speeding_mins_1d ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_duration_mins_1d ELSE 0 END)
       ELSE 0 END AS severe_speeding_ratio_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_ended_in_morning_1d ELSE 0 END) AS trip_ended_in_morning_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_ended_in_morning_1d ELSE 0 END) AS trip_ended_in_morning_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_ended_in_morning_1d ELSE 0 END) AS trip_ended_in_morning_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_ended_at_night_1d ELSE 0 END) AS trip_ended_at_night_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_ended_at_night_1d ELSE 0 END) AS trip_ended_at_night_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_ended_at_night_1d ELSE 0 END) AS trip_ended_at_night_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_below_freezing_1d ELSE 0 END) AS trip_count_below_freezing_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_below_freezing_1d ELSE 0 END) AS trip_count_below_freezing_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_below_freezing_1d ELSE 0 END) AS trip_count_below_freezing_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN trip_count_over_35_1d ELSE 0 END) AS trip_count_over_35_1d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN trip_count_over_35_1d ELSE 0 END) AS trip_count_over_35_30d,
       SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN trip_count_over_35_1d ELSE 0 END) AS trip_count_over_35_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN crashes_1d ELSE 0 END), 0) AS crashes_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN crashes_1d ELSE 0 END), 0) AS crashes_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN crashes_1d ELSE 0 END), 0) AS crashes_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN speeding_1d ELSE 0 END), 0) AS speeding_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN speeding_1d ELSE 0 END), 0) AS speeding_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN speeding_1d ELSE 0 END), 0) AS speeding_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN following_distance_1d ELSE 0 END), 0) AS following_distance_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN following_distance_1d ELSE 0 END), 0) AS following_distance_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN following_distance_1d ELSE 0 END), 0) AS following_distance_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN mobile_usage_violations_1d ELSE 0 END), 0) AS mobile_usage_violations_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN mobile_usage_violations_1d ELSE 0 END), 0) AS mobile_usage_violations_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN mobile_usage_violations_1d ELSE 0 END), 0) AS mobile_usage_violations_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN harsh_braking_1d ELSE 0 END), 0) AS harsh_braking_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN harsh_braking_1d ELSE 0 END), 0) AS harsh_braking_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN harsh_braking_1d ELSE 0 END), 0) AS harsh_braking_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN harsh_turns_1d ELSE 0 END), 0) AS harsh_turns_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN harsh_turns_1d ELSE 0 END), 0) AS harsh_turns_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN harsh_turns_1d ELSE 0 END), 0) AS harsh_turns_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN no_seatbelt_violations_1d ELSE 0 END), 0) AS no_seatbelt_violations_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN no_seatbelt_violations_1d ELSE 0 END), 0) AS no_seatbelt_violations_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN no_seatbelt_violations_1d ELSE 0 END), 0) AS no_seatbelt_violations_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN distracted_driving_1d ELSE 0 END), 0) AS distracted_driving_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN distracted_driving_1d ELSE 0 END), 0) AS distracted_driving_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN distracted_driving_1d ELSE 0 END), 0) AS distracted_driving_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN rolling_stop_1d ELSE 0 END), 0) AS rolling_stop_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN rolling_stop_1d ELSE 0 END), 0) AS rolling_stop_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN rolling_stop_1d ELSE 0 END), 0) AS rolling_stop_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN harsh_accelerations_1d ELSE 0 END), 0) AS harsh_accelerations_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN harsh_accelerations_1d ELSE 0 END), 0) AS harsh_accelerations_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN harsh_accelerations_1d ELSE 0 END), 0) AS harsh_accelerations_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN drowsiness_1d ELSE 0 END), 0) AS drowsiness_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN drowsiness_1d ELSE 0 END), 0) AS drowsiness_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN drowsiness_1d ELSE 0 END), 0) AS drowsiness_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN lane_departure_1d ELSE 0 END), 0) AS lane_departure_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN lane_departure_1d ELSE 0 END), 0) AS lane_departure_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN lane_departure_1d ELSE 0 END), 0) AS lane_departure_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN hos_violation_count_1d ELSE 0 END), 0) AS hos_violation_count_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN hos_violation_count_1d ELSE 0 END), 0) AS hos_violation_count_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN hos_violation_count_1d ELSE 0 END), 0) AS hos_violation_count_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN total_events_1d ELSE 0 END), 0) AS total_events_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_events_1d ELSE 0 END), 0) AS total_events_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_events_1d ELSE 0 END), 0) AS total_events_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN coached_events_1d ELSE 0 END), 0) AS coached_events_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN coached_events_1d ELSE 0 END), 0) AS coached_events_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN coached_events_1d ELSE 0 END), 0) AS coached_events_60d,
       COALESCE(FIRST(CASE WHEN nth_driving_day = 1 THEN virtual_coaching_1d ELSE 0 END), 0) AS virtual_coaching_1d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN virtual_coaching_1d ELSE 0 END), 0) AS virtual_coaching_30d,
       COALESCE(SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN virtual_coaching_1d ELSE 0 END), 0) AS virtual_coaching_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_rural_driving_environment_locations ELSE 0 END) AS proportion_rural_driving_environment_locations_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_rural_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_rural_driving_environment_locations_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_rural_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_rural_driving_environment_locations_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_urban_driving_environment_locations ELSE 0 END) AS proportion_urban_driving_environment_locations_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_urban_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_urban_driving_environment_locations_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_urban_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_urban_driving_environment_locations_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_mixed_driving_environment_locations ELSE 0 END) AS proportion_mixed_driving_environment_locations_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_mixed_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_mixed_driving_environment_locations_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_mixed_driving_environment_locations ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN total_driving_environment_locations ELSE 0 END)
            ELSE 0
       END AS proportion_mixed_driving_environment_locations_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_living_street ELSE 0 END) AS proportion_roadway_living_street_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_living_street ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_living_street_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_living_street ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_living_street_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_motorway ELSE 0 END) AS proportion_roadway_motorway_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_motorway ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_motorway_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_motorway ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_motorway_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_primary ELSE 0 END) AS proportion_roadway_primary_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_primary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_primary_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_primary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_primary_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_residential ELSE 0 END) AS proportion_roadway_residential_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_residential ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_residential_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_residential ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_residential_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_secondary ELSE 0 END) AS proportion_roadway_secondary_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_secondary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_secondary_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_secondary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_secondary_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_service ELSE 0 END) AS proportion_roadway_service_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_service ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_service_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_service ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_service_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_tertiary ELSE 0 END) AS proportion_roadway_tertiary_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_tertiary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_tertiary_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_tertiary ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_tertiary_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_trunk ELSE 0 END) AS proportion_roadway_trunk_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_trunk ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_trunk_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_trunk ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_trunk_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_unclassified ELSE 0 END) AS proportion_roadway_unclassified_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_unclassified ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_unclassified_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_unclassified ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_unclassified_60d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN proportion_roadway_null ELSE 0 END) AS proportion_roadway_null_1d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_roadway_null ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 29 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_null_30d,
       CASE WHEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END) > 0
            THEN SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_roadway_null ELSE 0 END) /
                 SUM(CASE WHEN nth_driving_day BETWEEN 1 AND 59 THEN count_total_locations ELSE 0 END)
            ELSE 0
       END AS proportion_roadway_null_60d,
       MAX(CASE WHEN nth_driving_day = 1 THEN is_eld_relevant_device ELSE 0 END) AS is_eld_relevant_device,
       FIRST(CASE WHEN nth_driving_day = 1 THEN primary_vehicle_model ELSE NULL END) AS primary_vehicle_model,
       FIRST(CASE WHEN nth_driving_day = 1 THEN primary_vehicle_make ELSE NULL END) AS primary_vehicle_make,
       FIRST(CASE WHEN nth_driving_day = 1 THEN primary_max_weight_lbs ELSE NULL END) AS primary_max_weight_lbs,
       FIRST(CASE WHEN nth_driving_day = 1 THEN primary_gross_vehicle_weight_rating ELSE NULL END) AS primary_gross_vehicle_weight_rating,
       FIRST(CASE WHEN nth_driving_day = 1 THEN max_trip_end_time_ms ELSE NULL END) AS max_trip_end_time_ms_1d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN dayofweek_sin ELSE 0 END) AS dayofweek_sin_1d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN dayofweek_cos ELSE 0 END) AS dayofweek_cos_1d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN dayofyear_sin ELSE 0 END) AS dayofyear_sin_1d,
       FIRST(CASE WHEN nth_driving_day = 1 THEN dayofyear_cos ELSE 0 END) AS dayofyear_cos_1d
FROM base
GROUP BY 1,2,3)

SELECT
  driver_features.*,
  dim_organizations.* EXCEPT (org_id),
  org_size.n_org_vg_devices,
  org_size.n_org_cm_devices,
  COALESCE(driver_crash_counts.labelled_crash_count_next_1_day, 0) AS labelled_crash_count_next_1_day,
  COALESCE(driver_crash_counts.labelled_crash_count_next_7_days,0) AS labelled_crash_count_next_7_days,
  COALESCE(safety_org_settings.mobile_usage_enabled, FALSE) AS mobile_usage_enabled_org,
  COALESCE(safety_org_settings.mobile_usage_audio_alerts_enabled, FALSE) AS mobile_usage_audio_alerts_enabled_org,
  COALESCE(safety_org_settings.seatbelt_enabled,FALSE) AS seatbelt_enabled_org,
  COALESCE(safety_org_settings.seatbelt_audio_alerts_enabled, FALSE) AS seatbelt_audio_alerts_enabled_org,
  COALESCE(safety_org_settings.inattentive_driving_enabled,FALSE) AS inattentive_driving_enabled_org,
  COALESCE(safety_org_settings.inattentive_driving_audio_alerts_enabled,FALSE) AS inattentive_driving_audio_alerts_enabled_org,
  COALESCE(safety_org_settings.drowsiness_detection_enabled,FALSE) AS drowsiness_detection_enabled_org,
  COALESCE(safety_org_settings.drowsiness_detection_audio_alerts_enabled,FALSE) AS drowsiness_detection_audio_alerts_enabled_org,
  COALESCE(safety_org_settings.rolling_stop_enabled,FALSE) AS rolling_stop_enabled_org,
  COALESCE(safety_org_settings.in_cab_stop_sign_violation_enabled,FALSE) AS in_cab_stop_sign_violation_enabled_org,
  COALESCE(safety_org_settings.forward_collision_warning_enabled,FALSE) AS forward_collision_warning_enabled_org,
  COALESCE(safety_org_settings.forward_collision_warning_audio_alerts_enabled,FALSE) AS forward_collision_warning_audio_alerts_enabled_org,
  COALESCE(safety_org_settings.following_distance_enabled,FALSE) AS following_distance_enabled_org,
  COALESCE(safety_org_settings.following_distance_audio_alerts_enabled,FALSE) AS following_distance_audio_alerts_enabled_org,
  COALESCE(driver_tenure.driver_tenure_days, 0) AS driver_tenure_days,
  COALESCE(driver_crash_counts.labelled_crash_count_prev_1_year, 0) AS labelled_crash_count_prev_1_year,
  COALESCE(org_coaching_history.org_30d_coaching_history, 0) AS org_30d_coaching_history,
  COALESCE(org_coaching_history.org_30d_virtual_coaching_history, 0) AS org_30d_virtual_coaching_history,
  driver_crash_counts.labelled_crash_count_next_1_day_source,
  driver_crash_counts.labelled_crash_count_next_7_days_source,
  driver_crash_counts.labelled_crash_count_prev_1_year_source
FROM driver_features
LEFT JOIN  org_size
  ON org_size.org_id = driver_features.org_id
LEFT JOIN driver_crash_counts
  ON driver_crash_counts.org_id = driver_features.org_id
  AND driver_crash_counts.driver_id = driver_features.driver_id
LEFT JOIN dim_organizations
  ON dim_organizations.org_id = driver_features.org_id
LEFT JOIN safety_org_settings
  ON safety_org_settings.org_id = driver_features.org_id
LEFT JOIN driver_tenure
  ON driver_tenure.org_id = driver_features.org_id
  AND driver_tenure.driver_id = driver_features.driver_id
LEFT JOIN org_coaching_history
  ON org_coaching_history.org_id = driver_features.org_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""Calculation of driver features sourced from trip, safety, and location data for a date for 1d, 30d, 60d windows of days driven..""",
        row_meaning="""Each row represents trip, safety, and organizational metadata for a driver for a date.""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2023-09-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_driver_features"),
        PrimaryKeyDQCheck(name="dq_pk_dim_driver_features", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_dim_driver_features",
            non_null_columns=PRIMARY_KEYS,
        ),
    ],
    backfill_batch_size=3,
    upstreams=["product_analytics_staging.stg_driver_features",
               "datamodel_core.lifetime_device_activity",
               "product_analytics.dim_organizations_safety_settings",
               "datamodel_core.dim_organizations",
               "datamodel_platform.dim_drivers",
               "product_analytics_staging.stg_crash_trips"],
)
def dim_driver_features(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_driver_features")
    partition_keys = partition_keys_from_context(context)[0]
    context.log.info(str(partition_keys))
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
            PARTITION_START=PARTITION_START,
        )
    if len(partition_keys) > 1:
        query = """ ( """ + query + """ )"""
        for partition_key in partition_keys[1:]:
            query = query + """ UNION ALL ( """ + QUERY.format(PARTITION_START=partition_key) + """)"""
    context.log.info(f"{query}")
    return query
