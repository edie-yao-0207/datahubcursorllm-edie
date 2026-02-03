from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    driver_id_default_description,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

databases = {
    "database_bronze": Database.DATAMODEL_TELEMATICS_BRONZE,
    "database_silver": Database.DATAMODEL_TELEMATICS_SILVER,
    "database_gold": Database.DATAMODEL_TELEMATICS,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2015-05-31")

pipeline_group_name = "fct_trips_daily"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

fct_trips_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The calendar date in 'YYYY-mm-dd' format corresponding to the trip"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device that the trip is linked to"
        },
    },
    {
        "name": "trip_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Indicates the type of trip: location_based, engine_based"
        },
    },
    {
        "name": "start_time_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The start timestamp of the trip in epoch milliseconds"
        },
    },
    {
        "name": "start_time_utc",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The start timestamp of the trip in UTC"},
    },
    {
        "name": "end_time_ms",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The end timestamp of the trip in epoch milliseconds"},
    },
    {
        "name": "end_time_utc",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The end timestamp of the trip in UTC"},
    },
    {
        "name": "duration_mins",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The duration of the trip in minutes"},
    },
    {
        "name": "start_latitude",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The starting latitude of the trip"},
    },
    {
        "name": "start_longitude",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The starting longitude of the trip"},
    },
    {
        "name": "end_latitude",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The ending latitude of the trip"},
    },
    {
        "name": "end_longitude",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The ending latitude of the trip"},
    },
    {
        "name": "start_state",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The state where the trip originated from"},
    },
    {
        "name": "end_state",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The state where the trip ended in"},
    },
    {
        "name": "start_country",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Start country of trip"},
    },
    {
        "name": "end_country",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "End country of trip"},
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "driver_assignment_source",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The source used for assigning a driver to this trip. E.g., HOS, Tachograph"
        },
    },
    {
        "name": "distance_meters",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The distance in meters covered during the trip"},
    },
    {
        "name": "distance_miles",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "The distance in miles covered during the trip"},
    },
    {
        "name": "trip_end_reason",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The reason why this trip entry was deemed to have ended"
        },
    },
    {
        "name": "max_speed_kmph",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The maximum speed in kmph during the course of this trip"
        },
    },
    {
        "name": "max_speed_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The epoch timestamp in milliseconds when the maximum speed was observed"
        },
    },
    {
        "name": "over_speed_limit_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle exceeded the speed limit during the trip, in minutes"
        },
    },
    {
        "name": "not_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was observed to not be speeding during the trip, in minutes"
        },
    },
    {
        "name": "light_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was lightly speeding according to the org's settings, in minutes"
        },
    },
    {
        "name": "moderate_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was moderately speeding according to the org's settings, in minutes"
        },
    },
    {
        "name": "heavy_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was heavily speeding according to the org's settings, in minutes"
        },
    },
    {
        "name": "severe_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was severely speeding according to the org's settings, in minutes"
        },
    },
    {
        "name": "trip_start_odometer",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The starting odometer reading of the vehicle on the trip"
        },
    },
    {
        "name": "trip_end_odometer",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ending odometer reading of the vehicle on the trip"
        },
    },
    {
        "name": "fuel_consumed_ml",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The fuel consumed in ml during the trip"},
    },
    {
        "name": "engine_on_mins",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Time the engine was on during the trip, in minutes"},
    },
    {
        "name": "engine_idle_mins",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Time the engine was idle during the trip, in minutes"},
    },
    {
        "name": "created_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The epoch timestamp when the trip was created in the Samsara cloud. This may differ from when the trip actually started (see start_time_utc) due to data ingestion delays."
        },
    },
    {
        "name": "updated_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The epoch timestamp when the trip was updated in the Samsara cloud"
        },
    },
]

fct_trips_query = """--sql
    SELECT
        CAST(date AS string) AS date
        , org_id
        , device_id
        , CASE
            WHEN version = 101 THEN 'location_based'
            WHEN version = 201 THEN 'engine_based'
            ELSE 'invalid' -- there should be no trips of this type based on WHERE clauses below
        END AS trip_type
        , proto.start.time AS start_time_ms
        , from_unixtime(proto.start.time/1000) as start_time_utc
        , proto.end.time AS end_time_ms
        , from_unixtime(proto.end.time/1000) as end_time_utc
        , (proto.end.time - proto.start.time)/60000 AS duration_mins
        , proto.start.latitude AS start_latitude
        , proto.start.longitude AS start_longitude
        , proto.end.latitude AS end_latitude
        , proto.end.longitude AS end_longitude
        , proto.start.place.state AS start_state
        , proto.end.place.state AS end_state
        , proto.start.place.country AS start_country
        , proto.end.place.country AS end_country
        , CASE
            WHEN COALESCE(driver_id, 0) < 0
            AND driver_id_exclude_0_drivers > 0 THEN driver_id_exclude_0_drivers
            ELSE driver_id
        END AS driver_id
        -- from driverassignmentsmodels/models.go
        , CASE proto.driver_source_id
            WHEN -1 THEN 'Invalid'
            WHEN 0 THEN 'Unknown'
            WHEN 1 THEN 'HOS'
            WHEN 2 THEN 'IdCard'
            WHEN 3 THEN 'Static'
            WHEN 4 THEN 'FaceId'
            WHEN 5 THEN 'Tachograph'
            WHEN 6 THEN 'SafetyManual'
            WHEN 7 THEN 'RFID'
            WHEN 8 THEN 'Trailer'
            WHEN 9 THEN 'External'
            WHEN 10 THEN 'QRCode'
            ELSE 'Unknown'
        END AS driver_assignment_source
        , proto.trip_distance.distance_meters
        , COALESCE(proto.trip_distance.distance_meters, 0) * 0.000621371 AS distance_miles
        -- from go/src/samsaradev.io/stats/trips/tripsproto/trips.proto
        , CASE proto.trip_end_reason
            WHEN 1 THEN 'BorderCrossed'
            WHEN 2 THEN 'NoMovementAboveSpeedThreshold'
            WHEN 3 THEN 'TripTooLong'
            -- Note: Reasons 1-3 are for location_based trips whereas reason 4 is for engine_based trips only
            WHEN 4 THEN 'NoEquipmentActivity'
            ELSE 'Unknown'
        END AS trip_end_reason
        , proto.trip_speeding.max_speed_kmph
        , proto.trip_speeding.max_speed_at AS max_speed_at_ms
        , proto.trip_speeding.over_speed_limit_ms/60000 AS over_speed_limit_mins
        , proto.trip_speeding_custom.not_speeding_ms/60000 AS not_speeding_mins
        , proto.trip_speeding_custom.light_speeding_ms/60000 AS light_speeding_mins
        , proto.trip_speeding_custom.moderate_speeding_ms/60000 AS moderate_speeding_mins
        , proto.trip_speeding_custom.heavy_speeding_ms/60000 AS heavy_speeding_mins
        , proto.trip_speeding_custom.severe_speeding_ms/60000 AS severe_speeding_mins
        , proto.trip_odometers.start_odometer as trip_start_odometer
        , proto.trip_odometers.end_odometer as trip_end_odometer
        , proto.trip_fuel.fuel_consumed_ml
        , proto.trip_engine.engine_on_ms/60000 AS engine_on_mins
        , proto.trip_engine.engine_idle_ms/60000 AS engine_idle_mins
        , created_at AS created_at_ms
        , updated_at AS updated_at_ms
    FROM
        trips2db_shards.trips
    WHERE
        1 = 1
        AND date = '{DATEID}'
        AND proto.start.time < proto.end.time
        -- Only include trips that have completed
        -- Vehicles on engine-based trips may not traverse a non-zero distance
        AND ((version = 101 AND proto.trip_distance.distance_meters > 0) OR version = 201)
        -- Filter out very long trips
        AND (proto.end.time - proto.start.time) < (24 * 60 * 60 * 1000)
        AND (proto.trip_distance.distance_meters) * 0.000621371 <= 1440
--endsql
"""

FCT_TRIPS_PRIMARY_KEYS = ["date", "org_id", "device_id", "trip_type", "start_time_ms"]
fct_trips_assets = build_assets_from_sql(
    name="fct_trips",
    schema=fct_trips_schema,
    description=build_table_description(
        table_desc=""" A daily record of trips that started on that day along with the various attributes of the trip.
        Cleaned up version of trips2db_shards.trips. """,
        row_meaning="""A unique trip record along with the various attributes of the trip. Filter by the date column to limit the results to trips that started within a time period.

        A location-based trip begins when a vehicle achieves a speed of at least 5mph, and ends when the vehicle either remains below 5mph for more than 5 minutes or when the vehicle crosses a state or national border.

        An engine-based trip begins when the equipment turns on and ends when the equipment is turned off. Specifically, this looks at the state of the osDEquipmentActivity objectstat.""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_trips_query,
    primary_keys=FCT_TRIPS_PRIMARY_KEYS,
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

fct_trips_us = fct_trips_assets[AWSRegions.US_WEST_2.value]
fct_trips_eu = fct_trips_assets[AWSRegions.EU_WEST_1.value]
fct_trips_ca = fct_trips_assets[AWSRegions.CA_CENTRAL_1.value]


dqs["fct_trips"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_fct_trips",
        database=databases["database_gold_dev"],
        table="fct_trips",
        blocking=True,
    )
)

dqs["fct_trips"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_trips",
        table="fct_trips",
        primary_keys=FCT_TRIPS_PRIMARY_KEYS,
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_trips"].append(
    NonNullDQCheck(
        name="dq_null_check_fct_trips",
        database=databases["database_gold_dev"],
        table="fct_trips",
        non_null_columns=[
            "org_id",
            "device_id",
            "trip_type",
            "start_time_ms",
            "start_time_utc",
            "end_time_ms",
            "end_time_utc",
            "duration_mins",
            "start_latitude",
            "start_longitude",
            "end_latitude",
            "end_longitude",
            "driver_id",
            "trip_end_reason",
            "trip_start_odometer",
            "trip_end_odometer",
        ],
        blocking=True,
    )
)

fct_trips_daily_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The calendar date in 'YYYY-mm-dd' format"},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The Internal ID for the customer's Samsara org"},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The ID of the customer device"},
    },
    {
        "name": "trip_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Indicates the type of trip: location_based, engine_based"
        },
    },
    {
        "name": "trip_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of trips logged by the device on the day"
        },
    },
    {
        "name": "total_duration_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The total duration of trips logged by the device on the day, in minutes"
        },
    },
    {
        "name": "total_distance_meters",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The total distance during trips logged by the device on the day in meters"
        },
    },
    {
        "name": "total_distance_miles",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The total distance during trips logged by the device on the day in miles"
        },
    },
    {
        "name": "max_speed_kmph",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The max speed logged by the device on the day in kmph"
        },
    },
    {
        "name": "total_over_speed_limit_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The total time spent by the device over the speed limit on the day, in minutes"
        },
    },
    {
        "name": "total_not_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total duration the vehicle was observed to not be speeding during the day, in minutes"
        },
    },
    {
        "name": "total_light_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": """Total duration the vehicle was lightly speeding during the day
                according to the org's settings, in minutes"""
        },
    },
    {
        "name": "total_moderate_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": """Total duration the vehicle was moderately speeding during the day
                according to the org's settings, in minutes"""
        },
    },
    {
        "name": "total_heavy_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": """Total duration the vehicle was heavily speeding during the day
                according to the org's settings, in minutes"""
        },
    },
    {
        "name": "total_severe_speeding_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": """Total duration the vehicle was severely speeding during the day
                according to the org's settings, in minutes"""
        },
    },
    {
        "name": "total_fuel_consumed_ml",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "The total fuel consumed in ml during the day"},
    },
    {
        "name": "total_engine_on_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total time the engine was on during the trip, in minutes"
        },
    },
    {
        "name": "total_engine_idle_mins",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Total time the engine was idle during the trip, in minutes"
        },
    },
]


fct_trips_daily_query = """--sql
    SELECT
        date
        , org_id
        , device_id
        , trip_type
        , COUNT(1) AS trip_count
        , SUM(duration_mins) AS total_duration_mins
        , SUM(distance_meters) AS total_distance_meters
        , SUM(distance_miles) AS total_distance_miles
        , MAX(max_speed_kmph) AS max_speed_kmph
        , SUM(over_speed_limit_mins) AS total_over_speed_limit_mins
        , SUM(not_speeding_mins) AS total_not_speeding_mins
        , SUM(light_speeding_mins) AS total_light_speeding_mins
        , SUM(moderate_speeding_mins) AS total_moderate_speeding_mins
        , SUM(heavy_speeding_mins) AS total_heavy_speeding_mins
        , SUM(severe_speeding_mins) AS total_severe_speeding_mins
        , SUM(fuel_consumed_ml) AS total_fuel_consumed_ml
        , SUM(engine_on_mins) AS total_engine_on_mins
        , SUM(engine_idle_mins) AS total_engine_idle_mins
    FROM
        `{database_gold_dev}`.fct_trips
    WHERE
        date = '{DATEID}'
    GROUP BY
        date
        , org_id
        , device_id
        , trip_type
--endsql"""

FCT_TRIPS_DAILY_PRIMARY_KEYS = ["date", "org_id", "device_id", "trip_type"]
fct_trips_daily_assets = build_assets_from_sql(
    name="fct_trips_daily",
    schema=fct_trips_daily_schema,
    description=build_table_description(
        table_desc=""" Trip data aggregated at day-device-trip_type level. Also has additional attributes related to first/last trip dates for the device """,
        row_meaning=""" Aggregate trip metrics at a day-device-trip_type level for trips taken by a specific device during a day. Filter by the date column to limit the results to trips that started within a time period. """,
        related_table_info={
            "datamodel_telematics.fct_trips": """ A more granular trip level table that feeds into this dataset """
        },
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_trips_daily_query,
    primary_keys=FCT_TRIPS_DAILY_PRIMARY_KEYS,
    upstreams=[AssetKey([databases["database_gold_dev"], "dq_fct_trips"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

fct_trips_daily_us = fct_trips_daily_assets[AWSRegions.US_WEST_2.value]
fct_trips_daily_eu = fct_trips_daily_assets[AWSRegions.EU_WEST_1.value]
fct_trips_daily_ca = fct_trips_daily_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_trips_daily"].append(
    NonNullDQCheck(
        name="dq_null_check_fct_trips_daily",
        database=databases["database_gold_dev"],
        table="fct_trips_daily",
        non_null_columns=[
            "date",
            "org_id",
            "device_id",
            "trip_type",
            "trip_count",
        ],
        blocking=False,
    )
)

dqs["fct_trips_daily"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_check_fct_trips_daily",
        database=databases["database_gold_dev"],
        table="fct_trips_daily",
        blocking=True,
    )
)

dqs["fct_trips_daily"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_trips_daily",
        table="fct_trips_daily",
        primary_keys=FCT_TRIPS_DAILY_PRIMARY_KEYS,
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dq_assets = dqs.generate()
