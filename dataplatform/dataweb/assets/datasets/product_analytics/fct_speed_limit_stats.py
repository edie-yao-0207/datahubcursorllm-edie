from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    get_all_regions
)
from dataclasses import dataclass
from typing import List

PRIMARY_KEYS = [
    "date",
    "org_id",
    "device_id",
    "time",
]

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
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DEVICE_ID,
        },
    },
    {
        "name": "time",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.TIME,
        },
    },
    {
        "name": "way_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Open Street Maps (OSM) Way ID.",
        },
    },
    {
        "name": "speed_limit_meters_per_second",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Speed limit for current position in meters per second.",
        },
    },
    {
        "name": "location_speed_limit_source",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Source of the speed limit, if speed limit comes from location stat. Can join with `definitions.location_speed_limit_source_type_enums` for source names.",
        },
    },
    {
        "name": "backend_speed_limit_source",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Source of the speed limit, if speed limit comes from backend stat. Can join with `definitions.backend_speed_limit_source_type_enums` for source names.",
        },
    },
    {
        "name": "map_matching_method",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Map matching method (if stat is from backend), per enum value. Can join with `definitions.map_matching_method_type_enums` for method names.",
        },
    },
    {
        "name": "has_speed_limit",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether a speed limit is available. False if neither location nor backend stat has a speed limit for the current position.",
        },
    },
    {
        "name": "latitude_degrees",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Current position's latitude, in degrees.",
        },
    },
    {
        "name": "longitude_degrees",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Current position's longitude, in degrees.",
        },
    },
    {
        "name": "gps_speed_meters_per_second",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "GPS-measured device speed, in meters per second.",
        },
    },
    {
        "name": "ecu_speed_meters_per_second",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "ECU (electronic control unit) measured device speed, in meters per second.",
        },
    },
    {
        "name": "heading_degrees",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Device's compass heading, in degrees.",
        },
    },
    {
        "name": "revgeo_country",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The country the device is in based on reverse geolocation.",
        },
    },
    {
        "name": "revgeo_state",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The device's state based on reverse geolocation.",
        },
    },
    {
        "name": "revgeo_city",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The device's city based on reverse geolocation.",
        },
    },
    {
        "name": "hdop",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Horizontal dilution of precision; specifies propagation of uncertainty in the measurement of the GPS coordinates.",
        },
    },
    {
        "name": "vdop",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Vertical dilution of precision; specifies propagation of uncertainty in the measurement of the GPS coordinates.",
        },
    },
    {
        "name": "altitude_meters",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Device altitude, in meters, from GPS.",
        },
    },
    {
        "name": "geoid_height_correction_meters",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Height correction, in meters, from GPS.",
        },
    },
    {
        "name": "accuracy_millimeters",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The accuracy of the location datapoint, in millimeters.",
        },
    },
]


@dataclass
class CellTimestampMapping:
    """
    Maps a group of deployment cell IDs (e.g. us2, prod) to a timestmap.
    Used to model the LaunchDarkly flag `use-backend-speed-limit-time-milliseconds`.
    """

    cells: List[str]
    timestamp: int

    def serialize_cells(self) -> str:
        """
        Build a string from cell IDs that is suitable to use in an `IN` clause.
        """

        return ", ".join(f"'{c}'" for c in self.cells)


# This mapping represents the cutover time from `location` to `osdbackendcomputedspeedlimit`
# for computing the speed limit for an org, based on which cell it is assigned to.
# It is copied from LaunchDarkly: https://app.launchdarkly.com/projects/samsara/flags/use-backend-speed-limit-time-milliseconds/
cell_timestamp_maps = [
    CellTimestampMapping(cells=["us2", "us3", "sf1"], timestamp=1730373000000),
    CellTimestampMapping(cells=["us4", "us5"], timestamp=1730463000000),
    CellTimestampMapping(cells=["us6", "us7"], timestamp=1730722200000),
    CellTimestampMapping(cells=["us8", "us9"], timestamp=1730808600000),
    CellTimestampMapping(cells=["us10", "us11"], timestamp=1730895000000),
    CellTimestampMapping(cells=["prod"], timestamp=1730981400000),
]


def pick_stat_based_on_speed_limit_source(
    location_column: str, backend_column: str
) -> str:
    """
    Build case statement to pick a column based on the preferred stat for speed limits.
    """

    #
    cell_timestamp_cases = "\n".join(
        f"when cell_id in ({c.serialize_cells()}) then {c.timestamp}"
        for c in cell_timestamp_maps
    )
    max_timestamp = max(*[c.timestamp for c in cell_timestamp_maps])
    cell_timestamp_case_statement = f"""
    case
      {cell_timestamp_cases}
      else {max_timestamp}
    end
    """

    return f"""
    case
      -- Pick location stat when speed limit source = FIRMWARE_OSRM_MATCH,
      -- or time comes before backend stat was available for the org.
      when location_speed_limit_source = 4 then {location_column}
      when l.time < ({cell_timestamp_case_statement}) then {location_column}
      else {backend_column}
    end
    """


def generate_query(partition_start: str, partition_end: str) -> str:

    backend_stat_object_prefix = "value.proto_value.backend_computed_speed_limit"

    query = rf"""
    with org_clusters as (
      select
        org_id,
        cell_id
      from clouddb.org_cells
    ),
    location_stats as (
      select
        /*+ BROADCAST(c) */
        date,
        l.org_id,
        coalesce(cell_id, 'prod') as cell_id,
        device_id,
        time,
        cast(value.way_id as bigint) as way_id,
        value.has_speed_limit as location_has_speed_limit,
        value.speed_limit_meters_per_second as location_speed_limit_meters_per_second,
        value.speed_limit_source as location_speed_limit_source,
        value.gps_speed_meters_per_second,
        value.ecu_speed_meters_per_second,
        value.latitude as latitude_degrees,
        value.longitude as longitude_degrees,
        value.heading_degrees,
        value.revgeo_country,
        value.revgeo_state,
        value.revgeo_city,
        value.hdop,
        value.vdop,
        value.altitude_meters,
        value.geoid_height_correction_meters,
        value.accuracy_millimeters
      from
        kinesisstats_history.location l
      left join org_clusters c
        on l.org_id = c.org_id
      where
        date between '{partition_start}' and '{partition_end}'
    ),
    backend_stats as (
      select
        date,
        org_id,
        object_id as device_id,
        time,
        cast({backend_stat_object_prefix}.way_id as bigint) as way_id,
        {backend_stat_object_prefix}.has_speed_limit as backend_has_speed_limit,
        {backend_stat_object_prefix}.speed_limit_meters_per_second as backend_speed_limit_meters_per_second,
        {backend_stat_object_prefix}.speed_limit_source as backend_speed_limit_source,
        {backend_stat_object_prefix}.map_matching_method
      from kinesisstats_history.osdbackendcomputedspeedlimit
      where
        date between '{partition_start}' and '{partition_end}'
        and not value.is_databreak
        and not value.is_end
    )
    select
      l.date,
      l.org_id,
      l.device_id,
      l.time,
      coalesce(l.way_id, b.way_id) as way_id,
      {pick_stat_based_on_speed_limit_source(location_column='location_speed_limit_meters_per_second', backend_column='backend_speed_limit_meters_per_second')} as speed_limit_meters_per_second,
      {pick_stat_based_on_speed_limit_source(location_column='location_speed_limit_source', backend_column='NULL')} as location_speed_limit_source,
      {pick_stat_based_on_speed_limit_source(location_column='NULL', backend_column='backend_speed_limit_source')} as backend_speed_limit_source,
      {pick_stat_based_on_speed_limit_source(location_column='NULL', backend_column='map_matching_method')} as map_matching_method,
      {pick_stat_based_on_speed_limit_source(location_column='coalesce(location_has_speed_limit, FALSE)', backend_column='coalesce(backend_has_speed_limit, FALSE)')} as has_speed_limit,
      l.longitude_degrees,
      l.latitude_degrees,
      l.gps_speed_meters_per_second,
      l.ecu_speed_meters_per_second,
      l.heading_degrees,
      l.revgeo_state,
      l.revgeo_country,
      l.revgeo_city,
      l.hdop,
      l.vdop,
      l.altitude_meters,
      l.geoid_height_correction_meters,
      l.accuracy_millimeters
    from location_stats l
    left join backend_stats b
      on l.date = b.date
        and l.org_id = b.org_id
        and l.device_id = b.device_id
        and l.time = b.time
    """
    return query


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""
        Provides device locations with current speed limits, where available.
        Picks speed limit from either `location` stat or `osdbackendcomputedspeedlimit` stat,
        based on how the speed limit was sourced. If it comes from firmware OSRM match,
        `speed_limit_meters_per_second` comes from the location stat; otherwise, it comes
        from backend if available.
        """,
        row_meaning="""
        Each row maps 1:1 to a row in `kinesisstats.location`. If `has_speed_limit = TRUE`, either
        `backend_speed_limit_source` or `location_speed_limit_source` will be nonnull depending
        on where `speed_limit_meters_per_second` is sourced from.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "clouddb.org_cells",
        "kinesisstats_history.location",
        "kinesisstats_history.osdbackendcomputedspeedlimit",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_speed_limit_stats", block_before_write=True
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_speed_limit_stats",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_speed_limit_stats",
            non_null_columns=["time", "has_speed_limit"],
            block_before_write=True,
        ),
    ],
    backfill_start_date="2024-04-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
)
def fct_speed_limit_stats(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]

    return generate_query(partition_start=PARTITION_START, partition_end=PARTITION_END)
