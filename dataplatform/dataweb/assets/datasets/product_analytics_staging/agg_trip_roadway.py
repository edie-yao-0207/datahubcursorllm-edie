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
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "device_id", "trip_start_time_ms", "trip_end_time_ms"]

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
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID,
        },
    },
    {
        "name": "trip_start_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trip start time in epoch milliseconds."
        },
    },
    {
        "name": "trip_end_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trip end time in epoch milliseconds."
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
        "name": "count_total_locations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a trip."
        },
    },
    {
        "name": "count_living_street",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'living_street' Open Street Map highway tag."
        },
    },
    {
        "name": "count_motorway",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'motorway' Open Street Map highway tag."
        },
    },
    {
        "name": "count_primary",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'primary' Open Street Map highway tag."
        },
    },
    {
        "name": "count_residential",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'residential' Open Street Map highway tag."
        },
    },
    {
        "name": "count_secondary",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'secondary' Open Street Map highway tag."
        },
    },
    {
        "name": "count_service",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'service' Open Street Map highway tag."
        },
    },
    {
        "name": "count_tertiary",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'tertiary' Open Street Map highway tag."
        },
    },
    {
        "name": "count_trunk",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'trunk' Open Street Map highway tag."
        },
    },
    {
        "name": "count_unclassified",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching 'unclassified' Open Street Map highway tag."
        },
    },
    {
        "name": "count_null_osm_tag",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment" : "Total number of locations observed for a matching no Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_living_street",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'living_street' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_motorway",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'motorway' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_primary",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'primary' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_residential",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'residential' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_secondary",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'secondary' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_service",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'service' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_tertiary",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'tertiary' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_trunk",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'trunk' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_unclassified",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching 'unclassified' Open Street Map highway tag."
        },
    },
    {
        "name": "proportion_null_osm_tag",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "Proportion of locations observed for a matching no Open Street Map highway tag."
        },
    },
    {
        "name": "predominant_osm_tag_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment" : "The predominant Open Street Map highway tag name for a trip."
        },
    },
    {
        "name": "proportion_predominant_osm_tag",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment" : "The proportion of locations observed for the predominant Open Street Map highway tag name for a trip."
        },
    }
]

QUERY = """
WITH trips AS (
SELECT  *
FROM datamodel_telematics.fct_trips
WHERE date between '{PARTITION_START}' and '{PARTITION_END}'
AND trip_type = 'location_based'
AND start_state is not null
AND end_state is not null),
trip_ways AS (
SELECT loc.date,
       trips.driver_id,
       loc.device_id,
       loc.org_id,
       loc.value.way_id,
       trips.start_time_ms AS trip_start_time_ms,
       trips.end_time_ms AS trip_end_time_ms,
       COUNT(*) AS location_observation_count
FROM kinesisstats_history.location loc
INNER JOIN trips
  ON trips.org_id = loc.org_id
  AND trips.device_id = loc.device_id
  AND loc.time BETWEEN trips.start_time_ms AND trips.end_time_ms
WHERE loc.date between '{PARTITION_START}' and '{PARTITION_END}'
GROUP BY ALL),
osm_highway_tag_pre AS (
select  trip_ways.*,
        map_data.osm_highway
from trip_ways
left join safety_map_data.osm_can_20231221_eur_20231221_mex_20231221_usa_20231221__tomtom_202312__resolved_speed_limits map_data
on map_data.osm_way_id = trip_ways.way_id),
osm_tag_agg_pre AS (
select
date,
driver_id,
device_id,
org_id,
trip_start_time_ms,
trip_end_time_ms,
SUM(CASE WHEN osm_highway = 'living_street' THEN location_observation_count ELSE 0 END) as count_living_street,
SUM(CASE WHEN osm_highway = 'motorway' THEN location_observation_count ELSE 0 END) as count_motorway,
SUM(CASE WHEN osm_highway = 'primary' THEN location_observation_count ELSE 0 END) as count_primary,
SUM(CASE WHEN osm_highway = 'residential' THEN location_observation_count ELSE 0 END) as count_residential,
SUM(CASE WHEN osm_highway = 'secondary' THEN location_observation_count ELSE 0 END) as count_secondary,
SUM(CASE WHEN osm_highway = 'service' THEN location_observation_count ELSE 0 END) as count_service,
SUM(CASE WHEN osm_highway = 'tertiary' THEN location_observation_count ELSE 0 END) as count_tertiary,
SUM(CASE WHEN osm_highway = 'trunk' THEN location_observation_count ELSE 0 END) as count_trunk,
SUM(CASE WHEN osm_highway = 'unclassified' THEN location_observation_count ELSE 0 END) as count_unclassified,
SUM(CASE WHEN osm_highway IS NULL THEN location_observation_count ELSE 0 END) as count_null_osm_tag,
SUM(location_observation_count) AS count_total_locations
from osm_highway_tag_pre
group by all),
osm_tag_agg AS (
select *, GREATEST(
        count_living_street,count_motorway, count_primary, count_residential, count_secondary,
        count_service, count_tertiary, count_trunk, count_unclassified, count_null_osm_tag
      )  as predominant_osm_tag_ct
from osm_tag_agg_pre)

select date,
       driver_id,
       trip_start_time_ms,
       trip_end_time_ms,
       device_id,
       org_id,
       count_total_locations,
       count_living_street,
       count_motorway,
       count_primary,
       count_residential,
       count_secondary,
       count_service,
       count_tertiary,
       count_trunk,
       count_unclassified,
       count_null_osm_tag,
       count_living_street / count_total_locations as proportion_living_street,
       count_motorway / count_total_locations as proportion_motorway,
       count_primary / count_total_locations as proportion_primary,
       count_residential / count_total_locations as proportion_residential,
       count_secondary / count_total_locations as proportion_secondary,
       count_service / count_total_locations as proportion_service,
       count_tertiary / count_total_locations as proportion_tertiary,
       count_trunk / count_total_locations as proportion_trunk,
       count_unclassified / count_total_locations as proportion_unclassified,
       count_null_osm_tag / count_total_locations as proportion_null_osm_tag,
       case when count_living_street = predominant_osm_tag_ct THEN 'living_street'
            when count_motorway = predominant_osm_tag_ct THEN 'motorway'
            when count_primary = predominant_osm_tag_ct THEN 'primary'
            when  count_residential = predominant_osm_tag_ct THEN 'residential'
            when count_secondary = predominant_osm_tag_ct THEN 'secondary'
            when count_service = predominant_osm_tag_ct THEN 'service'
            when count_tertiary = predominant_osm_tag_ct THEN 'tertiary'
            when count_trunk = predominant_osm_tag_ct THEN 'trunk'
            when count_unclassified = predominant_osm_tag_ct THEN 'unclassified'
            when count_null_osm_tag = predominant_osm_tag_ct THEN 'null'
            else 'other' end as predominant_osm_tag_name,
      case when count_living_street = predominant_osm_tag_ct THEN count_living_street / count_total_locations
           when count_motorway = predominant_osm_tag_ct THEN count_motorway / count_total_locations
           when count_primary = predominant_osm_tag_ct THEN count_primary / count_total_locations
           when count_residential = predominant_osm_tag_ct THEN count_residential / count_total_locations
           when count_secondary = predominant_osm_tag_ct THEN count_secondary / count_total_locations
           when count_service = predominant_osm_tag_ct THEN count_service / count_total_locations
           when count_tertiary = predominant_osm_tag_ct THEN count_tertiary / count_total_locations
           when count_trunk = predominant_osm_tag_ct THEN count_trunk / count_total_locations
           when count_unclassified = predominant_osm_tag_ct THEN count_unclassified / count_total_locations
           when count_null_osm_tag = predominant_osm_tag_ct THEN count_null_osm_tag / count_total_locations
           else 0 end as proportion_predominant_osm_tag
from osm_tag_agg
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Breakdown of trips by Open Street Map roadway type metadata (ex: motorway, residential, etc.)""",
        row_meaning="""Each row represents the roadway metadata for a single trip.""",
        related_table_info={},
        table_type=TableType.AGG,
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
        NonEmptyDQCheck(name="dq_non_empty_agg_trip_roadway"),
        PrimaryKeyDQCheck(name="dq_pk_agg_trip_roadway", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_agg_trip_roadway",
            non_null_columns=PRIMARY_KEYS,
        ),
    ],
    backfill_batch_size=3,
    upstreams=[
        "kinesisstats_history.location",
        "datamodel_telematics.fct_trips",
        "safety_map_data.osm_can_20231221_eur_20231221_mex_20231221_usa_20231221__tomtom_202312__resolved_speed_limits"
    ],
)
def agg_trip_roadway(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_trip_roadway")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
