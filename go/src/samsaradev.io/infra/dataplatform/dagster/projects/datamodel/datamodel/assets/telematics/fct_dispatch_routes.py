from dagster import AssetKey, BackfillPolicy, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
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

daily_partition_def = DailyPartitionsDefinition(start_date="2023-11-01")

pipeline_group_name = "fct_dispatch_routes"

BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=10)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_dispatch_routes_schema = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "The calendar date in 'YYYY-mm-dd' format corresponding to the creation of the route"
        },
    },
    {
        "name": "route_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The unique ID of the route. Note - There is a possibility that a single route_id can exist across multiple orgs"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "route_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name of the route"},
    },
    {
        "name": "parent_recurring_route_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "In case of a recurring route, the route_id that has been used as a template to create this route"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the route is configured for a vehicle"
        },
    },
    {
        "name": "trailer_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the route is configured for a trailer"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The driver ID if assigned to the route"},
    },
    {
        "name": "created_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp in UTC when the route was created"},
    },
    {
        "name": "updated_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp in UTC when the route was updated"},
    },
    {
        "name": "scheduled_start_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The UTC timestamp indicating when the route was scheduled to begin"
        },
    },
    {
        "name": "scheduled_end_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The UTC timestamp indicating when the route was scheduled to end"
        },
    },
    {
        "name": "num_stops",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Total number of stops configured for the route"},
    },
    {
        "name": "num_stops_by_state",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "long",
            "valueContainsNull": False,
        },
        "nullable": True,
        "metadata": {
            "comment": """Number of stops in the route grouped by their curent state.
                     State can be one of unassigned, scheduled, enroute, completed, skipped or arrived"""
        },
    },
    {
        "name": "is_completed_route",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": """Boolean flag indicating whether the route is considered to be complete.
            Defined as the route having at least one stop that is completed/arrived and no stops that
            are in a non-terminal state (unassigned, scheduled or enroute)"""
        },
    },
    {
        "name": "is_route_completed_on_time",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": """Boolean flag indicating whether the route is considered to be complete on time.
                     i.e., whether the last completed/arrived stop is earlier than the scheduled end timestamp"""
        },
    },
]


stg_dispatch_routes_query = """
--sql

WITH
  jobs AS (
    SELECT
      id,
      org_id,
      dispatch_route_id,
      job_state,
      completed_at
    FROM
      dispatchdb_shards.shardable_dispatch_jobs
    WHERE
      `date` BETWEEN DATE('{PARTITION_KEY_RANGE_START}')-1 AND DATE('{PARTITION_KEY_RANGE_END}')+31
  ),
  routes AS (
    SELECT
      r.id AS route_id,
      r.org_id,
      r.name AS route_name,
      r.parent_recurring_route_id,
      r.vehicle_id AS device_id,
      r.trailer_device_id,
      r.driver_id,
      r.created_at AS created_ts,
      r.updated_at AS updated_ts,
      TIMESTAMP(FROM_UNIXTIME(r.scheduled_start_ms / 1000)) AS scheduled_start_ts,
      TIMESTAMP(FROM_UNIXTIME(r.scheduled_end_ms / 1000)) AS scheduled_end_ts,
      CASE j.job_state
        WHEN 0 THEN 'unassigned'
        WHEN 1 THEN 'scheduled'
        WHEN 2 THEN 'enroute'
        WHEN 3 THEN 'completed'
        WHEN 4 THEN 'skipped'
        WHEN 5 THEN 'arrived'
        ELSE NULL
      END AS stop_state,
      COUNT(j.id) AS num_stops,
      MAX(NULLIF(j.completed_at, '1970-01-01')) AS max_stop_completed_ts
    FROM
      dispatchdb_shards.shardable_dispatch_routes r
      LEFT JOIN jobs j ON r.id = j.dispatch_route_id
      AND r.org_id = j.org_id
    WHERE
      DATE(r.created_at) BETWEEN DATE('{PARTITION_KEY_RANGE_START}') AND DATE('{PARTITION_KEY_RANGE_END}')
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12
  )
SELECT
  DATE(created_ts) AS `date`,
  route_id,
  org_id,
  route_name,
  parent_recurring_route_id,
  device_id,
  trailer_device_id,
  driver_id,
  created_ts,
  updated_ts,
  scheduled_start_ts,
  scheduled_end_ts,
  SUM(num_stops) AS num_stops,
  MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT (stop_state, num_stops))) AS num_stops_by_state,
  CASE
    WHEN IFNULL(num_stops_by_state['completed'], 0) + IFNULL(num_stops_by_state['skipped'], 0) = SUM(num_stops) THEN TRUE
    ELSE FALSE
  END AS is_completed_route,
  ANY (max_stop_completed_ts <= scheduled_end_ts) AND is_completed_route AS is_route_completed_on_time
FROM
  routes
WHERE
  stop_state IS NOT NULL
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12
UNION ALL
SELECT
  DATE(created_ts) AS `date`,
  route_id,
  org_id,
  route_name,
  parent_recurring_route_id,
  device_id,
  trailer_device_id,
  driver_id,
  created_ts,
  updated_ts,
  scheduled_start_ts,
  scheduled_end_ts,
  0 AS num_stops,
  MAP() AS num_stops_by_state,
  FALSE AS is_completed_route,
  FALSE AS is_route_completed_on_time
FROM
  routes
WHERE
  stop_state IS NULL
"""

stg_dispatch_routes_assets = build_assets_from_sql(
    name="stg_dispatch_routes",
    schema=stg_dispatch_routes_schema,
    description="""A staging table containing dispatch route related information as well as the statuses of all associated stops""",
    sql_query=stg_dispatch_routes_query,
    primary_keys=["date", "route_id", "org_id"],
    upstreams=[
        AssetKey(["dispatchdb_shards", "shardable_dispatch_routes"]),
        AssetKey(["dispatchdb_shards", "shardable_dispatch_jobs"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

stg_dispatch_routes_us = stg_dispatch_routes_assets[AWSRegions.US_WEST_2.value]
stg_dispatch_routes_eu = stg_dispatch_routes_assets[AWSRegions.EU_WEST_1.value]
stg_dispatch_routes_ca = stg_dispatch_routes_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_dispatch_routes"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_dispatch_routes",
        table="stg_dispatch_routes",
        primary_keys=["date", "route_id", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

fct_dispatch_routes_schema = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "The calendar date in 'YYYY-mm-dd' format corresponding to the creation of the route"
        },
    },
    {
        "name": "route_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The unique ID of the route. Note - There is a possibility that a single route_id can exist across multiple orgs"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The Samsara cloud dashboard ID that the data belongs to"
        },
    },
    {
        "name": "route_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The Internal ID for the customer's Samsara org"},
    },
    {
        "name": "parent_recurring_route_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "In case of a recurring route, the route_id that has been used as a template to create this route"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the route is configured for a vehicle"
        },
    },
    {
        "name": "trailer_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the route is configured for a trailer"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The driver ID if assigned to the route"},
    },
    {
        "name": "created_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp in UTC when the route was created"},
    },
    {
        "name": "updated_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp in UTC when the route was updated"},
    },
    {
        "name": "scheduled_start_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The UTC timestamp indicating when the route was scheduled to begin"
        },
    },
    {
        "name": "scheduled_end_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The UTC timestamp indicating when the route was scheduled to end"
        },
    },
    {
        "name": "num_stops",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Total number of stops configured for the route"},
    },
    {
        "name": "num_stops_by_state",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "long",
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": """Number of stops in the route grouped by their curent state.
                     State can be one of unassigned, scheduled, enroute, completed, skipped or arrived"""
        },
    },
    {
        "name": "is_completed_route",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": """Boolean flag indicating whether the route is considered to be complete.
            Defined as the route having at least one stop that is completed/arrived and no stops that
            are in a non-terminal state (unassigned, scheduled or enroute)"""
        },
    },
    {
        "name": "is_route_completed_on_time",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": """Boolean flag indicating whether the route is considered to be complete on time.
                     i.e., whether the last completed/arrived stop is earlier than the scheduled end timestamp"""
        },
    },
]


fct_dispatch_routes_query = """
--sql

SELECT
  `date`,
  route_id,
  org_id,
  route_name,
  parent_recurring_route_id,
  device_id,
  trailer_device_id,
  driver_id,
  created_ts,
  updated_ts,
  scheduled_start_ts,
  scheduled_end_ts,
  num_stops,
  num_stops_by_state,
  is_completed_route,
  is_route_completed_on_time
FROM
    `{database_silver_dev}`.stg_dispatch_routes
WHERE
    {PARTITION_FILTERS}
"""

fct_dispatch_routes_assets = build_assets_from_sql(
    name="fct_dispatch_routes",
    schema=fct_dispatch_routes_schema,
    description=build_table_description(
        table_desc="""This table provides a daily record of routes created on that day.
The table also contains information on the statuses of all associated stops within a route.
A route is equivalent to a planned trip""",
        row_meaning="""A unique route created on a specific date, along with details on the configuration of the route.
Note - There is a possibility that a single route_id can exist across multiple orgs. Please keep that in mind when aggregating route counts.
""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_dispatch_routes_query,
    primary_keys=["date", "route_id", "org_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_dispatch_routes"])],
    regions=[
        AWSRegions.US_WEST_2.value,
        AWSRegions.EU_WEST_1.value,
        AWSRegions.CA_CENTRAL_1.value,
    ],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

fct_dispatch_routes_us = fct_dispatch_routes_assets[AWSRegions.US_WEST_2.value]
fct_dispatch_routes_eu = fct_dispatch_routes_assets[AWSRegions.EU_WEST_1.value]
fct_dispatch_routes_ca = fct_dispatch_routes_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_dispatch_routes"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_dispatch_routes",
        table="fct_dispatch_routes",
        primary_keys=["date", "route_id", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_dispatch_routes"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_fct_dispatch_routes",
        database=databases["database_gold_dev"],
        table="fct_dispatch_routes",
        blocking=True,
    )
)

dqs["fct_dispatch_routes"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_dispatch_routes",
        database=databases["database_gold_dev"],
        table="fct_dispatch_routes",
        non_null_columns=[
            "route_id",
            "org_id",
            "created_ts",
            "num_stops",
            "is_completed_route",
        ],
        blocking=True,
    )
)

dq_assets = dqs.generate()
