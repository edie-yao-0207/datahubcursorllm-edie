from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
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

daily_partition_def = DailyPartitionsDefinition(start_date="2023-09-01")

pipeline_group_name = "fct_hos_logs"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_hos_logs_schema = [
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
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "notes",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The annotation linked to the HoS log"},
    },
    {
        "name": "is_assigned",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the HoS log entry has been assigned to a driver"
        },
    },
    {
        "name": "is_annotated",
        "type": "boolean",
        "nullable": True,
        "metadata": {"comment": "Whether the HoS log entry has been annotated"},
    },
]

stg_hos_logs_query = """
--sql

SELECT
    `date`,
    org_id,
    vehicle_id AS device_id,
    driver_id,
    notes,
    (driver_id > 0) AS is_assigned,
    (
        notes IS NOT NULL
        AND TRIM(notes) <> ''
    ) AS is_annotated
FROM
    compliancedb_shards.driver_hos_logs
WHERE
    `date` = '{DATEID}'
    AND (
        -- Exclude logs marked as inactive
        log_proto.event_record_status <> 2
        OR log_proto.event_record_status IS NULL
    )
"""

stg_hos_logs_assets = build_assets_from_sql(
    name="stg_hos_logs",
    schema=stg_hos_logs_schema,
    description="""Fact table for HoS (hours of service) log data with a few attributes related to the logs.
                Excludes HoS logs marked as inactive""",
    sql_query=stg_hos_logs_query,
    primary_keys=["date", "device_id", "org_id"],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

stg_hos_logs_us = stg_hos_logs_assets[AWSRegions.US_WEST_2.value]
stg_hos_logs_eu = stg_hos_logs_assets[AWSRegions.EU_WEST_1.value]
stg_hos_logs_ca = stg_hos_logs_assets[AWSRegions.CA_CENTRAL_1.value]

fct_hos_logs_schema = [
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
        "name": "first_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the first HoS log logged by the device"
        },
    },
    {
        "name": "last_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the last HoS log logged by the device"
        },
    },
    {
        "name": "first_assigned_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the first assigned HoS log logged by the device"
        },
    },
    {
        "name": "last_assigned_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the last assigned HoS log logged by the device"
        },
    },
    {
        "name": "first_annotated_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the first annotated HoS log logged by the device"
        },
    },
    {
        "name": "last_annotated_hos_log_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date corresponding to the last annotated HoS log logged by the device"
        },
    },
    {
        "name": "hos_log_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of HoS logs logged by the device on the day. Excludes logs marked as inactive due to being inaccurate/outdated"
        },
    },
    {
        "name": "assigned_hos_log_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of assigned HoS logs logged by the device on the day. Excludes logs marked as inactive due to being inaccurate/outdated"
        },
    },
    {
        "name": "annotated_hos_log_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of annotated HoS logs logged by the device on the day. Excludes logs marked as inactive due to being inaccurate/outdated"
        },
    },
    {
        "name": "assigned_or_annotated_hos_log_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The total number of assigned or annotated HoS logs logged by the device on the day. Excludes logs marked as inactive due to being inaccurate/outdated"
        },
    },
]

fct_hos_logs_init_query = """
--sql

SELECT
    '{DATEID}' AS `date`,
    org_id,
    vehicle_id AS device_id,
    MIN(`date`) AS first_hos_log_date,
    MAX(`date`) AS last_hos_log_date,
    MIN(
        CASE
            WHEN (driver_id > 0) THEN `date`
            ELSE NULL
        END
    ) AS first_assigned_hos_log_date,
    MAX(
        CASE
            WHEN (driver_id > 0) THEN `date`
            ELSE NULL
        END
    ) AS last_assigned_hos_log_date,
    MIN(
        CASE
            WHEN (
                notes IS NOT NULL
                AND TRIM(notes) <> ''
            ) THEN `date`
            ELSE NULL
        END
    ) AS first_annotated_hos_log_date,
    MAX(
        CASE
            WHEN (
                notes IS NOT NULL
                AND TRIM(notes) <> ''
            ) THEN `date`
            ELSE NULL
        END
    ) AS last_annotated_hos_log_date,
    COUNT(
        CASE
            WHEN `date` = '{DATEID}' THEN 1
            ELSE NULL
        END
    ) AS hos_log_count,
    COUNT(
        CASE
            WHEN `date` = '{DATEID}'
            AND (driver_id > 0) THEN 1
            ELSE NULL
        END
    ) AS assigned_hos_log_count,
    COUNT(
        CASE
            WHEN `date` = '{DATEID}'
            AND (
                notes IS NOT NULL
                AND TRIM(notes) <> ''
            ) THEN 1
            ELSE NULL
        END
    ) AS annotated_hos_log_count,
    COUNT(
        CASE
            WHEN `date` = '{DATEID}'
            AND (
                driver_id > 0
                OR (
                    notes IS NOT NULL
                    AND TRIM(notes) <> ''
                )
            ) THEN 1
            ELSE NULL
        END
    ) AS assigned_or_annotated_hos_log_count
FROM
    compliancedb_shards.driver_hos_logs
WHERE
    `date` <= '{DATEID}'
    AND (
        -- Exclude logs marked as inactive
        log_proto.event_record_status <> 2
        OR log_proto.event_record_status IS NULL
    )
GROUP BY
    org_id,
    device_id
"""

fct_hos_logs_query = """
--sql

WITH
    yesterday AS (
        SELECT
            org_id,
            device_id,
            first_hos_log_date,
            last_hos_log_date,
            first_assigned_hos_log_date,
            last_assigned_hos_log_date,
            first_annotated_hos_log_date,
            last_annotated_hos_log_date
        FROM
            `{database_gold_dev}`.fct_hos_logs
        WHERE
            `date` = CAST('{DATEID}' AS DATE) - 1
    ),
    today AS (
        SELECT
            `date`,
            org_id,
            device_id,
            COUNT(1) AS hos_log_count,
            SUM(CAST(is_assigned AS INT)) AS assigned_hos_log_count,
            SUM(CAST(is_annotated AS INT)) AS annotated_hos_log_count,
            SUM(
                CAST(
                    is_assigned
                    OR is_annotated AS INT
                )
            ) AS assigned_or_annotated_hos_log_count
        FROM
            `{database_silver_dev}`.stg_hos_logs
        WHERE
            `date` = '{DATEID}'
        GROUP BY
            `date`,
            org_id,
            device_id
    )
SELECT
    '{DATEID}' AS `date`,
    COALESCE(t.org_id, y.org_id) AS org_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(y.first_hos_log_date, t.date) AS first_hos_log_date,
    CASE
        WHEN COALESCE(y.last_hos_log_date, '0101-01-01') < COALESCE(t.date, '0101-01-01') THEN t.date
        ELSE y.last_hos_log_date
    END AS last_hos_log_date,
    COALESCE(
        y.first_assigned_hos_log_date,
        CASE
            WHEN COALESCE(t.assigned_hos_log_count, 0) > 0 THEN t.date
            ELSE NULL
        END
    ) AS first_assigned_hos_log_date,
    CASE
        WHEN COALESCE(t.assigned_hos_log_count, 0) > 0
        AND COALESCE(y.last_assigned_hos_log_date, '0101-01-01') < COALESCE(t.date, '0101-01-01') THEN t.date
        ELSE y.last_assigned_hos_log_date
    END AS last_assigned_hos_log_date,
    COALESCE(
        y.first_annotated_hos_log_date,
        CASE
            WHEN COALESCE(t.annotated_hos_log_count, 0) > 0 THEN t.date
            ELSE NULL
        END
    ) AS first_annotated_hos_log_date,
    CASE
        WHEN COALESCE(t.annotated_hos_log_count, 0) > 0
        AND COALESCE(y.last_annotated_hos_log_date, '0101-01-01') < COALESCE(t.date, '0101-01-01') THEN t.date
        ELSE y.last_annotated_hos_log_date
    END AS last_annotated_hos_log_date,
    t.hos_log_count,
    t.assigned_hos_log_count,
    t.annotated_hos_log_count,
    t.assigned_or_annotated_hos_log_count
FROM
    today t
    FULL OUTER JOIN yesterday y ON y.org_id = t.org_id
    AND y.device_id = t.device_id
"""

fct_hos_logs_assets = build_assets_from_sql(
    name="fct_hos_logs",
    schema=fct_hos_logs_schema,
    description=build_table_description(
        table_desc="""A daily record of HoS (hours of service) log counts at a device level.
        Also has additional attributes related to first/last log dates for the device""",
        row_meaning="""HoS (hours of service) log counts logged for a device on a given day.
        Filter by the date column to limit the results to HoS logs created within a time period. """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_hos_logs_query,
    primary_keys=["date", "device_id", "org_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "stg_hos_logs"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    depends_on_past=True,
    # alt_query(init) is used only for the start_date of the daily_partition_def above
    query_type="cumulative",
    custom_query_params={"alt_query": fct_hos_logs_init_query},
)

fct_hos_logs_us = fct_hos_logs_assets[AWSRegions.US_WEST_2.value]
fct_hos_logs_eu = fct_hos_logs_assets[AWSRegions.EU_WEST_1.value]
fct_hos_logs_ca = fct_hos_logs_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_hos_logs"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_hos_logs",
        table="fct_hos_logs",
        primary_keys=["date", "device_id", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_hos_logs"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_fct_hos_logs",
        database=databases["database_gold_dev"],
        table="fct_hos_logs",
        blocking=True,
    )
)

dqs["fct_hos_logs"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_hos_logs",
        database=databases["database_gold_dev"],
        table="fct_hos_logs",
        non_null_columns=[
            "org_id",
            "device_id",
            "first_hos_log_date",
            "last_hos_log_date",
        ],
        blocking=True,
    )
)

dq_assets = dqs.generate()
