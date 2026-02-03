from dagster import AssetKey, BackfillPolicy, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
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
    "database_core_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
    "database_core_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2023-11-01")

pipeline_group_name = "fct_dvirs"

BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=10)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_dvirs_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The calendar date in 'YYYY-mm-dd' format"},
    },
    {
        "name": "dvir_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The unique ID of the DVIR"},
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
            "comment": "The ID of the customer device if the DVIR is for a vehicle"
        },
    },
    {
        "name": "trailer_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the DVIR is for a trailer"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The ID of the driver creating the DVIR"},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the user creating the DVIR in case the creator is not a driver"
        },
    },
    {
        "name": "dvir_template_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The unique ID or the template used in creating the DVIR"
        },
    },
    {
        "name": "created_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp when the driver/user created the DVIR"},
    },
    {
        "name": "updated_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp for when the DVIR was most recently updated"
        },
    },
    {
        "name": "dvir_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The type of the DVIR. One of 'truck', 'trailer' or 'truck+trailer' "
        },
    },
    {
        "name": "inspection_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """The type of inspection performed as a part of the DVIR creation
            .One of 'pretrip', 'posttrip' or 'mechanic' """
        },
    },
    {
        "name": "defects_need_correction",
        "type": "byte",
        "nullable": True,
        "metadata": {
            "comment": "1 or 0 depending on whether there were any defects noted as a part of the DVIR that need to be resolved"
        },
    },
    {
        "name": "defects_corrected",
        "type": "byte",
        "nullable": True,
        "metadata": {
            "comment": "1 or 0 depending on whether defects noted as a part of the DVIR were resolved"
        },
    },
    {
        "name": "defects_ignored",
        "type": "byte",
        "nullable": True,
        "metadata": {
            "comment": "1 or 0 depending on whether defects noted as a part of the DVIR can be ignored"
        },
    },
    {
        "name": "dvir_resolving_driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Driver ID of the driver who next marked the asset as safe, if applicable"
        },
    },
    {
        "name": "dvir_resolving_user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "User ID of the mechanic to next marked the asset as safe, if applicable"
        },
    },
    {
        "name": "dvir_resolved_signed_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Timestamp for when the asset was next marked as safe"},
    },
    {
        "name": "dvir_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The current status of the DVIR. One of 'safe', 'unsafe' or 'resolved'"
        },
    },
]


stg_dvirs_query = """
--sql

SELECT
    `date`,
    id AS dvir_id,
    org_id,
    vehicle_id AS device_id,
    trailer_device_id,
    NULLIF(driver_id, 0) AS driver_id,
    NULLIF(author_user_id, 0) AS user_id,
    dvir_template_uuid,
    created_at AS created_ts,
    updated_at AS updated_ts,
    dvir_type,
    CASE inspection_type
        WHEN 1 THEN 'pretrip'
        WHEN 2 THEN 'posttrip'
        WHEN 3 THEN 'mechanic'
        ELSE NULL
    END AS inspection_type,
    defects_need_correction,
    defects_corrected,
    defects_ignored,
    NULLIF(next_safe_dvir_driver_id, 0) AS dvir_resolving_driver_id,
    NULLIF(next_safe_dvir_user_id, 0) AS dvir_resolving_user_id,
    NULLIF(next_safe_dvir_signed_at, '1970-01-01') AS dvir_resolved_signed_ts,
    CASE
        WHEN (
            defects_corrected = 1
            OR defects_ignored = 1
        ) THEN 'resolved'
        WHEN defects_need_correction = 0 THEN 'safe'
        ELSE 'unsafe'
    END AS dvir_status
FROM
    statsdb.dvirs
WHERE
    {PARTITION_FILTERS}
"""

stg_dvirs_assets = build_assets_from_sql(
    name="stg_dvirs",
    schema=stg_dvirs_schema,
    description=""" Staging table at a DVIR grain which is a cleaned up version of statsdb.dvirs """,
    sql_query=stg_dvirs_query,
    primary_keys=["date", "dvir_id"],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

stg_dvirs_us = stg_dvirs_assets[AWSRegions.US_WEST_2.value]
stg_dvirs_eu = stg_dvirs_assets[AWSRegions.EU_WEST_1.value]
stg_dvirs_ca = stg_dvirs_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_dvirs"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_dvirs",
        table="stg_dvirs",
        primary_keys=["date", "dvir_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

fct_dvirs_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The calendar date in 'YYYY-mm-dd' format"},
    },
    {
        "name": "dvir_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The unique ID of the DVIR"},
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
        "metadata": {
            "comment": "The ID of the customer device if the DVIR is for a vehicle"
        },
    },
    {
        "name": "trailer_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device if the DVIR is for a trailer"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The ID of the driver creating the DVIR"},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the user creating the DVIR in case the creator is not a driver"
        },
    },
    {
        "name": "dvir_template_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The unique ID or the template used in creating the DVIR"
        },
    },
    {
        "name": "created_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The timestamp when the driver/user created the DVIR"},
    },
    {
        "name": "updated_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp for when the DVIR was most recently updated"
        },
    },
    {
        "name": "dvir_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The type of the DVIR. One of 'truck', 'trailer' or 'truck+trailer' "
        },
    },
    {
        "name": "inspection_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """The type of inspection performed as a part of the DVIR creation
            .One of 'pretrip', 'posttrip' or 'mechanic' """
        },
    },
    {
        "name": "dvir_resolving_driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Driver ID of the driver who next marked the asset as safe, if applicable"
        },
    },
    {
        "name": "dvir_resolving_user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "User ID of the mechanic to next marked the asset as safe, if applicable"
        },
    },
    {
        "name": "dvir_resolved_signed_ts",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Timestamp for when the asset was next marked as safe"},
    },
    {
        "name": "dvir_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The current status of the DVIR. One of 'safe', 'unsafe' or 'resolved'"
        },
    },
]

fct_dvirs_query = """
--sql
SELECT
    `date`,
    dvir_id,
    org_id,
    device_id,
    trailer_device_id,
    driver_id,
    user_id,
    dvir_template_uuid,
    created_ts,
    updated_ts,
    dvir_type,
    inspection_type,
    dvir_resolving_driver_id,
    dvir_resolving_user_id,
    dvir_resolved_signed_ts,
    dvir_status
FROM
    `{database_silver_dev}`.stg_dvirs
WHERE
    {PARTITION_FILTERS}
"""

fct_dvirs_assets = build_assets_from_sql(
    name="fct_dvirs",
    schema=fct_dvirs_schema,
    description=build_table_description(
        table_desc=""" A daily record of driver vehicle inspection records (DVIRs) created on that day.
        Additional attributes of the DVIR along with the most current status of the DVIR are also included. """,
        row_meaning=""" A unique driver vehicle inspection record along with the various attributes of the DVIR.
        Filter by the date column to limit the results to DVIRs created within a time period. """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_dvirs_query,
    primary_keys=["date", "dvir_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_dvirs"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

fct_dvirs_us = fct_dvirs_assets[AWSRegions.US_WEST_2.value]
fct_dvirs_eu = fct_dvirs_assets[AWSRegions.EU_WEST_1.value]
fct_dvirs_ca = fct_dvirs_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_dvirs"].append(
    NonNullDQCheck(
        name="dq_null_fct_dvirs",
        database=databases["database_gold_dev"],
        table="fct_dvirs",
        non_null_columns=["dvir_id", "org_id", "dvir_type", "dvir_status"],
        blocking=False,
    )
)

dqs["fct_dvirs"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_dvirs",
        table="fct_dvirs",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_dvirs"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_dvirs",
        table="fct_dvirs",
        primary_keys=["date", "dvir_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dq_assets = dqs.generate()
