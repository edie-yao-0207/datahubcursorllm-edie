from collections import defaultdict
from datetime import datetime, timedelta

from dagster import AssetKey

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    Operator,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_source_assets,
    build_table_description,
    get_all_regions,
)

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

pipeline_group_name = "dim_product_variants"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=None,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

dqs["stg_product_variants"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_product_variants",
        table="stg_product_variants",
        primary_keys=["product_id", "variant_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_product_variants"].append(
    NonNullDQCheck(
        name="dq_non_null_stg_product_variants",
        table="stg_product_variants",
        non_null_columns=["product_id", "variant_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    ),
)

dim_product_variants_schema = [
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The internal ID of the Samsara product"},
    },
    {
        "name": "variant_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The internal ID of the Samsara product variant"},
    },
    {
        "name": "variant_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name of the product variant"},
    },
    {
        "name": "batch_list_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The batch list name of the product variant"},
    },
    {
        "name": "hw_variant_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The hardware sku version of the name used in Salesforce"
        },
    },
]

dim_product_variants_query = """
--sql
with src as (
select
    cast(product_id as bigint) as product_id
    ,cast(variant_id as bigint) as variant_id
    ,cast(variant_name as string) as variant_name
    ,cast(batch_list_name as string) as batch_list_name
from {database_silver_dev}.stg_product_variants
)
select
    product_id
    ,variant_id
    ,variant_name
    ,batch_list_name
    -- overrides for variant naming as required for HW use cases in SFDC
    ,case
    when (product_id = 55 and variant_id = 4) then 'HW-SGR1'
    when (product_id = 76 and variant_id = 0) then 'ACC-WM11'
    when (product_id = 94 and variant_id = 0) then 'ACC-CM-ANLG'
    when (product_id = 123 and variant_id = 0) then 'HW-CM25'
    when (product_id = 126 and variant_id = 0) then 'HW-CM-AHD1'
    when (product_id = 213 and variant_id = 0) then 'HW-MC-AIM4'
    when product_id = 178 then concat('HW-', replace(variant_name,'Freya','VG55'))
    when split(variant_name,'-')[0] not in ('MobileApp', 'ACC', 'CBL', 'App', 'HW', 'none')
        then concat('HW-', variant_name)
    else variant_name end as hw_variant_name
from src
"""


dim_product_variants_assets = build_assets_from_sql(
    name="dim_product_variants",
    schema=dim_product_variants_schema,
    description=build_table_description(
        table_desc="""Table providing mapping from Product ID + variant ID to human-readable variant names """,
        row_meaning="""A unique entry for a product variant along with the corresponding product ID.
        Note that the default variant_id (0) can be repeated across multiple products""",
        related_table_info={},
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=dim_product_variants_query,
    primary_keys=["product_id", "variant_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_product_variants"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=None,
)

dim_product_variants_us = dim_product_variants_assets[AWSRegions.US_WEST_2.value]
dim_product_variants_eu = dim_product_variants_assets[AWSRegions.EU_WEST_1.value]
dim_product_variants_ca = dim_product_variants_assets[AWSRegions.CA_CENTRAL_1.value]

yesterday = datetime.now() - timedelta(days=1)

dqs["dim_product_variants"].append(
    SQLDQCheck(
        name="dq_unique_hw_variant_names_count",
        database=databases["database_gold_dev"],
        sql_query=f"""
        with current_count as (
            select count(distinct variant_name) as variant_count from datamodel_core.dim_product_variants
        ),
        previous_count as (
            select count(distinct variant_name) as variant_count from datamodel_core.dim_product_variants@{yesterday.strftime("%Y%m%d")}23595909999
        )
        select current_count.variant_count * 1.0 / previous_count.variant_count as observed_value
        from current_count, previous_count
        """,
        expected_value=(1, 2),
        blocking=True,
        multi_date=True,
        operator=Operator.between,
    )
)

dqs["dim_product_variants"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_product_variants",
        table="dim_product_variants",
        primary_keys=["product_id", "variant_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_product_variants"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_dim_product_variants",
        database=databases["database_gold_dev"],
        table="dim_product_variants",
        blocking=True,
    )
)


dq_assets = dqs.generate()
