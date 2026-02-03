from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "sku"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.ORG_ID},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": ColumnDescription.SAM_NUMBER},
    },
    {
        "name": "is_trial",
        "type": "boolean",
        "nullable": True,
        "metadata": {"comment": "Boolean value equal to TRUE when license is under free trial, otherwise FALSE or NULL."},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The type of organization a deivce is in (ie customer, internal, test, etc)."
        },
    },
    {
        "name": "sku",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "License sku, does not represent quantity of licenses only that at least one license is held."},
    },
    {
        "name": "product_family",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Most recent family of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."},
    },
    {
        "name": "sub_product_line",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Most recent sub_product_line_c of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."},
    },
    {
        "name": "quantity",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Quantity of licenses held by organization as of date."
        },
    },
    {
        "name": "date_expired",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "Date that sku was last seen being active for org_id"
        },
    },
    {
        "name": "date_first_active",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date where sku was first polled as being active for org_id. Earliest date is 2023-10-30."
        },
    },
    {
        "name": "tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Pricing and packaging tier of license, sourced from the license SKU - premier, essential, public sector, enterprise"
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Samsara cloud region"},
    },
]

QUERY = """
    WITH orgs_us AS (
        SELECT date,
        org_id,
        'us-west-2' AS region
        FROM datamodel_core.dim_organizations
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type = 0
        AND account_first_purchase_date IS NOT NULL
    ),
    orgs_eu AS (
        SELECT date,
        org_id,
        'eu-west-1' AS region
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type = 0
        AND account_first_purchase_date IS NOT NULL
    ),
    orgs_ca AS (
        SELECT date,
        org_id,
        'ca-central-1' AS region
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        AND internal_type = 0
        AND account_first_purchase_date IS NOT NULL
    ),
    licenses_us AS (
        SELECT CAST(date AS STRING) date,
            org_id,
            sam_number,
            is_trial,
            internal_type,
            sku,
            product_family,
            sub_product_line,
            quantity,
            date_expired,
            date_first_active,
            tier,
            'us-west-2' AS region
        FROM datamodel_platform.dim_licenses
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    licenses_eu AS (
        SELECT CAST(date AS STRING) date,
            org_id,
            sam_number,
            is_trial,
            internal_type,
            sku,
            product_family,
            sub_product_line,
            quantity,
            date_expired,
            date_first_active,
            tier,
            'eu-west-1' AS region
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_licenses`
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    ),
    licenses_ca AS (
        SELECT CAST(date AS STRING) date,
            org_id,
            sam_number,
            is_trial,
            internal_type,
            sku,
            product_family,
            sub_product_line,
            quantity,
            date_expired,
            date_first_active,
            tier,
            'ca-central-1' AS region
        FROM data_tools_delta_share_ca.datamodel_platform.dim_licenses
        WHERE date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    )
    SELECT l.*
    FROM licenses_us l
    JOIN orgs_us o
    ON l.date = o.date
    AND l.org_id = o.org_id

    UNION

    SELECT l.*
    FROM licenses_eu l
    JOIN orgs_eu o
    ON l.date = o.date
    AND l.org_id = o.org_id

    UNION

    SELECT l.*
    FROM licenses_ca l
    JOIN orgs_ca o
    ON l.date = o.date
    AND l.org_id = o.org_id
"""


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""Active licenses data to be used in metrics repo. NOTE: this table is now deprecated. Please use edw.silver.fct_license_orders going forward.""",
        row_meaning="""Each row contains an active licenses on the given day in question""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    backfill_start_date="2023-01-01",
    backfill_batch_size=14,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_licenses_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_dim_licenses_global", primary_keys=PRIMARY_KEYS, block_before_write=True
        ),
        NonNullDQCheck(
            name="dq_non_null_dim_licenses_global", non_null_columns=PRIMARY_KEYS, block_before_write=True
        ),
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_licenses",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
    ],
)
def dim_licenses_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_licenses_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[1]
    query = QUERY.format(
        FIRST_PARTITION_START=PARTITION_START,
        FIRST_PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
