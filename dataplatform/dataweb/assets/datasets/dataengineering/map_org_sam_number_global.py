from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
    ColumnDescription,
)
from dataweb.userpkgs.utils import build_table_description


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""A dataset that unifies organization data from US and EU regions, providing a mapping between org_id and sam_number.""",
        row_meaning="""Organization mapping data for a specific date""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {
            "name": "date",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
            },
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
            "metadata": {
                "comment": ColumnDescription.SAM_NUMBER
            },
        },
        {
            "name": "region",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "Samsara cloud region"},
        },
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
    ],
    primary_keys=[
        "date",
        "org_id",
        "region",
    ],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_map_org_sam_number_global"),
        NonNullDQCheck(
            name="dq_non_null_map_org_sam_number_global",
            non_null_columns=["date", "org_id", "region"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_map_org_sam_number_global",
            primary_keys=[
                "date",
                "org_id",
                "region",
            ],
            block_before_write=True,
        ),
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def map_org_sam_number_global(context: AssetExecutionContext) -> str:

    query = """
    WITH
    us_orgs AS (
        SELECT
            orgs.date,
            orgs.org_id,
            orgs.sam_number,
            'us-west-2' AS region
        FROM datamodel_core.dim_organizations AS orgs
        WHERE orgs.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY 1,2,3,4
    ),
    eu_orgs AS (
        SELECT
            orgs.date,
            orgs.org_id,
            orgs.sam_number,
            'eu-west-1' AS region
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` AS orgs
        WHERE orgs.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY 1,2,3,4
    ),
    ca_orgs AS (
        SELECT
            orgs.date,
            orgs.org_id,
            orgs.sam_number,
            'ca-central-1' AS region
        FROM data_tools_delta_share_ca.datamodel_core.dim_organizations AS orgs
        WHERE orgs.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
        GROUP BY 1,2,3,4
    )
    SELECT
        date,
        org_id,
        sam_number,
        region
    FROM us_orgs

    UNION

    SELECT
        date,
        org_id,
        sam_number,
        region
    FROM eu_orgs

    UNION

    SELECT
        date,
        org_id,
        sam_number,
        region
    FROM ca_orgs
    """
    return query

