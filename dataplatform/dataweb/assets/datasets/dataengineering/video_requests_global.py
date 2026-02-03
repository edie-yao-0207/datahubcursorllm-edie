from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""Video request statuses across a given day""",
        row_meaning="""Video request event for a given device""",
        # related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "id", "type": "long", "nullable": False, "metadata": {"comment": "ID of video request"}},
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date of the video request, in `YYYY-mm-dd` format."}},
        {"name": "created_at_ms", "type": "long", "nullable": False, "metadata": {"comment": "Time at which request was made"}},
        {"name": "device_type", "type": "string", "nullable": False, "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        }},
        {
            "name": "org_id",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "The Samsara cloud dashboard ID that the data belongs to"
            },
        },
        {
            "name": "sam_number",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Queried from edw.silver.dim_customer. Identification of the account"
            },
        },
        {
            "name": "account_size_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_arr_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": """ARR from the previously publicly reporting quarter in the buckets:
                <0
                0 - 100k
                100k - 500K,
                500K - 1M
                1M+

                Note - ARR information is calculated at the account level, not the SAM Number level
                An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
                """
            },
        },
        {
            "name": "account_industry",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Industry classification of the account."},
        },
        {"name": "region", "type": "string", "nullable": False, "metadata": {"comment": "AWS cloud region where record comes from"}},
        {
            "name": "is_retrieval_successful",
            "type": "integer",
            "nullable": False,
            "metadata": {"comment": "Whether retrieval was successful or not"},
        },
    ],
    upstreams=[],
    primary_keys=[
        "id",
        "date",
    ],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_video_requests_global"),
        NonNullDQCheck(name="dq_non_null_video_requests_global", non_null_columns=["date", "org_id", "region", "is_retrieval_successful", "id", "created_at_ms", "device_type"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_video_requests_global", primary_keys=["id", "date"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
)
def video_requests_global(context: AssetExecutionContext) -> str:

    query = """
    WITH
    us_videos_requested AS (
        SELECT
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM clouddb.historical_video_requests hvr -- find video retrieval records
        INNER JOIN datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
            ON hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN datamodel_core.dim_organizations as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        where hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --mult
            AND hvr.is_incognito = 0 -- not incognito (internal user)
        GROUP BY 1,2,3,4,5,6,7,8,9
    ),
    us_video_requests_succeeded AS (
        select
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM clouddb.historical_video_requests hvr-- find video retrieval records
        INNER JOIN datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
            ON  hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN datamodel_core.dim_organizations as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        WHERE
            hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --multicam
            AND hvr.is_incognito = 0 -- not incognito (internal user)

            -- below filters are used to remove failing video retrieval attempts (limit to only passing retrievals)
            AND hvr.id not in (
                        -- Exclude Retrievals that never finished processing
                        select clouddb.historical_video_requests.id
                        from clouddb.historical_video_requests
                        where
                        date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND asset_ready_at_ms is not null
                        AND completed_at_ms is null -- Don't include if uploaded less than 2 hours ago
                        AND asset_ready_at_ms < (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 2 * 60000 * 60)
                        AND state = 0
                    )
            AND hvr.id not in (
                        -- Exclude Retrievals that never uploaded
                        select a.id
                        from clouddb.historical_video_requests a
                        INNER JOIN datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
                        ON a.device_id = devices.device_id
                        AND a.date = devices.date
                        left join (select * from kinesisstats_history.osddashcamreport where date >= '{FIRST_PARTITION_START}') as dashcamreport
                        on a.id = cast(dashcamreport.value.int_value as int)
                        where a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND dashcamreport.time is not null
                        AND a.asset_ready_at_ms is null
                        -- Don't include if requested less than 24 hours ago
                        AND a.created_at_ms <= (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 24 * 60000 * 60) -- end_date
                        AND a.state = 0
                    ) -- Exclude Retrievals Marked as Failed
            AND hvr.id not in (
                        select b.id
                        from clouddb.historical_video_requests b
                        where b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        -- Failed and TimedOut
                        AND b.state in (1, 3)
                    ) -- Exclude Cancelled 1 Hour after upload (indicating stuck)
            AND hvr.id not in (
                        select c.id
                        from clouddb.historical_video_requests c
                        join datastreams_history.video_retrieval_cancelled_log
                        on c.id = datastreams_history.video_retrieval_cancelled_log.historical_video_request_id
                        where c.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        and datastreams_history.video_retrieval_cancelled_log.date >= c.date
                        AND (unix_millis(TIMESTAMP(datastreams_history.video_retrieval_cancelled_log.timestamp)) - c.asset_ready_at_ms) > 60 * 60000
                    )
        GROUP BY 1,2,3,4,5,6,7,8,9

    ),
    us_video_requests AS (
        SELECT
            vrr.id,
            vrr.date,
            vrr.created_at_ms,
            vrr.device_type,
            vrr.org_id,
            vrr.sam_number,
            vrr.account_size_segment,
            vrr.account_arr_segment,
            vrr.account_industry,
            'us-west-2' AS region,
            NVL2(vrs.id, 1, 0) AS is_retrieval_successful
    FROM us_videos_requested vrr
    LEFT JOIN us_video_requests_succeeded vrs
    ON  vrr.date = vrs.date
    AND vrr.id = vrs.id
    AND  vrr.created_at_ms = vrs.created_at_ms
    AND  vrr.org_id = vrs.org_id
    AND  vrr.sam_number = vrs.sam_number
    AND  vrr.device_type = vrs.device_type
    AND  vrr.account_size_segment = vrs.account_size_segment
    AND  vrr.account_industry = vrs.account_industry
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
    eu_videos_requested AS (
        SELECT
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` hvr -- find video retrieval records
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` as devices -- this join is to exclude multi-cam devices
            ON hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        where hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --mult
            AND hvr.is_incognito = 0 -- not incognito (internal user)
        GROUP BY 1,2,3,4,5,6,7,8,9
    ),
    eu_video_requests_succeeded AS (
        select
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` hvr-- find video retrieval records
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` as devices -- this join is to exclude multi-cam devices
            ON  hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        WHERE
            hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --multicam
            AND hvr.is_incognito = 0 -- not incognito (internal user)

            -- below filters are used to remove failing video retrieval attempts (limit to only passing retrievals)
            AND hvr.id not in (
                        -- Exclude Retrievals that never finished processing
                        select id
                        from delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1`
                        where
                        date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND asset_ready_at_ms is not null
                        AND completed_at_ms is null -- Don't include if uploaded less than 2 hours ago
                        AND asset_ready_at_ms < (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 2 * 60000 * 60)
                        AND state = 0
                    )
            AND hvr.id not in (
                        -- Exclude Retrievals that never uploaded
                        select a.id
                        from delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` a
                        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` as devices -- this join is to exclude multi-cam devices
                        ON a.device_id = devices.device_id
                        AND a.date = devices.date
                        left join (select * from delta.`s3://samsara-eu-kinesisstats-delta-lake/table/deduplicated/osDDashcamReport` where date >= '{FIRST_PARTITION_START}') as dashcamreport
                        on a.id = cast(dashcamreport.value.int_value as int)
                        where a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND dashcamreport.time is not null
                        AND a.asset_ready_at_ms is null
                        -- Don't include if requested less than 24 hours ago
                        AND a.created_at_ms <= (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 24 * 60000 * 60) -- end_date
                        AND a.state = 0
                    ) -- Exclude Retrievals Marked as Failed
            AND hvr.id not in (
                        select b.id
                        from delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` b
                        where b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        -- Failed and TimedOut
                        AND b.state in (1, 3)
                    ) -- Exclude Cancelled 1 Hour after upload (indicating stuck)
            AND hvr.id not in (
                        select c.id
                        from delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` c
                        join delta.`s3://samsara-eu-data-streams-delta-lake/video_retrieval_cancelled_log` vrcl
                        on c.id = vrcl.historical_video_request_id
                        where c.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        and vrcl.date >= c.date
                        AND (unix_millis(TIMESTAMP(vrcl.timestamp)) - c.asset_ready_at_ms) > 60 * 60000
                    )
        GROUP BY 1,2,3,4,5,6,7,8,9

    ),
    eu_video_requests AS (
        SELECT
            vrr.id,
            vrr.date,
            vrr.created_at_ms,
            vrr.device_type,
            vrr.org_id,
            vrr.sam_number,
            vrr.account_size_segment,
            vrr.account_arr_segment,
            vrr.account_industry,
            'eu-west-1' AS region,
            NVL2(vrs.id, 1, 0) AS is_retrieval_successful
    FROM eu_videos_requested vrr
    LEFT JOIN eu_video_requests_succeeded vrs
    ON  vrr.date = vrs.date
    AND vrr.id = vrs.id
    AND  vrr.created_at_ms = vrs.created_at_ms
    AND  vrr.org_id = vrs.org_id
    AND  vrr.sam_number = vrs.sam_number
    AND  vrr.device_type = vrs.device_type
    AND  vrr.account_size_segment = vrs.account_size_segment
    AND  vrr.account_industry = vrs.account_industry
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
ca_videos_requested AS (
        SELECT
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` hvr -- find video retrieval records
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
            ON hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        where hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --mult
            AND hvr.is_incognito = 0 -- not incognito (internal user)
        GROUP BY 1,2,3,4,5,6,7,8,9
    ),
    ca_video_requests_succeeded AS (
        select
            hvr.id,
            hvr.date,
            hvr.created_at_ms,
            devices.product_name as device_type,
            orgs.org_id,
            orgs.sam_number,
            orgs.account_size_segment,
            orgs.account_arr_segment,
            orgs.account_industry
        FROM delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` hvr-- find video retrieval records
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
            ON  hvr.device_id = devices.device_id
            AND hvr.date = devices.date
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations as orgs -- this join is to find non-internal records
            ON orgs.org_id = devices.org_id
            AND hvr.date = orgs.date
        WHERE
            hvr.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
            AND orgs.internal_type = 0 -- non-internal
            AND orgs.account_first_purchase_date IS NOT NULL -- had at least one purchase
            AND devices.product_id not in (46) --multicam
            AND hvr.is_incognito = 0 -- not incognito (internal user)

            -- below filters are used to remove failing video retrieval attempts (limit to only passing retrievals)
            AND hvr.id not in (
                        -- Exclude Retrievals that never finished processing
                        select id
                        from delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1`
                        where
                        date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND asset_ready_at_ms is not null
                        AND completed_at_ms is null -- Don't include if uploaded less than 2 hours ago
                        AND asset_ready_at_ms < (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 2 * 60000 * 60)
                        AND state = 0
                    )
            AND hvr.id not in (
                        -- Exclude Retrievals that never uploaded
                        select a.id
                        from delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` a
                        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices as devices -- this join is to exclude multi-cam devices
                        ON a.device_id = devices.device_id
                        AND a.date = devices.date
                        left join (select * from delta.`s3://samsara-ca-kinesisstats-delta-lake/table/deduplicated/osDDashcamReport` where date >= '{FIRST_PARTITION_START}') as dashcamreport
                        on a.id = cast(dashcamreport.value.int_value as int)
                        where a.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        AND dashcamreport.time is not null
                        AND a.asset_ready_at_ms is null
                        -- Don't include if requested less than 24 hours ago
                        AND a.created_at_ms <= (unix_timestamp(date_add('{FIRST_PARTITION_END}', 1), 'yyyy-MM-dd') * 1000 - 24 * 60000 * 60) -- end_date
                        AND a.state = 0
                    ) -- Exclude Retrievals Marked as Failed
            AND hvr.id not in (
                        select b.id
                        from delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` b
                        where b.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        -- Failed and TimedOut
                        AND b.state in (1, 3)
                    ) -- Exclude Cancelled 1 Hour after upload (indicating stuck)
            AND hvr.id not in (
                        select c.id
                        from delta.`s3://samsara-ca-rds-delta-lake/table-parquet/prod-clouddb/prod_db/historical_video_requests_v1` c
                        join delta.`s3://samsara-ca-data-streams-delta-lake/video_retrieval_cancelled_log` vrcl
                        on c.id = vrcl.historical_video_request_id
                        where c.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
                        and vrcl.date >= c.date
                        AND (unix_millis(TIMESTAMP(vrcl.timestamp)) - c.asset_ready_at_ms) > 60 * 60000
                    )
        GROUP BY 1,2,3,4,5,6,7,8,9

    ),
    ca_video_requests AS (
        SELECT
            vrr.id,
            vrr.date,
            vrr.created_at_ms,
            vrr.device_type,
            vrr.org_id,
            vrr.sam_number,
            vrr.account_size_segment,
            vrr.account_arr_segment,
            vrr.account_industry,
            'ca-central-1' AS region,
            NVL2(vrs.id, 1, 0) AS is_retrieval_successful
    FROM ca_videos_requested vrr
    LEFT JOIN ca_video_requests_succeeded vrs
    ON  vrr.date = vrs.date
    AND vrr.id = vrs.id
    AND  vrr.created_at_ms = vrs.created_at_ms
    AND  vrr.org_id = vrs.org_id
    AND  vrr.sam_number = vrs.sam_number
    AND  vrr.device_type = vrs.device_type
    AND  vrr.account_size_segment = vrs.account_size_segment
    AND  vrr.account_industry = vrs.account_industry
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
    SELECT
        id,
        date,
        created_at_ms,
        device_type,
        org_id,
        sam_number,
        account_size_segment,
        account_arr_segment,
        account_industry,
        region,
        is_retrieval_successful
    FROM us_video_requests

    UNION

    SELECT
        id,
        date,
        created_at_ms,
        device_type,
        org_id,
        sam_number,
        account_size_segment,
        account_arr_segment,
        account_industry,
        region,
        is_retrieval_successful
    FROM eu_video_requests

    UNION

    SELECT
        id,
        date,
        created_at_ms,
        device_type,
        org_id,
        sam_number,
        account_size_segment,
        account_arr_segment,
        account_industry,
        region,
        is_retrieval_successful
    FROM ca_video_requests
    """
    return query
