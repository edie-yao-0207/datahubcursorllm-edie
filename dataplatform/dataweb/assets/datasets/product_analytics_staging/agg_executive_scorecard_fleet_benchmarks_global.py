from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TrendDQCheck,
    table,
)
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.executive_scorecard_constants import (
    MAX_RETRIES,
    PARTITIONING,
    PRIMARY_KEYS,
    SCHEMA,
)
from dataweb.userpkgs.constants import (
    ACCOUNT_BILLING_COUNTRY,
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.product_usage_utils import (
    get_enabled_users_sql,
    load_product_usage_config,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)


QUERY = """
WITH pubsec_us AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM finopsdb.customer_info
),
pubsec_eu AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
),
account_arr AS (
    -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
    SELECT
        account_id,
        customer_arr,
        customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
),
account_arr_sam AS (
    -- Handles cases where there's no account_id match (take max for SAM instead)
    SELECT sam_number_undecorated AS sam_number,
    MAX_BY(customer_arr, customer_arr) AS customer_arr,
    MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
    GROUP BY sam_number_undecorated
),
orgs_ga_status AS (
    -- All orgs have access
    SELECT
        o.org_id,
        org_name,
        CASE
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
            ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
        END AS account_arr_segment,
        COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
        {ACCOUNT_BILLING_COUNTRY}
        COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
        COALESCE(o.account_id, 'No Account ID') AS account_id,
        COALESCE(o.account_name, 'No Account Name') AS account_name,
        c.org_category,
        'us-west-2' AS region
    FROM datamodel_core.dim_organizations o
    JOIN product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_us p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'

    UNION ALL

    SELECT
        o.org_id,
        org_name,
        CASE
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
            ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
        END AS account_arr_segment,
        COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
        {ACCOUNT_BILLING_COUNTRY}
        COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
        COALESCE(o.account_id, 'No Account ID') AS account_id,
        COALESCE(o.account_name, 'No Account Name') AS account_name,
        c.org_category,
        'eu-west-1' AS region
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    JOIN data_tools_delta_share.product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_eu p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
),
license_filter AS (
    -- Despite serving all customers, we want to make sure the org has at least one semi-active license of the core types
    SELECT DISTINCT o.org_id
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN datamodel_core.dim_organizations o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND (fo.net_quantity > 0)
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license = TRUE
        AND fo.product_sku != 'LIC-VG-ASAT'

    UNION ALL

    SELECT DISTINCT o.org_id
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND (fo.net_quantity > 0)
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license = TRUE
        AND fo.product_sku != 'LIC-VG-ASAT'
),
benchmarks_orgs_with_old_utilization_us AS (
    -- Filters out orgs on the old report still (will eventually be removed)
    SELECT
        o.id AS org_id,
        MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
        MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
    FROM
        releasemanagementdb_shards.feature_packages AS fp
    JOIN releasemanagementdb_shards.feature_package_self_serve AS fpss
        ON fpss.feature_package_uuid = fp.uuid
    JOIN clouddb.organizations AS o
        ON o.id = fpss.org_id
    WHERE
        fp.ld_feature_key = 'data-products-vehicle-utilization-benchmark'
        AND fp.deleted = 0
    GROUP BY
        ALL
    HAVING
        is_feature_enabled = 1
),
feature_packages_eu AS (
    SELECT
        ld_feature_key,
        uuid
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_packages_v0`
    WHERE deleted = 0

    UNION ALL

    SELECT
        ld_feature_key,
        uuid
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_packages_v0`
    WHERE deleted = 0
),
feature_package_self_serve_eu AS (
    SELECT
        feature_package_uuid,
        enabled,
        updated_at,
        org_id
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_package_self_serve_v2`

    UNION ALL

    SELECT
        feature_package_uuid,
        enabled,
        updated_at,
        org_id
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_package_self_serve_v2`
),
benchmarks_orgs_with_old_utilization_eu AS (
    -- This is the EU filter for removing orgs on the old report
    SELECT
        o.id AS org_id,
        MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
        MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
    FROM
        feature_packages_eu AS fp
        JOIN feature_package_self_serve_eu AS fpss
            ON fpss.feature_package_uuid = fp.uuid
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` AS o
            ON o.id = fpss.org_id
    WHERE
        fp.ld_feature_key = 'data-products-vehicle-utilization-benchmark'
    GROUP BY
        ALL
    HAVING
        is_feature_enabled = 1
),
benchmark_orgs_with_old_utilization AS (
    SELECT *
    FROM benchmarks_orgs_with_old_utilization_us

    UNION ALL

    SELECT *
    FROM benchmarks_orgs_with_old_utilization_eu
),
enabled_orgs AS (
    -- Update (6/16): the utilization flag is no longer a gate to benchmarks usage, so removing it as a filter condition
    -- It'll still be used for tracking usage between the GA and flag update date
    SELECT *
    FROM orgs_ga_status
    WHERE (
        '{PARTITION_START}' >= '{UTILIZATION_UPDATE_DATE}'
        OR org_id NOT IN (
            SELECT DISTINCT
                org_id
            FROM
                benchmark_orgs_with_old_utilization
            WHERE enabled_date <= '{PARTITION_START}'
        )
    )
),
customer_orgs_eu AS (
    -- EU customer orgs who flipped on Fleet Benchmarks before it was released
    SELECT CAST('562949953429372' AS BIGINT) AS org_id, CAST('2025-05-07' AS DATE) AS benchmark_feature_enabled_date, CAST('2025-05-07T12:18:53.121' AS TIMESTAMP) AS benchmark_feature_enabled_datetime

    UNION ALL

    SELECT CAST('562949953426393' AS BIGINT), CAST('2025-05-06' AS DATE), CAST('2025-05-06T08:23:44.642' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953422822' AS BIGINT), CAST('2025-05-13' AS DATE), CAST('2025-05-13T13:37:16.674' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953427357' AS BIGINT), CAST('2025-05-05' AS DATE), CAST('2025-05-05T20:58:10.430' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953428308' AS BIGINT), CAST('2025-05-13' AS DATE), CAST('2025-05-13T08:38:33.625' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429561' AS BIGINT), CAST('2025-03-12' AS DATE), CAST('2025-03-12T20:30:54.359' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429764' AS BIGINT), CAST('2025-05-02' AS DATE), CAST('2025-05-02T09:49:32.941' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429799' AS BIGINT), CAST('2025-05-02' AS DATE), CAST('2025-05-02T07:46:13.978' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953427017' AS BIGINT), CAST('2025-05-02' AS DATE), CAST('2025-05-02T10:14:18.380' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953427471' AS BIGINT), CAST('2025-05-06' AS DATE), CAST('2025-05-06T11:35:09.636' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429797' AS BIGINT), CAST('2025-05-12' AS DATE), CAST('2025-05-12T13:39:33.123' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953428383' AS BIGINT), CAST('2025-05-09' AS DATE), CAST('2025-05-09T13:49:47.985' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429946' AS BIGINT), CAST('2025-05-07' AS DATE), CAST('2025-05-07T17:28:05.612' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953426706' AS BIGINT), CAST('2025-05-08' AS DATE), CAST('2025-05-08T09:47:49.557' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953426677' AS BIGINT), CAST('2025-05-01' AS DATE), CAST('2025-05-01T16:41:11.959' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429025' AS BIGINT), CAST('2025-05-07' AS DATE), CAST('2025-05-07T10:26:05.762' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429839' AS BIGINT), CAST('2025-05-07' AS DATE), CAST('2025-05-07T12:32:44.117' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429834' AS BIGINT), CAST('2025-05-08' AS DATE), CAST('2025-05-08T09:36:30.922' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429334' AS BIGINT), CAST('2025-05-09' AS DATE), CAST('2025-05-09T17:13:24.762' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429974' AS BIGINT), CAST('2025-05-06' AS DATE), CAST('2025-05-06T05:10:11.547' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953428930' AS BIGINT), CAST('2025-05-08' AS DATE), CAST('2025-05-08T09:37:05.229' AS TIMESTAMP)

    UNION ALL

    SELECT CAST('562949953429259' AS BIGINT), CAST('2025-05-06' AS DATE), CAST('2025-05-06T12:42:56.085' AS TIMESTAMP)
),
{ENABLED_USERS_SQL}
relevant_route_loads_us AS (
    -- Usage is defined as:
    -- 1. Hits to the page before the GA date for orgs in the Beta opt-in
    -- 2. Hits to the page after the GA date and before the flag update date for orgs not on the FF
    -- 3. Hits to the page after the flag update date
    -- Join to enabled users to solve case of changing emails
    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM perf_infra.org_route_loads AS a
    JOIN (
        SELECT
            org_id,
            benchmark_feature_enabled_date,
            benchmark_feature_enabled_datetime
        FROM
            dataanalytics_dev.fleet_benchmarks_orgs_in_beta
        WHERE
            internal_type_name = 'Customer Org'
    ) AS b
        ON a.org_id = b.org_id
        AND a.date >= b.benchmark_feature_enabled_date
        AND a.timestamp >= b.benchmark_feature_enabled_datetime
    LEFT OUTER JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date < '{GA_DATE}'
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'

    UNION ALL

    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM perf_infra.org_route_loads AS a
    LEFT JOIN benchmarks_orgs_with_old_utilization_us AS b
        ON a.org_id = b.org_id
    LEFT OUTER JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date BETWEEN '{GA_DATE}' AND DATE_SUB('{UTILIZATION_UPDATE_DATE}', 1)
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND b.org_id IS NULL
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'

    UNION ALL

    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM perf_infra.org_route_loads AS a
    LEFT JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date >= '{UTILIZATION_UPDATE_DATE}'
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
),
org_route_loads_eu AS (
    -- This is the EU equivalent of perf_infra.org_route_loads
    SELECT
        date,
        timestamp,
        resource,
        org_id,
        org.name AS name,
        user_id,
        usr.email AS email,
        duration_ms,
        raw_url,
        owner AS team_owner,
        sloGrouping AS slo_grouping,
        initial_load,
        http_protocol,
        h2_possible,
        trace_id
    FROM delta.`s3://samsara-eu-data-streams-delta-lake/frontend_routeload` frl
    INNER JOIN perf_infra.route_config rc -- This is the same in US/EU
        ON frl.resource = rc.path
    INNER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` org
        ON frl.org_id = org.id
    INNER JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/users_v1` usr
        ON frl.user_id = usr.id
    WHERE
        usr.email NOT LIKE '%@samsara.com'
        AND had_error = false
        AND org.internal_type != 1
        AND duration_ms < 60000
),
relevant_route_loads_eu AS (
    -- Usage is defined as:
    -- 1. Hits to the page before the GA date for orgs in the Beta opt-in
    -- 2. Hits to the page after the GA date for orgs not on the FF
    -- 3. Hits to the page after the flag update date
    -- Join to enabled users to solve case of changing emails
    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM org_route_loads_eu AS a
    JOIN (
        SELECT
            org_id,
            benchmark_feature_enabled_date,
            benchmark_feature_enabled_datetime
        FROM
            customer_orgs_eu
    ) AS b
        ON a.org_id = b.org_id
    AND a.date >= b.benchmark_feature_enabled_date
    AND a.timestamp >= b.benchmark_feature_enabled_datetime
    LEFT OUTER JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date < '{GA_DATE}'
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'

    UNION ALL

    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM org_route_loads_eu AS a
    LEFT JOIN benchmarks_orgs_with_old_utilization_eu AS b
        ON a.org_id = b.org_id
    LEFT OUTER JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date BETWEEN '{GA_DATE}' AND DATE_SUB('{UTILIZATION_UPDATE_DATE}', 1)
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND b.org_id IS NULL
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'


    UNION ALL

    SELECT
        a.org_id,
        a.date AS event_date,
        COALESCE(eu.user_id, a.email) AS user_id
    FROM org_route_loads_eu AS a
    LEFT OUTER JOIN enabled_users eu
        ON a.email = eu.email
        AND a.org_id = eu.org_id
    WHERE
        a.resource = '/o/:org_id/fleet/reports/fleet_benchmarks'
        AND a.email NOT LIKE '%@samsara.com'
        AND a.email NOT LIKE '%samsara.canary%'
        AND a.email NOT LIKE '%samsara.forms.canary%'
        AND a.email NOT LIKE '%samsaracanarydevcontractor%'
        AND a.email NOT LIKE '%samsaratest%'
        AND a.email NOT LIKE '%@samsara%'
        AND a.email NOT LIKE '%@samsara-service-account.com'
        AND a.date >= '{UTILIZATION_UPDATE_DATE}'
        AND a.email NOT IN (
            'majcherlk@gmail.com', 'jonathancobian8+standardadmin_org@gmail.com',
            'jonathancobian8+standardadmin_tag@gmail.com', 'jonathancobian8+fulladmin_org@gmail.com',
            'jonathancobian8+fulladmin_tag@gmail.com', 'jonathancobian8+customreadwrite_org@gmail.com',
            'jonathancobian8+customreadwrite_tag@gmail.com', 'jonathancobian8+customcombo_tag@gmail.com',
            'jonathancobian8+customreadonly_org@gmail.com', 'jonathancobian8+customreadonly_tag@gmail.com',
            'jonathancobian8+readonlyadmin_org@gmail.com', 'jonathancobian8+readonlyadmin_tag@gmail.com'
        )
        AND a.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
),
relevant_route_loads AS (
    SELECT * FROM relevant_route_loads_us

    UNION ALL

    SELECT * FROM relevant_route_loads_eu
),
routes_aggregated AS (
    SELECT
        org_id,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN 1 END) AS usage_weekly,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN 1 END) AS usage_monthly,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN 1 END) AS usage_prior_month,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN 1 END) AS usage_weekly_prior_month,
        COUNT(DISTINCT CASE WHEN event_date = '{PARTITION_START}' THEN user_id END) AS daily_active_user,
        COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN user_id END) AS weekly_active_users,
        COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN user_id END) AS monthly_active_users,
        MAX(CASE WHEN event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day
    FROM relevant_route_loads
    GROUP BY org_id
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (page visits)
-- usage_monthly: Usage in the last 28 days (page visits)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (page visits)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (page visits)
-- daily_active_user: Number of unique users using the feature on the day in question (in this context, visiting the page)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (in this context, visiting the page)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (in this context, visiting the page)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days
-- org_active_day: Whether the org used the feature on the day in question
-- org_active_week: Whether the org used the feature in the last 7 days
-- org_active_week_prior_month: Whether the org used the feature in the 7 days prior to the last 28 days
-- org_active_month: Whether the org used the feature in the last 28 days
-- org_active_prior_month: Whether the org used the feature in the 28 days prior to the last 28 days
SELECT
    '{PARTITION_START}' AS date,
    o.org_id,
    o.org_name,
    o.org_category,
    o.account_arr_segment,
    o.account_size_segment_name,
    o.account_billing_country,
    o.region,
    o.sam_number,
    o.account_id,
    o.account_name,
    1 AS enabled,
    COALESCE(r.usage_weekly, 0) AS usage_weekly,
    COALESCE(r.usage_monthly, 0) AS usage_monthly,
    COALESCE(r.usage_prior_month, 0) AS usage_prior_month,
    COALESCE(r.usage_weekly_prior_month, 0) AS usage_weekly_prior_month,
    COALESCE(r.daily_active_user, 0) AS daily_active_user,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= '{PARTITION_START}' THEN eu.user_id END), 0) AS daily_enabled_users,
    COALESCE(r.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 6) THEN eu.user_id END), 0) AS weekly_enabled_users,
    COALESCE(r.monthly_active_users, 0) AS monthly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 27) THEN eu.user_id END), 0) AS monthly_enabled_users,
    COALESCE(r.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM enabled_orgs o
JOIN license_filter lf
    ON o.org_id = lf.org_id
LEFT OUTER JOIN routes_aggregated r
    ON r.org_id = o.org_id
LEFT OUTER JOIN enabled_users eu
    ON eu.org_id = o.org_id
GROUP BY
    o.org_id,
    o.org_name,
    o.org_category,
    o.account_arr_segment,
    o.account_size_segment_name,
    o.account_billing_country,
    o.region,
    o.sam_number,
    o.account_id,
    o.account_name,
    r.usage_weekly,
    r.usage_monthly,
    r.usage_prior_month,
    r.usage_weekly_prior_month,
    r.daily_active_user,
    r.weekly_active_users,
    r.monthly_active_users,
    r.org_active_day
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Fleet Benchmarks feature""",
        row_meaning="""Each row represents an org's usage of Fleet Benchmarks over various time periods""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONING,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=MAX_RETRIES,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_fleet_benchmarks_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_fleet_benchmarks_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_fleet_benchmarks_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_fleet_benchmarks_global_enabled_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_enabled_users < weekly_active_users
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_fleet_benchmarks_global_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_monthly < usage_weekly
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_fleet_benchmarks_global_high_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_weekly > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_fleet_benchmarks_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(
            name="dq_trend_agg_executive_scorecard_fleet_benchmarks_global_enablement",
            lookback_days=1,
            tolerance=0.05,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_fleet_benchmarks_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_fleet_benchmarks_global
                    WHERE date = (SELECT DISTINCT DATE_SUB(date, 7) FROM df)
                )
                SELECT CASE WHEN last_week_count = 0 THEN 0
                            ELSE ABS(today_count - last_week_count) * 1.0 / last_week_count
                            END AS observed_value
                FROM today_usage
                CROSS JOIN last_week_usage
            """,
            expected_value=0.2,
            operator=Operator.lte,
            block_before_write=False,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:perf_infra.org_route_loads",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:releasemanagementdb_shards.feature_packages",
        "us-west-2|eu-west-1:releasemanagementdb_shards.feature_package_self_serve",
        "us-west-2|eu-west-1:clouddb.organizations",
        "us-west-2|eu-west-1:dataanalytics_dev.fleet_benchmarks_orgs_in_beta",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users_organizations",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations_permissions",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_fleet_benchmarks_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_fleet_benchmarks_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("fleet_benchmarks")
    enabled_users_sql = get_enabled_users_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        GA_DATE='2025-05-14',
        UTILIZATION_UPDATE_DATE='2025-06-17',
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
        ENABLED_USERS_SQL=enabled_users_sql,
    )
    context.log.info(f"{query}")
    return query
