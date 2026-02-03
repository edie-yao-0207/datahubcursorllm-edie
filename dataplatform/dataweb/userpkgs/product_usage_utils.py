import json
from datetime import datetime, timedelta
from dataweb.userpkgs.constants import (
    GLOBAL_ARR_QUERY,
    GLOBAL_PUBSEC_QUERY,
)
from dataweb.userpkgs.executive_scorecard_constants import (
    INITIAL_LAUNCHDARKLY_DATE,
    LAUNCHDARKLY_FALSE_JOIN_CONDITION,
    LAUNCHDARKLY_TRUE_JOIN_CONDITION,
    SERVES_ALL_LICENSE_CTE,
)
from pathlib import Path
from typing import (
    Dict,
    Optional,
)

def _generate_base_orgs_sql(partition_start: str, regions: list[str] = None) -> str:
    """
    Generate SQL for the base organizations table

    Parameters:
        partition_start (str): The date partition being processed
        regions (list[str], optional): List of regions to filter by
    Returns: SQL string
    """

    if regions is not None:
        region = regions[0] # right now, this will only have a single entry
        if region == 'us':
            return f"""
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
                COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
                COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
                COALESCE(o.account_id, 'No Account ID') AS account_id,
                COALESCE(o.account_name, 'No Account Name') AS account_name,
                c.org_category,
                o.created_at,
                o.locale,
                o.release_type,
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
                o.date = '{partition_start}'
            """
        else: # eu
            return f"""
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
                COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
                WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
                WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
                WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
                WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
                WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
                WHEN o.account_billing_country IS NULL THEN NULL
                ELSE 'Other' END, 'Unknown') AS account_billing_country,
                COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
                COALESCE(o.account_id, 'No Account ID') AS account_id,
                COALESCE(o.account_name, 'No Account Name') AS account_name,
                c.org_category,
                o.created_at,
                o.locale,
                o.release_type,
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
                o.date = '{partition_start}'
            """
    else:
        # no region filter, union us and eu
        return f"""
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
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
            WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
            WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
            WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
            WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
            WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
            WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
            WHEN o.account_billing_country IS NULL THEN NULL
            ELSE 'Other' END, 'Unknown') AS account_billing_country,
            COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
            COALESCE(o.account_id, 'No Account ID') AS account_id,
            COALESCE(o.account_name, 'No Account Name') AS account_name,
            c.org_category,
            o.created_at,
            o.locale,
            o.release_type,
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
            o.date = '{partition_start}'

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
            COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
            WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
            WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
            WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
            WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
            WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
            WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
            WHEN o.account_billing_country IS NULL THEN NULL
            ELSE 'Other' END, 'Unknown') AS account_billing_country,
            COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
            COALESCE(o.account_id, 'No Account ID') AS account_id,
            COALESCE(o.account_name, 'No Account Name') AS account_name,
            c.org_category,
            o.created_at,
            o.locale,
            o.release_type,
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
            o.date = '{partition_start}'
        """


def _generate_launchdarkly_sql(flag_name: list[str], partition_start: str) -> str:
    """
    Generate SQL for integrating LaunchDarkly logic into one of the Exec Scorecard (Product Usage dashboard) tables
    Parameters:
        flag_name (list[str]): The list of names of the LaunchDarkly feature flags to integrate.
        partition_start (str): The date partition being processed
    Returns: SQL string
    """

    quoted_flags = [f"'{flag}'" for flag in flag_name]
    flag_filter = f" IN ({', '.join(quoted_flags)})"

    return f"""
    relevant_segments AS (
        -- Get any segments for the FF
        SELECT *
        FROM datamodel_launchdarkly_bronze.segment_flag
        WHERE
            key {flag_filter}
            AND date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.segment_flag
                WHERE date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    variations AS (
        -- Get all variations for the FF
        SELECT
            ffv.value,
            ffv.feature_flag_key,
            vim.variation_index
        FROM datamodel_launchdarkly_bronze.feature_flag_variation ffv
        JOIN release_management.ld_flag_variation_index_map vim
            ON ffv.feature_flag_key = vim.key
            AND UPPER(ffv.value) = UPPER(vim.variation_value)
        WHERE
            ffv.feature_flag_key {flag_filter}
            AND ffv.date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_variation
                WHERE date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    fallthrough_variation AS (
        SELECT *
        FROM datamodel_launchdarkly_bronze.feature_flag_environment
        WHERE
            feature_flag_key {flag_filter}
            AND date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment
                WHERE date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    segment_parsing AS (
    -- Get various rules from segments for org enablement
        SELECT
            rs.key AS feature_flag_key,
            rc.segment_rule_id AS rule_id,
            rc.attribute,
            rc.op,
            rc.negate,
            FROM_JSON(rc.values, 'array<string>') AS value_array
        FROM datamodel_launchdarkly_bronze.rule_clause rc
        JOIN relevant_segments rs
            ON rc.segment_key = rs.segment_key
        WHERE
            rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
            AND rc.date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.rule_clause rc
                WHERE rc.date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    relevant_rules AS (
        -- Get all relevant rules for the FF
        SELECT *
        FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
        WHERE
            feature_flag_key {flag_filter}
            AND date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
                WHERE date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    rule_clauses_parsed AS (
        -- Parse to find all orgs that are also enabled from FF rules
        SELECT
            rc.feature_flag_key,
            rc.feature_flag_environment_rule_id AS rule_id,
            rc.attribute,
            rc.op,
            rc.negate,
            LOWER(v.value) AS variation,
            FROM_JSON(rc.values, 'array<string>') AS value_array
        FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
        JOIN relevant_rules ffr
            ON ffr.id = rc.feature_flag_environment_rule_id
        JOIN variations v
            ON ffr.variation = v.variation_index
            AND rc.feature_flag_key = v.feature_flag_key
        WHERE
            rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
            AND rc.date = (
                SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause
                WHERE date <= GREATEST('{partition_start}', '{INITIAL_LAUNCHDARKLY_DATE}')
            )
    ),
    launchdarkly_orgs AS (
        SELECT DISTINCT
            feature_flag_key,
            rule_id,
            attribute,
            'true' AS variation,
            op,
            negate,
            value
        FROM segment_parsing
        LATERAL VIEW EXPLODE(value_array) AS value

        UNION

        SELECT DISTINCT
            feature_flag_key,
            rule_id,
            attribute,
            variation,
            op,
            negate,
            value
        FROM rule_clauses_parsed
        LATERAL VIEW EXPLODE(value_array) AS value
    ),
    pivoted_conditions AS (
        -- Pivot to apply multiple conditions in the same rule if exists
        SELECT
            feature_flag_key,
            rule_id,
            variation,
            op,
            negate,
            COLLECT_SET(CASE WHEN attribute = 'orgId' THEN value ELSE NULL END) AS org_ids,
            COLLECT_SET(CASE WHEN attribute = 'orgLocale' THEN value ELSE NULL END) AS org_locales,
            COLLECT_SET(CASE WHEN attribute = 'orgReleaseType' THEN value ELSE NULL END) AS org_release_types,
            COLLECT_SET(CASE WHEN attribute = 'orgCreatedAt' THEN value ELSE NULL END) AS org_created_at,
            COLLECT_SET(CASE WHEN attribute = 'samNumber' THEN value ELSE NULL END) AS sam_numbers,
            COLLECT_SET(CASE WHEN attribute = 'orgIsInternal' THEN value ELSE NULL END) AS is_internal
        FROM launchdarkly_orgs
        GROUP BY feature_flag_key, rule_id, variation, op, negate
    ),
    pivoted_conditions_exploded AS (
        SELECT
            feature_flag_key,
            rule_id,
            variation,
            op,
            negate,
            org_ids,
            org_locales,
            org_release_types,
            sam_numbers,
            is_internal,
            exploded_ts AS org_created_at_ts
        FROM (
            SELECT
                *,
                EXPLODE(
                    CASE
                    WHEN SIZE(org_created_at) = 0 OR org_created_at IS NULL THEN ARRAY(NULL)
                    ELSE org_created_at
                    END
                ) AS exploded_ts
            FROM pivoted_conditions
        ) pc
    ),"""


def _generate_opt_in_orgs_sql(flag_name: list[str], opt_in_filter: str) -> str:
    """
    Generates SQL for handling self-serve orgs
    This will be used if the release phase is Open Beta

    Parameters:
        flag_name(list[str]): List of names of FFs
        opt_in_filter (str): The filter condition to apply for opt-in orgs
    Returns: SQL string
    """

    quoted_flags = [f"'{flag}'" for flag in flag_name]
    flag_filter = f" IN ({', '.join(quoted_flags)})"

    if opt_in_filter != "":
        return f"""
        opt_in_orgs_us AS (
            -- Orgs who have opted in via the Feature Flag
            SELECT
                o.id AS org_id,
                fp.ld_feature_key,
                MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
                MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
            FROM releasemanagementdb_shards.feature_packages AS fp
            JOIN releasemanagementdb_shards.feature_package_self_serve AS fpss
                ON fpss.feature_package_uuid = fp.uuid
            JOIN clouddb.organizations AS o
                ON o.id = fpss.org_id
            WHERE
                fp.ld_feature_key {flag_filter}
                AND fp.deleted = 0 -- still active
                {opt_in_filter}
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
                org_id,
                scope
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_package_self_serve_v2`

            UNION ALL

            SELECT
                feature_package_uuid,
                enabled,
                updated_at,
                org_id,
                scope
            FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_package_self_serve_v2`
        ),
        opt_in_orgs_eu AS (
            -- Orgs who have opted in via the Feature Flag
            SELECT
                o.id AS org_id,
                fp.ld_feature_key,
                MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
                MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
            FROM feature_packages_eu AS fp
            JOIN feature_package_self_serve_eu AS fpss
                ON fpss.feature_package_uuid = fp.uuid
            JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` AS o
                ON o.id = fpss.org_id
            WHERE
                fp.ld_feature_key {flag_filter}
                {opt_in_filter}
            GROUP BY
                ALL
            HAVING
                is_feature_enabled = 1
        ),"""
    else:
        return f"""
    opt_in_orgs_us AS (
        -- Orgs who have opted in via the Feature Flag
        SELECT
            o.id AS org_id,
            fp.ld_feature_key,
            MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
            MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
        FROM releasemanagementdb_shards.feature_packages AS fp
        JOIN releasemanagementdb_shards.feature_package_self_serve AS fpss
            ON fpss.feature_package_uuid = fp.uuid
        JOIN clouddb.organizations AS o
            ON o.id = fpss.org_id
        WHERE
            fp.ld_feature_key {flag_filter}
            AND fp.deleted = 0 -- still active
            AND fpss.scope = 1 -- org-scoped
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
            org_id,
            scope
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagement-shard-1db/releasemanagementdb/feature_package_self_serve_v2`

        UNION ALL

        SELECT
            feature_package_uuid,
            enabled,
            updated_at,
            org_id,
            scope
        FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-releasemanagementdb/releasemanagementdb/feature_package_self_serve_v2`
    ),
    opt_in_orgs_eu AS (
        -- Orgs who have opted in via the Feature Flag
        SELECT
            o.id AS org_id,
            fp.ld_feature_key,
            MAX_BY(fpss.enabled, fpss.updated_at) as is_feature_enabled,
            MAX_BY(DATE(fpss.updated_at), fpss.updated_at) AS enabled_date
        FROM feature_packages_eu AS fp
        JOIN feature_package_self_serve_eu AS fpss
            ON fpss.feature_package_uuid = fp.uuid
        JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` AS o
            ON o.id = fpss.org_id
        WHERE
            fp.ld_feature_key {flag_filter}
            AND fpss.scope = 1 --org-scoped
        GROUP BY
            ALL
        HAVING
            is_feature_enabled = 1
    ),"""



def _generate_license_sql(partition_start: str, skus: list[str], min_quantity: int = 0, override_orgs: list[int] = None) -> str:
    """
    Generate SQL for the licenses CTE
    Parameters:
        partition_start (str): The date partition being processed.
        skus (list[str]): List of license names to filter by
        min_quantity (int): Minimum quantity required for the license to be considered valid
        override_orgs (list[int]): List of org IDs that should bypass license count check
    Returns: SQL string
    """
    quoted_skus = [f"'{sku}'" for sku in skus]
    licenses_filter = f"fo.product_sku IN ({', '.join(quoted_skus)})"
    if override_orgs:
        quoted_override_orgs = [str(org) for org in override_orgs]
        org_filter = f"o.org_id IN ({', '.join(quoted_override_orgs)})"

        return f"""
        relevant_licenses AS (
            SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
            FROM edw.silver.fct_license_orders_daily_snapshot fo
            JOIN datamodel_core.dim_organizations o
                ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
            JOIN edw.silver.license_hw_sku_xref xref
                ON fo.product_sku = xref.license_sku
            WHERE
                o.date = '{partition_start}'
                AND DATE(fo._run_dt) BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}'
                AND (
                    (fo.net_quantity > {min_quantity})
                    OR (fo.net_quantity > 0 AND {org_filter})
                )
                AND (
                    {licenses_filter}
                )

            UNION ALL

            SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
            FROM edw.silver.fct_license_orders_daily_snapshot fo
            JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
                ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
            JOIN edw.silver.license_hw_sku_xref xref
                ON fo.product_sku = xref.license_sku
            WHERE
                o.date = '{partition_start}'
                AND DATE(fo._run_dt) BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}'
                AND (
                    (fo.net_quantity > {min_quantity})
                    OR (fo.net_quantity > 0 AND {org_filter})
                )
                AND (
                    {licenses_filter}
                )
        ),
        relevant_licenses_deduped AS (
            SELECT DISTINCT org_id
            FROM relevant_licenses
        ),
        """
    else:
        return f"""
        relevant_licenses AS (
            SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
            FROM edw.silver.fct_license_orders_daily_snapshot fo
            JOIN datamodel_core.dim_organizations o
                ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
            JOIN edw.silver.license_hw_sku_xref xref
                ON fo.product_sku = xref.license_sku
            WHERE
                o.date = '{partition_start}'
                AND DATE(fo._run_dt) BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}'
                AND (
                    (fo.net_quantity > {min_quantity})
                )
                AND (
                    {licenses_filter}
                )

            UNION ALL

            SELECT DISTINCT o.org_id, fo.product_sku AS sku, xref.is_legacy_license
            FROM edw.silver.fct_license_orders_daily_snapshot fo
            JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
                ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
            JOIN edw.silver.license_hw_sku_xref xref
                ON fo.product_sku = xref.license_sku
            WHERE
                o.date = '{partition_start}'
                AND DATE(fo._run_dt) BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}'
                AND (
                    (fo.net_quantity > {min_quantity})
                )
                AND (
                    {licenses_filter}
                )
        ),
        relevant_licenses_deduped AS (
            SELECT DISTINCT org_id
            FROM relevant_licenses
        ),
        """

def _generate_license_dependency_sql(partition_start: str, add_on_skus: list[str]) -> str:
    """
    Generate SQL for a license dependency condition. Legacy license don't need an add-on license, but core licenses do is the assumption we follow.
    Parameters:
        partition_start (str): The date partition being processed.
        add_on_skus (list[str]): List of add-on licenses that need to be associated with a core license
    Returns: SQL string
    """
    quoted_add_on_skus = [f"'{sku}'" for sku in add_on_skus]
    licenses_filter = f"sku IN ({', '.join(quoted_add_on_skus)})"

    return f"""
    relevant_licenses_legacy AS (
        SELECT DISTINCT org_id
        FROM relevant_licenses
        WHERE is_legacy_license
    ),
    relevant_licenses_core AS (
        SELECT DISTINCT org_id
        FROM relevant_licenses
        WHERE is_legacy_license = FALSE
        AND sku NOT IN ({', '.join(quoted_add_on_skus)})
    ),
    relevant_licenses_add_on AS (
        SELECT DISTINCT org_id
        FROM relevant_licenses
        WHERE {licenses_filter}
    ),
    relevant_licenses_final AS (
        SELECT DISTINCT org_id
        FROM relevant_licenses_legacy

        UNION

        SELECT DISTINCT c.org_id
        FROM relevant_licenses_core c
        JOIN relevant_licenses_add_on a
            ON c.org_id = a.org_id
    ),
    """


def _generate_complete_license_sql(partition_start: str, skus: list[str], add_on_skus: list[str] = None, min_quantity: int = 0, override_orgs: list[int] = None) -> str:
    """
    Generate complete license SQL that combines base license filtering with optional add-on dependencies.

    Parameters:
        partition_start (str): The date partition being processed
        skus (list[str]): List of base license SKUs to filter by
        add_on_skus (list[str]): Optional list of add-on license SKUs for dependency logic
        min_quantity (int): Minimum quantity required for the license to be considered valid
        override_orgs (list[int]): Optional list of org IDs that should bypass license count check
    Returns:
        str: Complete SQL query for license filtering
    """
    # Start with base license SQL
    base_license_sql = _generate_license_sql(partition_start, skus, min_quantity, override_orgs)

    # If no add-on SKUs, just return the base license SQL
    if not add_on_skus:
        extra_str = """
        relevant_licenses_final AS (
            SELECT DISTINCT org_id
            FROM relevant_licenses_deduped
        ),
        """
        return base_license_sql + extra_str

    # Add the dependency logic
    dependency_sql = _generate_license_dependency_sql(partition_start, add_on_skus)

    # Combine both SQL parts
    return base_license_sql + dependency_sql


def _get_relevant_enablement_record(enablement_array: list, partition_start: str) -> dict:
    """
    Find the most relevant enablement record based on the effective date.
    Returns the record whose effective_date is the maximum before or equal to the run_date.

    Parameters:
        enablement_array (list): List of enablement records from the JSON
        partition_start (str): The date we're running for (format: YYYY-MM-DD).

    Returns:
        dict: The most relevant enablement record
    """
    if not enablement_array:
        raise ValueError("No enablement records found")

    # Filter records where effective_date <= partition_start
    most_recent_record_before_run_date = None
    for record in enablement_array:
        effective_date = record.get('effective_date')
        if effective_date and effective_date <= partition_start:
            # only update if we haven't set a record to `most_recent_record_before_run_date` yet or if the current record is more recent than `most_recent_record_before_run_date`
            if most_recent_record_before_run_date is None or effective_date >= most_recent_record_before_run_date.get('effective_date'):
                most_recent_record_before_run_date = record

    if most_recent_record_before_run_date is None:
        raise ValueError(f"No valid enablement records found for run_date {partition_start}")

    return most_recent_record_before_run_date


def _get_relevant_user_eligibility_record(user_eligibility_array: list, partition_start: str) -> Optional[dict]:
    """
    Find the most relevant user eligibility record based on the effective date.
    Returns the record whose effective_date is the maximum before or equal to the run_date.

    Parameters:
        user_eligibility_array (list): List of user eligibility records from the JSON
        partition_start (str): The date we're running for (format: YYYY-MM-DD).

    Returns:
        dict: The most relevant user eligibility record, or None if no records found
    """
    if not user_eligibility_array:
        return None

    # Filter records where effective_date <= partition_start
    most_recent_record_before_run_date = None
    for record in user_eligibility_array:
        effective_date = record.get('effective_date')
        if effective_date and effective_date <= partition_start:
            # only update if we haven't set a record yet or if the current record is more recent
            if most_recent_record_before_run_date is None or effective_date >= most_recent_record_before_run_date.get('effective_date'):
                most_recent_record_before_run_date = record

    return most_recent_record_before_run_date


def _generate_enabled_users_sql(partition_start: str, user_eligibility_record: dict) -> str:
    """
    Generate SQL for enabled users based on user eligibility configuration.

    Parameters:
        partition_start (str): The date partition being processed (format: YYYY-MM-DD)
        user_eligibility_record (dict): The relevant user eligibility record from the JSON

    Returns:
        str: SQL CTE for enabled_users
    """
    # Check if serves_all_users is true
    serves_all_users = user_eligibility_record.get('serves_all_users', False)

    if serves_all_users:
        # Get all users from raw_clouddb_users_organizations joined with raw_clouddb_users
        # Look at 28-day range (partition_start - 27 days to partition_start)
        # Filter out internal/canary users
        return f"""
            enabled_users AS (
                SELECT DISTINCT
                    uo.organization_id AS org_id,
                    uo.user_id,
                    u.email,
                    uo.expire_at
                FROM datamodel_platform_bronze.raw_clouddb_users_organizations uo
                JOIN datamodel_platform_bronze.raw_clouddb_users u
                    ON uo.user_id = u.id
                    AND uo.date = u.date
                WHERE uo.date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}'
                AND (uo.expire_at IS NULL OR DATE(uo.expire_at) >= DATE_SUB('{partition_start}', 27))
                AND NOT (
                    u.email LIKE '%@samsara.com'
                    OR u.email LIKE '%samsara.canary%'
                    OR u.email LIKE '%samsara.forms.canary%'
                    OR u.email LIKE '%samsaracanarydevcontractor%'
                    OR u.email LIKE '%samsaratest%'
                    OR u.email LIKE '%@samsara%'
                    OR u.email LIKE '%@samsara-service-account.com'
                )

                UNION ALL

                SELECT DISTINCT
                    uo.organization_id AS org_id,
                    uo.user_id,
                    u.email,
                    uo.expire_at
                FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` uo
                JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users` u
                    ON uo.user_id = u.id
                    AND uo.date = u.date
                WHERE uo.date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}'
                AND (uo.expire_at IS NULL OR DATE(uo.expire_at) >= DATE_SUB('{partition_start}', 27))
                AND NOT (
                    u.email LIKE '%@samsara.com'
                    OR u.email LIKE '%samsara.canary%'
                    OR u.email LIKE '%samsara.forms.canary%'
                    OR u.email LIKE '%samsaracanarydevcontractor%'
                    OR u.email LIKE '%samsaratest%'
                    OR u.email LIKE '%@samsara%'
                    OR u.email LIKE '%@samsara-service-account.com'
                )
            ),
        """

    # Action-based eligibility
    actions = user_eligibility_record.get('actions', [])
    action_logic = user_eligibility_record.get('action_logic', 'AND')
    tag_roles_supported = user_eligibility_record.get('tag_roles_supported', True)

    # Build the action filter based on action_logic
    if action_logic == 'AND':
        # All actions must be present
        action_conditions = ' AND '.join([f"ARRAY_CONTAINS(p.actions, '{action}')" for action in actions])
    else:  # OR
        # At least one action must be present
        action_conditions = ' OR '.join([f"ARRAY_CONTAINS(p.actions, '{action}')" for action in actions])

    # Build tag filter
    tag_filter = ""
    if not tag_roles_supported:
        tag_filter = "AND p.tag_id IS NULL"

    # Use latest 28 days from dim_users_organizations_permissions (per its MAX(date))
    # while keeping raw users/orgs on the partition's 28-day window. Join on org_id/user_id only.
    return f"""
    enabled_users AS (
        SELECT DISTINCT
            uo.organization_id AS org_id,
            uo.user_id,
            u.email,
            uo.expire_at
        FROM datamodel_platform_bronze.raw_clouddb_users_organizations uo
        JOIN datamodel_platform_bronze.raw_clouddb_users u
            ON uo.user_id = u.id
            AND uo.date = u.date
        JOIN (
            SELECT * FROM datamodel_platform.dim_users_organizations_permissions
            WHERE date BETWEEN DATE_SUB((SELECT MAX(date) FROM datamodel_platform.dim_users_organizations_permissions), 27)
                AND (SELECT MAX(date) FROM datamodel_platform.dim_users_organizations_permissions)
        ) p
            ON uo.organization_id = p.org_id
            AND uo.user_id = p.user_id
        WHERE uo.date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}'
            AND (uo.expire_at IS NULL OR DATE(uo.expire_at) >= DATE_SUB('{partition_start}', 27))
            AND ({action_conditions})
            {tag_filter}
            AND NOT (
                u.email LIKE '%@samsara.com'
                OR u.email LIKE '%samsara.canary%'
                OR u.email LIKE '%samsara.forms.canary%'
                OR u.email LIKE '%samsaracanarydevcontractor%'
                OR u.email LIKE '%samsaratest%'
                OR u.email LIKE '%@samsara%'
                OR u.email LIKE '%@samsara-service-account.com'
            )

        UNION ALL

        SELECT DISTINCT
            uo.organization_id AS org_id,
            uo.user_id,
            u.email,
            uo.expire_at
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` uo
        JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users` u
            ON uo.user_id = u.id
            AND uo.date = u.date
        JOIN (
            SELECT * FROM data_tools_delta_share.datamodel_platform.dim_users_organizations_permissions
            WHERE date BETWEEN DATE_SUB((SELECT MAX(date) FROM data_tools_delta_share.datamodel_platform.dim_users_organizations_permissions), 27)
                AND (SELECT MAX(date) FROM data_tools_delta_share.datamodel_platform.dim_users_organizations_permissions)
        ) p
            ON uo.organization_id = p.org_id
            AND uo.user_id = p.user_id
        WHERE uo.date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}'
            AND (uo.expire_at IS NULL OR DATE(uo.expire_at) >= DATE_SUB('{partition_start}', 27))
            AND ({action_conditions})
            {tag_filter}
            AND NOT (
                u.email LIKE '%@samsara.com'
                OR u.email LIKE '%samsara.canary%'
                OR u.email LIKE '%samsara.forms.canary%'
                OR u.email LIKE '%samsaracanarydevcontractor%'
                OR u.email LIKE '%samsaratest%'
                OR u.email LIKE '%@samsara%'
                OR u.email LIKE '%@samsara-service-account.com'
            )
    ),
    """


def get_enabled_users_sql(json_content: dict, partition_start: str) -> Optional[str]:
    """
    Generate the SQL CTE for enabled users based on the JSON configuration.
    This function parses the user_eligibility_array and generates the appropriate SQL.

    Parameters:
        json_content (dict): Parsed JSON containing the product usage configuration
        partition_start (str): The date partition being processed (format: YYYY-MM-DD)

    Returns:
        str: SQL CTE for enabled_users, or None if no user eligibility is configured
    """
    user_eligibility_array = json_content.get('user_eligibility_array')

    if not user_eligibility_array:
        return None

    # Get the relevant user eligibility record based on effective_date
    user_eligibility_record = _get_relevant_user_eligibility_record(user_eligibility_array, partition_start)

    if user_eligibility_record is None:
        return None

    return _generate_enabled_users_sql(partition_start, user_eligibility_record)


def _get_enablement_sql(partition_start: str, serves_all: bool, release_phase: str, feature_flag_name: list[str], locale_filter: str, release_type_filter: str, opt_in_filter: str, license_config: dict = None, regions: list[str] = None) -> str:
    """
    Generate the enablement SQL based on the provided conditions.

    Parameters:
        partition_start (str): The date partition being processed (format: YYYY-MM-DD)
        serves_all (bool): Whether the product serves all orgs
        license_config (dict): License configuration details
        release_phase (str): The release phase of the product
        feature_flag_name (list[str]): List of name of the LaunchDarkly feature flags
        locale_filter (str): SQL condition for locale filtering
        release_type_filter (str): SQL condition for release type filtering
        opt_in_filter (str): SQL condition for opt-in orgs filtering
        regions (list[str]): List of regions to filter organizations by

    Returns:
        str: Complete SQL query for relevant organizations
    """

    enablement_str = ""

    ## if serves all is true, we get all orgs and everything else is ignored
    if serves_all:
        base_orgs = _generate_base_orgs_sql(partition_start=partition_start, regions=regions)
        enablement_str = f"""
        {GLOBAL_PUBSEC_QUERY}
        {GLOBAL_ARR_QUERY}
        {SERVES_ALL_LICENSE_CTE}
        relevant_orgs AS (
            {base_orgs}
        ),
        final_relevant_orgs AS (
            -- Final relevant orgs have an applicable license as well
            SELECT
                o.org_id,
                o.org_name,
                o.account_arr_segment,
                o.account_size_segment_name,
                o.account_billing_country,
                o.sam_number,
                o.account_id,
                o.account_name,
                o.org_category,
                o.region
            FROM relevant_orgs o
            JOIN license_filter lf
                ON o.org_id = lf.org_id
            WHERE 1=1
            {release_type_filter}
            {locale_filter}
        ),
        """.format(PARTITION_START=partition_start)
    else:
        ## get LD orgs to filter out any that were explicitly marked as False
        launchdarkly_orgs = _generate_launchdarkly_sql(feature_flag_name, partition_start) if feature_flag_name is not None else ""
        skus = license_config.get('skus') if license_config is not None else None
        license_required = license_config.get('license_required') if license_config is not None else None
        add_on_skus = license_config.get('add_on_skus') if license_config is not None else None
        license_min_quantity = license_config.get('min_quantity', 0) if license_config is not None else None
        license_override_orgs = license_config.get('override_orgs') if license_config is not None else None
        quoted_flags = [f"'{flag}'" for flag in feature_flag_name] if feature_flag_name is not None else []
        flag_filter = f" IN ({', '.join(quoted_flags)})" if quoted_flags else ""
        ## get all orgs with an applicable license
        licensed_orgs = _generate_complete_license_sql(partition_start, skus, add_on_skus, license_min_quantity, license_override_orgs) if license_config is not None else ""
        base_orgs = _generate_base_orgs_sql(partition_start=partition_start, regions=regions)
        ## Closed Beta means that we get LaunchDarkly orgs and include licenses if needed
        if release_phase == "Closed Beta":
            if license_required:
                ## Join license with LD data
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
            elif license_config is not None and license_required is False:
                # Union the two
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license that are not explicitly excluded from LD
                    -- 2. Orgs that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
            else:
                # Just LD orgs
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
        ## Open Beta means that we get LaunchDarkly orgs, opted-in orgs, and licenses if needed
        elif release_phase == "Open Beta":
            opt_in_orgs = _generate_opt_in_orgs_sql(feature_flag_name, opt_in_filter)
            if license_required:
                ## Both opt-in and LD orgs need licenses
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                {opt_in_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license from self-serve that are not explicitly excluded from LD
                    -- 2. Orgs with a license that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_us oo
                        ON o.org_id = oo.org_id
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_eu oo
                        ON o.org_id = oo.org_id
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
            elif license_config is not None and license_required == False:
                # Self-serve needs license, not LD
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                {opt_in_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license from self-serve that are not explicitly excluded from LD
                    -- 2. Orgs that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_us oo
                        ON o.org_id = oo.org_id
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_eu oo
                        ON o.org_id = oo.org_id
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
            else:
                # Just LD orgs and self-serve orgs
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                {opt_in_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs from self-serve that are not explicitly excluded from LD
                    -- 2. Orgs that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_us oo
                        ON o.org_id = oo.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN opt_in_orgs_eu oo
                        ON o.org_id = oo.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}
                    AND enabled_date <= '{partition_start}'

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
        ## GA means that we mainly consider licenses (and orgs explicitly excluded from LD if a feature flag is present)
        else:
            ## license_config will always have something or else it'd be serves_all
            ## license required means that we get all orgs (except those explicitly excluded from LD) with a license
            if license_required and feature_flag_name is not None:
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license that are not explicitly excluded from LD
                    -- 2. Orgs with a license that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                    ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
            elif license_required and feature_flag_name is None:
                enablement_str = f"""
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    WHERE 1=1
                    {release_type_filter}
                    {locale_filter}
                ),
                """
            else:
                ## if license is not required, we need to union launchdarkly orgs with those who have a license
                # this shouldn't be reached if there is no FF since then, it'd be serves_all
                enablement_str = f"""
                {launchdarkly_orgs}
                {GLOBAL_PUBSEC_QUERY}
                {GLOBAL_ARR_QUERY}
                relevant_orgs AS (
                    {base_orgs}
                ),
                {licensed_orgs}
                final_relevant_orgs AS (
                    -- Final set of relevant orgs for enablement
                    -- 1. Orgs with a license that are not explicitly excluded from LD
                    -- 2. Orgs that are explicitly granted access by LD
                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                    WHERE ldo.variation IS NULL
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN relevant_licenses_final rld
                        ON o.org_id = rld.org_id
                    LEFT OUTER JOIN fallthrough_variation fv
                        ON fv.feature_flag_key {flag_filter}
                    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                        ON {LAUNCHDARKLY_FALSE_JOIN_CONDITION}
                        AND ldo.feature_flag_key = fv.feature_flag_key
                    WHERE (
                        ldo.variation IS NULL
                        AND fv.fallthrough_variation = 1 -- true by default
                    )
                    {release_type_filter}
                    {locale_filter}

                    UNION

                    SELECT
                        o.org_id,
                        o.org_name,
                        o.account_arr_segment,
                        o.account_size_segment_name,
                        o.account_billing_country,
                        o.sam_number,
                        o.account_id,
                        o.account_name,
                        o.org_category,
                        o.region
                    FROM relevant_orgs o
                    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
                    ON {LAUNCHDARKLY_TRUE_JOIN_CONDITION}
                    WHERE 1=1
                ),
                """
    return enablement_str


def get_final_relevant_orgs(json_content: dict, partition_start: str) -> str:
    """
    Generate the final SQL for relevant enabled orgs based on the JSON configuration.
    This function does all the JSON parsing and ties together all the helper functions above

    Parameters:
        json_content (str): JSON string containing the product usage configuration
        partition_start (str): The date partition being processed (format: YYYY-MM-DD)
    Returns:
        str: Complete SQL query for relevant organizations
    """
    # Parse the JSON content
    enablement_array = json_content.get('enablement_array')

    if not enablement_array:
        raise ValueError("No enablement records found in JSON")

    # Get the relevant enablement record
    enablement_record = _get_relevant_enablement_record(enablement_array, partition_start)

    # Extract enablement conditions
    serves_all = enablement_record.get('serves_all')
    license_config = enablement_record.get('licenses')
    release_phase = enablement_record.get('release_phase')
    feature_flag_name = enablement_record.get('feature_flag_name')
    locales = enablement_record.get('locales')
    regions = enablement_record.get('regions')
    opt_in_scopes = enablement_record.get('opt_in_scopes')
    if locales is not None and 'na_all_locales' in locales:
        # Remove 'na_all_locales' and add the specific NA locales
        locales = [locale for locale in locales if locale != 'na_all_locales'] + ['us', 'ca', 'mx']
    if locales is not None and 'eu_all_locales' in locales:
        # Remove 'eu_all_locales' and add the specific EU locales
        locales = [locale for locale in locales if locale != 'eu_all_locales'] + [
            'at', 'be', 'ch', 'cz', 'de', 'dk', 'es', 'fr', 'gb', 'ie', 'im', 'it', 'lu', 'nl', 'pl', 'pt', 'ro', 'sk'
        ]
    release_types = enablement_record.get('release_types')
    quoted_release_types = [f"'{rt}'" for rt in release_types] if release_types else None
    quoted_locales = [f"'{locale}'" for locale in locales] if locales else None
    release_type_filter = f"AND o.release_type IN ({', '.join(quoted_release_types)})" if quoted_release_types else ""
    locale_filter = f"AND o.locale IN ({', '.join(quoted_locales)})" if quoted_locales else ""
    opt_in_filter = f"AND fpss.scope IN ({', '.join([str(scope) for scope in opt_in_scopes])})" if opt_in_scopes else ""

    enablement_str = _get_enablement_sql(
        partition_start=partition_start,
        serves_all=serves_all,
        license_config=license_config,
        release_phase=release_phase,
        feature_flag_name=feature_flag_name,
        locale_filter=locale_filter,
        release_type_filter=release_type_filter,
        opt_in_filter=opt_in_filter,
        regions=regions
    )

    return enablement_str


def _get_date_filter_clause(partition_start: str, active_period: dict, date_column: str) -> str:
    """
    Generate the appropriate date filter clause based on active period.
    Parameters:
        partition_start (str): The partition start date
        active_period (dict): Optional active period with start_date and end_date
        date_column (str): The date column to filter on
    Returns:
        str: SQL date filter clause
    """
    if active_period and not active_period.get('is_full_window', True):
        # Use specific active period dates
        start_date = active_period['start_date']
        end_date = active_period['end_date']
        return f"TO_DATE({date_column}) BETWEEN '{start_date}' AND '{end_date}'"
    else:
        # Use full 56-day window
        return f"TO_DATE({date_column}) BETWEEN DATE_SUB('{partition_start}', 55) AND '{partition_start}'"


def _get_relevant_usage_records_in_window(usage_array: list, partition_start: str, window_days: int = 56) -> list:
    """
    Find all relevant usage records that are active within a specified day window before the run date.
    Returns records with metadata about their active period within the window.
    Parameters:
        usage_array (list): List of usage records from the JSON
        partition_start (str): The date we're running for (format: YYYY-MM-DD)
        window_days (int): Number of days to look back from partition_start (default: 56)
    Returns:
        list: All usage records that are active within the window period, with active_period metadata
    """
    if not usage_array:
        raise ValueError("No usage records found")

    # Calculate the start of the window period
    partition_date = datetime.strptime(partition_start, "%Y-%m-%d")
    window_start_date = partition_date - timedelta(days=window_days - 1)  # -1 because we include the partition_start date
    window_start_str = window_start_date.strftime("%Y-%m-%d")

    # Sort all records by effective_date
    sorted_records = sorted(usage_array, key=lambda x: x.get('effective_date', ''))

    # Find records that are active during the window period
    active_records = []

    for i, record in enumerate(sorted_records):
        effective_date = record.get('effective_date')
        if not effective_date:
            continue

        # Check if this record is effective before or during the window
        if effective_date <= partition_start:
            # Find when this record was superseded (if at all)
            superseded_date = None
            for j in range(i + 1, len(sorted_records)):
                later_record = sorted_records[j]
                later_effective_date = later_record.get('effective_date')

                # If there's a later record that became effective before the partition start,
                # this record was superseded on that date
                if later_effective_date and later_effective_date <= partition_start:
                    superseded_date = later_effective_date
                    break

            # Calculate the active period within the window
            active_start = max(effective_date, window_start_str)
            active_end = superseded_date if superseded_date else partition_start

            # Check if this record was active during any part of the window
            if active_start <= active_end and active_start <= partition_start:
                # Create a copy of the record with active period metadata
                record_with_period = record.copy()
                record_with_period['active_period'] = {
                    'start_date': active_start,
                    'end_date': active_end,
                    'is_full_window': (active_start == window_start_str and active_end == partition_start) # active the entire time
                }
                active_records.append(record_with_period)

    if not active_records:
        raise ValueError(f"No valid usage records found for run_date {partition_start} within {window_days} day window")

    return active_records


def _generate_cloud_route_sql(route_names: list[str], partition_start: str, active_period: dict = None) -> str:
    """
    Generates SQL for querying Cloud routes data
    Parameters:
        route_names (list[str]): List of Cloud route names
        partition_start (str): The partition start date
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    date_filter = _get_date_filter_clause(partition_start, active_period, "cr.mp_date")

    # for cloud route events, we will count all actions
    return f"""
    cloud_route_events AS (
        SELECT
            CAST(cr.orgid AS BIGINT) AS org_id,
            TO_DATE(cr.mp_date) AS date,
            COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
        FROM mixpanel_samsara.cloud_route AS cr
        LEFT OUTER JOIN enabled_users e_user
            ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
            AND CAST(cr.orgid AS BIGINT) = e_user.org_id
        WHERE
            cr.routename IN (
                {', ' .join(f"'{x}'" for x in route_names)}
            ) -- Use the list of routenames
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
            AND cr.orgid IS NOT NULL
            AND {date_filter}
    ),
    """


def _generate_mixpanel_sql(mixpanel_events: list[str], partition_start: str, route_names: Optional[list[str]] = None, active_period: dict = None) -> str:
    """
    Generates SQL for querying various Mixpanel events
    Parameters:
        mixpanel_events (list[str]): List of Mixpanel events
        partition_start (str): The partition start date
        route_names (Optional[list[str]]): Optional list of route names to filter by
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    # Build route filter clause if route_names is provided
    quoted_route_names = [f"'{route_name}'" for route_name in route_names] if route_names else None
    route_name_filter = f"AND cr.routename IN ({', '.join(quoted_route_names)})" if quoted_route_names else ""

    date_filter = _get_date_filter_clause(partition_start, active_period, "cr.mp_date")

    # Generate individual SELECT statements for each event
    select_statements = []
    for event in mixpanel_events:
        select_stmt = f"""
        SELECT
            CAST(cr.orgid AS BIGINT) AS org_id,
            TO_DATE(cr.mp_date) AS date,
            COALESCE(e_user.user_id, cr.mp_user_id, cr.mp_device_id) AS user_id
        FROM mixpanel_samsara.{event} AS cr
        LEFT OUTER JOIN enabled_users e_user
            ON COALESCE(cr.mp_user_id, cr.mp_device_id) = e_user.email
            AND CAST(cr.orgid AS BIGINT) = e_user.org_id
        WHERE
            COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.com'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.canary%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsara.forms.canary%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%samsaratest%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara%'
            AND COALESCE(cr.mp_user_id, cr.mp_device_id) NOT LIKE '%@samsara-service-account.com'
            AND cr.orgid IS NOT NULL
            {route_name_filter}
            AND {date_filter}
        """
        select_statements.append(select_stmt)

    # Join all SELECT statements with UNION ALL
    joined_statements = " \nUNION ALL\n ".join(select_statements)

    # for Mixpanel events, we will count all actions
    # we do the aggregation later on in case we're combining it with cloud routes
    final_sql_str = f"""
    mixpanel_events AS (
        {joined_statements}
    ),
    """

    return final_sql_str


def _generate_alerts_sql(partition_start: str, alerts: dict, active_period: dict = None) -> str:
    """
    Generates SQL for querying alerts data

    Parameters:
        partition_start (str): The partition start date
        alerts (dict): Dictionary of alert details to filter on
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    start_date = active_period['start_date']

    trigger_types = alerts.get('trigger_types')
    action_types = alerts.get('action_types')

    # Build trigger types filter
    trigger_filter = ""
    if trigger_types:
        trigger_types_str = ', '.join(str(t) for t in trigger_types)
        trigger_filter = f"AND (\n            primary_trigger_type_id IN ({trigger_types_str})\n            OR secondary_trigger_type_id IN ({trigger_types_str})\n        )"

    # Build action types filter
    action_filter = ""
    if action_types:
        action_conditions = []
        for action_type in action_types:
            action_conditions.append(f"ARRAY_CONTAINS(alert_action_types_ids, {action_type})")
        action_filter = f"AND (\n            {' OR '.join(action_conditions)}\n        )"

    # for alerts events, we see if an alert has been created either before or during that window
    # and then we count the number of alerts created
    select_stmt = f"""
    alerts_events AS (
        SELECT DISTINCT
            org_id,
            DATE(created_ts_utc) AS date,
            alert_config_uuid
        FROM datamodel_platform.dim_alert_configs
        WHERE
            date = '{partition_start}'
            AND (DATE(deleted_ts_utc) IS NULL OR DATE(deleted_ts_utc) > '{start_date}') -- not deleted
            AND is_disabled = FALSE -- still active
            AND is_admin = FALSE -- visible
            {trigger_filter}
            {action_filter}

        UNION ALL

        SELECT DISTINCT
            org_id,
            DATE(created_ts_utc) AS date,
            alert_config_uuid
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_alert_configs`
        WHERE
            date = '{partition_start}'
            AND (DATE(deleted_ts_utc) IS NULL OR DATE(deleted_ts_utc) > '{start_date}') -- not deleted
            AND is_disabled = FALSE -- still active
            AND is_admin = FALSE -- visible
            {trigger_filter}
            {action_filter}
    ),
    alerts_events_aggregated AS (
        SELECT
            org_id,
            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN alert_config_uuid END) AS usage_weekly,
            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN alert_config_uuid END) AS usage_monthly,
            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN alert_config_uuid END) AS usage_prior_month,
            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN alert_config_uuid END) AS usage_weekly_prior_month,
            CAST(0 AS BIGINT) AS daily_active_user,
            CAST(0 AS BIGINT) AS weekly_active_users,
            CAST(0 AS BIGINT) AS monthly_active_users,
            MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day,
        FROM alerts_events
        GROUP BY org_id
    ),
    """

    return select_stmt


def _generate_mobile_logs_sql(partition_start: str, event_types: list[str], active_period: dict = None) -> str:
    """
    Generates SQL for querying mobile logs data

    Parameters:
        partition_start (str): The partition start date
        event_types (list[str]): List of event types to filter on
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    date_filter = _get_date_filter_clause(partition_start, active_period, "date")

    # Build event types filter
    event_filter = ""
    if event_types:
        event_types_str = ', '.join([f"'{event_type}'" for event_type in event_types])
        event_filter = f"AND event_type IN ({event_types_str})"

    # for mobile log events, we will just count all actions
    select_stmt = f"""
    mobile_log_events AS (
        SELECT DISTINCT
        ml.org_id,
        ml.date AS date,
        CASE WHEN ml.user_id != -1 THEN ml.user_id ELSE ml.driver_id END AS user_id
        FROM datastreams_history.mobile_logs ml
        LEFT OUTER JOIN users_us u
            ON ml.user_id = u.user_id AND ml.user_id != -1
        WHERE
            {date_filter}
            {event_filter}
            AND (ml.user_id = -1 OR u.user_id IS NULL) -- non-internal users

        UNION ALL

        SELECT DISTINCT
            ml.org_id,
            ml.date AS date,
            CASE WHEN ml.user_id != -1 THEN ml.user_id ELSE ml.driver_id END AS user_id
        FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_logs` ml
        LEFT OUTER JOIN users_eu u
            ON ml.user_id = u.user_id AND ml.user_id != -1
        WHERE
            {date_filter}
            {event_filter}
            AND (ml.user_id = -1 OR u.user_id IS NULL) -- non-internal users
    ),
    mobile_log_events_aggregated AS (
        SELECT
            org_id,
            COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
            COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
            COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
            COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
            COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
        FROM mobile_log_events
        GROUP BY org_id
    ),
    """

    return select_stmt


def _generate_api_usage_sql(partition_start: str, api_endpoints: list[Dict[str, str]], active_period: dict = None) -> str:
    """
    Generates SQL for querying API usage data

    Parameters:
        partition_start (str): The partition start date
        api_endpoints (list[Dict[str, str]]): List of API endpoints to filter on, each dict should have 'method' and 'route'
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    date_filter = _get_date_filter_clause(partition_start, active_period, "date")

    # Build API endpoints filter
    api_filter = ""
    if api_endpoints:
        endpoint_conditions = []
        for endpoint in api_endpoints:
            method = endpoint.get('method', '')
            route = endpoint.get('route', '')
            if method and route:
                endpoint_conditions.append(f"(method = '{method}' AND route = '{route}')")

        if endpoint_conditions:
            api_filter = f"AND (\n            {' OR '.join(endpoint_conditions)}\n        )"

    # for API events, we will get the total number of requests
    # and then sum them
    select_stmt = f"""
    api_events AS (
        SELECT DISTINCT
            org_id,
            date,
            SUM(num_requests) AS total_requests
        FROM product_analytics.fct_api_usage
        WHERE {date_filter}
            AND status_code = 200
            {api_filter}
        GROUP BY
            org_id,
            date

        UNION ALL

        SELECT DISTINCT
            org_id,
            date,
            SUM(num_requests) AS total_requests
        FROM data_tools_delta_share.product_analytics.fct_api_usage
        WHERE {date_filter}
            AND status_code = 200
            {api_filter}
        GROUP BY
            org_id,
            date
    ),
    api_usage_aggregated AS (
        SELECT
            org_id,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN total_requests END) AS usage_weekly,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN total_requests END) AS usage_monthly,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN total_requests END) AS usage_prior_month,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN total_requests END) AS usage_weekly_prior_month,
            CAST(0 AS BIGINT) AS daily_active_user,
            CAST(0 AS BIGINT) AS weekly_active_users,
            CAST(0 AS BIGINT) AS monthly_active_users,
            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
        FROM api_events
        GROUP BY org_id
    ),
    """

    return select_stmt


def _generate_custom_query_sql(custom_query: dict, partition_start: str, active_period: dict = None) -> str:
    """
    Generates SQL for querying custom query data

    Parameters:
        custom_query (dict): Custom query dictionary, containing US/EU SQL files
        partition_start (str): The partition start date
        active_period (dict): Optional active period with start_date and end_date
    Returns: SQL string
    """

    date_filter = _get_date_filter_clause(partition_start, active_period, "date")

    us_sql_file = custom_query.get('sql_file')
    eu_sql_file = custom_query.get('eu_sql_file')
    filter_internal_usage = custom_query.get('filter_internal_usage')
    has_user_id_field = custom_query.get('has_user_id_field')
    usage_field = custom_query.get('usage_field')
    binary_query = custom_query.get('binary_query')
    date_filter_json = custom_query.get('date_filter', True)
    operation = custom_query.get('operation', 'COUNT')

    us_sql_str = ""
    eu_sql_str = ""

    # Read and process US SQL file
    if us_sql_file:
        us_sql_file_path = Path(__file__).parent / "product_usage" / us_sql_file
        if us_sql_file_path.exists():
            with open(us_sql_file_path, 'r') as f:
                us_sql = f.read().format(PARTITION_START=partition_start)
                if filter_internal_usage:
                    if date_filter_json:
                        us_sql_str = f"""
                        events_us AS (
                            {us_sql}
                        ),
                        events_us_filtered AS (
                            SELECT e.*
                            FROM events_us e
                            LEFT OUTER JOIN users_us u
                                ON e.user_id = u.user_id
                            WHERE
                                {date_filter}
                                AND u.user_id IS NULL -- filter out internal usage
                        ),
                        """
                    else:
                        us_sql_str = f"""
                        events_us AS (
                            {us_sql}
                        ),
                        events_us_filtered AS (
                            SELECT e.*
                            FROM events_us e
                            LEFT OUTER JOIN users_us u
                                ON e.user_id = u.user_id
                            WHERE
                                u.user_id IS NULL -- filter out internal usage
                        ),
                        """
                else:
                    if date_filter_json:
                        us_sql_str = f"""
                        events_us AS (
                            {us_sql}
                        ),
                        events_us_filtered AS (
                            SELECT e.*
                            FROM events_us e
                            WHERE {date_filter}
                        ),
                        """
                    else:
                        us_sql_str = f"""
                        events_us AS (
                            {us_sql}
                        ),
                        events_us_filtered AS (
                            SELECT e.*
                            FROM events_us e
                        ),
                        """
        else:
            raise FileNotFoundError(f"US SQL file not found: {us_sql_file_path}")

    # Read and process EU SQL file
    if eu_sql_file:
        eu_sql_file_path = Path(__file__).parent / "product_usage" / eu_sql_file
        if eu_sql_file_path.exists():
            with open(eu_sql_file_path, 'r') as f:
                eu_sql = f.read().format(PARTITION_START=partition_start)
                if filter_internal_usage:
                    if date_filter_json:
                        eu_sql_str = f"""
                        events_eu AS (
                            {eu_sql}
                        ),
                        events_eu_filtered AS (
                            SELECT e.*
                            FROM events_eu e
                            LEFT OUTER JOIN users_eu u
                                ON e.user_id = u.user_id
                            WHERE
                                {date_filter}
                                AND u.user_id IS NULL -- filter out internal usage
                        ),
                        """
                    else:
                       eu_sql_str = f"""
                        events_eu AS (
                            {eu_sql}
                        ),
                        events_eu_filtered AS (
                            SELECT e.*
                            FROM events_eu e
                            LEFT OUTER JOIN users_eu u
                                ON e.user_id = u.user_id
                            WHERE
                                u.user_id IS NULL -- filter out internal usage
                        ),
                        """
                else:
                    if date_filter_json:
                        eu_sql_str = f"""
                        events_eu AS (
                            {eu_sql}
                        ),
                        events_eu_filtered AS (
                            SELECT e.*
                            FROM events_eu e
                            WHERE {date_filter}
                        ),
                        """
                    else:
                        eu_sql_str = f"""
                        events_eu AS (
                            {eu_sql}
                        ),
                        events_eu_filtered AS (
                            SELECT e.*
                            FROM events_eu e
                        ),
                        """
        else:
            raise FileNotFoundError(f"EU SQL file not found: {eu_sql_file_path}")
    if usage_field:
        # count distinct uses of the usage field
        if has_user_id_field:
            if binary_query:
                if operation == 'COUNT':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
                elif operation == 'SUM':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            SUM(CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            SUM(CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            SUM(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            SUM(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
            else:
                if operation == 'COUNT':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT * FROM events_us_filtered

                        UNION ALL

                        SELECT * FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
                elif operation == 'SUM':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT * FROM events_us_filtered

                        UNION ALL

                        SELECT * FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
        else:
            if binary_query:
                if operation == 'COUNT':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            COUNT(DISTINCT CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            COUNT(DISTINCT CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
                elif operation == 'SUM':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            SUM(CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            SUM(CASE WHEN date <= '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            SUM(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            SUM(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
            else:
                if operation == 'COUNT':
                    # add user id field so that it can be used in later deduplication, even if it's fictitious
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
                elif operation == 'SUM':
                    union_sql = f"""
                    custom_query_events AS (
                        SELECT *, -1 AS user_id FROM events_us_filtered

                        UNION ALL

                        SELECT *, -1 AS user_id FROM events_eu_filtered
                    ),
                    custom_query_events_aggregated AS (
                        SELECT
                            org_id,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN {usage_field} END) AS usage_weekly,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN {usage_field} END) AS usage_monthly,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_prior_month,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN {usage_field} END) AS usage_weekly_prior_month,
                            CAST(0 AS BIGINT) AS daily_active_user,
                            CAST(0 AS BIGINT) AS weekly_active_users,
                            CAST(0 AS BIGINT) AS monthly_active_users,
                            MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                        FROM custom_query_events
                        GROUP BY org_id
                    ),
                    """
    else:
        # track whether something was switched on before a certain time, instead of counting events normally
        if binary_query:
            # add user id field so that it can be used in later deduplication, even if it's fictitious
            union_sql = f"""
            custom_query_events AS (
                SELECT *, -1 AS user_id FROM events_us_filtered

                UNION ALL

                SELECT *, -1 AS user_id FROM events_eu_filtered
            ),
            custom_query_events_aggregated AS (
                SELECT
                    org_id,
                    COUNT(CASE WHEN date <= '{partition_start}' THEN 1 END) AS usage_weekly,
                    COUNT(CASE WHEN date <= '{partition_start}' THEN 1 END) AS usage_monthly,
                    COUNT(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                    COUNT(CASE WHEN date <= DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                    CAST(0 AS BIGINT) AS daily_active_user,
                    CAST(0 AS BIGINT) AS weekly_active_users,
                    CAST(0 AS BIGINT) AS monthly_active_users,
                    MAX(CASE WHEN date <= '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                FROM custom_query_events
                GROUP BY org_id
            ),
            """
        # now just count all uses, no need to count distinct
        elif has_user_id_field:
            union_sql = f"""
            custom_query_events AS (
                SELECT * FROM events_us_filtered

                UNION ALL

                SELECT * FROM events_eu_filtered
            ),
            custom_query_events_aggregated AS (
                SELECT
                    org_id,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                    COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                    MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                FROM custom_query_events
                GROUP BY org_id
            ),
            """
        else:
            # add user id field so that it can be used in later deduplication, even if it's fictitious
            union_sql = f"""
            custom_query_events AS (
                SELECT *, -1 AS user_id FROM events_us_filtered

                UNION ALL

                SELECT *, -1 AS user_id FROM events_eu_filtered
            ),
            custom_query_events_aggregated AS (
                SELECT
                    org_id,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                    CAST(0 AS BIGINT) AS daily_active_user,
                    CAST(0 AS BIGINT) AS weekly_active_users,
                    CAST(0 AS BIGINT) AS monthly_active_users,
                    MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                FROM custom_query_events
                GROUP BY org_id
            ),
            """

    final_sql = us_sql_str + eu_sql_str + union_sql
    return final_sql


def get_final_usage_sql(json_content: dict, partition_start: str) -> str:
    """
    Generate the final SQL for relevant enabled orgs based on the JSON configuration.
    This function does all the JSON parsing and ties together all the helper functions above
    Parameters:
        json_content (str): JSON string containing the product usage configuration
        partition_start (str): The date partition being processed (format: YYYY-MM-DD)
    Returns:
        str: Complete SQL query for relevant organizations
    """

    usage_array = json_content.get('usage_array')

    if not usage_array:
        raise ValueError("No usage records found in JSON")

    # Get the relevant usage records
    usage_records = _get_relevant_usage_records_in_window(usage_array, partition_start)

    sql_statements = []

    for usage_record in usage_records:
        route_names = usage_record.get('route_names')
        mixpanel_metric_names = usage_record.get('mixpanel_metric_names')
        alerts = usage_record.get('alerts')
        mobile_logs_event_types = usage_record.get('mobile_logs_event_types')
        api_endpoints = usage_record.get('api_endpoints')
        custom_query = usage_record.get('custom_query')
        active_period = usage_record.get('active_period')

        # Generate SQL for this usage record
        if custom_query:
            # Use custom query if provided
            custom_sql = _generate_custom_query_sql(custom_query, partition_start, active_period)
            sql_statements.append(custom_sql)
        elif route_names:
            # Only route names, no Mixpanel
            route_sql = _generate_cloud_route_sql(route_names, partition_start, active_period)
            if mixpanel_metric_names:
                # Include Mixpanel events if specified, filtered to a route
                sql_statements.append(route_sql)
                mixpanel_sql = _generate_mixpanel_sql(mixpanel_metric_names, partition_start, route_names, active_period)
                ## need to combine these to get actual totals, so handling aggregation here
                full_agg = f"""
                total_events AS (
                    SELECT * FROM cloud_route_events

                    UNION ALL

                    SELECT * FROM mixpanel_events
                ),
                total_events_aggregated AS (
                    SELECT
                        org_id,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                        COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                        COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                        COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                        MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                    FROM total_events
                    GROUP BY org_id
                ),
                """
                full_str = mixpanel_sql + full_agg
                sql_statements.append(full_str)
            else:
                # apply cloud route aggregation here since we don't bring in mixpanel in this case
                route_agg = f"""
                cloud_routes_aggregated AS (
                    SELECT
                        org_id,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                        COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                        COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                        COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                        COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                        MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                    FROM cloud_route_events
                    GROUP BY org_id
                ),
                """
                route_str = route_sql + route_agg
                sql_statements.append(route_str)
        elif mixpanel_metric_names:
            # Mixpanel events, no route filtering
            mixpanel_sql = _generate_mixpanel_sql(mixpanel_metric_names, partition_start, route_names=None, active_period=active_period)
            mixpanel_agg = f"""
            mixpanel_events_aggregated AS (
                SELECT
                    org_id,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN 1 END) AS usage_weekly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN 1 END) AS usage_monthly,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 55) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_prior_month,
                    COUNT(CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 34) AND DATE_SUB('{partition_start}', 28) THEN 1 END) AS usage_weekly_prior_month,
                    COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN user_id END) AS daily_active_user,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN user_id END) AS weekly_active_users,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN user_id END) AS monthly_active_users,
                    MAX(CASE WHEN date = '{partition_start}' THEN 1 ELSE 0 END) AS org_active_day
                FROM mixpanel_events
                GROUP BY org_id
            ),
            """
            mixpanel_str = mixpanel_sql + mixpanel_agg
            sql_statements.append(mixpanel_str)
        elif alerts:
            # Get alerts SQL
            alerts_sql = _generate_alerts_sql(partition_start, alerts, active_period)
            sql_statements.append(alerts_sql)
        elif mobile_logs_event_types:
            # Get mobile logs SQL
            mobile_logs_sql = _generate_mobile_logs_sql(partition_start, mobile_logs_event_types, active_period)
            sql_statements.append(mobile_logs_sql)
        elif api_endpoints:
            # Get API usage SQL
            api_usage_sql = _generate_api_usage_sql(partition_start, api_endpoints, active_period)
            sql_statements.append(api_usage_sql)

    # Combine all CTEs together
    if not sql_statements:
        raise ValueError("No valid SQL statements generated from usage records")

    # Build list of available aggregated CTEs based on what was generated
    # This will then do a union based on which usage metrics we needed
    available_ctes = []
    if any('cloud_routes_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('cloud_routes_aggregated')
    if any('mixpanel_events_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('mixpanel_events_aggregated')
    if any('alerts_events_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('alerts_events_aggregated')
    if any('mobile_log_events_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('mobile_log_events_aggregated')
    if any('api_usage_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('api_usage_aggregated')
    if any('total_events_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('total_events_aggregated')
    if any('custom_query_events_aggregated' in stmt for stmt in sql_statements):
        available_ctes.append('custom_query_events_aggregated')

    if available_ctes:
        # Build user events union for CTEs that have user_id
        user_event_ctes = []
        if any('cloud_route_events' in stmt for stmt in sql_statements):
            user_event_ctes.append('cloud_route_events')
        if any('mixpanel_events' in stmt for stmt in sql_statements):
            user_event_ctes.append('mixpanel_events')
        if any('mobile_log_events' in stmt for stmt in sql_statements):
            user_event_ctes.append('mobile_log_events')
        if any('custom_query_events' in stmt for stmt in sql_statements):
            # This will have user_id always (as we add -1 when it's not there)
            user_event_ctes.append('custom_query_events')
        if any('total_events' in stmt for stmt in sql_statements):
            user_event_ctes.append('total_events')
            # drop these as they are handled by total_events
            user_event_ctes.remove('cloud_route_events')
            user_event_ctes.remove('mixpanel_events')

        # Build union for usage metrics (all CTEs)
        union_parts = " UNION ALL ".join([f"SELECT * FROM {cte}" for cte in available_ctes])

        # Build union for user events (only CTEs with user_id)
        user_events_union = ""
        if user_event_ctes:
            user_events_union = " UNION ALL ".join([f"SELECT org_id, date, user_id FROM {cte}" for cte in user_event_ctes])

        if user_event_ctes:
            final_aggregation = f"""
            all_user_events AS (
                {user_events_union}
            ),
            user_metrics AS (
                SELECT
                    org_id,
                    COUNT(DISTINCT CASE WHEN date = '{partition_start}' THEN CAST(user_id AS STRING) END) AS daily_active_user,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 6) AND '{partition_start}' THEN CAST(user_id AS STRING) END) AS weekly_active_users,
                    COUNT(DISTINCT CASE WHEN date BETWEEN DATE_SUB('{partition_start}', 27) AND '{partition_start}' THEN CAST(user_id AS STRING) END) AS monthly_active_users
                FROM all_user_events
                WHERE CAST(user_id AS STRING) != '-1' -- adding to filter out custom query events that don't actually have a user id
                GROUP BY org_id
            ),
            final_usage_metrics AS (
                SELECT
                    m.org_id,
                    COALESCE(SUM(m.usage_weekly), 0) AS usage_weekly,
                    COALESCE(SUM(m.usage_monthly), 0) AS usage_monthly,
                    COALESCE(SUM(m.usage_prior_month), 0) AS usage_prior_month,
                    COALESCE(SUM(m.usage_weekly_prior_month), 0) AS usage_weekly_prior_month,
                    COALESCE(um.daily_active_user, 0) AS daily_active_user,
                    COALESCE(um.weekly_active_users, 0) AS weekly_active_users,
                    COALESCE(um.monthly_active_users, 0) AS monthly_active_users,
                    COALESCE(MAX(m.org_active_day), 0) AS org_active_day
                FROM (
                    {union_parts}
                ) m
                FULL OUTER JOIN user_metrics um ON m.org_id = um.org_id
                GROUP BY m.org_id, um.daily_active_user, um.weekly_active_users, um.monthly_active_users
            )
            """
        else:
            # No user events available, just aggregate usage metrics
            final_aggregation = f"""
            final_usage_metrics AS (
                SELECT
                    org_id,
                    SUM(usage_weekly) AS usage_weekly,
                    SUM(usage_monthly) AS usage_monthly,
                    SUM(usage_prior_month) AS usage_prior_month,
                    SUM(usage_weekly_prior_month) AS usage_weekly_prior_month,
                    CAST(0 AS BIGINT) AS daily_active_user,
                    CAST(0 AS BIGINT) AS weekly_active_users,
                    CAST(0 AS BIGINT) AS monthly_active_users,
                    MAX(org_active_day) AS org_active_day
                FROM (
                    {union_parts}
                ) all_metrics
                GROUP BY org_id
            )
            """
        sql_statements.append(final_aggregation)

    final_sql = "\n".join(sql_statements)

    return final_sql


def load_product_usage_config(feature_name: str) -> dict:
    """
    Load the product usage configuration from the given path.
    """
    product_usage_path = Path(__file__).parent / "product_usage" / f"{feature_name}.json"
    with open(product_usage_path, "r") as f:
        return json.load(f)


# Define what gets imported with "from product_usage_utils import *"
__all__ = [
    'get_final_relevant_orgs',
    'load_product_usage_config',
    'get_final_usage_sql',
]
