from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    array_of,
    struct_of,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Definitions,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
-- Extract common signal flags at the device level
WITH dims AS (
    SELECT
        dim.*,
        COALESCE(MAX(rollup.type = 'can_connected' AND rollup.count_month_days_covered > 0), FALSE) AS is_connected,
        COALESCE(MAX(rollup.type = 'engine_state' AND rollup.count_month_days_covered > 0), FALSE) AS has_engine_state,
        SIZE(coverage.not_covered_signal_types) > 0 AS has_p0_diagnostic_gap,
        COALESCE(vin.is_malformed_vin, FALSE) AS is_malformed_vin,
        COALESCE(vin.is_ecm_vin, FALSE) AS is_ecm_vin,
        COALESCE(vin.is_missing_vin, TRUE) AS is_missing_vin,
        COALESCE(vin.is_invalid_year, TRUE) AS is_invalid_year,
        COALESCE(vin.is_invalid_make, FALSE) AS is_invalid_make,
        COALESCE(vin.is_invalid_model, FALSE) AS is_invalid_model,
        COALESCE(vin.is_invalid_primary_fuel_type, FALSE) AS is_invalid_primary_fuel_type,
        COALESCE(vin.is_invalid_secondary_fuel_type, FALSE) AS is_invalid_secondary_fuel_type,
        vin.wmi,
        vin.vds
    FROM {product_analytics_staging}.dim_telematics_coverage_full AS dim
    LEFT JOIN {product_analytics_staging}.fct_telematics_coverage_rollup_full AS rollup USING (date, org_id, device_id)
    LEFT JOIN {product_analytics_staging}.fct_telematics_coverage_summary AS coverage USING (date, org_id, device_id)
    LEFT JOIN {product_analytics_staging}.fct_telematics_vin_issues AS vin USING (date, org_id, device_id)
    WHERE dim.date BETWEEN "{date_start}" AND "{date_end}"
    GROUP BY ALL
),

mm AS (
    SELECT
        date,
        market,
        make,
        model,
        COUNT(*) AS count_distinct_device_id
    FROM {product_analytics_staging}.dim_telematics_coverage_full AS dim
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
    GROUP BY ALL
),

wmi_vds_counts AS (
    SELECT date, market, wmi, vds, COUNT(*) AS count
    FROM {product_analytics_staging}.fct_telematics_vin_issues
    JOIN product_analytics_staging.fct_telematics_coverage_dimensions USING (date, org_id, device_id)
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
    GROUP BY ALL
),

installs AS (
    SELECT
        install.date,
        population.market,
        population.device_type,
        population.equipment_type,
        population.engine_type,
        population.fuel_type,
        population.make,
        population.model,
        population.year,
        population.engine_model,
        COLLECT_SET(
            NAMED_STRUCT(
                "product_name", population.product_name,
                "variant_name", population.variant_name,
                "cable_name", population.cable_name
            )
        ) AS acceptable
    FROM product_analytics_staging.agg_telematics_preferred_install AS install
    JOIN product_analytics_staging.agg_telematics_populations AS population USING (date, grouping_hash)
    WHERE install.date BETWEEN "{date_start}" AND "{date_end}"
      AND population.grouping_level = "market + device_type + product_name + variant_name + cable_name + engine_type + fuel_type + make + model + year + engine_model"
    GROUP BY ALL
),

-- Apply detection logic for all devices
device_issues AS (
    SELECT
        dim.date,
        dim.org_id,
        dim.device_id,

        ARRAY(
            NAMED_STRUCT(
                'issue', 'no_issue_detected',
                'active', NOT (
                    dim.has_p0_diagnostic_gap
                    -- If we don't know the engine_type then we don't know what the top priority diagnostics
                    -- are. These are defined in definitions.telematics_market_priority by market and engine_type.
                    OR dim.engine_type IS NULL
                    OR dim.engine_type = "UNKNOWN"
                )
            ),
            NAMED_STRUCT(
                'issue', 'power_only_cable',
                'active', NOT dim.diagnostics_capable
            ),
            NAMED_STRUCT(
                'issue', 'has_impacting_feature_flag',
                'active', impacted.device_id IS NOT NULL AND SIZE(impacted.ff_set) > 0
            ),
            NAMED_STRUCT(
                'issue', 'hw_upgrade_opportunity',
                'active', NOT ARRAY_CONTAINS(
                    install.acceptable,
                    NAMED_STRUCT(
                        'product_name', dim.product_name,
                        'variant_name', dim.variant_name,
                        'cable_name', dim.cable_name
                    )
                ) AND SIZE(install.acceptable) > 0
            ),
            NAMED_STRUCT(
                'issue', 'no_can_connection',
                'active', NOT dim.is_connected
            ),
            NAMED_STRUCT(
                'issue', 'no_engine_state',
                'active', NOT dim.has_engine_state
            ),
            NAMED_STRUCT(
                'issue', 'missing_vin_possible_license_plate',
                'active', dim.is_missing_vin AND dim.license_plate IS NOT NULL
            ),
            NAMED_STRUCT(
                'issue', 'missing_vin',
                'active', dim.is_missing_vin
            ),
            NAMED_STRUCT(
                'issue', 'malformed_ecm_vin',
                'active', dim.is_ecm_vin AND dim.is_malformed_vin
            ),
            NAMED_STRUCT(
                'issue', 'malformed_user_vin',
                'active', NOT dim.is_ecm_vin AND dim.is_malformed_vin
            ),
            NAMED_STRUCT(
                'issue', 'incomplete_decode_low_wmi_vds',
                'active', (
                    wmi_vds.count < 10
                    AND (
                        dim.is_invalid_make
                        OR dim.is_invalid_model
                        OR dim.is_invalid_year
                    )
                    AND (
                        dim.is_invalid_primary_fuel_type
                        AND dim.is_invalid_secondary_fuel_type
                    )
                )
            ),
            NAMED_STRUCT(
                'issue', 'invalid_year',
                'active', dim.is_invalid_year
            ),
            NAMED_STRUCT(
                'issue', 'p0_diagnostic_gap_minimal_mm',
                'active', dim.has_p0_diagnostic_gap AND mm.count_distinct_device_id < 30
            ),
            NAMED_STRUCT(
                'issue', 'p0_diagnostic_gap',
                'active', dim.has_p0_diagnostic_gap
            )
        ) AS device_classified_issues,

        TRANSFORM(
            impacted.issue_set,
            issue -> NAMED_STRUCT(
                'issue', issue,
                'active', TRUE
            )
        ) AS active_issues,

        ARRAY_UNION(
          COALESCE(device_classified_issues, ARRAY()),
          COALESCE(active_issues, ARRAY())
        ) AS all_issues

    FROM dims dim
    LEFT JOIN product_analytics_staging.fct_telematics_ff_impacted_devices AS impacted
        USING (date, org_id, device_id)
    LEFT JOIN installs AS install
        USING (date, market, device_type, engine_type, fuel_type, make, model, year, engine_model)
    LEFT JOIN mm
        USING (date, market, make, model)
    LEFT JOIN wmi_vds_counts AS wmi_vds
        USING (date, market, wmi, vds)
),

-- Explode and enrich with metadata
exploded AS (
    SELECT
        dim.date,
        dim.org_id,
        dim.device_id,
        issue.issue,
        issue.active
    FROM device_issues AS dim
    LATERAL VIEW EXPLODE(all_issues) AS issue
    WHERE issue.active
),

exploded_with_metadata AS (
    SELECT
        exploded.*,
        rank.rank,
        rank.details
    FROM exploded
    LEFT JOIN definitions.telematics_issue_rank AS rank USING (issue)
),

-- Rebuild and sort triggered issues
with_ranked_issues AS (
    SELECT
        date,
        org_id,
        device_id,
        ARRAY_SORT(
            COLLECT_LIST(
                NAMED_STRUCT(
                    'issue', issue,
                    'active', active,
                    'rank', rank,
                    'details', details
                )
            ),
            (a, b) -> IF(a.rank < b.rank, -1, IF(a.rank > b.rank, 1, 0))
        ) AS triggered_issues
    FROM exploded_with_metadata
    GROUP BY ALL
)

SELECT
  date,
  org_id,
  device_id,
  triggered_issues,
  ELEMENT_AT(triggered_issues, 1).issue AS top_issue
FROM with_ranked_issues
WHERE size(triggered_issues) > 0
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="triggered_issues",
        type=array_of(struct_of(
            (str(ColumnType.ISSUE), DataType.STRING),
            ("active", DataType.BOOLEAN),
            ("rank", DataType.LONG),
            ("details", DataType.STRING),
        )),
        nullable=True,
        metadata=Metadata(comment="The triggered issues."),
    ),
    Column(
        name="top_issue",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The top issue."),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics issue classifier.",
        row_meaning="Telematics issue classifier for a device over a given time period.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.TELEMATICS_ISSUE_RANK),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_FF_IMPACTED_DEVICES),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_PREFERRED_INSTALL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_SUMMARY),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_VIN_ISSUES),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_ISSUE_CLASSIFIER.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_issue_classifier(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)