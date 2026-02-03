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
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Definitions,
    DataModelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)

QUERY = """
WITH vin AS (
    SELECT
        date,
        org_id,
        device_id,
        substring(vin, 1, 3) AS wmi
    FROM datamodel_core_bronze.raw_vindb_shards_device_vin_metadata AS vininfo
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
  d.date,
  d.org_id,
  d.device_id,
  COLLECT_SET(IF(impacting.has_coverage_impact, ff.feature_flag, NULL)) as ff_set,
  COLLECT_SET(issue.label) as issue_set,
  COALESCE(MAX(ff.feature_flag = "enable-aggregated-engine-hours-read-path"), FALSE) AS has_aggregated_engine_hours

FROM {product_analytics_staging}.dim_telematics_coverage_full AS d

LEFT JOIN vin AS v USING (date, org_id, device_id)

LEFT JOIN definitions.telematics_ld_targets AS ff
  ON (d.org_id = ff.org_id OR ff.org_id IS NULL)
 AND (d.device_id = ff.device_id OR ff.device_id IS NULL)
 AND (d.make RLIKE ff.make OR ff.make IS NULL)
 AND (d.model RLIKE ff.model OR ff.model IS NULL)
 AND (d.year BETWEEN ff.year_start AND ff.year_end OR (ff.year_start IS NULL and ff.year_end IS NULL))
 AND (d.engine_model RLIKE ff.engine_model OR ff.engine_model IS NULL)
 AND (d.fuel_type RLIKE ff.fuel_type OR ff.fuel_type IS NULL)
 AND (d.market = ff.market OR ff.market IS NULL)

LEFT JOIN definitions.telematics_ld_metadata AS impacting USING (feature_flag)

LEFT JOIN definitions.telematics_issues AS issue
  ON (issue.make IS NULL OR d.make RLIKE issue.make)
 AND (issue.model IS NULL OR d.model RLIKE issue.model)
 AND ((issue.year_start IS NULL AND issue.year_end IS NULL) OR (d.year BETWEEN issue.year_start AND issue.year_end))
 AND (issue.engine_model IS NULL OR d.engine_model RLIKE issue.engine_model)
 AND (issue.engine_type IS NULL OR d.engine_type = issue.engine_type)
 AND (issue.fuel_type IS NULL OR d.fuel_type RLIKE issue.fuel_type)
 AND (issue.wmi IS NULL OR issue.wmi = v.wmi)
 AND (issue.market IS NULL OR issue.market = d.market)

WHERE d.date BETWEEN "{date_start}" AND "{date_end}"

GROUP BY ALL
HAVING SIZE(ff_set) > 0 OR has_aggregated_engine_hours OR SIZE(issue_set) > 0
"""

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="ff_set",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="The set of feature flags that are impacting the device."),
    ),
    Column(
        name="issue_set",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="The set of issue labels that are impacting the device."),
    ),
    Column(
        name="has_aggregated_engine_hours",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the device has aggregated engine hours."),
    )
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Statistics on feature flags impacting devices.",
        row_meaning="Feature flags impacting devices",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
        AnyUpstream(Definitions.TELEMATICS_LD_TARGETS),
        AnyUpstream(Definitions.TELEMATICS_LD_METADATA),
        AnyUpstream(Definitions.TELEMATICS_ISSUES),
        AnyUpstream(DataModelCoreBronze.RAW_VINDB_SHARDS_DEVICE_VIN_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_FF_IMPACTED_DEVICES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    max_retries=5,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_ff_impacted_devices(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
