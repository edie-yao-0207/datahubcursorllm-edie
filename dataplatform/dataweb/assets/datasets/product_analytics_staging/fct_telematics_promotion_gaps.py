"""
fct_telematics_promotion_gaps

Daily analysis of telematics signal promotion gaps across vehicle characteristics.
Identifies missing year combinations in signal definitions by comparing existing promotions
against expected coverage ranges, enriched with device counts and audit information from
the telematics promotion gap review process.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataclasses import replace
from dataweb.userpkgs.firmware.table import (
    Definitions,
    ProductAnalyticsStaging,
)
from .dim_device_vehicle_properties import MMYEF_HASH_EXPRESSION, MMYEF_ID
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description

COLUMNS = [
    replace(MMYEF_ID, nullable=False, primary_key=True),
    ColumnType.MAKE,
    ColumnType.MODEL,
    Column(
        name="year",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Gap year where signal promotion is missing"),
    ),
    Column(
        name="engine_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Engine model for the gap year"),
    ),
    Column(
        name="powertrain",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Powertrain type ID from definitions.properties_fuel_powertrain"
        ),
    ),
    Column(
        name="fuel_group",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Fuel group type ID from definitions.properties_fuel_fuelgroup"
        ),
    ),
    Column(
        name="trim",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Vehicle trim level for the gap year"),
    ),
    Column(
        name="data_identifier",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Data identifier for the signal definition"),
    ),
    Column(
        name="obd_value",
        type=DataType.LONG,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="OBD value ID from definitions.obd_values"),
    ),
    ColumnType.BIT_START,
    ColumnType.BIT_LENGTH,
    Column(
        name="endian",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Endianness ID from definitions.signal_endian"),
    ),
    ColumnType.SCALE,
    Column(
        name="internal_scaling",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Internal scaling factor for the signal"),
    ),
    Column(
        name="sign",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Sign type ID from definitions.signal_sign"),
    ),
    ColumnType.OFFSET,
    ColumnType.MINIMUM,
    ColumnType.MAXIMUM,
    Column(
        name="request_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Request ID for the signal definition"),
    ),
    Column(
        name="response_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Response ID for the signal definition"),
    ),
    Column(
        name="data_source_id",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Data source ID from dim_combined_signal_definitions. Raw enum value for storage efficiency."
        ),
    ),
    ColumnType.IS_BROADCAST,
    ColumnType.IS_STANDARD,
    Column(
        name="audit_notes",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Audit notes from the telematics promotion gap review process"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH promoted AS (
  SELECT DISTINCT
      signal_catalog_id,
      make,
      model,
      year,
      engine_model,
      powertrain,
      fuel_group,
      trim,
      data_identifier,
      obd_value,
      bit_start,
      bit_length,
      endian,
      scale,
      internal_scaling,
      sign,
      offset,
      minimum,
      maximum,
      request_id,
      response_id,
      data_source_id,
      is_broadcast,
      is_standard,
      mmyef_id
  FROM product_analytics_staging.dim_combined_signal_definitions
  WHERE COALESCE(is_standard, false) = false
    AND make IS NOT NULL
    AND model IS NOT NULL
    AND year IS NOT NULL
    AND obd_value IS NOT NULL
),

bounds AS (
  SELECT 
      make,
      model,
      fuel_group,
      powertrain,
      obd_value,
      MIN(year) AS min_year,
      MAX(year) AS max_year
  FROM promoted
  GROUP BY ALL
),

expected AS (
  SELECT 
      b.make,
      b.model,
      b.fuel_group,
      b.powertrain,
      b.obd_value,
      EXPLODE(SEQUENCE(GREATEST(b.min_year - 2, 1980),
                       LEAST(b.max_year + 2, YEAR(current_date()) + 2))) AS gap_year
  FROM bounds b
),

existing AS (
  SELECT DISTINCT 
      make,
      model,
      fuel_group,
      powertrain,
      obd_value,
      year AS have_year
  FROM promoted
),

gap_years AS (
  SELECT 
      e.make,
      e.model,
      e.fuel_group,
      e.powertrain,
      e.obd_value,
      e.gap_year
  FROM expected e
  LEFT ANTI JOIN existing x
    ON e.make <=> x.make 
   AND e.model <=> x.model
   AND e.fuel_group <=> x.fuel_group 
   AND e.powertrain <=> x.powertrain
   AND e.obd_value <=> x.obd_value 
   AND e.gap_year <=> x.have_year
),

gap_templates AS (
  SELECT
      p.signal_catalog_id,
      g.make,
      g.model,
      g.fuel_group,
      g.powertrain,
      g.obd_value,
      g.gap_year,
      p.engine_model,
      p.trim,
      p.data_identifier,
      p.bit_start,
      p.bit_length,
      p.endian,
      p.scale,
      p.internal_scaling,
      p.sign,
      p.offset,
      p.minimum,
      p.maximum,
      p.request_id,
      p.response_id,
      p.data_source_id,
      p.is_broadcast,
      p.is_standard,
      ROW_NUMBER() OVER (
        PARTITION BY 
            g.make,
            g.model,
            g.fuel_group,
            g.powertrain,
            g.obd_value,
            g.gap_year
        ORDER BY 
            ABS(p.year - g.gap_year) ASC,
            p.year DESC
      ) AS rn
  FROM gap_years g
  JOIN promoted p
    ON g.make <=> p.make
   AND g.model <=> p.model
   AND g.fuel_group <=> p.fuel_group
   AND g.powertrain <=> p.powertrain
   AND g.obd_value <=> p.obd_value
),

best_templates AS (
  SELECT
      signal_catalog_id,
      make,
      model,
      fuel_group,
      powertrain,
      obd_value,
      gap_year AS year,
      engine_model,
      trim,
      data_identifier,
      bit_start,
      bit_length,
      endian,
      scale,
      internal_scaling,
      sign,
      offset,
      minimum,
      maximum,
      request_id,
      response_id,
      data_source_id,
      is_broadcast,
      is_standard
  FROM gap_templates
  WHERE rn = 1
),

hashed AS (
  SELECT
      signal_catalog_id,
      -- MMYEF ID for the gap year using same hash function as dim_device_vehicle_properties
      {mmyef_hash_expression} AS mmyef_id,
      make,
      model,
      year,
      engine_model,
      powertrain,
      fuel_group,
      trim,
      data_identifier,
      obd_value,
      bit_start,
      bit_length,
      endian,
      scale,
      internal_scaling,
      sign,
      offset,
      minimum,
      maximum,
      request_id,
      response_id,
      data_source_id,
      is_broadcast,
      is_standard
  FROM best_templates bt
),

final AS (
  SELECT
    hashed.*,
    review.is_potential_gap,
    review.notes AS audit_notes,
    pop.count

  FROM hashed

  -- Audit notes from telematics promotion gap review table
  LEFT JOIN definitions.telematics_promotion_gap_review AS review
    ON review.mmyef_id = hashed.mmyef_id
   AND review.obd_value = hashed.obd_value

  LEFT JOIN (
    SELECT mmyef_id, COUNT(*) AS count
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
    GROUP BY ALL
  ) AS pop ON hashed.mmyef_id = pop.mmyef_id
)

SELECT
  mmyef_id,
  make,
  model,
  year,
  engine_model,
  powertrain,
  fuel_group,
  trim,
  data_identifier,
  obd_value,
  bit_start,
  bit_length,
  endian,
  scale,
  internal_scaling,
  sign,
  offset,
  minimum,
  maximum,
  request_id,
  response_id,
  data_source_id,
  is_broadcast,
  is_standard,
  audit_notes
FROM final
WHERE count IS NOT NULL AND (is_potential_gap IS NULL OR is_potential_gap)
ORDER BY count DESC
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Comprehensive analysis of telematics signal promotion gaps across vehicle characteristics. "
        "Identifies missing year combinations in signal definitions by comparing existing promotions "
        "against expected coverage ranges, enriched with device population counts. Filters for gaps "
        "that have active devices in the fleet (based on dim_device_vehicle_properties) and excludes "
        "gaps that have been reviewed and marked as non-potential. Results ordered by device count "
        "descending to prioritize high-impact gaps. "
        "This partitionless reference table stores only raw enum values for optimal storage efficiency - "
        "consumers can join with definition tables when human-readable names are needed.",
        row_meaning="Each row represents one missing signal promotion for a specific vehicle "
        "make/model/year/engine/fuel/powertrain/trim and OBD value combination with active devices, "
        "with template configuration derived from closest existing definition and audit status from review process.",
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(Definitions.TELEMATICS_PROMOTION_GAP_REVIEW),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_PROMOTION_GAPS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_telematics_promotion_gaps(context: AssetExecutionContext) -> str:
    """
    Analyze telematics signal promotion gaps across vehicle characteristics with device population prioritization.

    This partitionless asset:
    1. Identifies existing signal promotions from dim_combined_signal_definitions
    2. Calculates expected year coverage ranges based on existing promotions (+/- 2 years)
    3. Finds gaps where signal promotions are missing for specific year combinations using null-safe joins
    4. Creates templates for missing years based on closest existing definitions
    5. Computes MMYEF hash for gap years using the standard hash expression
    6. Enriches with device population counts from dim_device_vehicle_properties
    7. Adds audit information from definitions.telematics_promotion_gap_review
    8. Filters for gaps with active devices and excludes reviewed non-potential gaps
    9. Orders by device count descending to prioritize high-impact gaps
    10. Stores only raw enum values following DataWeb enumeration storage best practices

    The result provides a prioritized reference table of actionable signal promotion gaps,
    focusing on vehicles with active devices in the fleet and excluding dismissed gaps.
    Supports data-driven decisions for expanding signal definitions based on real fleet composition.
    """
    # Format the query with MMYEF hash expression
    return QUERY.format(mmyef_hash_expression=MMYEF_HASH_EXPRESSION.strip())
