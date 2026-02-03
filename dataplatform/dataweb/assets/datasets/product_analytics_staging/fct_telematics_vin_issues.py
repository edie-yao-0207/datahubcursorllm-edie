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
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Definitions,
    DatamodelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH transliteration AS (
  SELECT *,
    SUBSTR(vin, 1, 3) AS wmi,
    SUBSTR(vin, 4, 6) AS vds,
    SUBSTR(vin, 9, 1) AS check_digit,
    SUBSTR(vin, 10, 1) AS model_year_code,
    SUBSTR(vin, 11, 1) AS plant_code,
    SUBSTR(vin, 12, 6) AS serial_number,
    SPLIT(vin, '') AS vin_chars,
    ARRAY(8,7,6,5,4,3,2,10,0,9,8,7,6,5,4,3,2) AS vin_weights
  FROM datamodel_core_bronze.raw_vindb_shards_device_vin_metadata
  WHERE date BETWEEN '{date_start}' AND '{date_end}'
),

transformed AS (
  -- ISO 3779
  SELECT *,
    TRANSFORM(vin_chars, c ->
      CASE
        WHEN c IN ('A','B','C','D','E','F','G','H') THEN ASCII(c) - ASCII('A') + 1
        WHEN c = 'J' THEN 1
        WHEN c = 'K' THEN 2
        WHEN c = 'L' THEN 3
        WHEN c = 'M' THEN 4
        WHEN c = 'N' THEN 5
        WHEN c = 'P' THEN 7
        WHEN c = 'R' THEN 9
        WHEN c = 'S' THEN 2
        WHEN c = 'T' THEN 3
        WHEN c = 'U' THEN 4
        WHEN c = 'V' THEN 5
        WHEN c = 'W' THEN 6
        WHEN c = 'X' THEN 7
        WHEN c = 'Y' THEN 8
        WHEN c = 'Z' THEN 9
        WHEN c BETWEEN '0' AND '9' THEN CAST(c AS INT)
        ELSE 0
      END
    ) AS vin_values
  FROM transliteration
),

zipped AS (
  SELECT *, ARRAYS_ZIP(vin_values, vin_weights) AS value_weight_pairs
  FROM transformed
),

check_sum_calc AS (
  SELECT *,
    REDUCE(
      value_weight_pairs,
      0,
      (acc, pair) -> acc + pair.vin_values * pair.vin_weights,
      acc -> acc
    ) AS vin_weighted_sum
  FROM zipped
)

SELECT
  date,
  org_id,
  device_id,
  vin,
  wmi,
  vds,
  check_digit,
  model_year_code,
  plant_code,
  serial_number,
  make,
  model,
  year,
  engine_model,
  primary_fuel_type,
  secondary_fuel_type,
  gross_vehicle_weight_rating,

  SPLIT(gross_vehicle_weight_rating, ':')[0] AS class_label,
  REGEXP_EXTRACT(gross_vehicle_weight_rating, r'Class (\w+)') AS class_code,
  TRIM(SPLIT(gross_vehicle_weight_rating, ':')[1]) AS class_range,

  CASE
    WHEN REGEXP_EXTRACT(gross_vehicle_weight_rating, r'Class (\d+)') IN ('1', '2') THEN 'LIGHT'
    WHEN REGEXP_EXTRACT(gross_vehicle_weight_rating, r'Class (\d+)') IN ('3', '4', '5', '6') THEN 'MEDIUM'
    WHEN REGEXP_EXTRACT(gross_vehicle_weight_rating, r'Class (\d+)') IN ('7', '8') THEN 'HEAVY'
    ELSE 'UNKNOWN'
  END AS weight_class,

  vin IS NULL OR vin = '' AS is_missing_vin,

  is_ecm_vin = 1 AS is_ecm_vin,

  LENGTH(vin) != 17 OR NOT vin RLIKE '^[A-HJ-NPR-Z0-9]+$' AS is_malformed_vin,

  year IS NULL OR year < 1981 OR year > (YEAR(NOW()) + 50) AS is_invalid_year,

  make IS NULL OR make IN ('UNKNOWN', 'OTHER', 'NA', 'N/A', 'INVALID', '') AS is_invalid_make,
  model IS NULL OR model IN ('UNKNOWN', 'OTHER', 'NA', 'N/A', 'INVALID', '') AS is_invalid_model,
  engine_model IS NULL OR engine_model IN ('UNKNOWN', 'OTHER', 'NA', 'N/A', 'INVALID', '') AS is_invalid_engine_model,
  primary_fuel_type IS NULL OR primary_fuel_type IN ('UNKNOWN', 'OTHER', 'NA', 'N/A', 'INVALID', '') AS is_invalid_primary_fuel_type,
  secondary_fuel_type IS NULL OR secondary_fuel_type IN ('UNKNOWN', 'OTHER', 'NA', 'N/A', 'INVALID', '') AS is_invalid_secondary_fuel_type,

  wmi IS NULL OR wmi_issues.wmi IS NOT NULL AS is_weak_wmi,
  vds IS NULL OR vds = '' AS is_weak_vds,

  CASE
    WHEN is_malformed_vin THEN FALSE
    WHEN check_digit =
         CASE WHEN MOD(vin_weighted_sum, 11) = 10 THEN 'X'
              ELSE CAST(MOD(vin_weighted_sum, 11) AS STRING)
         END THEN TRUE
    ELSE FALSE
  END AS is_check_digit_valid

FROM check_sum_calc
LEFT JOIN definitions.telematics_wmi_issues as wmi_issues USING (wmi)
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="vin",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The VIN of the device."),
    ),
    ColumnType.WMI,
    ColumnType.VDS,
    Column(
        name="check_digit",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The check digit of the device VIN."),
    ),
    ColumnType.MODEL_YEAR_CODE,
    Column(
        name="plant_code",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The plant code of the device VIN."),
    ),
    Column(
        name="serial_number",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The serial number of the device VIN."),
    ),
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.PRIMARY_FUEL_TYPE,
    ColumnType.SECONDARY_FUEL_TYPE,
    Column(
        name="gross_vehicle_weight_rating",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The gross vehicle weight rating of the device VIN."),
    ),
    Column(
        name="class_label",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The class label of the device VIN."),
    ),
    Column(
        name="class_code",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The class code of the device VIN."),
    ),
    Column(
        name="class_range",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The class range of the device VIN."),
    ),
    Column(
        name="weight_class",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The weight class of the device VIN."),
    ),
    Column(
        name="is_missing_vin",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the VIN is missing."),
    ),
    Column(
        name="is_ecm_vin",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the VIN is from an ECM source."),
    ),
    Column(
        name="is_malformed_vin",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the VIN is malformed."),
    ),
    Column(
        name="is_invalid_year",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the year is invalid."),
    ),
    Column(
        name="is_invalid_make",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the make is invalid."),
    ),
    Column(
        name="is_invalid_model",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the model is invalid."),
    ),
    Column(
        name="is_invalid_engine_model",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the engine model is invalid."),
    ),
    Column(
        name="is_invalid_primary_fuel_type",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the primary fuel type is invalid."),
    ),
    Column(
        name="is_invalid_secondary_fuel_type",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the secondary fuel type is invalid."),
    ),
    Column(
        name="is_weak_wmi",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the WMI is weak. A weak WMI is part of the telematics_wmi_issues table."),
    ),
    Column(
        name="is_weak_vds",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the VDS is weak. A weak VDS is either empty or null."),
    ),
    Column(
        name="is_check_digit_valid",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the check digit is valid."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics VIN issues.",
        row_meaning="Telematics VIN issues for a device over a given time period.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(DatamodelCoreBronze.RAW_VINDB_SHARDS_DEVICE_VIN_METADATA),
        AnyUpstream(Definitions.TELEMATICS_WMI_ISSUES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_VIN_ISSUES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_vin_issues(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)