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
    AttributedbShards,
    DatamodelCore,
    DatamodelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
-- Base attribute extraction for all relevant filtered vehicles
WITH all_attributes AS (
  SELECT
    a.org_id,
    b.entity_id AS device_id,
    LOWER(a.name) AS attr_name,
    LOWER(c.string_value) AS attr_value
  FROM
    attributedb_shards.attributes a
  LEFT JOIN
    attributedb_shards.attribute_cloud_entities b 
      ON a.uuid = b.attribute_id AND a.org_id = b.org_id
  LEFT JOIN
    attributedb_shards.attribute_values c 
      ON a.uuid = c.attribute_id AND b.attribute_value_id = c.uuid AND a.org_id = c.org_id
),

vin_mmy_info AS (
  SELECT
    date,
    org_id,
    id AS device_id,
    user_note,
    name
  FROM
    datamodel_core_bronze.raw_productdb_devices
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND user_note IS NOT NULL
),

info AS (
  SELECT
    org_id,
    device_id,
    MAX(IF(attr_name LIKE '%oem%' OR attr_name LIKE '%make%' OR attr_name LIKE '%manufacturer%', attr_value, NULL)) AS make,
    MAX(IF(attr_name LIKE '%model%' AND attr_name NOT LIKE '%year%', attr_value, NULL)) AS model,
    MAX(IF(attr_name LIKE '%year%', attr_value, NULL)) AS year,
    MAX(IF(attr_name LIKE '%equipment class%' OR attr_name LIKE '%equipment type%' OR attr_name LIKE "%vehicle type%", attr_value, NULL)) AS equipment_type,
    MAX(IF(attr_name LIKE '%serial number%' OR attr_name LIKE '%serial%', attr_value, NULL)) AS serial_pin
  FROM all_attributes
  GROUP BY ALL
),

devices AS (
  SELECT
    dev.date,
    dev.org_id,
    dev.device_id,
    dev.associated_vehicle_make,
    dev.associated_vehicle_model,
    CASE
        WHEN dev.associated_vehicle_year >= 1980 THEN dev.associated_vehicle_year
        ELSE NULL
    END AS associated_vehicle_year,
    dev.device_type,
    dim.market,
    dim.product_name,
    dim.variant_name,
    dim.cable_name,
    CASE
        WHEN dim.engine_type IN ("", "UNKNOWN") THEN NULL
        ELSE dim.engine_type
    END AS engine_type,
    CASE
        WHEN dim.fuel_type IN ("", "UNKNOWN") THEN NULL
        ELSE dim.fuel_type
    END AS fuel_type,
    CASE
        WHEN dim.engine_model IN ("", "UNKNOWN") THEN NULL
        ELSE dim.engine_model
    END AS engine_model,
    dim.internal_type,
    dim.diagnostics_capable,
    dim.asset_type_name
  FROM datamodel_core.dim_devices AS dev
  JOIN product_analytics.dim_device_dimensions AS dim USING (date, org_id, device_id)
  WHERE dim.date BETWEEN "{date_start}" AND "{date_end}"
    AND dim.org_id NOT IN (0, 1)
    AND dim.include_in_agg_metrics
    AND dim.internal_type = 0
    AND dim.quarantine_enabled = 0
),

license_plate_info AS (
  SELECT
    date,
    org_id,
    id AS device_id,
    proto.LicensePlate AS license_plate
  FROM datamodel_core_bronze.raw_productdb_devices
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND proto.LicensePlate IS NOT NULL
    AND proto.LicensePlate != ""
)

SELECT
  dim.date,
  dim.org_id,
  dim.device_id,
  vmi.user_note AS attr_user_note,
  info.make AS attr_make,
  info.model AS attr_model,
  info.year AS attr_year,
  dim.associated_vehicle_make AS vin_make,
  dim.associated_vehicle_model AS vin_model,
  dim.associated_vehicle_year AS vin_year,
  info.equipment_type AS attr_equipment_type,
  info.serial_pin,
  REGEXP_EXTRACT(vmi.user_note, 'VIN:\s+(.*?)\s+Description:', 1) AS attr_user_note_vin,
  COALESCE(NULLIF(REGEXP_EXTRACT(vmi.user_note, 'Description:\s+(\S+\s+\S+\s+(\S+))', 2), ''), attr_model) AS attr_user_note_model,

  -- Safe string version of the year
  REGEXP_EXTRACT(attr_year, '\d{{{{4}}}}', 0) AS prefiltered_year,

  -- Normalize model from raw or user note
  COALESCE(attr_model, attr_user_note_model) AS prefiltered_model,

  -- Normalize make
  UPPER(
    CASE 
      WHEN vin_make IS NOT NULL THEN vin_make
      WHEN LOWER(attr_make) LIKE 'john deere%' THEN 'JOHN DEERE'
      WHEN LOWER(attr_make) LIKE 'caterpillar%' THEN 'CATERPILLAR'
      WHEN LOWER(attr_make) LIKE 'komatsu%' THEN 'KOMATSU'
      WHEN LOWER(attr_make) LIKE 'case%' THEN 'CASE'
      WHEN LOWER(attr_make) LIKE 'doosan%' THEN 'DOOSAN'
      WHEN LOWER(attr_make) LIKE 'hitachi%' THEN 'HITACHI'
      WHEN LOWER(attr_make) LIKE 'kubota%' THEN 'KUBOTA'
      WHEN LOWER(attr_make) LIKE 'volvo%' THEN 'VOLVO'
      WHEN LOWER(attr_make) LIKE 'jcb%' THEN 'JCB'
      WHEN LOWER(attr_make) LIKE 'bobcat%' THEN 'BOBCAT'
      WHEN LOWER(attr_make) LIKE 'kobelco%' THEN 'KOBECO'
      WHEN LOWER(attr_make) LIKE 'daimler%' THEN 'DAIMLER'
      WHEN LOWER(attr_make) LIKE 'mercedes%' THEN 'MERCEDES'
      WHEN LOWER(attr_make) LIKE 'man%' THEN 'MAN'
      WHEN LOWER(attr_make) LIKE 'hyundai%' THEN 'HYUNDAI'
      WHEN LOWER(attr_make) LIKE 'isuzu%' THEN 'ISUZU'
      WHEN LOWER(attr_make) LIKE 'hino%' THEN 'HINO'
      WHEN LOWER(attr_make) LIKE 'ford%' THEN 'FORD'
      WHEN LOWER(attr_make) LIKE 'chevy%' THEN 'CHEVY'
      WHEN LOWER(attr_make) LIKE 'gmc%' THEN 'GMC'
      WHEN LOWER(attr_make) LIKE 'nissan%' THEN 'NISSAN'
      ELSE COALESCE(UPPER(SPLIT(attr_make, ' ')[0]), "UNKNOWN")
    END
  ) AS make,

  -- Normalize model by removing make prefix
  UPPER(
    CASE
      WHEN vin_model IS NOT NULL then vin_model
      WHEN LOWER(prefiltered_model) LIKE 'caterpillar%' THEN UPPER(TRIM(SUBSTRING(prefiltered_model, LENGTH('caterpillar') + 2)))
      WHEN LOWER(prefiltered_model) LIKE 'john deere%' THEN UPPER(TRIM(SUBSTRING(prefiltered_model, LENGTH('john deere') + 2)))
      WHEN LOWER(prefiltered_model) LIKE 'komatsu%' THEN UPPER(TRIM(SUBSTRING(prefiltered_model, LENGTH('komatsu') + 2)))
      ELSE COALESCE(UPPER(prefiltered_model), "UNKNOWN")
    END
  ) AS model,

  -- Final year: cast only if it's a clean 4-digit number
  CASE
    WHEN vin_year IS NOT NULL THEN vin_year
    WHEN REGEXP_EXTRACT(attr_year, '\d{{{{4}}}}', 0) RLIKE '^\d{{{{4}}}}$' THEN TRY_CAST(REGEXP_EXTRACT(attr_year, '\d{{{{4}}}}', 0) AS INT)
    ELSE -1
  END AS year,

  -- Normalized equipment type
  CASE 
    WHEN dim.asset_type_name != "Equipment" THEN "NONE"
    WHEN LOWER(attr_equipment_type) LIKE '%excavator%' THEN 'EXCAVATOR'
    WHEN LOWER(attr_equipment_type) LIKE '%dozer%' THEN 'DOZER'
    WHEN LOWER(attr_equipment_type) LIKE '%grader%' THEN 'GRADER'
    WHEN LOWER(attr_equipment_type) LIKE '%backhoe%' THEN 'BACKHOE'
    WHEN LOWER(attr_equipment_type) LIKE '%tractor%' THEN 'TRACTOR'
    WHEN LOWER(attr_equipment_type) LIKE '%loader%' THEN 'LOADER'
    WHEN LOWER(attr_equipment_type) LIKE '%truck%' THEN 'OFF HIGHWAY TRUCK'
    ELSE "OTHER"
  END AS equipment_type,

  CASE
    WHEN dim.device_type = 'AG - Asset Gateway' THEN 'AG'
    WHEN dim.device_type = 'SG - Site Gateway' THEN 'SG'
    WHEN dim.device_type = 'CM - AI Dash Cam' THEN 'CM'
    WHEN dim.device_type = 'Trailer' THEN 'TR'
    WHEN dim.device_type = 'AT - Asset Tracker' THEN 'AT'
    WHEN dim.device_type = 'VG - Vehicle Gateway' THEN 'VG'
    WHEN dim.device_type LIKE "%OEM%" THEN 'OEM'
    WHEN dim.device_type IS NOT NULL THEN dim.device_type
    ELSE "UNKNOWN"
  END AS device_type,

  dim.market,
  dim.product_name,
  dim.variant_name,
  dim.cable_name,

  COALESCE(dim.engine_type, "UNKNOWN") AS engine_type,
  COALESCE(dim.fuel_type, "UNKNOWN") AS fuel_type,
  COALESCE(dim.engine_model, "UNKNOWN") AS engine_model,

  dim.internal_type,
  dim.diagnostics_capable,
  lpi.license_plate

FROM devices AS dim
LEFT JOIN vin_mmy_info AS vmi USING(date, org_id, device_id)
LEFT JOIN info USING (org_id, device_id)
LEFT JOIN license_plate_info AS lpi USING (date, org_id, device_id)
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="attr_user_note",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The user note of the device.",
        ),
    ),
    Column(
        name="attr_make",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The make of the device.",
        ),
    ),
    Column(
        name="attr_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The model of the device.",
        ),
    ),
    Column(
        name="attr_year",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The year of the device.",
        ),
    ),
    Column(
        name="vin_make",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The make of the device.",
        ),
    ),
    Column(
        name="vin_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The model of the device.",
        ),
    ),
    Column(
        name="vin_year",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The year of the device.",
        ),
    ),
    Column(
        name="attr_equipment_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The equipment type of the device.",
        ),
    ),
    Column(
        name="serial_pin",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The serial pin of the device.",
        ),
    ),
    Column(
        name="attr_user_note_vin",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The user note of the device.",
        ),
    ),
    Column(
        name="attr_user_note_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The user note model of the device.",
        ),
    ),
    Column(
        name="prefiltered_year",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The prefiltered year of the device.",
        ),
    ),
    Column(
        name="prefiltered_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The prefiltered model of the device.",
        ),
    ),
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.EQUIPMENT_TYPE,
    ColumnType.MARKET,
    ColumnType.DEVICE_TYPE,
    ColumnType.PRODUCT_NAME,
    ColumnType.VARIANT_NAME,
    ColumnType.CABLE_NAME,
    ColumnType.ENGINE_TYPE,
    ColumnType.FUEL_TYPE,
    ColumnType.ENGINE_MODEL,
    ColumnType.INTERNAL_TYPE,
    ColumnType.DIAGNOSTICS_CAPABLE,
    Column(
        name="license_plate",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The license plate of the device.",
        ),
    ),
]
SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Device manual dimensions.",
        row_meaning="The manual dimensions of devices. Dimensions are generated by the customer or from the VIN decoder. Helps with filling gaps where the VIN is not supported on a device. VIN metadata is always preferred.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(AttributedbShards.ATTRIBUTES),
        AnyUpstream(AttributedbShards.ATTRIBUTE_CLOUD_ENTITIES),
        AnyUpstream(AttributedbShards.ATTRIBUTE_VALUES),
        AnyUpstream(DatamodelCore.DIM_DEVICES),
        AnyUpstream(DatamodelCoreBronze.RAW_PRODUCTDB_DEVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_DIMENSIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    max_retries=5,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_coverage_dimensions(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
    )