from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)
from .raw_productdb_devices_table import raw_productdb_devices_schema

key_prefix = "dataengineering_dev"

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2023-08-20")

pipeline_group_name = "dim_devices"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

raw_productsdb_gateways_query = """
SELECT
    gateways.*,
    '{DATEID}' AS date

FROM productsdb.gateways{TIMETRAVEL_DATE} gateways
"""

raw_productdb_devices_query = """

SELECT
    devices.*,
    '{DATEID}' AS date

FROM productsdb.devices{TIMETRAVEL_DATE} devices
"""


def get_sharded_timetravel_query(sharded_query_template, shard_ids):
    return "            UNION ALL".join(
        [sharded_query_template.format(shard_id=shard_id) for shard_id in shard_ids]
    )


def get_raw_vindb_shards_device_vin_metadata_query(region):
    # Shard configuration per region (based on vindb.go):
    #   US: NumShards=6 -> shards 0-5
    #   EU: NumShards=2 -> shards 0-1
    #   CA: NumShards=1 -> shard 0 only
    if region == AWSRegions.US_WEST_2.value:
        shard_ids = list(range(6))  # [0, 1, 2, 3, 4, 5]
    elif region == AWSRegions.CA_CENTRAL_1.value:
        shard_ids = [0]  # Only shard 0 exists in CA
    else:
        shard_ids = list(range(2))  # [0, 1] for EU

    raw_vindb_shards_device_vin_metadata_query_template = """
    SELECT
        *,
        '{{DATEID}}' AS date
    FROM (
        SELECT /*+ BROADCAST(os) */ shards.*
            FROM (
                {sharded_timetravel_query}
            ) shards
                LEFT OUTER JOIN appconfigs.org_shards os
                    ON shards.org_id = os.org_id
                WHERE os.shard_type = 1 AND os.data_type = "vin"
            )
    """

    sharded_timetravel_query_template = """
                SELECT
                    "vin_shard_{shard_id}" AS shard_name,
                    _filename, _op, _rowid, _timestamp, adaptive_cruise_control, adaptive_driving_beam,
                    air_bag_location_curtain, air_bag_location_front, air_bag_location_seat_cushion,
                    air_bag_location_side, anti_lock_brake_system, auto_pedestrian_alerting_sound,
                    auto_reverse_system, axle_configuration, axles, battery_a, battery_cells,
                    battery_info, battery_kwh, battery_packs, battery_type, bed_type, blind_spots_monitor,
                    body_cab_type, body_class, brake_system_desc, brake_system_type, cable_id, charger_level,
                    charger_power_kw, crash_imminent_braking, device_id, doors, drive_assist, drive_type,
                    dynamic_brake_support, electrification_level, electronic_stability_control, engine_configuration,
                    engine_cylinders, engine_displacement_cc, engine_hp, engine_kw, engine_manufacturer, engine_model,
                    foward_collision_warning, gross_vehicle_weight_rating, is_ecm_vin, keyless_ignition, lane_departure_warning,
                    lane_keep_system, make, max_weight_lbs, model, org_id, park_assist, partition, ped_auto_emergency_braking,
                    plant_information, primary_fuel_type, product_id, rear_visibility_system, secondary_fuel_type,
                    semi_auto_headlamp_beam_switching, series, series_secondary, steering_location, tire_pressure_monitoring_sys,
                    trailer_body_type, trailer_type, transmission_speeds, transmission_styles, trim, trim_secondary, updated_at,
                    valve_train_design, vin, year
                FROM vin_shard_{shard_id}db.device_vin_metadata{{TIMETRAVEL_DATE}}
    """

    return raw_vindb_shards_device_vin_metadata_query_template.format(
        sharded_timetravel_query=get_sharded_timetravel_query(
            sharded_timetravel_query_template, shard_ids
        )
    )


def get_raw_fueldb_shards_fuel_types_query(region):
    # Shard configuration per region (based on fueldb.go):
    #   US: NumShards=6 -> shards 0-5
    #   EU: NumShards=2 -> shards 0-1
    #   CA: NumShards=1 -> shard 0 only
    if region == AWSRegions.US_WEST_2.value:
        shard_ids = list(range(6))  # [0, 1, 2, 3, 4, 5]
    elif region == AWSRegions.CA_CENTRAL_1.value:
        shard_ids = [0]  # Only shard 0 exists in CA
    else:
        shard_ids = list(range(2))  # [0, 1] for EU

    raw_fueldb_shards_fuel_types_query_template = """
    SELECT
        *,
        '{{DATEID}}' AS date
    FROM (
        SELECT /*+ BROADCAST(os) */ shards.*
            FROM (
                {sharded_timetravel_query}
            ) shards
                LEFT OUTER JOIN appconfigs.org_shards os
                    ON shards.org_id = os.org_id
                WHERE os.shard_type = 1 AND os.data_type = "fuel"
            )
    """

    sharded_timetravel_query_template = """
                SELECT
                    "fuel_shard_{shard_id}" AS shard_name,
                    _filename, _op, _rowid, _timestamp, battery_charge_type, configurable_fuel_id,
                    created_at, created_by, device_id, diesel_type, engine_type, gaseous_type,
                    gasoline_type, hydrogen_type, org_id, partition, source, uuid
                FROM fuel_shard_{shard_id}db.fuel_types{{TIMETRAVEL_DATE}}
    """

    return raw_fueldb_shards_fuel_types_query_template.format(
        sharded_timetravel_query=get_sharded_timetravel_query(
            sharded_timetravel_query_template, shard_ids
        )
    )


stg_vg_devices_step_1_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
    {
        "name": "variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "associated_vehicle_vin",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Vehicle identification number (VIN) of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "camera_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "CM associated with the VG"},
    },
    {
        "name": "camera_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "CM associated with the VG. Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "camera_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "CM associated with the VG"},
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "CM associated with the VG"},
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
        },
    },
    {
        "name": "is_unregulated_vehicle",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Does the vehicle exempts driver from following HOS rules"
        },
    },
    {
        "name": "vehicle_obd_type",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "OBD type of device for the vehicle"},
    },
    {
        "name": "config_override_json",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "json view of device level override configs"},
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Trim of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the device"},
    },
    {
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle. Will be translated downstream"
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the product. Sourced from definitions.products"
        },
    },
]


stg_vg_devices_step_1_query = """
-- For backfills involving older partitions, pick the latest partition available from raw source tables

WITH fuel_types AS (
SELECT
    device_id,
    org_id,
    MAX_BY(engine_type, created_at) AS engine_type
FROM datamodel_core_bronze.raw_fueldb_shards_fuel_types
WHERE date = (SELECT MIN(date)
              FROM datamodel_core_bronze.raw_fueldb_shards_fuel_types
              WHERE date >= '{DATEID}')
GROUP BY 1,2
),
vin_metadata AS (
SELECT
    device_id,
    org_id,
    vin,
    engine_model,
    primary_fuel_type,
    secondary_fuel_type,
    trim,
    engine_manufacturer,
    max_weight_lbs,
    gross_vehicle_weight_rating
FROM datamodel_core_bronze.raw_vindb_shards_device_vin_metadata
WHERE date = (SELECT MAX(date)
              FROM datamodel_core_bronze.raw_vindb_shards_device_vin_metadata
              WHERE date <= '{DATEID}')
)
SELECT
    raw_productdb_devices.org_id AS org_id,
    raw_productdb_devices.id AS device_id,
    raw_productdb_devices.serial,
    raw_productdb_devices.product_id,
    'VG - Vehicle Gateway' AS device_type,
    raw_productdb_devices.variant_id,

    -- Vehicle Data
    raw_productdb_devices.vin AS associated_vehicle_vin,
    raw_productdb_devices.make AS associated_vehicle_make,
    raw_productdb_devices.model AS associated_vehicle_model,
    CASE
        WHEN raw_productdb_devices.year = 0
        THEN NULL
        ELSE raw_productdb_devices.year
    END AS associated_vehicle_year,

    -- Assoctiated devices
    UPPER(REPLACE(REPLACE(raw_productdb_devices.camera_serial, '-', ''), ' ', '')) AS camera_serial,
    raw_productdb_devices.camera_product_id AS camera_product_id,
    raw_productdb_devices.camera_variant_id,
    raw_productdb_devices.updated_at,
    '{DATEID}' AS date,
    COALESCE(
        raw_productdb_devices.device_settings_proto.unregulated_vehicle,
        FALSE
    ) AS is_unregulated_vehicle,
    raw_productdb_devices.obd_type AS vehicle_obd_type,
    raw_productdb_devices.config_override_json,

    -- Additional vehicle attributes
    vin_metadata.engine_model AS associated_vehicle_engine_model,
    vin_metadata.primary_fuel_type AS associated_vehicle_primary_fuel_type,
    vin_metadata.secondary_fuel_type AS associated_vehicle_secondary_fuel_type,
    CASE fuel_types.engine_type
        WHEN 0 THEN 'ICE'
        WHEN 1 THEN 'BEV'
        WHEN 2 THEN 'PHEV'
        WHEN 3 THEN 'HYBRID'
        WHEN 4 THEN 'HYDROGEN'
    END AS associated_vehicle_engine_type,
    vin_metadata.trim AS associated_vehicle_trim,
    vin_metadata.engine_manufacturer AS associated_vehicle_engine_manufacturer,
    vin_metadata.max_weight_lbs AS associated_vehicle_max_weight_lbs,
    vin_metadata.gross_vehicle_weight_rating AS associated_vehicle_gross_vehicle_weight_rating,
    raw_productdb_devices.name AS device_name,
    raw_productdb_devices.asset_type,
    products.name AS product_name

  FROM datamodel_core_bronze.raw_productdb_devices
LEFT OUTER JOIN definitions.products
ON (raw_productdb_devices.product_id = products.product_id)
LEFT OUTER JOIN fuel_types
ON (raw_productdb_devices.id = fuel_types.device_id
    AND raw_productdb_devices.org_id = fuel_types.org_id)
LEFT OUTER JOIN vin_metadata
ON (raw_productdb_devices.id = vin_metadata.device_id
    AND raw_productdb_devices.org_id = vin_metadata.org_id
    AND LOWER(TRIM(raw_productdb_devices.vin)) = LOWER(TRIM(vin_metadata.vin)))
WHERE raw_productdb_devices.date = '{DATEID}'
AND (products.name LIKE 'VG%' OR products.name = 'Test-VG')
"""


stg_non_vg_devices_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """Attribute defining the device type (e.g. VG, CM, AG, ...).
            CMs will also include product name values from their parent devices where they are denoted as cameras instead of devices"""
        },
    },
    {
        "name": "variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "associated_vehicle_vin",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Vehicle identification number (VIN) of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "vg_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "VG associated with the device"},
    },
    {
        "name": "vg_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "VG associated with the device"},
    },
    {
        "name": "vg_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "VG associated with the device. Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "vg_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "VG associated with the device"},
    },
    {
        "name": "vg_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "VG associated with the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "VG associated with the device"},
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Trim of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the device"},
    },
    {
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle. Will be translated downstream"
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the product. Sourced from definitions.products"
        },
    },
]

stg_non_vg_devices_query = """

WITH vg_devices (
-- Addressing edge case where a camera_serial is attached to more than one device. Only a handful of cases
SELECT stg_vg_devices_step_1.camera_serial,
        MAX_BY(device_id, updated_at) AS device_id,
        MAX_BY(org_id, updated_at) AS org_id,
        MAX_BY(serial, updated_at) AS serial,
        MAX_BY(product_id, updated_at) AS product_id,
        MAX_BY(device_type, updated_at) AS device_type,
        MAX_BY(variant_id, updated_at) AS variant_id,
        MAX_BY(associated_vehicle_vin, updated_at) AS associated_vehicle_vin,
        MAX_BY(associated_vehicle_make, updated_at) AS associated_vehicle_make,
        MAX_BY(associated_vehicle_model, updated_at) AS associated_vehicle_model,
        MAX_BY(associated_vehicle_year, updated_at) AS associated_vehicle_year,
        MAX_BY(associated_vehicle_engine_model, updated_at) AS associated_vehicle_engine_model,
        MAX_BY(associated_vehicle_primary_fuel_type, updated_at) AS associated_vehicle_primary_fuel_type,
        MAX_BY(associated_vehicle_secondary_fuel_type, updated_at) AS associated_vehicle_secondary_fuel_type,
        MAX_BY(associated_vehicle_engine_type, updated_at) AS associated_vehicle_engine_type,
        MAX_BY(associated_vehicle_trim, updated_at) AS associated_vehicle_trim,
        MAX_BY(associated_vehicle_engine_manufacturer, updated_at) AS associated_vehicle_engine_manufacturer,
        MAX_BY(associated_vehicle_max_weight_lbs, updated_at) AS associated_vehicle_max_weight_lbs,
        MAX_BY(associated_vehicle_gross_vehicle_weight_rating, updated_at) AS associated_vehicle_gross_vehicle_weight_rating
    FROM {database_silver_dev}.stg_vg_devices_step_1
    WHERE date = '{DATEID}'
    GROUP BY 1
)
SELECT
  non_vg_devices.org_id,
  non_vg_devices.device_id,
  non_vg_devices.serial,
  non_vg_devices.product_id,
  non_vg_devices.device_type,
  non_vg_devices.variant_id,

  -- Vehicle Date
  COALESCE(vg_devices.associated_vehicle_vin, non_vg_devices.associated_vehicle_vin)
    AS associated_vehicle_vin,
  COALESCE(vg_devices.associated_vehicle_make, non_vg_devices.associated_vehicle_make)
    AS associated_vehicle_make,
  COALESCE(vg_devices.associated_vehicle_model, non_vg_devices.associated_vehicle_model)
    AS associated_vehicle_model,
  COALESCE(vg_devices.associated_vehicle_year, non_vg_devices.associated_vehicle_year)
    AS associated_vehicle_year,

  -- Assoctiated devices
  vg_devices.device_id AS vg_device_id,
  vg_devices.serial AS vg_serial,
  vg_devices.product_id AS vg_product_id,
  vg_devices.device_type AS vg_device_type,
  vg_devices.variant_id AS vg_variant_id,
  non_vg_devices.updated_at,
  '{DATEID}' AS date,

  -- Additional vehicle attributes
  COALESCE(old_cm_vg_devices.associated_vehicle_engine_model, vg_devices.associated_vehicle_engine_model) AS associated_vehicle_engine_model,
  COALESCE(old_cm_vg_devices.associated_vehicle_primary_fuel_type, vg_devices.associated_vehicle_primary_fuel_type) AS associated_vehicle_primary_fuel_type,
  COALESCE(old_cm_vg_devices.associated_vehicle_secondary_fuel_type, vg_devices.associated_vehicle_secondary_fuel_type) AS associated_vehicle_secondary_fuel_type,
  COALESCE(old_cm_vg_devices.associated_vehicle_engine_type, vg_devices.associated_vehicle_engine_type) AS associated_vehicle_engine_type,
  COALESCE(old_cm_vg_devices.associated_vehicle_trim, vg_devices.associated_vehicle_trim) AS associated_vehicle_trim,
  COALESCE(old_cm_vg_devices.associated_vehicle_engine_manufacturer, vg_devices.associated_vehicle_engine_manufacturer) AS associated_vehicle_engine_manufacturer,
  COALESCE(old_cm_vg_devices.associated_vehicle_max_weight_lbs, vg_devices.associated_vehicle_max_weight_lbs) AS associated_vehicle_max_weight_lbs,
  COALESCE(old_cm_vg_devices.associated_vehicle_gross_vehicle_weight_rating, vg_devices.associated_vehicle_gross_vehicle_weight_rating) AS associated_vehicle_gross_vehicle_weight_rating,
  device_name,
  asset_type,
  product_name

FROM
(
SELECT
    raw_productdb_devices.org_id AS org_id,
    raw_productdb_devices.id AS device_id,
    NULL AS old_cm_vg_device_id,
    raw_productdb_devices.serial,
    raw_productdb_devices.product_id,
    CASE
      WHEN products.name LIKE 'CM%' THEN 'CM - AI Dash Cam'
      WHEN products.name = 'Trailer' THEN 'Trailer'
      WHEN products.name LIKE 'AG%' THEN 'AG - Asset Gateway'
      WHEN products.name LIKE 'IG%' OR products.name LIKE 'GW%' THEN 'IG - Industrial Gateway'
      WHEN products.name LIKE 'SG%' OR products.name = 'VMS Camera' THEN 'SG - Site Gateway'
      WHEN products.name LIKE 'AT%' OR products.name = 'Gretel' OR products.name = 'TL11' OR products.name LIKE '%Hansel%' THEN 'AT - Asset Tracker'
      WHEN products.name LIKE 'AHD%' OR products.name LIKE 'NVR%' OR products.name LIKE 'AIM%' THEN 'Camera Connector'
      WHEN products.name = 'App Telematics Device' THEN products.name
      WHEN products.name = 'Asset' THEN 'Virtual Asset'
      WHEN products.name LIKE 'OEM%' THEN 'OEM - Original Equipment Manufacturer'
      WHEN products.name LIKE 'VS%' THEN 'VS - Vision System'
      WHEN products.name LIKE 'LM%' OR products.name = 'Graphite' THEN 'LM - Level Monitor'
      WHEN products.name LIKE 'CW%' THEN 'Connected Worker'
      WHEN products.name LIKE 'MC%' THEN 'Samsara Camera'
      WHEN products.name LIKE 'MN%' THEN 'Camera Display'
      ELSE 'UNKNOWN'
    END AS device_type,
    raw_productdb_devices.variant_id,

    -- Vehicle Data
    raw_productdb_devices.vin AS associated_vehicle_vin,
    raw_productdb_devices.make AS associated_vehicle_make,
    raw_productdb_devices.model AS associated_vehicle_model,
    CASE
        WHEN raw_productdb_devices.year = 0
        THEN NULL
        ELSE raw_productdb_devices.year
    END AS associated_vehicle_year,
    raw_productdb_devices.updated_at,
    raw_productdb_devices.name AS device_name,
    raw_productdb_devices.asset_type,
    products.name AS product_name
  FROM datamodel_core_bronze.raw_productdb_devices
LEFT OUTER JOIN definitions.products
ON (raw_productdb_devices.product_id = products.product_id)
WHERE raw_productdb_devices.date = '{DATEID}'
AND products.name NOT LIKE 'VG%'
AND products.name <> 'Test-VG'

UNION ALL

SELECT
    raw_productdb_devices.org_id AS org_id,
    cast(concat(string(raw_productdb_devices.camera_product_id), string(raw_productdb_devices.id)) AS long) AS device_id,
    raw_productdb_devices.id AS old_cm_vg_device_id,
    UPPER(REPLACE(REPLACE(raw_productdb_devices.camera_serial, '-', ''), ' ', '')) AS serial,
    raw_productdb_devices.camera_product_id AS product_id,
    'CM - AI Dash Cam' AS device_type,
    raw_productdb_devices.camera_variant_id,

    -- Vehicle Data
    raw_productdb_devices.vin AS associated_vehicle_vin,
    raw_productdb_devices.make AS associated_vehicle_make,
    raw_productdb_devices.model AS associated_vehicle_model,
    CASE
        WHEN raw_productdb_devices.year = 0
        THEN NULL
        ELSE raw_productdb_devices.year
    END AS associated_vehicle_year,
    raw_productdb_devices.updated_at,
    NULL AS device_name,
    NULL AS asset_type,
    products.name AS product_name

  FROM datamodel_core_bronze.raw_productdb_devices
  LEFT OUTER JOIN definitions.products
  ON (raw_productdb_devices.camera_product_id = products.product_id)
WHERE raw_productdb_devices.date = '{DATEID}'
AND raw_productdb_devices.camera_product_id IN (30,31,25) -- Edge case CM devices without rows in the devices table
) non_vg_devices
LEFT OUTER JOIN vg_devices
ON (non_vg_devices.serial = vg_devices.camera_serial)
LEFT OUTER JOIN vg_devices old_cm_vg_devices
ON (non_vg_devices.old_cm_vg_device_id = old_cm_vg_devices.device_id
    AND non_vg_devices.org_id = old_cm_vg_devices.org_id)

"""

stg_vg_devices_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """Attribute defining the device type (e.g. VG, CM, AG, ...).
            CMs will also include product name values from their parent devices where they are denoted as cameras instead of devices"""
        },
    },
    {
        "name": "variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "associated_vehicle_vin",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Vehicle identification number (VIN) of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "camera_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "CM device ID associated with the VG"},
    },
    {
        "name": "camera_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "CM serial associated with the VG"},
    },
    {
        "name": "camera_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "CM associated with the VG. Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "camera_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "CM device type associated with the VG"},
    },
    {
        "name": "camera_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "CM variant ID associated with the VG"},
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "is_unregulated_vehicle",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Does the vehicle exempts driver from following HOS rules"
        },
    },
    {
        "name": "vehicle_obd_type",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "OBD type of device for the vehicle"},
    },
    {
        "name": "config_override_json",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "json view of device level override configs"},
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Trim of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the device"},
    },
    {
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle. Will be translated downstream"
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the product. Sourced from definitions.products"
        },
    },
]


stg_vg_devices_query = """

WITH stg_non_vg_devices_no_dupes (
  -- Addressing edge case where a serial number is associated with multiple devices
SELECT stg_non_vg_devices.serial,
MAX_BY(stg_non_vg_devices.device_id, updated_at) AS camera_device_id,
MAX_BY(stg_non_vg_devices.device_type, updated_at) AS camera_device_type
FROM {database_silver_dev}.stg_non_vg_devices
WHERE stg_non_vg_devices.date = '{DATEID}'
GROUP BY 1
)
SELECT stg_vg_devices_step_1.org_id,
    stg_vg_devices_step_1.device_id,
    stg_vg_devices_step_1.serial,
    stg_vg_devices_step_1.product_id,
    stg_vg_devices_step_1.device_type,
    stg_vg_devices_step_1.variant_id,

    -- Vehicle Data
    stg_vg_devices_step_1.associated_vehicle_vin,
    stg_vg_devices_step_1.associated_vehicle_make,
    stg_vg_devices_step_1.associated_vehicle_model,
    stg_vg_devices_step_1.associated_vehicle_year,

    -- Assoctiated devices
    stg_non_vg_devices_no_dupes.camera_device_id,
    stg_vg_devices_step_1.camera_serial,
    stg_vg_devices_step_1.camera_product_id,
    stg_non_vg_devices_no_dupes.camera_device_type,
    stg_vg_devices_step_1.camera_variant_id,
    '{DATEID}' AS date,
    stg_vg_devices_step_1.is_unregulated_vehicle,
    stg_vg_devices_step_1.vehicle_obd_type,
    stg_vg_devices_step_1.config_override_json,

    -- Additional vehicle attributes
    stg_vg_devices_step_1.associated_vehicle_engine_model,
    stg_vg_devices_step_1.associated_vehicle_primary_fuel_type,
    stg_vg_devices_step_1.associated_vehicle_secondary_fuel_type,
    stg_vg_devices_step_1.associated_vehicle_engine_type,
    stg_vg_devices_step_1.associated_vehicle_trim,
    stg_vg_devices_step_1.associated_vehicle_engine_manufacturer,
    stg_vg_devices_step_1.associated_vehicle_max_weight_lbs,
    stg_vg_devices_step_1.associated_vehicle_gross_vehicle_weight_rating,
    stg_vg_devices_step_1.device_name,
    stg_vg_devices_step_1.asset_type,
    stg_vg_devices_step_1.product_name

FROM {database_silver_dev}.stg_vg_devices_step_1
LEFT OUTER JOIN stg_non_vg_devices_no_dupes
ON (stg_vg_devices_step_1.camera_serial = stg_non_vg_devices_no_dupes.serial)
WHERE stg_vg_devices_step_1.date = '{DATEID}'


"""


stg_vg_multicam_cm_devices_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "vg_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "ID of VG device"},
    },
    {
        "name": "multicam_num",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Row number for each multicam per VG (1, 2, etc.), ordered by multicam_device_id"
        },
    },
    {
        "name": "vg_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for VG device"},
    },
    {
        "name": "vg_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Device type of VG device"},
    },
    {
        "name": "vg_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "vg_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "multicam_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "ID of Multicam device"},
    },
    {
        "name": "multicam_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for multicam device"},
    },
    {
        "name": "multicam_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Device type for multicam device"},
    },
    {
        "name": "multicam_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "multicam_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "camera_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "ID of CM device"},
    },
    {
        "name": "camera_serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for CM device"},
    },
    {
        "name": "camera_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Device type for CM device"},
    },
    {
        "name": "camera_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "camera_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Trim of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
]

stg_vg_multicam_cm_devices_query = """
WITH multicam_collections_raw AS (
SELECT
    org_id,
    device_id AS multicam_device_id,
    collections.product_id AS multicam_product_id,
    collection_uuid,
    start_at,
    end_at,
    updated_at,
    -- Rank by updated_at DESC to get the latest active association per multicam device
    -- This handles cases where a multicam has multiple active associations (end_at IS NULL)
    ROW_NUMBER() OVER (PARTITION BY org_id, device_id ORDER BY updated_at DESC) AS rn
FROM deviceassociationsdb_shards.device_collection_associations collections
JOIN definitions.products AS products
ON collections.product_id = products.product_id
WHERE (products.name LIKE 'AHD%' OR products.name LIKE 'NVR%' OR products.name LIKE 'AIM%')
AND end_at IS NULL
),
multicam_collections AS (
SELECT
    org_id,
    multicam_device_id,
    multicam_product_id,
    collection_uuid,
    start_at,
    end_at
FROM multicam_collections_raw
WHERE rn = 1
),
multicam_vg_collections_raw AS (
SELECT
    org_id,
    device_id AS vg_device_id,
    collections.product_id AS vg_product_id,
    collection_uuid,
    start_at,
    end_at,
    updated_at,
    -- Rank by updated_at DESC to get the latest VG association per collection (multicam)
    -- This handles cases where a multicam has multiple VGs with end_at IS NULL - we pick one VG per multicam
    ROW_NUMBER() OVER (PARTITION BY org_id, collection_uuid ORDER BY updated_at DESC) AS rn
FROM deviceassociationsdb_shards.device_collection_associations AS collections
JOIN definitions.products AS products
ON collections.product_id = products.product_id
WHERE products.name LIKE 'VG%'
AND collection_uuid IN (SELECT collection_uuid FROM multicam_collections)
AND end_at IS NULL
),
multicam_vg_collections AS (
SELECT
    org_id,
    vg_device_id,
    vg_product_id,
    collection_uuid,
    start_at,
    end_at
FROM multicam_vg_collections_raw
WHERE rn = 1
),
vg_multicam_joined AS (
SELECT DISTINCT
    '{DATEID}' AS date,
    ovc.org_id,
    ovc.vg_device_id,
    vd.serial AS vg_serial,
    vd.device_type AS vg_device_type,
    vd.variant_id AS vg_variant_id,
    ovc.vg_product_id,
    oc.multicam_device_id AS multicam_device_id,
    oc.start_at AS multicam_start_at,
    nvd.device_type AS multicam_device_type,
    nvd.variant_id AS multicam_variant_id,
    nvd.serial AS multicam_serial,
    oc.multicam_product_id,
    vd.camera_device_id AS camera_device_id,
    vd.camera_device_type AS camera_device_type,
    vd.camera_variant_id AS camera_variant_id,
    vd.camera_serial AS camera_serial,
    vd.camera_product_id AS camera_product_id,
    vd.associated_vehicle_make,
    vd.associated_vehicle_model,
    vd.associated_vehicle_year,
    vd.associated_vehicle_engine_model,
    vd.associated_vehicle_primary_fuel_type,
    vd.associated_vehicle_secondary_fuel_type,
    vd.associated_vehicle_engine_type,
    vd.associated_vehicle_trim,
    vd.associated_vehicle_engine_manufacturer,
    vd.associated_vehicle_max_weight_lbs,
    vd.associated_vehicle_gross_vehicle_weight_rating
FROM multicam_vg_collections AS ovc
JOIN multicam_collections AS oc
ON ovc.collection_uuid = oc.collection_uuid
JOIN {database_silver_dev}.stg_non_vg_devices nvd -- brings in additional multicam fields
ON nvd.device_id = oc.multicam_device_id
    AND nvd.org_id = oc.org_id
JOIN {database_silver_dev}.stg_vg_devices vd -- join to bring in corresponding CM details
ON vd.device_id = ovc.vg_device_id
    AND vd.org_id = ovc.org_id
WHERE nvd.date = '{DATEID}'
AND vd.date = '{DATEID}'
)
SELECT
    date,
    org_id,
    vg_device_id,
    ROW_NUMBER() OVER (PARTITION BY org_id, vg_device_id ORDER BY multicam_start_at DESC, multicam_device_id) AS multicam_num,
    vg_serial,
    vg_device_type,
    vg_variant_id,
    vg_product_id,
    multicam_device_id,
    multicam_device_type,
    multicam_variant_id,
    multicam_serial,
    multicam_product_id,
    camera_device_id,
    camera_device_type,
    camera_variant_id,
    camera_serial,
    camera_product_id,
    associated_vehicle_make,
    associated_vehicle_model,
    associated_vehicle_year,
    associated_vehicle_engine_model,
    associated_vehicle_primary_fuel_type,
    associated_vehicle_secondary_fuel_type,
    associated_vehicle_engine_type,
    associated_vehicle_trim,
    associated_vehicle_engine_manufacturer,
    associated_vehicle_max_weight_lbs,
    associated_vehicle_gross_vehicle_weight_rating
FROM vg_multicam_joined
"""

dim_devices_fast_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """Attribute defining the device type (e.g. VG, CM, AG, ...).
            CMs will also include product name values from their parent devices where they are denoted as cameras instead of devices"""
        },
    },
    {
        "name": "variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "associated_devices",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "string",
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Map describing devices associated with the device_id. For example the VG associated with the CM or vice versa"
        },
    },
    {
        "name": "associated_devices_array",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "device_id", "type": "string", "nullable": True},
                    {"name": "serial", "type": "string", "nullable": True},
                    {"name": "product_id", "type": "string", "nullable": True},
                    {"name": "device_type", "type": "string", "nullable": True},
                    {"name": "variant_id", "type": "string", "nullable": True},
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of structs describing devices associated with the device_id. Each struct contains device_id, serial, product_id, device_type, and variant_id"
        },
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Trim of the vehicle associated with the device",
        },
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the device"},
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the product. Sourced from definitions.products"
        },
    },
    {
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle"
        },
    },
    {
        "name": "asset_type_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Based on asset_type. 1 = 'Trailer' 2 = 'Equipment' 3 = 'Unpowered' 4 = 'Vehicle' 0 = 'None'"
        },
    },
    {
        "name": "asset_type_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Used last_cable_id, num_canbus_stats and product_name to determine more granular asset type attributes"
        },
    },
    {
        "name": "last_cable_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdobdcableid, last logged cable_id"
        },
    },
    {
        "name": "last_cable_id_time",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdobdcableid, date of the last logged cable_id"
        },
    },
    {
        "name": "num_canbus_stats",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdcanbustype. Has the device ever logged canbus stats"
        },
    },
    {
        "name": "gateway_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Id of gateway on date matching device_id sourced from productsdb.gateways"
        },
    },
    {
        "name": "weekly_availability_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Availability in milliseconds that the asset (ex: vehicle) is able to be utilized, as determined by the customer with a default value equivalent to 12 hours per day. For instance, some assets may be available for 12 hours per day and others for 24 hours per day depending on the asset."
        },
    },
    {
        "name": "oem_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of corresponding OEM carrier (if exists)."},
    },
]


dim_devices_fast_query = """

WITH gateway_mapping AS (
SELECT id AS gateway_id,
       device_id
FROM datamodel_core_bronze.raw_productsdb_gateways
WHERE date = '{DATEID}'
/* TODO CHANGE DB */),
cable_history AS
(
  SELECT osdobdcableid.object_id AS device_id,
  MAX_BY(osdobdcableid.value.int_value, time) AS last_cable_id,
  MAX(time) AS last_cable_id_time,
  MAX(date) AS date
  FROM kinesisstats_history.osdobdcableid
  WHERE osdobdcableid.value.int_value IS NOT NULL
  AND date <= '{DATEID}'
  AND org_id not in (0, 1)
  AND to_date(from_unixtime(time / 1000)) = date
  and value.is_end = false
  and value.is_databreak = false
  GROUP BY 1
),
canbus_stat_history AS (
  SELECT
    object_id AS device_id,
    COUNT(*) AS num_canbus_stats
  FROM kinesisstats_history.osdcanbustype
  WHERE date <= '{DATEID}'
  GROUP BY 1
),
availability AS (
SELECT device_id,
       org_id,
       weekly_availability_ms
FROM jobschedulesdb_shards.asset_available_hours
),
main AS (
  SELECT
  '{DATEID}' AS date,
  vgd.org_id,
  vgd.device_id,
  vgd.serial,
  vgd.product_id,
  vgd.device_type,
  vgd.variant_id,
  vgd.associated_vehicle_make,
  vgd.associated_vehicle_model,
  vgd.associated_vehicle_year,
  CASE
    -- VG with CM, multicam 1, and multicam 2
    WHEN vgd.camera_serial IS NOT NULL AND olv1.multicam_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'camera_device_id', vgd.camera_device_id,
            'camera_serial', vgd.camera_serial,
            'camera_product_id', vgd.camera_product_id,
            'camera_device_type', vgd.camera_device_type,
            'camera_variant_id', vgd.camera_variant_id,
            'multicam_device_id', olv1.multicam_device_id,
            'multicam_serial', olv1.multicam_serial,
            'multicam_product_id', olv1.multicam_product_id,
            'multicam_device_type', olv1.multicam_device_type,
            'multicam_variant_id', olv1.multicam_variant_id,
            'multicam_2_device_id', olv2.multicam_device_id,
            'multicam_2_serial', olv2.multicam_serial,
            'multicam_2_product_id', olv2.multicam_product_id,
            'multicam_2_device_type', olv2.multicam_device_type,
            'multicam_2_variant_id', olv2.multicam_variant_id
        )
    -- VG with CM and only multicam 1 (no multicam 2)
    WHEN vgd.camera_serial IS NOT NULL AND olv1.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'camera_device_id', vgd.camera_device_id,
            'camera_serial', vgd.camera_serial,
            'camera_product_id', vgd.camera_product_id,
            'camera_device_type', vgd.camera_device_type,
            'camera_variant_id', vgd.camera_variant_id,
            'multicam_device_id', olv1.multicam_device_id,
            'multicam_serial', olv1.multicam_serial,
            'multicam_product_id', olv1.multicam_product_id,
            'multicam_device_type', olv1.multicam_device_type,
            'multicam_variant_id', olv1.multicam_variant_id
        )
    -- VG with multicam 1 and multicam 2 (no CM)
    WHEN olv1.multicam_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'multicam_device_id', olv1.multicam_device_id,
            'multicam_serial', olv1.multicam_serial,
            'multicam_product_id', olv1.multicam_product_id,
            'multicam_device_type', olv1.multicam_device_type,
            'multicam_variant_id', olv1.multicam_variant_id,
            'multicam_2_device_id', olv2.multicam_device_id,
            'multicam_2_serial', olv2.multicam_serial,
            'multicam_2_product_id', olv2.multicam_product_id,
            'multicam_2_device_type', olv2.multicam_device_type,
            'multicam_2_variant_id', olv2.multicam_variant_id
        )
    -- VG with only multicam 1 (no CM, no multicam 2)
    WHEN olv1.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'multicam_device_id', olv1.multicam_device_id,
            'multicam_serial', olv1.multicam_serial,
            'multicam_product_id', olv1.multicam_product_id,
            'multicam_device_type', olv1.multicam_device_type,
            'multicam_variant_id', olv1.multicam_variant_id
        )
    -- VG with only CM (no multicam)
    WHEN vgd.camera_serial IS NOT NULL
    THEN
        MAP(
            'camera_device_id', vgd.camera_device_id,
            'camera_serial', vgd.camera_serial,
            'camera_product_id', vgd.camera_product_id,
            'camera_device_type', vgd.camera_device_type,
            'camera_variant_id', vgd.camera_variant_id
        )
  END AS associated_devices,
  -- Array version of associated_devices for easier querying
  CASE
    -- VG with CM, multicam 1, and multicam 2
    WHEN vgd.camera_serial IS NOT NULL AND olv1.multicam_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(vgd.camera_device_id AS STRING), 'serial', vgd.camera_serial, 'product_id', CAST(vgd.camera_product_id AS STRING), 'device_type', vgd.camera_device_type, 'variant_id', CAST(vgd.camera_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv1.multicam_device_id AS STRING), 'serial', olv1.multicam_serial, 'product_id', CAST(olv1.multicam_product_id AS STRING), 'device_type', olv1.multicam_device_type, 'variant_id', CAST(olv1.multicam_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv2.multicam_device_id AS STRING), 'serial', olv2.multicam_serial, 'product_id', CAST(olv2.multicam_product_id AS STRING), 'device_type', olv2.multicam_device_type, 'variant_id', CAST(olv2.multicam_variant_id AS STRING))
        )
    -- VG with CM and only multicam 1 (no multicam 2)
    WHEN vgd.camera_serial IS NOT NULL AND olv1.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(vgd.camera_device_id AS STRING), 'serial', vgd.camera_serial, 'product_id', CAST(vgd.camera_product_id AS STRING), 'device_type', vgd.camera_device_type, 'variant_id', CAST(vgd.camera_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv1.multicam_device_id AS STRING), 'serial', olv1.multicam_serial, 'product_id', CAST(olv1.multicam_product_id AS STRING), 'device_type', olv1.multicam_device_type, 'variant_id', CAST(olv1.multicam_variant_id AS STRING))
        )
    -- VG with multicam 1 and multicam 2 (no CM)
    WHEN olv1.multicam_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.multicam_device_id AS STRING), 'serial', olv1.multicam_serial, 'product_id', CAST(olv1.multicam_product_id AS STRING), 'device_type', olv1.multicam_device_type, 'variant_id', CAST(olv1.multicam_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv2.multicam_device_id AS STRING), 'serial', olv2.multicam_serial, 'product_id', CAST(olv2.multicam_product_id AS STRING), 'device_type', olv2.multicam_device_type, 'variant_id', CAST(olv2.multicam_variant_id AS STRING))
        )
    -- VG with only multicam 1 (no CM, no multicam 2)
    WHEN olv1.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.multicam_device_id AS STRING), 'serial', olv1.multicam_serial, 'product_id', CAST(olv1.multicam_product_id AS STRING), 'device_type', olv1.multicam_device_type, 'variant_id', CAST(olv1.multicam_variant_id AS STRING))
        )
    -- VG with only CM (no multicam)
    WHEN vgd.camera_serial IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(vgd.camera_device_id AS STRING), 'serial', vgd.camera_serial, 'product_id', CAST(vgd.camera_product_id AS STRING), 'device_type', vgd.camera_device_type, 'variant_id', CAST(vgd.camera_variant_id AS STRING))
        )
  END AS associated_devices_array,
  vgd.associated_vehicle_engine_model,
  vgd.associated_vehicle_primary_fuel_type,
  vgd.associated_vehicle_secondary_fuel_type,
  vgd.associated_vehicle_engine_type,
  vgd.associated_vehicle_trim,
  vgd.associated_vehicle_engine_manufacturer,
  vgd.associated_vehicle_max_weight_lbs,
  vgd.associated_vehicle_gross_vehicle_weight_rating,
  vgd.device_name,
  vgd.asset_type,
  vgd.product_name,
  CASE
    WHEN vgd.asset_type = 1 THEN 'Trailer'
    WHEN vgd.asset_type = 2 THEN 'Equipment'
    WHEN vgd.asset_type = 3 THEN 'Unpowered'
    WHEN vgd.asset_type = 4 THEN 'Vehicle'
    WHEN vgd.asset_type = 0 THEN 'None'
  ELSE 'Other'
  END AS asset_type_name
  FROM {database_silver_dev}.stg_vg_devices vgd
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices olv1
  ON vgd.device_id = olv1.vg_device_id
    AND vgd.org_id = olv1.org_id
    AND vgd.date = olv1.date
    AND olv1.multicam_num = 1
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices olv2
  ON vgd.device_id = olv2.vg_device_id
    AND vgd.org_id = olv2.org_id
    AND vgd.date = olv2.date
    AND olv2.multicam_num = 2
  WHERE vgd.date = '{DATEID}'

  UNION ALL

  SELECT
  '{DATEID}' AS date,
  nvd.org_id,
  nvd.device_id,
  nvd.serial,
  nvd.product_id,
  nvd.device_type,
  nvd.variant_id,
  COALESCE(nvd.associated_vehicle_make, olv1.associated_vehicle_make) AS associated_vehicle_make,
  COALESCE(nvd.associated_vehicle_model, olv1.associated_vehicle_model) AS associated_vehicle_model,
  COALESCE(nvd.associated_vehicle_year, olv1.associated_vehicle_year) AS associated_vehicle_year,
  CASE
    -- Multicam device with VG, CM, and a sibling multicam
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'vg_device_id', olv1.vg_device_id,
            'vg_serial', olv1.vg_serial,
            'vg_product_id', olv1.vg_product_id,
            'vg_device_type', olv1.vg_device_type,
            'vg_variant_id', olv1.vg_variant_id,
            'camera_device_id', olv1.camera_device_id,
            'camera_serial', olv1.camera_serial,
            'camera_product_id', olv1.camera_product_id,
            'camera_device_type', olv1.camera_device_type,
            'camera_variant_id', olv1.camera_variant_id,
            'multicam_device_id', olv2.multicam_device_id,
            'multicam_serial', olv2.multicam_serial,
            'multicam_product_id', olv2.multicam_product_id,
            'multicam_device_type', olv2.multicam_device_type,
            'multicam_variant_id', olv2.multicam_variant_id
        )
    -- Multicam device with VG and CM (no sibling multicam)
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NOT NULL
    THEN
        MAP(
            'vg_device_id', olv1.vg_device_id,
            'vg_serial', olv1.vg_serial,
            'vg_product_id', olv1.vg_product_id,
            'vg_device_type', olv1.vg_device_type,
            'vg_variant_id', olv1.vg_variant_id,
            'camera_device_id', olv1.camera_device_id,
            'camera_serial', olv1.camera_serial,
            'camera_product_id', olv1.camera_product_id,
            'camera_device_type', olv1.camera_device_type,
            'camera_variant_id', olv1.camera_variant_id
        )
    -- Multicam device with VG, no CM, but has sibling multicam
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'vg_device_id', olv1.vg_device_id,
            'vg_serial', olv1.vg_serial,
            'vg_product_id', olv1.vg_product_id,
            'vg_device_type', olv1.vg_device_type,
            'vg_variant_id', olv1.vg_variant_id,
            'multicam_device_id', olv2.multicam_device_id,
            'multicam_serial', olv2.multicam_serial,
            'multicam_product_id', olv2.multicam_product_id,
            'multicam_device_type', olv2.multicam_device_type,
            'multicam_variant_id', olv2.multicam_variant_id
        )
    -- Multicam device with VG but no CM (no sibling multicam)
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NULL
    THEN
        MAP(
            'vg_device_id', olv1.vg_device_id,
            'vg_serial', olv1.vg_serial,
            'vg_product_id', olv1.vg_product_id,
            'vg_device_type', olv1.vg_device_type,
            'vg_variant_id', olv1.vg_variant_id
        )
    -- CM with VG and multicam
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NOT NULL AND cam2.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'vg_device_id', nvd.vg_device_id,
            'vg_serial', nvd.vg_serial,
            'vg_product_id', nvd.vg_product_id,
            'vg_device_type', nvd.vg_device_type,
            'vg_variant_id', nvd.vg_variant_id,
            'multicam_device_id', cam1.multicam_device_id,
            'multicam_serial', cam1.multicam_serial,
            'multicam_product_id', cam1.multicam_product_id,
            'multicam_device_type', cam1.multicam_device_type,
            'multicam_variant_id', cam1.multicam_variant_id,
            'multicam_2_device_id', cam2.multicam_device_id,
            'multicam_2_serial', cam2.multicam_serial,
            'multicam_2_product_id', cam2.multicam_product_id,
            'multicam_2_device_type', cam2.multicam_device_type,
            'multicam_2_variant_id', cam2.multicam_variant_id
        )
    -- CM with VG and one multicam (no second multicam)
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NOT NULL
    THEN
        MAP(
            'vg_device_id', nvd.vg_device_id,
            'vg_serial', nvd.vg_serial,
            'vg_product_id', nvd.vg_product_id,
            'vg_device_type', nvd.vg_device_type,
            'vg_variant_id', nvd.vg_variant_id,
            'multicam_device_id', cam1.multicam_device_id,
            'multicam_serial', cam1.multicam_serial,
            'multicam_product_id', cam1.multicam_product_id,
            'multicam_device_type', cam1.multicam_device_type,
            'multicam_variant_id', cam1.multicam_variant_id
        )
    -- CM with VG and no multicam
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NULL
    THEN
        MAP(
            'vg_device_id', nvd.vg_device_id,
            'vg_serial', nvd.vg_serial,
            'vg_product_id', nvd.vg_product_id,
            'vg_device_type', nvd.vg_device_type,
            'vg_variant_id', nvd.vg_variant_id
        )
  END AS associated_devices,
  -- Array version of associated_devices for easier querying
  CASE
    -- Multicam device with VG, CM, and a sibling multicam
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NOT NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.vg_device_id AS STRING), 'serial', olv1.vg_serial, 'product_id', CAST(olv1.vg_product_id AS STRING), 'device_type', olv1.vg_device_type, 'variant_id', CAST(olv1.vg_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv1.camera_device_id AS STRING), 'serial', olv1.camera_serial, 'product_id', CAST(olv1.camera_product_id AS STRING), 'device_type', olv1.camera_device_type, 'variant_id', CAST(olv1.camera_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv2.multicam_device_id AS STRING), 'serial', olv2.multicam_serial, 'product_id', CAST(olv2.multicam_product_id AS STRING), 'device_type', olv2.multicam_device_type, 'variant_id', CAST(olv2.multicam_variant_id AS STRING))
        )
    -- Multicam device with VG and CM (no sibling multicam)
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.vg_device_id AS STRING), 'serial', olv1.vg_serial, 'product_id', CAST(olv1.vg_product_id AS STRING), 'device_type', olv1.vg_device_type, 'variant_id', CAST(olv1.vg_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv1.camera_device_id AS STRING), 'serial', olv1.camera_serial, 'product_id', CAST(olv1.camera_product_id AS STRING), 'device_type', olv1.camera_device_type, 'variant_id', CAST(olv1.camera_variant_id AS STRING))
        )
    -- Multicam device with VG, no CM, but has sibling multicam
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NULL AND olv2.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.vg_device_id AS STRING), 'serial', olv1.vg_serial, 'product_id', CAST(olv1.vg_product_id AS STRING), 'device_type', olv1.vg_device_type, 'variant_id', CAST(olv1.vg_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(olv2.multicam_device_id AS STRING), 'serial', olv2.multicam_serial, 'product_id', CAST(olv2.multicam_product_id AS STRING), 'device_type', olv2.multicam_device_type, 'variant_id', CAST(olv2.multicam_variant_id AS STRING))
        )
    -- Multicam device with VG but no CM (no sibling multicam)
    WHEN (nvd.product_name LIKE 'AHD%' OR nvd.product_name LIKE 'NVR%' OR nvd.product_name LIKE 'AIM%') AND olv1.vg_device_id IS NOT NULL AND olv1.camera_device_id IS NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(olv1.vg_device_id AS STRING), 'serial', olv1.vg_serial, 'product_id', CAST(olv1.vg_product_id AS STRING), 'device_type', olv1.vg_device_type, 'variant_id', CAST(olv1.vg_variant_id AS STRING))
        )
    -- CM with VG and multicam
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NOT NULL AND cam2.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(nvd.vg_device_id AS STRING), 'serial', nvd.vg_serial, 'product_id', CAST(nvd.vg_product_id AS STRING), 'device_type', nvd.vg_device_type, 'variant_id', CAST(nvd.vg_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(cam1.multicam_device_id AS STRING), 'serial', cam1.multicam_serial, 'product_id', CAST(cam1.multicam_product_id AS STRING), 'device_type', cam1.multicam_device_type, 'variant_id', CAST(cam1.multicam_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(cam2.multicam_device_id AS STRING), 'serial', cam2.multicam_serial, 'product_id', CAST(cam2.multicam_product_id AS STRING), 'device_type', cam2.multicam_device_type, 'variant_id', CAST(cam2.multicam_variant_id AS STRING))
        )
    -- CM with VG and one multicam (no second multicam)
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NOT NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(nvd.vg_device_id AS STRING), 'serial', nvd.vg_serial, 'product_id', CAST(nvd.vg_product_id AS STRING), 'device_type', nvd.vg_device_type, 'variant_id', CAST(nvd.vg_variant_id AS STRING)),
            NAMED_STRUCT('device_id', CAST(cam1.multicam_device_id AS STRING), 'serial', cam1.multicam_serial, 'product_id', CAST(cam1.multicam_product_id AS STRING), 'device_type', cam1.multicam_device_type, 'variant_id', CAST(cam1.multicam_variant_id AS STRING))
        )
    -- CM with VG and no multicam
    WHEN nvd.device_type LIKE 'CM%' AND nvd.vg_device_id IS NOT NULL AND cam1.multicam_device_id IS NULL
    THEN
        ARRAY(
            NAMED_STRUCT('device_id', CAST(nvd.vg_device_id AS STRING), 'serial', nvd.vg_serial, 'product_id', CAST(nvd.vg_product_id AS STRING), 'device_type', nvd.vg_device_type, 'variant_id', CAST(nvd.vg_variant_id AS STRING))
        )
  END AS associated_devices_array,
  COALESCE(nvd.associated_vehicle_engine_model, olv1.associated_vehicle_engine_model) AS associated_vehicle_engine_model,
  COALESCE(nvd.associated_vehicle_primary_fuel_type, olv1.associated_vehicle_primary_fuel_type) AS associated_vehicle_primary_fuel_type,
  COALESCE(nvd.associated_vehicle_secondary_fuel_type, olv1.associated_vehicle_secondary_fuel_type) AS associated_vehicle_secondary_fuel_type,
  COALESCE(nvd.associated_vehicle_engine_type, olv1.associated_vehicle_engine_type) AS associated_vehicle_engine_type,
  COALESCE(nvd.associated_vehicle_trim, olv1.associated_vehicle_trim) AS associated_vehicle_trim,
  COALESCE(nvd.associated_vehicle_engine_manufacturer, olv1.associated_vehicle_engine_manufacturer) AS associated_vehicle_engine_manufacturer,
  COALESCE(nvd.associated_vehicle_max_weight_lbs, olv1.associated_vehicle_max_weight_lbs) AS associated_vehicle_max_weight_lbs,
  COALESCE(nvd.associated_vehicle_gross_vehicle_weight_rating, olv1.associated_vehicle_gross_vehicle_weight_rating) AS associated_vehicle_gross_vehicle_weight_rating,
  nvd.device_name,
  nvd.asset_type,
  nvd.product_name,
  CASE
    WHEN nvd.asset_type = 1 THEN 'Trailer'
    WHEN nvd.asset_type = 2 THEN 'Equipment'
    WHEN nvd.asset_type = 3 THEN 'Unpowered'
    WHEN nvd.asset_type = 4 THEN 'Vehicle'
    WHEN nvd.asset_type = 0 THEN 'None'
  ELSE 'Other'
END AS asset_type_name
  FROM {database_silver_dev}.stg_non_vg_devices nvd
  -- olv1: Find the row for this multicam device (gives us the VG)
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices olv1
    ON nvd.device_id = olv1.multicam_device_id
    AND nvd.org_id = olv1.org_id
    AND nvd.date = olv1.date
  -- olv2: Find the sibling multicam via the VG from olv1
  -- Use multicam_num to get exactly one sibling (if multicam 1, get 2; if multicam 2, get 1)
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices olv2
    ON olv1.vg_device_id = olv2.vg_device_id
    AND olv1.org_id = olv2.org_id
    AND olv1.date = olv2.date
    AND olv2.multicam_num = CASE WHEN olv1.multicam_num = 1 THEN 2 ELSE 1 END
  -- cam1: Find the first multicam row for this CM device
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices cam1
    ON nvd.device_id = cam1.camera_device_id
    AND nvd.org_id = cam1.org_id
    AND nvd.date = cam1.date
    AND cam1.multicam_num = 1
  -- cam2: Find the second multicam via the VG from cam1
  LEFT OUTER JOIN {database_silver_dev}.stg_vg_multicam_cm_devices cam2
    ON cam1.vg_device_id = cam2.vg_device_id
    AND cam1.org_id = cam2.org_id
    AND cam1.date = cam2.date
    AND cam2.multicam_num = 2
  WHERE nvd.date = '{DATEID}'
)
SELECT
'{DATEID}' AS date,
main.org_id,
main.device_id,
main.serial,
main.product_id,
main.device_type,
main.variant_id,
main.associated_vehicle_make,
main.associated_vehicle_model,
main.associated_vehicle_year,
main.associated_devices,
main.associated_devices_array,
main.associated_vehicle_engine_model,
main.associated_vehicle_primary_fuel_type,
main.associated_vehicle_secondary_fuel_type,
main.associated_vehicle_engine_type,
main.associated_vehicle_trim,
main.associated_vehicle_engine_manufacturer,
main.associated_vehicle_max_weight_lbs,
main.associated_vehicle_gross_vehicle_weight_rating,
main.device_name,
main.product_name,
main.asset_type,
main.asset_type_name,
CASE
      WHEN (
        last_cable_id = 1
        AND product_name IN (
          'AG26',
          'AG26-EU',
          'AG24',
          'AG24-EU',
          'AG52',
          'AG52-EU',
          'AG53',
          'AG53-EU'
        )
      )
      OR (
        last_cable_id = 101
        AND product_name IN (
          'AG26',
          'AG26-EU',
          'AG24',
          'AG24-EU',
          'AG52',
          'AG52-EU',
          'AG53',
          'AG53-EU'
        )
      ) THEN 'Smart Trailers'
      WHEN (
        last_cable_id = 3
        AND product_name IN (
          'AG26',
          'AG26-EU',
          'AG24',
          'AG24-EU',
          'AG52',
          'AG52-EU',
          'AG53',
          'AG53-EU'
        )
      )
      OR (
        last_cable_id = 100
        AND product_name IN (
          'AG26',
          'AG26-EU',
          'AG24',
          'AG24-EU',
          'AG52',
          'AG52-EU',
          'AG53',
          'AG53-EU'
        )
      ) THEN 'Smart Trailers'
      WHEN (
        last_cable_id = 5
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
      )
      OR (
        last_cable_id = 12
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
      ) THEN 'Smart Trailers'
      WHEN (
        last_cable_id = 5
        AND product_name IN ('AG26', 'AG26-EU', 'AG24', 'AG24-EU')
      ) THEN 'Smart Trailers'
      WHEN (
        last_cable_id = 0
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
        AND asset_type_name = 'Trailer'
      ) THEN 'Smart Trailers'
      WHEN (
        last_cable_id = 20
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
      )
      OR (
        last_cable_id = 2
        AND COALESCE(num_canbus_stats, 0) = 0
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
      ) THEN 'Connected Equipment'
      WHEN (
        last_cable_id = 0
        AND product_name IN ('AG46P', 'AG46P-EU', 'AG52', 'AG52-EU', 'AG53', 'AG53-EU')
        AND asset_type_name = 'Equipment'
      ) THEN 'Connected Equipment'
      WHEN (
        last_cable_id = 2
        AND product_name IN ('AG26', 'AG26-EU', 'AG24', 'AG24-EU')
        AND COALESCE(num_canbus_stats, 0) = 0
      ) THEN 'Connected Equipment'
      WHEN (
        last_cable_id = 2
        AND COALESCE(num_canbus_stats, 0) > 0
        AND product_name IN ('AG24', 'AG24-EU', 'AG26', 'AG26-EU')
      ) THEN 'Connected Equipment'
      WHEN (
        last_cable_id = 0
        AND product_name IN ('AG26', 'AG26-EU', 'AG24', 'AG24-EU')
        AND COALESCE(num_canbus_stats, 0) > 0
        AND asset_type_name = 'Equipment'
      ) THEN 'Connected Equipment'
      WHEN product_name IN (
        'AG21',
        'AG45',
        'AG45-EU',
        'AG46',
        'AG46-EU',
        'AG51',
        'AG51-EU'
      ) THEN 'Connected Equipment'
      WHEN product_name IN (
        'EM21',
        'EM22',
        'EM23',
        'EM31',
        'ACC-CRGO',
        'ACC-DM11'
      ) THEN 'Smart Trailers'
      ELSE 'Other'
    END AS asset_type_segment,
cable_history.last_cable_id,
cable_history.last_cable_id_time,
canbus_stat_history.num_canbus_stats,
gateway_mapping.gateway_id,
weekly_availability_ms,
oem_types.name AS oem_name
FROM main
LEFT OUTER JOIN cable_history
ON (main.device_id = cable_history.device_id)
LEFT OUTER JOIN canbus_stat_history
ON (main.device_id = canbus_stat_history.device_id)
LEFT OUTER JOIN gateway_mapping
ON (main.device_id = gateway_mapping.device_id)
LEFT OUTER JOIN availability
ON (main.device_id = availability.device_id
    AND main.org_id = availability.org_id)
LEFT OUTER JOIN oemdb_shards.oem_sources oem_sources
ON (main.device_id = oem_sources.device_id
    AND main.org_id = oem_sources.org_id)
LEFT OUTER JOIN definitions.oem_type_name oem_types
ON (oem_sources.oem_type = oem_types.id)
"""


dim_devices_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for organizations. An account has multiple organizations under it. Joins to datamodel_core.dim_organizations"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for devices. Joins to datamodel_core.dim_devices"
        },
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": """Attribute defining the device type (e.g. VG, CM, AG, ...).
            CMs will also include product name values from their parent devices where they are denoted as cameras instead of devices"""
        },
    },
    {
        "name": "variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The product variant of the device. Join to datamodel_core.dim_product_variants for more variant information"
        },
    },
    {
        "name": "associated_vehicle_make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Make of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Model of the vehicle associated with the device"},
    },
    {
        "name": "associated_vehicle_year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Year of the vehicle associated with the device"},
    },
    {
        "name": "associated_devices",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "string",
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Map describing devices associated with the device_id. For example the VG associated with the CM or vice versa"
        },
    },
    {
        "name": "associated_devices_array",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "device_id", "type": "string", "nullable": True},
                    {"name": "serial", "type": "string", "nullable": True},
                    {"name": "product_id", "type": "string", "nullable": True},
                    {"name": "device_type", "type": "string", "nullable": True},
                    {"name": "variant_id", "type": "string", "nullable": True},
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of structs describing devices associated with the device_id. Each struct contains device_id, serial, product_id, device_type, and variant_id"
        },
    },
    {
        "name": "associated_vehicle_engine_model",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine model of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_primary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Primary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_secondary_fuel_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Secondary fuel type of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_engine_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine type of the vehicle associated with the device (BEV/Hybrid/PHEV/ICE)"
        },
    },
    {
        "name": "associated_vehicle_trim",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Trim of the vehicle associated with the device",
        },
    },
    {
        "name": "associated_vehicle_engine_manufacturer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Engine manufacturer of the vehicle associated with the device"
        },
    },
    {
        "name": "associated_vehicle_max_weight_lbs",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Maximum weight of the vehicle"},
    },
    {
        "name": "associated_vehicle_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Weight rating of the vehicle"},
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the device"},
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of the product. Sourced from definitions.products"
        },
    },
    {
        "name": "asset_type",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Used to determine if the asset is Trailer, Equipment, Unpowered, Vehicle"
        },
    },
    {
        "name": "asset_type_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Based on asset_type. 1 = 'Trailer' 2 = 'Equipment' 3 = 'Unpowered' 4 = 'Vehicle' 0 = 'None'"
        },
    },
    {
        "name": "asset_type_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Used last_cable_id, num_canbus_stats and product_name to determine more granular asset type attributes"
        },
    },
    {
        "name": "last_cable_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdobdcableid, last logged cable_id"
        },
    },
    {
        "name": "last_cable_id_time",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdobdcableid, date of the last logged cable_id"
        },
    },
    {
        "name": "num_canbus_stats",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Based on kinesisstats_history.osdcanbustype. Has the device ever logged canbus stats"
        },
    },
    {
        "name": "gateway_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Id of gateway on date matching device_id sourced from productsdb.gateways"
        },
    },
    {
        "name": "weekly_availability_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Availability in milliseconds that the asset (ex: vehicle) is able to be utilized, as determined by the customer with a default value equivalent to 12 hours per day. For instance, some assets may be available for 12 hours per day and others for 24 hours per day depending on the asset."
        },
    },
    {
        "name": "oem_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of corresponding OEM carrier for device (if exists)."
        },
    },
]


dim_devices_query = """

SELECT
date,
org_id,
device_id,
serial,
product_id,
device_type,
variant_id,
associated_vehicle_make,
associated_vehicle_model,
associated_vehicle_year,
associated_devices,
associated_devices_array,
associated_vehicle_engine_model,
associated_vehicle_primary_fuel_type,
associated_vehicle_secondary_fuel_type,
associated_vehicle_engine_type,
associated_vehicle_trim,
associated_vehicle_engine_manufacturer,
associated_vehicle_max_weight_lbs,
associated_vehicle_gross_vehicle_weight_rating,
device_name,
product_name,
asset_type,
asset_type_name,
asset_type_segment,
last_cable_id,
last_cable_id_time,
num_canbus_stats,
gateway_id,
weekly_availability_ms,
oem_name
FROM {database_gold_dev}.dim_devices_fast
WHERE date = '{DATEID}'
"""


dim_devices_sensitive_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "date of table snapshot"},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for organizations. An account has multiple organizations under it. Joins to datamodel_core.dim_organizations"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for devices. Joins to datamodel_core.dim_devices"
        },
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique serial number identifier for each device"},
    },
    {
        "name": "associated_vehicle_vin",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Vehicle identification number (VIN) of the vehicle associated with the device. Considered PII"
        },
    },
]


dim_devices_sensitive_query = """

SELECT
    '{DATEID}' AS date,
    org_id,
    device_id,
    serial,
    associated_vehicle_vin
FROM {database_silver_dev}.stg_vg_devices
WHERE date = '{DATEID}'

UNION ALL

SELECT
    '{DATEID}' AS date,
    org_id,
    device_id,
    serial,
    associated_vehicle_vin
FROM {database_silver_dev}.stg_non_vg_devices
WHERE date = '{DATEID}'
"""
assets = build_assets_from_sql(
    name="raw_productsdb_gateways",
    sql_query=raw_productsdb_gateways_query,
    description="""Retrieves all gateway data as of that time""",
    primary_keys=["date", "device_id", "id"],
    upstreams=[],
    schema=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

raw_productsdb_gateways_1 = assets[AWSRegions.US_WEST_2.value]
raw_productsdb_gateways_2 = assets[AWSRegions.EU_WEST_1.value]
raw_productsdb_gateways_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["raw_productsdb_gateways"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_raw_productsdb_gateways",
        table="raw_productsdb_gateways",
        primary_keys=["date", "device_id", "id"],
        blocking=True,
        database=databases["database_bronze_dev"],
    )
)

assets = build_assets_from_sql(
    name="raw_productdb_devices",
    schema=raw_productdb_devices_schema,
    sql_query=raw_productdb_devices_query,
    description="""Retrieves all devices data as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_productdb_devices_1 = assets[AWSRegions.US_WEST_2.value]
raw_productdb_devices_2 = assets[AWSRegions.EU_WEST_1.value]
raw_productdb_devices_3 = assets[AWSRegions.CA_CENTRAL_1.value]

assets = build_assets_from_sql(
    name="raw_vindb_shards_device_vin_metadata",
    schema=None,
    sql_query=get_raw_vindb_shards_device_vin_metadata_query(
        AWSRegions.US_WEST_2.value
    ),
    description="""Retrieves all VIN metadata as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_vindb_shards_device_vin_metadata_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_vindb_shards_device_vin_metadata",
    schema=None,
    sql_query=get_raw_vindb_shards_device_vin_metadata_query(
        AWSRegions.EU_WEST_1.value
    ),
    description="""Retrieves all VIN metadata as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_vindb_shards_device_vin_metadata_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_vindb_shards_device_vin_metadata",
    schema=None,
    sql_query=get_raw_vindb_shards_device_vin_metadata_query(
        AWSRegions.CA_CENTRAL_1.value
    ),
    description="""Retrieves all VIN metadata as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_vindb_shards_device_vin_metadata_3 = assets[AWSRegions.CA_CENTRAL_1.value]

assets = build_assets_from_sql(
    name="raw_fueldb_shards_fuel_types",
    schema=None,
    sql_query=get_raw_fueldb_shards_fuel_types_query(AWSRegions.US_WEST_2.value),
    description="""Retrieves all fuel type data as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_fueldb_shards_fuel_types_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_fueldb_shards_fuel_types",
    schema=None,
    sql_query=get_raw_fueldb_shards_fuel_types_query(AWSRegions.EU_WEST_1.value),
    description="""Retrieves all fuel type data as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_fueldb_shards_fuel_types_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_fueldb_shards_fuel_types",
    schema=None,
    sql_query=get_raw_fueldb_shards_fuel_types_query(AWSRegions.CA_CENTRAL_1.value),
    description="""Retrieves all fuel type data as of that time""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_fueldb_shards_fuel_types_3 = assets[AWSRegions.CA_CENTRAL_1.value]

assets = build_assets_from_sql(
    name="stg_vg_devices_step_1",
    schema=stg_vg_devices_step_1_schema,
    sql_query=stg_vg_devices_step_1_query,
    description="""Grabs vehicle data for VG devices""",
    primary_keys=["date", "device_id"],
    upstreams=[
        AssetKey([databases["database_bronze_dev"], "raw_productdb_devices"]),
        AssetKey(
            [databases["database_bronze_dev"], "raw_vindb_shards_device_vin_metadata"]
        ),
        AssetKey([databases["database_bronze_dev"], "raw_fueldb_shards_fuel_types"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_vg_devices_step_1_1 = assets[AWSRegions.US_WEST_2.value]
stg_vg_devices_step_1_2 = assets[AWSRegions.EU_WEST_1.value]
stg_vg_devices_step_1_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="stg_non_vg_devices",
    schema=stg_non_vg_devices_schema,
    sql_query=stg_non_vg_devices_query,
    description="""Grabs vehicle data for non-VG devices""",
    primary_keys=["date", "device_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "stg_vg_devices_step_1"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_non_vg_devices_1 = assets[AWSRegions.US_WEST_2.value]
stg_non_vg_devices_2 = assets[AWSRegions.EU_WEST_1.value]
stg_non_vg_devices_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["stg_non_vg_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_non_vg_devices",
        table="stg_non_vg_devices",
        primary_keys=["date", "device_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


assets = build_assets_from_sql(
    name="stg_vg_devices",
    schema=stg_vg_devices_schema,
    sql_query=stg_vg_devices_query,
    description="""Grabs all device data for VG devices""",
    primary_keys=["date", "device_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_non_vg_devices"])],
    regions=[
        AWSRegions.US_WEST_2.value,
        AWSRegions.EU_WEST_1.value,
        AWSRegions.CA_CENTRAL_1.value,
    ],
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_vg_devices_1 = assets[AWSRegions.US_WEST_2.value]
stg_vg_devices_2 = assets[AWSRegions.EU_WEST_1.value]
stg_vg_devices_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["stg_vg_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_vg_devices",
        table="stg_vg_devices",
        primary_keys=["date", "device_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

assets = build_assets_from_sql(
    name="stg_vg_multicam_cm_devices",
    schema=stg_vg_multicam_cm_devices_schema,
    sql_query=stg_vg_multicam_cm_devices_query,
    description="""Grabs all device data for VG/Multicam/CM groupings. This in turn is used for the associated_devices field downstream""",
    primary_keys=["date", "vg_device_id", "multicam_num"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_non_vg_devices"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_vg_devices"]),
    ],
    regions=[
        AWSRegions.US_WEST_2.value,
        AWSRegions.EU_WEST_1.value,
        AWSRegions.CA_CENTRAL_1.value,
    ],
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_vg_multicam_cm_devices_1 = assets[AWSRegions.US_WEST_2.value]
stg_vg_multicam_cm_devices_2 = assets[AWSRegions.EU_WEST_1.value]
stg_vg_multicam_cm_devices_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_vg_multicam_cm_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_vg_multicam_cm_devices",
        table="stg_vg_multicam_cm_devices",
        primary_keys=["date", "vg_device_id", "multicam_num"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

assets = build_assets_from_sql(
    name="dim_devices_fast",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of devices.
A device is a vehicle, an asset, or other unit tracked by Samsara. Example devices include:
    - Vehicle
    - Dash camera
    - Asset

Note that when a Samsara VG is uninstalled from a vehicle and moved to another vehicle,
the device_id stays the same for that vehicle,
and the device serial information on this table is what changes for that device_id

This table has a similar use case as datamodel_core.dim_devices, though with fewer columns and a faster landing time.
Please use this table if you are not dependent on additional columns that will appear in dim_devices""",
        row_meaning="""Has the same meaning as rows in datamodel_core.dim_devices
A unique device as recorded on a specific date.
This allows you to track and analyze devices over time.
If you only care about a single date,
filter for the devices on that date (or the latest date for the most up to date information)""",
        related_table_info={
            "datamodel_core.dim_devices": """gives you more columns, but will not be updated as quickly
This table lands 2-4 hours earlier than datamodel_core.dim_devices
                                        """
        },
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_devices_fast_schema,
    sql_query=dim_devices_fast_query,
    primary_keys=["date", "device_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_vg_devices"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_non_vg_devices"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_vg_multicam_cm_devices"]),
        AssetKey([databases["database_bronze_dev"], "dq_raw_productsdb_gateways"]),
    ],
    regions=[
        AWSRegions.US_WEST_2.value,
        AWSRegions.EU_WEST_1.value,
        AWSRegions.CA_CENTRAL_1.value,
    ],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_devices_fast_1 = assets[AWSRegions.US_WEST_2.value]
dim_devices_fast_2 = assets[AWSRegions.EU_WEST_1.value]
dim_devices_fast_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["dim_devices_fast"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_devices_fast",
        table="dim_devices_fast",
        primary_keys=["date", "device_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices_fast"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_devices_fast",
        table="dim_devices_fast",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices_fast"].append(
    NonNullDQCheck(
        name="dq_non_null_dim_devices_fast",
        database=databases["database_gold_dev"],
        table="dim_devices_fast",
        non_null_columns=["date", "device_id", "device_type"],
        blocking=True,
    )
)

dqs["dim_devices_fast"].append(
    SQLDQCheck(
        name="dq_no_unknown_device_types_dim_devices_fast",
        database=databases["database_gold_dev"],
        sql_query="""SELECT COUNT(*) AS observed_value
                     FROM {database_gold_dev}.dim_devices_fast
                     WHERE device_type = 'UNKNOWN'
                     """.format(
            DATEID="{DATEID}", database_gold_dev=databases["database_gold_dev"]
        ),
        expected_value=0,
        blocking=False,
    )
)

dqs["dim_devices_fast"].append(
    TrendDQCheck(
        name="dim_devices_fast_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="dim_devices_fast",
        tolerance=0.02,
        blocking=True,
    )
)

assets = build_assets_from_sql(
    name="dim_devices",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of devices.
A device is a vehicle, an asset, or other unit tracked by Samsara. Example devices include:
    - Vehicle
    - Dash camera
    - Asset

Note that when a Samsara VG is uninstalled from a vehicle and moved to another vehicle,
the device_id stays the same for that vehicle,
and the device serial information on this table is what changes for that device_id""",
        row_meaning="""A unique device as recorded on a specific date.
This allows you to track and analyze devices over time.
If you only care about a single date,
filter for the devices on that date (or the latest date for the most up to date information)""",
        related_table_info={
            "datamodel_core.dim_devices_fast": """gives you a subset of these columns and will be refreshed
more quickly (see datamodel_core.dim_devices_fast for more info)
                                        """
        },
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_devices_schema,
    sql_query=dim_devices_query,
    primary_keys=["date", "device_id"],
    upstreams=[AssetKey([databases["database_gold_dev"], "dq_dim_devices_fast"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_devices_1 = assets[AWSRegions.US_WEST_2.value]
dim_devices_2 = assets[AWSRegions.EU_WEST_1.value]
dim_devices_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["dim_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_devices",
        table="dim_devices",
        primary_keys=["date", "device_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_devices",
        table="dim_devices",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices"].append(
    NonNullDQCheck(
        name="dq_non_null_dim_devices",
        database=databases["database_gold_dev"],
        table="dim_devices",
        non_null_columns=["date", "device_id", "device_type"],
        blocking=True,
    )
)

dqs["dim_devices"].append(
    SQLDQCheck(
        name="dq_no_unknown_device_types_dim_devices",
        database=databases["database_gold_dev"],
        sql_query="""SELECT COUNT(*) AS observed_value
                     FROM {database_gold_dev}.dim_devices
                     WHERE device_type = 'UNKNOWN'
                     """.format(
            DATEID="{DATEID}", database_gold_dev=databases["database_gold_dev"]
        ),
        expected_value=0,
        blocking=False,
    )
)

dqs["dim_devices"].append(
    TrendDQCheck(
        name="dim_devices_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="dim_devices",
        tolerance=0.02,
        blocking=True,
    )
)


assets = build_assets_from_sql(
    name="dim_devices_sensitive",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of devices, but limited to a subset of columns
that are deemed sensitive because they contain PII.
Please treat this carefully and do not persist into downstream tables
without consulting the Data Engineering team.""",
        row_meaning="""Has the same meaning as rows in datamodel_core.dim_devices
A unique device as recorded on a specific date.
This allows you to track and analyze devices over time.
If you only care about a single date,
filter for the devices on that date (or the latest date for the most up to date information)""",
        related_table_info={
            "datamodel_core.dim_devices": """has fewer columns, but these columns are not as sensitive"""
        },
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_devices_sensitive_schema,
    sql_query=dim_devices_sensitive_query,
    primary_keys=["date", "device_id"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_vg_devices"])],
    regions=[
        AWSRegions.US_WEST_2.value,
        AWSRegions.EU_WEST_1.value,
        AWSRegions.CA_CENTRAL_1.value,
    ],
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_devices_sensitive_1 = assets[AWSRegions.US_WEST_2.value]
dim_devices_sensitive_2 = assets[AWSRegions.EU_WEST_1.value]
dim_devices_sensitive_3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["dim_devices_sensitive"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_devices_sensitive",
        table="dim_devices_sensitive",
        primary_keys=["date", "device_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices_sensitive"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_devices_sensitive",
        table="dim_devices_sensitive",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_devices_sensitive"].append(
    TrendDQCheck(
        name="dim_devices_sensitive_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="dim_devices_sensitive",
        tolerance=0.02,
        blocking=True,
    )
)

dq_assets = dqs.generate()
