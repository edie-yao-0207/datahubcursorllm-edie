from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import (
    DataModelCore,
    DataModelCoreBronze,
    DataPrepFirmware,
    Definitions,
    ProductAnalytics,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.GATEWAY_ID,
    ColumnType.INTERNAL_TYPE,
    ColumnType.PRODUCT_ID,
    ColumnType.VARIANT_ID,
    ColumnType.QUARANTINE_ENABLED,
    ColumnType.TOTAL_DISTANCE_METERS,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.PRIMARY_FUEL_TYPE,
    ColumnType.SECONDARY_FUEL_TYPE,
    ColumnType.ELECTRIFICATION_LEVEL,
    ColumnType.ROLLOUT_STAGE_ID,
    ColumnType.PRODUCT_GROUP_ID,
    ColumnType.PRODUCT_PROGRAM_ID,
    ColumnType.FIRMWARE_PROGRAM_ENROLLMENT_TYPE,
    ColumnType.SERIAL,
    ColumnType.HAS_MODI,
    ColumnType.HAS_OCTO,
    ColumnType.BUILD,
    ColumnType.CABLE_ID,
    ColumnType.PRODUCT_VERSION_STR,
    ColumnType.LOCALE,
    ColumnType.ENGINE_HP,
    ColumnType.ASSET_SUBTYPE,
    ColumnType.HAS_CM2X,
    ColumnType.HAS_CM3X,
    ColumnType.HAS_FALKO,
    ColumnType.HAS_J1708_USB,
    ColumnType.HAS_USB_SERIAL,
    ColumnType.HAS_BRIGID,
    ColumnType.BOM_VERSION,
    ColumnType.PRODUCT_NAME,
    ColumnType.CABLE_NAME,
    ColumnType.ENGINE_TYPE,
    ColumnType.FUEL_TYPE,
    ColumnType.VARIANT_NAME,
    ColumnType.MARKET,
    ColumnType.ASSET_TYPE_NAME,
    ColumnType.ASSET_TYPE_SEGMENT,
    ColumnType.INTERNAL_TYPE_NAME,
    ColumnType.TIMESTAMP_LAST_CABLE_ID,
    ColumnType.INCLUDE_IN_AGG_METRICS,
    ColumnType.DIAGNOSTICS_CAPABLE,
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

SELECT
    device.date
    , device.org_id
    , device.device_id
    , device.gateway_id
    , org.internal_type
    , device.product_id
    , device.variant_id
    , CAST(org.quarantine_enabled AS BYTE) AS quarantine_enabled
    , qualified_trips.total_distance_meters AS total_distance_meters
    , device.associated_vehicle_make AS make
    , device.associated_vehicle_model AS model
    , device.associated_vehicle_year AS year
    , device.associated_vehicle_engine_model AS engine_model
    , device.associated_vehicle_primary_fuel_type AS primary_fuel_type
    , device.associated_vehicle_secondary_fuel_type AS secondary_fuel_type
    , vin.electrification_level
    , vin.engine_hp
    , vin.body_class AS asset_subtype
    , stage.rollout_stage_id
    , stage.product_group_id
    , stage.product_program_id
    , stage.product_program_id_type AS firmware_program_enrollment_type
    , device.serial
    , usb.has_modi
    , usb.has_octo
    , build.build
    , device.last_cable_id as cable_id
    , hwinfo.product_version_str
    , org.locale
    , COALESCE(usb.has_cm2x, FALSE) as has_cm2x
    , COALESCE(usb.has_cm3x, FALSE) as has_cm3x
    , COALESCE(usb.has_falko, FALSE) as has_falko
    , COALESCE(usb.has_j1708_usb, FALSE) as has_j1708_usb
    , COALESCE(usb.has_usb_serial, FALSE) as has_usb_serial
    , COALESCE(usb.has_brigid, FALSE) as has_brigid
    , hwinfo.bom_version
    , device.product_name
    , product.cable_name
    , fuel_type.engine_type
    , fuel_type.fuel_type
    , platform_variant.variant_name

    , (
        CASE
            WHEN UPPER(org.locale) in ("MX") then UPPER(org.locale)
            WHEN UPPER(org.locale) in ("CA", "US") then "NA"
            ELSE "EMEA"
        END
    ) AS market

    , device.asset_type_name
    , device.asset_type_segment
    , internal_type.name AS internal_type_name
    , timestamp_millis(device.last_cable_id_time) as timestamp_last_cable_id
    -- Exclude virtual devices and devices which have not driven today from aggregate metrics
    , COALESCE(qualified_trips.qualified, FALSE) AND NOT COALESCE(build.build, '') ILIKE "%virtualdevice%" AS include_in_agg_metrics
    , product.diagnostics_capable

FROM
    datamodel_core.dim_devices AS device

INNER JOIN
    datamodel_core.dim_organizations AS org
    ON device.org_id = org.org_id

LEFT JOIN
    datamodel_core_bronze.raw_vindb_shards_device_vin_metadata AS vin
    ON device.date = vin.date
    AND device.org_id = vin.org_id
    AND device.device_id = vin.device_id

LEFT JOIN
    dataprep_firmware.gateway_daily_rollout_stages AS stage
    ON device.date = stage.date
    AND device.org_id = stage.org_id
    AND device.device_id = stage.device_id
    AND device.gateway_id = stage.gateway_id

LEFT JOIN
    {product_analytics}.dim_attached_usb_devices AS usb
    ON device.date = usb.date
    AND device.org_id = usb.org_id
    AND device.device_id = usb.device_id

LEFT JOIN
    {product_analytics}.dim_firmware_builds AS build
    ON device.date = build.date
    AND device.org_id = build.org_id
    AND device.device_id = build.device_id

LEFT JOIN
    {product_analytics_staging}.agg_qualified_device_trips AS qualified_trips
    ON device.date = qualified_trips.date
    AND device.org_id = qualified_trips.org_id
    AND device.device_id = qualified_trips.device_id

LEFT JOIN
    {product_analytics_staging}.stg_device_hwinfo AS hwinfo
    ON device.date == hwinfo.date
    AND device.org_id == hwinfo.org_id
    AND device.gateway_id == hwinfo.gateway_id

LEFT JOIN
    {product_analytics_staging}.stg_daily_fuel_type AS fuel_type
    ON device.date = fuel_type.date
    AND device.org_id = fuel_type.org_id
    AND device.device_id = fuel_type.device_id

LEFT JOIN
    {product_analytics_staging}.fct_telematics_products AS product
    ON device.product_id = product.product_id
    AND device.last_cable_id = product.cable_id

LEFT JOIN
    datamodel_core.dim_product_variants AS platform_variant
    ON device.product_id = platform_variant.product_id
    AND device.variant_id = platform_variant.variant_id

LEFT JOIN
    definitions.organization_internal_types AS internal_type
    ON org.internal_type = internal_type.id

WHERE
    device.date BETWEEN "{date_start}" AND "{date_end}"
    AND device.org_id NOT IN (0, 1)
    -- Ignore missing products for now. There are around 15 devices per
    -- day with no mapping
    AND platform_variant.variant_name IS NOT NULL
    -- Use the latest data available from dim_organizations. This doesn't have to exactly
    -- match the date from dim_devices as we're only using this data to get device locale
    -- information.
    AND org.date == (select max(date) from datamodel_core.dim_organizations)
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Aggregate all device dimensions to a single table. This provides install and customer context for downstream computation.",
        row_meaning="All device dimensions for a given device",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(upstream)
        for upstream in [
            ProductAnalytics.DIM_ATTACHED_USB_DEVICES,
            ProductAnalyticsStaging.STG_DEVICE_HWINFO,
            ProductAnalytics.DIM_FIRMWARE_BUILDS,
            DataModelCore.DIM_DEVICES,
            DataModelCore.DIM_ORGANIZATIONS,
            DataModelCoreBronze.RAW_VINDB_SHARDS_DEVICE_VIN_METADATA,
            DataPrepFirmware.GATEWAY_DAILY_ROLLOUT_STAGES,
            ProductAnalyticsStaging.STG_DAILY_FUEL_TYPE,
            DataModelCore.DIM_PRODUCT_VARIANTS,
            Definitions.ORGANIZATION_INTERNAL_TYPES,
            ProductAnalyticsStaging.FCT_TELEMATICS_PRODUCTS,
            ProductAnalyticsStaging.AGG_QUALIFIED_DEVICE_TRIPS,
        ]
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    # This appears to be superseded by the backfill_batch_size in the schedule definition.
    backfill_batch_size=1,
    # autoBroadcastJoinThreshold is set to -1 to prevent Spark from attempting to broadcast
    # a table that exceeds the ByteBuffer maximum capacity of 2 GB. Interstingly, the error
    # that one may see without this configuration is the following (and not the standard
    # org.apache.spark.sql.execution.OutOfMemorySparkException: Size of broadcasted table far exceeds estimates and exceeds limit):
    # ----
    # py4j.protocol.Py4JJavaError: An error occurred while calling o543.saveAsTable: java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException: Initial capacity 730219499 exceeds maximum capacity of 536870912
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        min_workers=4,
        max_workers=16,
        spark_conf_overrides={
            "spark.sql.autoBroadcastJoinThreshold": "-1",
        },
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.DIM_DEVICE_DIMENSIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    priority=5,
    max_retries=5,
)
def dim_device_dimensions(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
