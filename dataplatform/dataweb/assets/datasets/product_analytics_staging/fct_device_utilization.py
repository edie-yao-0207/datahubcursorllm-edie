from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "device_id"]

SCHEMA =  [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.ORG_ID},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DEVICE_ID},
    },
    {
        "name": "total_available_hours",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Sum of available hours as configured by customer, with a default of 12 hours per week, summed per device per day."},
    },
    {
        "name": "total_utilized_hours",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Sum of trip hours within lookback window for vehicle type devices."},
    },
    {
        "name": "utilization_percent",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Percentage quotient of total utilized hours over total available hours for the device and lookback window."},
    },
]

QUERY = """
WITH
   dates as (
    SELECT EXPLODE(SEQUENCE(to_date('{PARTITION_START}'), to_date('{PARTITION_END}'), INTERVAL 1 DAY)) AS date),
    devices_pre AS (
    SELECT
    date,
    org_id,
    device_id,
    coalesce(weekly_availability_ms/ 25200000.0, 12) AS daily_available_hours
    FROM datamodel_core.dim_devices
    WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
    AND gateway_id IS NOT NULL
    AND asset_type_name = 'Vehicle'),
    devices AS (
      SELECT devices_pre.* EXCEPT(date),
             dates.date,
             2 as test_var
      FROM devices_pre
      LEFT JOIN dates ON 1=1),
    trips AS (
    SELECT date,
        device_id,
        org_id,
        sum(end_time_ms - start_time_ms) / (3600 * 1000) AS trip_hours
    FROM datamodel_telematics.fct_trips
    WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    GROUP BY ALL),
  daily_settings_pre AS (
  SELECT OrgId as org_id,
        SPLIT(EntityId, 'DEVICE#')[1] AS device_id,
        `_approximate_creation_date_time` AS created_at,
        Settings AS settings
  FROM dynamodb.settings
  WHERE EntityId like 'DEVICE#%'
  AND CAST(Settings AS STRING) ilike '%weeklyavailability%'),
  daily_settings_ranked AS (
  SELECT *,
        FROM_JSON(settings, 'STRUCT<
        weeklyAvailability: STRUCT<
          friday: BIGINT,
          monday: BIGINT,
          saturday: BIGINT,
          sunday: BIGINT,
          thursday: BIGINT,
          tuesday: BIGINT,
          wednesday: BIGINT>>').weeklyAvailability as weekly_availability,
        RANK() OVER (PARTITION BY org_id, device_id ORDER BY created_at DESC) AS rnk
  FROM daily_settings_pre
  WHERE device_id IS NOT NULL),
  daily_availability AS (
  SELECT org_id,
        device_id,
        weekly_availability,
  ARRAY(
   CAST(CASE WHEN weekly_availability.sunday > 0 THEN weekly_availability.sunday / 360000.0 ELSE 0 END AS DOUBLE),
   CAST(CASE WHEN weekly_availability.monday > 0 THEN weekly_availability.monday / 3600000.0 ELSE 0 END AS DOUBLE),
   CAST(CASE WHEN weekly_availability.tuesday > 0 THEN weekly_availability.tuesday / 3600000.0 ELSE 0 END AS DOUBLE),
   CAST(CASE WHEN weekly_availability.wednesday > 0 THEN weekly_availability.wednesday / 3600000.0 ELSE 0 END AS DOUBLE),
   CAST(CASE WHEN weekly_availability.thursday > 0 THEN weekly_availability.thursday / 3600000.0 ELSE 0 END AS DOUBLE),
   CAST(CASE WHEN weekly_availability.friday > 0 THEN weekly_availability.friday / 3600000.0 ELSE 0 END AS DOUBLE),
    CAST(CASE WHEN weekly_availability.saturday > 0 THEN weekly_availability.saturday / 3600000.0 ELSE 0 END AS DOUBLE)) AS weekly_available
  FROM daily_settings_ranked
  WHERE rnk = 1)


   SELECT CAST(devices.date AS STRING) AS date,
        devices.org_id,
        devices.device_id,
        CAST(COALESCE(MAX(daily_availability.weekly_available[DAYOFWEEK(devices.date) - 1]),(MAX(devices.daily_available_hours))) AS DOUBLE) AS total_available_hours,
        CAST(coalesce(sum(trips.trip_hours),0) AS DOUBLE) as total_utilized_hours,
        CASE WHEN CAST(coalesce(sum(trips.trip_hours),0) AS DOUBLE) > 0 AND
                  CAST(COALESCE(MAX(daily_availability.weekly_available[DAYOFWEEK(devices.date) - 1]),(MAX(devices.daily_available_hours))) AS DOUBLE)
                  = 0
              THEN 100.0
              ELSE CAST(COALESCE(round(100.0 * COALESCE(sum(trips.trip_hours),0) /
                  COALESCE(MAX(daily_availability.weekly_available[DAYOFWEEK(devices.date) - 1]),(MAX(devices.daily_available_hours))), 2),0) AS DOUBLE) END AS utilization_percent
  FROM devices
  LEFT OUTER JOIN daily_availability ON
    daily_availability.org_id = devices.org_id
    AND daily_availability.device_id = devices.device_id
  LEFT OUTER JOIN trips ON
    trips.org_id = devices.org_id
    AND trips.device_id = devices.device_id
    AND trips.date = devices.date
  GROUP BY ALL
  ORDER BY 1 ASC, 3 ASC


"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Regional) Dataset for daily utilized hours and available hours by device and organization.""",
        row_meaning="""One row per device per date.""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "datamodel_core.dim_devices",
        "datamodel_telematics.fct_trips",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_device_utilization"),
        NonNullDQCheck(name="dq_non_null_device_utilization", non_null_columns=["total_available_hours", "total_utilized_hours"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_device_utilization", primary_keys=PRIMARY_KEYS, block_before_write=True)
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def fct_device_utilization(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
