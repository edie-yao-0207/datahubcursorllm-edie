from dagster import AssetExecutionContext, DailyPartitionsDefinition
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
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "org_id"]


SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "severe_speeding_enabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether org has severe speeding alerts enabled or not"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding violations"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit_enum",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding violations (enum)"
        },
    },
    {
        "name": "severe_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": True,
        "metadata": {
            "comment": "Speed threshold at which to set off severe speeding alerts"
        },
    },
    {
        "name": "severe_speeding_bucket",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Represents which type of limits are set for Severe Speeding (MPH or percentage, and whether it's using the default or less/greater than default). If nothing is set, the default is ALL (which is an umbrella group for all cohorts)."
        },
    },
    {
        "name": "mobile_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has mobile usage alerts enabled or not"},
    },
    {
        "name": "mobile_usage_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for mobile usage violations or not"
        },
    },
    {
        "name": "mobile_usage_minimum_speed_mph",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Minimum speed (in MPH) at which to set off mobile usage violations"
        },
    },
    {
        "name": "mobile_usage_bucket",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Represents which limits are set for mobile usage (based on in-cab alerts and speed threshold). If not enabled, the default is ALL (which is an umbrella group for all cohorts)."
        },
    },
    {
        "name": "inattentive_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has inattentive driving alerts enabled or not"},
    },
    {
        "name": "inattentive_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for inattentive driving violations or not"
        },
    },
    {
        "name": "inattentive_driving_minimum_speed_enum",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Minimum speed setting for triggering inattentive driving alerts (enum)"
        },
    },
    {
        "name": "inattentive_driving_bucket",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Represents which limits are set for inattentive driving (based on in-cab alerts and speed threshold). If not enabled, the default is ALL (which is an umbrella group for all cohorts)."
        },
    },
]

QUERY = """
with safety_settings as (
  select distinct org_id,
  severe_speeding_enabled,
  severity_settings_speed_over_limit_unit,
  severity_settings_speed_over_limit_unit_enum,
  severe_speeding_speed_over_limit_threshold,
  mobile_usage_enabled,
  mobile_usage_audio_alerts_enabled,
  mobile_usage_minimum_speed_mph,
  inattentive_driving_enabled,
  inattentive_driving_audio_alerts_enabled,
  inattentive_driving_minimum_speed_enum
  from product_analytics.dim_organizations_safety_settings
  where date = '{PARTITION_START}'
)
select
'{PARTITION_START}' as date,
org_id,
COALESCE(severe_speeding_enabled, false) AS severe_speeding_enabled,
severity_settings_speed_over_limit_unit,
severity_settings_speed_over_limit_unit_enum,
severe_speeding_speed_over_limit_threshold,
case
  when severe_speeding_enabled
  then
    case
    when severity_settings_speed_over_limit_unit_enum = 1 --percentage
      then --rounding as 30 isn't exactly 30 in the data
        case
          when round(severe_speeding_speed_over_limit_threshold * 100) < 30
          then 'LT_PERCENT_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold * 100) = 30
          then 'EQ_PERCENT_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold * 100) > 30
          then 'GT_PERCENT_LIMIT'
        end
    when severity_settings_speed_over_limit_unit_enum = 2 --miles per hour
      then
        case
          when round(severe_speeding_speed_over_limit_threshold) < 15
          then 'LT_MPH_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold) = 15
          then 'EQ_MPH_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold) > 15
          then 'GT_MPH_LIMIT'
        end
    when severity_settings_speed_over_limit_unit_enum = 3 --kilometers per hour
      then
        case
          when round(severe_speeding_speed_over_limit_threshold) < round((15 / 0.6213711922))
          then 'LT_MPH_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold) = round((15 / 0.6213711922))
          then 'EQ_MPH_LIMIT'
          when round(severe_speeding_speed_over_limit_threshold) > round((15 / 0.6213711922))
          then 'GT_MPH_LIMIT'
        end
    when severity_settings_speed_over_limit_unit_enum = 4 -- milliknots per hour
      then --round to nearest 10s for precision
      case
        when round(severe_speeding_speed_over_limit_threshold, -1) < round((15 * 0.868976 * 1000), -1)
        then 'LT_MPH_LIMIT'
        when round(severe_speeding_speed_over_limit_threshold, -1) = round((15 * 0.868976 * 1000), -1)
        then 'EQ_MPH_LIMIT'
        when round(severe_speeding_speed_over_limit_threshold, -1) > round((15 * 0.868976 * 1000), -1)
        then 'GT_MPH_LIMIT'
      end
    end
  else --severe speeding is disabled, assign null
    null
  end as severe_speeding_bucket,
  COALESCE(mobile_usage_enabled, FALSE) AS mobile_usage_enabled,
  mobile_usage_audio_alerts_enabled,
  mobile_usage_minimum_speed_mph,
  case
    when mobile_usage_enabled
      then
      case
        when mobile_usage_audio_alerts_enabled and mobile_usage_minimum_speed_mph < 10
          then 'ALERTS_ON_LT_MPH_LIMIT'
        when mobile_usage_audio_alerts_enabled and mobile_usage_minimum_speed_mph >= 10
          then 'ALERTS_ON_GTE_MPH_LIMIT'
        when not mobile_usage_audio_alerts_enabled and mobile_usage_minimum_speed_mph < 10
          then 'ALERTS_OFF_LT_MPH_LIMIT'
        when not mobile_usage_audio_alerts_enabled and mobile_usage_minimum_speed_mph >= 10
          then 'ALERTS_OFF_GTE_MPH_LIMIT'
      end
    else -- mobile usage is disabled, assign null
      null
    end as mobile_usage_bucket,
COALESCE(inattentive_driving_enabled, FALSE) AS inattentive_driving_enabled,
inattentive_driving_audio_alerts_enabled,
inattentive_driving_minimum_speed_enum,
case
    when inattentive_driving_enabled
      then
      case
        when inattentive_driving_audio_alerts_enabled and inattentive_driving_minimum_speed_enum = 1 -- 0 MPH
          then 'ALERTS_ON_LT_MPH_LIMIT'
        when inattentive_driving_audio_alerts_enabled and (inattentive_driving_minimum_speed_enum = 0 or inattentive_driving_minimum_speed_enum > 1) -- 10 MPH and above
          then 'ALERTS_ON_GTE_MPH_LIMIT'
        when inattentive_driving_audio_alerts_enabled and (inattentive_driving_minimum_speed_enum is null) -- Take default
          then 'ALERTS_ON_GTE_MPH_LIMIT'
        when not inattentive_driving_audio_alerts_enabled and inattentive_driving_minimum_speed_enum = 1 -- 0 MPH
          then 'ALERTS_OFF_LT_MPH_LIMIT'
        when not inattentive_driving_audio_alerts_enabled and (inattentive_driving_minimum_speed_enum = 0 or inattentive_driving_minimum_speed_enum > 1) -- 10 MPH and above
          then 'ALERTS_OFF_GTE_MPH_LIMIT'
        when not inattentive_driving_audio_alerts_enabled and (inattentive_driving_minimum_speed_enum is null) -- Take default
          then 'ALERTS_OFF_GTE_MPH_LIMIT'
      end
    else -- inattentive driving is disabled, assign null
      null
    end as inattentive_driving_bucket
from safety_settings
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""This table stores configurable metric buckets for any configurable metrics in the org settings table.""",
        row_meaning="""Each row shows the setting metadata along with which bucket it belongs to for a given org""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_config_buckets"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_config_buckets",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_config_buckets",
            non_null_columns=["date", "org_id"],
            block_before_write=True,
        ),
    ],
    upstreams=[
        "product_analytics.dim_organizations_safety_settings",
    ],
)
def stg_organization_config_buckets(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_organization_config_buckets")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    df = spark.sql(query)
    context.log.info(f"{query}")
    return df
