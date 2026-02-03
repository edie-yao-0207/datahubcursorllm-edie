import boto3
from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
)
from slack import WebClient

from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.utils import (
    Database,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
)

databases_USE = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}


databases = {
    "database_bronze": Database.DATAMODEL_DEV,
    "database_silver": Database.DATAMODEL_DEV,
    "database_gold": Database.DATAMODEL_DEV,
}

dim_devices_job = define_asset_job(
    name="dim_devices",
    selection=AssetSelection.groups("dim_devices"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
dim_devices_schedule = build_schedule_from_partitioned_job(
    job=dim_devices_job,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=4,
)

dim_devices_job_eu = define_asset_job(
    name="dim_devices_eu",
    selection=AssetSelection.groups("dim_devices_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
dim_devices_schedule_eu = build_schedule_from_partitioned_job(
    job=dim_devices_job_eu,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=4,
)

dim_devices_job_ca = define_asset_job(
    name="dim_devices_ca",
    selection=AssetSelection.groups("dim_devices_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
dim_devices_schedule_ca = build_schedule_from_partitioned_job(
    job=dim_devices_job_ca,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=4,
)

dim_organizations_job = define_asset_job(
    "dim_organizations", AssetSelection.groups("dim_organizations")
)
dim_organizations_schedule = build_schedule_from_partitioned_job(
    job=dim_organizations_job,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=6,
)

dim_organizations_job_eu = define_asset_job(
    "dim_organizations_eu", AssetSelection.groups("dim_organizations_eu")
)
dim_organizations_schedule_eu = build_schedule_from_partitioned_job(
    job=dim_organizations_job_eu,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=6,
)

dim_organizations_job_ca = define_asset_job(
    "dim_organizations_ca", AssetSelection.groups("dim_organizations_ca")
)
dim_organizations_schedule_ca = build_schedule_from_partitioned_job(
    job=dim_organizations_job_ca,
    description="Daily partitioned schedule",
    minute_of_hour=15,
    hour_of_day=6,
)

dim_product_variants_job = define_asset_job(
    "dim_product_variants", AssetSelection.groups("dim_product_variants")
)
dim_product_variants_schedule = ScheduleDefinition(
    job=dim_product_variants_job,
    description="Daily schedule",
    cron_schedule="5 0 * * *",
)

dim_product_variants_job_eu = define_asset_job(
    "dim_product_variants_eu", AssetSelection.groups("dim_product_variants_eu")
)
dim_product_variants_schedule_eu = ScheduleDefinition(
    job=dim_product_variants_job_eu,
    description="Daily schedule",
    cron_schedule="5 0 * * *",
)

dim_product_variants_job_ca = define_asset_job(
    "dim_product_variants_ca", AssetSelection.groups("dim_product_variants_ca")
)
dim_product_variants_schedule_ca = ScheduleDefinition(
    job=dim_product_variants_job_ca,
    description="Daily schedule",
    cron_schedule="5 0 * * *",
)

dim_organizations_tenure_job = define_asset_job(
    name="dim_organizations_tenure",
    selection=AssetSelection.groups("dim_organizations_tenure"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_organizations_tenure_schedule = ScheduleDefinition(
    job=dim_organizations_tenure_job,
    description="Daily schedule",
    cron_schedule="00 8 * * *",
)

dim_organizations_tenure_job_eu = define_asset_job(
    name="dim_organizations_tenure_eu",
    selection=AssetSelection.groups("dim_organizations_tenure_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_organizations_tenure_schedule_eu = ScheduleDefinition(
    job=dim_organizations_tenure_job_eu,
    description="Daily schedule",
    cron_schedule="00 8 * * *",
)

dim_organizations_tenure_job_ca = define_asset_job(
    name="dim_organizations_tenure_ca",
    selection=AssetSelection.groups("dim_organizations_tenure_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_organizations_tenure_schedule_ca = ScheduleDefinition(
    job=dim_organizations_tenure_job_ca,
    description="Daily schedule",
    cron_schedule="00 8 * * *",
)

dim_organizations_settings_job = define_asset_job(
    "dim_organizations_settings",
    AssetSelection.groups("dim_organizations_settings"),
)
dim_organizations_settings_schedule = build_schedule_from_partitioned_job(
    job=dim_organizations_settings_job,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=7,
)

dim_organizations_settings_job_eu = define_asset_job(
    "dim_organizations_settings_eu",
    AssetSelection.groups("dim_organizations_settings_eu"),
)
dim_organizations_settings_schedule_eu = build_schedule_from_partitioned_job(
    job=dim_organizations_settings_job_eu,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=7,
)

dim_organizations_settings_job_ca = define_asset_job(
    "dim_organizations_settings_ca",
    AssetSelection.groups("dim_organizations_settings_ca"),
)
dim_organizations_settings_schedule_ca = build_schedule_from_partitioned_job(
    job=dim_organizations_settings_job_ca,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=7,
)

dim_devices_settings_job = define_asset_job(
    "dim_devices_settings", AssetSelection.groups("dim_devices_settings")
)
dim_devices_settings_schedule = build_schedule_from_partitioned_job(
    job=dim_devices_settings_job,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=13,
)

dim_devices_settings_job_eu = define_asset_job(
    "dim_devices_settings_eu",
    AssetSelection.groups("dim_devices_settings_eu"),
)
dim_devices_settings_schedule_eu = build_schedule_from_partitioned_job(
    job=dim_devices_settings_job_eu,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=13,
)

dim_devices_settings_job_ca = define_asset_job(
    "dim_devices_settings_ca",
    AssetSelection.groups("dim_devices_settings_ca"),
)
dim_devices_settings_schedule_ca = build_schedule_from_partitioned_job(
    job=dim_devices_settings_job_ca,
    description="Daily partitioned schedule",
    minute_of_hour=0,
    hour_of_day=13,
)


fct_gateway_device_intervals_job = define_asset_job(
    name="fct_gateway_device_intervals_job",
    selection=AssetSelection.groups("gateway_device_intervals"),
)

fct_gateway_device_intervals_eu_job = define_asset_job(
    name="fct_gateway_device_intervals_eu_job",
    selection=AssetSelection.groups("gateway_device_intervals_eu"),
)

fct_gateway_device_intervals_schedule = ScheduleDefinition(
    job=fct_gateway_device_intervals_job,
    description="Daily schedule",
    cron_schedule="0 7 * * *",
)

fct_gateway_device_intervals_eu_schedule = ScheduleDefinition(
    job=fct_gateway_device_intervals_eu_job,
    description="Daily schedule",
    cron_schedule="0 7 * * *",
)

fct_gateway_device_intervals_ca_job = define_asset_job(
    name="fct_gateway_device_intervals_ca_job",
    selection=AssetSelection.groups("gateway_device_intervals_ca"),
)

fct_gateway_device_intervals_ca_schedule = ScheduleDefinition(
    job=fct_gateway_device_intervals_ca_job,
    description="Daily schedule",
    cron_schedule="0 7 * * *",
)


@run_failure_sensor(
    monitored_jobs=[
        dim_devices_job,
        dim_devices_job_eu,
        dim_devices_job_ca,
        dim_product_variants_job,
        dim_product_variants_job_eu,
        dim_product_variants_job_ca,
        dim_organizations_job,
        dim_organizations_job_eu,
        dim_organizations_job_ca,
    ],
    description=(
        "Sensor that monitors and sends slack alerts for failed job run status"
    ),
    minimum_interval_seconds=900,
)
def slack_failure_alert(context):
    ssm = boto3.client("ssm", region_name="us-west-2")
    response = ssm.get_parameter(Name="DAGSTER_SLACK_BOT_TOKEN", WithDecryption=True)
    slack_token = response["Parameter"]["Value"]
    client = WebClient(token=slack_token)
    client.chat_postMessage(
        channel=f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}",
        text=(
            f"Dim Devices events Dagster pipeline failed for run "
            f"{context.dagster_run}"
        ),
    )
    return
