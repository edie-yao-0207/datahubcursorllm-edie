import boto3
from dagster import AssetKey, AssetSelection, define_asset_job, run_failure_sensor
from slack import WebClient

from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.utils import (
    AWSRegions,
    Database,
    apply_db_overrides,
    build_backfill_schedule,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
    date_range_from_current_date,
)

active_devices_job = define_asset_job(
    name="active_devices",
    selection=AssetSelection.groups("active_devices"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

databases = {
    "database_silver": Database.DATAMODEL_CORE_SILVER,
}

database_dev_overrides = {
    "database_silver_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

stg_device_activity_daily = AssetKey(
    [
        AWSRegions.US_WEST_2.value,
        databases["database_silver_dev"],
        "stg_device_activity_daily",
    ]
)

stg_device_activity_daily_job = define_asset_job(
    name="stg_device_activity_daily",
    selection=AssetSelection.keys(stg_device_activity_daily),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

active_devices_backfill_schedule = build_backfill_schedule(
    job=stg_device_activity_daily_job,
    cron="00 09 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)

active_devices_job_eu = define_asset_job(
    name="active_devices_eu",
    selection=AssetSelection.groups("active_devices_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

stg_device_activity_daily_eu = AssetKey(
    [
        AWSRegions.EU_WEST_1.value,
        databases["database_silver_dev"],
        "stg_device_activity_daily",
    ]
)

stg_device_activity_daily_eu_job = define_asset_job(
    name="stg_device_activity_daily_eu",
    selection=AssetSelection.keys(stg_device_activity_daily_eu),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

active_devices_eu_backfill_schedule = build_backfill_schedule(
    job=stg_device_activity_daily_eu_job,
    cron="00 09 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)

active_devices_job_ca = define_asset_job(
    name="active_devices_ca",
    selection=AssetSelection.groups("active_devices_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

stg_device_activity_daily_ca = AssetKey(
    [
        AWSRegions.CA_CENTRAL_1.value,
        databases["database_silver_dev"],
        "stg_device_activity_daily",
    ]
)

stg_device_activity_daily_ca_job = define_asset_job(
    name="stg_device_activity_daily_ca",
    selection=AssetSelection.keys(stg_device_activity_daily_ca),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

active_devices_ca_backfill_schedule = build_backfill_schedule(
    job=stg_device_activity_daily_ca_job,
    cron="00 09 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)


@run_failure_sensor(
    monitored_jobs=[active_devices_job, active_devices_job_eu, active_devices_job_ca],
    description="Sensor that monitors and sends slack alerts for failed job run status",
    minimum_interval_seconds=900,
)
def slack_failure_alert(context):
    ssm = boto3.client("ssm", region_name="us-west-2")
    response = ssm.get_parameter(Name="DAGSTER_SLACK_BOT_TOKEN", WithDecryption=True)
    slack_token = response["Parameter"]["Value"]
    client = WebClient(token=slack_token)
    client.chat_postMessage(
        channel=f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}",
        text=f"Dim Devices events Dagster pipeline failed for run {context.dagster_run}",
    )
    return
