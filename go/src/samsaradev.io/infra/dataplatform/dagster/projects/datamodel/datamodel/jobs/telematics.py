import boto3
from dagster import (
    AssetSelection,
    BackfillPolicy,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
)
from slack import WebClient

from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.utils import (
    AWSRegions,
    build_backfill_schedule,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
    date_range_from_current_date,
)

fct_trips_daily_job = define_asset_job(
    "fct_trips_daily",
    AssetSelection.groups("fct_trips_daily"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_trips_daily_schedule = build_schedule_from_partitioned_job(
    job=fct_trips_daily_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

fct_trips_daily_backfill_schedule = build_backfill_schedule(
    job=fct_trips_daily_job,
    cron="30 8,21 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)

fct_trips_daily_eu_job = define_asset_job(
    "fct_trips_daily_eu",
    AssetSelection.groups("fct_trips_daily_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_trips_daily_eu_schedule = build_schedule_from_partitioned_job(
    job=fct_trips_daily_eu_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

fct_trips_daily_eu_backfill_schedule = build_backfill_schedule(
    job=fct_trips_daily_eu_job,
    cron="30 8 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)

fct_trips_daily_ca_job = define_asset_job(
    "fct_trips_daily_ca",
    AssetSelection.groups("fct_trips_daily_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_trips_daily_ca_schedule = build_schedule_from_partitioned_job(
    job=fct_trips_daily_ca_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

fct_trips_daily_ca_backfill_schedule = build_backfill_schedule(
    job=fct_trips_daily_ca_job,
    cron="30 8 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 7, "offset": 1},
)

fct_hos_logs_job = define_asset_job(
    "fct_hos_logs",
    AssetSelection.groups("fct_hos_logs"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_hos_logs_schedule = build_schedule_from_partitioned_job(
    job=fct_hos_logs_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=12,
)

fct_hos_logs_eu_job = define_asset_job(
    "fct_hos_logs_eu",
    AssetSelection.groups("fct_hos_logs_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_hos_logs_eu_schedule = build_schedule_from_partitioned_job(
    job=fct_hos_logs_eu_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=12,
)

fct_hos_logs_ca_job = define_asset_job(
    "fct_hos_logs_ca",
    AssetSelection.groups("fct_hos_logs_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_hos_logs_ca_schedule = build_schedule_from_partitioned_job(
    job=fct_hos_logs_ca_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=12,
)

dim_eld_relevant_devices_job = define_asset_job(
    "dim_eld_relevant_devices",
    AssetSelection.groups("dim_eld_relevant_devices"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_eld_relevant_devices_schedule = build_schedule_from_partitioned_job(
    job=dim_eld_relevant_devices_job,
    description="Daily partitioned schedule",
    minute_of_hour=45,
    hour_of_day=12,
)

dim_eld_relevant_devices_backfill_schedule = build_backfill_schedule(
    job=dim_eld_relevant_devices_job,
    cron="00 19 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 3, "offset": 1},
)

dim_eld_relevant_devices_eu_job = define_asset_job(
    "dim_eld_relevant_devices_eu",
    AssetSelection.groups("dim_eld_relevant_devices_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_eld_relevant_devices_ca_job = define_asset_job(
    "dim_eld_relevant_devices_ca",
    AssetSelection.groups("dim_eld_relevant_devices_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_eld_relevant_devices_eu_schedule = build_schedule_from_partitioned_job(
    job=dim_eld_relevant_devices_eu_job,
    description="Daily partitioned schedule",
    minute_of_hour=45,
    hour_of_day=12,
)

dim_eld_relevant_devices_ca_schedule = build_schedule_from_partitioned_job(
    job=dim_eld_relevant_devices_ca_job,
    description="Daily partitioned schedule",
    minute_of_hour=45,
    hour_of_day=12,
)

dim_eld_relevant_devices_backfill_eu_schedule = build_backfill_schedule(
    job=dim_eld_relevant_devices_eu_job,
    cron="00 19 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 3, "offset": 1},
)

dim_eld_relevant_devices_backfill_ca_schedule = build_backfill_schedule(
    job=dim_eld_relevant_devices_ca_job,
    cron="00 19 * * *",
    partitions_fn=date_range_from_current_date,
    options={"days": 3, "offset": 1},
)

fct_dvirs_job = define_asset_job(
    "fct_dvirs",
    AssetSelection.groups("fct_dvirs"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)


fct_dvirs_eu_job = define_asset_job(
    "fct_dvirs_eu",
    AssetSelection.groups("fct_dvirs_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_dvirs_ca_job = define_asset_job(
    "fct_dvirs_ca",
    AssetSelection.groups("fct_dvirs_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_dvirs_backfill_schedule = build_backfill_schedule(
    job=fct_dvirs_job,
    cron="00 1 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 60, "offset": 1},
)

fct_dvirs_backfill_eu_schedule = build_backfill_schedule(
    job=fct_dvirs_eu_job,
    cron="00 1 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 60, "offset": 1},
)

fct_dvirs_backfill_ca_schedule = build_backfill_schedule(
    job=fct_dvirs_ca_job,
    cron="00 1 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 60, "offset": 1},
)

fct_dispatch_routes_job = define_asset_job(
    "fct_dispatch_routes",
    AssetSelection.groups("fct_dispatch_routes"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_dispatch_routes_eu_job = define_asset_job(
    "fct_dispatch_routes_eu",
    AssetSelection.groups("fct_dispatch_routes_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_dispatch_routes_ca_job = define_asset_job(
    "fct_dispatch_routes_ca",
    AssetSelection.groups("fct_dispatch_routes_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_dispatch_routes_backfill_schedule = build_backfill_schedule(
    job=fct_dispatch_routes_job,
    cron="00 2 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 1},
)

fct_dispatch_routes_backfill_eu_schedule = build_backfill_schedule(
    job=fct_dispatch_routes_eu_job,
    cron="00 2 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 1},
)

fct_dispatch_routes_backfill_ca_schedule = build_backfill_schedule(
    job=fct_dispatch_routes_ca_job,
    cron="00 2 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 1},
)

stg_weather_alerts_job = define_asset_job(
    name="stg_weather_alerts_job",
    selection=AssetSelection.groups("fct_weather_alerts"),
)

stg_weather_alerts_schedule = build_schedule_from_partitioned_job(
    job=stg_weather_alerts_job,
    minute_of_hour=5,
    hour_of_day=6,
)


@run_failure_sensor(
    monitored_jobs=[
        fct_trips_daily_job,
        fct_trips_daily_eu_job,
        fct_trips_daily_ca_job,
        fct_hos_logs_job,
        fct_hos_logs_eu_job,
        fct_hos_logs_ca_job,
        fct_dvirs_job,
        fct_dvirs_eu_job,
        fct_dvirs_ca_job,
        fct_dispatch_routes_job,
        fct_dispatch_routes_eu_job,
        fct_dispatch_routes_ca_job,
    ],
    description=(
        "Sensor that monitors and sends slack alerts for failed job run status"
    ),
    minimum_interval_seconds=900,
)
def telematics_slack_failure_alert(context):
    ssm = boto3.client("ssm", region_name=AWSRegions.US_WEST_2.value)
    response = ssm.get_parameter(Name="DAGSTER_SLACK_BOT_TOKEN", WithDecryption=True)
    slack_token = response["Parameter"]["Value"]
    client = WebClient(token=slack_token)
    client.chat_postMessage(
        channel=f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}",
        text=f"Dagster pipeline failed for run {context.dagster_run}",
    )
    return
