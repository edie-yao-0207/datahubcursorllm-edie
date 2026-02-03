from dagster import AssetSelection, BackfillPolicy, define_asset_job

from ..common.utils import (
    build_backfill_schedule,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
    date_range_from_current_date,
)

fct_safety_events_job = define_asset_job(
    "fct_safety_events",
    AssetSelection.groups("fct_safety_events"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
fct_safety_events_eu_job = define_asset_job(
    "fct_safety_events_eu",
    AssetSelection.groups("fct_safety_events_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_safety_events_ca_job = define_asset_job(
    "fct_safety_events_ca",
    AssetSelection.groups("fct_safety_events_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
safety_ai_events_job = define_asset_job(
    "safety_ai_events",
    AssetSelection.groups("safety_ai_events"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
fct_firmware_events_job = define_asset_job(
    "fct_firmware_events",
    AssetSelection.groups("fct_firmware_events"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)
fct_firmware_events_eu_job = define_asset_job(
    "fct_firmware_events_eu",
    AssetSelection.groups("fct_firmware_events_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_firmware_events_ca_job = define_asset_job(
    "fct_firmware_events_ca",
    AssetSelection.groups("fct_firmware_events_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

# Backfill last 28 days every day
# Comparison of updated_date vs date in safetydb.activity_events shows that about 99% of activity events
# occur within 28 days of the safety event.
fct_safety_events_backfill_schedule_daily = build_backfill_schedule(
    job=fct_safety_events_job,
    cron="00 12/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 28, "offset": 0},
)
fct_safety_events_eu_backfill_schedule_daily = build_backfill_schedule(
    job=fct_safety_events_eu_job,
    cron="15 12/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_eu_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 28, "offset": 0},
)

fct_safety_events_ca_backfill_schedule_daily = build_backfill_schedule(
    job=fct_safety_events_ca_job,
    cron="30 12/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_ca_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 28, "offset": 0},
)
# Backfill last 365 days every Sunday
# Comparison of updated_date vs date in safetydb.activity_events shows that about 99.9% of activity events
# occur within 365 days of the safety event.
fct_safety_events_backfill_schedule_weekly = build_backfill_schedule(
    job=fct_safety_events_job,
    cron="30 12 * * 0",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_backfill_weekly",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 335, "offset": 30},
)
fct_safety_events_eu_backfill_schedule_weekly = build_backfill_schedule(
    job=fct_safety_events_eu_job,
    cron="45 12 * * 0",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_eu_backfill_weekly",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 335, "offset": 30},
)

fct_safety_events_ca_backfill_schedule_weekly = build_backfill_schedule(
    job=fct_safety_events_ca_job,
    cron="0 13 * * 0",
    partitions_fn=date_range_from_current_date,
    name="fct_safety_events_ca_backfill_weekly",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 335, "offset": 30},
)
safety_ai_events_backfill_schedule = build_backfill_schedule(
    job=safety_ai_events_job,
    cron="30 13/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="safety_ai_events_backfill",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 3, "offset": 0},
)

# Backfill last 2 days every day
# Analysis of auditlog data shows that about >99.9% of events land within osdaccelerometer within 2 days.
fct_firmware_events_backfill_schedule_daily = build_backfill_schedule(
    job=fct_firmware_events_job,
    cron="00 14/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_firmware_events_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 2, "offset": 0},
)
fct_firmware_events_eu_backfill_schedule_daily = build_backfill_schedule(
    job=fct_firmware_events_eu_job,
    cron="15 14/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_firmware_events_eu_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 2, "offset": 0},
)

fct_firmware_events_ca_backfill_schedule_daily = build_backfill_schedule(
    job=fct_firmware_events_ca_job,
    cron="30 14/8 * * *",
    partitions_fn=date_range_from_current_date,
    name="fct_firmware_events_ca_backfill_daily",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    options={"days": 2, "offset": 0},
)
