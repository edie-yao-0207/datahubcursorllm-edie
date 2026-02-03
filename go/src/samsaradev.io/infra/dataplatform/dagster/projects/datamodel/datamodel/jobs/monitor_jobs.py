from dagster import AssetSelection, ScheduleDefinition, define_asset_job

monitor_job = define_asset_job("monitors", AssetSelection.groups("monitors"))
monitor_schedule = ScheduleDefinition(job=monitor_job, cron_schedule="25 * * * *")

monitor_eu_job = define_asset_job("monitors_eu", AssetSelection.groups("monitors_eu"))
monitor_eu_schedule = ScheduleDefinition(job=monitor_eu_job, cron_schedule="35 * * * *")

monitor_ca_job = define_asset_job("monitors_ca", AssetSelection.groups("monitors_ca"))
monitor_ca_schedule = ScheduleDefinition(job=monitor_ca_job, cron_schedule="45 * * * *")
