from dagster import (
    AssetKey,
    AssetSelection,
    MonthlyPartitionsDefinition,
    ScheduleDefinition,
    StaticPartitionsDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    run_failure_sensor,
)
from slack import WebClient

from ..assets.platform import dim_device_activation
from ..assets.platform import dim_licenses as licenses
from ..assets.platform import user_activity
from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.utils import (
    AWSRegions,
    BackfillPolicy,
    Database,
    apply_db_overrides,
    build_backfill_schedule,
    datamodel_job_failure_hook,
    datamodel_job_success_hook,
    date_range_from_current_date,
)

databases = {
    "database_bronze": Database.DATAMODEL_PLATFORM_BRONZE,
    "database_silver": Database.DATAMODEL_PLATFORM_SILVER,
    "database_gold": Database.DATAMODEL_PLATFORM,
    "database_core_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_core_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
    "database_core_gold_dev": Database.DATAMODEL_DEV,
    "database_core_bronze_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

dim_device_owner_activator = [dim_device_activation.dim_device_owner_activator]
dim_device_owner_activator_eu = [dim_device_activation.dim_device_owner_activator_eu]
dim_device_owner_activator_ca = [dim_device_activation.dim_device_owner_activator_ca]
stg_device_activation = [dim_device_activation.stg_device_activation]
stg_device_activation_eu = [dim_device_activation.stg_device_activation_eu]
stg_device_activation_ca = [dim_device_activation.stg_device_activation_ca]

stg_active_licenses_job = define_asset_job(
    name="stg_active_licenses_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "stg_active_licenses",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "dq_stg_active_licenses",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

stg_active_licenses_eu_job = define_asset_job(
    name="stg_active_licenses_eu_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "stg_active_licenses",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "dq_stg_active_licenses",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

stg_active_licenses_ca_job = define_asset_job(
    name="stg_active_licenses_ca_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "stg_active_licenses",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "dq_stg_active_licenses",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

stg_device_activation_job = define_asset_job(
    name="stg_device_activation_job",
    selection=stg_device_activation,
)

stg_device_activation_eu_job = define_asset_job(
    name="stg_device_activation_eu_job", selection=stg_device_activation_eu
)

stg_device_activation_ca_job = define_asset_job(
    name="stg_device_activation_ca_job", selection=stg_device_activation_ca
)

dim_device_activation_job = define_asset_job(
    name="dim_device_activation_job",
    selection=AssetSelection.keys(
        AssetKey(
            [
                AWSRegions.US_WEST_2.value,
                databases["database_core_gold"],
                "dim_device_owner_activator",
            ],
        ),
        AssetKey(
            [
                AWSRegions.US_WEST_2.value,
                databases["database_core_gold_dev"],
                "dq_dim_device_owner_activator",
            ],
        ),
    ),
)

dim_device_activation_eu_job = define_asset_job(
    name="dim_device_activation_eu_job",
    selection=AssetSelection.keys(
        AssetKey(
            [
                AWSRegions.EU_WEST_1.value,
                databases["database_core_gold"],
                "dim_device_owner_activator",
            ]
        ),
        AssetKey(
            [
                AWSRegions.EU_WEST_1.value,
                databases["database_core_gold_dev"],
                "dq_dim_device_owner_activator",
            ]
        ),
    ),
)

dim_device_activation_ca_job = define_asset_job(
    name="dim_device_activation_ca_job",
    selection=AssetSelection.keys(
        AssetKey(
            [
                AWSRegions.CA_CENTRAL_1.value,
                databases["database_core_gold"],
                "dim_device_owner_activator",
            ]
        ),
        AssetKey(
            [
                AWSRegions.CA_CENTRAL_1.value,
                databases["database_core_gold_dev"],
                "dq_dim_device_owner_activator",
            ]
        ),
    ),
)

raw_clouddb_drivers_job = define_asset_job(
    name="raw_clouddb_drivers_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "raw_clouddb_drivers",
        ],
    ),
)

raw_clouddb_drivers_eu_job = define_asset_job(
    name="raw_clouddb_drivers_eu_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "raw_clouddb_drivers",
        ],
    ),
)

raw_clouddb_drivers_ca_job = define_asset_job(
    name="raw_clouddb_drivers_ca_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "raw_clouddb_drivers",
        ],
    ),
)

driver_activity_staging_job = define_asset_job(
    name="driver_activity_staging_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "stg_driver_app_events",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "dq_stg_driver_app_events",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "fct_driver_activity",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_fct_driver_activity",
        ],
    ),
)

driver_activity_staging_eu_job = define_asset_job(
    name="driver_activity_staging_eu_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "stg_driver_app_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "dq_stg_driver_app_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "fct_driver_activity",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_fct_driver_activity",
        ],
    ),
)

driver_activity_staging_ca_job = define_asset_job(
    name="driver_activity_staging_ca_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "stg_driver_app_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "dq_stg_driver_app_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "fct_driver_activity",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_fct_driver_activity",
        ],
    ),
)

driver_activity_job = define_asset_job(
    name="driver_activity_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dim_drivers",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_dim_drivers",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

driver_activity_eu_job = define_asset_job(
    name="driver_activity_eu_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dim_drivers",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_dim_drivers",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

driver_activity_ca_job = define_asset_job(
    name="driver_activity_ca_job",
    selection=AssetSelection.groups("driver_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dim_drivers",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_dim_drivers",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

raw_clouddb_users_tables_job = define_asset_job(
    name="raw_clouddb_user_tables_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [AWSRegions.US_WEST_2.value, databases["database_bronze"], "raw_clouddb_users"],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "raw_clouddb_users_organizations",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "raw_clouddb_custom_roles",
        ],
    ),
)

raw_clouddb_users_tables_eu_job = define_asset_job(
    name="raw_clouddb_user_tables_eu_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [AWSRegions.EU_WEST_1.value, databases["database_bronze"], "raw_clouddb_users"],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "raw_clouddb_users_organizations",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "raw_clouddb_custom_roles",
        ],
    ),
)

raw_clouddb_users_tables_ca_job = define_asset_job(
    name="raw_clouddb_user_tables_ca_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "raw_clouddb_users",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "raw_clouddb_users_organizations",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "raw_clouddb_custom_roles",
        ],
    ),
)


user_activity_staging_job = define_asset_job(
    name="user_activity_staging_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "stg_user_login_events",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "dq_stg_user_login_events",
        ],
        [AWSRegions.US_WEST_2.value, databases["database_silver"], "stg_cloud_routes"],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "dq_stg_cloud_routes",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "stg_fleet_app_events",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_silver"],
            "dq_stg_fleet_app_events",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "fct_user_activity",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_fct_user_activity",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

user_activity_job = define_asset_job(
    name="user_activity_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dim_users",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dim_users_organizations",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_dim_users",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_dim_users_organizations",
        ],
    ),
)

user_activity_staging_eu_job = define_asset_job(
    name="user_activity_staging_eu_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "stg_user_login_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "dq_stg_user_login_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "stg_fleet_app_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_silver"],
            "dq_stg_fleet_app_events",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "fct_user_activity",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_fct_user_activity",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

user_activity_eu_job = define_asset_job(
    name="user_activity_eu_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dim_users",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dim_users_organizations",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_dim_users",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_dim_users_organizations",
        ],
    ),
)

user_activity_staging_ca_job = define_asset_job(
    name="user_activity_staging_ca_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "stg_user_login_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "dq_stg_user_login_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "stg_fleet_app_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_silver"],
            "dq_stg_fleet_app_events",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "fct_user_activity",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_fct_user_activity",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

user_activity_ca_job = define_asset_job(
    name="user_activity_ca_job",
    selection=AssetSelection.groups("user_activity").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dim_users",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dim_users_organizations",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_dim_users",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_dim_users_organizations",
        ],
    ),
)

dim_device_activation_schedule = build_schedule_from_partitioned_job(
    job=dim_device_activation_job,
    minute_of_hour=5,
    hour_of_day=10,
)

dim_device_activation_eu_schedule = build_schedule_from_partitioned_job(
    job=dim_device_activation_eu_job,
    minute_of_hour=5,
    hour_of_day=10,
)

dim_device_activation_ca_schedule = build_schedule_from_partitioned_job(
    job=dim_device_activation_ca_job,
    minute_of_hour=5,
    hour_of_day=10,
)

stg_device_activation_schedule = build_schedule_from_partitioned_job(
    job=stg_device_activation_job,
    minute_of_hour=5,
    hour_of_day=2,
)

stg_device_activation_eu_schedule = build_schedule_from_partitioned_job(
    job=stg_device_activation_eu_job,
    minute_of_hour=5,
    hour_of_day=2,
)

stg_device_activation_ca_schedule = build_schedule_from_partitioned_job(
    job=stg_device_activation_ca_job,
    minute_of_hour=5,
    hour_of_day=2,
)

dim_licenses_job = define_asset_job(
    name="dim_licenses_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.US_WEST_2.value,
            databases["database_bronze"],
            "stg_license_assignment",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dim_license_assignment",
        ],
        [
            AWSRegions.US_WEST_2.value,
            databases["database_gold"],
            "dq_dim_license_assignment",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_licenses_eu_job = define_asset_job(
    name="dim_licenses_eu_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_bronze"],
            "stg_license_assignment",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dim_license_assignment",
        ],
        [
            AWSRegions.EU_WEST_1.value,
            databases["database_gold"],
            "dq_dim_license_assignment",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_licenses_ca_job = define_asset_job(
    name="dim_licenses_ca_job",
    selection=AssetSelection.groups("dim_licenses").key_prefixes(
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_bronze"],
            "stg_license_assignment",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dim_license_assignment",
        ],
        [
            AWSRegions.CA_CENTRAL_1.value,
            databases["database_gold"],
            "dq_dim_license_assignment",
        ],
    ),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_licenses_schedule = build_schedule_from_partitioned_job(
    job=dim_licenses_job,
    minute_of_hour=30,
    hour_of_day=5,
)

dim_licenses_eu_schedule = build_schedule_from_partitioned_job(
    job=dim_licenses_eu_job,
    minute_of_hour=30,
    hour_of_day=5,
)

dim_licenses_ca_schedule = build_schedule_from_partitioned_job(
    job=dim_licenses_ca_job,
    minute_of_hour=30,
    hour_of_day=5,
)

raw_clouddb_users_tables_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_users_tables_job,
    minute_of_hour=0,
    hour_of_day=4,
)

raw_clouddb_users_tables_eu_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_users_tables_eu_job,
    minute_of_hour=0,
    hour_of_day=4,
)

raw_clouddb_users_tables_ca_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_users_tables_ca_job,
    minute_of_hour=0,
    hour_of_day=4,
)

user_activity_staging_schedule = build_schedule_from_partitioned_job(
    job=user_activity_staging_job,
    minute_of_hour=15,
    hour_of_day=11,
)

user_activity_staging_eu_schedule = build_schedule_from_partitioned_job(
    job=user_activity_staging_eu_job,
    minute_of_hour=15,
    hour_of_day=11,
)

user_activity_staging_ca_schedule = build_schedule_from_partitioned_job(
    job=user_activity_staging_ca_job,
    minute_of_hour=15,
    hour_of_day=11,
)

user_activity_schedule = build_schedule_from_partitioned_job(
    job=user_activity_job,
    minute_of_hour=15,
    hour_of_day=12,
)

user_activity_eu_schedule = build_schedule_from_partitioned_job(
    job=user_activity_eu_job,
    minute_of_hour=15,
    hour_of_day=12,
)

user_activity_ca_schedule = build_schedule_from_partitioned_job(
    job=user_activity_ca_job,
    minute_of_hour=15,
    hour_of_day=12,
)

raw_clouddb_drivers_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_drivers_job,
    minute_of_hour=30,
    hour_of_day=6,
)

raw_clouddb_drivers_eu_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_drivers_eu_job,
    minute_of_hour=30,
    hour_of_day=6,
)

raw_clouddb_drivers_ca_schedule = build_schedule_from_partitioned_job(
    job=raw_clouddb_drivers_ca_job,
    minute_of_hour=30,
    hour_of_day=6,
)

driver_activity_staging_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_staging_job,
    minute_of_hour=30,
    hour_of_day=7,
)

driver_activity_staging_eu_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_staging_eu_job,
    minute_of_hour=30,
    hour_of_day=7,
)

driver_activity_staging_ca_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_staging_ca_job,
    minute_of_hour=30,
    hour_of_day=7,
)

driver_activity_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_job,
    minute_of_hour=5,
    hour_of_day=9,
)

driver_activity_eu_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_eu_job,
    minute_of_hour=5,
    hour_of_day=9,
)

driver_activity_ca_schedule = build_schedule_from_partitioned_job(
    job=driver_activity_ca_job,
    minute_of_hour=5,
    hour_of_day=9,
)

dim_alert_configs_job = define_asset_job(
    "dim_alert_configs",
    AssetSelection.groups("dim_alert_configs"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_alert_configs_eu_job = define_asset_job(
    "dim_alert_configs_eu",
    AssetSelection.groups("dim_alert_configs_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_alert_configs_ca_job = define_asset_job(
    "dim_alert_configs_ca",
    AssetSelection.groups("dim_alert_configs_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

dim_alert_configs_schedule = build_schedule_from_partitioned_job(
    job=dim_alert_configs_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

dim_alert_configs_eu_schedule = build_schedule_from_partitioned_job(
    job=dim_alert_configs_eu_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

dim_alert_configs_ca_schedule = build_schedule_from_partitioned_job(
    job=dim_alert_configs_ca_job,
    description="Daily partitioned schedule",
    minute_of_hour=30,
    hour_of_day=6,
)

fct_alert_incidents_job = define_asset_job(
    "fct_alert_incidents",
    AssetSelection.groups("fct_alert_incidents"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_alert_incidents_eu_job = define_asset_job(
    "fct_alert_incidents_eu",
    AssetSelection.groups("fct_alert_incidents_eu"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_alert_incidents_ca_job = define_asset_job(
    "fct_alert_incidents_ca",
    AssetSelection.groups("fct_alert_incidents_ca"),
    hooks={datamodel_job_failure_hook, datamodel_job_success_hook},
)

fct_alert_incidents_backfill_schedule = build_backfill_schedule(
    job=fct_alert_incidents_job,
    cron="30 6 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 0},
)

fct_alert_incidents_backfill_eu_schedule = build_backfill_schedule(
    job=fct_alert_incidents_eu_job,
    cron="30 6 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 0},
)

fct_alert_incidents_backfill_ca_schedule = build_backfill_schedule(
    job=fct_alert_incidents_ca_job,
    cron="30 6 * * *",
    partitions_fn=date_range_from_current_date,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    options={"days": 30, "offset": 0},
)


@run_failure_sensor(
    monitored_jobs=[
        stg_active_licenses_job,
        stg_active_licenses_eu_job,
        dim_licenses_job,
        dim_licenses_eu_job,
        driver_activity_job,
        driver_activity_eu_job,
        user_activity_job,
        user_activity_eu_job,
        dim_alert_configs_job,
        dim_alert_configs_eu_job,
        fct_alert_incidents_job,
        fct_alert_incidents_eu_job,
    ],
    description="Sensor that monitors and sends slack alerts for failed job run status",
    minimum_interval_seconds=900,
)
def platform_slack_failure_alert(context):
    ssm = boto3.client("ssm", region_name=AWSRegions.US_WEST_2.value)
    response = ssm.get_parameter(Name="DAGSTER_SLACK_BOT_TOKEN", WithDecryption=True)
    slack_token = response["Parameter"]["Value"]
    client = WebClient(token=slack_token)
    client.chat_postMessage(
        channel=f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}",
        text=f"Dagster pipeline failed for run {context.dagster_run}",
    )
    return
