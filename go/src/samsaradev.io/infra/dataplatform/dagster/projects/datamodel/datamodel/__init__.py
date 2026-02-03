import warnings
import zlib

from dagster import (
    Definitions,
    ExperimentalWarning,
    FilesystemIOManager,
    InMemoryIOManager,
    SourceAsset,
    load_assets_from_modules,
)

# Turn off Experimental Features warning for now
warnings.filterwarnings("ignore", category=ExperimentalWarning)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from pyspark.sql import DataFrame, SparkSession

from .assets import source_assets
from .assets.activity import active_devices
from .assets.core_dimensions import (
    dim_devices,
    dim_devices_settings,
    dim_organizations,
    dim_organizations_settings,
    dim_organizations_tenure,
    dim_product_variants,
    fct_gateway_device_intervals,
)
from .assets.examples import (
    example_dq_pipeline_assets,
    example_pyspark_assets_pipeline,
    example_region_assets,
    example_time_partitioned_assets,
    fct_partitioned_object_stats,
    starter_task,
)
from .assets.platform import (
    dim_alert_configs,
    dim_device_activation,
    dim_licenses,
    driver_activity,
    fct_alert_incidents,
    user_activity,
)
from .assets.safety import ai_events, fct_firmware_events, fct_safety_events
from .assets.telematics import (
    dim_eld_relevant_devices,
    fct_dispatch_routes,
    fct_dvirs,
    fct_hos_logs,
    fct_trips_daily,
    fct_weather_alerts,
)
from .jobs import (
    activity,
    core_dimensions,
    example_jobs,
    metadata_jobs,
    monitor_jobs,
    platform,
    safety,
    telematics,
)
from .ops import datahub_scrapes, metadata, metadata_15m, monitors
from .resources import (
    databricks_cluster_specs,
    deltalake_io_managers,
    metadata_io_manager,
)

all_assets = load_assets_from_modules(
    [
        ai_events,
        datahub_scrapes,
        example_pyspark_assets_pipeline,
        example_dq_pipeline_assets,
        example_region_assets,
        metadata,
        metadata_15m,
        monitors,
        dim_devices,
        active_devices,
        dim_organizations,
        fct_partitioned_object_stats,
        dim_product_variants,
        example_time_partitioned_assets,
        fct_trips_daily,
        fct_hos_logs,
        dim_eld_relevant_devices,
        starter_task,
        dim_licenses,
        metadata,
        dim_device_activation,
        dim_organizations_tenure,
        fct_dvirs,
        fct_dispatch_routes,
        user_activity,
        driver_activity,
        fct_safety_events,
        fct_firmware_events,
        dim_organizations_settings,
        dim_devices_settings,
        dim_alert_configs,
        fct_alert_incidents,
        fct_weather_alerts,
        fct_gateway_device_intervals,
    ]
)


final_asset_list = [
    # Add AutoMaterialized Assets Here:
    # Add Source Assets Here:
    *source_assets.load_source_assets(source_assets),
    # Add Other Assets Here:
    *all_assets,
]

# The below logic generate the asset sensors for gold layer assets
# Assets are grouped into sensors based on pipeline group and partition def
# Assets with and without partitions are also decoupled
sensor_group_dict = {}
sensor_group_non_partitioned_dict = {}

for asset in final_asset_list:
    if isinstance(asset, SourceAsset):
        continue

    asset_key = asset.keys_by_output_name["result"]

    sensor_substring_skiplist = [
        "dataengineering_dev",
        "datamodel_dev",
        "dataplatform_dev",
        "firmware_dev",
        "bronze",
        "silver",
        "dataprep",
        "auditlog",
    ]

    # If the asset has any of the listed substrings, do not create the sensor
    if any(
        substring in asset_key.to_user_string()
        for substring in sensor_substring_skiplist
    ):
        continue

    # Sensors need to be decoupled by partition def and have unique names.
    # Leveraging the hash on the partition_def to create the sensor name
    partition_definition = str(asset.partitions_def)
    partition_definition_hash = (
        zlib.crc32(partition_definition.encode("utf-8")) & 0xFFFFFFFF
    )

    asset_pipeline_group = asset.group_names_by_key[asset.keys_by_output_name["result"]]

    # Grouping US, EU, and CA assets together
    if asset_pipeline_group.endswith("_eu"):
        asset_pipeline_group = asset_pipeline_group[:-3]
    elif asset_pipeline_group.endswith("_ca"):
        asset_pipeline_group = asset_pipeline_group[:-3]

    asset_pipeline_group += "_" + str(partition_definition_hash)

    if asset.partitions_def is not None:
        if asset_pipeline_group in sensor_group_dict:
            sensor_group_dict[asset_pipeline_group].append(asset_key)
        else:
            sensor_group_dict[asset_pipeline_group] = [asset_key]
    else:
        if asset_pipeline_group in sensor_group_non_partitioned_dict:
            sensor_group_non_partitioned_dict[asset_pipeline_group].append(asset_key)
        else:
            sensor_group_non_partitioned_dict[asset_pipeline_group] = [asset_key]


defs = Definitions(
    assets=final_asset_list,
    jobs=[
        example_jobs.assets_with_databricks_step_launcher,
        safety.fct_safety_events_job,
        safety.fct_safety_events_eu_job,
        safety.fct_safety_events_ca_job,
        safety.fct_firmware_events_job,
        safety.fct_firmware_events_eu_job,
        safety.fct_firmware_events_ca_job,
        safety.safety_ai_events_job,
        monitor_jobs.monitor_job,
        monitor_jobs.monitor_eu_job,
        monitor_jobs.monitor_ca_job,
        metadata_jobs.metadata_job_amundsen,
        metadata_jobs.metadata_job_datahub_6h,
        metadata_jobs.metadata_job_datahub_glue_ingestion,
        metadata_jobs.metadata_job_datahub_hourly,
        metadata_jobs.metadata_job_datahub_15m,
        metadata_jobs.metadata_job_datahub_scrape,
        metadata_jobs.metadata_job_datahub_biztech,
        metadata_jobs.s3_lb_ingest_job,
        # metadata_jobs.delete_unused_tables_ad_hoc_job,  # uncomment if you want to delete unused tables manually
        core_dimensions.dim_devices_job,
        core_dimensions.dim_devices_job_eu,
        core_dimensions.dim_devices_job_ca,
        core_dimensions.dim_organizations_job,
        core_dimensions.dim_organizations_job_eu,
        core_dimensions.dim_organizations_job_ca,
        core_dimensions.dim_product_variants_job,
        core_dimensions.dim_product_variants_job_eu,
        core_dimensions.dim_product_variants_job_ca,
        core_dimensions.dim_organizations_tenure_job,
        core_dimensions.dim_organizations_tenure_job_eu,
        core_dimensions.dim_organizations_tenure_job_ca,
        core_dimensions.dim_organizations_settings_job,
        core_dimensions.dim_organizations_settings_job_eu,
        core_dimensions.dim_organizations_settings_job_ca,
        core_dimensions.dim_devices_settings_job,
        core_dimensions.dim_devices_settings_job_eu,
        core_dimensions.dim_devices_settings_job_ca,
        activity.active_devices_job,
        activity.active_devices_job_eu,
        activity.active_devices_job_ca,
        activity.stg_device_activity_daily_job,
        activity.stg_device_activity_daily_eu_job,
        activity.stg_device_activity_daily_ca_job,
        telematics.fct_trips_daily_job,
        telematics.fct_trips_daily_eu_job,
        telematics.fct_trips_daily_ca_job,
        telematics.fct_hos_logs_job,
        telematics.fct_hos_logs_eu_job,
        telematics.fct_hos_logs_ca_job,
        telematics.dim_eld_relevant_devices_job,
        telematics.dim_eld_relevant_devices_eu_job,
        telematics.dim_eld_relevant_devices_ca_job,
        telematics.fct_dvirs_job,
        telematics.fct_dvirs_eu_job,
        telematics.fct_dvirs_ca_job,
        telematics.fct_dispatch_routes_job,
        telematics.fct_dispatch_routes_eu_job,
        telematics.fct_dispatch_routes_ca_job,
        telematics.stg_weather_alerts_job,
        platform.dim_licenses_job,
        platform.dim_licenses_eu_job,
        platform.dim_licenses_ca_job,
        platform.stg_active_licenses_eu_job,
        platform.stg_active_licenses_ca_job,
        platform.dim_device_activation_job,
        platform.dim_device_activation_eu_job,
        platform.dim_device_activation_ca_job,
        platform.stg_device_activation_job,
        platform.stg_device_activation_eu_job,
        platform.stg_device_activation_ca_job,
        platform.raw_clouddb_drivers_job,
        platform.raw_clouddb_drivers_eu_job,
        platform.raw_clouddb_drivers_ca_job,
        platform.raw_clouddb_users_tables_job,
        platform.raw_clouddb_users_tables_eu_job,
        platform.raw_clouddb_users_tables_ca_job,
        platform.user_activity_staging_job,
        platform.user_activity_staging_eu_job,
        platform.user_activity_staging_ca_job,
        platform.user_activity_job,
        platform.user_activity_eu_job,
        platform.user_activity_ca_job,
        platform.driver_activity_staging_job,
        platform.driver_activity_staging_eu_job,
        platform.driver_activity_staging_ca_job,
        platform.driver_activity_job,
        platform.driver_activity_eu_job,
        platform.driver_activity_ca_job,
        platform.dim_alert_configs_job,
        platform.dim_alert_configs_eu_job,
        platform.dim_alert_configs_ca_job,
        platform.fct_alert_incidents_job,
        platform.fct_alert_incidents_eu_job,
        platform.fct_alert_incidents_ca_job,
        core_dimensions.fct_gateway_device_intervals_job,
        core_dimensions.fct_gateway_device_intervals_eu_job,
    ],
    resources={
        "databricks_pyspark_step_launcher": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
        ),
        "databricks_pyspark_step_launcher_eu": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="eu-west-1",
            max_workers=4,
            instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
        ),
        "databricks_pyspark_step_launcher_ca": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="ca-central-1",
            max_workers=4,
            instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
        ),
        "databricks_pyspark_step_launcher_datahub_scrapes": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            max_workers=1,
            instance_pool_type=databricks_cluster_specs.GENERAL_PURPOSE_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
        ),
        "databricks_pyspark_step_launcher_datahub_scrapes_high_memory": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            max_workers=4,
            spark_version="12.2.x-scala2.12",
            instance_pool_type=databricks_cluster_specs.MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
                "spark.driver.memory": "10g",
                "spark.executor.memory": "10g",
            },
        ),
        "databricks_pyspark_step_launcher_firmware": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            instance_pool_type=databricks_cluster_specs.MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            max_workers=16,
            spark_conf={"spark.sql.autoBroadcastJoinThreshold", "-1"},
        ),
        "databricks_pyspark_step_launcher_firmware_eu": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="eu-west-1",
            instance_pool_type=databricks_cluster_specs.MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            max_workers=12,
        ),
        "databricks_pyspark_step_launcher_firmware_ca": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="ca-central-1",
            instance_pool_type=databricks_cluster_specs.MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            max_workers=12,
        ),
        "databricks_pyspark_step_launcher_dagster_cost_eval": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            driver_instance_type="md-fleet.large",
            worker_instance_type="rd-fleet.xlarge",
            max_workers=4,
        ),
        "databricks_pyspark_step_launcher_dagster_cost_eval_eu": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="eu-west-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            driver_instance_type="md-fleet.large",
            worker_instance_type="rd-fleet.xlarge",
            max_workers=4,
        ),
        "databricks_pyspark_step_launcher_dagster_cost_eval_ca": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="ca-central-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            spark_conf_overrides={
                "spark.databricks.sql.initial.catalog.name": "default",
            },
            driver_instance_type="md-fleet.large",
            worker_instance_type="rd-fleet.xlarge",
            max_workers=4,
        ),
        "databricks_pyspark_step_launcher_devices_settings": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "databricks_pyspark_step_launcher_devices_settings_eu": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="eu-west-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "databricks_pyspark_step_launcher_devices_settings_ca": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="ca-central-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "databricks_pyspark_step_launcher_firmware_events": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="us-west-2",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "databricks_pyspark_step_launcher_firmware_events_eu": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="eu-west-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "databricks_pyspark_step_launcher_firmware_events_ca": databricks_cluster_specs.ConfigurableDatabricksStepLauncher(
            region="ca-central-1",
            data_security_mode="SINGLE_USER",
            single_user_name="dev-databricks-dagster@samsara.com",
            driver_instance_type="rd-fleet.xlarge",
            worker_instance_type="rd-fleet.4xlarge",
            max_workers=16,
        ),
        "io_manager": deltalake_io_managers.DeltaTableIOManager(),
        "fs_io_manager": FilesystemIOManager(),
        "in_memory_io_manager": InMemoryIOManager(),
        "metadata_io_manager": metadata_io_manager.MetadataIOManager(),
    },
    schedules=[
        safety.safety_ai_events_backfill_schedule,
        safety.fct_safety_events_backfill_schedule_daily,
        safety.fct_safety_events_eu_backfill_schedule_daily,
        safety.fct_safety_events_ca_backfill_schedule_daily,
        safety.fct_safety_events_backfill_schedule_weekly,
        safety.fct_safety_events_eu_backfill_schedule_weekly,
        safety.fct_safety_events_ca_backfill_schedule_weekly,
        safety.fct_firmware_events_backfill_schedule_daily,
        safety.fct_firmware_events_eu_backfill_schedule_daily,
        safety.fct_firmware_events_ca_backfill_schedule_daily,
        core_dimensions.dim_devices_schedule,
        core_dimensions.dim_devices_schedule_eu,
        core_dimensions.dim_devices_schedule_ca,
        core_dimensions.dim_organizations_schedule,
        core_dimensions.dim_organizations_schedule_eu,
        core_dimensions.dim_organizations_schedule_ca,
        core_dimensions.dim_product_variants_schedule,
        core_dimensions.dim_product_variants_schedule_eu,
        core_dimensions.dim_product_variants_schedule_ca,
        core_dimensions.dim_organizations_tenure_schedule,
        core_dimensions.dim_organizations_tenure_schedule_eu,
        core_dimensions.dim_organizations_tenure_schedule_ca,
        core_dimensions.dim_organizations_settings_schedule,
        core_dimensions.dim_organizations_settings_schedule_eu,
        core_dimensions.dim_organizations_settings_schedule_ca,
        core_dimensions.dim_devices_settings_schedule,
        core_dimensions.dim_devices_settings_schedule_eu,
        core_dimensions.dim_devices_settings_schedule_ca,
        telematics.fct_trips_daily_schedule,
        telematics.fct_trips_daily_backfill_schedule,
        telematics.fct_trips_daily_eu_schedule,
        telematics.fct_trips_daily_eu_backfill_schedule,
        telematics.fct_trips_daily_ca_schedule,
        telematics.fct_trips_daily_ca_backfill_schedule,
        telematics.fct_hos_logs_schedule,
        telematics.fct_hos_logs_eu_schedule,
        telematics.fct_hos_logs_ca_schedule,
        telematics.dim_eld_relevant_devices_schedule,
        telematics.dim_eld_relevant_devices_eu_schedule,
        telematics.dim_eld_relevant_devices_ca_schedule,
        telematics.dim_eld_relevant_devices_backfill_schedule,
        telematics.dim_eld_relevant_devices_backfill_eu_schedule,
        telematics.dim_eld_relevant_devices_backfill_ca_schedule,
        telematics.fct_dvirs_backfill_schedule,
        telematics.fct_dvirs_backfill_eu_schedule,
        telematics.fct_dvirs_backfill_ca_schedule,
        telematics.fct_dispatch_routes_backfill_schedule,
        telematics.fct_dispatch_routes_backfill_eu_schedule,
        telematics.fct_dispatch_routes_backfill_ca_schedule,
        telematics.stg_weather_alerts_schedule,
        monitor_jobs.monitor_schedule,
        monitor_jobs.monitor_eu_schedule,
        monitor_jobs.monitor_ca_schedule,
        metadata_jobs.metadata_amundsen_schedule,
        metadata_jobs.metadata_datahub_6h_schedule,
        metadata_jobs.metadata_datahub_glue_schedule,
        metadata_jobs.metadata_databricks_schedule,
        metadata_jobs.metadata_datahub_stats_schedule,
        metadata_jobs.metadata_datahub_hourly_schedule,
        metadata_jobs.metadata_datahub_15m_schedule,
        metadata_jobs.metadata_datahub_scrape_schedule,
        metadata_jobs.metadata_datahub_biztech_schedule,
        platform.dim_licenses_schedule,
        platform.dim_licenses_eu_schedule,
        platform.dim_licenses_ca_schedule,
        platform.stg_device_activation_schedule,
        platform.stg_device_activation_eu_schedule,
        platform.stg_device_activation_ca_schedule,
        platform.dim_device_activation_schedule,
        platform.dim_device_activation_eu_schedule,
        platform.dim_device_activation_ca_schedule,
        platform.raw_clouddb_drivers_schedule,
        platform.raw_clouddb_drivers_eu_schedule,
        platform.raw_clouddb_drivers_ca_schedule,
        platform.raw_clouddb_users_tables_schedule,
        platform.raw_clouddb_users_tables_eu_schedule,
        platform.raw_clouddb_users_tables_ca_schedule,
        platform.user_activity_staging_schedule,
        platform.user_activity_staging_eu_schedule,
        platform.user_activity_staging_ca_schedule,
        platform.user_activity_schedule,
        platform.user_activity_eu_schedule,
        platform.user_activity_ca_schedule,
        platform.driver_activity_staging_schedule,
        platform.driver_activity_staging_eu_schedule,
        platform.driver_activity_staging_ca_schedule,
        platform.driver_activity_schedule,
        platform.driver_activity_eu_schedule,
        platform.driver_activity_ca_schedule,
        platform.dim_alert_configs_schedule,
        platform.dim_alert_configs_eu_schedule,
        platform.dim_alert_configs_ca_schedule,
        platform.fct_alert_incidents_backfill_schedule,
        platform.fct_alert_incidents_backfill_eu_schedule,
        platform.fct_alert_incidents_backfill_ca_schedule,
        core_dimensions.fct_gateway_device_intervals_schedule,
        core_dimensions.fct_gateway_device_intervals_eu_schedule,
        core_dimensions.fct_gateway_device_intervals_ca_schedule,
        activity.active_devices_backfill_schedule,
        activity.active_devices_eu_backfill_schedule,
        activity.active_devices_ca_backfill_schedule,
    ],
    sensors=[
        telematics.telematics_slack_failure_alert,
        platform.platform_slack_failure_alert,
        metadata_jobs.s3_lb_sensor,
    ],
)
