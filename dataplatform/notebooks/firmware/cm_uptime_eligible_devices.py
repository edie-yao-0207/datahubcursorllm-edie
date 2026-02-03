# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# COMMAND ----------

table_name = "dataprep_firmware.cm_uptime_eligible_devices"

# COMMAND ----------

# Only retrieve CMs that have been active in the last 14 days. Also, retrieve the
# CM device_id from the associations table since subsequent joins will be on cm_device_id.
active_cms_df = spark.sql(
    """--sql
    SELECT
        a.date
        , a.org_id
        , b.vg_device_id
        , b.vg_gateway_id
        , b.cm_device_id
        , b.cm_gateway_id
        -- Will need this to distinguish between Octos and CMs downstream
        , c.product_id AS cm_product_id
    FROM
        dataprep_firmware.active_devices_based_on_heartbeat_in_last_14_days AS a
    JOIN
        dataprep_firmware.cm_octo_vg_daily_associations_unique AS b
        ON a.date = b.date
        AND a.device_id = b.cm_device_id
    LEFT JOIN datamodel_core.dim_devices AS c
        ON b.date = c.date
        AND a.org_id = c.org_id
        AND b.cm_device_id = c.device_id -- Only need to get product_id for CM/Octo
    WHERE
        a.date >= '{}'
        AND a.date <= '{}'
    --endsql
    """.format(
        start_date, end_date
    )
)

# Only retrieve CMs with a firmware that has recording state manager (RSM)
valid_firmware_df = spark.sql(
    """--sql
    WITH firmware_build AS (
        SELECT
            date
            , LAG(date, 1) OVER (PARTITION BY gateway_id ORDER BY date) AS date_previous
            , gateway_id
            , latest_build_on_day
            , LAG(latest_build_on_day, 1) OVER (PARTITION BY gateway_id ORDER BY date) AS latest_build_on_day_previous
        FROM
            dataprep_firmware.device_daily_firmware_builds
        WHERE
            -- Look back an extra day back to get previous date's firmware details
            date >= CAST(DATE_SUB('{}', 1) AS STRING)
            AND date <= '{}'
    )
    SELECT
        date
        , date_previous
        , gateway_id
        , latest_build_on_day
        , latest_build_on_day_previous
    FROM 
        firmware_build
    WHERE
        date >= '{}'
        AND date <= '{}'
        -- RSM is only enabled on cm-35 and later firmware builds
        -- 2024-11-18_cm-35.0.2_dd860851ca
        AND (
            SUBSTRING(latest_build_on_day, 1, 16) LIKE '2024-11-18_cm-35%' 
            OR SUBSTRING(latest_build_on_day, 1, 10) > '2024-11-18'
        )
        -- Also filter out device/dates on which the device first upgraded to an eligible firmware
        -- since there may be low recording periods prior to the firmware being upgraded.
        AND (
            SUBSTRING(latest_build_on_day_previous, 1, 16) LIKE '2024-11-18_cm-35%'
            OR SUBSTRING(latest_build_on_day_previous, 1, 10) > '2024-11-18'
            -- If a device has been inactive, then on the first date on which it becomes active
            -- the latest_build_on_day_previous will be NULL.
            OR latest_build_on_day_previous IS NULL
        )
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# Join in RSM-disabled data, when available.
# Note: A previous version of this notebook combined the RSM-enabled check with the firmware version check,
# using the table stg_devices_settings_latest_date, and INNER JOINed that resultant dataframe with
# active_valid_cms_df. This resulted in a number of missing uptime entries since that upstream table only
# pulls configs for devices that have sent config signals within its lookback window (at the current time 365 days).
recording_state_manager_setting_df = spark.sql(
    """--sql
    WITH camera_config AS (
        SELECT
            date
            , LAG(date, 1) OVER (PARTITION BY device_id ORDER BY date) AS date_previous
            , org_id
            , device_id
            , device_config.camera_config.recording_with_recording_manager_enabled AS recording_with_recording_manager_enabled
            , LAG(device_config.camera_config.recording_with_recording_manager_enabled, 1) OVER (PARTITION BY org_id, device_id ORDER BY date) AS recording_with_recording_manager_enabled_previous
        FROM
            datamodel_core_silver.stg_devices_settings_latest_date
        WHERE
            -- Look back an extra day back to get previous date's config
            date >= CAST(DATE_SUB('{}', 1) AS STRING)
            AND date <= '{}'
    )
    SELECT
        date
        , date_previous
        , org_id
        , device_id
        , recording_with_recording_manager_enabled
        , recording_with_recording_manager_enabled_previous
    FROM camera_config
    WHERE 
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# COMMAND ----------

# Capture a single consistent timestamp for the entire job,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Retrieve only those CMs with a firmware version that has Recording State Manager (RSM)
active_valid_cms_df = (
    active_cms_df.alias("a")
    .join(
        valid_firmware_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        # Omitting org_id will result in duplications for devices that switch orgs on a date
        # TODO: Update valid_firmware_df data source to include org_id
        # & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_gateway_id") == F.col("b.gateway_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.cm_gateway_id",
        "a.cm_product_id",
        "a.vg_device_id",
        "a.vg_gateway_id",
        "b.latest_build_on_day",
        "b.latest_build_on_day_previous",
    )
    # Drop duplicates since data source for valid_firmware_df doesn't have org_id
    .dropDuplicates()
)

# Exclude CMs with RSM explicitly disabled
active_valid_cms_rsm_not_disabled_df = (
    active_valid_cms_df.alias("a")
    .join(
        recording_state_manager_setting_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_device_id") == F.col("b.device_id")),
        how="left",
    )
    # In Spark SQL, comparisons with NULL values return NULL (which is treated as False in filters).
    # Therefore, we need to explicitly allow for NULL values to pass through, since we want to keep
    # any devices that don't explicitly disable RSM (vast majority of devices have RSM enabled and
    # RSM was disabled only for firmware testing purposes).
    .filter(
        (
            (F.col("b.recording_with_recording_manager_enabled").isNull())
            & (F.col("b.recording_with_recording_manager_enabled_previous").isNull())
        )
        | (
            (F.col("b.recording_with_recording_manager_enabled") == 1)
            & (F.col("b.recording_with_recording_manager_enabled_previous").isNull())
        )
        | (
            (F.col("b.recording_with_recording_manager_enabled").isNull())
            & (F.col("b.recording_with_recording_manager_enabled_previous") == 1)
        )
        | ~(
            (F.col("b.recording_with_recording_manager_enabled") == 2)
            | (F.col("b.recording_with_recording_manager_enabled_previous") == 2)
        )
    )
    .select(
        "a.date",
        "a.org_id",
        "a.cm_device_id",
        "a.cm_gateway_id",
        "a.cm_product_id",
        "a.vg_device_id",
        "a.vg_gateway_id",
        "a.latest_build_on_day",
        "a.latest_build_on_day_previous",
        "b.recording_with_recording_manager_enabled",
        "b.recording_with_recording_manager_enabled_previous",
    )
    .dropDuplicates()
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
)

# COMMAND ----------

create_or_update_table(
    table_name,
    active_valid_cms_rsm_not_disabled_df,
    "date",
    [
        "date",
        "org_id",
        "cm_device_id",
        "cm_gateway_id",
        "vg_device_id",
        "vg_gateway_id",
    ],
)
