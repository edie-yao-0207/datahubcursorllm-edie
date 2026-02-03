from datetime import datetime, timedelta

DATE = str((datetime.utcnow() - timedelta(days=1)).date())

query = f"""
WITH todays_configs AS (
    SELECT
        date,
        org_id,
        object_id AS device_id,
        time AS start_time,
        s3_proto_value.reported_device_config.device_config.accel_mgr_config.imu_harsh_event_config.vehicle_type AS vehicle_type,
        LEAD(s3_proto_value.reported_device_config.device_config.accel_mgr_config.imu_harsh_event_config.vehicle_type) OVER(
        PARTITION BY
            org_id,
            object_id
        ORDER BY
            time ASC
        ) AS next_vehicle_type
    FROM
        kinesisstats.osdreporteddeviceconfig_with_s3_big_stat
    WHERE
        date = '{DATE}'
        AND s3_proto_value.reported_device_config.device_config.accel_mgr_config.imu_harsh_event_config IS NOT NULL
),
/* only pull configs where we have changes */
todays_configs_only_changes AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        vehicle_type
    FROM todays_configs
    WHERE
        next_vehicle_type IS NULL
        OR next_vehicle_type != vehicle_type
),
/* get latest configs and split on date so we can join on date partition */
latest_configs AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        vehicle_type
    FROM datascience.imu_harsh_event_config_vehicle_type_intervals
    WHERE
        date = DATE_SUB('{DATE}', 1)
        AND is_latest_config
),
latest_configs_start_today AS (
    SELECT
        '{DATE}' AS date,
        org_id,
        device_id,
        TO_UNIX_TIMESTAMP('{DATE}', 'yyyy-MM-dd') * 1000 AS start_time,
        vehicle_type
    FROM latest_configs
),
/* UNION ALL UPDATES */
updates AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        COALESCE(
            LEAD(start_time) OVER(
                PARTITION BY
                    org_id,
                    device_id
                ORDER BY
                    start_time ASC
            ),
            TO_UNIX_TIMESTAMP('{DATE}', 'yyyy-MM-dd') * 1000 + 2*24*60*60*1000
        ) - 1 AS end_time,
        vehicle_type,
        LEAD(start_time) OVER(
                PARTITION BY
                    org_id,
                    device_id
                ORDER BY
                    start_time ASC
          ) IS NULL AS is_latest_config
    FROM (
        SELECT * FROM todays_configs_only_changes
        UNION ALL
        SELECT * FROM latest_configs
        UNION ALL
        SELECT * FROM latest_configs_start_today
    )
)

MERGE INTO datascience.imu_harsh_event_config_vehicle_type_intervals AS target
USING updates
ON target.date = updates.date
    AND target.start_time = updates.start_time
    AND target.device_id = updates.device_id
    AND target.org_id = updates.org_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""

spark.sql(query)
