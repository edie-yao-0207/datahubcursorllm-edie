-- Get instances of regen braking during the date range
WITH osd_regen_braking_ms AS (
    SELECT
        date,
        org_id,
        object_id,
        FROM_UNIXTIME(time  / 1000) AS time,
        value.int_value as duration_ms
    FROM
        kinesisstats.osDRegenBrakingMs
    WHERE
        date >= ${start_date}
        AND date < ${end_date}
),

-- Get instances of retarder braking during the date range
osd_aggregate_retarder_braking_ms AS (
    SELECT
        date,
        org_id,
        object_id,
        FROM_UNIXTIME(time / 1000) AS time,
        value.int_value as duration_ms
    FROM
        kinesisstats.osDAggregateDurationRetarderBrakingMs
    WHERE
        date >= ${start_date}
        AND date < ${end_date}
),

-- Combine regen and retarder braking instances into a single table to more easily aggregate them
combined_braking AS (
    SELECT
        date,
        org_id,
        object_id,
        time,
        duration_ms
    FROM
        osd_regen_braking_ms
    UNION ALL
    SELECT
        date,
        org_id,
        object_id,
        time,
        duration_ms
    FROM
        osd_aggregate_retarder_braking_ms
),

-- Aggregate the cumulative duration of all braking instances by hour
wear_free_braking_duration_ms_hourly AS (
    SELECT
        date,
        org_id,
        object_id,
        DATE_TRUNC('hour', time) AS interval_start, -- rounds down the time to the nearest hour
        SUM(duration_ms) AS hourly_aggregated_duration_ms
    FROM
        combined_braking
    GROUP BY
        date,
        org_id,
        object_id,
        interval_start
),

-- Get driver assignments for each vehicle during the date range
driver_assignments AS (
    SELECT
        org_id,
        device_id,
        driver_id,
        FROM_UNIXTIME(start_time / 1000) AS start_time,
        FROM_UNIXTIME(end_time / 1000) AS end_time
    FROM
        fuel_energy_efficiency_report.driver_assignments
)

SELECT
    ha.date,
    ha.org_id,
    ha.object_id AS device_id,
    COALESCE(da.driver_id, 0) AS driver_id,
    ha.interval_start,
    ha.interval_start + interval 1 hour AS interval_end,
    ha.hourly_aggregated_duration_ms as duration_ms
FROM
    wear_free_braking_duration_ms_hourly ha
LEFT JOIN
    driver_assignments da
ON
    ha.org_id = da.org_id
    AND ha.object_id = da.device_id
    AND ha.interval_start >= da.start_time
    AND ha.interval_start < da.end_time
ORDER BY
    ha.date, ha.org_id, ha.object_id, ha.interval_start
