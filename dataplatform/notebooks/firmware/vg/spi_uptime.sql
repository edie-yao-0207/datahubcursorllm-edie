-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW attached_modi_device_days AS (
  SELECT
    dev.date,
    dev.org_id,
    dev.device_id
  FROM data_analytics.vg3x_daily_summary as dev
  WHERE
    dev.has_modi = TRUE
    AND
    dev.product_id in (24, 35)
    AND
    dev.date >= DATE_SUB(CURRENT_DATE(), 10)
    AND
    dev.date <= DATE_SUB(CURRENT_DATE(), 1)
)
;


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW integrated_mcu_device_days AS (
  SELECT
    dev.date,
    dev.org_id,
    dev.device_id
  FROM data_analytics.vg3x_daily_summary as dev
  WHERE
    dev.product_id in (53, 89, 178)
    AND
    dev.date >= DATE_SUB(CURRENT_DATE(), 10)
    AND
    dev.date <= DATE_SUB(CURRENT_DATE(), 1)
)
;


-- COMMAND ----------

-- Create ranges by using LEAD() on date, time, is_databreak, is_start, and is_end
--   is_databreak=true means that row can't be used as a start or end time for a range
--   is_start=true means that row can't be used as an end time for a range, and
--   is_end=true means that row can't be used as a start time for a range.
-- So we only want ranges where:
--   start_is_databreak and end_is_databreak are false,
--   start_is_end is false, and
--   end_is_start is false.
-- The rest of the time, we can't be sure about what was happening, so we need to filter those out.
--
-- These ranges can span multiple days, so we need to split those ranges into multiple rows. Do so
-- using a range join with a list of all dates in our lookback filter on the start and end dates.
-- This duplicates the multi-day range rows for each date the range spans but we still need to trim
-- the start and end times so they are bound within each day.
--   The new end time becomes the least of the end time and the end of the joined date
--   The new start time becomes the greatest of the start time and the start of the joined date

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW spi_comm_state_ranges AS (

  WITH
    unfiltered_ranges AS (
      SELECT
        mcs.org_id,
        mcs.object_id,
        mcs.`date` AS `start_date`,
        mcs.`time` AS start_time,
        LEAD(mcs.`date`, 1) OVER (PARTITION BY mcs.object_id ORDER BY mcs.`date`, mcs.`time`) AS end_date,
        LEAD(mcs.`time`, 1) OVER (PARTITION BY mcs.object_id ORDER BY mcs.`date`, mcs.`time`) AS end_time,
        mcs.value.is_databreak AS start_is_databreak,
        mcs.value.is_start AS start_is_start,
        mcs.value.is_end AS start_is_end,
        LEAD(mcs.value.is_databreak, 1) OVER (PARTITION BY mcs.object_id ORDER BY mcs.`date`, mcs.`time`) AS end_is_databreak,
        LEAD(mcs.value.is_start, 1) OVER (PARTITION BY mcs.object_id ORDER BY mcs.`date`, mcs.`time`) AS end_is_start,
        LEAD(mcs.value.is_end, 1) OVER (PARTITION BY mcs.object_id ORDER BY mcs.`date`, mcs.`time`) AS end_is_end,
        COALESCE(mcs.value.int_value, 0) AS spi_comm_state
      FROM
        kinesisstats.osDModiCommState mcs
      INNER JOIN
        (
          SELECT * FROM attached_modi_device_days
          UNION
          SELECT * FROM integrated_mcu_device_days
        ) dev
        ON
          mcs.object_id = dev.device_id
          AND
          mcs.`date` = dev.`date`
    )

  SELECT
    ufr.org_id,
    ufr.object_id,
    interval.interval_date AS `date`,
    GREATEST(ufr.start_time, UNIX_TIMESTAMP(interval.interval_date) * 1000) AS start_time,
    LEAST(ufr.end_time, UNIX_TIMESTAMP(DATE_ADD(interval.interval_date, 1)) * 1000) AS end_time,
    LEAST(ufr.end_time, UNIX_TIMESTAMP(DATE_ADD(interval.interval_date, 1)) * 1000) - GREATEST(ufr.start_time, UNIX_TIMESTAMP(interval.interval_date) * 1000) AS duration,
    ufr.spi_comm_state
  FROM unfiltered_ranges ufr
  LEFT JOIN
    (
      SELECT
        EXPLODE(SEQUENCE(DATE_SUB(CURRENT_DATE(), 10), CURRENT_DATE(), INTERVAL 1 DAY)) AS interval_date
    ) interval
  ON
    interval.interval_date BETWEEN ufr.start_date AND ufr.end_date
  WHERE
    ufr.start_is_databreak = FALSE
    AND
    ufr.start_is_end = FALSE
    AND
    ufr.end_is_databreak = FALSE
    AND
    ufr.end_is_start = FALSE

)
;


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW power_state_ranges AS (

  WITH
    unfiltered_ranges AS (
      (
        SELECT
          pwr.org_id,
          pwr.object_id,
          pwr.`date` AS `start_date`,
          pwr.`time` AS start_time,
          LEAD(pwr.`date`, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_date,
          LEAD(pwr.`time`, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_time,
          pwr.value.is_databreak AS start_is_databreak,
          pwr.value.is_start AS start_is_start,
          pwr.value.is_end AS start_is_end,
          LEAD(pwr.value.is_databreak, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_databreak,
          LEAD(pwr.value.is_start, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_start,
          LEAD(pwr.value.is_end, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_end,
          IF(COALESCE(pwr.value.int_value, 0) = 1, 1, 0) AS power_state -- 1: Normal power
        FROM
          kinesisstats.osDPowerState pwr
        INNER JOIN
          attached_modi_device_days dev
          ON
            pwr.object_id = dev.device_id
            AND
            pwr.`date` = dev.`date`
      )
      UNION
      (
        SELECT
          pwr.org_id,
          pwr.object_id,
          pwr.`date` AS `start_date`,
          pwr.`time` AS start_time,
          LEAD(pwr.`date`, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_date,
          LEAD(pwr.`time`, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_time,
          pwr.value.is_databreak AS start_is_databreak,
          pwr.value.is_start AS start_is_start,
          pwr.value.is_end AS start_is_end,
          LEAD(pwr.value.is_databreak, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_databreak,
          LEAD(pwr.value.is_start, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_start,
          LEAD(pwr.value.is_end, 1) OVER (PARTITION BY pwr.object_id ORDER BY pwr.`date`, pwr.`time`) AS end_is_end,
          IF(COALESCE(pwr.value.int_value, 0) NOT IN (2, 10), 1, 0) AS power_state -- 2: OFF, 10: Rebooting to read only
        FROM
          kinesisstats.osDPowerState pwr
        INNER JOIN
          integrated_mcu_device_days dev
          ON
            pwr.object_id = dev.device_id
            AND
            pwr.`date` = dev.`date`
      )
    )

  SELECT
    ufr.org_id,
    ufr.object_id,
    interval.interval_date AS `date`,
    GREATEST(ufr.start_time, UNIX_TIMESTAMP(interval.interval_date) * 1000) AS start_time,
    LEAST(ufr.end_time, UNIX_TIMESTAMP(DATE_ADD(interval.interval_date, 1)) * 1000) AS end_time,
    LEAST(ufr.end_time, UNIX_TIMESTAMP(DATE_ADD(interval.interval_date, 1)) * 1000) - GREATEST(ufr.start_time, UNIX_TIMESTAMP(interval.interval_date) * 1000) AS duration,
    ufr.power_state
  FROM unfiltered_ranges ufr
  LEFT JOIN
    (
      SELECT
        EXPLODE(SEQUENCE(DATE_SUB(CURRENT_DATE(), 10), CURRENT_DATE(), INTERVAL 1 DAY)) AS interval_date
    ) interval
  ON
    interval.interval_date BETWEEN ufr.start_date AND ufr.end_date
  WHERE
    ufr.start_is_databreak = FALSE
    AND
    ufr.start_is_end = FALSE
    AND
    ufr.end_is_databreak = FALSE
    AND
    ufr.end_is_start = FALSE

)
;


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW spi_uptime AS (

  WITH
    spi_uptime_durations AS (
      SELECT
        pwr.date,
        pwr.org_id,
        pwr.object_id,
        pwr.start_time,
        pwr.end_time,
        pwr.power_state,
        pwr.duration,
        SUM(LEAST(comms.end_time, pwr.end_time) - GREATEST(comms.start_time, pwr.start_time)) AS comms_duration
      FROM
        power_state_ranges pwr
      LEFT JOIN
        spi_comm_state_ranges comms
      ON
        pwr.org_id = comms.org_id
        AND
        pwr.object_id = comms.object_id
        AND
        comms.start_time <= pwr.end_time
        AND
        comms.end_time >= pwr.start_time
        AND
        comms.end_time IS NOT NULL
        AND
        comms.start_time IS NOT NULL
        AND
        comms.spi_comm_state = 1
      GROUP BY
        pwr.date,
        pwr.org_id,
        pwr.object_id,
        pwr.start_time,
        pwr.end_time,
        pwr.power_state,
        pwr.duration
    )

  SELECT
    date,
    org_id,
    object_id AS device_id,
    sum(comms_duration) AS spi_comms_time,
    sum(duration) AS normal_power_time
  FROM spi_uptime_durations
  WHERE power_state = 1
  GROUP BY
    date,
    org_id,
    object_id

)
;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.spi_uptime
USING DELTA
PARTITIONED BY (date)
SELECT * FROM spi_uptime
;


-- COMMAND ----------

ALTER TABLE data_analytics.spi_uptime SET TBLPROPERTIES ('comment' = 'Uptime computations for the connection between a VG and its integrated MCU or between a VG without an integrated MCU and its connected diagnostic hub');
ALTER TABLE data_analytics.spi_uptime CHANGE date COMMENT 'UTC date for which the data was computed';
ALTER TABLE data_analytics.spi_uptime CHANGE org_id COMMENT 'ID of the Samsara organization';
ALTER TABLE data_analytics.spi_uptime CHANGE device_id COMMENT 'ID of the Samsara device';
ALTER TABLE data_analytics.spi_uptime CHANGE spi_comms_time COMMENT 'The total amount of time (milliseconds) that the communication was active';
ALTER TABLE data_analytics.spi_uptime CHANGE normal_power_time COMMENT 'The total amount of time (milliseconds) that the communication was expected to be active. For devices with integrated MCUs this is the entire time the device was powered. For devices with connected diagnostic hubs, this is the time the device was in normal power mode';


-- COMMAND ----------

-- Because this data is based on ranges that can span multiple days, as our lookback filter
-- window moves it's possible that a blind INSERT OVERWRITE will clear data for a device or
-- replace it with data that is less complete. To mitigate this we join with our output table
-- and select the data on the dates that are the most complete. Unfortunately the definition of
-- more complete is further complicated by the fact that we are joining two sets of ranges that
-- are logged asynchronously from each other so it's theoretically possible to have a complete
-- set of one and not the other other. Since spi_comms_time depends on normal_power_time, use
-- the data from the table that has the larger spi_comms_time, which should imply that both sets
-- of ranges are the most complete.
-- To quantify how these ranges impact the output I checked all of the osDModiCommState range
-- durations for the past year and found only 0.05% were larger than the current 10 day filter
-- being used. This didn't take into account the is_databreak, is_start, and is_end flags so it's
-- possible that a significant portion of that 0.05% is ranges that we don't care about, like if
-- the gateway is unpowered for a long time. In any case, I don't think missing 0.05% of data is
-- enough to skew these stats significantly
CREATE OR REPLACE TEMP VIEW spi_uptime_updates AS (
  SELECT
    date,
    org_id,
    device_id,
    -- If either of the values in the `>` comparison are NULL then the expression always evaluates to FALSE.
    IF(new.spi_comms_time > old.spi_comms_time OR old.spi_comms_time IS NULL, new.spi_comms_time, old.spi_comms_time) AS spi_comms_time,
    IF(new.spi_comms_time > old.spi_comms_time OR old.spi_comms_time IS NULL, new.normal_power_time, old.normal_power_time) AS normal_power_time
  FROM
    spi_uptime new
  FULL JOIN
    data_analytics.spi_uptime old
  USING
    (date, org_id, device_id)
)

-- COMMAND ----------

-- Using dynamic partition overwrite mode means we don't need to specify which partitions to overwrite when using INSERT OVERWRITE
-- More info: https://spark.apache.org/docs/latest/configuration.html (search spark.sql.sources.partitionOverwriteMode)
SET spark.sql.sources.partitionOverwriteMode = dynamic
;


-- COMMAND ----------

INSERT OVERWRITE TABLE data_analytics.spi_uptime
SELECT * FROM spi_uptime_updates
;

