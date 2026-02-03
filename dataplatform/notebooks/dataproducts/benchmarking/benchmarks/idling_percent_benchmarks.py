# Databricks notebook source
# MAGIC %md
# MAGIC # Idling Percent Benchmarks
# MAGIC This notebook calculates benchmark tables for idling percent. The benchmarks are calculated by looking at the previous 8 weeks using `playground.org_segments_v2`.
# MAGIC
# MAGIC The following tables are created:
# MAGIC * `playground.idling_percent_benchmarks_by_week`
# MAGIC
# MAGIC Requirement: Must run `org_segments` first to get the most up to date orgs for benchmark calculations!

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ignored_org_ids AS (
# MAGIC SELECT
# MAGIC     EXPLODE(
# MAGIC         ARRAY(1, 43810, 43509, 43811, 43518, 43519, 43511, 43512, 43513, 43514, 43801, 43515, 43516, 43517, 5289, 1782)
# MAGIC     ) AS org_id
# MAGIC );
# MAGIC
# MAGIC -- Skip engine state points with no state changes.
# MAGIC CREATE OR REPLACE TEMP VIEW engine_state_transitions AS (
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     object_id,
# MAGIC     time,
# MAGIC     CASE
# MAGIC       -- A databreak means the previous value should be considered an "end" which corresponds with the OFF state (0).
# MAGIC       WHEN LEAD(value.is_databreak) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) THEN 0
# MAGIC       -- Otherwise, just use the int_value.
# MAGIC       ELSE value.int_value
# MAGIC     END AS int_value,
# MAGIC     LAG(value.int_value) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS prev_int_value,
# MAGIC     value.is_databreak AS is_databreak
# MAGIC   FROM kinesisstats.osdenginestate
# MAGIC   WHERE date >= date_add(current_date(), -449)
# MAGIC     AND org_id NOT IN (SELECT org_id FROM ignored_org_ids)
# MAGIC   )
# MAGIC WHERE int_value != prev_int_value
# MAGIC   AND NOT is_databreak
# MAGIC );
# MAGIC
# MAGIC -- Map engine state transitions to intervals.
# MAGIC CREATE OR REPLACE TEMP VIEW engine_state_intervals AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   object_id,
# MAGIC   time AS start_ms,
# MAGIC   CASE
# MAGIC     WHEN int_value = 0 THEN "OFF"
# MAGIC     WHEN int_value = 1 THEN "ON"
# MAGIC     WHEN int_Value = 2 THEN "IDLE"
# MAGIC   END AS state,
# MAGIC   COALESCE(
# MAGIC     LEAD(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC),
# MAGIC     CAST(unix_timestamp(CAST (current_date() AS TIMESTAMP)) * 1000 AS BIGINT)
# MAGIC   ) AS end_ms
# MAGIC FROM engine_state_transitions
# MAGIC );
# MAGIC
# MAGIC -- Expand engine state intervals by covering hours.
# MAGIC CREATE OR REPLACE TEMP VIEW engine_state_intervals_by_covering_hour AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   object_id,
# MAGIC   state,
# MAGIC   start_ms,
# MAGIC   end_ms,
# MAGIC   EXPLODE(
# MAGIC     SEQUENCE(
# MAGIC       DATE_TRUNC('hour', from_unixtime(start_ms / 1000)),
# MAGIC       DATE_TRUNC('hour', from_unixtime(end_ms / 1000)), -- SEQUENCE() takes inclusive end time.
# MAGIC       INTERVAL 1 hour
# MAGIC     )
# MAGIC   ) AS hour_start
# MAGIC FROM engine_state_intervals
# MAGIC WHERE start_ms < end_ms
# MAGIC );
# MAGIC
# MAGIC -- Split engine state intervals by hour.
# MAGIC CREATE OR REPLACE TEMP VIEW engine_state_intervals_by_hour AS (
# MAGIC SELECT
# MAGIC   DATE(hour_start) AS date,
# MAGIC --   year(hour_start) AS year,
# MAGIC   case
# MAGIC     when month(hour_start) = 1 and weekofyear(hour_start) = 53 then year(hour_start)-1
# MAGIC     else year(hour_start)
# MAGIC   end as year, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   weekofyear(hour_start) as week,
# MAGIC   org_id,
# MAGIC   object_id,
# MAGIC   state,
# MAGIC   GREATEST(start_ms, CAST(unix_timestamp(hour_start) * 1000 AS BIGINT)) AS start_ms,
# MAGIC   LEAST(end_ms, CAST(unix_timestamp(hour_start + INTERVAL 1 HOUR) * 1000 AS BIGINT)) AS end_ms
# MAGIC FROM engine_state_intervals_by_covering_hour
# MAGIC );
# MAGIC
# MAGIC create or replace temp view engine_duration_by_week as
# MAGIC select
# MAGIC   week,
# MAGIC   year,
# MAGIC   org_id,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when state = "ON" or state = "IDLE" then end_ms-start_ms
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as on_duration_ms,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when state = "IDLE" then end_ms-start_ms
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as idle_duration_ms
# MAGIC from engine_state_intervals_by_hour
# MAGIC group by week, year, org_id;
# MAGIC
# MAGIC -- Expand digital input configurations.
# MAGIC CREATE OR REPLACE TEMP VIEW expanded_devices AS (
# MAGIC SELECT
# MAGIC   g.organization_id AS org_id,
# MAGIC   d.id AS device_id,
# MAGIC   d.digi1_type_id,
# MAGIC   d.digi2_type_id,
# MAGIC   EXPLODE_OUTER(d.device_settings_proto.digi_inputs.inputs) AS input
# MAGIC FROM productsdb.devices AS d
# MAGIC JOIN clouddb.groups AS g ON g.id = d.group_id
# MAGIC );
# MAGIC
# MAGIC -- Valid PTO equipment types.
# MAGIC CREATE OR REPLACE TEMP VIEW pto_device_types AS (
# MAGIC   VALUES (4),(5),(6),(7),(9),(10),(11)
# MAGIC );
# MAGIC
# MAGIC -- Find all devices and PTO ports.
# MAGIC CREATE OR REPLACE TEMP VIEW pto_devices AS (
# MAGIC SELECT org_id, device_id, 1 AS port FROM expanded_devices
# MAGIC WHERE digi1_type_id IN (SELECT * FROM pto_device_types)
# MAGIC UNION
# MAGIC SELECT org_id, device_id, 2 AS port FROM expanded_devices
# MAGIC WHERE digi2_type_id IN (SELECT * FROM pto_device_types)
# MAGIC UNION
# MAGIC SELECT org_id, device_id, input.port AS port FROM expanded_devices
# MAGIC WHERE input.input.input_type IN (SELECT * FROM pto_device_types)
# MAGIC );
# MAGIC
# MAGIC -- Digital input port on/off values.
# MAGIC CREATE OR REPLACE TEMP VIEW digio AS (
# MAGIC   SELECT *, 1 AS port FROM kinesisstats.osddigioinput1
# MAGIC   UNION SELECT *, 2 AS port FROM kinesisstats.osddigioinput2
# MAGIC   UNION SELECT *, 3 AS port FROM kinesisstats.osddigioinput3
# MAGIC   UNION SELECT *, 4 AS port FROM kinesisstats.osddigioinput4
# MAGIC   UNION SELECT *, 5 AS port FROM kinesisstats.osddigioinput5
# MAGIC   UNION SELECT *, 6 AS port FROM kinesisstats.osddigioinput6
# MAGIC   UNION SELECT *, 7 AS port FROM kinesisstats.osddigioinput7
# MAGIC   UNION SELECT *, 8 AS port FROM kinesisstats.osddigioinput8
# MAGIC   UNION SELECT *, 9 AS port FROM kinesisstats.osddigioinput9
# MAGIC   UNION SELECT *, 10 AS port FROM kinesisstats.osddigioinput10
# MAGIC  );
# MAGIC
# MAGIC -- Filter digio on/off values by valid PTO ports.
# MAGIC CREATE OR REPLACE TEMP VIEW pto_port_states AS (
# MAGIC SELECT
# MAGIC   pd.org_id,
# MAGIC   pd.device_id,
# MAGIC   pd.port,
# MAGIC   d.time,
# MAGIC   d.value.int_value AS value
# MAGIC FROM pto_devices AS pd
# MAGIC JOIN digio as d
# MAGIC ON pd.port = d.port
# MAGIC AND pd.org_id = d.org_id
# MAGIC AND pd.device_id = d.object_id
# MAGIC WHERE date >= date_add(current_date(), -449)
# MAGIC   AND d.org_id NOT IN (SELECT org_id FROM ignored_org_ids)
# MAGIC );
# MAGIC
# MAGIC -- Filter PTO values for ON/OFF transitions per port.
# MAGIC CREATE OR REPLACE TEMP VIEW pto_port_state_transitions AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   port,
# MAGIC   time,
# MAGIC   value
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     device_id,
# MAGIC     port,
# MAGIC     time,
# MAGIC     value,
# MAGIC     LAG(value) OVER (PARTITION BY org_id, device_id, port ORDER BY time ASC) AS prev_value
# MAGIC   FROM pto_port_states
# MAGIC )
# MAGIC WHERE value != prev_value
# MAGIC );
# MAGIC
# MAGIC -- Combine PTO intervals.
# MAGIC -- For example, a device has two equipments:
# MAGIC --   equipment A is in use on port 1 between (10, 50),
# MAGIC --   equipment B is in use on port 2 between (30, 70),
# MAGIC -- we want to find the union of these intervals, i.e. (10, 70),
# MAGIC -- when we calculate PTO intervals, so we don't double count
# MAGIC -- duration (30, 50) when we report total ON time.
# MAGIC --
# MAGIC -- Perform a running sum of "transition" for each device.
# MAGIC -- The total represents "number of overlapping intervals".
# MAGIC -- When total=1, start an interval. When total=0, close the interval.
# MAGIC CREATE OR REPLACE TEMP VIEW pto_transition_running_sum AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   time,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN value = 1 THEN 1
# MAGIC       -- When the first value is not equal to 1, avoid starting the total at -1 by returning 0 instead.
# MAGIC       WHEN time = FIRST_VALUE(time) OVER (PARTITION BY org_id, device_id ORDER BY time) THEN 0
# MAGIC       ELSE -1
# MAGIC     END
# MAGIC   ) OVER (PARTITION BY org_id, device_id ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total
# MAGIC FROM pto_port_state_transitions
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW pto_on_intervals AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   start_ms,
# MAGIC   end_ms
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     device_id,
# MAGIC     total,
# MAGIC     time AS start_ms,
# MAGIC     COALESCE(
# MAGIC       LEAD(time) OVER (PARTITION BY org_id, device_id ORDER BY time ASC),
# MAGIC       CAST(unix_timestamp(CAST (current_date() AS TIMESTAMP)) * 1000 AS BIGINT)
# MAGIC     ) AS end_ms
# MAGIC   FROM (
# MAGIC     -- Filter open and close events.
# MAGIC     SELECT
# MAGIC       org_id,
# MAGIC       device_id,
# MAGIC       time,
# MAGIC       total
# MAGIC     FROM pto_transition_running_sum
# MAGIC     WHERE total = 0 OR total = 1
# MAGIC   )
# MAGIC )
# MAGIC WHERE total = 1
# MAGIC );
# MAGIC
# MAGIC -- Filter engine state intervals by IDLE state.
# MAGIC CREATE OR REPLACE TEMP VIEW engine_idle_intervals AS (
# MAGIC SELECT * FROM engine_state_intervals WHERE state = "IDLE"
# MAGIC );
# MAGIC
# MAGIC -- Find overlap between engine idle intervals and PTO intervals.
# MAGIC CREATE OR REPLACE TEMP VIEW aux_engine_state_intervals AS (
# MAGIC SELECT
# MAGIC   pto.org_id,
# MAGIC   pto.device_id,
# MAGIC   GREATEST(pto.start_ms, idle.start_ms) AS interval_start,
# MAGIC   LEAST(pto.end_ms, idle.end_ms) AS interval_end
# MAGIC FROM pto_on_intervals pto
# MAGIC JOIN engine_idle_intervals idle
# MAGIC ON pto.org_id = idle.org_id
# MAGIC AND pto.device_id = idle.object_id
# MAGIC AND pto.start_ms <= idle.end_ms AND pto.end_ms >= idle.start_ms
# MAGIC );
# MAGIC
# MAGIC -- Expand aux_engine_state_intervals by covering hours.
# MAGIC CREATE OR REPLACE TEMP VIEW aux_engine_state_intervals_by_covering_hour AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   interval_start,
# MAGIC   interval_end,
# MAGIC   EXPLODE(
# MAGIC     SEQUENCE(
# MAGIC       DATE_TRUNC('hour', FROM_UNIXTIME(interval_start / 1000)),
# MAGIC       DATE_TRUNC('hour', FROM_UNIXTIME(interval_end / 1000)), -- SEQUENCE() takes inclusive end time.
# MAGIC       INTERVAL 1 hour
# MAGIC     )
# MAGIC   ) AS hour_start
# MAGIC FROM aux_engine_state_intervals
# MAGIC WHERE interval_start < interval_end
# MAGIC );
# MAGIC
# MAGIC -- Calculate aux_engine_state intervals, split at hour boundary.
# MAGIC CREATE OR REPLACE TEMP VIEW aux_engine_state_intervals_by_hour AS (
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   DATE(hour_start) AS date,
# MAGIC --   year(hour_start) AS year,
# MAGIC   case
# MAGIC     when month(hour_start) = 1 and weekofyear(hour_start) = 53 then year(hour_start)-1
# MAGIC     else year(hour_start)
# MAGIC   end as year, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   weekofyear(hour_start) as week,
# MAGIC   hour_start AS interval_start,
# MAGIC   GREATEST(interval_start,  UNIX_TIMESTAMP(hour_start)         * 1000) AS aux_during_idle_start_ms,
# MAGIC   LEAST(   interval_end,   (UNIX_TIMESTAMP(hour_start) + 3600) * 1000) AS aux_during_idle_end_ms
# MAGIC FROM aux_engine_state_intervals_by_covering_hour
# MAGIC );
# MAGIC
# MAGIC create or replace temp view aux_engine_state_duration_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   year,
# MAGIC   week,
# MAGIC   sum(aux_during_idle_end_ms - aux_during_idle_start_ms) as aux_during_idle_ms
# MAGIC from aux_engine_state_intervals_by_hour
# MAGIC group by org_id, week, year;
# MAGIC
# MAGIC create or replace temp view engine_duration_with_aux_by_week as
# MAGIC select
# MAGIC   ed.org_id,
# MAGIC   ed.year,
# MAGIC   ed.week,
# MAGIC   on_duration_ms,
# MAGIC   idle_duration_ms,
# MAGIC   coalesce(aux_during_idle_ms, 0) as aux_during_idle_ms
# MAGIC from engine_duration_by_week as ed
# MAGIC left join aux_engine_state_duration_by_week as ad
# MAGIC   on ed.org_id = ad.org_id
# MAGIC   and ed.week = ad.week
# MAGIC   and ed.year = ad.year
# MAGIC where on_duration_ms > 0;
# MAGIC
# MAGIC create or replace temp view idling_percent_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   concat(year,'-', right(concat('00',CAST(week as string)),2)) as week,
# MAGIC   on_duration_ms,
# MAGIC   idle_duration_ms,
# MAGIC   aux_during_idle_ms,
# MAGIC   (idle_duration_ms-aux_during_idle_ms)/on_duration_ms * 100 as idling_percent
# MAGIC from engine_duration_with_aux_by_week;
# MAGIC
# MAGIC -- Label each org with their cohort
# MAGIC create or replace temp view org_cohort_idling_percent_by_week as
# MAGIC select
# MAGIC   ip.*,
# MAGIC   cohort_id
# MAGIC from idling_percent_by_week as ip
# MAGIC inner join playground.org_segments_v2 as os
# MAGIC   on ip.org_id = os.org_id;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view idling_percent_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(on_duration_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_on_duration_ms_past_x_weeks,
# MAGIC   sum(idle_duration_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_idle_duration_ms_past_x_weeks,
# MAGIC   sum(aux_during_idle_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_aux_during_idle_ms_past_x_weeks
# MAGIC from org_cohort_idling_percent_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view idling_percent_benchmark_metrics_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   (total_idle_duration_ms_past_x_weeks - total_aux_during_idle_ms_past_x_weeks)/total_on_duration_ms_past_x_weeks * 100 as idling_percent
# MAGIC from idling_percent_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view idling_percent_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(idling_percent) as mean,
# MAGIC   approx_percentile(idling_percent, 0.5) as median,
# MAGIC   approx_percentile(idling_percent, 0.1) as top10_percentile
# MAGIC from idling_percent_benchmark_metrics_by_week
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(idling_percent) as mean,
# MAGIC       approx_percentile(idling_percent, 0.5) as median,
# MAGIC       approx_percentile(idling_percent, 0.1) as top10_percentile
# MAGIC   from idling_percent_benchmark_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists playground.idling_percent_benchmarks_by_week;
# MAGIC create table playground.idling_percent_benchmarks_by_week(
# MAGIC   select * from idling_percent_benchmarks_by_week
# MAGIC );

# COMMAND ----------
