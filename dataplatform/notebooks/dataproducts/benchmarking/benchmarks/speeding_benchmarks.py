# Databricks notebook source
# MAGIC %md
# MAGIC # Speeding Benchmarks
# MAGIC This notebook calculates benchmark tables for speeding. The benchmarks are calculated by looking at the previous 8 weeks using `playground.org_segments_v2`.
# MAGIC
# MAGIC The following tables are created:
# MAGIC * `playground.light_speeding_benchmarks_by_week`
# MAGIC * `playground.moderate_speeding_benchmarks_by_week`
# MAGIC * `playground.heavy_speeding_benchmarks_by_week`
# MAGIC * `playground.severe_speeding_benchmarks_by_week`
# MAGIC * `playground.all_speeding_benchmarks_by_week`
# MAGIC
# MAGIC Requirement: Must run `org_segments` first to get the most up to date orgs for benchmark calculations!

# COMMAND ----------

# MAGIC %sql
# MAGIC --Speed data
# MAGIC /* Trips data for speeding */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449);
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_speeding_mph AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   week,
# MAGIC   COALESCE(SUM(proto.end.time - proto.start.time),0) AS trip_duration_ms,
# MAGIC   COALESCE(SUM(proto.trip_speeding_mph.not_speeding_ms),0) AS not_speeding_ms,
# MAGIC   COALESCE(SUM(proto.trip_speeding_mph.light_speeding_ms),0) AS light_speeding_ms,
# MAGIC   COALESCE(SUM(proto.trip_speeding_mph.moderate_speeding_ms),0) AS moderate_speeding_ms,
# MAGIC   COALESCE(SUM(proto.trip_speeding_mph.heavy_speeding_ms),0) AS heavy_speeding_ms,
# MAGIC   COALESCE(SUM(proto.trip_speeding_mph.severe_speeding_ms),0) AS severe_speeding_ms
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, week;
# MAGIC
# MAGIC create or replace temporary view org_speeding_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   week,
# MAGIC   SUM(trip_duration_ms) as total_trip_duration_ms,
# MAGIC   SUM(not_speeding_ms) as total_not_speeding_ms,
# MAGIC   SUM(light_speeding_ms) as total_light_speeding_ms,
# MAGIC   SUM(moderate_speeding_ms) as total_moderate_speeding_ms,
# MAGIC   SUM(heavy_speeding_ms) as total_heavy_speeding_ms,
# MAGIC   SUM(severe_speeding_ms) as total_severe_speeding_ms
# MAGIC from device_speeding_mph
# MAGIC group by org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view org_speeding_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   week,
# MAGIC   sum(total_trip_duration_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_trip_duration_ms,
# MAGIC   sum(total_not_speeding_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_not_speeding_ms,
# MAGIC   sum(total_light_speeding_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_light_speeding_ms,
# MAGIC   sum(total_moderate_speeding_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_moderate_speeding_ms,
# MAGIC   sum(total_heavy_speeding_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_heavy_speeding_ms,
# MAGIC   sum(total_severe_speeding_ms) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_severe_speeding_ms
# MAGIC from org_speeding_by_week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view org_speeding_metrics_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   week,
# MAGIC   total_light_speeding_ms/total_trip_duration_ms * 100 as light_speeding_pct,
# MAGIC   total_moderate_speeding_ms/total_trip_duration_ms * 100 as moderate_speeding_pct,
# MAGIC   total_heavy_speeding_ms/total_trip_duration_ms * 100 as heavy_speeding_pct,
# MAGIC   total_severe_speeding_ms/total_trip_duration_ms* 100 as severe_speeding_pct
# MAGIC from org_speeding_past_x_weeks;
# MAGIC
# MAGIC -- Label each org with their cohort
# MAGIC create or replace temp view org_cohort_speeding_metrics_by_week as
# MAGIC select
# MAGIC   om.*,
# MAGIC   light_speeding_pct + moderate_speeding_pct + heavy_speeding_pct + severe_speeding_pct as all_speeding_pct,
# MAGIC   cohort_id
# MAGIC from org_speeding_metrics_by_week as om
# MAGIC inner join playground.org_segments_v2 as os
# MAGIC   on om.org_id = os.org_id;
# MAGIC
# MAGIC cache table org_cohort_speeding_metrics_by_week;
# MAGIC
# MAGIC -- Benchmark for individual metric
# MAGIC create or replace temp view light_speeding_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(light_speeding_pct) as mean,
# MAGIC   approx_percentile(light_speeding_pct,0.5) as median,
# MAGIC   approx_percentile(light_speeding_pct,0.1) as top10_percentile
# MAGIC from org_cohort_speeding_metrics_by_week
# MAGIC group by cohort_id,week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(light_speeding_pct) as mean,
# MAGIC       approx_percentile(light_speeding_pct, 0.5) as median,
# MAGIC       approx_percentile(light_speeding_pct, 0.1) as top10_percentile
# MAGIC   from org_cohort_speeding_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id,week;
# MAGIC
# MAGIC -- Benchmark for individual metric
# MAGIC create or replace temp view moderate_speeding_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(moderate_speeding_pct) as mean,
# MAGIC   approx_percentile(moderate_speeding_pct,0.5) as median,
# MAGIC   approx_percentile(moderate_speeding_pct,0.1) as top10_percentile
# MAGIC from org_cohort_speeding_metrics_by_week
# MAGIC group by cohort_id,week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(moderate_speeding_pct) as mean,
# MAGIC       approx_percentile(moderate_speeding_pct, 0.5) as median,
# MAGIC       approx_percentile(moderate_speeding_pct, 0.1) as top10_percentile
# MAGIC   from org_cohort_speeding_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id,week;
# MAGIC
# MAGIC -- Benchmark for individual metric
# MAGIC create or replace temp view heavy_speeding_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(heavy_speeding_pct) as mean,
# MAGIC   approx_percentile(heavy_speeding_pct,0.5) as median,
# MAGIC   approx_percentile(heavy_speeding_pct,0.1) as top10_percentile
# MAGIC from org_cohort_speeding_metrics_by_week
# MAGIC group by cohort_id,week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(heavy_speeding_pct) as mean,
# MAGIC       approx_percentile(heavy_speeding_pct, 0.5) as median,
# MAGIC       approx_percentile(heavy_speeding_pct, 0.1) as top10_percentile
# MAGIC   from org_cohort_speeding_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id,week;
# MAGIC
# MAGIC -- Benchmark for individual metric
# MAGIC create or replace temp view severe_speeding_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(severe_speeding_pct) as mean,
# MAGIC   approx_percentile(severe_speeding_pct,0.5) as median,
# MAGIC   approx_percentile(severe_speeding_pct,0.1) as top10_percentile
# MAGIC from org_cohort_speeding_metrics_by_week
# MAGIC group by cohort_id,week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(severe_speeding_pct) as mean,
# MAGIC       approx_percentile(severe_speeding_pct, 0.5) as median,
# MAGIC       approx_percentile(severe_speeding_pct, 0.1) as top10_percentile
# MAGIC   from org_cohort_speeding_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id,week;
# MAGIC
# MAGIC -- Benchmark for individual metric
# MAGIC create or replace temp view all_speeding_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(all_speeding_pct) as mean,
# MAGIC   approx_percentile(all_speeding_pct,0.5) as median,
# MAGIC   approx_percentile(all_speeding_pct,0.1) as top10_percentile
# MAGIC from org_cohort_speeding_metrics_by_week
# MAGIC group by cohort_id,week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(all_speeding_pct) as mean,
# MAGIC       approx_percentile(all_speeding_pct, 0.5) as median,
# MAGIC       approx_percentile(all_speeding_pct, 0.1) as top10_percentile
# MAGIC   from org_cohort_speeding_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id,week;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists playground.severe_speeding_benchmarks_by_week;
# MAGIC create table playground.severe_speeding_benchmarks_by_week(
# MAGIC   select * from severe_speeding_benchmarks_by_week
# MAGIC );
# MAGIC
# MAGIC drop table if exists playground.heavy_speeding_benchmarks_by_week;
# MAGIC create table playground.heavy_speeding_benchmarks_by_week(
# MAGIC   select * from heavy_speeding_benchmarks_by_week
# MAGIC );
# MAGIC
# MAGIC drop table if exists playground.moderate_speeding_benchmarks_by_week;
# MAGIC create table playground.moderate_speeding_benchmarks_by_week(
# MAGIC   select * from moderate_speeding_benchmarks_by_week
# MAGIC );
# MAGIC
# MAGIC drop table if exists playground.light_speeding_benchmarks_by_week;
# MAGIC create table playground.light_speeding_benchmarks_by_week(
# MAGIC   select * from light_speeding_benchmarks_by_week
# MAGIC );
# MAGIC
# MAGIC drop table if exists playground.all_speeding_benchmarks_by_week;
# MAGIC create table playground.all_speeding_benchmarks_by_week(
# MAGIC   select * from all_speeding_benchmarks_by_week
# MAGIC );

# COMMAND ----------
