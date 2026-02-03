# Databricks notebook source
# MAGIC %md
# MAGIC # Harsh Events Benchmarks
# MAGIC This notebook calculates benchmark tables for harsh events. The benchmarks are calculated by looking at the previous 8 weeks using `playground.org_segments_v2`.
# MAGIC
# MAGIC The following tables are created:
# MAGIC * `playground.harsh_accel_benchmarks_by_week_osda`
# MAGIC * `playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs`
# MAGIC * `playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs`
# MAGIC * `playground.harsh_brake_benchmarks_by_week_osda`
# MAGIC * `playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs`
# MAGIC * `playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs`
# MAGIC * `playground.harsh_turn_benchmarks_by_week_osda`
# MAGIC * `playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs`
# MAGIC * `playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs`
# MAGIC * `playground.harsh_events_all_benchmarks_by_week_osda`
# MAGIC * `playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs`
# MAGIC * `playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs`
# MAGIC
# MAGIC Requirement: Must run `org_segments` first to get the most up to date orgs for benchmark calculations!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get orgs that set a custom threshold
# MAGIC create or replace temporary view custom_threshold_orgs as (
# MAGIC   select
# MAGIC     org_id
# MAGIC   from
# MAGIC     safetydb_shards.org_threshold_scalars
# MAGIC   where
# MAGIC     (use_advanced_scalars = 1 and
# MAGIC     advanced_scalars.harsh_accel_x_threshold_scalars.passenger != 1 and
# MAGIC     advanced_scalars.harsh_accel_x_threshold_scalars.light_duty != 1 and
# MAGIC     advanced_scalars.harsh_accel_x_threshold_scalars.heavy_duty != 1
# MAGIC     )
# MAGIC   or
# MAGIC     (use_advanced_scalars = 0 and
# MAGIC     harsh_accel_threshold_scalar != 1
# MAGIC     )
# MAGIC );
# MAGIC
# MAGIC cache table custom_threshold_orgs;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Accel

# COMMAND ----------

# MAGIC %md
# MAGIC ### osDAccelerometer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh accel data
# MAGIC create or replace temporary view harsh_accel_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.object_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when value.proto_value.accelerometer_event.harsh_accel_type = 1 then 1 else 0 end as is_harsh_accel
# MAGIC   from kinesisstats.osdAccelerometer as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.object_id = c.device_id
# MAGIC   where
# MAGIC     value.proto_value.accelerometer_event.harsh_accel_type in (1) and
# MAGIC     value.proto_value.accelerometer_event.last_gps.speed >= 4344.8812095 and
# MAGIC     value.is_databreak = 'false' and
# MAGIC     value.is_end = 'false' and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_accel_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_accel) as total_harsh_accel
# MAGIC   from harsh_accel_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_accel_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_accel, 0) as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_accel_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_accel_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_accel as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from harsh_accel_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_accel_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) as total_harsh_accel,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_accel_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_accel_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_accel_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_benchmark_metrics_by_week_osda as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_accel_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_accel_events_per_1000mi
# MAGIC from harsh_accel_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_accel_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_accel_benchmark_metrics_by_week_osda
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_accel_benchmark_metrics_by_week_osda
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_accel_benchmarks_by_week_osda;
# MAGIC create table playground.harsh_accel_benchmarks_by_week_osda(
# MAGIC   select * from harsh_accel_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB including custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh accel data
# MAGIC create or replace temporary view harsh_accel_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 1 then 1 else 0 end as is_harsh_accel
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (1) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_accel_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_accel) as total_harsh_accel
# MAGIC   from harsh_accel_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_accel_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_accel, 0) as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_accel_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_accel_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_accel as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from harsh_accel_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_accel_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) as total_harsh_accel,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_accel_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_accel_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_accel_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_accel_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_accel_events_per_1000mi
# MAGIC from harsh_accel_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_accel_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_accel_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_accel_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs;
# MAGIC create table playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs(
# MAGIC   select * from harsh_accel_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB exluding custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh accel data
# MAGIC create or replace temporary view harsh_accel_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 1 then 1 else 0 end as is_harsh_accel
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (1) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35) and
# MAGIC     a.org_id not in (select cto.org_id from custom_threshold_orgs cto where cto.org_id == a.org_id);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_accel_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_accel) as total_harsh_accel
# MAGIC   from harsh_accel_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_accel_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_accel, 0) as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_accel_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_accel_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_accel as total_harsh_accel,
# MAGIC   total_distance_mi
# MAGIC from harsh_accel_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_accel_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) as total_harsh_accel,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_accel_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_accel) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_accel_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_accel_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_accel_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_accel_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_accel_events_per_1000mi
# MAGIC from harsh_accel_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_accel_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_accel_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_accel_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_accel_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_accel_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs;
# MAGIC create table playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs(
# MAGIC   select * from harsh_accel_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Brake

# COMMAND ----------

# MAGIC %md
# MAGIC ### osDAccelerometer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh brake data
# MAGIC create or replace temporary view harsh_brake_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.object_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when value.proto_value.accelerometer_event.harsh_accel_type = 2 then 1 else 0 end as is_harsh_brake
# MAGIC   from kinesisstats.osdAccelerometer as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.object_id = c.device_id
# MAGIC   where
# MAGIC     value.proto_value.accelerometer_event.harsh_accel_type in (2) and
# MAGIC     value.proto_value.accelerometer_event.last_gps.speed >= 4344.8812095 and
# MAGIC     value.is_databreak = 'false' and
# MAGIC     value.is_end = 'false' and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_brake_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_brake) as total_harsh_brake
# MAGIC   from harsh_brake_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_brake_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_brake, 0) as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_brake_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_brake_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_brake as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from harsh_brake_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_brake_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) as total_harsh_brake,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_brake_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_brake_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_brake_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_benchmark_metrics_by_week_osda as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_brake_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_brake_events_per_1000mi
# MAGIC from harsh_brake_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_brake_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_brake_benchmark_metrics_by_week_osda
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_brake_benchmark_metrics_by_week_osda
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_brake_benchmarks_by_week_osda;
# MAGIC create table playground.harsh_brake_benchmarks_by_week_osda(
# MAGIC   select * from harsh_brake_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB including custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh brake data
# MAGIC create or replace temporary view harsh_brake_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 2 then 1 else 0 end as is_harsh_brake
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (2) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_brake_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_brake) as total_harsh_brake
# MAGIC   from harsh_brake_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_brake_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_brake, 0) as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_brake_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_brake_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_brake as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from harsh_brake_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_brake_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) as total_harsh_brake,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_brake_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_brake_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_brake_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_brake_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_brake_events_per_1000mi
# MAGIC from harsh_brake_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_brake_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_brake_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_brake_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs;
# MAGIC create table playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs(
# MAGIC   select * from harsh_brake_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB including custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh brake data
# MAGIC create or replace temporary view harsh_brake_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 2 then 1 else 0 end as is_harsh_brake
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (2) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35) and
# MAGIC     a.org_id not in (select cto.org_id from custom_threshold_orgs cto where cto.org_id == a.org_id);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_brake_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_brake) as total_harsh_brake
# MAGIC   from harsh_brake_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_brake_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_brake, 0) as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_brake_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_brake_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_brake as total_harsh_brake,
# MAGIC   total_distance_mi
# MAGIC from harsh_brake_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_brake_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) as total_harsh_brake,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_brake_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_brake) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_brake_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_brake_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_brake_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_brake_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_brake_events_per_1000mi
# MAGIC from harsh_brake_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_brake_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_brake_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_brake_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_brake_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_brake_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs;
# MAGIC create table playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs(
# MAGIC   select * from harsh_brake_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Turns

# COMMAND ----------

# MAGIC %md
# MAGIC ### osDAcceleromter

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh turn data
# MAGIC create or replace temporary view harsh_turn_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.object_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when value.proto_value.accelerometer_event.harsh_accel_type = 6 then 1 else 0 end as is_harsh_turn
# MAGIC   from kinesisstats.osdAccelerometer as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.object_id = c.device_id
# MAGIC   where
# MAGIC     value.proto_value.accelerometer_event.harsh_accel_type in (6) and
# MAGIC     value.proto_value.accelerometer_event.last_gps.speed >= 4344.8812095 and
# MAGIC     value.is_databreak = 'false' and
# MAGIC     value.is_end = 'false' and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_turn_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_turn) as total_harsh_turn
# MAGIC   from harsh_turn_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_turn_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_turn, 0) as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_turn_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_turn_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_turn as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from harsh_turn_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_turn_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) as total_harsh_turn,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_turn_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_turn_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_turn_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_benchmark_metrics_by_week_osda as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_turn_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_turn_events_per_1000mi
# MAGIC from harsh_turn_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_turn_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_turn_benchmark_metrics_by_week_osda
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_turn_benchmark_metrics_by_week_osda
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_turn_benchmarks_by_week_osda;
# MAGIC create table playground.harsh_turn_benchmarks_by_week_osda(
# MAGIC   select * from harsh_turn_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB including custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh turn data
# MAGIC create or replace temporary view harsh_turn_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 6 then 1 else 0 end as is_harsh_turn
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (6) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_turn_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_turn) as total_harsh_turn
# MAGIC   from harsh_turn_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_turn_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_turn, 0) as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_turn_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_turn_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_turn as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from harsh_turn_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_turn_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) as total_harsh_turn,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_turn_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_turn_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_turn_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_turn_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_turn_events_per_1000mi
# MAGIC from harsh_turn_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_turn_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_turn_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_turn_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs;
# MAGIC create table playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs(
# MAGIC   select * from harsh_turn_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB excluding custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get harsh turn data
# MAGIC create or replace temporary view harsh_turns_data as
# MAGIC   select
# MAGIC     a.org_id,
# MAGIC     a.device_id as device_id,
# MAGIC     a.date as date,
# MAGIC     case when detail_proto.accel_type = 6 then 1 else 0 end as is_harsh_turn
# MAGIC   from safetydb_shards.safety_events as a
# MAGIC   left join clouddb.organizations as b on
# MAGIC     a.org_id = b.id
# MAGIC   left join productsdb.gateways as c on
# MAGIC     a.org_id = c.org_id and
# MAGIC     a.device_id = c.device_id
# MAGIC   where
# MAGIC     detail_proto.accel_type in (6) and
# MAGIC     detail_proto.start.speed_milliknots >= 4344.8812095 and
# MAGIC     detail_proto.stop.speed_milliknots >= 4344.8812095 and
# MAGIC     a.date >= date_add(current_date(), -449) and
# MAGIC     a.date <= current_date() and
# MAGIC     b.internal_type != 1  and
# MAGIC     b.quarantine_enabled != 1 and
# MAGIC     c.product_id in (7,24,17,35) and
# MAGIC     a.org_id not in (select cto.org_id from custom_threshold_orgs cto where cto.org_id == a.org_id);
# MAGIC
# MAGIC -- First calculate on device level to do some device level filtering
# MAGIC create or replace temporary view device_harsh_turn_count as (
# MAGIC   select org_id,
# MAGIC     device_id,
# MAGIC     date,
# MAGIC     SUM(is_harsh_turn) as total_harsh_turn
# MAGIC   from harsh_turn_data
# MAGIC   group by org_id, device_id, date
# MAGIC );
# MAGIC
# MAGIC /* Trips data for speeding and total distance driven */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW trip_intervals AS
# MAGIC SELECT
# MAGIC   device_id,
# MAGIC   a.date as date,
# MAGIC   org_id,
# MAGIC   proto
# MAGIC FROM trips2db_shards.trips as a
# MAGIC WHERE a.date >= date_add(current_date(), -449) and
# MAGIC a.date <= current_date();
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW device_trip_distance AS
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   date,
# MAGIC   SUM(proto.trip_distance.distance_meters)*0.000621371 AS total_distance_mi
# MAGIC FROM trip_intervals
# MAGIC GROUP BY org_id, device_id, date;
# MAGIC
# MAGIC -- Filter out data on devices level
# MAGIC create or replace temp view harsh_turn_data_devices as
# MAGIC select
# MAGIC   d.org_id,
# MAGIC   d.device_id,
# MAGIC   d.date,
# MAGIC   coalesce(total_harsh_turn, 0) as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from device_trip_distance as d
# MAGIC left join device_harsh_turn_count as h
# MAGIC   on d.org_id = h.org_id
# MAGIC   and d.device_id = h.device_id
# MAGIC   and d.date = h.date
# MAGIC order by org_id, device_id, date;
# MAGIC
# MAGIC create or replace temp view harsh_turn_data_devices_by_week as
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   total_harsh_turn as total_harsh_turn,
# MAGIC   total_distance_mi
# MAGIC from harsh_turn_data_devices
# MAGIC where
# MAGIC   total_distance_mi > 5 and
# MAGIC   total_distance_mi < 1200;
# MAGIC
# MAGIC -- Aggregate into org/week level
# MAGIC create or replace temp view harsh_turn_data_org_by_week as
# MAGIC select
# MAGIC   he.org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) as total_harsh_turn,
# MAGIC   sum(total_distance_mi) as total_distance_mi
# MAGIC from harsh_turn_data_devices_by_week as he
# MAGIC inner join playground.org_segments_v2 as os -- join with org segments
# MAGIC   on he.org_id = os.org_id
# MAGIC group by cohort_id, he.org_id, week
# MAGIC order by cohort_id, he.org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week should use data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_data_org_past_x_weeks as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   sum(total_harsh_turn) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_harsh_turn_past_x_weeks,
# MAGIC   sum(total_distance_mi) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as total_distance_mi_past_x_weeks
# MAGIC from harsh_turn_data_org_by_week
# MAGIC order by cohort_id, org_id, week;
# MAGIC
# MAGIC -- Benchmark calculations for a given week aggregates data from the past 8 weeks
# MAGIC create or replace temp view harsh_turn_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as
# MAGIC select
# MAGIC   org_id,
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   total_harsh_turn_past_x_weeks/total_distance_mi_past_x_weeks * 1000 as harsh_turn_events_per_1000mi
# MAGIC from harsh_turn_data_org_past_x_weeks
# MAGIC order by org_id, cohort_id, week;
# MAGIC
# MAGIC create or replace temp view harsh_turn_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_turn_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_turn_events_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_turn_events_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_turn_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs;
# MAGIC create table playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs(
# MAGIC   select * from harsh_turn_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Event All

# COMMAND ----------

# MAGIC %md
# MAGIC ### osDAcceleromter

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view harsh_events_all_metrics_by_week as
# MAGIC select
# MAGIC      a.org_id,
# MAGIC      a.cohort_id,
# MAGIC      a.week,
# MAGIC      harsh_accel_events_per_1000mi,
# MAGIC      harsh_brake_events_per_1000mi,
# MAGIC      harsh_turn_events_per_1000mi,
# MAGIC      harsh_accel_events_per_1000mi + harsh_brake_events_per_1000mi + harsh_turn_events_per_1000mi as harsh_events_all_per_1000mi
# MAGIC from harsh_accel_benchmark_metrics_by_week_osda as a
# MAGIC full outer join harsh_brake_benchmark_metrics_by_week_osda as b
# MAGIC   on a.org_id = b.org_id
# MAGIC   and a.cohort_id = b.cohort_id
# MAGIC   and a.week = b.week
# MAGIC full outer join harsh_turn_benchmark_metrics_by_week_osda as t
# MAGIC   on a.org_id = t.org_id
# MAGIC   and a.cohort_id = t.cohort_id
# MAGIC   and a.week = t.week;
# MAGIC
# MAGIC create or replace temp view harsh_events_all_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_events_all_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_events_all_metrics_by_week
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_events_all_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_events_all_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_events_all_benchmarks_by_week_osda;
# MAGIC create table playground.harsh_events_all_benchmarks_by_week_osda(
# MAGIC   select * from harsh_events_all_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB including custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view harsh_events_all_metrics_by_week as
# MAGIC select
# MAGIC      a.org_id,
# MAGIC      a.cohort_id,
# MAGIC      a.week,
# MAGIC      harsh_accel_events_per_1000mi,
# MAGIC      harsh_brake_events_per_1000mi,
# MAGIC      harsh_turn_events_per_1000mi,
# MAGIC      harsh_accel_events_per_1000mi + harsh_brake_events_per_1000mi + harsh_turn_events_per_1000mi as harsh_events_all_per_1000mi
# MAGIC from harsh_accel_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as a
# MAGIC full outer join harsh_brake_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as b
# MAGIC   on a.org_id = b.org_id
# MAGIC   and a.cohort_id = b.cohort_id
# MAGIC   and a.week = b.week
# MAGIC full outer join harsh_turn_benchmark_metrics_by_week_safetydb_including_custom_threshold_orgs as t
# MAGIC   on a.org_id = t.org_id
# MAGIC   and a.cohort_id = t.cohort_id
# MAGIC   and a.week = t.week;
# MAGIC
# MAGIC create or replace temp view harsh_events_all_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_events_all_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_events_all_metrics_by_week
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_events_all_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_events_all_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs;
# MAGIC create table playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs(
# MAGIC   select * from harsh_events_all_benchmarks_by_week
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC ### safetyDB excluding custom threshold orgs

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view harsh_events_all_metrics_by_week as
# MAGIC select
# MAGIC      a.org_id,
# MAGIC      a.cohort_id,
# MAGIC      a.week,
# MAGIC      harsh_accel_events_per_1000mi,
# MAGIC      harsh_brake_events_per_1000mi,
# MAGIC      harsh_turn_events_per_1000mi,
# MAGIC      harsh_accel_events_per_1000mi + harsh_brake_events_per_1000mi + harsh_turn_events_per_1000mi as harsh_events_all_per_1000mi
# MAGIC from harsh_accel_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as a
# MAGIC full outer join harsh_brake_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as b
# MAGIC   on a.org_id = b.org_id
# MAGIC   and a.cohort_id = b.cohort_id
# MAGIC   and a.week = b.week
# MAGIC full outer join harsh_turn_benchmark_metrics_by_week_safetydb_excluding_custom_threshold_orgs as t
# MAGIC   on a.org_id = t.org_id
# MAGIC   and a.cohort_id = t.cohort_id
# MAGIC   and a.week = t.week;
# MAGIC
# MAGIC create or replace temp view harsh_events_all_benchmarks_by_week as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   week,
# MAGIC   avg(harsh_events_all_per_1000mi) as mean,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC   approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC from harsh_events_all_metrics_by_week
# MAGIC group by cohort_id, week
# MAGIC union (
# MAGIC   select
# MAGIC       99 as cohort_id,
# MAGIC       week,
# MAGIC       avg(harsh_events_all_per_1000mi) as mean,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.5) as median,
# MAGIC       approx_percentile(harsh_events_all_per_1000mi, 0.1) as top10_percentile
# MAGIC   from harsh_events_all_metrics_by_week
# MAGIC   group by week
# MAGIC )
# MAGIC order by cohort_id, week;
# MAGIC
# MAGIC drop table if exists playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs;
# MAGIC create table playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs(
# MAGIC   select * from harsh_events_all_benchmarks_by_week
# MAGIC  );

# COMMAND ----------
