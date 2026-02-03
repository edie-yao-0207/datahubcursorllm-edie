# Databricks notebook source
# MAGIC %sql
# MAGIC -- Separate trips that start on
# MAGIC create or replace temp view trips_start_end_same_day as
# MAGIC select
# MAGIC   trips.date,
# MAGIC   trips.org_id,
# MAGIC   trips.device_id,
# MAGIC --   timestamp(proto.start.time/1000) as start_ts,
# MAGIC --   timestamp(proto.end.time/1000) as end_ts,
# MAGIC   hour(timestamp(proto.start.time/1000)) as start_hour,
# MAGIC   hour(timestamp(proto.end.time/1000)) as end_hour
# MAGIC --   proto.end.time-proto.start.time as duration_ms
# MAGIC from trips2db_shards.trips trips
# MAGIC left join productsdb.gateways gw on
# MAGIC   gw.org_id = trips.org_id and
# MAGIC   gw.device_id = trips.device_id
# MAGIC where date >= add_months(current_date(),-12)
# MAGIC and (proto.end.time-proto.start.time)<(24*60*60*1000) /*Filter out very long trips*/
# MAGIC and proto.start.time != proto.end.time /* legit trips only */
# MAGIC and day(timestamp(proto.start.time/1000)) = day(timestamp(proto.end.time/1000))
# MAGIC and gw.product_id in (7,24,17,35);
# MAGIC
# MAGIC /* Trips that start and end on different days need to be split up */
# MAGIC create or replace temp view trips_start_end_diff_day as
# MAGIC select
# MAGIC   date,
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC --   timestamp(proto.start.time/1000) as start_ts,
# MAGIC --   timestamp(proto.end.time/1000) as end_ts,
# MAGIC   hour(timestamp(proto.start.time/1000)) as start_hour,
# MAGIC   23 as end_hour
# MAGIC --   proto.end.time-proto.start.time as duration_ms
# MAGIC from trips2db_shards.trips trips
# MAGIC where date >= add_months(current_date(),-12)
# MAGIC and (proto.end.time-proto.start.time)<(24*60*60*1000) /*Filter out very long trips*/
# MAGIC and proto.start.time != proto.end.time /* legit trips only */
# MAGIC and day(timestamp(proto.start.time/1000)) != day(timestamp(proto.end.time/1000)) /* same day trips */
# MAGIC union all
# MAGIC select
# MAGIC   date_add(date, 1) as date,
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   0 as start_hour,
# MAGIC   hour(timestamp(proto.end.time/1000)) as end_hour
# MAGIC from trips2db_shards.trips trips
# MAGIC where date >= add_months(current_date(),-12)
# MAGIC and (proto.end.time-proto.start.time)<(24*60*60*1000) /*Filter out very long trips*/
# MAGIC and proto.start.time != proto.end.time /* legit trips only */
# MAGIC and day(timestamp(proto.start.time/1000)) != day(timestamp(proto.end.time/1000));
# MAGIC
# MAGIC create or replace temp view trips_start_end_hours as
# MAGIC select
# MAGIC   *
# MAGIC from trips_start_end_same_day
# MAGIC union all
# MAGIC select
# MAGIC   *
# MAGIC from trips_start_end_diff_day;
# MAGIC
# MAGIC cache table trips_start_end_hours;
# MAGIC
# MAGIC select * from trips_start_end_hours order by date, org_id, device_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create start to end array for each trip and explode to create a row for each hour
# MAGIC create or replace temp view trip_hours as
# MAGIC select
# MAGIC   date,
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   explode(sequence(start_hour, end_hour)) as hour
# MAGIC from trips_start_end_hours;
# MAGIC
# MAGIC create or replace temp view daily_operating_hours as
# MAGIC select
# MAGIC   date,
# MAGIC   case
# MAGIC     when month(date) = 1 and weekofyear(date) = 53 then concat(year(date)-1,'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC     else concat(year(date),'-', right(concat('00',CAST(weekofyear(date) as string)),2))
# MAGIC   end as week, -- handle weekofyear edge cases at end of year/beginning of year
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   count(distinct hour) as operating_hours
# MAGIC from trip_hours
# MAGIC group by date, org_id, device_id;
# MAGIC
# MAGIC cache table daily_operating_hours;
# MAGIC
# MAGIC select * from daily_operating_hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view weekly_operating_hours as
# MAGIC select
# MAGIC   week,
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   sum(operating_hours) as weekly_operating_hours,
# MAGIC   sum(operating_hours)/168*100 as utilization /*168 hours in aweek*/
# MAGIC from daily_operating_hours
# MAGIC group by week, org_id, device_id;
# MAGIC
# MAGIC select * from weekly_operating_hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view org_weekly_operating_hours as
# MAGIC select
# MAGIC   week,
# MAGIC   org_id,
# MAGIC   avg(weekly_operating_hours) as avg_weekly_operating_hours,
# MAGIC   avg(utilization) as avg_utilization
# MAGIC from weekly_operating_hours
# MAGIC group by week, org_id;
# MAGIC
# MAGIC
# MAGIC cache table org_weekly_operating_hours;
# MAGIC
# MAGIC select * from org_weekly_operating_hours order by org_id, week;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view org_operating_hours_past_x_weeks as
# MAGIC select
# MAGIC   week,
# MAGIC   org_id,
# MAGIC   avg(avg_weekly_operating_hours) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as avg_hours_used_per_week,
# MAGIC   avg(avg_utilization) over (partition by org_id order by week rows between 8 preceding and 1 preceding) as avg_utilization_per_week
# MAGIC from org_weekly_operating_hours;
# MAGIC
# MAGIC select * from org_operating_hours_past_x_weeks order by org_id, week;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view cohort_weekly_operating_hours as
# MAGIC select
# MAGIC   week,
# MAGIC --   org_id,
# MAGIC   cohort_id,
# MAGIC   avg(avg_hours_used_per_week) as mean,
# MAGIC   approx_percentile(avg_hours_used_per_week, 0.5) as median,
# MAGIC   approx_percentile(avg_hours_used_per_week, 0.9) as top10_percentile
# MAGIC --   avg(utilization) as avg_utilization
# MAGIC from org_operating_hours_past_x_weeks as oh
# MAGIC inner join playground.org_segments_v2 as os
# MAGIC   on oh.org_id = os.org_id
# MAGIC group by week, cohort_id
# MAGIC union (
# MAGIC   select
# MAGIC       week,
# MAGIC       99 as cohort_id,
# MAGIC     avg(avg_hours_used_per_week) as mean,
# MAGIC     approx_percentile(avg_hours_used_per_week, 0.5) as median,
# MAGIC     approx_percentile(avg_hours_used_per_week, 0.9) as top10_percentile
# MAGIC   from org_operating_hours_past_x_weeks
# MAGIC   group by week
# MAGIC );
# MAGIC
# MAGIC cache table cohort_weekly_operating_hours;
# MAGIC
# MAGIC select * from cohort_weekly_operating_hours order by cohort_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view veh_utilization_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "VEHICLE_UTILIZATION" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from cohort_weekly_operating_hours
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC select * from veh_utilization_benchmarks_latest;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view veh_benchmarks as
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from veh_utilization_benchmarks_latest;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from veh_benchmarks

# COMMAND ----------

s3_bucket = "s3://samsara-benchmarking-metrics"
metric_group = "vehicle_utilization"

query = "select * from veh_benchmarks order by metric_type, cohort_id"

benchmark_metrics_sdf = spark.sql(query)
benchmark_metrics_df = benchmark_metrics_sdf.toPandas()
benchmark_metrics_df.head()

benchmark_metrics_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/{metric_group}_benchmark_metrics")

# COMMAND ----------
