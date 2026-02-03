# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Fleet Benchmarks
# MAGIC Requirement: Must run `generate_org_segments` first to get the most up to date orgs for benchmark calculations!

# COMMAND ----------

# MAGIC %md
# MAGIC # Safety Benchmarks

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/harsh_events_benchmarks

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/speeding_benchmarks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grab most recent benchmarks
# MAGIC create or replace temp view harsh_accel_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "HARSH_ACCEL" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view harsh_brake_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "HARSH_BRAKE" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view harsh_turn_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "HARSH_TURN" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view harsh_events_all_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "HARSH_EVENTS_ALL" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view light_speeding_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "LIGHT_SPEEDING" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.light_speeding_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view moderate_speeding_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "MODERATE_SPEEDING" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.moderate_speeding_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view heavy_speeding_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "HEAVY_SPEEDING" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.heavy_speeding_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view severe_speeding_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "SEVERE_SPEEDING" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.severe_speeding_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC create or replace temp view all_speeding_benchmarks_latest as
# MAGIC select
# MAGIC   *,
# MAGIC   "ALL_SPEEDING" as metric_type
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.all_speeding_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union all data
# MAGIC create or replace temp view safety_benchmark_metrics as
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from harsh_accel_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from harsh_brake_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from harsh_turn_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from harsh_events_all_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from light_speeding_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from moderate_speeding_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from heavy_speeding_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from severe_speeding_benchmarks_latest
# MAGIC union
# MAGIC select cohort_id, metric_type, mean, median, top10_percentile
# MAGIC   from all_speeding_benchmarks_latest;
# MAGIC
# MAGIC select * from safety_benchmark_metrics order by cohort_id, metric_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save prod benchmarks
# MAGIC drop table if exists dataproducts.safety_benchmark_metrics_prev;
# MAGIC create table if not exists dataproducts.safety_benchmark_metrics_prev as (
# MAGIC   select * from dataproducts.safety_benchmark_metrics
# MAGIC );
# MAGIC
# MAGIC drop table if exists dataproducts.safety_benchmark_metrics;
# MAGIC create table if not exists dataproducts.safety_benchmark_metrics as (
# MAGIC   select * from safety_benchmark_metrics
# MAGIC );

# COMMAND ----------

s3_bucket = "s3://samsara-benchmarking-metrics"
metric_group = "safety"

query = "select * from dataproducts.safety_benchmark_metrics order by metric_type, cohort_id"

benchmark_metrics_sdf = spark.sql(query)
benchmark_metrics_df = benchmark_metrics_sdf.toPandas()
benchmark_metrics_df.head()

benchmark_metrics_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/{metric_group}_benchmark_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC # Idling Percent Benchmarks

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/idling_percent_benchmarks

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view idling_percent_benchmarks as
# MAGIC select
# MAGIC   cohort_id,
# MAGIC   "IDLING_PERCENT" as metric_type,
# MAGIC   mean,
# MAGIC   median,
# MAGIC   top10_percentile
# MAGIC from (
# MAGIC   select *, max(week) over(partition by cohort_id) latest_week
# MAGIC   from playground.idling_percent_benchmarks_by_week
# MAGIC )
# MAGIC where week=latest_week;
# MAGIC
# MAGIC select * from idling_percent_benchmarks order by cohort_id, metric_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save prod benchmarks
# MAGIC drop table if exists dataproducts.idling_percent_benchmarks_prev;
# MAGIC create table if not exists dataproducts.idling_percent_benchmarks_prev as (
# MAGIC   select * from dataproducts.idling_percent_benchmarks
# MAGIC );
# MAGIC
# MAGIC drop table if exists dataproducts.idling_percent_benchmarks;
# MAGIC create table if not exists dataproducts.idling_percent_benchmarks as (
# MAGIC   select * from idling_percent_benchmarks
# MAGIC );

# COMMAND ----------

s3_bucket = "s3://samsara-benchmarking-metrics"
metric_group = "idling_percent"

query = "select * from dataproducts.idling_percent_benchmarks order by metric_type, cohort_id"

benchmark_metrics_sdf = spark.sql(query)
benchmark_metrics_df = benchmark_metrics_sdf.toPandas()
benchmark_metrics_df.head()

benchmark_metrics_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/{metric_group}_benchmark_metrics")

# COMMAND ----------
