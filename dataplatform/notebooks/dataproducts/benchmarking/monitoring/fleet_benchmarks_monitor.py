# Databricks notebook source
# MAGIC %md
# MAGIC # Run Benchmarks
# MAGIC Requirement: Must run `org_segments` or `org_segments_monitor` first to get the most up to date orgs for benchmark calculations!

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/harsh_events_benchmarks

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/speeding_benchmarks

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/idling_percent_benchmarks

# COMMAND ----------

# MAGIC %md
# MAGIC # Metrics Prep

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safety

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
# MAGIC
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
# MAGIC cache table safety_benchmark_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view safety_benchmarks_prod_vs_new as
# MAGIC -- Combine prod and new benchmarks
# MAGIC select
# MAGIC   prod.cohort_id,
# MAGIC   prod.metric_type,
# MAGIC   concat(round(prod.mean,3), " -> ",round(new.mean,3), " (", round(((new.mean - prod.mean)/prod.mean)*100,1),"%)") as mean,
# MAGIC   case when concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)") is null then
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)")
# MAGIC   end as median,
# MAGIC   case when concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", round(((new.top10_percentile - prod.top10_percentile)/prod.top10_percentile)*100,1),"%)") is null then
# MAGIC     concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", round(((new.top10_percentile - prod.top10_percentile)/prod.top10_percentile)*100,1),"%)")
# MAGIC   end as top10_percent
# MAGIC from dataproducts.safety_benchmark_metrics as prod
# MAGIC join safety_benchmark_metrics as new
# MAGIC on prod.cohort_id = new.cohort_id
# MAGIC and prod.metric_type = new.metric_type
# MAGIC order by cohort_id, metric_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from safety_benchmarks_prod_vs_new;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idling Percent

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

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view idling_percent_benchmarks_prod_vs_new as
# MAGIC -- Combine prod and new benchmarks
# MAGIC select
# MAGIC   prod.cohort_id,
# MAGIC   prod.metric_type,
# MAGIC   concat(round(prod.mean,3), " -> ",round(new.mean,3), " (", round(((new.mean - prod.mean)/prod.mean)*100,1),"%)") as mean,
# MAGIC   case when concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)") is null then
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)")
# MAGIC   end as median,
# MAGIC   case when concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", round(((new.top10_percentile - prod.top10_percentile)/prod.top10_percentile)*100,1),"%)") is null then
# MAGIC     concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.top10_percentile,3), " -> ",round(new.top10_percentile,3), " (", round(((new.top10_percentile - prod.top10_percentile)/prod.top10_percentile)*100,1),"%)")
# MAGIC   end as top10_percent
# MAGIC from dataproducts.idling_percent_benchmarks as prod
# MAGIC join idling_percent_benchmarks as new
# MAGIC on prod.cohort_id = new.cohort_id
# MAGIC and prod.metric_type = new.metric_type
# MAGIC order by cohort_id, metric_type;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## MPGE

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/benchmarks/mpge_benchmarks

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view mpge_benchmarks_prod_vs_new_all as
# MAGIC -- Combine prod and new benchmarks
# MAGIC select
# MAGIC   prod.cohort_id,
# MAGIC   prod.metric_type,
# MAGIC   round(((new.mean - prod.mean)/prod.mean)*100,1) as mean_diff,
# MAGIC   round(((new.median - prod.median)/prod.median)*100,1) as median_diff,
# MAGIC   round(((new.top10 - prod.top10)/prod.top10)*100,1) as top10_diff,
# MAGIC   concat(round(prod.mean,3), " -> ",round(new.mean,3), " (", round(((new.mean - prod.mean)/prod.mean)*100,1),"%)") as mean,
# MAGIC   case when concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)") is null then
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.median,3), " -> ",round(new.median,3), " (", round(((new.median - prod.median)/prod.median)*100,1),"%)")
# MAGIC   end as median,
# MAGIC   case when concat(round(prod.top10,3), " -> ",round(new.top10,3), " (", round(((new.top10 - prod.top10)/prod.top10)*100,1),"%)") is null then
# MAGIC     concat(round(prod.top10,3), " -> ",round(new.top10,3), " (", "0.0","%)")
# MAGIC   else
# MAGIC     concat(round(prod.top10,3), " -> ",round(new.top10,3), " (", round(((new.top10 - prod.top10)/prod.top10)*100,1),"%)")
# MAGIC   end as top10_percent
# MAGIC from playground.top_mmy_mpge_benchmarks_v1 as prod
# MAGIC join playground.top_mmy_mpge_benchmarks_v1_staging as new
# MAGIC on prod.cohort_id = new.cohort_id
# MAGIC and prod.metric_type = new.metric_type
# MAGIC order by cohort_id, metric_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mpge_benchmarks_prod_vs_new_all;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view mpge_benchmarks_prod_vs_new as
# MAGIC select
# MAGIC cohort_id,
# MAGIC metric_type,
# MAGIC mean,
# MAGIC median,
# MAGIC top10_percent
# MAGIC from mpge_benchmarks_prod_vs_new_all
# MAGIC where mean_diff > 2 or median_diff > 2 or top10_diff > 2;

# COMMAND ----------

# MAGIC %md
# MAGIC # Monitor Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %run /Shared/Shared/slack-tables

# COMMAND ----------

from datetime import date
from datetime import timedelta

yesterday = date.today() - timedelta(days=1)

#
# Data Inspection Helpers
#

__METRICS__ = {}


def add_metrics(
    key,
    title,
    databricks_edit_cell_url,
    body,
    disable_real_slack_post_for_testing=False,
    slack_channels=[],
):
    """
    add_metrics constructs a slack post and adds it to the __METRICS__ global object

    - key is a unique identifier for this metric set
    - title goes at the beginning of the slack post
    - if databricks_edit_cell_url is passed, we'll append an edit link to the slack post
    - body is an array of strings that make up the slack post
    - set disable_real_slack_post_for_testing to disable posting this metric to slack
    - slack_channels is where we'll post these metrics
    """
    __METRICS__[key] = {
        "slack_text": make_metrics(
            title=title, databricks_edit_cell_url=databricks_edit_cell_url, body=body
        ),
        "slack_channels": slack_channels,
        "disable_real_slack_post_for_testing": disable_real_slack_post_for_testing,
    }
    print(__METRICS__[key]["slack_text"])


def print_metrics():
    if len(__METRICS__.items()) == 0:
        print("NO METRICS COLLECTED")
        return

    for metric in __METRICS__.values():
        print("=================")
        print(metric["slack_text"])
        print("\n\n")


def send_metrics(slack_channel_override=None, key_override=None):
    for (key, metric) in __METRICS__.items():
        if key_override is not None and key != key_override:
            continue

        if slack_channel_override is not None:
            send_slack_message_to_channels(
                metric["slack_text"], [slack_channel_override]
            )
            continue

        if metric["disable_real_slack_post_for_testing"]:
            continue

        send_slack_message_to_channels(metric["slack_text"], metric["slack_channels"])


#
# Data presentation helpers
#


def print_historical_comparison(before, after, distance_str):
    comparison = "up" if after > before else "down"
    return "{}, {} {} from {} {}".format(
        round(after, 2),
        comparison,
        round(abs(after - before), 2),
        round(before, 2),
        distance_str,
    )


def make_metric_row(title, value):
    """make_metric_row formats a bulletted KV row for slack"""
    strValue = f"{value:,}" if not isinstance(value, str) else value
    return f"\t• {title}: *{strValue}*"


def make_metric_row_group(title, body):
    """make_metric_row_group groups some rows under a title, surrounded by empty lines"""
    rows = [title] + body
    return "\n".join(rows)


faqsLink = "https://samsaur.us/daily-metrics-faq"


def make_metrics(
    title,
    body,
    databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267286889/command/3067644267287979",
):
    """make_metrics constructs a slack post"""
    metrics_post_rows = [f"*{title}* ({yesterday})"] + body
    metrics_post_rows.append(
        f"<{databricks_edit_cell_url}|Edit these metrics> | <{faqsLink}|FAQs doc>"
    )
    metrics_post = "\n".join(metrics_post_rows)
    return metrics_post


# COMMAND ----------

# MAGIC %md
# MAGIC ## Add & Send Metrics

# COMMAND ----------

metric_types = [
    "HARSH_ACCEL",
    "HARSH_BRAKE",
    "HARSH_TURN",
    "HARSH_EVENTS_ALL",
    "LIGHT_SPEEDING",
    "MODERATE_SPEEDING",
    "HEAVY_SPEEDING",
    "SEVERE_SPEEDING",
    "ALL_SPEEDING",
]

for m in metric_types:
    print(m)
    metrics_sdf = spark.sql(
        "select * from safety_benchmarks_prod_vs_new where metric_type like '%{}%' order by cohort_id, metric_type".format(
            m
        )
    ).rdd.collect()
    columns = ["cohort_id", "metric_type", "mean", "median", "top10%"]

    row_data = []
    for result in metrics_sdf:
        row_data.append(
            [
                str(result.cohort_id),
                str(result.metric_type),
                str(result.mean),
                str(result.median),
                str(result.top10_percent),
            ]
        )

    safety_benchmarks_refresh_metrics = make_table("", columns, row_data)

    try:
        add_metrics(
            disable_real_slack_post_for_testing=False,
            slack_channels=["fleet-benchmarks-refresh"],
            key=f"{m}",
            title=f"{m} Refresh Metrics (Prod vs New)",
            databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267297623/command/3067644267298932",
            body=[
                "\n".join(safety_benchmarks_refresh_metrics),
            ],
        )

    except Exception as e:
        print(e)

# COMMAND ----------

metrics_sdf = spark.table("idling_percent_benchmarks_prod_vs_new").rdd.collect()
columns = ["cohort_id", "metric_type", "mean", "median", "top10%"]

row_data = []
for result in metrics_sdf:
    row_data.append(
        [
            str(result.cohort_id),
            str(result.metric_type),
            str(result.mean),
            str(result.median),
            str(result.top10_percent),
        ]
    )

idling_percent_benchmarks_refresh_metrics = make_table("", columns, row_data)

try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["fleet-benchmarks-refresh"],
        key="IDLING_PERCENT",
        title="IDLING PERCENT Benchmarks Refresh Metrics (Prod vs New)",
        databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267297623/command/3067644267298932",
        body=[
            "\n".join(idling_percent_benchmarks_refresh_metrics),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

metrics_sdf = spark.table("mpge_benchmarks_prod_vs_new").rdd.collect()
columns = ["cohort_id", "metric_type", "mean", "median", "top10%"]

row_data = []
for result in metrics_sdf:
    row_data.append(
        [
            str(result.cohort_id),
            str(result.metric_type),
            str(result.mean),
            str(result.median),
            str(result.top10_percent),
        ]
    )

mpge_benchmarks_refresh_metrics = make_table("", columns, row_data)

try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["fleet-benchmarks-refresh"],
        key="MPGE",
        title="MPGE Benchmarks Refresh Metrics (Prod vs New, greater than 2% difference)",
        databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267297623/command/3067644267298932",
        body=[
            "\n".join(mpge_benchmarks_refresh_metrics),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

send_metrics()

# COMMAND ----------
