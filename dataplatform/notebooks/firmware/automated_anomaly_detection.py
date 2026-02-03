# Databricks notebook source
# MAGIC %md
# MAGIC # Installations

# COMMAND ----------

# MAGIC %pip install cvxopt
# MAGIC %pip install slack_bolt
# MAGIC %pip install slack_sdk
# MAGIC %pip install -U csaps
# MAGIC %pip install ruptures
# MAGIC %pip install statsmodels==0.14.0
# MAGIC %pip install scipy==1.11.1

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
import boto3
import matplotlib_inline.backend_inline
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from io import BytesIO
from slack_sdk import WebClient
import requests
import time
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
import pandas as pd
import pytz

matplotlib_inline.backend_inline.set_matplotlib_formats("png2x")

# COMMAND ----------

# MAGIC %run ./metrics_monitored

# COMMAND ----------

# MAGIC %run ./data_analysis_library

# COMMAND ----------

# MAGIC %md
# MAGIC # Constants and Configuration Variables

# COMMAND ----------

DAYS_IN_WEEKEND = 2
DAYS_IN_WEEK = 7
DAYS_IN_YEAR = 365

# Slack channel name and id to post alerts to
channel_name = "alerts-safety-firmware"
channel_id = "C042W972GDQ"

# In short term detection, checking if any breakpoints were detected in the past days_in_past_to_review days
# i.e. days_in_past_to_review = 1 only checks for the most recent day, while days_in_past_to_review = 2 checks if there were any breakpoints in the most recent 2 days
days_in_past_to_review = 2

# Number of days to consider for short term trend prior to the current day
short_term_days_considered_before = 30 + days_in_past_to_review
long_term_days_considered_before = 730

# Number of additional days to query for short term trend in case certain days are filtered out due
# to too few devices.
short_term_additional_days_considered_before = 15

# Percentiles to run for short term trend
percentiles = [0.1, 0.3, 0.5, 0.7, 0.9]

# alerting threshold for long term change in value between week to past month and past year
# determined by checking changes in historical data and manually determining what seems outside of normal noise
epsilon = 0.02

# days to consider for long term trend (e.g. put in 5 to run on the 5th of the month)
# currently set to ~ biweekly
days = [1, 15]

# min device count necessary to run anomaly detection
min_device_count = 1000

# stages to consider for short term trend
rollout_stages = ["ALL", "1000", "2000", "3000", "4000", "5000", "6000"]

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions

# COMMAND ----------


def send_alert(text, image):
    file_name = (
        text.lower().replace(":", "").replace(",", "").replace(" ", "_") + "_plot.png"
    )
    response = client.files_getUploadURLExternal(
        filename=file_name, length=image.getbuffer().nbytes
    )

    upload_url = response["upload_url"]
    file_id = response["file_id"]

    headers = {"Content-Type": "application/octet-stream"}
    upload_response = requests.post(
        url=upload_url, headers=headers, data=image.getvalue()
    )

    if upload_response.status_code != 200:
        raise Exception(
            f"File upload failed with status code {upload_response.status_code}"
        )

    response2 = client.files_completeUploadExternal(
        files=[{"id": file_id, "title": "plot"}],
        channel_id=channel_id,
        initial_comment=text,
    )


def generate_graph(data_with_date, anomaly_date, scale_factor, offset):
    x = [v["date"] for v in data_with_date]
    y = [(v["value"] / scale_factor) - offset for v in data_with_date]
    fig, ax = plt.subplots(figsize=[18, 3])
    ax.plot(x, y)
    ax.scatter(
        anomaly_date,
        y[x.index(anomaly_date)],
        color="red",
        label="Trend change point",
    )

    ax.legend()
    ax.set_xticklabels(x, rotation=270)

    plt.close()

    image_stream = BytesIO()
    fig.savefig(image_stream, format="png", bbox_inches="tight")
    image_stream.seek(0)

    return image_stream


# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate Metrics

# COMMAND ----------


def upgrade_dates(start_date_str, end_date_str):
    return """
        with lag as (
            select
                a.date,
                b.org_id,
                b.device_id,
                a.latest_build_on_day,
                lag(a.latest_build_on_day) over(partition by gateway_id order by date asc) as prev_latest_build_on_day
            from dataprep_firmware.device_daily_firmware_builds as a
            join clouddb.gateways as b
                on a.gateway_id = b.id
            where a.date >= '{}'
                and a.date <= '{}'
        )
        select
            date,
            org_id,
            device_id
        from lag
        where prev_latest_build_on_day != latest_build_on_day
        group by
            date,
            org_id,
            device_id
    """.format(
        start_date_str, end_date_str
    )


def aggregate_metrics(start_date, end_date):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    devices_df = spark.sql(
        """
                         (select
                            a.org_id,
                            a.device_id,
                            a.product_id,
                            a.product_program_id,
                            a.rollout_stage_id
                          from dataprep_firmware.gateway_daily_rollout_stages as a
                          join clouddb.organizations as b
                            on a.org_id = b.id
                          left join internaldb.simulated_orgs as c
                            on a.org_id = c.org_id
                          where c.org_id is null
                            and a.org_id != 1
                            and b.internal_type = 0
                            and date = '{}')
                            union
                          (select
                            a.org_id,
                            a.device_id,
                            a.product_id,
                            'ALL' as product_program_id,
                            'ALL' as rollout_stage_id
                          from dataprep_firmware.gateway_daily_rollout_stages as a
                          join clouddb.organizations as b
                            on a.org_id = b.id
                          left join internaldb.simulated_orgs as c
                            on a.org_id = c.org_id
                          where c.org_id is null
                            and a.org_id != 1
                            and b.internal_type = 0
                            and date = '{}')
                            """.format(
            end_date, end_date
        )
    )

    metric_df = None
    for metric_name in metrics_list:
        query = metrics_list[metric_name]["data_query"]
        metric_query_df = (
            spark.sql(query(start_date_str, end_date_str))
            .withColumn("metric_name", F.lit(metric_name))
            .withColumn("goal", F.lit(metrics_list[metric_name]["goal"]))
            .withColumn("min_value", F.lit(metrics_list[metric_name]["min_value"]))
            .withColumn("max_value", F.lit(metrics_list[metric_name]["max_value"]))
            .withColumn(
                "product_ids",
                F.array(
                    [F.lit(pid) for pid in metrics_list[metric_name]["product_ids"]]
                ),
            )
        )

        if metric_df is None:
            metric_df = metric_query_df
        else:
            metric_df = metric_df.unionAll(metric_query_df)

    upgrade_dates_df = spark.sql(upgrade_dates(start_date_str, end_date_str))

    agg_metrics_df = (
        metric_df.alias("a")
        .join(devices_df.alias("b"), ["org_id", "device_id"])
        .join(
            upgrade_dates_df.alias("c"),
            (F.col("a.date") == F.col("c.date"))
            & (F.col("a.org_id") == F.col("c.org_id"))
            & (F.col("a.device_id") == F.col("c.device_id")),
            how="left",
        )
        .filter(
            (F.expr("array_contains(a.product_ids, b.product_id)"))
            & (F.col("b.rollout_stage_id").isin(rollout_stages))
        )
        .select(
            "a.date",
            "a.metric_name",
            "a.metric_subname",
            "b.product_program_id",
            "b.rollout_stage_id",
            "b.product_id",
            "a.goal",
            "a.min_value",
            "a.max_value",
            "a.value",
            F.when(F.col("c.device_id").isNotNull(), F.lit(True))
            .otherwise(F.lit(F.lit(False)))
            .alias("upgrade"),
        )
        .withColumn(
            "day_type",
            F.when(F.dayofweek(F.col("date")).isin(1, 7), "weekend").otherwise(
                "weekday"
            ),
        )
        .groupBy(
            "date",
            "day_type",
            "metric_name",
            "metric_subname",
            "product_program_id",
            "rollout_stage_id",
            "product_id",
            "goal",
            "min_value",
            "max_value",
            "upgrade",
        )
        .agg(
            F.percentile_approx(F.col("value"), percentiles).alias("values"),
            F.count(F.col("value")).alias("device_count"),
        )
        .select(
            "date",
            "day_type",
            "upgrade",
            "metric_name",
            "metric_subname",
            "product_program_id",
            "rollout_stage_id",
            "product_id",
            "goal",
            "min_value",
            "max_value",
            "device_count",
            F.col("values"),
            F.lit(percentiles).alias("percentiles"),
        )
        .withColumn(
            "percentile_value",
            F.explode(F.arrays_zip(F.col("percentiles"), F.col("values"))),
        )
        .drop("percentiles", "values")
        .select(
            "date",
            "day_type",
            "upgrade",
            "metric_name",
            "metric_subname",
            "product_program_id",
            "rollout_stage_id",
            "product_id",
            "goal",
            "min_value",
            "max_value",
            "device_count",
            F.col("percentile_value.percentiles").alias("percentile"),
            F.col("percentile_value.values").alias("value"),
        )
        .orderBy("date")
        .groupBy(
            "metric_name",
            "metric_subname",
            "day_type",
            "upgrade",
            "product_program_id",
            "rollout_stage_id",
            "product_id",
            "percentile",
            "goal",
            "min_value",
            "max_value",
        )
        .agg(
            F.collect_list(
                F.struct(F.col("date"), F.col("value"), F.col("device_count"))
            ).alias("values")
        )
    )

    return agg_metrics_df


def detect_wrapper(
    data_col,
    end_date_str,
    short_term_days_considered_before,
    min_value,
    max_value,
    min_device_count,
):
    data_col_filtered = [
        {"date": item["date"], "value": item["value"]}
        for item in data_col
        if item["device_count"] >= min_device_count
    ]

    if len(data_col_filtered) < short_term_days_considered_before:
        return (
            "insufficient historical data got {} days but needed {}".format(
                len(data_col_filtered), short_term_days_considered_before
            ),
            None,
            None,
            None,
            None,
            None,
            None,
        )

    data_pd = pd.DataFrame(data_col_filtered, columns=["date", "value"])

    if end_date_str not in data_pd["date"].values:
        return (
            "no data for zoom in point {}".format(end_date_str),
            None,
            None,
            None,
            None,
            None,
            None,
        )

    if data_pd["value"].max() == data_pd["value"].min():
        return "", [], [], [], [], None, None

    if min_value == None:
        min_value = data_pd["value"].min()
    if max_value == None:
        max_value = data_pd["value"].max()

    scale_factor = 100.0 / (max_value - min_value)
    offset = min_value

    data_pd["value"] = data_pd["value"] - offset
    data_pd["value"] = data_pd["value"] * scale_factor

    try:
        _, trend, _, _, _, _, _, change_x, change_y, _, data_with_date = detect(
            data_pd,
            time_column="date",
            value_column="value",
            frequency="d",
            data_type="continuous",
            task={"trend": "BOCPD"},
            zoom_in_point=end_date_str,
            zoom_in_before=short_term_days_considered_before,
            figure=False,
        )
    except:
        return (
            "Error running BOCPD data: {}, zoom_in_point: {}".format(
                data_pd, end_date_str
            ),
            None,
            None,
            None,
            None,
            None,
            None,
        )

    data_with_date_struct = [
        {"date": row[0].strftime("%Y-%m-%d"), "value": float(row[1])}
        for row in data_with_date.iteritems()
    ]

    trend_ret = [float(v) for v in trend]
    change_x_ret = [float(v) for v in change_x]
    change_y_ret = []
    if len(change_y) > 0:
        change_y_ret = [float(v) for v in change_y[0]]

    return (
        "",
        trend_ret,
        change_x_ret,
        change_y_ret,
        data_with_date_struct,
        float(scale_factor),
        float(offset),
    )


def run_short_term_detection(agg_metrics_df, end_date):
    end_date_str = end_date.strftime("%Y-%m-%d")

    detect_udf = F.udf(
        detect_wrapper,
        T.StructType(
            [
                T.StructField("error", T.StringType()),
                T.StructField("trend", T.ArrayType(T.DoubleType())),
                T.StructField("change_x", T.ArrayType(T.DoubleType())),
                T.StructField("change_y", T.ArrayType(T.DoubleType())),
                T.StructField(
                    "data_with_date",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("date", T.StringType()),
                                T.StructField("value", T.DoubleType()),
                            ]
                        )
                    ),
                ),
                T.StructField("scale_factor", T.DoubleType()),
                T.StructField("offset", T.DoubleType()),
            ]
        ),
    )

    anomalies_df = agg_metrics_df.withColumn(
        "anomalies",
        detect_udf(
            F.col("values"),
            F.lit(end_date_str),
            F.lit(short_term_days_considered_before),
            F.col("min_value"),
            F.col("max_value"),
            F.lit(min_device_count),
        ),
    ).drop("values")

    return anomalies_df


def get_anomaly_dates(anomalies_col, end_date_str, goal):
    if anomalies_col["error"] != "":
        return None

    if len(anomalies_col["change_x"]) == 0:
        return None

    scaled_goal = None
    if goal is not None:
        scaled_goal = anomalies_col["scale_factor"] * (goal - anomalies_col["offset"])

    anomaly_dates = []
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    for index in anomalies_col["change_x"]:
        index_int = int(index)
        anomaly_date_str = anomalies_col["data_with_date"][index_int]["date"]
        anomaly_date = datetime.strptime(anomaly_date_str, "%Y-%m-%d")

        if anomaly_date < end_date - timedelta(days=days_in_past_to_review):
            continue

        current_value = anomalies_col["data_with_date"][index_int]["value"]
        trend_value = anomalies_col["trend"][index_int]
        if scaled_goal is not None and abs(scaled_goal - current_value) < abs(
            scaled_goal - trend_value
        ):
            continue

        anomaly_dates.append(anomaly_date_str)

    if len(anomaly_dates) == 0:
        return None

    return anomaly_dates


def check_for_anomalies(anomalies_df, end_date):
    end_date_str = end_date.strftime("%Y-%m-%d")
    anomaly_dates_udf = F.udf(get_anomaly_dates, T.ArrayType(T.StringType()))
    anomalies_with_dates_df = anomalies_df.withColumn(
        "anomaly_dates",
        anomaly_dates_udf(F.col("anomalies"), F.lit(end_date_str), F.col("goal")),
    )
    return anomalies_with_dates_df


def create_update_or_insert_into_table(
    target_table, update_df, partition_col, merge_on_cols, update_matched
):
    update_df.createOrReplaceTempView("update")

    spark.sql(
        """
    create table if not exists {}
    using delta
    partitioned by ({})
    as
    select * from update
    """.format(
            target_table, partition_col
        )
    )

    merge_logic = "on " + " and ".join(
        ["target.{} = updates.{}".format(v, v) for v in merge_on_cols]
    )

    if update_matched:
        spark.sql(
            """
            merge into {} as target
            using update as updates
            {}
            when matched then update set *
            when not matched then insert *
        """.format(
                target_table, merge_logic
            )
        )
    else:
        spark.sql(
            """
            merge into {} as target
            using update as updates
            {}
            when not matched then insert *
        """.format(
                target_table, merge_logic
            )
        )


def update_anomalies_table(df, update_matched):
    create_update_or_insert_into_table(
        "dataprep_firmware.safety_firmware_metrics_anomalies_detected",
        df,
        "current_date",
        [
            "current_date",
            "day_type",
            "upgrade",
            "product_program_id",
            "rollout_stage_id",
            "product_id",
            "metric_name",
            "metric_subname",
            "percentile",
            "anomaly_date",
        ],
        update_matched,
    )


# COMMAND ----------
# get dates for UTC and Pacific timezones
now_utc = datetime.now(pytz.utc)
now_pacific = now_utc.astimezone(pytz.timezone("US/Pacific"))

# Check if Pacific date is "behind" UTC date
utc_date = now_utc.date()
pacific_date = now_pacific.date()

# If Pacific date is still "yesterday" but UTC has moved on, use 3 days
days_back = 3 if pacific_date < utc_date else 2

# Calculate end date in UTC
end_date = now_utc - timedelta(days=days_back)

# Define today as UTC datetime for use in date formatting
today = now_utc

start_date = end_date - timedelta(
    days=(
        (short_term_days_considered_before * DAYS_IN_WEEK) / DAYS_IN_WEEKEND
        + short_term_additional_days_considered_before
    )
)

# COMMAND ----------

agg_metrics_df = aggregate_metrics(start_date, end_date)
agg_metrics_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
    "dataprep_firmware.aggregated_metrics_for_anomaly_detection"
)

# COMMAND ----------

agg_metrics_loaded_df = spark.sql(
    "select * from dataprep_firmware.aggregated_metrics_for_anomaly_detection"
).repartition(2048)
change_points_df = run_short_term_detection(agg_metrics_loaded_df, end_date)
anomalies_df = check_for_anomalies(change_points_df, end_date)
final_df = (
    anomalies_df.withColumn("current_date", F.lit(today.strftime("%Y-%m-%d")))
    .withColumn(
        "anomaly_date",
        F.explode(F.coalesce(F.col("anomaly_dates"), F.array(F.lit(None)))),
    )
    .withColumn("is_anomaly", F.col("anomaly_date").isNotNull())
    .withColumn("alerted", F.lit(False))
)

# COMMAND ----------

update_anomalies_table(final_df, False)

# COMMAND ----------

client = get_client()

region = boto3.session.Session().region_name
region_name = "NA"
if region == "eu-west-1":
    region_name = "EU"
elif region == "ca-central-1":
    region_name = "CA"

anomalies_table_df = spark.sql(
    "select * from dataprep_firmware.safety_firmware_metrics_anomalies_detected"
)
current_day_anomalies_df = anomalies_table_df.filter(
    (F.col("alerted") == False) & (F.col("is_anomaly") == True)
)

today_string = today.strftime("%Y-%m-%d")

if current_day_anomalies_df.filter("metric_name = 'Test'").count() == 0:
    notification_text = "{}: Anomaly detection failed for Test metric in {}".format(
        today_string, region_name
    )
    client.chat_postMessage(channel=channel_name, text=notification_text)
    raise Exception(notification_text)

anomalies_already_alerted_df = anomalies_table_df.filter(
    (F.col("alerted") == True) & (F.col("is_anomaly") == True)
)
anomalies_to_alert_df = (
    current_day_anomalies_df.alias("a")
    .join(
        anomalies_already_alerted_df.alias("b"),
        (F.col("a.product_program_id") == F.col("b.product_program_id"))
        & (F.col("a.rollout_stage_id") == F.col("b.rollout_stage_id"))
        & (F.col("a.product_id") == F.col("b.product_id"))
        & (F.col("a.metric_name") == F.col("b.metric_name"))
        & (F.col("a.metric_subname") == F.col("b.metric_subname"))
        & (F.col("a.percentile") == F.col("b.percentile"))
        & (F.col("a.day_type") == F.col("b.day_type"))
        & (F.col("a.upgrade") == F.col("b.upgrade"))
        & (F.col("a.anomaly_date") == F.col("b.anomaly_date")),
        how="left",
    )
    .filter(F.col("b.anomaly_date").isNull())
    .filter(F.col("a.metric_name") != "Test")
    .select(current_day_anomalies_df["*"])
)

anomalies_to_alert_collected = anomalies_to_alert_df.collect()

if len(anomalies_to_alert_collected) > 0:
    notification_text = "{}: Anomalies detected in {}!\n".format(
        today_string, region_name
    )
    client.chat_postMessage(channel=channel_name, text=notification_text)
    for i in range(len(anomalies_to_alert_collected)):
        row = anomalies_to_alert_collected[i]
        graph_desc_text = "Anomaly Date: {},  Region: {}, Metric Name: {}, Metric Subname: {}, Day Type: {}, Upgrade: {}, Product ID: {}, Product Program ID: {}, Rollout Stage ID: {}, Percentile: {}".format(
            row["anomaly_date"],
            region_name,
            row["metric_name"],
            row["metric_subname"],
            row["day_type"],
            row["upgrade"],
            row["product_id"],
            row["product_program_id"],
            row["rollout_stage_id"],
            row["percentile"],
        )
        graph_image = generate_graph(
            row["anomalies"]["data_with_date"],
            row["anomaly_date"],
            row["anomalies"]["scale_factor"],
            row["anomalies"]["offset"],
        )
        send_alert(graph_desc_text, graph_image)
else:
    notification_text = "{}: No anomalies detected in {}\n".format(
        today_string, region_name
    )
    client.chat_postMessage(channel=channel_name, text=notification_text)

anomalies_alerted_df = anomalies_to_alert_df.withColumn("alerted", F.lit(True))
update_anomalies_table(anomalies_alerted_df, True)

# COMMAND ----------

# def run_long_term_detection_on_all_metrics_and_alert(metrics):
#     alerts = []
#     trend_worsening = []
#     long_term_checked = []

#     # Check if there are any anomalies
#     for metric in metrics:
#         for stat in metric["stats"]:
#             if "avg" in stat:
#                 (
#                     _,
#                     trend,
#                     _,
#                     _,
#                     _,
#                     _,
#                     _,
#                     change_x,
#                     change_y,
#                     detected_lag,
#                     data_with_date,
#                     season_image,
#                 ) = detect(
#                     metric["data"],
#                     time_column="date",
#                     value_column=stat,
#                     frequency="d",
#                     data_type="continuous",
#                     task={"season": "MSTL"},
#                     figsize=[18, 3],
#                     period=[DAYS_IN_WEEK, DAYS_IN_YEAR],
#                     figure=False,
#                 )
#                 # make sure data length is long enough
#                 if len(trend) <= DAYS_IN_WEEK * 5:
#                     continue

#                 past_wk = sum(trend[-DAYS_IN_WEEK:]) / DAYS_IN_WEEK
#                 month_ago = (
#                     sum(trend[-DAYS_IN_WEEK * 5 : -DAYS_IN_WEEK * 4]) / DAYS_IN_WEEK
#                 )

#                 if len(trend) > DAYS_IN_YEAR + DAYS_IN_WEEK:
#                     year_ago = (
#                         sum(trend[-DAYS_IN_YEAR - DAYS_IN_WEEK : -DAYS_IN_YEAR])
#                         / DAYS_IN_WEEK
#                     )
#                     year_change = (
#                         abs(metric["goal"] - past_wk) / abs(metric["goal"] - year_ago)
#                     ) - 1
#                     month_change = (
#                         abs(metric["goal"] - past_wk) / abs(metric["goal"] - month_ago)
#                     ) - 1

#                     if year_change > epsilon or month_change > epsilon:
#                         alerts.append(
#                             {
#                                 "name": metric["name"],
#                                 "image": season_image,
#                                 "link": metric["dashboard"],
#                                 "wk_val": past_wk,
#                                 "month_val": month_ago,
#                                 "year_val": year_ago,
#                                 "month_change": past_wk / month_ago - 1,
#                                 "year_change": past_wk / year_ago - 1,
#                             }
#                         )
#                         trend_worsening.append(metric["name"])
#                 else:
#                     month_change = (
#                         abs(metric["goal"] - past_wk) / abs(metric["goal"] - month_ago)
#                     ) - 1

#                     if month_change > epsilon:
#                         alerts.append(
#                             {
#                                 "name": metric["name"],
#                                 "image": season_image,
#                                 "link": metric["dashboard"],
#                                 "wk_val": past_wk,
#                                 "month_val": month_ago,
#                                 "month_change": past_wk / month_ago - 1,
#                             }
#                         )
#                         trend_worsening.append(metric["name"])

#                 long_term_checked.append(
#                     {"name": metric["name"] + ": " + stat, "link": metric["dashboard"]}
#                 )

#     # Create slack notification
#     if len(alerts) == 0:
#         attachment = init_alert(
#             "#23C552",
#             "*" + yesterday_date + ": No long term trend worsenings detected.*",
#         )
#     else:
#         attachment = init_alert(
#             "#F84F31",
#             "*"
#             + yesterday_date
#             + ": Long term trend worsenings in "
#             + ", ".join(trend_worsening)
#             + ".*\n<!channel>\n",
#         )

#         for detail in alerts:
#             # What metric + current value
#             text = ""
#             if detail["link"] is None:
#                 text = detail["name"]
#             else:
#                 text = "<" + detail["link"] + "|" + detail["name"] + ">"
#             if "year_val" in detail:
#                 text += (
#                     "\n\tWeekly Average Value This Week: "
#                     + str("{:.2f}".format(detail["wk_val"]))
#                     + "\n\t Weekly Average Value One Month Ago: "
#                     + str("{:.2f}".format(detail["month_val"]))
#                     + "\n\t Weekly Average Value One Year Ago: "
#                     + str("{:.2f}".format(detail["year_val"]))
#                 )
#                 text += (
#                     "\n\n\tChange from last month: "
#                     + str("{:.2f}".format(detail["month_change"]))
#                     + "\n\t Change from last year: "
#                     + str("{:.2f}".format(detail["year_change"]))
#                 )
#             else:
#                 text += (
#                     "\n\tWeekly Average Value This Week: "
#                     + str("{:.2f}".format(detail["wk_val"]))
#                     + "\n\t Weekly Average Value One Month Ago: "
#                     + str("{:.2f}".format(detail["month_val"]))
#                 )
#                 text += "\n\n\tChange from last month: " + str(
#                     "{:.2f}".format(detail["month_change"])
#                 )

#             add_text(attachment, text)
#             add_image(attachment, detail["image"])

#     checked = "Checked:"
#     for metric in long_term_checked:
#         if metric["link"] is not None:
#             checked += "\n\t<" + metric["link"] + "|" + metric["name"] + ">"
#         else:
#             checked += "\n\t" + metric["name"]

#     add_text(attachment, checked)

#     # Wait for Slack API to post image before attachment, s.t. most recent message is the notification
#     time.sleep(2)

#     notification_text = "Biweekly report: " + (
#         "No long term trend worsenings detected."
#         if len(alerts) == 0
#         else "Long term trend worsenings detected. Check details."
#     )
#     client.chat_postMessage(
#         channel=channel_name, text=notification_text, attachments=[attachment]
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC # Long term trend: Run and Display in Slack

# COMMAND ----------

# # Runs long term trend alerts biweekly
# if today.day in days:
#     metrics = filter_and_aggregate_metric("'ALL'", long_term=True)
#     run_long_term_detection_on_all_metrics_and_alert(metrics)
