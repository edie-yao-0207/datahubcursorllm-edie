# Databricks notebook source
# MAGIC %run /backend/dataproducts/benchmarking/org_segmentation/org_segments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save previous org attributes to enable comparison stats
# MAGIC drop table if exists dataproducts.org_attributes_monitor_prev;
# MAGIC create table if not exists dataproducts.org_attributes_monitor_prev as (
# MAGIC   select * from dataproducts.org_attributes_monitor
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dataproducts.org_attributes_monitor;
# MAGIC create table if not exists dataproducts.org_attributes_monitor as (
# MAGIC   select * from playground.org_attributes
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save previous org segments to enable comparison stats
# MAGIC drop table if exists dataproducts.org_segments_monitor_prev;
# MAGIC create table if not exists dataproducts.org_segments_monitor_prev as (
# MAGIC   select * from dataproducts.org_segments_monitor
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dataproducts.org_segments_monitor;
# MAGIC create table if not exists dataproducts.org_segments_monitor as (
# MAGIC   select * from playground.org_segments_v2
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Monitor Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

import pandas as pd
import operator

# construct comparison cohorts which contains the list of orgs for each cohort
# ie.
# {
# '00': [21, 567, 6800 ...],
# '01': [...],
# '...': [...]
# }
def construct_comparison_cohorts(org_cohort_df):
    cohort_ids = [
        "00",
        "01",
        "02",
        "03",
        "10",
        "11",
        "12",
        "13",
        "20",
        "21",
        "22",
        "23",
    ]
    comparison_cohorts = {}
    for cid in cohort_ids:
        comparison_cohorts[cid] = org_cohort_df[org_cohort_df["cohort_id"] == cid]

    return comparison_cohorts


# Find similar cohorts by merging og cohort org list with the org list of the new cohort.
# We take the max of this output later to get the most similar cohort
def find_most_similar_cohort(og_cohort, comparison_cohorts):
    #   returns dict of compared cohorts with the count of similar org_ids
    results = {}
    results_raw_count = {}
    for key in comparison_cohorts:
        curr_comparison_cohort = comparison_cohorts[key]
        merge_df = og_cohort.merge(
            curr_comparison_cohort, on="org_id", how="outer", indicator=True
        )
        results_raw_count[key] = merge_df.groupby("_merge").count()["org_id"].both

        removed_org_ids = merge_df[merge_df["_merge"] == "left_only"]
        added_org_ids = merge_df[merge_df["_merge"] == "right_only"]

        results[key] = {
            "left_df_count": og_cohort["org_id"].count(),
            "right_df_count": comparison_cohorts[key]["org_id"].count(),
            "left_only": merge_df.groupby("_merge").count()["org_id"].left_only,
            "right_only": merge_df.groupby("_merge").count()["org_id"].right_only,
            "both": merge_df.groupby("_merge").count()["org_id"].both,
            "removed_org_ids": merge_df[merge_df["_merge"] == "left_only"],
            "added_org_ids": merge_df[merge_df["_merge"] == "right_only"],
        }

    return results, results_raw_count


def match_cohort_results(org_cohort_run1, org_cohort_run2):
    raw_results = {}
    results = {}
    comparison_cohorts1 = construct_comparison_cohorts(org_cohort_run1)
    comparison_cohorts2 = construct_comparison_cohorts(org_cohort_run2)

    # Create a list that contains the similar orgs for each cohort id between the og and new data.
    # We take the max below to find the most simlilar org
    for cohort_id in comparison_cohorts1:
        comparisons, comparison_counts = find_most_similar_cohort(
            comparison_cohorts1[cohort_id], comparison_cohorts2
        )
        raw_results[cohort_id] = comparisons
        results[cohort_id] = comparison_counts

    cohort_ids = []
    new_cohort_id = []
    old_cohort_count = []
    new_cohort_count = []
    orgs_removed_count = []
    orgs_removed_reassigned_cohort = []
    orgs_removed_dropped_from_cohorts = []
    orgs_added_count = []
    orgs_added_reassigned_cohort = []
    orgs_added_previously_no_cohort = []
    orgs_same_count = []
    orgs_same_percentage = []

    raw_cohort_results = {}
    for cohort_id in results:
        # Note: PySpark lib overwrites the max function which causes error
        most_similar_org = max(results[cohort_id].items(), key=operator.itemgetter(1))[
            0
        ]  # most similar cohort id in new data
        new_cohort_results = raw_results[cohort_id][most_similar_org]

        (
            removed_reassigned_orgs,
            removed_orgs,
            removed_reassigned_orgs_df,
        ) = compare_removed_cohorts_to_new_org_cohorts(
            new_cohort_results["removed_org_ids"], org_cohort_run2
        )
        (
            added_reassigned_orgs,
            orgs_previously_without_cohort,
            added_reassigned_orgs_df,
        ) = compare_added_cohorts_to_old_org_cohorts(
            new_cohort_results["added_org_ids"], org_cohort_run1
        )

        #   Add to dict with all supporting data
        new_cohort_results["removed_org_ids"] = removed_reassigned_orgs_df
        new_cohort_results["added_org_ids"] = added_reassigned_orgs_df
        raw_cohort_results[cohort_id] = new_cohort_results

        cohort_ids.append(cohort_id)
        new_cohort_id.append(most_similar_org)
        old_cohort_count.append(new_cohort_results["left_df_count"])
        new_cohort_count.append(new_cohort_results["right_df_count"])
        orgs_removed_count.append(new_cohort_results["left_only"])
        orgs_removed_reassigned_cohort.append(removed_reassigned_orgs)
        orgs_removed_dropped_from_cohorts.append(removed_orgs)
        orgs_added_count.append(new_cohort_results["right_only"])
        orgs_added_reassigned_cohort.append(added_reassigned_orgs)
        orgs_added_previously_no_cohort.append(orgs_previously_without_cohort)
        orgs_same_count.append(new_cohort_results["both"])
        orgs_same_percentage.append(
            (new_cohort_results["both"] / new_cohort_results["left_df_count"]) * 100
        )

    result_summary = pd.DataFrame(
        data={
            "cohort_id": cohort_ids,
            "new_cohort_id": new_cohort_id,
            "prev_count": old_cohort_count,
            "new_count": new_cohort_count,
            #     "orgs_removed_count": orgs_removed_count,
            "reassigned_to_another_cohort": orgs_removed_reassigned_cohort,
            #     "reassigned_to_orgs": list(new_cohort_results['removed_org_ids']),
            "orgs_dropped_no_assignment": orgs_removed_dropped_from_cohorts,
            #     "orgs_added_count": orgs_added_count,
            "reassigned_to_this_cohort": orgs_added_reassigned_cohort,
            #     "reassigned_from_orgs": list(new_cohort_results['added_org_ids']),
            "orgs_added_previously_no_cohort": orgs_added_previously_no_cohort,
            #     "orgs_same_count": orgs_same_count,
            "orgs_same_percentage": orgs_same_percentage,
        }
    )

    relabel_mapping = pd.DataFrame(
        data={"original_cohort_id": cohort_ids, "flipped_cohort_id": new_cohort_id}
    )

    relabel_mapping = relabel_mapping.set_index("flipped_cohort_id").to_dict()[
        "original_cohort_id"
    ]

    return result_summary, raw_cohort_results, relabel_mapping


def compare_removed_cohorts_to_new_org_cohorts(removed_cohorts_df, new_org_cohorts):
    del removed_cohorts_df["_merge"]
    merge_df = removed_cohorts_df.merge(
        new_org_cohorts, on="org_id", how="left", indicator=True
    )
    orgs_reassigned_cohort = merge_df.groupby("_merge").count()["org_id"].both
    orgs_now_without_cohort = removed_cohorts_df.org_id.count() - orgs_reassigned_cohort
    del merge_df["cohort_id_y"]
    merge_df.columns = ["org_id", "origin_cohort", "dest_cohort", "_merge"]
    return orgs_reassigned_cohort, orgs_now_without_cohort, merge_df


def compare_added_cohorts_to_old_org_cohorts(added_cohorts_df, old_org_cohorts):
    del added_cohorts_df["_merge"]
    merge_df = old_org_cohorts.merge(
        added_cohorts_df, on="org_id", how="right", indicator=True
    )
    orgs_reassigned_cohort = merge_df.groupby("_merge").count()["org_id"].both
    orgs_previously_without_cohort = (
        added_cohorts_df.org_id.count() - orgs_reassigned_cohort
    )
    del merge_df["cohort_id_x"]
    merge_df.columns = ["org_id", "origin_cohort", "dest_cohort", "_merge"]
    return orgs_reassigned_cohort, orgs_previously_without_cohort, merge_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics

# COMMAND ----------

query = "select * from dataproducts.org_segments_v2"
org_segments_v2_sdf = spark.sql(query)
org_segments_v2_df = org_segments_v2_sdf.toPandas()

query = "select * from dataproducts.org_segments_monitor"
org_segments_monitor_sdf = spark.sql(query)
org_segments_monitor_df = org_segments_monitor_sdf.toPandas()

# COMMAND ----------

results, raw_cohort_results, _ = match_cohort_results(
    org_segments_v2_df, org_segments_monitor_df
)
results.head(12)

# COMMAND ----------

metrics = pd.DataFrame()
metrics["cohort_id"] = results["cohort_id"]
metrics["prev_count"] = results["prev_count"]
metrics["new_count"] = results["new_count"]
metrics["delta"] = results["new_count"] - results["prev_count"]
metrics["reassigned_from"] = results["reassigned_to_another_cohort"]
metrics["reassigned_to"] = results["reassigned_to_this_cohort"]
metrics["dropped"] = results["orgs_dropped_no_assignment"]
metrics["new"] = results["orgs_added_previously_no_cohort"]
metrics["similarity"] = results["orgs_same_percentage"].round(decimals=2)

# COMMAND ----------

metrics

# COMMAND ----------

summary = {}
summary["prev_total_orgs"] = metrics["prev_count"].sum()
summary["new_total_orgs"] = metrics["new_count"].sum()
summary["delta_total_orgs"] = summary["new_total_orgs"] - summary["prev_total_orgs"]
summary["total_dropped"] = metrics["dropped"].sum()
summary["total_new"] = metrics["new"].sum()
summary["average_similary (%)"] = metrics["similarity"].mean().round(decimals=2)

# COMMAND ----------

summary

# COMMAND ----------

# MAGIC %md
# MAGIC # Send Metrics

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

metrics_sdf = spark.createDataFrame(metrics).rdd.collect()
columns = [
    "cohort_id",
    "prev_count",
    "new_count",
    "Δ",
    "reassigned_from",
    "reassigned_to",
    "dropped",
    "new",
    "similarity (%)",
]

row_data = []
for result in metrics_sdf:
    row_data.append(
        [
            str(result.cohort_id),
            str(result.prev_count),
            str(result.new_count),
            str(result.delta),
            str(result.reassigned_from),
            str(result.reassigned_to),
            str(result.dropped),
            str(result.new),
            str(result.similarity),
        ]
    )

org_segments_refresh_metrics = make_table("", columns, row_data)

# COMMAND ----------

try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["fleet-benchmarks-refresh"],
        key="org_segments_metrics",
        title="Org Segments Refresh Metrics",
        databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267286889/command/3067644267287995",
        body=[
            "\n".join(org_segments_refresh_metrics),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["fleet-benchmarks-refresh"],
        key="org_segments_summary",
        title="Org Segments Refresh Summary",
        databricks_edit_cell_url="https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3067644267286889/command/3067644267287995",
        body=[
            make_metric_row(title="Prev Total Orgs", value=summary["prev_total_orgs"]),
            make_metric_row(title="New Total Orgs", value=summary["new_total_orgs"]),
            make_metric_row(
                title="Delta Total Orgs", value=summary["delta_total_orgs"]
            ),
            make_metric_row(title="Total Dropped", value=summary["total_dropped"]),
            make_metric_row(title="Total New", value=summary["total_new"]),
            make_metric_row(
                title="Avg Similarity %", value=summary["average_similary (%)"]
            ),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

send_metrics()

# COMMAND ----------
