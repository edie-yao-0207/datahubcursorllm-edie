# Databricks notebook source
# MAGIC %md ## Imports

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %run /Shared/Shared/slack-tables

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# Following install command was commented out as they are no longer
# supported in UC clusters. Please ensure that this notebook is run
# in a cluster with the required libraries installed.
# dbutils.library.installPyPI("salesforce_reporting")

# COMMAND ----------

import calendar
from datetime import date, datetime, timedelta
from typing import List

# Salesforce, Slack and pandas imports
import boto3
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_date, date_sub, from_json
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from slack import WebClient
from slack.errors import SlackApiError

import salesforce_reporting as sfr

# COMMAND ----------


internal_orgs = (
    spark.sql(
        "select distinct id as OrgId from clouddb.organizations where internal_type = 1"
    )
    .rdd.flatMap(lambda x: x)
    .collect()
)

# COMMAND ----------

# MAGIC %md ## Import mobile logging data

# COMMAND ----------

# DBTITLE 1,Import mobile logging data
# Define the set of event types we want in our view
# (Add relevant log event types here, so they'll be available later)
event_types = [
    "FLEET_INSTALLER_COMPLETE_FLOW",
    "FLEET_INSTALLER_CONFIRM_CABLE",
    "FLEET_INSTALLER_CAPTURE_STILL_IMAGE",
    "FLEET_INSTALLER_TAKE_PHOTO_ERROR",
    "FLEET_INSTALLER_DONE_CALIBRATING",
    "FLEET_INSTALLER_PRESSED_HELP_BUTTON",
    "FLEET_INSTALLER_VIN_SCAN_MODAL_PRESSED_BUTTON",
    "FLEET_INSTALLER_ATTEMPTED_INCOMPATIBLE_VARIANT",
    "FLEET_INSTALLER_ATTEMPTED_STANDARD_INSTALL_ON_H_COMPATIBLE_VEHICLE",
    "FLEET_INSTALLER_ATTEMPTED_TO_CONFIRM_INVALID_VIN",
    "FLEET_INSTALLER_EXIT_FLOW",
    "GLOBAL_NAVIGATE",
    "DRIVER_HOS_CERTIFY_AND_SUBMIT_CHARTS",
    "DRIVER_FINISH_SIGNIN_FLOW",
    "DRIVER_START_WIZARD",
    "DRIVER_FINISH_WIZARD",
    "DRIVER_PAUSE_WIZARD",
    "DRIVER_RESUME_WIZARD",
    "DRIVER_WIZARD_PRESS_HELP",
    "DRIVER_WIZARD_SUBMIT_FEEDBACK",
    "DRIVER_ABANDON_WIZARD",
    "DRIVER_WIZARD_ALERT_MISSING_PRECONDITIONS",
    "DRIVER_WIZARD_ALERT_CURRENT_NODE_IS_REQUIRED",
    "GLOBAL_CODEPUSH_SUCCEEDED_UPDATE",
    "DRIVER_HOS_DUTY_STATUS_CHANGE_COMPONENT_SUBMIT",
    "DRIVER_EU_HOS_SET_STATUS",
    "FLEET_SAFETY_VIDEO_VIEWED",
    "DRIVER_COMPLETED_PASSWORD_RESET",
    "FLEET_CATCH_DIAGNOSTICS_ERROR",
    "DRIVER_LOG_DOCUMENT_SCANNING_START",
    "DRIVER_LOG_DOCUMENT_SCANNING_END",
]

# 2020-08-19 is when IX launched
start_date = "2020-08-19"

# Import the mobile_troy_app_logs data
spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_app_logs"
).load().filter(f"Date >= '{start_date}'").filter(
    col("EventType").isin(event_types)
).filter(
    ~col("OrgId").isin(internal_orgs)
).createOrReplaceTempView(
    "filtered_mobile_app_logs"
)

# Import the mobile_troy_queueing_uploader_mutation data
spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_queueing_uploader_mutation"
).load().filter(f"Date >= '{start_date}'").filter(
    ~col("OrgId").isin(internal_orgs)
).createOrReplaceTempView(
    "filtered_mobile_queueing_uploader_mutations"
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Best practice is to cache the table you import as BigQuery (e.g., to handle NULL values)
# MAGIC cache table filtered_mobile_app_logs;
# MAGIC cache table filtered_mobile_queueing_uploader_mutations;

# COMMAND ----------


mobile_queueing_uploader_mutations_all_time = spark.table(
    "filtered_mobile_queueing_uploader_mutations"
)
mobile_queueing_uploader_mutations_yesterday = (
    mobile_queueing_uploader_mutations_all_time.filter(
        col("Date") >= date_sub(current_date(), 1)
    ).filter(col("Date") < current_date())
)

mobile_app_logs_all_time = spark.table("filtered_mobile_app_logs")
mobile_app_logs_yesterday = mobile_app_logs_all_time.filter(
    col("Date") >= date_sub(current_date(), 1)
).filter(col("Date") < current_date())
mobile_app_logs_past_week = mobile_app_logs_all_time.filter(
    col("Date") >= date_sub(current_date(), 7)
).filter(col("Date") < current_date())

# COMMAND ----------

# MAGIC %md # Helpers

# COMMAND ----------

# DBTITLE 1,Data Presentation & Inspection

yesterday = date.today() - timedelta(days=1)

#
# Data Inspection Helpers
#

__METRICS__ = {}


def add_metrics(
    key,
    title,
    body,
    disable_real_slack_post_for_testing=False,
    slack_channels=[],
    day=None,
):
    """
    add_metrics constructs a slack post and adds it to the __METRICS__ global object

    - key is a unique identifier for this metric set
    - title goes at the beginning of the slack post
    - body is an array of strings that make up the slack post
    - set disable_real_slack_post_for_testing to disable posting this metric to slack
    - slack_channels is where we'll post these metrics
    - day is optional for which day of the week to post on. This should be a string ie Tuesday
    """
    __METRICS__[key] = {
        "slack_text": make_metrics(title=title, body=body),
        "slack_channels": slack_channels,
        "disable_real_slack_post_for_testing": disable_real_slack_post_for_testing,
        "day": day,
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
    curDay = calendar.day_name[datetime.today().weekday()].lower()
    for (key, metric) in __METRICS__.items():
        if key_override is not None and key != key_override:
            continue
        if metric["day"] is not None and metric["day"].lower() != curDay:
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


def make_metric_row(title, value, includeTab=True):
    """make_metric_row formats a bulletted KV row for slack"""
    strValue = f"{value:,}" if not isinstance(value, str) else value
    tabValue = "\t"
    if not includeTab:
        tabValue = ""
    return f"{tabValue}• {title}: *{strValue}*"


def make_metric_row_group(title, body):
    """make_metric_row_group groups some rows under a title, surrounded by empty lines"""
    rows = [title] + body
    return "\n".join(rows)


faqsLink = "https://samsaur.us/daily-product-metrics-faq"


def make_metrics(title, body):
    """make_metrics constructs a slack post"""
    metrics_post_rows = [f"*{title}* ({yesterday})"] + body
    metrics_post_rows.append(f"<{faqsLink}|FAQs doc>")
    metrics_post = "\n".join(metrics_post_rows)
    return metrics_post


# COMMAND ----------


# DBTITLE 1,Slack Helpers for sending CSV


# Sends an image to the specified slack channel. The image should be specified via dbfs path
# ex: /dbfs/mnt/mounted_folder/directory/img.png


def send_csv_to_channels(file_path: str, slack_channels: List[str]):
    client = get_client()
    for cur_channel in slack_channels:
        try:
            client.files_upload_v2(
                file=file_path,
                channels=cur_channel,
            )
        except SlackApiError as e:
            return f"error uploading file: {e}"


# COMMAND ----------

# DBTITLE 1,Mobile Data Helpers
#
# Data Gathering Helpers
#


global_navigate_schema = StructType(
    [StructField(name="cleanDestination", dataType=StringType(), nullable=True)]
)

mobile_app_navigations_yesterday = mobile_app_logs_yesterday.filter(
    col("EventType") == "GLOBAL_NAVIGATE"
).withColumn("json", from_json(col("JSONParams"), global_navigate_schema))

mobile_app_navigations_past_week = mobile_app_logs_past_week.filter(
    col("EventType") == "GLOBAL_NAVIGATE"
).withColumn("json", from_json(col("JSONParams"), global_navigate_schema))

mobile_queueing_uploader_successes_yesterday = (
    mobile_queueing_uploader_mutations_yesterday.filter(col("Status") == "OK")
)


def count_mobile_queueing_uploader_successes_yesterday(query_name):
    return mobile_queueing_uploader_successes_yesterday.filter(
        col("QueryName") == query_name
    ).count()


def count_mobile_navigations_yesterday(destination):
    """count_mobile_navigations_yesterday returns the number of navigations from yesterday to the given destination"""
    return mobile_app_navigations_yesterday.filter(
        col("json.cleanDestination") == destination
    ).count()


def count_mobile_logs_yesterday(event_type):
    """count_mobile_logs_yesterday returns the number of logs from yesterday with the given event_type"""
    if not event_type in event_types:
        raise Exception(
            f"event_types does not include {event_type}! Edit event_types at the top of the notebook"
        )
    return mobile_app_logs_yesterday.filter(col("EventType") == event_type).count()


def count_mobile_logs_all_time(event_type):
    """count_mobile_logs_all_time returns the number of logs since start_date with the given event_type"""
    if not event_type in event_types:
        raise Exception(
            f"event_types does not include {event_type}! Edit event_types at the top of the notebook"
        )
    return mobile_app_logs_all_time.filter(col("EventType") == event_type).count()


def count_mobile_logs_with_matching_json_filter_yesterday(event_type, json_param_value):
    """count_mobile_logs_yesterday returns the number of logs from yesterday with the given event_type and json_filter"""
    if not event_type in event_types:
        raise Exception(
            f"event_types does not include {event_type}! Edit event_types at the top of the notebook"
        )
    return (
        mobile_app_logs_yesterday.filter((col("EventType")) == event_type)
        .filter(col("JSONParams").contains(json_param_value))
        .count()
    )


# COMMAND ----------

# MAGIC %md # Metrics

# COMMAND ----------

# DBTITLE 1,Elizabeth's Test Cell
# try:
#     completed_flows_schema = StructType([StructField("deviceId", LongType(), True), StructField("hasVg", BooleanType(), True), StructField("hasCm", BooleanType(), True), StructField("hasAg", BooleanType(), True)])
#     completed_flows = mobile_app_logs_all_time.filter(col("EventType") == "FLEET_INSTALLER_COMPLETE_FLOW").withColumn('json', from_json(col('JSONParams'), completed_flows_schema))

#     total_completed_vg_count = completed_flows.filter(col('json.hasVg')).count() # either VG only, or VG + CM
#     total_completed_cm_count = completed_flows.filter(col('json.hasCm') & ~col('json.hasVg')).count() # CM only
#     total_completed_vg_cm_count = total_completed_vg_count + total_completed_cm_count
# #     totalVgCm = completed_flows.filter(col('json.hasVg') & col('json.hasCm')).count()
# #     totalVgOnly = completed_flows.filter(col('json.hasVg') & ~col('json.hasCm')).count()
# #     totalCmOnly = completed_flows.filter(~col('json.hasVg') & col('json.hasCm')).count()
#     total_completed_ag_count = completed_flows.filter(col('json.hasAg')).count()

# except Exception as e: print(e)


# COMMAND ----------

# DBTITLE 1,EM swap/install experience
try:
    #
    # Completed installations
    #
    completed_flows_schema = StructType(
        [
            StructField("deviceId", LongType(), True),
            StructField("hasEm", BooleanType(), True),
            StructField("isPairGateways", BooleanType(), True),
        ]
    )
    completed_flows = mobile_app_logs_all_time.filter(
        col("EventType") == "FLEET_INSTALLER_COMPLETE_FLOW"
    ).withColumn("json", from_json(col("JSONParams"), completed_flows_schema))

    completed_em_flows = completed_flows.filter(col("json.hasEm"))
    completed_em_swap_count = completed_em_flows.filter(
        col("json.isPairGateways")
    ).count()
    completed_em_install_count = completed_em_flows.count() - completed_em_swap_count

    total_users_completed_count = completed_flows.select("userID").distinct().count()
    total_orgs_completed_count = (
        completed_flows.select("orgId")
        .filter(col("orgId").isNotNull())
        .distinct()
        .count()
    )

    #
    # CSAT scores
    #
    feedbacks = (
        spark.table("clouddb.feedbacks")
        .filter(col("user_agent") == "admin")
        .filter(col("product_team_owner") == "samsara-dev/fleet-foundations")
    )
    feedbacks_with_rating_em = feedbacks.filter(
        col("text").contains("@@SAMSARA_INSTALLER_RATING_EM@@")
    ).withColumn(
        "rating", F.split(feedbacks.text, "@@SAMSARA_INSTALLER_RATING_EM@@")[1]
    )
    avg_csat_em_ix = feedbacks_with_rating_em.agg(F.avg("rating")).first()[0]
    if avg_csat_em_ix == None:
        avg_csat_em_ix = 0

    avg_csat_em_ix = round(avg_csat_em_ix, 2)
    total_csat_responses_em = feedbacks_with_rating_em.count()

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["eu-hos-metrics"],
        key="em-swap",
        title="Daily EM IX Metrics",
        body=[
            make_metric_row_group(
                title="Unique IX customers",
                body=[
                    make_metric_row(
                        title="EM Installs(all time)", value=completed_em_install_count
                    ),
                    make_metric_row(
                        title="EM Swaps(all time)", value=completed_em_swap_count
                    ),
                    make_metric_row(
                        title="Avg IX CSAT (EMs, all time)",
                        value=f"{avg_csat_em_ix}/5 [{total_csat_responses_em} responses]",
                    ),
                ],
            )
        ],
    )

except Exception as e:
    print(e)


# COMMAND ----------

# DBTITLE 1,Installer Experience
try:
    #
    # CM Image Captures
    #
    taken_photo_yesterday = count_mobile_logs_yesterday(
        "FLEET_INSTALLER_CAPTURE_STILL_IMAGE"
    )
    photo_error_yesterday = count_mobile_logs_yesterday(
        "FLEET_INSTALLER_TAKE_PHOTO_ERROR"
    )
    photo_success_rate_str = (
        "{:.1%}".format(
            (taken_photo_yesterday - photo_error_yesterday) / taken_photo_yesterday
        )
        if taken_photo_yesterday > 0
        else "N/A"
    )

    #
    # CM Calibrations
    #
    completed_calibration_schema = StructType(
        [StructField("skipped", BooleanType(), True)]
    )
    completed_calibrations_yesterday = mobile_app_logs_yesterday.filter(
        col("EventType") == "FLEET_INSTALLER_DONE_CALIBRATING"
    ).withColumn("json", from_json(col("JSONParams"), completed_calibration_schema))

    total_calibration_completions = completed_calibrations_yesterday.count()
    total_calibration_skips = completed_calibrations_yesterday.filter(
        col("json.skipped")
    ).count()

    #
    # Diagnostics try/catch errors
    #
    diagnostics_hidden_errors_yesterday = count_mobile_logs_yesterday(
        event_type="FLEET_CATCH_DIAGNOSTICS_ERROR"
    )

    #
    # Customer Feedback (CSAT)
    #
    feedbacks = (
        spark.table("clouddb.feedbacks")
        .filter(col("user_agent") == "admin")
        .filter(col("product_team_owner") == "samsara-dev/fleet-foundations")
    )
    feedbacks_with_rating = feedbacks.filter(
        col("text").contains("@@SAMSARA_INSTALLER_RATING@@")
    ).withColumn("rating", F.split(feedbacks.text, "@@SAMSARA_INSTALLER_RATING@@")[1])
    avg_csat = round(feedbacks_with_rating.agg(F.avg("rating")).first()[0], 2)
    total_csat_responses = feedbacks_with_rating.count()

    #     start roll out 12/6
    #     feedbacks_with_rating_pre_54_NAH = feedbacks_with_rating.filter(col('created_at') < "2021-12-06")
    #     avg_csat_pre_54_NAH = round(feedbacks_with_rating_pre_54_NAH.agg(F.avg("rating")).first()[0], 2)
    #     total_csat_responses_pre_54_NAH = feedbacks_with_rating_pre_54_NAH.count()

    #     feedbacks_with_rating_post_54_NAH = feedbacks_with_rating.filter(col('created_at') >= "2021-12-06")
    #     avg_csat_post_54_NAH = round(feedbacks_with_rating_post_54_NAH.agg(F.avg("rating")).first()[0], 2)
    #     total_csat_responses_post_54_NAH = feedbacks_with_rating_post_54_NAH.count()
    #   end roll out 12/6

    #     feedbacks_with_rating_past_week = feedbacks_with_rating.filter(col('created_at') > date_sub(current_date(),7))
    #     avg_csat_past_week = round(feedbacks_with_rating_past_week.agg(F.avg("rating")).first()[0], 2)
    #     total_csat_responses_past_week = feedbacks_with_rating_past_week.count()

    feedbacks_with_rating_ag = feedbacks.filter(
        col("text").contains("@@SAMSARA_INSTALLER_RATING_AG@@")
    ).withColumn(
        "rating", F.split(feedbacks.text, "@@SAMSARA_INSTALLER_RATING_AG@@")[1]
    )
    avg_csat_ag_ix = round(feedbacks_with_rating_ag.agg(F.avg("rating")).first()[0], 2)
    total_csat_responses_ag = feedbacks_with_rating_ag.count()

    hos_feedbacks_with_rating = feedbacks.filter(
        col("text").contains("@@SAMSARA_HOS_RATING@@")
    ).withColumn("rating", F.split(feedbacks.text, "@@SAMSARA_HOS_RATING@@")[1])
    hos_avg_csat = round(hos_feedbacks_with_rating.agg(F.avg("rating")).first()[0], 2)
    hos_total_csat_responses = hos_feedbacks_with_rating.count()

    #
    # Completed installations
    #
    completed_flows_schema = StructType(
        [
            StructField("deviceId", LongType(), True),
            StructField("hasVg", BooleanType(), True),
            StructField("hasCm", BooleanType(), True),
            StructField("hasAg", BooleanType(), True),
        ]
    )
    completed_flows = mobile_app_logs_all_time.filter(
        col("EventType") == "FLEET_INSTALLER_COMPLETE_FLOW"
    ).withColumn("json", from_json(col("JSONParams"), completed_flows_schema))

    total_completed_vg_count = completed_flows.filter(
        col("json.hasVg")
    ).count()  # either VG only, or VG + CM
    total_completed_cm_count = completed_flows.filter(
        col("json.hasCm") & ~col("json.hasVg")
    ).count()  # CM only
    total_completed_vg_cm_count = total_completed_vg_count + total_completed_cm_count
    #     totalVgCm = completed_flows.filter(col('json.hasVg') & col('json.hasCm')).count()
    #     totalVgOnly = completed_flows.filter(col('json.hasVg') & ~col('json.hasCm')).count()
    #     totalCmOnly = completed_flows.filter(~col('json.hasVg') & col('json.hasCm')).count()
    total_completed_ag_count = completed_flows.filter(col("json.hasAg")).count()

    total_users_completed_count = completed_flows.select("userID").distinct().count()
    total_orgs_completed_count = (
        completed_flows.select("orgId")
        .filter(col("orgId").isNotNull())
        .distinct()
        .count()
    )

    #
    # Cable Confirmations
    #
    confirmed_cable = mobile_app_logs_all_time.filter(
        col("EventType") == "FLEET_INSTALLER_CONFIRM_CABLE"
    )
    total_users_confirmed_cable = confirmed_cable.select("userID").distinct().count()

    #
    # Installer Role Users
    #
    #     installer_role_count = spark.table("clouddb.users_organizations").filter(
    #       ~col("clouddb.users_organizations.organization_id").isin(internal_orgs)
    #     ).filter(
    #       col("clouddb.users_organizations.role_id") == 24
    #     ).select('user_id').distinct().count()

    weekly_active_users = (
        mobile_app_navigations_past_week.filter(
            col("json.cleanDestination").startswith("/fleet")
        )
        .select("userID")
        .distinct()
        .count()
    )
    weekly_active_orgs = (
        mobile_app_navigations_past_week.filter(
            col("json.cleanDestination").startswith("/fleet")
        )
        .select("orgID")
        .distinct()
        .count()
    )

    weekly_active_users_routing = (
        mobile_app_navigations_past_week.filter(
            col("json.cleanDestination").startswith("/fleet/dispatch")
        )
        .select("userID")
        .distinct()
        .count()
    )

    #
    # Push notification preferences
    #
    user_org_preferences_schema = StructType(
        [StructField("enabled", BooleanType(), True)]
    )
    driver_message_pn_preferences = (
        spark.table("userorgpreferencesdb_shards.user_org_preferences")
        .filter(col("key_enum") == 3)
        .withColumn("json", from_json(col("value_str"), user_org_preferences_schema))
    )
    total_driver_pn_users = (
        driver_message_pn_preferences.filter(col("json.enabled"))
        .select("user_id")
        .distinct()
        .count()
    )

    total_orgs_pn_alerts = spark.sql(
        "select count(distinct org_id) from alertsdb.alert_admin_recipients"
    ).rdd.collect()[0][0]

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["installer-experience"],
        key="ix",
        title="Daily IX Metrics",
        day="Tuesday",
        body=[
            make_metric_row_group(
                title="Unique IX customers",
                body=[
                    make_metric_row(
                        title="Users",
                        value=f"{total_users_completed_count} users ({total_orgs_completed_count} orgs)",
                    ),
                    make_metric_row(
                        title="VG/CM Installs", value=total_completed_vg_cm_count
                    ),
                    make_metric_row(
                        title="AG Installs", value=total_completed_ag_count
                    ),
                ],
            ),
            make_metric_row_group(
                title="Admin app customers",
                body=[
                    make_metric_row(
                        title="Weekly active users", value=weekly_active_users
                    ),
                    make_metric_row(
                        title="Weekly active orgs", value=weekly_active_orgs
                    ),
                    make_metric_row(
                        title="Unique users with driver notifications",
                        value=total_driver_pn_users,
                    ),
                    make_metric_row(
                        title="Orgs using PN alerts (might be out of date)",
                        value=total_orgs_pn_alerts,
                    ),
                    make_metric_row(
                        title="Routing WAU", value=weekly_active_users_routing
                    ),
                ],
            ),
            make_metric_row_group(
                title="Customer feedback",
                body=[
                    make_metric_row(
                        title="Avg IX CSAT (VG/CMs, all time)",
                        value=f"{avg_csat}/5 [{total_csat_responses} responses]",
                    ),
                    #                   start roll out 12/6
                    #                     make_metric_row(title = "Avg IX CSAT (VG/CMs, pre-Loki)", value = f"{avg_csat_pre_54_NAH}/5 [{total_csat_responses_pre_54_NAH} responses]"),
                    #                     make_metric_row(title = "Avg IX CSAT (VG/CMs, Loki era)", value = f"{avg_csat_post_54_NAH}/5 [{total_csat_responses_post_54_NAH} responses]"),
                    #                   end roll out 12/6
                    make_metric_row(
                        title="Avg IX CSAT (AGs, all time)",
                        value=f"{avg_csat_ag_ix}/5 [{total_csat_responses_ag} responses]",
                    ),
                    make_metric_row(
                        title="Avg HOS CSAT (all time)",
                        value=f"{hos_avg_csat}/5 [{hos_total_csat_responses} responses]",
                    ),
                ],
            ),
            #             make_metric_row_group(
            #                 title = "Diagnostics Errors",
            #                 body = [
            #                     make_metric_row(title = "Errors caught (yesterday)", value = f"{diagnostics_hidden_errors_yesterday} errors yesterday"),
            #                 ]
            #             ),
            make_metric_row_group(
                title=f"Other",
                body=[
                    #                     make_metric_row(title = "Cables confirmed", value = total_users_confirmed_cable),
                    #                     make_metric_row(
                    #                         title = "CM image success rate (yesterday only)",
                    #                         value = f"{photo_success_rate_str} [{taken_photo_yesterday - photo_error_yesterday} successes / {taken_photo_yesterday} attempts]"
                    #                     ),
                    make_metric_row(
                        title="CM Calibrations (yesterday only)",
                        value=f"{total_calibration_completions - total_calibration_skips} completed, {total_calibration_skips} skipped",
                    ),
                    "<https://mixpanel.com/project/1192200/view/907/app/dashboards#id=1392004|🔗 Report Expectations mixpanel dashboard>",
                    #                     "<https://mixpanel.com/report/1192200/view/907/funnels/#view/10701101/ix-cloud-discovery-conversion|🔗 Cloud discovery mixpanel dashboard>",
                    #                     "<https://mixpanel.com/report/1192200/view/907/insights#report/11359031/mobile-trip-history|🔗 Trip history mixpanel dashboard>",
                ],
            ),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Device Services
try:
    import boto3

    s3 = get_s3_resource("samsara-databricks-playground-read")
    bucket = s3.Bucket("samsara-databricks-playground")

    # Pull down current cable diffs so we don't report diffs we've already seen
    # Set vins to empty [] to reset counts
    #   vins = []

    vin_df = (
        spark.read.format("csv")
        .options(header="true", inferSchema="true")
        .load(
            "s3a://samsara-databricks-playground/douglasboyle/cables-diff-all/cables-diff-all.csv"
        )
    )
    vins = set(vin_df.select("VIN").distinct().rdd.map(lambda r: r[0]).collect())

    # First delete the current files in s3 so that we can place new files
    bucket = s3.Bucket("samsara-databricks-playground")
    for obj in bucket.objects.filter(Prefix="douglasboyle/cables-diff-latest/"):
        s3.Object(bucket.name, obj.key).delete()
    for obj in bucket.objects.filter(Prefix="douglasboyle/cables-diff-all/"):
        s3.Object(bucket.name, obj.key).delete()

    # We don't have control over the filename when databricks writes the csv to s3 (databricks calls the csv file 'part-*'.)  This function renames the file
    def rename_databricks_file(prefix, new_filename):
        files_in_s3 = get_s3_client(
            "samsara-databricks-playground-read"
        ).list_objects_v2(Bucket="samsara-databricks-playground", Prefix=prefix,)[
            "Contents"
        ]
        for file in files_in_s3:
            cur_filename = file["Key"].split("/")[-1]
            if cur_filename.startswith("part-"):
                print(cur_filename)
                dbutils.fs.mv(
                    f"dbfs:/mnt/samsara-databricks-playground/{prefix}/{cur_filename}",
                    f"dbfs:/mnt/samsara-databricks-playground/{prefix}/{new_filename}",
                )

    cable_diff_query = """
with devices as (
SELECT
  d.id,
  d.org_id,
  d.vin,
  d.make,
  d.model,
  d.year
FROM
  productsdb.devices d
  JOIN clouddb.organizations o ON d.org_id = o.id
WHERE
  product_id = 24
  AND vin != ""
  AND vin IS NOT null
  AND o.internal_type != 1
),
devices_cables as (
select
  org_id,
  object_id,
  MAX(STRUCT(time, value.int_value)).int_value as cable_id
from
  kinesisstats.osdobdcableid
where
  date > "2020-11-01"
  and value.int_value != 0
group by
  org_id,
  object_id
),
vin_cable_recommended as (
select vin, cable_id as api_cable_id, notes, timestamp from datastreams.cable_selection_logs where vin is not null and is_api_request=true
),
cables_diff as (
select d.id, d.org_id, d.vin, d.make, d.model, d.year, api_cable_id, cable_id, notes, timestamp from vin_cable_recommended v join devices d on v.vin=d.vin join devices_cables dc on d.id=dc.object_id and d.org_id=dc.org_id
),
cables_diff_latest as (
with t1 as (
  select *, ROW_NUMBER() OVER (PARTITION BY vin order by timestamp desc, id desc) as rn from cables_diff
)
select * from t1 where rn = 1
)
select
id, org_id, vin, make, model, year, api_cable_id, cable_id, notes as api_notes, timestamp, concat('https://cloud.samsara.com/o/', org_id, '/devices/', id, '/show_v2') AS show_v2_link
from
cables_diff_latest
order by TRIM(make) desc, TRIM(model) desc, year desc
  """

    columns = [
        "device_id",
        "org_id",
        "vin",
        "make",
        "model",
        "year",
        "api_cable_id",
        "device_cable_id",
        "api_notes",
        "date",
        "show_v2_link",
    ]

    def format_row(row):
        return [
            str(row.id),
            str(row.org_id),
            str(row.vin),
            str(row.make),
            str(row.model),
            str(row.year),
            str(row.api_cable_id),
            str(row.cable_id),
            str(row.api_notes),
            str(row.timestamp),
            str(row.show_v2_link),
        ]

    cable_diff_filter_query = "api_cable_id != device_cable_id and api_cable_id != 0 and not (api_cable_id = 1 and device_cable_id = 2) and device_cable_id < 7"
    row_data = []
    row_data.extend(list(map(format_row, spark.sql(cable_diff_query).rdd.collect())))
    df = spark.createDataFrame(row_data, schema=columns)
    df.coalesce(1).write.format("csv").option("header", "true").save(
        "s3://samsara-databricks-playground/douglasboyle/cables-diff-all/"
    )
    rename_databricks_file(
        prefix="douglasboyle/cables-diff-all", new_filename="cables-diff-all.csv"
    )
    total = df.count()
    print(df.filter(cable_diff_filter_query))
    diff_total = df.filter(cable_diff_filter_query).count()
    overall_accuracy = 100 * (total - diff_total) / float(total)

    # filter to only entries we haven't seen before
    df_latest = df.filter(~df.vin.isin(vins))
    latest_total = df_latest.count()
    # capture only diffs
    df_latest = df_latest.filter(cable_diff_filter_query)
    latest_diff_total = df_latest.count()
    latest_accuracy = "-"
    if latest_total != 0:
        latest_accuracy = 100 * (latest_total - latest_diff_total) / float(latest_total)
    df_latest = df_latest.filter(~df_latest.vin.isin(vins))
    df_latest.coalesce(1).write.format("csv").option("header", "true").save(
        "s3://samsara-databricks-playground/douglasboyle/cables-diff-latest/"
    )
    rename_databricks_file(
        prefix="douglasboyle/cables-diff-latest", new_filename="cables-diff-latest.csv"
    )

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["device-services-metrics"],
        key="cables",
        title="Cable Selection Accuracy",
        body=[
            make_metric_row_group(
                title="Cables Selection Accuracy",
                body=[
                    make_metric_row(title="Overall", value=str(overall_accuracy)),
                    make_metric_row(title="Yesterday", value=str(latest_accuracy)),
                    make_metric_row(title="Overall # of cable logs", value=str(total)),
                    make_metric_row(
                        title="Yesterday # of cable logs", value=str(latest_total)
                    ),
                ],
            )
        ],
    )

    file_path = "/dbfs/mnt/samsara-databricks-playground/douglasboyle/cables-diff-latest/cables-diff-latest.csv"
    if calendar.day_name[datetime.today().weekday()].lower() == "monday":
        send_csv_to_channels(file_path, slack_channels=["device-services-metrics"])


except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Loki Install metrics
try:
    import boto3

    s3 = get_s3_resource("samsara-databricks-playground-read")
    bucket = s3.Bucket("samsara-databricks-playground")

    # First delete the current files in s3 so that we can place new files
    for obj in bucket.objects.filter(Prefix="douglasboyle/loki-installs/"):
        s3.Object(bucket.name, obj.key).delete()

    # We don't have control over the filename when databricks writes the csv to s3 (databricks calls the csv file 'part-*'.)  This function renames the file
    def rename_databricks_file(prefix, new_filename):
        files_in_s3 = get_s3_client(
            "samsara-databricks-playground-read"
        ).list_objects_v2(Bucket="samsara-databricks-playground", Prefix=prefix,)[
            "Contents"
        ]
        for file in files_in_s3:
            cur_filename = file["Key"].split("/")[-1]
            if cur_filename.startswith("part-"):
                dbutils.fs.mv(
                    f"dbfs:/mnt/samsara-databricks-playground/{prefix}/{cur_filename}",
                    f"dbfs:/mnt/samsara-databricks-playground/{prefix}/{new_filename}",
                )

    spark.sql(
        """
    create or replace temp view device_cable_hist AS (
    SELECT
      date,
      org_id,
      object_id AS device_id,
      max((time, value.int_value)).int_value AS cable_id
    FROM kinesisstats.osdobdcableid
    WHERE date >= date_sub(current_date(),90)
      AND value.int_value IS NOT NULL
      AND value.int_value NOT IN (0)
      AND org_id not in (0, 1)
    GROUP BY
      date,
      org_id,
      object_id
    );
  """
    )

    spark.sql(
        """
    create or replace temp view device_cable_latest AS (
      select
        org_id,
        device_id,
        cable_id
      from
        device_cable_hist t1
      where
        date = (
          select
            max(date)
          from
            device_cable_hist t2
          where t1.device_id = t2.device_id
        )
    );
  """
    )

    # note: uses variant_id = 10 which is NAH variant
    loki_query = """
with loki_devices as (
  select
    d.id,
    g.id as gateway_id,
    d.org_id,
    d.product_id,
    d.variant_id,
    dc.cable_id,
    d.vin,
    d.make,
    d.model,
    d.year
  from
    productsdb.devices d
    join clouddb.organizations o on d.org_id = o.id
    join productsdb.gateways g on g.device_id = d.id
    join device_cable_latest dc on dc.device_id = d.id
  where
    d.variant_id = 10
    and internal_type = 0
),
loki_devices_with_engine_model as (
  select
    t1.id,
    t1.gateway_id,
    t1.org_id,
    t1.product_id,
    t1.variant_id,
    t1.cable_id,
    t1.vin,
    coalesce(trim(lower(t1.make)), "null") as make,
    coalesce(trim(lower(t1.model)), "null") as model,
    t1.year,
    coalesce(trim(lower(t2.engine_model)), "null") as engine_model
  from
    loki_devices t1
    left join vindb_shards.device_vin_metadata t2 on t1.id = t2.device_id
    and lower(t1.make) = lower(t2.make)
    and lower(t1.model) = lower(t2.model)
    and t1.year = t2.year
),
loki_devices_with_hb as (
  SELECT
    min(hb.date) as date,
    d.org_id,
    d.id,
    d.gateway_id,
    d.product_id,
    d.variant_id,
    d.cable_id,
    d.vin,
    d.make,
    d.model,
    d.year,
    d.engine_model
  FROM
    kinesisstats.osdhubserverdeviceheartbeat AS hb
    JOIN loki_devices_with_engine_model AS d ON hb.org_id = d.org_id
    AND hb.object_id = d.id
  WHERE
    hb.date >= "2021-11-01"
    AND hb.date <= current_date()
    AND hb.value.is_end = false
    AND hb.value.is_databreak = false
  GROUP BY
    d.org_id,
    d.id,
    d.gateway_id,
    d.product_id,
    d.variant_id,
    d.cable_id,
    d.vin,
    d.make,
    d.model,
    d.year,
    d.engine_model
),
loki_devices_final as (
  select
    t1.date,
    t1.id as device_id,
    t1.gateway_id,
    t1.org_id,
    t1.product_id,
    t1.variant_id,
    t1.cable_id,
    t1.vin,
    t1.make,
    t1.model,
    t1.year,
    t1.engine_model,
    case
      when t1.make = "null" or t1.make is null then "null"
      when t1.cable_id in (13, 16, 18, 19, 20, 22, 23, 24, 25) and t1.make not in ('mitsubishi', 'hino', 'isuzu', 'mitsubishi fuso') then "invalid"
      else "valid"
    end as valid_configuration,
    concat(
      'https://cloud.samsara.com/o/',
      t1.org_id,
      '/devices/',
      t1.id,
      '/show'
    ) as show_v2_link
  from
    loki_devices_with_hb t1
)
select
  *
from
  loki_devices_final
  """

    columns = [
        "date",
        "device_id",
        "org_id",
        "product_id",
        "variant_id",
        "cable_id",
        "vin",
        "make",
        "model",
        "year",
        "engine_model",
        "valid_configuration",
        "show_v2_link",
    ]

    def format_row(row):
        return [
            str(row.date),
            str(row.device_id),
            str(row.org_id),
            str(row.product_id),
            str(row.variant_id),
            str(row.cable_id),
            str(row.vin),
            str(row.make),
            str(row.model),
            str(row.year),
            str(row.engine_model),
            str(row.valid_configuration),
            str(row.show_v2_link),
        ]

    row_data = []
    row_data.extend(list(map(format_row, spark.sql(loki_query).rdd.collect())))
    df = spark.createDataFrame(row_data, schema=columns)

    valid = df.filter("valid_configuration = 'valid'").count()
    invalid = df.filter("valid_configuration = 'invalid'").count()
    null_config = df.filter("valid_configuration = 'null'").count()

    valid_yesterday = df.filter(
        "valid_configuration = 'valid' and date >= date_sub(current_date(), 1)"
    ).count()
    invalid_yesterday = df.filter(
        "valid_configuration = 'invalid' and date >= date_sub(current_date(), 1)"
    ).count()
    null_yesterday = df.filter(
        "valid_configuration = 'null' and date >= date_sub(current_date(), 1)"
    ).count()

    # Get yesterday's installs only
    df.filter("date >= date_sub(current_date(), 1)").coalesce(1).write.format(
        "csv"
    ).option("header", "true").save(
        "s3://samsara-databricks-playground/douglasboyle/loki-installs/"
    )
    rename_databricks_file(
        prefix="douglasboyle/loki-installs", new_filename="loki-installs.csv"
    )

    ##### LOKI CST METRICS #####
    cst_query = """
    select date, gateway from datastreams.cable_selection_logs where date >= "2021-12-06" and is_api_request=true
  """

    cst_columns = ["date", "gateway"]

    def format_row(row):
        return [str(row.date), str(row.gateway)]

    cst_row_data = []
    cst_row_data.extend(list(map(format_row, spark.sql(cst_query).rdd.collect())))
    cst_df = spark.createDataFrame(cst_row_data, schema=cst_columns)

    nah_total = cst_df.filter("gateway = 'HW-VG54-NAH'").count()
    non_nah_total = cst_df.filter("gateway != 'HW-VG54-NAH'").count()
    nah_yesterday = cst_df.filter(
        "gateway = 'HW-VG54-NAH' and date >= date_sub(current_date(), 1)"
    ).count()
    non_nah_yesterday = cst_df.filter(
        "gateway != 'HW-VG54-NAH' and date >= date_sub(current_date(), 1)"
    ).count()

    ##### LOKI IX METRICS #####

    # Completed Flows
    completed_flows_schema = StructType(
        [
            StructField("deviceId", LongType(), True),
            StructField("hasVg", BooleanType(), True),
            StructField("hasCm", BooleanType(), True),
            StructField("hasAg", BooleanType(), True),
            StructField("productId", LongType(), True),
            StructField("variantId", LongType(), True),
            StructField("isPairGateways", BooleanType(), True),
        ]
    )
    completed_flows_weekly = mobile_app_logs_past_week.filter(
        col("EventType") == "FLEET_INSTALLER_COMPLETE_FLOW"
    ).withColumn("json", from_json(col("JSONParams"), completed_flows_schema))
    weekly_completed_vg54_installs = completed_flows_weekly.filter(
        col("json.hasVg")
    ).filter(col("json.productId") == 53)
    weekly_completed_vg54_standard_installs = weekly_completed_vg54_installs.filter(
        col("json.variantId") == 0
    ).count()
    weekly_completed_vg54H_installs = weekly_completed_vg54_installs.filter(
        col("json.variantId") == 10
    ).count()
    incompatible_variant_attempts_weekly = mobile_app_logs_past_week.filter(
        col("EventType") == "FLEET_INSTALLER_ATTEMPTED_INCOMPATIBLE_VARIANT"
    ).count()

    # Help button presses
    pressed_help_button_schema = StructType(
        [
            StructField("deviceId", LongType(), True),
            StructField("productId", LongType(), True),
            StructField("variantId", LongType(), True),
        ]
    )
    vg54_pressed_help_button_weekly = (
        mobile_app_logs_past_week.filter(
            col("EventType") == "FLEET_INSTALLER_PRESSED_HELP_BUTTON"
        )
        .withColumn("json", from_json(col("JSONParams"), pressed_help_button_schema))
        .filter(col("json.productId") == 53)
    )
    vg54_standard_pressed_help_button_weekly = vg54_pressed_help_button_weekly.filter(
        col("json.variantId") == 0
    ).count()
    vg54H_pressed_help_button_weekly = vg54_pressed_help_button_weekly.filter(
        col("json.variantId") == 10
    ).count()

    #   help_navs_yesterday = mobile_app_navigations_yesterday.filter(col("json.cleanDestination") == "/fleet/installer/installTips").count()

    def to_ratio_str(numerator, denominator):
        return round(numerator / denominator, 2) if denominator > 0 else "N/A"

    #   vg54_standard_help_button_ratio_str = to_ratio_str(vg54_standard_pressed_help_button_weekly, weekly_completed_vg54_standard_installs)
    #   vg54H_help_button_ratio_str = to_ratio_str(vg54H_pressed_help_button_weekly, weekly_completed_vg54H_installs)

    # VIN Modal button presses
    vin_modal_button_schema = StructType(
        [
            StructField("modal", StringType(), True),
            StructField("button", StringType(), True),
            StructField("deviceId", LongType(), True),
            StructField("productId", LongType(), True),
            StructField("variantId", LongType(), True),
            StructField("vin", StringType(), True),
            StructField("isPairGateways", BooleanType(), True),
        ]
    )
    pressed_vin_modal_button_weekly = mobile_app_logs_past_week.filter(
        col("EventType") == "FLEET_INSTALLER_VIN_SCAN_MODAL_PRESSED_BUTTON"
    ).withColumn("json", from_json(col("JSONParams"), vin_modal_button_schema))

    skipped_vg54H_vin_scan_weekly_count = pressed_vin_modal_button_weekly.filter(
        (col("json.productId") == 53)
        & (col("json.variantId") == 10)
        & (col("json.button") == "continueAnyway")
    ).count()

    invalid_vin_attempt_weekly_count = mobile_app_logs_past_week.filter(
        col("EventType") == "FLEET_INSTALLER_ATTEMPTED_TO_CONFIRM_INVALID_VIN"
    ).count()

    # Feedback
    feedbacks = (
        spark.table("clouddb.feedbacks")
        .filter(col("user_agent") == "admin")
        .filter(col("product_team_owner") == "samsara-dev/fleet-foundations")
    )
    feedbacks_with_rating = feedbacks.filter(
        col("text").contains("@@SAMSARA_INSTALLER_RATING@@")
    ).withColumn("rating", F.split(feedbacks.text, "@@SAMSARA_INSTALLER_RATING@@")[1])
    avg_csat = round(feedbacks_with_rating.agg(F.avg("rating")).first()[0], 2)
    total_csat_responses = feedbacks_with_rating.count()

    feedbacks_with_rating_past_week = feedbacks_with_rating.filter(
        col("created_at") > date_sub(current_date(), 7)
    )
    avg_csat_past_week = round(
        feedbacks_with_rating_past_week.agg(F.avg("rating")).first()[0], 2
    )
    total_csat_responses_past_week = feedbacks_with_rating_past_week.count()

    ##### PACCAR Cables #####
    paccar_cable_devices_query = """
    WITH device_cable AS (
      SELECT
      org_id,
      object_id AS device_id,
      max((time, value.int_value)).int_value AS cable_id
      FROM kinesisstats.osdobdcableid
      WHERE date >= date_sub(current_date(),180)
        AND value.int_value IS NOT NULL
        AND value.int_value NOT IN (0)
      GROUP BY
        org_id,
        object_id
    ),
    device_hb AS (
      SELECT
      org_id,
      object_id AS device_id
      FROM kinesisstats.osdhubserverdeviceheartbeat hb
      WHERE date >= date_sub(current_date(),7)
        AND hb.value.is_end = false
        AND hb.value.is_databreak = false
      GROUP BY
        org_id,
        object_id
    )
    select
    distinct d.id as device_id
    from
      productsdb.devices d
      join clouddb.organizations o on d.org_id = o.id
      join productsdb.gateways g on d.id = g.device_id
      join device_hb hb on d.id = hb.device_id
      join device_cable dc on d.id = dc.device_id
      left join vindb_shards.device_vin_metadata v on v.device_id = d.id
    where
      d.product_id = 53
      and d.variant_id = 10
      and o.internal_type = 0
      and dc.cable_id = 26
    """

    paccar_devices = (
        spark.sql(paccar_cable_devices_query).rdd.flatMap(lambda x: x).collect()
    )
    paccar_installs_weekly_count = (
        weekly_completed_vg54_installs.filter(col("json.variantId") == 10)
        .filter(col("json.deviceId").isin(paccar_devices))
        .count()
    )

    ##### Orders #####
    two_weeks_ago = date.today() - timedelta(days=14)

    def get_last_two_weeks_orders(order_report, product_sku: str):
        daily_orders_for_product = {}
        try:
            daily_orders_for_product = qtd_orders_report.series_down(product_sku)
        except Exception:
            print("Missing order data for product_sku: " + product_sku)
            return -1

        total_orders = 0
        for date_str, order_count in daily_orders_for_product.items():
            order_date = datetime.strptime(date_str, "%m/%d/%Y").date()
            if order_date < date.today() and order_date >= two_weeks_ago:
                total_orders += order_count
        return int(total_orders)

    # biweekly numbers volume
    sf_connection = sfr.Connection(
        dbutils.secrets.get(scope="loki_salesforce", key="SF_USERNAME"),
        dbutils.secrets.get(scope="loki_salesforce", key="SF_PASSWORD"),
        dbutils.secrets.get(scope="loki_salesforce", key="SF_SECURITY_TOKEN"),
    )
    qtd_orders_report = sf_connection.get_report("00O4p000004cGVKEA2")
    qtd_orders_report = sfr.MatrixParser(qtd_orders_report)

    total_shipped_thor = get_last_two_weeks_orders(qtd_orders_report, "HW-VG54-NA")
    total_shipped_loki = get_last_two_weeks_orders(qtd_orders_report, "HW-VG54-NAH")

    ##### Build Message #####
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["vg54-h-fs-enablement"],
        key="loki_metrics",
        title="Loki Install Metrics",
        day="Monday",
        body=[
            make_metric_row_group(
                title="Installs in the field",
                body=[
                    make_metric_row(
                        title="Overall valid NAH configurations", value=str(valid)
                    ),
                    make_metric_row(
                        title="Overall invalid NAH configurations", value=str(invalid)
                    ),
                    make_metric_row(
                        title="Overall null configurations (no MMY)",
                        value=str(null_config),
                    ),
                    make_metric_row(
                        title="Yesterday # of valid NAH configurations installed",
                        value=str(valid_yesterday),
                    ),
                    make_metric_row(
                        title="Yesterday # of invalid NAH configurations installed",
                        value=str(invalid_yesterday),
                    ),
                    make_metric_row(
                        title="Yesterday # of null configurations installed",
                        value=str(null_yesterday),
                    ),
                    make_metric_row(
                        title="# devices using PACCAR cables (heartbeat in last week)",
                        value=len(paccar_devices),
                    ),
                    "Please see the valid_configurations column in the accompanying CSV for a breakdown of installs\n",
                ],
            ),
            make_metric_row_group(
                title="Cable Selection Tool stats",
                body=[
                    make_metric_row(
                        title="NAH recommendations since 12/6", value=str(nah_total)
                    ),
                    make_metric_row(
                        title="non NAH recommendations since 12/6",
                        value=str(non_nah_total),
                    ),
                    make_metric_row(
                        title="% NAH recommendations since 12/6",
                        value=round(
                            100 * float(nah_total) / float(nah_total + non_nah_total), 2
                        ),
                    ),
                    make_metric_row(
                        title="NAH recommendations yesterday", value=str(nah_yesterday)
                    ),
                    make_metric_row(
                        title="non NAH recommendations yesterday",
                        value=str(non_nah_yesterday),
                    ),
                    make_metric_row(
                        title="% NAH recommendations yesterday",
                        value=round(
                            100
                            * float(nah_yesterday)
                            / float(nah_yesterday + non_nah_yesterday),
                            2,
                        ),
                    ),
                ],
            ),
            make_metric_row_group(
                title="Orders",
                body=[
                    make_metric_row(
                        title="VG54 units shipped (last two weeks)",
                        value=f"Standard: {total_shipped_thor:,}, -NAH: {total_shipped_loki:,}",
                    ),
                ],
            ),
            make_metric_row_group(
                title="Installer Experience (weekly)",
                body=[
                    make_metric_row(
                        title="IX CSAT (weekly rolling)",
                        value=f"{avg_csat_past_week}/5 [{total_csat_responses_past_week} responses]",
                    ),
                    make_metric_row(
                        title="Completed VG54 Installs",
                        value=f"Standard: {weekly_completed_vg54_standard_installs:,}, -NAH: {weekly_completed_vg54H_installs:,}",
                    ),
                    make_metric_row(
                        title="Incompatible Loki install attempts",
                        value=incompatible_variant_attempts_weekly,
                    ),
                    #               make_metric_row(title = "Total 'Help' page navigations", value = help_navs_yesterday),
                    make_metric_row(
                        title="Total 'Help' button presses",
                        value=f"Standard: {vg54_standard_pressed_help_button_weekly}, -NAH: {vg54H_pressed_help_button_weekly}",
                    ),
                    make_metric_row(
                        title="Skipped -NAH VIN scans",
                        value=skipped_vg54H_vin_scan_weekly_count,
                    ),
                    make_metric_row(
                        title="Invalid VIN submissions",
                        value=invalid_vin_attempt_weekly_count,
                    ),
                    make_metric_row(
                        title="Completed Installs on devices w/ PACCAR cables (heartbeat in last week)",
                        value=paccar_installs_weekly_count,
                    ),
                ],
            ),
        ],
    )

    file_path = "/dbfs/mnt/samsara-databricks-playground/douglasboyle/loki-installs/loki-installs.csv"
    send_csv_to_channels(file_path, slack_channels=["vg54-h-fs-enablement"])
#   send_metrics(key_override="loki_metrics", slack_channel_override="vg54-h-fs-enablement")
except Exception as e:
    print(e)

# COMMAND ----------

try:
    # Args: table_name, audit_type_id, product_ids_list
    selectActivationsTemplate = """
      SELECT
        p.name,
        count(DISTINCT d.id) total_count,
        count(DISTINCT a.org_id) total_orgs,
        collect_set(d.id) id_list
      FROM
        auditsdb_shards.audits a
      INNER JOIN clouddb.{0}s d
        ON a.{0}_id = d.id
      INNER JOIN definitions.products p
        ON d.product_id = p.product_id
      WHERE a.audit_type_id = {1}
        AND a.date = CURRENT_DATE() - 1
        AND a.org_id NOT IN
          (SELECT DISTINCT id from clouddb.organizations WHERE internal_type = 1)
        AND d.product_id IN ({2})
      GROUP BY p.name, p.product_id
      ORDER BY p.product_id
    """

    productTypes = {
        24: "VG34",
        53: "VG54",
        89: "VG54-EU",
        68: "AG26",
        83: "AG26-EU",
        62: "AG46",
        65: "AG46-EU",
        84: "AG46P",
        46: "AG46P-EU",
        20: "EM21",
        22: "EM22",
        23: "DM11",
        29: "CRGO",
    }

    productIds = ",".join(map(str, productTypes.keys()))

    selectDevices = selectActivationsTemplate.format("device", 13, productIds)
    selectWidgets = selectActivationsTemplate.format("widget", 16, productIds)

    columns = [
        "Product",
        "# Assets",
        "# Orgs",
        "List (20 max)",  # toolshed link!
    ]

    def create_formatter(table_name):
        def format_row(row):
            idList = "%2C".join(map(str, row.id_list[:20]))
            return [
                row.name,
                str(row.total_count),
                str(row.total_orgs),
                Link(
                    "CloudDB",
                    f"https://toolshed.internal.samsara.com/database/cloud_shard_0?query=select+*+from+{table_name}+where+id+in+%28{idList}%29%3B&table={table_name}",
                ),
            ]

        return format_row

    row_data = []
    row_data.extend(
        list(map(create_formatter("devices"), spark.sql(selectDevices).rdd.collect()))
    )
    row_data.extend(
        list(map(create_formatter("widgets"), spark.sql(selectWidgets).rdd.collect()))
    )

    total_devices_table = make_table("", columns, row_data)[1:]

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["device-services-metrics"],
        key="cable_accuracy",
        title="Cable Accuracy",
        body=[
            "\n".join(total_devices_table),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Daily Cable Exchange Failures
try:
    cable_exchange_failures = """
WITH annotated_entries AS (
  SELECT
    CASE
         WHEN lower(notes) LIKE '%either j1939-universal or rp1226%' THEN 'j1939_or_rp1226'
         WHEN error_text = '' THEN 'successful'
         WHEN lower(error_text) LIKE '%something went wrong%' THEN 'unknown_error'
         WHEN lower(error_text) LIKE '%no cables found%' THEN 'no_cable_found'
         WHEN lower(error_text) LIKE '%invalid vin length%' THEN 'invalid_vin_length'
         WHEN lower(error_text) LIKE '%failed to decode vin%' THEN 'decode_vin_failure' -- not a length problem, just some other problem
         WHEN lower(error_text) LIKE '%manufacturer is not registered with nhtsa%' THEN 'not_registered_with_nhtsa'
         ELSE 'other'
    END AS reason,
    COUNT(*) AS num
    FROM datastreams.cable_selection_logs
    WHERE date = DATE_SUB(CURRENT_DATE(), 1) AND is_api_request=true
    GROUP BY 1
)

SELECT *
FROM annotated_entries
PIVOT (
  SUM(num) AS total
  FOR reason IN (
    'successful', 'j1939_or_rp1226', 'unknown_error', 'no_cable_found', 'invalid_vin_length', 'decode_vin_failure', 'not_registered_with_nhtsa'
  )
)
    """
    results = spark.sql(cable_exchange_failures).rdd.collect()

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["device-services-metrics"],
        key="cst",
        title="Cable Selection Failures",
        body=[
            make_metric_row_group(
                title="Cables Selection Stats",
                body=[
                    make_metric_row(
                        title="Successful", value=str(results[0].successful)
                    ),
                    make_metric_row(
                        title="J1939 or RP1226", value=str(results[0].j1939_or_rp1226)
                    ),
                    make_metric_row(
                        title="Unknown Error", value=str(results[0].unknown_error)
                    ),
                    make_metric_row(
                        title="No Cable Found", value=str(results[0].no_cable_found)
                    ),
                    make_metric_row(
                        title="Invalid Vin Length",
                        value=str(results[0].invalid_vin_length),
                    ),
                    make_metric_row(
                        title="Decode Vin Failure",
                        value=str(results[0].decode_vin_failure),
                    ),
                    make_metric_row(
                        title="Not Registered with NHTSA",
                        value=str(results[0].not_registered_with_nhtsa),
                    ),
                    "<https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/1016884753626508/command/1016884753626509|🔗 Here's a link to a notebook for core sampling these failures!>",
                ],
            )
        ],
    )
except Exception as e:
    print(e)


# COMMAND ----------

# DBTITLE 1,Compliance
try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["compliance-metrics"],
        key="compliance",
        title="Compliance Daily Metrics",
        body=[
            make_metric_row(
                title="Charts certified and submitted",
                value=count_mobile_logs_yesterday(
                    event_type="DRIVER_HOS_CERTIFY_AND_SUBMIT_CHARTS"
                ),
            ),
            make_metric_row(
                title="Duty status changes",
                value=count_mobile_logs_yesterday(
                    event_type="DRIVER_HOS_DUTY_STATUS_CHANGE_COMPONENT_SUBMIT"
                ),
            ),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,EU Hos
try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["eu-hos-metrics"],
        key="euHos",
        title="EU HOS Daily Metrics",
        body=[
            make_metric_row(
                title="EU HOS Statuses Set",
                value=count_mobile_logs_yesterday(
                    event_type="DRIVER_EU_HOS_SET_STATUS"
                ),
            ),
            make_metric_row(
                title="Navigations to (EU) Hours page",
                value=count_mobile_navigations_yesterday(destination="/driver/euHours"),
            ),
        ],
    )

except Exception as e:
    print(e)


# COMMAND ----------

# DBTITLE 1,Routing
# try:
#     add_metrics(
#         disable_real_slack_post_for_testing = False,
#         slack_channels = ["dev-routing"],
#         key = "route",
#         title = "Routing Daily Metrics",
#         body = [
#             make_metric_row(
#                 title = "Mobile: Driver manual events created",
#                 value = count_mobile_queueing_uploader_successes_yesterday(query_name = "TroyDriverCreateDispatchManualEvent")
#             ),
#             make_metric_row(
#                 title = "Mobile: Navigations to routes list",
#                 value = count_mobile_navigations_yesterday(destination = "/driver/dispatch/routes/list/tab/:dispatchRoutesListTabIdx(\\d+)")
#             )
#         ]
#     )

# except Exception as e: print(e)


# COMMAND ----------

# DBTITLE 1,Workflows Prep
# MAGIC %sql create
# MAGIC or replace temp view org_latest_alert_modifications as (
# MAGIC   SELECT
# MAGIC     o.id AS org_id,
# MAGIC     MAX(
# MAGIC       COALESCE(
# MAGIC         GREATEST(a.created_at, a.updated_at, a.deleted_at),
# MAGIC         from_unixtime(0)
# MAGIC       )
# MAGIC     ) as latest_modification_time
# MAGIC   FROM
# MAGIC     clouddb.organizations o
# MAGIC     LEFT JOIN clouddb.groups g ON o.id = g.organization_id
# MAGIC     LEFT JOIN clouddb.alerts a ON a.group_id = g.id
# MAGIC   GROUP BY
# MAGIC     o.id
# MAGIC );
# MAGIC create
# MAGIC or replace temp view alerts_v2_ga_enabled_orgs as (
# MAGIC   select
# MAGIC     org_id,
# MAGIC     updated_at as workflows_enabled_at
# MAGIC   from
# MAGIC     releasemanagementdb_shards.feature_package_self_serve
# MAGIC   where
# MAGIC     -- hard-coded from features table alerts-v2-ga feature
# MAGIC     feature_package_uuid = "39819C5F849B4844BEE1540D3D0090E3"
# MAGIC     and enabled = 1
# MAGIC );
# MAGIC create
# MAGIC or replace temp view iux_ga_enabled_orgs as (
# MAGIC   select
# MAGIC     org_id,
# MAGIC     updated_at as workflows_enabled_at
# MAGIC   from
# MAGIC     releasemanagementdb_shards.feature_package_self_serve
# MAGIC   where
# MAGIC     -- hard-coded from features table alerts-integrated-ux-ga feature
# MAGIC     feature_package_uuid = "296BBADA241E4E9D8F4A210545547CD4"
# MAGIC     and enabled = 1
# MAGIC );
# MAGIC create or replace temp view workflows_closed_beta_orgs as (
# MAGIC   select explode(array(
# MAGIC       562949953421392	,
# MAGIC       562949953421385	,
# MAGIC       24656	,
# MAGIC       70958	,
# MAGIC       81307	,
# MAGIC       43058	,
# MAGIC       562949953421423	,
# MAGIC       16444	,
# MAGIC       44530	,
# MAGIC       21263	,
# MAGIC       31577	,
# MAGIC       23464	,
# MAGIC       19405	,
# MAGIC       3383	,
# MAGIC       29215	,
# MAGIC       2103	,
# MAGIC       874	,
# MAGIC       48144	,
# MAGIC       47509	,
# MAGIC       17158	,
# MAGIC       22756	,
# MAGIC       37081	,
# MAGIC       24942	,
# MAGIC       25715	,
# MAGIC       22818	,
# MAGIC       23813	,
# MAGIC       42057	,
# MAGIC       58207	,
# MAGIC       34157	,
# MAGIC       17219	,
# MAGIC       53465	,
# MAGIC       21651	,
# MAGIC       8000228	,
# MAGIC       79868	,
# MAGIC       27777	,
# MAGIC       68469	,
# MAGIC       46424	,
# MAGIC       9000327	,
# MAGIC       77153	,
# MAGIC       34527	,
# MAGIC       867	,
# MAGIC       562949953424584	,
# MAGIC       11000576	,
# MAGIC       35186	,
# MAGIC       81325	,
# MAGIC       63382	,
# MAGIC       3766	,
# MAGIC       14393	,
# MAGIC       2579	,
# MAGIC       3029	,
# MAGIC       3031	,
# MAGIC       4708	,
# MAGIC       5570	,
# MAGIC       7096	,
# MAGIC       8364	,
# MAGIC       8725	,
# MAGIC       20549	,
# MAGIC       20800	,
# MAGIC       23538	,
# MAGIC       27186	,
# MAGIC       28592	,
# MAGIC       29252	,
# MAGIC       29995	,
# MAGIC       31209	,
# MAGIC       32848	,
# MAGIC       33020	,
# MAGIC       33426	,
# MAGIC       34742	,
# MAGIC       36358	,
# MAGIC       39487	,
# MAGIC       41122	,
# MAGIC       44388	,
# MAGIC       45582	,
# MAGIC       46380	,
# MAGIC       47846	,
# MAGIC       48310	,
# MAGIC       48770	,
# MAGIC       48773	,
# MAGIC       48778	,
# MAGIC       48779	,
# MAGIC       48780	,
# MAGIC       49122	,
# MAGIC       52255	,
# MAGIC       54943	,
# MAGIC       57092	,
# MAGIC       57666	,
# MAGIC       58157	,
# MAGIC       58668	,
# MAGIC       63618	,
# MAGIC       68614	,
# MAGIC       76468	,
# MAGIC       79878	,
# MAGIC       80820	,
# MAGIC       81489	,
# MAGIC       81820	,
# MAGIC       83585	,
# MAGIC       4000666	,
# MAGIC       5000146	,
# MAGIC       5000591	,
# MAGIC       6001353	,
# MAGIC       6001539	,
# MAGIC       6001758	,
# MAGIC       10000528	,
# MAGIC       10001055	,
# MAGIC       11000543	,
# MAGIC       11001262	,
# MAGIC       11001679	,
# MAGIC       562949953422154	,
# MAGIC       562949953422822	,
# MAGIC       562949953424499	,
# MAGIC       562949953425730	,
# MAGIC       562949953425855	,
# MAGIC       34265	,
# MAGIC       562949953421393	,
# MAGIC       28464	,
# MAGIC       29479	,
# MAGIC       58287	,
# MAGIC       19336	,
# MAGIC       2858	,
# MAGIC       51280	,
# MAGIC       80181	,
# MAGIC       7002017	,
# MAGIC       6001969	,
# MAGIC       3189	,
# MAGIC       10001326	,
# MAGIC       8936	,
# MAGIC       11001721	,
# MAGIC       46766	,
# MAGIC       18656	,
# MAGIC       19854	,
# MAGIC       4002191	,
# MAGIC       9002167	,
# MAGIC       35184	,
# MAGIC       9001500	,
# MAGIC       18977	,
# MAGIC       10002073	,
# MAGIC       22544	,
# MAGIC       5001788	,
# MAGIC       45420	,
# MAGIC       76415	,
# MAGIC       63404	,
# MAGIC       70380	,
# MAGIC       65684	,
# MAGIC       5002294	,
# MAGIC       27250	,
# MAGIC       42704	,
# MAGIC       4000989	,
# MAGIC       11002216	,
# MAGIC       5002199	,
# MAGIC       16780	,
# MAGIC       562949953424765	,
# MAGIC       63464	,
# MAGIC       61699	,
# MAGIC       3044	,
# MAGIC       562949953425295	,
# MAGIC       30286	,
# MAGIC       52196	,
# MAGIC       63113	,
# MAGIC       10002448	,
# MAGIC       8001907	,
# MAGIC       562949953425456	,
# MAGIC       8000291	,
# MAGIC       52281	,
# MAGIC       11001085	,
# MAGIC       73285	,
# MAGIC       55596	,
# MAGIC       38558	,
# MAGIC       73852	,
# MAGIC       6000632	,
# MAGIC       10002010	,
# MAGIC       6002222	,
# MAGIC       6002383	,
# MAGIC       82150	,
# MAGIC       60036	,
# MAGIC       562949953426209	,
# MAGIC       4002393	,
# MAGIC       8002605	,
# MAGIC       7279	,
# MAGIC       7002534	,
# MAGIC       26174	,
# MAGIC       45128	,
# MAGIC       59156	,
# MAGIC       57091	,
# MAGIC       4000074	,
# MAGIC       80962	,
# MAGIC       42680	,
# MAGIC       6002586	,
# MAGIC       562949953425353	,
# MAGIC       56758	,
# MAGIC       45448	,
# MAGIC       6001269	,
# MAGIC       49293	,
# MAGIC       10001949	,
# MAGIC       6002570	,
# MAGIC       5001065	,
# MAGIC       562949953425970	,
# MAGIC       11002304	,
# MAGIC       11002599	,
# MAGIC       11002275	,
# MAGIC       3000237	,
# MAGIC       70531	,
# MAGIC       70368	,
# MAGIC       562949953426603	,
# MAGIC       25687	,
# MAGIC       7002038	,
# MAGIC       11002319	,
# MAGIC       11000473	,
# MAGIC       562949953424899	,
# MAGIC       6002484	,
# MAGIC       6002813	,
# MAGIC       72157	,
# MAGIC       9002671	,
# MAGIC       10001744	,
# MAGIC       5002713	,
# MAGIC       75931	,
# MAGIC       6002652	,
# MAGIC       7002454	,
# MAGIC       7001500	,
# MAGIC       22524	,
# MAGIC       14974	,
# MAGIC       8085	,
# MAGIC       8001165	,
# MAGIC       43490	,
# MAGIC       22458	,
# MAGIC       6002808	,
# MAGIC       11002795	,
# MAGIC       631159	,
# MAGIC       63159	,
# MAGIC       53001	,
# MAGIC       9001356	,
# MAGIC       42410	,
# MAGIC       53989	,
# MAGIC       7002796	,
# MAGIC       11002826	,
# MAGIC       5002300	,
# MAGIC       31995
# MAGIC       )
# MAGIC     ) as org_id
# MAGIC );
# MAGIC create
# MAGIC or replace temp view workflows_adoption_metrics as (
# MAGIC   select
# MAGIC     (
# MAGIC       select
# MAGIC         count(*)
# MAGIC       from
# MAGIC         workflowsdb_shards.workflow_configs conf
# MAGIC         left join clouddb.organizations org on conf.org_id = org.id
# MAGIC         left join workflowsdb_shards.alert_migration_statuses status on conf.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and is_admin != 1
# MAGIC         and status.configs_migrated_at IS NULL
# MAGIC     ) as closed_beta_workflow_config_count,
# MAGIC     (
# MAGIC       select
# MAGIC         count(*)
# MAGIC       from
# MAGIC         workflowsdb_shards.workflow_incidents inc
# MAGIC         left join clouddb.organizations org on inc.org_id = org.id
# MAGIC         left join workflowsdb_shards.alert_migration_statuses status on inc.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and is_admin != 1
# MAGIC         and status.configs_migrated_at IS NULL
# MAGIC     ) as closed_beta_workflow_incident_count,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct conf.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.workflow_configs conf
# MAGIC         left join clouddb.organizations org on conf.org_id = org.id
# MAGIC         left join workflowsdb_shards.alert_migration_statuses status on conf.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and is_admin != 1
# MAGIC         and status.configs_migrated_at IS NULL
# MAGIC     ) as closed_beta_workflow_org_count,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select
# MAGIC             *
# MAGIC           from
# MAGIC             alerts_v2_ga_enabled_orgs a
# MAGIC           where
# MAGIC             status.org_id = a.org_id
# MAGIC         )
# MAGIC     ) as migrated_eligible_org_count_total,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         left join org_latest_alert_modifications m on m.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select
# MAGIC             *
# MAGIC           from
# MAGIC             alerts_v2_ga_enabled_orgs a
# MAGIC           where
# MAGIC             status.org_id = a.org_id
# MAGIC         )
# MAGIC         and COALESCE(m.latest_modification_time, 0) < status.configs_migrated_at
# MAGIC     ) as migrated_eligible_org_count_ready,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         left join org_latest_alert_modifications m on m.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select
# MAGIC             *
# MAGIC           from
# MAGIC             alerts_v2_ga_enabled_orgs a
# MAGIC           where
# MAGIC             status.org_id = a.org_id
# MAGIC         )
# MAGIC         and COALESCE(m.latest_modification_time, 0) >= status.configs_migrated_at
# MAGIC     ) as migrated_eligible_org_count_stale,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC     ) as migrated_enabled_org_count_total,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         left join org_latest_alert_modifications m on m.org_id = status.org_id
# MAGIC         join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and COALESCE(m.latest_modification_time, 0) < status.configs_migrated_at
# MAGIC     ) as migrated_enabled_org_count_ready,
# MAGIC     (
# MAGIC       select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         left join org_latest_alert_modifications m on m.org_id = status.org_id
# MAGIC         join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and COALESCE(m.latest_modification_time, 0) >= status.configs_migrated_at
# MAGIC     ) as migrated_enabled_org_count_stale,
# MAGIC     (
# MAGIC       select
# MAGIC         count(*)
# MAGIC       from
# MAGIC         workflowsdb_shards.workflow_configs conf
# MAGIC         left join clouddb.organizations org on conf.org_id = org.id
# MAGIC         left join workflowsdb_shards.alert_migration_statuses status on conf.org_id = status.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and is_admin != 1
# MAGIC         and conf.deleted_at IS NULL
# MAGIC         and conf.source_alert_id IS NOT NULL
# MAGIC         and status.configs_migrated_at IS NOT NULL
# MAGIC     ) as migrated_workflow_config_count,
# MAGIC     (
# MAGIC       select
# MAGIC         count(*)
# MAGIC       from
# MAGIC         workflowsdb_shards.workflow_configs conf
# MAGIC         left join clouddb.organizations org on conf.org_id = org.id
# MAGIC         left join workflowsdb_shards.alert_migration_statuses status on conf.org_id = status.org_id
# MAGIC         join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and is_admin != 1
# MAGIC         and conf.deleted_at IS NULL
# MAGIC         and conf.source_alert_id IS NULL
# MAGIC     ) as new_config_count_for_migrated_orgs,
# MAGIC     (
# MAGIC       select
# MAGIC         avg(
# MAGIC           (
# MAGIC             unix_timestamp(a.workflows_enabled_at) - unix_timestamp(status.processing_visible_at)
# MAGIC           ) / (60 * 60 * 24)
# MAGIC         )
# MAGIC       from
# MAGIC         workflowsdb_shards.alert_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC         join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC     ) as avg_days_to_enablement
# MAGIC );
# MAGIC create
# MAGIC or replace temp view iux_adoption_metrics as (
# MAGIC   select
# MAGIC     -- Total enabled org count
# MAGIC       (select count(*) from iux_ga_enabled_orgs i
# MAGIC         left join clouddb.organizations org on i.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         ) as enabled_org_count,
# MAGIC     -- Alerts 1.0 customer enabled org count
# MAGIC     (select
# MAGIC         count(*)
# MAGIC       from iux_ga_enabled_orgs i
# MAGIC         left join clouddb.organizations org on i.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and NOT EXISTS (
# MAGIC           select * from alerts_v2_ga_enabled_orgs a where a.org_id = i.org_id
# MAGIC         )
# MAGIC         and NOT EXISTS (
# MAGIC           select * from workflows_closed_beta_orgs a where a.org_id = i.org_id
# MAGIC         )
# MAGIC     ) as alerts_v1_enabled_org_count,
# MAGIC     -- Alerts 2.0 OB customer enabled org count
# MAGIC         (select
# MAGIC         count(*)
# MAGIC       from iux_ga_enabled_orgs i
# MAGIC         inner join alerts_v2_ga_enabled_orgs a on i.org_id = a.org_id
# MAGIC         left join clouddb.organizations org on i.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC     ) as alerts_v2_open_beta_enabled_org_count,
# MAGIC     -- Total eligible but not enabled org count
# MAGIC     (select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.integrated_ux_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select * from iux_ga_enabled_orgs i where i.org_id = status.org_id
# MAGIC         )
# MAGIC     ) as eligible_org_count,
# MAGIC     -- Alerts 1.0 eligible but not enabled org count
# MAGIC     (select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.integrated_ux_migration_statuses status
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select * from iux_ga_enabled_orgs i where i.org_id = status.org_id
# MAGIC         )
# MAGIC         and NOT EXISTS (
# MAGIC           select * from alerts_v2_ga_enabled_orgs a where a.org_id = status.org_id
# MAGIC         )
# MAGIC         and NOT EXISTS (
# MAGIC           select * from workflows_closed_beta_orgs a where a.org_id = status.org_id
# MAGIC         )
# MAGIC     ) as alerts_v1_eligible_org_count,
# MAGIC     -- Alerts 2.0 OB eligible but not enabled org count
# MAGIC     (select
# MAGIC         count(distinct status.org_id)
# MAGIC       from
# MAGIC         workflowsdb_shards.integrated_ux_migration_statuses status
# MAGIC         inner join alerts_v2_ga_enabled_orgs a on status.org_id = a.org_id
# MAGIC         left join clouddb.organizations org on status.org_id = org.id
# MAGIC       where
# MAGIC         internal_type != 1
# MAGIC         and status.processing_visible_at IS NOT NULL
# MAGIC         and NOT EXISTS (
# MAGIC           select * from iux_ga_enabled_orgs i where i.org_id = status.org_id
# MAGIC         )
# MAGIC     ) as alerts_v2_open_beta_eligible_org_count
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Workflows
try:
    workflows_adoption_metrics = spark.table("workflows_adoption_metrics").collect()[0]
    iux_adoption_metrics = spark.table("iux_adoption_metrics").collect()[0]
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["workflows-metrics"],
        key="workflows",
        title="Workflows Daily Metrics",
        body=[
            make_metric_row_group(
                title="Alerts 2.0 Closed Beta Customers",
                body=[
                    make_metric_row(
                        title="Org Count",
                        value=workflows_adoption_metrics[
                            "closed_beta_workflow_org_count"
                        ],
                    ),
                    make_metric_row(
                        title="Config Count",
                        value=workflows_adoption_metrics[
                            "closed_beta_workflow_config_count"
                        ],
                    ),
                    make_metric_row(
                        title="Incident Count",
                        value=workflows_adoption_metrics[
                            "closed_beta_workflow_incident_count"
                        ],
                    ),
                ],
            ),
            make_metric_row_group(
                title="Alerts 2.0 OB Migrated Customers",
                body=[
                    make_metric_row(
                        title="Eligible Orgs (Total)",
                        value=workflows_adoption_metrics[
                            "migrated_eligible_org_count_total"
                        ],
                    ),
                    make_metric_row(
                        title="Eligible Orgs (Ready)",
                        value=workflows_adoption_metrics[
                            "migrated_eligible_org_count_ready"
                        ],
                    ),
                    make_metric_row(
                        title="Eligible Orgs (Stale)",
                        value=workflows_adoption_metrics[
                            "migrated_eligible_org_count_stale"
                        ],
                    ),
                    make_metric_row(
                        title="Enabled Orgs (Total)",
                        value=workflows_adoption_metrics[
                            "migrated_enabled_org_count_total"
                        ],
                    ),
                    make_metric_row(
                        title="Enabled Orgs (Ready)",
                        value=workflows_adoption_metrics[
                            "migrated_enabled_org_count_ready"
                        ],
                    ),
                    make_metric_row(
                        title="Enabled Orgs (Stale)",
                        value=workflows_adoption_metrics[
                            "migrated_enabled_org_count_stale"
                        ],
                    ),
                    make_metric_row(
                        title="New Config Count",
                        value=workflows_adoption_metrics[
                            "new_config_count_for_migrated_orgs"
                        ],
                    ),
                    make_metric_row(
                        title="Migrated Config Count",
                        value=workflows_adoption_metrics[
                            "migrated_workflow_config_count"
                        ],
                    ),
                    make_metric_row(
                        title="Avg. Days To Enablement",
                        value=round(
                            workflows_adoption_metrics["avg_days_to_enablement"], 1
                        ),
                    ),
                ],
            ),
            make_metric_row_group(
                title="Integrated UX Customers",
                body=[
                    make_metric_row(
                        title="Enabled Org Count (Total)",
                        value=iux_adoption_metrics["enabled_org_count"],
                    ),
                    make_metric_row(
                        title="Enabled Org Count (from Alerts 1.0)",
                        value=iux_adoption_metrics["alerts_v1_enabled_org_count"],
                    ),
                    make_metric_row(
                        title="Enabled Org Count (from Alerts 2.0 Open Beta)",
                        value=iux_adoption_metrics[
                            "alerts_v2_open_beta_enabled_org_count"
                        ],
                    ),
                    make_metric_row(
                        title="Eligible Org Count (Total)",
                        value=iux_adoption_metrics["eligible_org_count"],
                    ),
                    make_metric_row(
                        title="Eligible Org Count (from Alerts 1.0)",
                        value=iux_adoption_metrics["alerts_v1_eligible_org_count"],
                    ),
                    make_metric_row(
                        title="Eligible Org Count (from Alerts 2.0 Open Beta)",
                        value=iux_adoption_metrics[
                            "alerts_v2_open_beta_eligible_org_count"
                        ],
                    ),
                ],
            ),
        ],
    )

except Exception as e:
    print(e)


# COMMAND ----------

# DBTITLE 1,Driver App Prep
# MAGIC %sql
# MAGIC create or replace temp view driver_sentiment_statistics as (
# MAGIC   with
# MAGIC     groupedRatings as (
# MAGIC       select
# MAGIC         COUNT(distinct driver_id) as count,
# MAGIC         COUNT(if (action="like", true, null)) as likes,
# MAGIC         COUNT(if (action="dislike", true, null)) as dislikes,
# MAGIC         ROUND(100 * ((COUNT(if (action="like", true, null)) - COUNT(if (action="dislike", true, null))) / (COUNT(if (action="like", true, null)) + COUNT(if (action="dislike", true, null))))) as Sentiment,
# MAGIC         cast (created_at as date) as createdAtDate,
# MAGIC         unix_timestamp(TIMESTAMP(cast (created_at as date))) as unixCreatedAt
# MAGIC       from
# MAGIC         clouddb.driver_rating_actions
# MAGIC       where
# MAGIC         action in ("like", "dislike")
# MAGIC         and date >= (current_date() - interval 60 days)
# MAGIC       group by
# MAGIC         createdAtDate
# MAGIC   )
# MAGIC
# MAGIC   select
# MAGIC     (select sentiment from groupedRatings order by unixCreatedAt desc limit 1) as today,
# MAGIC     (select avg(sentiment) over(order by unixCreatedAt range between (86400 * 7) preceding and current row) from groupedRatings order by unixCreatedAt desc limit 1) as seven_day_average,
# MAGIC     (select avg(sentiment) over(order by unixCreatedAt range between (86400 * 30) preceding and current row) from groupedRatings order by unixCreatedAt desc limit 1) as thirty_day_average,
# MAGIC     (select avg(sentiment) over(order by unixCreatedAt range between (86400 * 7 * 2) preceding and (86400 * 7 * 1) preceding) from groupedRatings order by unixCreatedAt desc limit 1) as seven_day_average_seven_days_ago,
# MAGIC     (select avg(sentiment) over(order by unixCreatedAt range between (86400 * 30 * 2) preceding and (86400 * 30 * 1) preceding) from groupedRatings order by unixCreatedAt desc limit 1) as thirty_day_average_thirty_days_ago
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Driver App
try:
    driver_sentiment_statistics = spark.table("driver_sentiment_statistics").collect()[
        0
    ]

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["d2-app-metrics"],
        key="drv",
        title="Driver App Daily Metrics",
        body=[
            make_metric_row(
                title="Codepush successes",
                value=count_mobile_logs_yesterday(
                    event_type="GLOBAL_CODEPUSH_SUCCEEDED_UPDATE"
                ),
            ),
            make_metric_row_group(
                title="Auth",
                body=[
                    make_metric_row(
                        title="Viewing driver changes",
                        value=count_mobile_queueing_uploader_successes_yesterday(
                            query_name="TroyDriverChangeViewingDriver"
                        ),
                    ),
                    make_metric_row(
                        title="Signin flows finished",
                        value=count_mobile_logs_yesterday(
                            event_type="DRIVER_FINISH_SIGNIN_FLOW"
                        ),
                    ),
                    make_metric_row(
                        title="Password Resets completed",
                        value=count_mobile_logs_yesterday(
                            event_type="DRIVER_COMPLETED_PASSWORD_RESET"
                        ),
                    ),
                ],
            ),
            make_metric_row_group(
                title="Sentiment",
                body=[
                    make_metric_row(
                        title="Seven day average",
                        value=print_historical_comparison(
                            driver_sentiment_statistics[
                                "seven_day_average_seven_days_ago"
                            ],
                            driver_sentiment_statistics["seven_day_average"],
                            "seven days ago",
                        ),
                    ),
                    make_metric_row(
                        title="Thirty day average",
                        value=print_historical_comparison(
                            driver_sentiment_statistics[
                                "thirty_day_average_thirty_days_ago"
                            ],
                            driver_sentiment_statistics["thirty_day_average"],
                            "thirty days ago",
                        ),
                    ),
                ],
            ),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Ingestion
try:
    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["high_data_usage_device_alerts"],
        key="ingestion",
        title="High Data Usage for Devices Metrics",
        body=[
            make_metric_row(
                title="VG Only: Median Data Usage MB",
                value=spark.table("playground.att_data_usage_statistics_vgs").collect()[
                    0
                ]["median_data_usage_Mb"],
            ),
            make_metric_row(
                title="VG Only: Mean Data Usage MB",
                value=spark.table("playground.att_data_usage_statistics_vgs").collect()[
                    0
                ]["mean_data_usage_Mb"],
            ),
            make_metric_row(
                title="VG Only: Standard Deviation MB",
                value=spark.table("playground.att_data_usage_statistics_vgs").collect()[
                    0
                ]["std_data_usage_Mb"],
            ),
            make_metric_row(
                title="VG Only: Device IDs 3 SD's Above Mean",
                value=spark.table("playground.att_data_usage_statistics_vgs").collect()[
                    0
                ]["device_ids"],
            ),
            make_metric_row(
                title="VGs With CMs: Median Data Usage MB",
                value=spark.table(
                    "playground.att_data_usage_statistics_vgs_with_cms"
                ).collect()[0]["median_data_usage_Mb"],
            ),
            make_metric_row(
                title="VGs With CMs: Mean Data Usage MB",
                value=spark.table(
                    "playground.att_data_usage_statistics_vgs_with_cms"
                ).collect()[0]["mean_data_usage_Mb"],
            ),
            make_metric_row(
                title="VGs With CMs: Standard Deviation MB",
                value=spark.table(
                    "playground.att_data_usage_statistics_vgs_with_cms"
                ).collect()[0]["std_data_usage_Mb"],
            ),
            make_metric_row(
                title="VGs With CMs: Device IDs 3 SD's Above Mean",
                value=spark.table(
                    "playground.att_data_usage_statistics_vgs_with_cms"
                ).collect()[0]["device_ids"],
            ),
        ],
    )

except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,CI/Deploys
try:
    import datetime as dt

    today = dt.date.today()
    week_ago = today - dt.timedelta(days=7)

    spark.read.format("bigquery").option(
        "table", "backend.TestMetrics"
    ).load().createOrReplaceTempView("testmetrics")

    toptenpackages = spark.sql(
        """
    select col.Testcase, avg(col.Elapsed) as avg_time_secs
    from (
        select explode(Tests)
        from testmetrics
        where GitBranch = "master" and StartTime > '{}'
      )
    where col.Result = "pass"
    group by col.Testcase
    order by avg_time_secs desc
    limit 10
  """.format(
            week_ago
        )
    ).rdd.collect()

    metric_rows = [
        make_metric_row(
            title=pkg.Testcase,
            value="{} seconds".format(round(pkg.avg_time_secs)),
        )
        for pkg in toptenpackages
    ]

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["dev-exp-oncall"],
        key="testresultmetrics",
        title="Top 10 Test Package Times ({} to {})".format(week_ago, today),
        body=metric_rows,
        day="Tuesday",
    )
except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Release Management
try:
    import datetime as dt

    today = dt.date.today()
    week_ago = today - dt.timedelta(days=7)

    spark.read.format("bigquery").option(
        "table", "backend.TestMetrics"
    ).load().createOrReplaceTempView("testmetrics")

    toptenpackages = spark.sql(
        """
    select col.Testcase, avg(col.Elapsed) as avg_time_secs
    from (
        select explode(Tests)
        from testmetrics
        where GitBranch = "master" and StartTime > '{}'
      )
    where col.Result = "pass"
      and col.Testcase like "%releasemanagement%"
    group by col.Testcase
    order by avg_time_secs desc
    limit 10
  """.format(
            week_ago
        )
    ).rdd.collect()

    metric_rows = [
        make_metric_row(
            title=pkg.Testcase,
            value="{} seconds".format(round(pkg.avg_time_secs)),
        )
        for pkg in toptenpackages
    ]

    add_metrics(
        disable_real_slack_post_for_testing=False,
        slack_channels=["alerts-release-management"],
        key="releasemanagementtoptesttimes",
        title="Top 10 Test Package Times ({} to {})".format(week_ago, today),
        body=metric_rows,
        day="Tuesday",
    )
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md # Send It

# COMMAND ----------

#
# Display all the metrics inside the notebook
#

print_metrics()

# COMMAND ----------

#
# SEND TO REAL SLACK CHANNELS
#

# send_metrics(key_override="cst", slack_channel_override="device-services-metrics")
send_metrics()

# COMMAND ----------
