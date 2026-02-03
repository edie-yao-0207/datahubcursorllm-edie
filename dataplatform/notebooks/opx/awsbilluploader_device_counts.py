# Databricks notebook source
# MAGIC %md
# MAGIC ### awsbilluploader normalized device counts
# MAGIC
# MAGIC This notebook exports the daily device count metrics that are used alongside data from awsbilluploader to populate finops tableau dashboards, eg: https://10az.online.tableau.com/#/site/samsaradashboards/views/CostDashboard-CirpoCopy/CostDashboard
# MAGIC

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import datetime
import time

import boto3
from dateutil.relativedelta import relativedelta

EXPORT_BUCKET = "samsara-awsbilluploader"

session = boto3.session.Session()
s3 = get_s3_client("samsara-awsbilluploader-readwrite")

# get the first day of the month containing the given date.
def month_start(dt) -> datetime.date:
    return dt.replace(day=1)


def month_end(dt) -> datetime.date:
    return dt + relativedelta(day=31)


def write_csv(df, key):
    s3.put_object(
        Body=df.toPandas().to_csv().encode(),
        Bucket=EXPORT_BUCKET,
        Key=key,
        ACL="bucket-owner-full-control",
    )


# return first day of month previous to given date
def month_previous(dt: datetime.date) -> datetime.date:
    return (dt - relativedelta(months=1)).replace(day=1)


def write_device_counts_for_month(dt: datetime.date):
    start = month_start(dt)
    end = month_end(dt)
    res = spark.sql(
        """
          SELECT
            active_date,
            device_region,
            product_id,
            name,
            product_family,
            active_device_days
          FROM
            finops.cost_active_devices_daily_w_totals
          where
            active_date >= '{start}' AND active_date <= '{end}'
        """.format(
            start=start, end=end
        )
    )
    month = str(start.month).zfill(2)
    # The naming convention is defined in https://samsaradev.atlassian.net/browse/OPX-1276
    key = f"active_devices_daily/{start.year}/{month}/{int(time.time())}.csv"
    write_csv(res, key)


today = datetime.date.today()
write_device_counts_for_month(today)
write_device_counts_for_month(month_previous(today))


# COMMAND ----------
