# Databricks notebook source
# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %run /Shared/Shared/slack-tables

# COMMAND ----------

metrics_to_send = {}
channels_to_send = ["alerts-stce-fw-crux"]
workspace_name = str.split(spark.conf.get("spark.databricks.workspaceUrl"), ".")[0]
region_name = str.replace(workspace_name, "samsara-dev-", "")

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta, date
from delta.tables import *
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, ByteType
import struct


# COMMAND ----------

# All Hansel data for yesterday
df = spark.sql(
    f"""
    select c.*
from hardware_analytics.at11_data as c
join clouddb.organizations as o
on c.peripheral_org_id = o.id
where o.internal_type = 0
and c.date = current_date() - 1
and c.peripheral_serial in (select serial from hardware_analytics.at11_devices where build not in ('EVT'))
"""
).createOrReplaceTempView("hansel_yesterday_data")

# COMMAND ----------

# Corrupt data - boots ? 100k, and adv_internal_us <> 8
# TODO: adv_internal_us will have to be removed as newer devices will not have the same config
hansel_corrupt_data_sf = spark.sql(
    f"""   
select peripheral_device_id, peripheral_asset_id, peripheral_org_id, 
        scanner_object_id, scanner_product_id, scanner_org_id, count(*) as total_corrupt_packets
from hansel_yesterday_data as han
where protocol_version is null
and flags is null
and  reset_reason is null
and  tx_power_dbm is null
and  boot_count is null
and  temp_c is null
and  batt is null
and  fw_version is null
and  error_code is null
group by peripheral_device_id, peripheral_asset_id, peripheral_org_id, 
        scanner_org_id, scanner_product_id, scanner_object_id

"""
)
display(hansel_corrupt_data_sf)

hansel_corrupt_data = hansel_corrupt_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.scanner_object_id),
            str(row.scanner_product_id),
            str(row.scanner_org_id),
            str(row.total_corrupt_packets),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_corrupt_data)))
hansel_corrupt_data_table = make_table("", hansel_corrupt_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_corrupt_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_corrupt_data_table)
]
if len(hansel_corrupt_data) > 0:
    metrics_to_send["hansel_corrupt_adv"] = {
        "slack_text": f"*Unhashable advertisement packet in {region_name} sent by *\n"
        + "\n".join(hansel_corrupt_data_table),
    }


# COMMAND ----------

# Corrupt data - boots ? 100k, and adv_internal_us <> 8
# TODO: adv_internal_us will have to be removed as newer devices will not have the same config
hansel_corrupt_data_sf = spark.sql(
    f"""
(select peripheral_device_id, peripheral_asset_id, peripheral_org_id 
from (    
select peripheral_device_id, peripheral_asset_id, peripheral_org_id, max_by(boot_count, time) as curr_boot
from hansel_yesterday_data as han
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
having curr_boot > 100000))
union
(
select peripheral_device_id, peripheral_asset_id, peripheral_org_id
from hansel_yesterday_data as han
where adv_interval_us <> 8000000
group by peripheral_device_id, peripheral_asset_id, peripheral_org_id
)
"""
)
display(hansel_corrupt_data_sf)

hansel_corrupt_data = hansel_corrupt_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_corrupt_data)))
hansel_corrupt_data_table = make_table("", hansel_corrupt_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_corrupt_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_corrupt_data_table)
]
if len(hansel_corrupt_data) > 0:
    metrics_to_send["hansel_corrupt"] = {
        "slack_text": f"*Assets with possibly corrupt data in {region_name}*\n"
        + "\n".join(hansel_corrupt_data_table),
    }


# COMMAND ----------

# FW version > 65535 or <> 121123 for a fw cut on Dec 11, 2023
hansel_corrupt_data_fw_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(fw_version) as fw
from hansel_yesterday_data as han
where fw_version > 65535 and fw_version <> 121123
group by peripheral_device_id, peripheral_asset_id, peripheral_org_id"""
)
display(hansel_corrupt_data_fw_sf)

hansel_corrupt_data_fw = hansel_corrupt_data_fw_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.fw),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_corrupt_data_fw)))
hansel_corrupt_data_fw_table = make_table(
    "", hansel_corrupt_data_fw_sf.columns, row_data
)[1:]

# Split into 8 rows each
hansel_corrupt_data_fw_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_corrupt_data_fw_table)
]

if len(hansel_corrupt_data_fw) > 0:
    metrics_to_send["hansel_corrupt2"] = {
        "slack_text": f"*Assets with bad fw version in {region_name}*\n"
        + "\n".join(hansel_corrupt_data_fw_table),
    }


# COMMAND ----------

# Too many boots since last time
# TODO: Weird filter to handle old devices between 1 and 100k
hansel_boot_issues_sf = spark.sql(
    f"""select  peripheral_device_id, coalesce(x.peripheral_asset_id, y.peripheral_asset_id) as peripheral_asset_id, coalesce(x.peripheral_org_id, y.peripheral_org_id) as peripheral_org_id, curr_boot, last_boot, x.curr_boot - y.last_boot as boot_on_day, (curr_time - last_time)/3600000 as elapse_time_hrs
from
(select peripheral_device_id, peripheral_asset_id, peripheral_org_id, max(time) as curr_time, max_by(boot_count, time) as curr_boot
from hansel_yesterday_data as han
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id) as x
full outer join
(select peripheral_asset_id, peripheral_org_id, max(time) as last_time, max_by(boot_count, time) as last_boot
from hardware_analytics.at11_data as han
where date <= current_date() - 2
and peripheral_org_id <> 1
group by peripheral_asset_id, peripheral_org_id) as y
on x.peripheral_asset_id = y.peripheral_asset_id
where x.curr_boot - y.last_boot between 1 and 100000
order by boot_on_day desc"""
)
display(hansel_boot_issues_sf)

hansel_boot_issues = hansel_boot_issues_sf.limit(10).rdd.collect()

total_assets_with_reboot = hansel_boot_issues_sf.count()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.curr_boot),
            str(row.last_boot),
            str(row.boot_on_day),
            str(row.elapse_time_hrs),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_boot_issues)))
hansel_boot_issues_table = make_table("", hansel_boot_issues_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_boot_issues_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_boot_issues_table)
]

if len(hansel_boot_issues) > 0:
    metrics_to_send["hanse_boot"] = {
        "slack_text": f"*Assets with reboots > 0 from the last time they were heard in {region_name}*\n"
        + f"\n Total Assets that meet the criteria *{total_assets_with_reboot}*"
        + "\n".join(hansel_boot_issues_table),
    }


# COMMAND ----------

# Mean Battery < 30
# TODO: Use only unique advertisements - currently handled with collect_set
hansel_battery_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(batt) as batt_rep, mean(batt) as mean_batt
from hansel_yesterday_data as han
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
having mean_batt < 3
order by mean_batt desc"""
)
display(hansel_battery_data_sf)

hansel_battery_data = hansel_battery_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.batt_rep),
            str(row.mean_batt),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_battery_data)))
hansel_battery_data_table = make_table("", hansel_battery_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_battery_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_battery_data_table)
]
if len(hansel_battery_data) > 0:
    metrics_to_send["hansel_battery"] = {
        "slack_text": f"*Assets with low battery in {region_name}*\n"
        + "\n".join(hansel_battery_data_table),
    }


# COMMAND ----------

# Distinct reset reasons
# TODO: Only matter if bootcount is high
hansel_reset_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(reset_reason) as resets, count(distinct(reset_reason)) as rs_cnt
from hansel_yesterday_data as han
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
having rs_cnt > 1"""
)
display(hansel_reset_data_sf)

hansel_reset_data = hansel_reset_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.resets),
            str(row.rs_cnt),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_reset_data)))
hansel_reset_data_table = make_table("", hansel_reset_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_reset_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_reset_data_table)
]

if len(hansel_reset_data) > 0:
    metrics_to_send["hansel_resets"] = {
        "slack_text": f"*Assets with high resets in {region_name}*\n"
        + "\n".join(hansel_reset_data_table),
    }


# COMMAND ----------

# Distinct flags
hansel_flag_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(flags) as flags
from hansel_yesterday_data as han
where flags not in  (23, 0)
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
display(hansel_flag_data_sf)

hansel_flag_data = hansel_flag_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.flags),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_flag_data)))
hansel_flag_data_table = make_table("", hansel_flag_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_flag_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_flag_data_table)
]
if len(hansel_flag_data) > 0:
    metrics_to_send["hansel_flags"] = {
        "slack_text": f"*Assets with flags other than 23 or 0 in {region_name}*\n"
        + "\n".join(hansel_flag_data_table),
    }

# COMMAND ----------

# Reset Reasons
hansel_reset_reason_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(reset_reason) as reset_reasons
from (select peripheral_device_id, peripheral_asset_id, peripheral_org_id,
            case when reset_reason = 1 then 'PERIPHERAL_RESET_SHUTDOWN_IO'
                when reset_reason = 2 then 'PERIPHERAL_RESET_SHUTDOWN_SWD'
                when reset_reason = 3 then 'PERIPHERAL_RESET_WATCHDOG'
                when reset_reason = 5 then 'PERIPHERAL_RESET_LOCKUP'
                when reset_reason = 6 then 'PERIPHERAL_RESET_TSD'
                when reset_reason = 8 then 'PERIPHERAL_RESET_LFXT'
                when reset_reason = 9 then 'PERIPHERAL_RESET_VDDR'
                when reset_reason = 10 then 'PERIPHERAL_RESET_VDDS'
                when reset_reason = 12 then 'PERIPHERAL_RESET_POR'
                end as reset_reason
    from hansel_yesterday_data
where reset_reason in (1, 2, 3, 5, 6, 8, 9, 10, 12) ) as han
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
# display(hansel_reset_reason_data_sf)

hansel_reset_reason_data = hansel_reset_reason_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.reset_reasons),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_reset_reason_data)))
hansel_reset_reason_data_table = make_table(
    "", hansel_reset_reason_data_sf.columns, row_data
)[1:]

# Split into 8 rows each
hansel_reset_reason_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_reset_reason_data_table)
]

if len(hansel_reset_reason_data) > 0:
    metrics_to_send["hansel_reset_reasons"] = {
        "slack_text": f"*Assets with critical reset reasons in {region_name}*\n"
        + "\n".join(hansel_reset_reason_data_table),
    }

# COMMAND ----------

# Reset Reasons
hansel_error_code_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(error_code) as error_codes
    from hansel_yesterday_data as han
    where error_code <> 0
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
# display(hansel_error_code_data_sf)

hansel_error_code_data = hansel_error_code_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.error_codes),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_error_code_data)))
hansel_error_code_data_table = make_table(
    "", hansel_error_code_data_sf.columns, row_data
)[1:]

# Split into 8 rows each
hansel_error_code_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_error_code_data_table)
]

if len(hansel_error_code_data) > 0:
    metrics_to_send["hansel_error_codes"] = {
        "slack_text": f"*Assets with error code <> 0 in {region_name}*\n"
        + "\n".join(hansel_error_code_data_table),
    }


# COMMAND ----------

# FEM power
if region_name == "us-west-2":
    acceptable_tx_power = "(30, 20, 21)"
else:
    acceptable_tx_power = "(30, 10, 13)"

hansel_tx_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, collect_set(tx_power_dbm) as tx_power_dbm
from hansel_yesterday_data as han
where tx_power_dbm not in {acceptable_tx_power}
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
display(hansel_tx_data_sf)

hansel_tx_data = hansel_tx_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.tx_power_dbm),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_tx_data)))
hansel_tx_data_table = make_table("", hansel_tx_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_tx_data_table = [
    x + "```\n```" if (i % 15 == 14) else x for i, x in enumerate(hansel_tx_data_table)
]

if len(hansel_tx_data) > 0:
    metrics_to_send["hansel_tx_power"] = {
        "slack_text": f"*Assets with tx_power other than {acceptable_tx_power} in {region_name}*\n"
        + "\n".join(hansel_tx_data_table),
    }


# COMMAND ----------

# 0.5%ile of low temperature
hansel_temp_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, min(temp_c) as min_temp
from hansel_yesterday_data as han
join (select percentile_approx(temp_c, 0.005) as p005
    from hansel_yesterday_data as h1) as p
where temp_c < p005
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
display(hansel_temp_data_sf)

hansel_temp_data = hansel_temp_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.min_temp),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_temp_data)))
hansel_temp_data_table = make_table("", hansel_temp_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_temp_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_temp_data_table)
]

if len(hansel_temp_data) > 0:
    metrics_to_send["hansel_temp_min"] = {
        "slack_text": f"*Assets with very low temperatures in {region_name}*\n"
        + "\n".join(hansel_temp_data_table),
    }


# COMMAND ----------

hansel_temp_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, max(temp_c) as max_temp
from hansel_yesterday_data as han
join (select percentile_approx(temp_c, 0.995) as p995
    from hansel_yesterday_data as h1) as p
where temp_c > p995
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
"""
)
display(hansel_temp_data_sf)

hansel_temp_data = hansel_temp_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.max_temp),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_temp_data)))
hansel_temp_data_table = make_table("", hansel_temp_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_temp_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_temp_data_table)
]

if len(hansel_temp_data) > 0:
    metrics_to_send["hansel_temp_max"] = {
        "slack_text": f"*Assets with very high temperatures in {region_name}*\n"
        + "\n".join(hansel_temp_data_table),
    }

# COMMAND ----------

# Assets with highest scans on day
hansel_scan_data_sf = spark.sql(
    f"""select peripheral_device_id, peripheral_asset_id, peripheral_org_id, count(*) as total_scans, sum(same_org) as total_scan_same_org, round(sum(same_org)*100/count(*),2) as perc_scan_same_org
from (select peripheral_device_id, peripheral_asset_id, peripheral_org_id, case when peripheral_org_id = scanner_org_id then 1 else 0 end as same_org
    from hansel_yesterday_data as han)
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
order by total_scans desc
limit 10
"""
)
display(hansel_scan_data_sf)

hansel_scan_data = hansel_scan_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.total_scans),
            str(row.total_scan_same_org),
            str(row.perc_scan_same_org),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_scan_data)))
hansel_scan_data_table = make_table("", hansel_scan_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_scan_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_scan_data_table)
]

if len(hansel_scan_data) > 0:
    metrics_to_send["hansel_scan_max"] = {
        "slack_text": f"*Assets with highest scans on day in {region_name}*\n"
        + "\n".join(hansel_scan_data_table),
    }


# COMMAND ----------

hansel_scan_data_sf = spark.sql(
    f"""select peripheral_asset_id, peripheral_org_id, peripheral_device_id, count(*) as total_scans, sum(same_org) as total_scan_same_org, round(sum(same_org)*100/count(*),2) as perc_scan_same_org
from (select peripheral_device_id, peripheral_asset_id, peripheral_org_id, case when peripheral_org_id = scanner_org_id then 1 else 0 end as same_org
    from hansel_yesterday_data as han)
group by peripheral_asset_id, peripheral_org_id, peripheral_device_id
order by total_scans asc
limit 10
"""
)
display(hansel_scan_data_sf)

hansel_scan_data = hansel_scan_data_sf.rdd.collect()


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.peripheral_device_id),
                f"https://cloud.samsara.com/devices/{row.peripheral_device_id}/show",
            ),
            str(row.peripheral_asset_id),
            str(row.peripheral_org_id),
            str(row.total_scans),
            str(row.total_scan_same_org),
            str(row.perc_scan_same_org),
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), hansel_scan_data)))
hansel_scan_data_table = make_table("", hansel_scan_data_sf.columns, row_data)[1:]

# Split into 8 rows each
hansel_scan_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(hansel_scan_data_table)
]
if len(hansel_scan_data) > 0:
    metrics_to_send["hansel_scan_min"] = {
        "slack_text": f"*Assets with lowest scans on day in {region_name}*\n"
        + "\n".join(hansel_scan_data_table),
    }

# COMMAND ----------

# Not a clean solution
for (key, metric) in metrics_to_send.items():
    if len(metric["slack_text"]) <= 4046:
        send_slack_message_to_channels(metric["slack_text"], channels_to_send)
    else:
        send_split_message = metric["slack_text"].split("```\n```")
        send_slack_message_to_channels(send_split_message[0] + "```", channels_to_send)
        for msg in send_split_message[1:-1]:
            send_slack_message_to_channels("```" + msg + "```", channels_to_send)
        send_slack_message_to_channels("```" + send_split_message[-1], channels_to_send)
