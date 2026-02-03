# Databricks notebook source
# Sends messaes to #marathon-device-outliers channel on slack
# Find devices with frequent cable voltage changes
# Find trips with 2 or more gaps of > 300s and > 8km gps distances - uses osdnordicgpsdebug table

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

# MAGIC %run /Shared/Shared/slack-tables

# COMMAND ----------

from datetime import date, datetime, time, timedelta

metrics_date = date.today() - timedelta(days=1)
metrics_to_send = {}
channels_to_send = ["marathon-device-outliers"]

# COMMAND ----------

# Devices that have serac cable voltage issues

serac_cable_issues_sf = spark.sql(
    f"""with z as (
                                        select object_id, time, date, org_id,
                                               coalesce(array_max(value.proto_value.auxiliary_voltages.aux_mv),0) as voltage,
                                               lag(coalesce(array_max(value.proto_value.auxiliary_voltages.aux_mv),0)) over(partition by object_id order by time asc) as prev_voltage
                                               from kinesisstats.osdauxiliaryvoltages
                                               where value.is_end is false and value.is_databreak is false and date = '{str(metrics_date)}'
                                         ),

                                         x as (
                                         select object_id, org_id, date, round(time/3600000, 0) time_hr,
                                                voltage, prev_voltage 
                                         from z 
                                         where (prev_voltage >= 8000 and voltage< 6000) or  (prev_voltage < 6000 and voltage >= 8000)
                                         )

                                    select t.object_id, t.org_id, o.name as org_name
                                    from(select time_hr, org_id, object_id, count(*) as total_changes
                                          from x
                                          group by time_hr, object_id, org_id
                                          having total_changes > 20) as t
                                    left join clouddb.organizations as o
                                    on o.id = t.org_id
                                    where o.id not in (select org_id from internaldb.simulated_orgs)
                                    group by object_id, org_id, name"""
)

serac_cable_issues = serac_cable_issues_sf.rdd.collect()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f'https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}&device-show-graphs=%5B"nrfZephyrCableVoltages"%5D',
            ),
            str(row.org_id),
            row.org_name,
        ]

    return format_row


row_data = []
row_data.extend(list(map(create_formatter(), serac_cable_issues)))
serac_cable_issues_table = make_table("", serac_cable_issues_sf.columns, row_data)[1:]

# Split into 8 rows each
serac_cable_issues_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(serac_cable_issues_table)
]

metrics_to_send["serac_voltages"] = {
    "slack_text": "*Serac Devices that have intermittent cable voltages*\n"
    + "\n".join(serac_cable_issues_table),
}

# COMMAND ----------

# Devices that have 8km and 5 min trip gaps

# serac_trips_with_gaps_sf = spark.sql(
#     f"""with trips_data as (
#                                     select *
#                                     from hardware_dev.meenu_ag52_trips_table
#                                     where date == '{str(metrics_date)}'
#                                     ),

#                                 gps as (
#                                     select gpd.*
#                                     from kinesisstats.osdnordicgpsdebug as gpd
#                                     where date = '{str(metrics_date)}' and gpd.org_id <> 1 and
#                                           (value.is_start is true or (value.is_end is false and value.is_databreak is false)) and
#                                           value.proto_value.nordic_gps_debug.reported_to_backend[0] is true
#                                     ),

#                                 -- final powered and trips data
#                                 powered as (
#                                     select gps.object_id, gps.org_id, gps.date, gps.value.time,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0]/1000000000 as lat,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.longitude_nd[0]/1000000000 as lon,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.time_to_fix_ms[0] as ttff,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.execution_time_ms[0] as exec_time,
#                                            value.proto_value.nordic_gps_debug.gps_scan_duration_ms[0] as gps_scan_time,
#                                            b.start_time,
#                                            b.end_time,
#                                            b.duration_s,
#                                            b.trip_rank,
#                                            b.org_name
#                                     from gps
#                                     inner join trips_data as b
#                                     on b.object_id = gps.object_id
#                                     where gps.time between b.start_time and b.end_time),

#                                 gps_all_data(select *,
#                                                     lag(lat) over(partition by object_id, trip_rank order by time asc) as lag_lat,
#                                                     lag(lon) over(partition by object_id, trip_rank order by time asc) as lag_lon,
#                                                     lag(time) over(partition by object_id, trip_rank order by time asc) as lag_time
#                                               from powered)

#                                 select met_data.*, trip_data.total_time_on_trip_hr, trip_data.total_trips
#                                 from (  select object_id, org_id, org_name, start_time, end_time,
#                                                cast(round(duration_s,0) as int) as trip_duration_s,
#                                                count(1) as gap_count
#                                         from (select *, del_time/hav_m as metric from (select *,
#                                                      (time - lag_time)/1000 as del_time,
#                                                      2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) +
#                                                      cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
#                                               from gps_all_data))
#                                         where del_time > 300 and hav_m > 8000
#                                         group by org_id, org_name, object_id, trip_rank, start_time, end_time, duration_s
#                                         having gap_count >= 10) as met_data
#                                  left join (select object_id, org_name, round(sum(duration_s)/3600,2) as total_time_on_trip_hr, count(distinct(trip_rank)) as total_trips
#                                             from trips_data
#                                             group by object_id, org_name) as trip_data
#                                  on met_data.object_id = trip_data.object_id and
#                                     met_data.org_name = trip_data.org_name
#                                  order by gap_count desc"""
# )

# serac_trips_with_gaps = serac_trips_with_gaps_sf.rdd.collect()


# def create_formatter():
#     def format_row(row):
#         return [
#             Link(
#                 str(row.object_id),
#                 f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={row.end_time}&duration={row.trip_duration_s}&device-show-graphs",
#             ),
#             str(row.org_id),
#             row.org_name,
#             str(row.start_time),
#             str(row.end_time),
#             str(row.trip_duration_s),
#             str(row.gap_count),
#             str(row.total_time_on_trip_hr),
#             str(row.total_trips),
#         ]

#     return format_row


# row_data = []
# row_data.extend(list(map(create_formatter(), serac_trips_with_gaps)))
# serac_trips_with_gaps_table = make_table(
#     "", serac_trips_with_gaps_sf.columns, row_data
# )[1:]

# # Split into 8 rows each
# serac_trips_with_gaps_table = [
#     x + "```\n```" if (i % 8 == 7) else x
#     for i, x in enumerate(serac_trips_with_gaps_table)
# ]

# metrics_to_send["serac_trip_gaps"] = {
#     "slack_text": "*Serac device trips that have at least 10 gaps > 8 km and > 5 mins*\n"
#     + "\n".join(serac_trips_with_gaps_table),
# }

# # COMMAND ----------

# # Devices that have 35km trip gaps

# serac_trips_with_gaps_sf = spark.sql(
#     f"""with trips_data as (
#                                     select *
#                                     from playground.meenu_ag52_trips_table
#                                     where date == '{str(metrics_date)}'
#                                     ),

#                                 gps as (
#                                     select gpd.*
#                                     from kinesisstats.osdnordicgpsdebug as gpd
#                                     where date = '{str(metrics_date)}' and gpd.org_id <> 1 and
#                                           (value.is_start is true or (value.is_end is false and value.is_databreak is false)) and
#                                           value.proto_value.nordic_gps_debug.reported_to_backend[0] is true
#                                     ),

#                                 -- final powered and trips data
#                                 powered as (
#                                     select gps.object_id, gps.org_id, gps.date, gps.value.time,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0]/1000000000 as lat,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.longitude_nd[0]/1000000000 as lon,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.time_to_fix_ms[0] as ttff,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.execution_time_ms[0] as exec_time,
#                                            value.proto_value.nordic_gps_debug.gps_scan_duration_ms[0] as gps_scan_time,
#                                            b.start_time,
#                                            b.end_time,
#                                            b.duration_s,
#                                            b.trip_rank,
#                                            b.org_name, b.avg_speed
#                                     from gps
#                                     inner join trips_data as b
#                                     on b.object_id = gps.object_id
#                                     where gps.time between b.start_time and b.end_time),

#                                 gps_all_data(select *,
#                                                     lag(lat) over(partition by object_id, trip_rank order by time asc) as lag_lat,
#                                                     lag(lon) over(partition by object_id, trip_rank order by time asc) as lag_lon,
#                                                     lag(time) over(partition by object_id, trip_rank order by time asc) as lag_time
#                                               from powered)

#                                 select met_data.*, trip_data.total_time_on_trip_hr, trip_data.total_trips
#                                 from (  select object_id, org_id, org_name, start_time, end_time,
#                                                cast(round(duration_s,0) as int) as trip_duration_s,
#                                                count(1) as gap_count
#                                         from (select *, del_time/hav_m as metric from (select *,
#                                                      (time - lag_time)/1000 as del_time,
#                                                      2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) +
#                                                      cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
#                                               from gps_all_data))
#                                         where hav_m > 35000
#                                         group by org_id, org_name, object_id, trip_rank, start_time, end_time, duration_s
#                                         having gap_count > 0) as met_data
#                                  left join (select object_id, org_name, round(sum(duration_s)/3600,2) as total_time_on_trip_hr, count(distinct(trip_rank)) as total_trips
#                                             from trips_data
#                                             group by object_id, org_name) as trip_data
#                                  on met_data.object_id = trip_data.object_id and
#                                     met_data.org_name = trip_data.org_name
#                                  order by gap_count desc"""
# )

# serac_trips_with_gaps = serac_trips_with_gaps_sf.rdd.collect()


# def create_formatter():
#     def format_row(row):
#         return [
#             Link(
#                 str(row.object_id),
#                 f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={row.end_time}&duration={row.trip_duration_s}&device-show-graphs",
#             ),
#             str(row.org_id),
#             row.org_name,
#             str(row.start_time),
#             str(row.end_time),
#             str(row.trip_duration_s),
#             str(row.gap_count),
#             str(row.total_time_on_trip_hr),
#             str(row.total_trips),
#         ]

#     return format_row


# row_data = []
# row_data.extend(list(map(create_formatter(), serac_trips_with_gaps)))
# serac_trips_with_gaps_table = make_table(
#     "", serac_trips_with_gaps_sf.columns, row_data
# )[1:]

# # Split into 8 rows each
# serac_trips_with_gaps_table = [
#     x + "```\n```" if (i % 8 == 7) else x
#     for i, x in enumerate(serac_trips_with_gaps_table)
# ]

# metrics_to_send["serac_trip_gaps_35k"] = {
#     "slack_text": "*Serac device trips that have gaps > 35km*\n"
#     + "\n".join(serac_trips_with_gaps_table),
# }

# COMMAND ----------

# # Devices that have trip gaps

# serac_trips_with_gaps_sf = spark.sql(
#     f"""with trips_data as (
#                                     select *
#                                     from playground.meenu_ag52_trips_table
#                                     where date == '{str(metrics_date)}'
#                                     ),

#                                 gps as (
#                                     select gpd.*
#                                     from kinesisstats.osdnordicgpsdebug as gpd
#                                     where date = '{str(metrics_date)}' and gpd.org_id <> 1 and
#                                           (value.is_start is true or (value.is_end is false and value.is_databreak is false)) and
#                                           value.proto_value.nordic_gps_debug.reported_to_backend[0] is true
#                                     ),

#                                 -- final powered and trips data
#                                 powered as (
#                                     select gps.object_id, gps.org_id, gps.date, gps.value.time,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.latitude_nd[0]/1000000000 as lat,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.longitude_nd[0]/1000000000 as lon,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.time_to_fix_ms[0] as ttff,
#                                            value.proto_value.nordic_gps_debug.gps_fix_info.execution_time_ms[0] as exec_time,
#                                            value.proto_value.nordic_gps_debug.gps_scan_duration_ms[0] as gps_scan_time,
#                                            b.start_time,
#                                            b.end_time,
#                                            b.duration_s,
#                                            b.trip_rank,
#                                            b.org_name
#                                     from gps
#                                     inner join trips_data as b
#                                     on b.object_id = gps.object_id
#                                     where gps.time between b.start_time and b.end_time),

#                                 gps_all_data(select *,
#                                                     lag(lat) over(partition by object_id, trip_rank order by time asc) as lag_lat,
#                                                     lag(lon) over(partition by object_id, trip_rank order by time asc) as lag_lon,
#                                                     lag(time) over(partition by object_id, trip_rank order by time asc) as lag_time
#                                               from powered)

#                                 select met_data.*, trip_data.total_time_on_trip_hr, trip_data.total_trips
#                                 from (  select object_id, org_id, org_name, start_time, end_time,
#                                                cast(round(duration_s,0) as int) as trip_duration_s,
#                                                count(1) as gap_count
#                                         from (select *, hav_m/del_time as metric from (select *,
#                                                      (time - lag_time)/1000 as del_time,
#                                                      2 * 6371 * 1000 * asin(sqrt((power(sin(radians((lat - lag_lat)/2)), 2) +
#                                                      cos(radians(lag_lat)) * cos(radians(lat)) * power(sin(radians((lon - lag_lon)/2)), 2) ))) as hav_m
#                                               from gps_all_data))
#                                         where metric > 40
#                                         group by org_id, org_name, object_id, trip_rank, start_time, end_time, duration_s
#                                         having gap_count > 0) as met_data
#                                  left join (select object_id, org_name, round(sum(duration_s)/3600,2) as total_time_on_trip_hr, count(distinct(trip_rank)) as total_trips
#                                             from trips_data
#                                             group by object_id, org_name) as trip_data
#                                  on met_data.object_id = trip_data.object_id and
#                                     met_data.org_name = trip_data.org_name
#                                  order by gap_count desc"""
# )

# serac_trips_with_gaps = serac_trips_with_gaps_sf.rdd.collect()


# def create_formatter():
#     def format_row(row):
#         return [
#             Link(
#                 str(row.object_id),
#                 f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={row.end_time}&duration={row.trip_duration_s}&device-show-graphs",
#             ),
#             str(row.org_id),
#             row.org_name,
#             str(row.start_time),
#             str(row.end_time),
#             str(row.trip_duration_s),
#             str(row.gap_count),
#             str(row.total_time_on_trip_hr),
#             str(row.total_trips),
#         ]

#     return format_row


# row_data = []
# row_data.extend(list(map(create_formatter(), serac_trips_with_gaps)))
# serac_trips_with_gaps_table = make_table(
#     "", serac_trips_with_gaps_sf.columns, row_data
# )[1:]

# # Split into 8 rows each
# serac_trips_with_gaps_table = [
#     x + "```\n```" if (i % 8 == 7) else x
#     for i, x in enumerate(serac_trips_with_gaps_table)
# ]

# metrics_to_send["serac_trip_high_speed"] = {
#     "slack_text": "*Serac device trips that > 90mph consecutive fixes*\n"
#     + "\n".join(serac_trips_with_gaps_table),
# }

# COMMAND ----------

# Total Trips and Trip durations

# serac_total_trips_sf = spark.sql(
#     f"""
#                                 select round(sum(duration_s)/3600,2) as total_duration_hr, count(1) as total_trips
#                                 from playground.meenu_ag52_trips_table
#                                 where date == '{str(metrics_date)}'
#                                 """
# )

# serac_total_trips = serac_total_trips_sf.rdd.collect()


# metrics_to_send["serac_total_trips"] = {
#     "slack_text": f"""Total Trips by Serac devices: {serac_total_trips[0]['total_trips']}""",
# }
# metrics_to_send["serac_total_trip_duration"] = {
#     "slack_text": f"""Total Trip time by Serac devices: {serac_total_trips[0]['total_duration_hr']} hrs""",
# }

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

# COMMAND ----------

metrics_to_send = {}

# COMMAND ----------

# Devices that have high reboots

reboots_sf = spark.sql(
    f""" select db.org_id, db.device_id as object_id, db.latest_gateway_id as gateway_id, g.serial, d.product_id, db.latest_build_on_day, db.boot_counts_on_day, fwd.build
        from dataprep.device_builds as db
        join productsdb.devices as d
        on db.device_id = d.id
        join clouddb.organizations as o
        on db.org_id = o.id
        join productsdb.gateways as g
        on db.latest_gateway_id = g.id
        left join (select build from firmwaredb.product_firmwares where product_id in (124,125) group by build) as fwd
        on db.latest_build_on_day = fwd.build
        where date = current_date()-1
        and o.id not in (select org_id from internaldb.simulated_orgs)
        and d.product_id in (124, 125)
        and o.internal_type == 0 
        order by db.boot_counts_on_day desc
    """
)

crevasse_reboots = (
    reboots_sf.filter("product_id = 124 and boot_counts_on_day >= 2")
    .select("org_id", "object_id", "gateway_id", "serial", "boot_counts_on_day")
    .limit(10)
    .rdd.collect()
)
serac_reboots = (
    reboots_sf.filter("product_id = 125 and boot_counts_on_day >= 5")
    .select("org_id", "object_id", "gateway_id", "serial", "boot_counts_on_day")
    .limit(10)
    .rdd.collect()
)

total_crevasse_reboots_devices = reboots_sf.filter(
    "product_id = 124 and boot_counts_on_day >= 2"
).count()
total_seracs_reboots_devices = reboots_sf.filter(
    "product_id = 125 and boot_counts_on_day >= 5"
).count()


crevasse_partial_build = (
    reboots_sf.filter("product_id = 124 and build is null")
    .select("org_id", "object_id", "gateway_id", "serial", "latest_build_on_day")
    .limit(10)
    .rdd.collect()
)
serac_partial_build = (
    reboots_sf.filter("product_id = 125 and build is null")
    .select("org_id", "object_id", "gateway_id", "serial", "latest_build_on_day")
    .limit(10)
    .rdd.collect()
)

total_crevasse_partial_build_devices = reboots_sf.filter(
    "product_id = 124 and build is null"
).count()
total_seracs_partial_build_devices = reboots_sf.filter(
    "product_id = 125 and build is null"
).count()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
            str(row.boot_counts_on_day),
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial", "reboots_on_day"]
row_data = []
row_data.extend(list(map(create_formatter(), crevasse_reboots)))
crevasse_reboots_table = make_table("", column_names, row_data)[1:]
row_data = []
row_data.extend(list(map(create_formatter(), serac_reboots)))
serac_reboots_table = make_table("", column_names, row_data)[1:]


# Split into 8 rows each
crevasse_reboots_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(crevasse_reboots_table)
]
serac_reboots_table = [
    x + "```\n```" if (i % 15 == 14) else x for i, x in enumerate(serac_reboots_table)
]

metrics_to_send["crevasse_reboots"] = {
    "slack_text": f"*Crevasse Devices that have rebooted >= 2 on {str(metrics_date)} *\n"
    + f"\n Total Devices that meet the criteria *{total_crevasse_reboots_devices}*"
    + "\n".join(crevasse_reboots_table),
}

metrics_to_send["serac_reboots"] = {
    "slack_text": f"*Serac Devices that have rebooted >= 5 on {str(metrics_date)} *\n"
    + f"\n Total Devices that meet the criteria *{total_seracs_reboots_devices}*"
    + "\n".join(serac_reboots_table),
}


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
            str(row.latest_build_on_day),
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial", "latest_build_on_day"]
row_data = []
row_data.extend(list(map(create_formatter(), crevasse_partial_build)))
crevasse_partial_build_table = make_table("", column_names, row_data)[1:]
row_data = []
row_data.extend(list(map(create_formatter(), serac_partial_build)))
serac_partial_build_table = make_table("", column_names, row_data)[1:]


# Split into 8 rows each
crevasse_partial_build_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(crevasse_partial_build_table)
]
serac_partial_build_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(serac_partial_build_table)
]

# metrics_to_send["crevasse_partial_build"] = {
#     "slack_text": f"*Crevasse Devices that were on partial build strings on {str(metrics_date)} *\n"
#     + f"\n Total Devices that meet the criteria *{total_crevasse_partial_build_devices}*"
#     + "\n".join(crevasse_partial_build_table),
# }

# metrics_to_send["serac_partial_build"] = {
#     "slack_text": f"*Serac Devices that were on partial build strings >= 5 on {str(metrics_date)} *\n"
#     + f"\n Total Devices that meet the criteria *{total_seracs_partial_build_devices}*"
#     + "\n".join(serac_partial_build_table),
# }


# COMMAND ----------


# Crevasse that have low fix success rate

# failed_fixes_sf = spark.sql(
#     f""" select a.object_id, a.org_id, g.id as gateway_id, a.serial, a.failed_fixes, a.total_attempts, a.fix_success
#     from (select object_id, org_id, serial, sum(no_fix) as failed_fixes, count(*) as total_attempts, (1-(sum(no_fix)/count(*)))*100 as fix_success
#         from (select gps.object_id, gps.org_id, d.serial, case when array_contains(gps.value.proto_value.nordic_gps_debug.no_fix, True) then 1 else 0 end as no_fix
#             from (select *
#                 from kinesisstats.osdnordicgpsdebug
#                 where date = '{str(metrics_date)}') as gps
#             join (select *
#                     from productsdb.devices
#                     where product_id = 124) as d
#             on d.id = gps.object_id)
#         group by object_id, org_id, serial
#         having fix_success < 70) as a
#     join productsdb.gateways as g
#     on a.serial = g.serial
#     join clouddb.organizations as o
#     on o.id = a.org_id
#     where o.internal_type=0
#     order by failed_fixes desc
#     """
# )

# crevasse_failed_fixes = failed_fixes_sf.limit(50).rdd.collect()
# total_crevasse_failed_fixes_devices = failed_fixes_sf.count()

# max_datetime_yesterday = int(
#     datetime.combine(metrics_date, time.max).timestamp() * 1000
# )


# def create_formatter():
#     def format_row(row):
#         return [
#             Link(
#                 str(row.object_id),
#                 f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
#             ),
#             str(row.org_id),
#             str(row.gateway_id),
#             row.serial,
#             str(row.failed_fixes),
#             str(row.total_attempts),
#             str(row.fix_success),
#         ]

#     return format_row


# column_names = failed_fixes_sf.columns
# row_data = []
# row_data.extend(list(map(create_formatter(), crevasse_failed_fixes)))
# crevasse_failed_fixes_table = make_table("", column_names, row_data)[1:]

# # Split into 8 rows each
# crevasse_failed_fixes_table = [
#     x + "```\n```" if (i % 15 == 14) else x
#     for i, x in enumerate(crevasse_failed_fixes_table)
# ]

# metrics_to_send["crevasse_failed_fixes"] = {
#     "slack_text": f"*Crevasse Devices with % successful fix < 70% on {str(metrics_date)} *\n"
#     + f"\n Total Devices that meet the criteria *{total_crevasse_failed_fixes_devices}*"
#     + "\n".join(crevasse_failed_fixes_table),
# }


# COMMAND ----------

# Crevasse that have more than 12 heartbeats

high_heartbeat_sf = spark.sql(
    f"""select a.*, g.id as gateway_id
        from (select h.object_id, h.org_id, d.serial, count(*) as heartbeats_on_day 
            from (select *
                from kinesisstats.osdhubserverdeviceheartbeat
                where date = '{str(metrics_date)}') as h
            join (select * 
                    from productsdb.devices 
                    where product_id = 124) as d
            on d.id = h.object_id
            group by h.object_id, h.org_id, d.serial) as a
    join productsdb.gateways as g
    on a.serial = g.serial
    join clouddb.organizations as o
    on o.id = a.org_id
    where o.internal_type=0
    and o.id not in (select org_id from internaldb.simulated_orgs)
    and heartbeats_on_day >= 12
    order by heartbeats_on_day desc
    """
)

crevasse_high_heartbeat = high_heartbeat_sf.limit(10).rdd.collect()
total_crevasse_high_heartbeat_devices = high_heartbeat_sf.count()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
            str(row.heartbeats_on_day),
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial", "heartbeats_on_day"]
row_data = []
row_data.extend(list(map(create_formatter(), crevasse_high_heartbeat)))
crevasse_high_heartbeat_table = make_table("", column_names, row_data)[1:]

# Split into 8 rows each
crevasse_high_heartbeat_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(crevasse_high_heartbeat_table)
]

metrics_to_send["crevasse_high_heartbeat"] = {
    "slack_text": f"*Crevasse Devices that have heartbeats >= 12 on {str(metrics_date)} *\n"
    + f"\n Total Devices that meet the criteria *{total_crevasse_high_heartbeat_devices}*"
    + "\n".join(crevasse_high_heartbeat_table),
}

# COMMAND ----------

# Serac that have more 2% battery decrease in 24 hrs

high_battery_drain_sf = spark.sql(
    f"""select z.object_id, z.org_id, g.id as gateway_id, g.serial as serial, max_by(soc_diff, time_diff) as soc_diff
        from (select x.object_id, x.org_id, (x.time-y.time)/3600000 as time_diff, y.soc - x.soc as soc_diff
            from (select date, max(time) as time, b.object_id, b.org_id, max_by(value.proto_value.battery_info.fuel_gauge.soc, time) as soc
                    from kinesisstats.osdbatteryinfo as b
                    join productsdb.devices as d
                    on d.id = b.object_id
                    where d.product_id = 125
                    and b.date = '{str(metrics_date)}'
                    group by b.date, b.object_id, b.org_id) as x
            join (select date, time, b.object_id, b.org_id, value.proto_value.battery_info.fuel_gauge.soc
                    from kinesisstats.osdbatteryinfo as b
                    join productsdb.devices as d
                    on d.id = b.object_id
                    where d.product_id = 125
                    and b.date = date_sub('{str(metrics_date)}', 1)) as y
            on x.object_id = y.object_id
            and x.org_id = y.org_id
            where (x.time-y.time)/3600000 between 20 and 25) as z
        join clouddb.organizations as o
        on o.id = z.org_id
        join productsdb.gateways as g
        on z.object_id = g.device_id
        where o.internal_type = 0
        and o.id not in (select org_id from internaldb.simulated_orgs)
        group by z.object_id, z.org_id, g.id, g.serial
        having soc_diff > 2
        order by soc_diff desc
    """
)

serac_high_battery_drain = high_battery_drain_sf.limit(10).rdd.collect()
total_serac_high_battery_drain_devices = high_battery_drain_sf.count()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
            str(row.soc_diff),
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial", "Charge decrease"]
row_data = []
row_data.extend(list(map(create_formatter(), serac_high_battery_drain)))
serac_high_battery_drain_table = make_table("", column_names, row_data)[1:]

# Split into 8 rows each
serac_high_battery_drain_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(serac_high_battery_drain_table)
]

metrics_to_send["serac_high_battery_drain"] = {
    "slack_text": f"*Serac Devices that have battery charge decrease >2% in the last 24 hrs on {str(metrics_date)} *\n"
    + f"\n Total Devices that meet the criteria *{total_serac_high_battery_drain_devices}*"
    + "\n".join(serac_high_battery_drain_table),
}


# COMMAND ----------

# Crevasse that have heartbeat but no fixes

# heartbeat_no_fix_sf = spark.sql(
#     f"""select devs.*, g.id as gateway_id
#         from (select h.object_id, h.org_id, d.serial
#             from (select *
#                 from kinesisstats.osdhubserverdeviceheartbeat
#                 where date = '{str(metrics_date)}') as h
#             join (select *
#                     from productsdb.devices
#                     where product_id = 124) as d
#             on d.id = h.object_id
#             group by h.object_id, h.org_id, d.serial) as devs
#         left join (select object_id, org_id, sum(no_fix)/count(*) as fix_fail
#                   from (select object_id, org_id, case when array_contains(value.proto_value.nordic_gps_debug.no_fix, True) then 1 else 0 end as no_fix
#                     from kinesisstats.osdnordicgpsdebug
#                     where date > date_sub('{str(metrics_date)}', 4)
#                     and date <= '{str(metrics_date)}')
#                     group by object_id, org_id
#                     having fix_fail < 1 ) as gps
#         on devs.object_id = gps.object_id
#         and devs.org_id = gps.org_id
#     join productsdb.gateways as g
#     on devs.serial = g.serial
#     join clouddb.organizations as o
#     on o.id = devs.org_id
#     where o.internal_type=0
#     and gps.object_id is null
#     """
# )

# crevasse_heartbeat_no_fix = heartbeat_no_fix_sf.limit(50).rdd.collect()
# total_crevasse_heartbeat_no_fix_devices = heartbeat_no_fix_sf.count()

# max_datetime_yesterday = int(
#     datetime.combine(metrics_date, time.max).timestamp() * 1000
# )


# def create_formatter():
#     def format_row(row):
#         return [
#             Link(
#                 str(row.object_id),
#                 f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
#             ),
#             str(row.org_id),
#             str(row.gateway_id),
#             row.serial,
#         ]

#     return format_row


# column_names = ["object_id", "org_id", "gateway_id", "serial"]
# row_data = []
# row_data.extend(list(map(create_formatter(), crevasse_heartbeat_no_fix)))
# crevasse_heartbeat_no_fix_table = make_table("", column_names, row_data)[1:]

# # Split into 8 rows each
# crevasse_heartbeat_no_fix_table = [
#     x + "```\n```" if (i % 15 == 14) else x
#     for i, x in enumerate(crevasse_heartbeat_no_fix_table)
# ]

# metrics_to_send["crevasse_heartbeat_no_fix"] = {
#     "slack_text": f"*Crevasse Devices that have heartbeat on {str(metrics_date)} but no fixes for the last 3 days*\n"
#     + f"\n Total Devices that meet the criteria *{total_crevasse_heartbeat_no_fix_devices}*"
#     + "\n".join(crevasse_heartbeat_no_fix_table),
# }

# COMMAND ----------

# Crevasse that have no heartbeat in the last 24 hrs.

no_heartbeat_24_sf = spark.sql(
    f"""with all_data as (
            select h.*, g.serial
            from (select date, object_id, org_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat
                where date > date_sub('{str(metrics_date)}', 2)) as h
            join productsdb.gateways as g
            on g.id = h.gateway_id
            join clouddb.organizations as o
            on h.org_id = o.id
            where g.product_id = 124
            and o.internal_type == 0
            and o.id not in (select org_id from internaldb.simulated_orgs)
            group by h.date, h.object_id, h.org_id, h.gateway_id, g.serial)

        select x.*, gh.first_heartbeat_date
        from (select *
            from all_data
            where date < '{str(metrics_date)}') as x
        left join (select *
            from all_data
            where date == '{str(metrics_date)}') as y
        on x.object_id = y.object_id
        and x.org_id = y.org_id
        and x.gateway_id = y.gateway_id
        left join hardware.gateways_heartbeat as gh
        on x.object_id = gh.object_id
        and x.org_id = gh.org_id
        and x.gateway_id = gh.gateway_id
        where y.gateway_id is null
        order by gh.first_heartbeat_date
    """
)

crevasse_no_heartbeat_24 = no_heartbeat_24_sf.limit(10).rdd.collect()
total_crevasse_no_heartbeat_24_devices = no_heartbeat_24_sf.count()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial"]
row_data = []
row_data.extend(list(map(create_formatter(), crevasse_no_heartbeat_24)))
crevasse_no_heartbeat_24_table = make_table("", column_names, row_data)[1:]

# Split into 8 rows each
crevasse_no_heartbeat_24_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(crevasse_no_heartbeat_24_table)
]

metrics_to_send["crevasse_no_heartbeat_24"] = {
    "slack_text": f"*Crevasse Devices that have no heartbeat on {str(metrics_date)} but with heartbeat the day before*\n"
    + f"\n Total Devices that meet the criteria *{total_crevasse_no_heartbeat_24_devices}*"
    + "\n".join(crevasse_no_heartbeat_24_table),
}

# COMMAND ----------

# Crevasse that have heartbeat but no data

heartbeat_no_data_sf = spark.sql(
    f"""with all_data as (
            select h.*, g.serial
            from (select date, object_id, org_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
                from kinesisstats.osdhubserverdeviceheartbeat
                where date > date_sub('{str(metrics_date)}', 6)
                and date <= '{str(metrics_date)}' ) as h
            join productsdb.gateways as g
            on g.id = h.gateway_id
            join clouddb.organizations as o
            on h.org_id = o.id
            where g.product_id = 124
            and o.internal_type == 0
            and o.id not in (select org_id from internaldb.simulated_orgs)
            group by h.date, h.object_id, h.org_id, h.gateway_id, g.serial)

        select x.* 
        from (select object_id, org_id, value.proto_value.nordic_lte_debug.lte_cell_info
              from kinesisstats.osdnordicltedebug
              where date > date_sub('{str(metrics_date)}', 6)
              and value.proto_value.nordic_lte_debug.lte_cell_info is not null
              group by object_id, org_id, value.proto_value.nordic_lte_debug.lte_cell_info ) as lte
        right join (select object_id, org_id, gateway_id, serial, count(*) as cnt
              from all_data
              group by object_id, org_id, gateway_id, serial
              having cnt > 4) as x
        on lte.object_id = x.object_id
        and lte.org_id = x.org_id
        where lte.lte_cell_info is null
    """
)

crevasse_heartbeat_no_data = heartbeat_no_data_sf.limit(10).rdd.collect()
total_crevasse_heartbeat_no_data_devices = heartbeat_no_data_sf.count()

max_datetime_yesterday = int(
    datetime.combine(metrics_date, time.max).timestamp() * 1000
)


def create_formatter():
    def format_row(row):
        return [
            Link(
                str(row.object_id),
                f"https://cloud.samsara.com/o/{row.org_id}/devices/{row.object_id}/show?end_ms={max_datetime_yesterday}",
            ),
            str(row.org_id),
            str(row.gateway_id),
            row.serial,
        ]

    return format_row


column_names = ["object_id", "org_id", "gateway_id", "serial"]
row_data = []
row_data.extend(list(map(create_formatter(), crevasse_heartbeat_no_data)))
crevasse_heartbeat_no_data_table = make_table("", column_names, row_data)[1:]

# Split into 8 rows each
crevasse_heartbeat_no_data_table = [
    x + "```\n```" if (i % 15 == 14) else x
    for i, x in enumerate(crevasse_heartbeat_no_data_table)
]

metrics_to_send["crevasse_heartbeat_no_data"] = {
    "slack_text": f"*Crevasse Devices that have heartbeat in the last 5 days but not LTE data*\n"
    + f"\n Total Devices that meet the criteria *{total_crevasse_heartbeat_no_data_devices}*"
    + "\n".join(crevasse_heartbeat_no_data_table),
}


# COMMAND ----------

# Not a clean solution
for (key, metric) in metrics_to_send.items():
    if len(metric["slack_text"]) <= 4046:
        send_slack_message_to_channels(metric["slack_text"], ["alerts-stce-fw-ag5x"])
    else:
        send_split_message = metric["slack_text"].split("```\n```")
        send_slack_message_to_channels(
            send_split_message[0] + "```", ["alerts-stce-fw-ag5x"]
        )
        for msg in send_split_message[1:-1]:
            send_slack_message_to_channels("```" + msg + "```", ["alerts-stce-fw-ag5x"])
        send_slack_message_to_channels(
            "```" + send_split_message[-1], ["alerts-stce-fw-ag5x"]
        )

# COMMAND ----------
