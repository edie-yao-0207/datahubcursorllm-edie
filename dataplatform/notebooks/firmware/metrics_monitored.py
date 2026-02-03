# Databricks notebook source
# Table schema must be date, org_id, device_id, value


def test_query(start_date, end_date):
    return """
  with devices as (
  select
    date,
    org_id,
    cm_device_id as device_id,
    "" as metric_subname
  from dataprep_firmware.cm_device_daily_metadata
  where date >= '{}'
    and date <= '{}'
  )
  select
    *,
    case when date >= date_sub('{}', 1) then 1 else 0 end as value
  from devices
""".format(
        start_date, end_date, end_date
    )


def ai_downtime_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      cm_device_id as device_id,
      "" as metric_subname,
      (sum(total_safe_mode_duration_ms) + sum(total_overheated_duration_ms)) / sum(total_trip_duration_ms) as value
    from dataprep_safety.cm_device_health_daily
    where date >= '{}'
      and date <= '{}'
    group by
      date,
      org_id,
      cm_device_id
  """.format(
        start_date, end_date
    )


def recording_uptime_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      cm_device_id as device_id,
      "" as metric_subname,
      total_trip_grace_recording_duration_ms / total_trip_connected_duration_ms as value
    from dataprep_safety.cm_device_health_daily
    where date >= '{}'
        and date <= '{}'
        and total_trip_duration_ms > 600000
        and cm_last_heartbeat_date > date_sub(date, 1)
        and cm_first_heartbeat_date < date
  """.format(
        start_date, end_date
    )


def cloud_connection_uptime_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      device_id,
      "" as metric_subname,
      connection_total_ms / total_ms as value
    from dataprep_firmware.cm_device_daily_cloud_connection
    where date >= '{}'
        and date <= '{}'
  """.format(
        start_date, end_date
    )


def usb_connection_uptime_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      device_id,
      "" as metric_subname,
      attached_ms / total_ms as value
    from dataprep_firmware.cm_device_daily_usb_enumerates
    where date >= '{}'
        and date <= '{}'
  """.format(
        start_date, end_date
    )


def cm_vg_connection_uptime_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      device_id,
      "" as metric_subname,
      dashcam_connected_ms / total_ms as value
    from dataprep_firmware.cm_device_daily_dashcam_vg_connection
    where date >= '{}'
        and date <= '{}'
  """.format(
        start_date, end_date
    )


def livestream_success_rate_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      device_id,
      "" as metric_subname,
      successful_connection_attempts / total_connection_attempts as value
    from dataprep_firmware.cm_device_daily_livestream_attempts
    where date >= '{}'
        and date <= '{}'
  """.format(
        start_date, end_date
    )


def video_retrieval_success_rate_query(start_date, end_date):
    return """
    select
      a.date,
      b.org_id,
      a.device_id,
      "" as metric_subname,
      sum(case when a.state = "Completed" then a.num_requests else 0 end) / sum(a.num_requests) as value
    from dataprep_firmware.device_daily_video_retrievals_by_state as a
    join clouddb.devices as b
      on a.device_id = b.id
    where date >= '{}'
      and date <= '{}'
    group by
      a.date,
      b.org_id,
      a.device_id
  """.format(
        start_date, end_date
    )


def data_usage_query(start_date, end_date):
    return """
    select
      a.date,
      a.org_id,
      b.device_id,
      "" as metric_subname,
      a.total_download_gbytes + a.total_upload_gbytes as value
    from dataprep_firmware.device_daily_data_usage_by_bucket as a
    join clouddb.gateways as b
      on a.org_id = b.org_id
      and a.gateway_id_for_all_products_except_device_id_for_vgs = b.id
    where date >= '{}'
      and date <= '{}'
  """.format(
        start_date, end_date
    )


def cpu_usage_on_trip_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      cm_device_id as device_id,
      "" as metric_subname,
      avg_cpu_usage_on_trip as value
    from dataprep_firmware.cm_device_daily_avg_cpu_usage_on_trip
    where date >= '{}'
      and date <= '{}'
  """.format(
        start_date, end_date
    )


def anomalies_per_trip_hour_query(start_date, end_date):
    return """
    select
      date,
      org_id,
      device_id,
      anomaly_service as metric_subname,
      (60 * 60 * 1000) * num_anomalies / total_trip_duration_ms as value
    from dataprep_firmware.device_daily_anomalies_by_service
    where date >= '{}'
      and date <= '{}'
      and total_trip_duration_ms > 0
  """.format(
        start_date, end_date
    )


def harsh_events_per_trip_hour_query(start_date, end_date):
    return """
    select
      a.date,
      a.org_id,
      a.linked_cm_id as device_id,
      a.harsh_event_type as metric_subname,
      60 * (sum(a.total_event_occurence) / sum(t.total_duration_mins)) as value
    from dataprep_safety.safety_harsh_events_upload_events as a
    left join dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
      on a.date = assoc.date
      and a.linked_cm_id = assoc.cm_device_id
    left join datamodel_telematics.fct_trips_daily as t
      on assoc.date = t.date
      and a.org_id = t.org_id
      and assoc.vg_device_id = t.device_id
      and t.trip_type = 'location_based'
    where a.date >= '{}'
      and a.date <= '{}'
    group by
      a.date,
      a.org_id,
      a.linked_cm_id,
      a.harsh_event_type
  """.format(
        start_date, end_date
    )


def qr_code_scanning_start_latency_secs_query(start_date, end_date):
    return """
    select
        a.date,
        a.org_id,
        a.device_id,
        "" as metric_subname,
        a.latency_ms / 1000 as value
    from dataprep_firmware.qr_code_scan_latency as a
    join dataprep_firmware.device_daily_qr_code_enabled as b
        on a.date = b.date
        and a.device_id = b.device_id
        and a.org_id = b.org_id
    where a.date >= '{}'
        and a.date <= '{}'
    """.format(
        start_date, end_date
    )


def first_sign_in_reminder_latency_secs_query(start_date, end_date):
    return """
    select
      a.date,
      a.org_id,
      a.device_id,
      "" as metric_subname,
      a.latency_ms / 1000 as value
    from dataprep_firmware.sign_in_reminder_alert_latency as a
    join dataprep_firmware.device_daily_qr_code_enabled as b
        on a.date = b.date
        and a.device_id = b.device_id
        and a.org_id = b.org_id
    where a.date >= '{}'
        and a.date <= '{}'
  """.format(
        start_date, end_date
    )


def sign_in_reminder_count_per_trip_query(start_date, end_date):
    return """
    select
        a.date,
        a.org_id,
        a.device_id,
        "" as metric_subname,
    coalesce(b.count, 0) / a.count as value
    from dataprep_firmware.sign_in_alerting_trip_count as a
    left join dataprep_firmware.sign_in_reminder_alert_count as b
        on a.org_id = b.org_id
        and a.device_id = b.device_id
        and a.date = b.date
    where a.date >= '{}'
        and a.date <= '{}'
  """.format(
        start_date, end_date
    )


# Number of tiles published per trip mile, subdivided by layer name.
def tiling_published_layer_count_per_trip_mile(start_date, end_date):
    return """
    select
        a.date,
        a.org_id,
        a.device_id,
        a.layer_name as metric_subname,
        sum(a.count) / (max(t.total_distance_meters) / 1609) as value
    from dataprep_firmware.tiling_published_layer_count as a
    join dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
      on a.date = assoc.date
      and a.device_id = assoc.cm_device_id
    join datamodel_telematics.fct_trips_daily as t
      on assoc.date = t.date
      and a.org_id = t.org_id
      and assoc.vg_device_id = t.device_id
      and t.trip_type = 'location_based'
    where a.date >= '{}'
        and a.date <= '{}'
    group by
      a.date,
      a.org_id,
      a.device_id,
      a.layer_name
    """.format(
        start_date, end_date
    )


# Number of edge speed limits per trip mile, subdivided by layer name and source.
def speeding_edge_speed_limits_per_trip_mile(start_date, end_date):
    return """
    select
      a.date,
      a.org_id,
      a.device_id,
      concat_ws("-", a.speed_limit_layer_source, a.speed_limit_source) as metric_subname,
      sum(a.count) / (max(t.total_distance_meters) / 1609) as value
    from
      dataprep_firmware.speeding_speed_limit_count as a
      join dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
        on a.date = assoc.date
        and a.device_id = assoc.cm_device_id
      join datamodel_telematics.fct_trips_daily as t
        on assoc.date = t.date
        and a.org_id = t.org_id
        and assoc.vg_device_id = t.device_id
        and t.trip_type = 'location_based'
    where a.date >= '{}'
        and a.date <= '{}'
    group by
      a.date,
      a.org_id,
      a.device_id,
      a.speed_limit_layer_source,
      a.speed_limit_source
  """.format(
        start_date, end_date
    )


# COMMAND ----------

metrics_list = {
    "Test": {
        "data_query": test_query,
        "goal": 0,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43],
    },
    "AI Downtime": {
        "data_query": ai_downtime_query,
        "goal": 0,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167],
    },
    "Legacy Recording Uptime": {
        "data_query": recording_uptime_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167],
    },
    "Cloud Connection Uptime": {
        "data_query": cloud_connection_uptime_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "USB Connection Uptime": {
        "data_query": usb_connection_uptime_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "CM VG Connection Uptime": {
        "data_query": cm_vg_connection_uptime_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "Livestream Success Rate": {
        "data_query": livestream_success_rate_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "Video Retrieval Success Rate": {
        "data_query": video_retrieval_success_rate_query,
        "goal": 1,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "Data Usage": {
        "data_query": data_usage_query,
        "goal": 0,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "CPU Usage on Trip": {
        "data_query": cpu_usage_on_trip_query,
        "goal": 0,
        "min_value": 0,
        "max_value": 1,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "Anomalies per Trip Hour": {
        "data_query": anomalies_per_trip_hour_query,
        "goal": 0,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "Harsh Events per Trip Hour": {
        "data_query": harsh_events_per_trip_hour_query,
        "goal": None,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167, 126],
    },
    "QR Code Scanning Start Latency Secs": {
        "data_query": qr_code_scanning_start_latency_secs_query,
        "goal": 0,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 155],
    },
    "First Sign In Reminder Latency Secs": {
        "data_query": first_sign_in_reminder_latency_secs_query,
        "goal": 0,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167],
    },
    "Sign In Reminder Count Per Trip": {
        "data_query": sign_in_reminder_count_per_trip_query,
        "goal": None,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167],
    },
    "Published Tile Layer Count per Trip Mile": {
        "data_query": tiling_published_layer_count_per_trip_mile,
        "goal": None,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167],
    },
    "Edge Speed Limits per Trip Mile": {
        "data_query": speeding_edge_speed_limits_per_trip_mile,
        "goal": None,
        "min_value": 0,
        "max_value": None,
        "product_ids": [43, 44, 155, 167],
    },
}
