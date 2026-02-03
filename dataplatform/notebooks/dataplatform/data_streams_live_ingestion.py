import time
from datetime import datetime, timedelta
from typing import List

import boto3
import concurrent.futures
import datadog
import service_credentials  # required for ssm cloud credentials


def setUpDataDog(region):
    # Get Datadog keys from parameter store and initialize datadog
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "standard-read-parameters-ssm"
        )
    )
    ssm_client = boto3_session.client("ssm", region_name=region)
    api_key = ssm_client.get_parameter(Name="DATADOG_API_KEY", WithDecryption=True)[
        "Parameter"
    ]["Value"]
    app_key = ssm_client.get_parameter(Name="DATADOG_APP_KEY", WithDecryption=True)[
        "Parameter"
    ]["Value"]
    datadog.initialize(api_key=api_key, app_key=app_key)


def send_datadog_count_metric(metric: str, count: int, tags: List[str]):
    try:
        datadog.api.Metric.send(
            metrics=[
                {
                    "metric": metric,
                    "points": [(time.time(), count)],
                    "type": "count",
                    "tags": tags,
                }
            ],
        )
    except Exception as e:
        print(f"Error sending metrics to Datadog: {e}")


def postMetrics(type, start_time, success, tableName):
    tags = [
        f"stream:{tableName}",
        f"region:{boto3.session.Session().region_name}",
        f"success:{success}",
    ]
    send_datadog_count_metric(f"datastreams.{type}.count", 1, tags)
    try:
        datadog.api.Metric.send(
            metric=f"datastreams.{type}.duration",
            points=time.time() - start_time,
            type="gauge",
            tags=tags,
        )
    except Exception as e:
        print(f"Error sending metrics to Datadog: {e}")


def generateCreateHistoryTableQuery(dataStream, dataStreamMetadata, s3prefix):
    columnNames = "`_firehoseIngestionDate` DATE"  # We will always have the firehose ingestion date as a column added to just the delta table
    partitionColumns = []
    for partitionColumn in dataStreamMetadata["partitionColumns"]:
        partitionColumnType = dataStreamMetadata["partitionColumns"][partitionColumn]
        columnNames += f", `{partitionColumn}` {partitionColumnType}"
        partitionColumns += [f"`{partitionColumn}`"]
    partitionColumns = ",".join(partitionColumns)
    createTableQuery = f'CREATE TABLE IF NOT EXISTS datastreams_history.{dataStream} ({columnNames}) USING DELTA LOCATION "s3://{s3prefix}data-streams-delta-lake/{dataStream}" PARTITIONED BY ({partitionColumns})'
    return createTableQuery


def generateCreateOrAlterViewQuery(dataStream):
    # SQL command to check if the view exists
    viewExists = (
        spark.sql("SHOW VIEWS IN datastreams")
        .filter(f"viewName = '{dataStream}'")
        .collect()
    )

    # Check if the view exists.
    if viewExists:
        # If the view exists, alter it.
        createOrAlterQuery = f"ALTER VIEW datastreams.{dataStream} AS SELECT * FROM datastreams_history.{dataStream} WHERE date > date_sub(current_date(), 365)"
    else:
        # If the view does not exist, create it.
        createOrAlterQuery = f"CREATE VIEW datastreams.{dataStream} AS SELECT * FROM datastreams_history.{dataStream} WHERE date > date_sub(current_date(), 365)"
    return createOrAlterQuery


def generateCopyIntoQuery(date, stream, streamInfo, s3prefix):
    if streamInfo["DateColumnOverrideExpression"] != "":
        dateExpression = streamInfo["DateColumnOverrideExpression"] + " as date"
    elif streamInfo["DateColumnOverride"] != "":
        dateExpression = f"date({streamInfo['DateColumnOverride']}) as date"
    else:
        dateExpression = f"cast('{date}' AS DATE) as date"
    query = f"COPY INTO datastreams_history.{stream} FROM (SELECT *, {dateExpression}, cast('{str(date)}' AS DATE) as _firehoseIngestionDate FROM 's3://{s3prefix}data-stream-lake/{stream}/data/date={str(date)}') FILEFORMAT = Parquet  COPY_OPTIONS ('mergeSchema' = 'true')"
    return query


def createTable(datastreamsMetadataInfo, s3prefix):
    # Fetch existing table names in raw and history DBs
    existing_history_tables = set(
        row["tableName"]
        for row in spark.sql("SHOW TABLES IN datastreams_history")
        .select("tableName")
        .collect()
    )
    for dataStream, dataStreamMetadata in datastreamsMetadataInfo.items():
        # Create datastreams_history tables if they don't exist
        if dataStream not in existing_history_tables:
            createHistoryTableQuery = generateCreateHistoryTableQuery(
                dataStream, dataStreamMetadata, s3prefix
            )
            create_history_table_start_time = time.time()
            print(f"Running create table query: {createHistoryTableQuery}")
            spark.sql(createHistoryTableQuery)
            postMetrics(
                "create_history_table",
                create_history_table_start_time,
                True,
                dataStream,
            )

        createOrAlterQuery = generateCreateOrAlterViewQuery(dataStream)
        create_view_start_time = time.time()
        print(f"Running create/alter view query: {createOrAlterQuery}")
        spark.sql(createOrAlterQuery)
        postMetrics("create_view", create_view_start_time, True, dataStream)


def copyPartitionQuery(stream, streamInfo, s3prefix):
    today = datetime.utcnow().date()

    def run_copy(target_date, day_label, metric_name):
        start_time = time.time()
        result = {"success": False, "no_data": False, "error": ""}
        try:
            query = generateCopyIntoQuery(target_date, stream, streamInfo, s3prefix)
            print(f"Running copy into query for {day_label}: {query}")
            spark.sql(query)
            print(f"Finished copying {day_label} data for {stream} query {query}")
            postMetrics(metric_name, start_time, True, stream)
            result["success"] = True
        except Exception as e:
            if "Path does not exist" not in str(e):
                print(
                    f"exception for stream: {stream} in copying {day_label}'s data, date: {target_date} exception: {e}"
                )
                postMetrics(metric_name, start_time, False, stream)
                result["error"] = str(e)
            else:
                # If the path doesn't exist we have no records to ingest and don't want to log it as an error
                print(
                    f"No data found for {stream} on {target_date}; skipping {day_label} copy."
                )
                # Optional observability: count "no data" occurrences
                send_datadog_count_metric(
                    "datastreams.live_ingestion.no_data_count",
                    1,
                    [
                        f"stream:{stream}",
                        f"region:{boto3.session.Session().region_name}",
                        f"day:{day_label}",
                    ],
                )
                result["no_data"] = True
        return result

    yesterday = today - timedelta(days=1)
    y_res = run_copy(yesterday, "yesterday", "copy_table_yesterday")
    t_res = run_copy(today, "today", "copy_table_today")
    return {"stream": stream, "yesterday": y_res, "today": t_res}


def getActiveDatastreamsMetadata(allDatastreamsMetadata, s3prefix):
    """
    getActiveDatastreamsMetadata filters out any datastreams that were never active from
    allDatastreamsMetadata by checking whether they have ever had raw data written to S3.
    """
    activeDatastreams = {}
    for streamName in allDatastreamsMetadata:
        try:
            dbutils.fs.ls(f"s3://{s3prefix}data-stream-lake/{streamName}/data")
        except:
            print(
                f"Skip processing datastream {streamName} because there is no \
                  raw data in s3://{s3prefix}data-stream-lake/{streamName}/data"
            )
            send_datadog_count_metric(
                "datastreams.live_ingestion.no_data_count",
                1,
                [
                    f"stream:{streamName}",
                    f"region:{boto3.session.Session().region_name}",
                ],
            )
            continue
        activeDatastreams[streamName] = allDatastreamsMetadata[streamName]
    return activeDatastreams


def main():
    region = boto3.session.Session().region_name
    s3prefix = None
    if region == "us-west-2":
        s3prefix = "samsara-"
    elif region == "eu-west-1":
        s3prefix = "samsara-eu-"
    elif region == "ca-central-1":
        s3prefix = "samsara-ca-"
    else:
        raise Exception("bad region: " + region)

    setUpDataDog(region)
    datastreamsMetadata = {
        "alert_sms_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "amapi_pubsub_messages": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "anomalous_stop_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "api_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "asset_characteristics_parses": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "backend_model_registry_ddb": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "battery_prediction_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "buildkite_job_error_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "buildkite_pipeline_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "cable_selection_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "ce_disassociation_evaluations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "ce_parsed_proxied_advertisements": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "cloud_app_user_interactions": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "cm_outward_obstruction_v2_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "cohort_profile_config_data": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "config_validation_output": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "config_version_history": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "connected_equipment_association_evaluations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "connected_equipment_fma_deliverability_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "connected_equipment_reefer_command_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "connected_sites_camera_device_auth_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "connected_sites_camera_stream_auth_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "construction_zone_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "crash_detector_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "crash_filter_transformed_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(event_ms / 1e3))",
        },
        "crux_aggregated_locations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "crux_observations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "crux_unaggregated_locations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "dashcam_drowsiness_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "dashcam_inward_mtl_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "dashcam_rlv_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "dashcam_rolling_stop_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "dashcam_vru_cw_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "data_connector_client_log_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "delete_s3_video_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "device_backfilled_cargo_state": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(timestamp)",
        },
        "dpf_failure_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "driver_assignment_audit_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "driver_assignment_dropped_backfill_messages": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "driver_assignment_manual_unassign": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "driver_assignment_settings_audit_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "driver_risk_score_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "driver_trip_risk_score_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "driver_trip_risk_score_v2_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "driverappproxy_http_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(timestamp)",
        },
        "dynamic_workflow_runs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "event_detection_checkpoint_status": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "facial_recognition_processing_audit_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "image_timestamp",
            "DateColumnOverrideExpression": "",
        },
        "features_turnoff_by_safeguard": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "firmware_model_registry_ddb": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "forward_collision_warning_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "forward_distance_v2_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "frontend_routeload": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "fuel_anomaly_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "fuel_change_estimator_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "fuel_level_denoising_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "go_test_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "gopls_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "gql_query_load": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "harsh_event_write_path_dropoff_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(event_ms / 1e3))",
        },
        "internal_rma_audit_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "jack_test": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "lane_departure_warning_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "messages_metadata": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "sent_at",
            "DateColumnOverrideExpression": "",
        },
        "ml_data_collection_requests": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "ml_device_configuration_record": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "ml_test_datastream": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "mobile_device_info": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "mobile_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "mobile_nav_routing_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "mobile_sentry_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "date_created",
            "DateColumnOverrideExpression": "",
        },
        "oauth_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "oem_api_data_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "to_date(received_at)",
        },
        "open_weather_severe_weather_alerts": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "org_sam_mapping_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "to_date(timestamp)",
        },
        "osm_way_metadata_refresh": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "owlgraph_application_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "predictive_maintenance_alert_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "to_date(timestamp)",
        },
        "qualifications_jjkeller_migration": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "created_at",
            "DateColumnOverrideExpression": "",
        },
        "rear_collision_warning_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "remote_support_feedback_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "routeplanning_cost_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "safety_asset_processing_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(timestamp_ms / 1e3))",
        },
        "safety_asset_url_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "safety_assetblurringserver_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "safety_cell_data_usage_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(asset_ms / 1e3))",
        },
        "safety_cvworker_audio_deprecation_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(timestamp_ms / 1e3))",
        },
        "safety_ser_sla_bucket_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(timestamp_ms / 1e3))",
        },
        "safety_webrtc_livestream_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(start_unix_ms / 1e3))",
        },
        "safety_workflow_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(timestamp_ms / 1e3))",
        },
        "scientist_experiment_logs": {
            "partitionColumns": {"date": "DATE", "experiment_name": "STRING"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "ser_event_trace": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "recorded_at",
            "DateColumnOverrideExpression": "",
        },
        "smart_maps_ingested_images": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "smart_maps_road_condition_notifications": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "smart_maps_vehicle_settings_changes": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "speed_limit_locations": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(time / 1e3))",
        },
        "speed_spike_detections": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "statsprocessor_future_timestamp_logs_dropped": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "stream_publish_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "tile_generation_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "transcodeworker_processing_duration_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(from_unixtime(asset_ms / 1e3))",
        },
        "triage_event_filter_checkpoint_status": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "vapi_webhook_messages": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "vbs_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "vehicle_misuse_anomalies": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "vehicle_positioning_inference_results": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "video_retrieval_cancelled_log": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "vin_decoding_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "webhook_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "workflow_alert_incident_comparison_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "timestamp",
            "DateColumnOverrideExpression": "",
        },
        "workforce_camera_device_add_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "workforce_camera_stream_add_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "date(created_at)",
        },
        "workforce_local_storage_and_retention_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_media_upload_recovery_attempts": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_hls_client_fetch_segment": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_hls_server_fetch_segment": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_hls_stream_error_end": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_hls_stream_error_start": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_stream_end_logs": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_stream_metric_intervals": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_streaming_page_states": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_unhealthy_webrtc_intervals": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_webrtc_fallback_events": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_webrtc_livestream_metrics": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
        "workforce_video_webrtc_sample_interval": {
            "partitionColumns": {"date": "DATE"},
            "DateColumnOverride": "",
            "DateColumnOverrideExpression": "",
        },
    }

    activeDatastreamsMetadata = getActiveDatastreamsMetadata(
        datastreamsMetadata, s3prefix
    )
    createTable(activeDatastreamsMetadata, s3prefix)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        future_to_stream = {}
        for stream in activeDatastreamsMetadata:
            streamInfo = activeDatastreamsMetadata[stream]
            future = executor.submit(copyPartitionQuery, stream, streamInfo, s3prefix)
            future_to_stream[future] = stream

        for future in concurrent.futures.as_completed(future_to_stream):
            stream = future_to_stream[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Copying stream {stream} failed with exception: {e}")
                results.append(
                    {
                        "stream": stream,
                        "yesterday": {
                            "success": False,
                            "no_data": False,
                            "error": str(e),
                        },
                        "today": {"success": False, "no_data": False, "error": str(e)},
                    }
                )

    # Build summary
    yesterday_success = sum(1 for r in results if r["yesterday"]["success"])
    yesterday_failed_streams = [
        r["stream"]
        for r in results
        if not r["yesterday"]["success"] and not r["yesterday"]["no_data"]
    ]
    yesterday_no_data_streams = [
        r["stream"] for r in results if r["yesterday"]["no_data"]
    ]
    today_success = sum(1 for r in results if r["today"]["success"])
    today_failed_streams = [
        r["stream"]
        for r in results
        if not r["today"]["success"] and not r["today"]["no_data"]
    ]
    today_no_data_streams = [r["stream"] for r in results if r["today"]["no_data"]]

    print("DataStreams copy summary:")
    print(f"- Yesterday successful: {yesterday_success}")
    print(
        f"- Yesterday failed: {len(yesterday_failed_streams)}; streams: {yesterday_failed_streams}"
    )
    print(
        f"- Yesterday no data: {len(yesterday_no_data_streams)}; streams: {yesterday_no_data_streams}"
    )
    print(f"- Today successful: {today_success}")
    print(
        f"- Today failed: {len(today_failed_streams)}; streams: {today_failed_streams}"
    )
    print(
        f"- Today no data: {len(today_no_data_streams)}; streams: {today_no_data_streams}"
    )

    if len(yesterday_failed_streams) > 0 or len(today_failed_streams) > 0:
        raise RuntimeError(
            "At least one copyPartitionQuery failed. See error messages above"
        )


if __name__ == "__main__":
    main()
