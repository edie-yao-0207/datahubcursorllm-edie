create or replace temporary view exploded_incr_cell_usage as (
  select
    date,
    org_id,
    explode(
      concat(
        value.proto_value.incremental_cellular_usage.attached_device_usage,
        ARRAY(
          named_struct(
            "object_id", object_id,
            "bucket", value.proto_value.incremental_cellular_usage.bucket,
            "total_upload_bytes", value.proto_value.incremental_cellular_usage.total_upload_bytes,
            "total_download_bytes", value.proto_value.incremental_cellular_usage.total_download_bytes,
            "hub_client_usage", cast(null as array<struct<usage_type:int, upload_bytes:bigint, download_bytes:bigint>>)
          )
        )
      )
    ) as usage
  from kinesisstats.osdincrementalcellularusage as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and a.time <= b.time_ms
);

create or replace temporary view exploded_buckets as (
  select
    date,
    org_id,
    usage.object_id as gateway_id_for_all_products_except_device_id_for_vgs,
    explode(usage.bucket) as bucket
  from exploded_incr_cell_usage
);

create or replace temporary view data_usage as (
  select
    date,
    org_id,
    gateway_id_for_all_products_except_device_id_for_vgs,
    case
      when bucket.bucket_type = 0 then "Invalid"
      when bucket.bucket_type = 1 then "HubClient"
      when bucket.bucket_type = 2 then "Upgrade"
      when bucket.bucket_type = 3 then "DeltaUpgrade"
      when bucket.bucket_type = 4 then "HotspotNonwhitelist"
      when bucket.bucket_type = 5 then "HotspotWhitelist"
      when bucket.bucket_type = 6 then "Livestream"
      when bucket.bucket_type = 7 then "MapTiling"
      when bucket.bucket_type = 8 then "BendixBackhaul"
      when bucket.bucket_type = 9 then "Ntp"
      when bucket.bucket_type = 10 then "GpsAssistance"
      when bucket.bucket_type = 11 then "Enrollment"
      when bucket.bucket_type = 12 then "TachographClient"
      when bucket.bucket_type = 13 then "TachographServer"
      when bucket.bucket_type = 14 then "MLAppModelDownload"
      when bucket.bucket_type = 15 then "BlePeriphUpgrade"
      when bucket.bucket_type = 99 then "DefaultUnassigned"
      when bucket.bucket_type = 100 then "ImageStartOfTrip"
      when bucket.bucket_type = 101 then "ImageEndOfTrip"
      when bucket.bucket_type = 102 then "ImagePeriodic"
      when bucket.bucket_type = 103 then "ImageOpportunisticCameraId"
      when bucket.bucket_type = 104 then "ImageAccelerometerEvent"
      when bucket.bucket_type = 105 then "ImageApi"
      when bucket.bucket_type = 106 then "ImageHiResStills"
      when bucket.bucket_type = 200 then "VideoRecall"
      when bucket.bucket_type = 201 then "VideoAccelerometerEvent"
      when bucket.bucket_type = 202 then "VideoPanicButton"
      when bucket.bucket_type = 203 then "VideoRfidEvent"
      when bucket.bucket_type = 204 then "VideoFalseNegativeEvent"
      when bucket.bucket_type = 205 then "VideoApi"
      when bucket.bucket_type = 206 then "VideoAutomation"
      when bucket.bucket_type = 207 then "VideoContinuousUpload"
      when bucket.bucket_type = 300 then "HyperlapseRecall"
      when bucket.bucket_type = 305 then "HyperlapseApi"
      when bucket.bucket_type = 306 then "HyperlapseVideoAutomation"
      else concat("Unknown (", cast(bucket.bucket_type as string), ")")
    end as bucket,
    sum(bucket.upload_bytes) / (1024 * 1024 * 1024) as total_upload_gbytes,
    sum(bucket.download_bytes) / (1024 * 1024 * 1024) as total_download_gbytes
  from exploded_buckets
  group by
    date,
    org_id,
    gateway_id_for_all_products_except_device_id_for_vgs,
    bucket.bucket_type
);

-- COMMAND ----------

merge into dataprep_firmware.device_daily_data_usage_by_bucket as target
using data_usage as updates
on target.org_id = updates.org_id
and target.gateway_id_for_all_products_except_device_id_for_vgs = updates.gateway_id_for_all_products_except_device_id_for_vgs
and target.date = updates.date
and target.bucket = updates.bucket
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------
