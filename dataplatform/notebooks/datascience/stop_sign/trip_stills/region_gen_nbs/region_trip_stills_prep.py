# Databricks notebook source
import os

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

step_values = get_steps_widgets()

# COMMAND ----------

mode, min_date = get_step_value("trip_still_prep", step_values)
trip_stills_s3urls_sdf = spark.sql(
    f"""
 with
   ufs as (
     select *
     from kinesisstats.osduploadedfileset
     where date >= "{min_date}"  and
     value.proto_value.uploaded_file_set.gateway_id is not null and
     value.proto_value.uploaded_file_set.file_timestamp is not null
   ),
   exploded as (
     select
       date,
       org_id,
       object_id as cm_device_id,
       value.proto_value.uploaded_file_set.file_timestamp,
       value.proto_value.uploaded_file_set.gateway_id as device_id,
       explode(value.proto_value.uploaded_file_set.s3urls) as uploaded_raw_s3url
     from ufs
   ),
   cleaned as (
      select
        *,
        concat(
          regexp_replace(
            uploaded_raw_s3url,
            "https://samsara-dashcam-videos.s3.us-west-2.amazonaws.com",
            "s3://samsara-dashcam-videos"
          ),
          ".jpeg"
        ) as s3url
      from exploded
      where uploaded_raw_s3url not rlike "camera-still-driver" and uploaded_raw_s3url rlike "camera-still"
   )

 select * from cleaned;
 """
)

# COMMAND ----------


@udf("string")
def process_s3url(s3url):
    if "?" in s3url:
        return s3url.split("?")[0] + ".jpeg"
    return s3url


# COMMAND ----------

trip_stills_s3urls_sdf = trip_stills_s3urls_sdf.withColumn(
    "s3url", process_s3url(F.col("s3url"))
)
trip_stills_s3urls_sdf.write.mode("ignore").saveAsTable(
    "stopsigns.trip_forward_stills_s3urls"
)
trip_stills_s3urls_sdf.createOrReplaceTempView("trip_stills_s3urls")

# COMMAND ----------
if mode != "ignore":
    spark.sql(
        """
    merge into stopsigns.trip_forward_stills_s3urls as target
    using trip_stills_s3urls as updates
    on target.org_id = updates.org_id
    and target.device_id = updates.device_id
    and target.cm_device_id = updates.cm_device_id
    and target.date = updates.date
    and target.s3url = updates.s3url
    when matched then update set *
    when not matched then insert * ;
    """
    )

# COMMAND ----------

# Kick off all notebooks
selected_regions = dbutils.widgets.get("regions").split(",")


for region_name in selected_regions:
    # kick off telemetry for all regions and the whole process starts
    trigger_next_nb(
        "/backend/datascience/stop_sign/telemetry/region_gen_nbs/region_telem_data_gen",
        f"{region_name}_telem_data_gen",
        region_name,
        additional_nb_params=step_values,
    )
