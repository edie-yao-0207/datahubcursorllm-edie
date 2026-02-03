# Databricks notebook source
# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.queryWatchdog.maxQueryTasks=150000

# COMMAND ----------


class TripStillLocGen:
    def __init__(self, city_name):
        self.city_name = city_name

    def get_query(self):
        return f"""
      select
        trip_stills.*,
        max(loc.time) as loc_time,
        max((loc.time, loc.lat)).lat as loc_lat,
        max((loc.time, loc.lng)).lng as loc_lng,
        max((loc.time, loc.speed_meters_per_second)).speed_meters_per_second as loc_speed_meters_per_second,
        max((loc.time, loc.loc_bearing)).loc_bearing as loc_bearing,
        max((loc.time, loc.hex_id_res_3)).hex_id_res_3 as hex_id_res_3,
        max((loc.time, loc.way_id)).way_id as way_id
      from stopsigns.trip_forward_stills_s3urls as trip_stills
      join stopsigns.{self.city_name}_loc_data as loc
        on trip_stills.device_id = loc.device_id and
        trip_stills.date = loc.date and
        (trip_stills.file_timestamp - loc.time) <= 10000 and
        trip_stills.file_timestamp >= loc.time
      group by
        trip_stills.date,
        trip_stills.org_id,
        trip_stills.cm_device_id,
        trip_stills.file_timestamp,
        trip_stills.device_id,
        trip_stills.uploaded_raw_s3url,
        trip_stills.s3url
        """

    def _gen_raw_loc_df(self, min_date_for_update):
        query = self.get_query()
        raw_loc = spark.sql(query)
        # filter by date
        raw_loc.filter(F.col("date") >= min_date_for_update)
        return raw_loc

    def _calc_trip_still_lat_lon(self, loc_sdf):
        trip_stills_locations_sdf = loc_sdf.withColumn(
            "distance_meters",
            ((col("file_timestamp") - col("loc_time")) / 1000)
            * col("loc_speed_meters_per_second"),
        )
        trip_stills_locations_sdf = trip_stills_locations_sdf.withColumn(
            "trip_still_lat",
            get_lat_for_dist_bearing(
                col("loc_lat"),
                col("loc_lng"),
                col("distance_meters"),
                col("loc_bearing"),
            ),
        )
        trip_stills_locations_sdf = trip_stills_locations_sdf.withColumn(
            "trip_still_lon",
            get_lon_for_dist_bearing(
                col("loc_lat"),
                col("loc_lng"),
                col("distance_meters"),
                col("loc_bearing"),
            ),
        )
        return trip_stills_locations_sdf

    def _gen_h3(self, trip_stills_locations_sdf):
        return trip_stills_locations_sdf.withColumn(
            "hex_id_res_13",
            lat_lng_to_h3_str_udf(
                col("trip_still_lat"), col("trip_still_lon"), lit(13)
            ),
        )

    def _select_cols(self, select_sdf):
        return select_sdf.select(
            "date",
            "org_id",
            "cm_device_id",
            "file_timestamp",
            "device_id",
            "uploaded_raw_s3url",
            "s3url",
            "trip_still_lat",
            "trip_still_lon",
            "hex_id_res_13",
            "hex_id_res_3",
            "loc_bearing",
            "loc_speed_meters_per_second",
            "way_id",
        )

    def run_and_save(self, mode="overwrite", min_date_for_update="2017-01-01"):
        if mode == "ignore":
            return
        raw_loc_sdf = self._gen_raw_loc_df(min_date_for_update)
        trip_stills_locations_sdf = self._calc_trip_still_lat_lon(raw_loc_sdf)
        h3_sdf = self._gen_h3(trip_stills_locations_sdf)
        write_sdf = self._select_cols(h3_sdf)
        write_table_with_date_partition(
            write_sdf,
            f"stopsigns.trip_stills_locations_{self.city_name}",
            min_date_for_update=min_date_for_update,
        )


# COMMAND ----------
