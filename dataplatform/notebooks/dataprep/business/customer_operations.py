# Databricks notebook source
from pyspark.sql.functions import *

spark.read.format("bigquery").option(
    "table", "samsara-data.backend.org_sfdc_account_latest"
).load().createOrReplaceTempView("sam_orgs")

cable_mappings = sqlContext.createDataFrame(
    [
        (17, 0, "Passenger"),
        (17, 1, "J1939"),
        (24, 1, "J1939_Universal"),
        (24, 2, "J1939_Type_1"),
        (24, 3, "J1708_with_USB"),
        (24, 4, "Passenger"),
        (24, 5, "J1939_Type_1_Volvo/Mack"),
        (24, 6, "J1708_power_only_or_RP1226_J1939_Type_1_or_2"),
        (24, 7, "J1939_Universal_Tachograph"),
    ],
    ["product_id", "cable_id", "type"],
)

cable_mappings.registerTempTable("cable_mappings")
cable_mappings.show()

# COMMAND ----------

driving_activity = spark.sql(
    """
select
  sam_orgs.SamNumber as sam_number,
  org_dates.date,
  trips.device_id,
  count(trips.device_id) as num_trips,
  sum((proto.end.time - proto.start.time)) as trip_duration,
  sum(proto.trip_distance.distance_meters) as trip_distance_meters,
  sum(proto.trip_fuel.fuel_consumed_ml) as fuel_consumed_ml,
  sum(proto.trip_speeding_mph.not_speeding_ms) as not_speeding_ms,
  sum(proto.trip_speeding_mph.light_speeding_ms) as light_speeding_ms,
  sum(proto.trip_speeding_mph.moderate_speeding_ms) as moderate_speeding_ms,
  sum(proto.trip_speeding_mph.heavy_speeding_ms) as heavy_speeding_ms,
  sum(proto.trip_speeding_mph.severe_speeding_ms) as severe_speeding_ms,
  count(events.detail_proto.accel_type) as event_count
from dataprep.customer_dates org_dates
join sam_orgs on org_dates.sam_number = sam_orgs.SamNumber
left join trips2db_shards.trips trips
  on org_dates.date = trips.date
  and sam_orgs.OrgId = trips.org_id
  trips.version = 101
left join safetydb_shards.safety_events events
  on trips.device_id = events.device_id
  and trips.org_id = events.org_id
  and events.event_ms between proto.start.time and proto.end.time
  and events.date = org_dates.date
where
  (proto.end.time-proto.start.time)<(24*60*60*1000) and -- Filter out very long trips
  proto.start.time != proto.end.time -- actual trips
group by
  sam_orgs.SamNumber,
  org_dates.date,
  trips.device_id
"""
)

driving_activity.createOrReplaceTempView("driving_activity")

# COMMAND ----------

device_data = spark.sql(
    """
select
  devices.id as device_id,
  sum(cell_usage.data_usage) as data_usage,
  sam_orgs.SamNumber as sam_number,
  org_dates.date
from dataprep.customer_dates org_dates
join sam_orgs on sam_orgs.SamNumber = org_dates.sam_number
join productsdb.devices devices
  on sam_orgs.OrgId = devices.org_id
left join dataprep_cellular.att_daily_usage cell_usage
  on devices.iccid = cell_usage.iccid
  and org_dates.date = cell_usage.record_received_date
group by
  sam_orgs.SamNumber,
  org_dates.date,
  devices.id
"""
)

device_data.createOrReplaceTempView("device_data")

# COMMAND ----------

# read entire history of dataprep table for aggregate fleet comp
vehicle_cable_data = spark.sql(
    """
select distinct
  sam_orgs.SamNumber as sam_number,
  c.device_id,
  max((c.date, c.cable_type)).cable_type as cable_id,
  d.product_id
from dataprep.customer_dates org_dates
join sam_orgs on sam_orgs.SamNumber = org_dates.sam_number
left join dataprep.device_cables as c
  on sam_orgs.OrgId = c.org_id
  and org_dates.date = c.date
left join productsdb.devices as d
  on sam_orgs.OrgId = d.org_id
  and c.device_id = d.id
where d.product_id in (17, 24)
group by
  sam_orgs.SamNumber,
  c.device_id,
  d.product_id
"""
)

vehicle_cable_data.createOrReplaceTempView("vehicle_cable_data")

# COMMAND ----------

cable_data_mapped = spark.sql(
    """
select
  vehicle_cable_data.*,
  map.type
from vehicle_cable_data
left join cable_mappings map
  on vehicle_cable_data.product_id = map.product_id
  and vehicle_cable_data.cable_id = map.cable_id
"""
)

cable_data_mapped.createOrReplaceTempView("cable_data_mapped")

# COMMAND ----------

cable_data_pivoted = (
    cable_data_mapped.groupBy("sam_number").pivot("type").agg(count("type")).fillna(0)
)
cable_data_pivoted = cable_data_pivoted.drop("null")
cable_data_pivoted = cable_data_pivoted.withColumnRenamed(
    "J1939_Type_1_Volvo/Mack", "J1939_Type_1_Volvo_Mack"
)
cable_data_pivoted.createOrReplaceTempView("cable_data_pivoted")

# COMMAND ----------

operations_data = spark.sql(
    """
select
  org_dates.sam_number,
  org_dates.date,
  first(cable_data_pivoted.J1708_power_only_or_RP1226_J1939_Type_1_or_2) as num_J1708_power_RP1226_J1939_type1_type2,
  first(cable_data_pivoted.J1708_with_USB) as num_J1708_usb,
  first(cable_data_pivoted.J1939) as num_J1939,
  first(cable_data_pivoted.J1939_Type_1) as num_J1939_type1,
  first(cable_data_pivoted.J1939_Type_1_Volvo_Mack) as num_J1939_type1_volvo_mack,
  first(cable_data_pivoted.J1939_Universal) as num_J1939_universal,
  first(cable_data_pivoted.J1939_Universal_Tachograph) as num_J1939_universal_tachograph,
  first(cable_data_pivoted.Passenger) as num_passenger,
  sum(driving_activity.num_trips) as num_trips,
  sum(driving_activity.trip_duration) as total_trip_duration,
  sum(driving_activity.trip_distance_meters) as total_trip_distance_meters,
  sum(driving_activity.fuel_consumed_ml) as total_fuel_consumed_ml,
  sum(driving_activity.not_speeding_ms) as total_not_speeding_ms,
  sum(driving_activity.light_speeding_ms) as total_light_speeding_ms,
  sum(driving_activity.moderate_speeding_ms) as total_moderate_speeding_ms,
  sum(driving_activity.heavy_speeding_ms) as total_heavy_speeding_ms,
  sum(driving_activity.severe_speeding_ms) as total_severe_speeding_ms,
  sum(driving_activity.event_count) as total_harsh_events,
  sum(device_data.data_usage) as total_data_usage
from dataprep.customer_dates org_dates
join cable_data_pivoted
  on cable_data_pivoted.sam_number = org_dates.sam_number
left join driving_activity
  on org_dates.sam_number = driving_activity.sam_number
  and org_dates.date = driving_activity.date
left join device_data
  on org_dates.sam_number = device_data.sam_number
  and driving_activity.device_id = device_data.device_id
  and org_dates.date = device_data.date
group by
  org_dates.sam_number,
  org_dates.date
"""
)

operations_data = operations_data.fillna(0)

# COMMAND ----------

operations_data.write.format("delta").mode("ignore").partitionBy("date").option(
    "overwriteSchema", "true"
).saveAsTable("dataprep.customer_operations")
operations_data_updates = operations_data.filter(
    col("date") >= date_sub(current_date(), 3)
)
operations_data_updates.write.mode("overwrite").partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(), 3)"
).saveAsTable("dataprep.customer_operations")
