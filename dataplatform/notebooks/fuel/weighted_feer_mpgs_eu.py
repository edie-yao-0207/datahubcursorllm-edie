# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace temp view feer_aggregated as
# MAGIC select
# MAGIC   org_id,
# MAGIC   object_id,
# MAGIC   sum(fuel_consumed_ml) as fuel_consumed_ml,
# MAGIC    -- combine gps and canonical distance for date ranges where v1 (gps) data and v3 (canonical) data exists
# MAGIC    -- these values should never exist for the same row so data duplication should not be a problem
# MAGIC   coalesce(sum(distance_traveled_m_gps), 0) + coalesce(sum(distance_traveled_m), 0) as distance_traveled_m,
# MAGIC   sum(cast(energy_consumed_kwh as double)) as energy_consumed_kwh,
# MAGIC   sum(electric_distance_traveled_m) as electric_distance_traveled_m
# MAGIC from report_staging.fuel_energy_efficiency_report_v4
# MAGIC where date >= date_add(current_date(), -60)
# MAGIC AND date <= current_date()
# MAGIC AND (fuel_consumed_ml > 0 or energy_consumed_kwh > 0)
# MAGIC -- include vehicles, not drivers
# MAGIC AND object_type = 1
# MAGIC AND fuel_consumed_ml > 5000 -- 1.3 US gallons
# MAGIC group by org_id, object_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view weighted_mpges as (
# MAGIC   select
# MAGIC     t2.make,
# MAGIC     t2.model,
# MAGIC     t2.year,
# MAGIC     CASE -- we need mpg calculations to know which hourly data points to throw out
# MAGIC       WHEN fuel_consumed_ml > 0 and energy_consumed_kwh > 0
# MAGIC         THEN electric_distance_traveled_m / distance_traveled_m * 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922)) +
# MAGIC             (1 - electric_distance_traveled_m / distance_traveled_m) * (distance_traveled_m - electric_distance_traveled_m) * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
# MAGIC       WHEN fuel_consumed_ml = 0 AND energy_consumed_kwh > 0
# MAGIC         THEN 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922))
# MAGIC       WHEN fuel_consumed_ml > 0 AND energy_consumed_kwh = 0
# MAGIC         THEN distance_traveled_m * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
# MAGIC       ELSE 0
# MAGIC     END weighted_mpge,
# MAGIC     t1.fuel_consumed_ml,
# MAGIC     t1.energy_consumed_kwh,
# MAGIC     t1.distance_traveled_m,
# MAGIC     t1.electric_distance_traveled_m
# MAGIC   from feer_aggregated t1
# MAGIC   left join
# MAGIC     productsdb.devices t2
# MAGIC     on
# MAGIC       t1.org_id = t2.org_id
# MAGIC       and t1.object_id = t2.id
# MAGIC   left join
# MAGIC     clouddb.organizations as t3
# MAGIC     on
# MAGIC       t1.org_id = t3.id
# MAGIC       and t3.internal_type <> 1
# MAGIC   WHERE t2.make IS NOT NULL and t2.model IS NOT NULL and t2.year IS NOT NULL
# MAGIC );
# MAGIC
# MAGIC cache table weighted_mpges;
# MAGIC
# MAGIC select * from weighted_mpges

# COMMAND ----------

# Save to S3 to copy over to US clusters for data compilation
from py4j.protocol import Py4JJavaError

s3_bucket = "s3://samsara-eu-benchmarking-metrics"

query = "select * from weighted_mpges"

vehicle_segments_sdf = spark.sql(query)
vehicle_segments_df = vehicle_segments_sdf.toPandas()
vehicle_segments_df.head()

try:
    vehicle_segments_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", True
    ).save(f"{s3_bucket}/vehicle_mpge_data_eu")
except Py4JJavaError as e:
    if "com.amazonaws.services.s3.model.MultiObjectDeleteException" not in str(e):
        raise e
    else:
        print(e)

# COMMAND ----------
