# Databricks notebook source
# Global Vars
first_n_vehicles = 200
exclusion_list = [
    "FORD F-350 2019",
    "KENWORTH W9 Series 2016",
    "FREIGHTLINER Columbia 2016",
    "CHEVROLET Equinox 2018",
    "CHEVROLET Equinox 2020",
    "FORD Ranger 2011",
    "KENWORTH T800 2005",
    "Isuzu NPR/NPR-HD/ NPR-XD 2018",
    "NISSAN Versa 2020",
    "FORD F-450 2013",
    "Opel Vivaro 2020",
    "BLUE BIRD BB Convention 2015",
    "FORD F-450 2008",
    "Isuzu NPR/NPR-HD/ NPR-XD 2019",
    "FORD Escape 2013",
]

# COMMAND ----------

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

# Read in EU org attribute CSV
eu_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("s3://samsara-benchmarking-metrics/vehicle_mpge_data_eu/")
)

eu_df_pandas = eu_df.toPandas()
eu_df_pandas.head()

# COMMAND ----------

eu_df.write.mode("overwrite").format("delta").saveAsTable(
    "evsandecodriving_dev.weighted_mpge_by_mmy_org_eu"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view union_weighted_mpges as
# MAGIC select * from weighted_mpges
# MAGIC union all
# MAGIC select make, model, year, weighted_mpge, fuel_consumed_ml, energy_consumed_kwh, distance_traveled_m, electric_distance_traveled_m from evsandecodriving_dev.weighted_mpge_by_mmy_org_eu;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table evsandecodriving_dev.weighted_mpge_by_mmy_org (
# MAGIC   select * from union_weighted_mpges
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view device_count_per_mmy as (
# MAGIC   select count(*) as mm_count, trim(both " " from make) as make, trim(both " " from model) as model, year
# MAGIC   from evsandecodriving_dev.weighted_mpge_by_mmy_org
# MAGIC   where
# MAGIC     make is not null and
# MAGIC     model is not null and
# MAGIC     year is not null and
# MAGIC     year != 0
# MAGIC   group by trim(both " " from make), trim(both " " from model), year
# MAGIC   order by mm_count desc
# MAGIC );
# MAGIC select * from device_count_per_mmy;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- get mpge table filtered of 0 and null values
# MAGIC create or replace temporary view weighted_mpge as (
# MAGIC select
# MAGIC     make,
# MAGIC     model,
# MAGIC     year,
# MAGIC     weighted_mpge
# MAGIC   from evsandecodriving_dev.weighted_mpge_by_mmy_org
# MAGIC   where make is not null
# MAGIC     and model is not null
# MAGIC     and year is not null
# MAGIC     and weighted_mpge >= 0
# MAGIC );
# MAGIC select * from weighted_mpge

# COMMAND ----------

query = f"select * from weighted_mpge"
mpge = spark.sql(query)
mpge_df = mpge.toPandas()

# Only include MMYs with at least 30 vehicles.
# Analysis found we need 30 vehicles to have a reasonable estimate of the true population mean.
query = "select * from device_count_per_mmy where mm_count >= 30"
mmy = spark.sql(query)
mmy_df = mmy.toPandas()

# COMMAND ----------

import re
import numpy as np

# The IQR method is used because it is a common method, and works well for skewed distributions.
# Many MMYs are skewed, and so standard deviation or z-score methods, which assume normality, are not appropriate.
def remove_outliers(data, threshold=1.5):
    """
    Remove outliers from an array based on the IQR method.

    Args:
        data (array-like): The input array (e.g., list, numpy array, or pandas Series).
        threshold (float): The IQR threshold for detecting outliers (default is 1.5).

    Returns:
        np.ndarray: A cleaned numpy array with outliers removed.
    """
    data = np.array(data)  # Ensure input is a numpy array
    if len(data) == 0:
        return data  # Return empty array if input is empty

    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1

    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr

    return data[(data >= lower_bound) & (data <= upper_bound)]


# COMMAND ----------

mmy_df["useable"] = True
mmy_df["mean"] = 0.0
mmy_df["median"] = 0.0
mmy_df["top10"] = 0.0
mmy_df["make_og"] = mmy_df["make"]
mmy_df["model_og"] = mmy_df["model"]
mmy_df["metric_type"] = "MPGE"
mmy_df.head()

# construct benchmark MPGe
for i, row in mmy_df.iterrows():
    make = row["make"]
    model = row["model"]
    year = row["year"]
    if f"{make} {model} {year}" in exclusion_list:
        continue

    filtered_df = mpge_df.loc[
        (mpge_df["make"] == make)
        & (mpge_df["model"] == model)
        & (mpge_df["year"] == year)
    ]
    weighted_mpge = filtered_df["weighted_mpge"]

    # Remove outliers from weighted_mpge
    weighted_mpge_with_outliers_removed = remove_outliers(weighted_mpge)

    # We have already filtered for MMYs with at least 30 vehicles.
    # Now, we may have filtered out more vehicles due to outliers, and may now have fewer than 30 vehicles.
    # Since the 30 minimum is partly an effort to reduce the impact of outliers, and we have already removed outliers, we relax the 30 constraint to 25.
    MINIMUM_SAMPLE_SIZE_AFTER_OUTLIER_REMOVAL = 25
    if (
        weighted_mpge_with_outliers_removed.size
        < MINIMUM_SAMPLE_SIZE_AFTER_OUTLIER_REMOVAL
    ):
        mmy_df["useable"][i] = False
        continue

    mmy_df["make"][i] = re.sub("[^0-9a-zA-Z]+", "_", str(mmy_df["make"][i]).strip())
    mmy_df["model"][i] = re.sub("[^0-9a-zA-Z]+", "_", str(mmy_df["model"][i]).strip())
    mmy_df["model_og"][i] = re.sub(",", "", str(mmy_df["model_og"][i]).strip())
    mmy_df["model_og"][i] = re.sub('"', "", str(mmy_df["model_og"][i]).strip())

    mmy_df["mean"][i] = np.mean(weighted_mpge)
    mmy_df["median"][i] = np.median(weighted_mpge)
    mmy_df["top10"][i] = weighted_mpge.quantile(0.90)

# COMMAND ----------

# get first n vehicle MMYs
useable_df = mmy_df[mmy_df["useable"]].copy()
useable_df = useable_df.reset_index()
final_df = useable_df[
    [
        "mm_count",
        "make",
        "model",
        "year",
        "metric_type",
        "mean",
        "median",
        "top10",
        "make_og",
        "model_og",
    ]
].copy()
final_df

# COMMAND ----------

working_copy_sdf = spark.createDataFrame(final_df)
working_copy_sdf.createOrReplaceTempView("working_copy")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table evsandecodriving_dev.top_mmy_mpge_benchmarks_v1 (
# MAGIC   select
# MAGIC     concat(make,'_',model,'_',year) as cohort_id,
# MAGIC     metric_type,
# MAGIC     mean,
# MAGIC     median,
# MAGIC     top10
# MAGIC   from working_copy
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table evsandecodriving_dev.top_mmy_mpge_cohorts_v1 (
# MAGIC   select
# MAGIC     concat(make,'_',model,'_',year) as cohort_id,
# MAGIC     make_og as make,
# MAGIC     model_og as model,
# MAGIC     year
# MAGIC   from working_copy
# MAGIC )

# COMMAND ----------

report_name = "industry_trends"
s3_bucket = f"s3://samsara-databricks-workspace/fuelservices/{report_name}"
query = "select * from evsandecodriving_dev.top_mmy_mpge_benchmarks_v1"

mmy_mpge_sdf = spark.sql(query)
mmy_mpge_df = mmy_mpge_sdf.toPandas()
mmy_mpge_df.head()

mmy_mpge_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/mpge_benchmark_metrics")

# COMMAND ----------

report_name = "industry_trends"
s3_bucket = f"s3://samsara-databricks-workspace/fuelservices/{report_name}"
query = "select * from evsandecodriving_dev.top_mmy_mpge_cohorts_v1"

mmy_mpge_sdf = spark.sql(query)
mmy_mpge_df = mmy_mpge_sdf.toPandas()
mmy_mpge_df.head()

mmy_mpge_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/vehicle_cohorts")

# COMMAND ----------

# PRODUCTION BUCKET DATA PROVIDER BENCHMARKS
from py4j.protocol import Py4JJavaError

s3_bucket = "s3://samsara-benchmarking-metrics"
metric_group = "mpge"

query = "select * from evsandecodriving_dev.top_mmy_mpge_benchmarks_v1"

benchmark_metrics_sdf = spark.sql(query)
benchmark_metrics_df = benchmark_metrics_sdf.toPandas()
benchmark_metrics_df.head()

try:
    benchmark_metrics_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", True
    ).save(f"{s3_bucket}/{metric_group}_benchmark_metrics")
except Py4JJavaError as e:
    if "com.amazonaws.services.s3.model.MultiObjectDeleteException" not in str(e):
        raise e
    else:
        print(e)

# COMMAND ----------

# PRODUCTION BUCKET DATA PROVIDER COHORTS
from py4j.protocol import Py4JJavaError

s3_bucket = "s3://samsara-benchmarking-metrics"
metric_group = "mpge"

query = "select * from evsandecodriving_dev.top_mmy_mpge_cohorts_v1"

vehicle_segments_sdf = spark.sql(query)
vehicle_segments_df = vehicle_segments_sdf.toPandas()
vehicle_segments_df.head()

try:
    vehicle_segments_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", True
    ).save(f"{s3_bucket}/vehicle_segments_v1")
except Py4JJavaError as e:
    if "com.amazonaws.services.s3.model.MultiObjectDeleteException" not in str(e):
        raise e
    else:
        print(e)
