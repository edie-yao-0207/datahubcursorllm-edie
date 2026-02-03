# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from datetime import datetime
import operator

# Helpers
import boto3
import numpy as np
import pandas as pd
from pyspark.sql.functions import avg, variance
from scipy.stats import boxcox, zscore
import sklearn as sk


def normalize_cluster_inputs(org_attrs_cpy, lower=0, upper=1):
    greater_than_lower = org_attrs_cpy["percent_passenger"] >= lower
    less_than_upper = org_attrs_cpy["percent_passenger"] < upper
    filtered_lower = org_attrs_cpy[greater_than_lower]
    mixed_fleets_filtered = filtered_lower[less_than_upper]
    mixed_fleets_filtered = mixed_fleets_filtered.dropna()
    mixed_fleets_filtered.index = np.arange(0, len(mixed_fleets_filtered))

    x = pd.Series(
        zscore(boxcox(mixed_fleets_filtered["distance_driven_miles_per_vehicle"], 0.5))
    )
    x = np.where(x.between(x.quantile(0), x.quantile(0.99)), x, None)
    mixed_fleets_filtered["distance_driven_miles_per_vehicle"] = x

    x = pd.Series(zscore(boxcox(mixed_fleets_filtered["trip_length"], 1)))
    x = np.where(x.between(x.quantile(0), x.quantile(0.99)), x, None)
    mixed_fleets_filtered["trip_length"] = x

    mixed_fleets_filtered = mixed_fleets_filtered.drop(["percent_passenger"], axis=1)
    mixed_fleets_filtered = mixed_fleets_filtered.drop(
        ["unique_active_vehicles"], axis=1
    )
    mixed_fleets_filtered = mixed_fleets_filtered.drop(["percent_trips_city"], axis=1)
    mixed_fleets_filtered = mixed_fleets_filtered.dropna()

    return mixed_fleets_filtered


def main():
    if boto3.session.Session().region_name != "us-west-2":
        return

    client = get_s3_client("samsara-benchmarking-metrics-read")
    bucket = "samsara-benchmarking-metrics"
    key = "org_attributes_eu/part"

    response = client.list_objects_v2(Bucket=bucket, Prefix=key)
    latest = max(response["Contents"], key=lambda x: x["LastModified"])

    # halt if the EU org attributes weren't updated today. The notebook that updates them is scheduled to run 2 hourse before this one.
    if latest["LastModified"].date() < datetime.now().date():
        raise Exception(
            f'EU org attributes are stale. Last updated {latest["LastModified"]}'
        )
    # Read in EU org attribute CSV
    eu_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("s3a://samsara-benchmarking-metrics/org_attributes_eu")
        .toPandas()
    )

    # Add a dummy column for percent_trips_city since we don't calculate this attribute for EU cloud customer orgs
    # shape: (670, 6)
    eu_df["percent_trips_city"] = 0

    query = f"""
    select
        org_id,
        distance_driven_miles_per_vehicle,
        trip_length,
        unique_active_vehicles,
        percent_passenger,
        percent_trips_city
    from
        dataproducts.org_attributes as a
    where
        unique_active_vehicles > 0
        and distance_driven_miles_per_vehicle > 5
        and org_id not in (
            29664,
            9547,
            47846,
            18883,
            23975,
            18892,
            25869,
            658,
            14481
        )
    """

    base_data_sdf = spark.sql(query)
    base_data = base_data_sdf.toPandas()
    base_data = pd.concat([base_data, eu_df])
    base_data.index = range(0, len(base_data))

    temp_0_25 = normalize_cluster_inputs(base_data.copy(), upper=0.25)
    temp_25_75 = normalize_cluster_inputs(base_data.copy(), lower=0.25, upper=0.75)
    temp_75_1 = normalize_cluster_inputs(base_data.copy(), lower=0.75)

    temp = pd.concat((temp_0_25, temp_25_75, temp_75_1))

    # Create PySpark DataFrame from Pandas
    org_attr_norm = spark.createDataFrame(temp)
    org_attr_norm.createOrReplaceTempView("org_attr_norm_all")

    # we can get the existing cluster centers by taking the average of distance
    # driven miles per vehicle and trip length for each cohort since each cluster
    # center (identified by the cohort ID) should be the mean of all points in
    # that cluster
    org_segments = spark.table("dataproducts.org_segments_v3")
    cluster_pre = (
        org_attr_norm.join(org_segments, org_attr_norm.org_id == org_segments.org_id)
        .groupBy("cohort_id")
        .agg(
            avg(org_attr_norm.distance_driven_miles_per_vehicle).alias(
                "center_dist_mpv"
            ),
            avg(org_attr_norm.trip_length).alias("center_trip_length"),
        )
    )

    cluster_pre.createOrReplaceTempView("prev_cluster_centers")

    now = datetime.now()

    spark.sql(
        f"""
    select
        a.org_id,
        min_by(
            -- find the cohort with the min distance to this org
            p.cohort_id,
            sqrt(
            -- distance function sqrt((x1-x2)^2 + (y1-y2)^2)
            pow(
                p.center_dist_mpv - a.distance_driven_miles_per_vehicle,
                2
            ) + pow(p.center_trip_length - a.trip_length, 2)
            )
        ) as cohort_id,
        date('{now.strftime("%Y-%m-%d")}') as created_at
    from
        org_attr_norm_all as a
    join dataproducts.org_attributes as oa on oa.org_id = a.org_id
    left join dataproducts.org_segments_v3 as s on s.org_id = a.org_id
    join prev_cluster_centers as p on (
            oa.percent_passenger between 0
            and 0.2499999
            and p.cohort_id between 0
            and 9
        )
        or (
            oa.percent_passenger between 0.25
            and 0.7499999
            and p.cohort_id between 10
            and 19
        )
        or (
            oa.percent_passenger between 0.75
            and 1
            and p.cohort_id between 20
            and 29
        )
    where
        s.org_id is null -- only get cohorts for orgs that are not already assigned
    group by
        a.org_id
    """
    ).write.insertInto("dataproducts.org_segments_v3")

    s3_bucket = f"s3://samsara-benchmarking-metrics"

    df = (
        spark.read.format("csv")
        .options(header="true", inferSchema="true")
        .load(f"{s3_bucket}/org_segments_v3")
    )

    os = spark.table("dataproducts.org_segments_v3")
    mismatches = df.join(os, os.org_id == df.org_id).filter(
        os.cohort_id != df.cohort_id
    )

    if mismatches.count() > 0:
        raise Exception("orgs changed cohorts")

    spark.sql(
        """
    select
        cast(org_id as bigint) AS org_id,
        if(cohort_id > 9, int(cohort_id), concat('0', int(cohort_id))) as cohort_id
    from
        dataproducts.org_segments_v3
    """
    ).coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(
        f"{s3_bucket}/org_segments_v3"
    )


main()
