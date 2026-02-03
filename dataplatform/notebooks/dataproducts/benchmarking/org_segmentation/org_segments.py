# Databricks notebook source
# MAGIC %md
# MAGIC # Org Segments
# MAGIC This notebook calculates org attributes, joins EU org attribute data, and runs K-Means clustering to assign cohorts to orgs.
# MAGIC
# MAGIC Running this module will result in the following output:
# MAGIC * `playground.org_attributes`
# MAGIC * `playground.org_segments_v2`
# MAGIC * `playground.org_segments_v2_prev`
# MAGIC
# MAGIC Note: These are temp tables that you should save into another table to persist.

# COMMAND ----------

#############################################################
# Org Attributes
# Calculate org attributes based on past 2 months of data.
#
# Playground: https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/1416880240387789/command/3121743340115561
#############################################################

# COMMAND ----------

# MAGIC %sql
# MAGIC -- https://samsaradev.atlassian.net/browse/DP-210
# MAGIC -- We want to separately classify outlier makes as non passenger vehicles
# MAGIC create or replace temporary view non_passenger_devices as (
# MAGIC select id from productsdb.devices where LOWER(make) like '%isuzu%' or LOWER(make) like '%hino%' or LOWER(make) like '%byd%'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fleet composition data to get num passenger vehicles per org
# MAGIC create or replace temporary view vehicle_cable_data_pre as
# MAGIC   select
# MAGIC    ad.org_id,
# MAGIC    ad.device_id,
# MAGIC    max((c.time, c.value.int_value)).int_value as cable_id
# MAGIC   from dataprep.active_devices as ad
# MAGIC   left join kinesisstats.osdobdcableid as c on
# MAGIC     c.org_id = ad.org_id and
# MAGIC     c.object_id = ad.device_id and
# MAGIC     c.date = ad.date and
# MAGIC     c.value.is_databreak = 'false' and
# MAGIC     c.value.is_end = 'false'and
# MAGIC     c.date >= add_months(current_date(),-2)
# MAGIC    where  ad.date >= add_months(current_date(),-2)
# MAGIC    group by ad.org_id, ad.device_id;
# MAGIC
# MAGIC
# MAGIC -- join with non_passenger_devices to alter cable_id value to non passenger identifier
# MAGIC create or replace temp view vehicle_cable_data as
# MAGIC select * from vehicle_cable_data_pre
# MAGIC where device_id not in (select * from non_passenger_devices)
# MAGIC union
# MAGIC select
# MAGIC   org_id,
# MAGIC   device_id,
# MAGIC   -1 as cable_id
# MAGIC from vehicle_cable_data_pre
# MAGIC where device_id in (select * from non_passenger_devices);
# MAGIC
# MAGIC -- Get org # active devices (VGs) past 2 month
# MAGIC -- Used for calculating fleet size
# MAGIC create or replace temporary view num_active_vg_org as
# MAGIC (
# MAGIC   select
# MAGIC     ad.org_id,
# MAGIC     count(distinct case when trip_count is not null and trip_count <> 0 then ad.device_id end) as unique_active_all_vehicles,
# MAGIC     count(distinct case when trip_count is not null and trip_count <> 0 and vc.cable_id = 4 then ad.device_id end) as unique_active_passenger_vehicles
# MAGIC   from dataprep.active_devices ad
# MAGIC   left join productsdb.gateways gw on
# MAGIC     gw.org_id = ad.org_id and
# MAGIC     gw.device_id = ad.device_id
# MAGIC   left join clouddb.organizations o on
# MAGIC     o.id = ad.org_id
# MAGIC   left join vehicle_cable_data vc on
# MAGIC     vc.org_id = ad.org_id and
# MAGIC     vc.device_id = ad.device_id
# MAGIC   where
# MAGIC     o.internal_type != 1 and
# MAGIC     o.quarantine_enabled != 1 and
# MAGIC     gw.product_id in (7,24,17,35) and
# MAGIC     ad.date >= add_months(current_date(),-2)
# MAGIC   group by
# MAGIC     ad.org_id
# MAGIC );
# MAGIC
# MAGIC -- Aggegate driving pattern data for past month
# MAGIC -- Total trip distance and number of trips per org as proxies for driving pattern
# MAGIC create or replace temp view fleet_driving_patterns as
# MAGIC select
# MAGIC   org_id,
# MAGIC   sum(proto.trip_distance.distance_meters) * 0.000621371 as total_trip_distance_miles,
# MAGIC   count(trips.device_id) as num_trips
# MAGIC from trips2db_shards.trips trips
# MAGIC where date >= add_months(current_date(),-2)
# MAGIC and (proto.end.time-proto.start.time)<(24*60*60*1000) /*Filter out very long trips*/
# MAGIC and proto.start.time != proto.end.time /* legit trips only */
# MAGIC group by org_id;
# MAGIC
# MAGIC --Get Trips
# MAGIC create or replace temporary view device_trips as (
# MAGIC   select
# MAGIC     org_id,
# MAGIC     COALESCE(trips.proto.trip_distance.distance_meters, 0)*0.000621371 as distance_miles,
# MAGIC   case when (upper(trips.proto.start.place.city) in ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHILADELPHIA', 'PHOENIX', 'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 'INDIANAPOLIS', 'JACKSONVILLE', 'SAN FRANCISCO', 'COLUMBUS', 'CHARLOTTE', 'FORT WORTH', 'DETROIT', 'EL PASO', 'MEMPHIS', 'SEATTLE', 'DENVER', 'WASHINGTON', 'BOSTON', 'NASHVILLE', 'BALTIMORE', 'OKLAHOMA CITY', 'LOUISVILLE', 'PORTLAND', 'LAS VEGAS', 'MILWAUKEE', 'ALBUQUERQUE', 'TUCSON', 'FRESNO', 'SACRAMENTO', 'LONG BEACH', 'KANSAS CITY', 'MESA', 'VIRGINIA BEACH', 'ATLANTA', 'COLORADO SPRINGS', 'OMAHA', 'RALEIGH', 'MIAMI', 'OAKLAND', 'MINNEAPOLIS', 'TULSA', 'CLEVELAND', 'WICHITA', 'ARLINGTON', 'NEW ORLEANS', 'BAKERSFIELD', 'TAMPA', 'HONOLULU', 'AURORA', 'ANAHEIM', 'SANTA ANA', 'ST. LOUIS', 'RIVERSIDE', 'CORPUS CHRISTI', 'LEXINGTON-FAYETTE', 'PITTSBURGH', 'ANCHORAGE', 'STOCKTON', 'CINCINNATI', 'ST. PAUL', 'TOLEDO', 'GREENSBORO', 'NEWARK', 'PLANO', 'HENDERSON', 'LINCOLN', 'BUFFALO', 'JERSEY CITY', 'CHULA VISTA', 'FORT WAYNE', 'ORLANDO', 'ST. PETERSBURG', 'CHANDLER', 'LAREDO', 'NORFOLK', 'DURHAM', 'MADISON', 'LUBBOCK', 'IRVINE', 'WINSTON-SALEM', 'GLENDALE', 'GARLAND', 'HIALEAH', 'RENO', 'CHESAPEAKE', 'GILBERT', 'BATON ROUGE', 'IRVING', 'SCOTTSDALE', 'NORTH LAS VEGAS', 'FREMONT', 'BOISE CITY', 'RICHMOND', 'SAN BERNARDINO')) or
# MAGIC    (upper(trips.proto.end.place.city) in ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHILADELPHIA', 'PHOENIX', 'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 'INDIANAPOLIS', 'JACKSONVILLE', 'SAN FRANCISCO', 'COLUMBUS', 'CHARLOTTE', 'FORT WORTH', 'DETROIT', 'EL PASO', 'MEMPHIS', 'SEATTLE', 'DENVER', 'WASHINGTON', 'BOSTON', 'NASHVILLE', 'BALTIMORE', 'OKLAHOMA CITY', 'LOUISVILLE', 'PORTLAND', 'LAS VEGAS', 'MILWAUKEE', 'ALBUQUERQUE', 'TUCSON', 'FRESNO', 'SACRAMENTO', 'LONG BEACH', 'KANSAS CITY', 'MESA', 'VIRGINIA BEACH', 'ATLANTA', 'COLORADO SPRINGS', 'OMAHA', 'RALEIGH', 'MIAMI', 'OAKLAND', 'MINNEAPOLIS', 'TULSA', 'CLEVELAND', 'WICHITA', 'ARLINGTON', 'NEW ORLEANS', 'BAKERSFIELD', 'TAMPA', 'HONOLULU', 'AURORA', 'ANAHEIM', 'SANTA ANA', 'ST. LOUIS', 'RIVERSIDE', 'CORPUS CHRISTI', 'LEXINGTON-FAYETTE', 'PITTSBURGH', 'ANCHORAGE', 'STOCKTON', 'CINCINNATI', 'ST. PAUL', 'TOLEDO', 'GREENSBORO', 'NEWARK', 'PLANO', 'HENDERSON', 'LINCOLN', 'BUFFALO', 'JERSEY CITY', 'CHULA VISTA', 'FORT WAYNE', 'ORLANDO', 'ST. PETERSBURG', 'CHANDLER', 'LAREDO', 'NORFOLK', 'DURHAM', 'MADISON', 'LUBBOCK', 'IRVINE', 'WINSTON-SALEM', 'GLENDALE', 'GARLAND', 'HIALEAH', 'RENO', 'CHESAPEAKE', 'GILBERT', 'BATON ROUGE', 'IRVING', 'SCOTTSDALE', 'NORTH LAS VEGAS', 'FREMONT', 'BOISE CITY', 'RICHMOND', 'SAN BERNARDINO')) then 1 else 0 end as start_end_in_city -- if the started or ended in a top 40 city
# MAGIC   from trips2db_shards.trips trips
# MAGIC   where
# MAGIC    trips.date >= add_months(current_date(),-2) and
# MAGIC   (trips.proto.end.time - trips.proto.start.time) <= 24*60*60*1000 -- filter out long trips
# MAGIC );
# MAGIC
# MAGIC create or replace temporary view org_trip_city_count as
# MAGIC select
# MAGIC   org_id,
# MAGIC   count(1) as total_num_trips,
# MAGIC   sum(start_end_in_city) as num_trips_start_end_in_city,
# MAGIC   sum(start_end_in_city)/count(1)*100 as percent_trips_start_end_city
# MAGIC from device_trips
# MAGIC group by org_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combine everything into org attributes
# MAGIC create or replace temp view org_attributes as
# MAGIC select
# MAGIC   fdp.org_id,
# MAGIC    -- total_devices_trips is the number of unique vehciles that were active in the queried timeframe
# MAGIC    -- org 1: 5 veh drive 5 mi for 30 days, org 2: 5 veh drive 5 miles for 1 day
# MAGIC   (total_trip_distance_miles / unique_active_all_vehicles)/60    as distance_driven_miles_per_vehicle,
# MAGIC   total_trip_distance_miles / num_trips                          as trip_length,
# MAGIC
# MAGIC   unique_active_all_vehicles                                     as unique_active_vehicles,
# MAGIC   unique_active_passenger_vehicles/unique_active_all_vehicles    as percent_passenger,
# MAGIC   percent_trips_start_end_city                                   as percent_trips_city
# MAGIC from fleet_driving_patterns as fdp
# MAGIC left join num_active_vg_org as navo
# MAGIC   on fdp.org_id = navo.org_id
# MAGIC left join org_trip_city_count as otcc
# MAGIC   on fdp.org_id = otcc.org_id;
# MAGIC
# MAGIC drop table if exists playground.org_attributes;
# MAGIC create table if not exists playground.org_attributes as (
# MAGIC   select *
# MAGIC   from org_attributes
# MAGIC );

# COMMAND ----------

#########################
# Read in EU Cloud data
#########################

# COMMAND ----------

# EU org attributes data is replicated to our US s3 bucket so mount it and read the csv
try:
    dbutils.fs.mount(
        f"s3a://samsara-benchmarking-metrics", f"/mnt/samsara-benchmarking-metrics"
    )
except:
    print("samsara-benchmarking-metrics directory already mounted")

# dbutils.fs.ls("/mnt/samsara-benchmarking-metrics")

# COMMAND ----------

# Read in EU org attribute CSV
eu_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/mnt/samsara-benchmarking-metrics/org_attributes_eu/")
    .toPandas()
)

# Add a dummy column for percent_trips_city since we don't calculate this attribute for EU cloud customer orgs
# shape: (670, 6)
eu_df["percent_trips_city"] = 0
# eu_df.head()

# COMMAND ----------

#############################################################
# K-Means Clustering
# Assign cohorts to each org based on past 2 months data
#
# Playground: https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/2461119259878347/command/3121743340115563
#############################################################

# COMMAND ----------

import operator

# Helpers
import numpy as np
import pandas as pd
from scipy.stats import boxcox, zscore
import sklearn as sk
from sklearn.cluster import KMeans

# COMMAND ----------

query = """
select * from playground.org_attributes
where unique_active_vehicles > 0 and
distance_driven_miles_per_vehicle > 5 and
org_id not in (29664, 9547, 47846, 18883, 23975, 18892, 25869, 658, 14481)
"""

base_data_sdf = spark.sql(query)
base_data = base_data_sdf.toPandas()
# base_data.head()

# COMMAND ----------

# Union US cloud and EU cloud orgs so we can train our clustering algorithm using all data
base_data = pd.concat([base_data, eu_df])
base_data.index = range(0, len(base_data))
# base_data.tail()

# COMMAND ----------

# Primarily heavy duty orgs
temp = base_data.copy()
greater_than_0 = temp["percent_passenger"] >= 0
less_than_25 = temp["percent_passenger"] < 0.25
filtered_0 = temp[greater_than_0]
mixed_fleets_0_25 = filtered_0[less_than_25]
mixed_fleets_0_25 = mixed_fleets_0_25.dropna()
mixed_fleets_0_25.index = np.arange(0, len(mixed_fleets_0_25))

x = pd.Series(
    zscore(boxcox(mixed_fleets_0_25["distance_driven_miles_per_vehicle"], 0.5))
)
x = np.where(x.between(x.quantile(0), x.quantile(0.99)), x, None)
mixed_fleets_0_25["distance_driven_miles_per_vehicle"] = x

x = pd.Series(zscore(boxcox(mixed_fleets_0_25["trip_length"], 1)))
x = np.where(x.between(x.quantile(0), x.quantile(0.99)), x, None)
mixed_fleets_0_25["trip_length"] = x

mixed_fleets_0_25 = mixed_fleets_0_25.drop(["percent_passenger"], axis=1)
mixed_fleets_0_25 = mixed_fleets_0_25.drop(["unique_active_vehicles"], axis=1)
mixed_fleets_0_25 = mixed_fleets_0_25.drop(["percent_trips_city"], axis=1)
mixed_fleets_0_25 = mixed_fleets_0_25.dropna()

mixed_fleets_0_25 = mixed_fleets_0_25.set_index("org_id")

# Primarily heavy duty orgs clustering
kmeans_0_25 = KMeans(
    n_clusters=4,
    init="k-means++",
    max_iter=10000,
    random_state=0,
    n_init=100,
    n_jobs=-1,
)
kmeans_0_25.fit(mixed_fleets_0_25)

kmeans_clusters = kmeans_0_25.fit_predict(mixed_fleets_0_25)
kmeans_data = mixed_fleets_0_25.copy()
kmeans_data["segment"] = kmeans_clusters

mixed_fleets_0_25["fleet_comp"] = 0
mixed_fleets_0_25["fleet_comp_segment"] = kmeans_clusters


# Mixed fleets
greater_than_25 = base_data["percent_passenger"] >= 0.25
less_than_75 = base_data["percent_passenger"] < 0.75
filtered_25 = base_data[greater_than_25].copy()
mixed_fleets_25_75 = filtered_25[less_than_75].dropna()
mixed_fleets_25_75.index = np.arange(0, len(mixed_fleets_25_75))

x = pd.Series(
    zscore(boxcox(mixed_fleets_25_75["distance_driven_miles_per_vehicle"], 0))
)
x = pd.Series(np.where(x.between(x.quantile(0), x.quantile(0.99)), x, None))
mixed_fleets_25_75["distance_driven_miles_per_vehicle"] = x

x = pd.Series(zscore(boxcox(mixed_fleets_25_75["trip_length"], 0)))
x = pd.Series(np.where(x.between(x.quantile(0.005), x.quantile(0.99)), x, None))

mixed_fleets_25_75["trip_length"] = x

mixed_fleets_25_75 = mixed_fleets_25_75.drop(["percent_passenger"], axis=1)
mixed_fleets_25_75 = mixed_fleets_25_75.drop(["percent_trips_city"], axis=1)
mixed_fleets_25_75 = mixed_fleets_25_75.drop(["unique_active_vehicles"], axis=1)
mixed_fleets_25_75 = mixed_fleets_25_75.dropna()

mixed_fleets_25_75 = mixed_fleets_25_75.set_index("org_id")

# Mixed fleets clustering
kmeans_25_75 = KMeans(
    n_clusters=4,
    init="k-means++",
    max_iter=10000,
    random_state=0,
    n_init=100,
    n_jobs=-1,
)
kmeans_25_75.fit(mixed_fleets_25_75)

kmeans_clusters = kmeans_25_75.fit_predict(mixed_fleets_25_75)
kmeans_data = mixed_fleets_25_75.copy()
kmeans_data["segment"] = kmeans_clusters

mixed_fleets_25_75["fleet_comp"] = 1
mixed_fleets_25_75["fleet_comp_segment"] = kmeans_clusters


# Primarily passenger
greater_than_75 = base_data["percent_passenger"] >= 0.75
less_than_1 = base_data["percent_passenger"] <= 1
filtered_75 = base_data[greater_than_75].copy()
mixed_fleets_75_1 = filtered_75[less_than_1].dropna()
mixed_fleets_75_1.index = np.arange(0, len(mixed_fleets_75_1))
mixed_fleets_75_1.head()

x = pd.Series(zscore(boxcox(mixed_fleets_75_1["distance_driven_miles_per_vehicle"], 0)))
x = pd.Series(np.where(x.between(x.quantile(0), x.quantile(0.98)), x, None))
mixed_fleets_75_1["distance_driven_miles_per_vehicle"] = x

x = pd.Series(zscore(boxcox(mixed_fleets_75_1["trip_length"], 0)))
x = pd.Series(np.where(x.between(x.quantile(0.01), x.quantile(0.99)), x, None))
mixed_fleets_75_1["trip_length"] = x

mixed_fleets_75_1 = mixed_fleets_75_1.drop(["percent_passenger"], axis=1)
mixed_fleets_75_1 = mixed_fleets_75_1.drop(["percent_trips_city"], axis=1)
mixed_fleets_75_1 = mixed_fleets_75_1.drop(["unique_active_vehicles"], axis=1)
mixed_fleets_75_1 = mixed_fleets_75_1.set_index("org_id")
mixed_fleets_75_1 = mixed_fleets_75_1.dropna()

# Primarily passenger clustering
kmeans_75_1 = KMeans(
    n_clusters=4,
    init="k-means++",
    max_iter=10000,
    random_state=0,
    n_init=100,
    n_jobs=-1,
)
kmeans_75_1.fit(mixed_fleets_75_1)

kmeans_clusters = kmeans_75_1.fit_predict(mixed_fleets_75_1)
kmeans_data = mixed_fleets_75_1.copy()
kmeans_data["segment"] = kmeans_clusters

mixed_fleets_75_1["fleet_comp"] = 2
mixed_fleets_75_1["fleet_comp_segment"] = kmeans_clusters

# COMMAND ----------

# concats dataframes together
fleet_comp_df = pd.concat([mixed_fleets_0_25, mixed_fleets_25_75, mixed_fleets_75_1])
fleet_comp_df.head()

working_copy = fleet_comp_df.copy()
working_copy = working_copy.drop("distance_driven_miles_per_vehicle", axis=1)
working_copy = working_copy.drop("trip_length", axis=1)

working_copy["cohort_id"] = working_copy["fleet_comp"].astype("str") + working_copy[
    "fleet_comp_segment"
].astype("str")

working_copy["org_id"] = working_copy.index
working_copy.index = range(0, len(working_copy))

working_copy_sdf = spark.createDataFrame(working_copy.dropna())
working_copy_sdf.createOrReplaceTempView("working_copy")

# COMMAND ----------

###################################################################################################
# Relabel using prod org segments
# K-Means clustering is non-deterministic so we apply a similarity algorithm based on majority
# similar orgs matched within each cohort between original and new to get the correct label
###################################################################################################

# COMMAND ----------


# construct comparison cohorts which contains the list of orgs for each cohort
# ie.
# {
# '00': [21, 567, 6800 ...],
# '01': [...],
# '...': [...]
# }
def construct_comparison_cohorts(org_cohort_df):
    cohort_ids = [
        "00",
        "01",
        "02",
        "03",
        "10",
        "11",
        "12",
        "13",
        "20",
        "21",
        "22",
        "23",
    ]
    comparison_cohorts = {}
    for cid in cohort_ids:
        comparison_cohorts[cid] = org_cohort_df[org_cohort_df["cohort_id"] == cid]

    return comparison_cohorts


# Find similar cohorts by merging og cohort org list with the org list of the new cohort.
# We take the max of this output later to get the most similar cohort
def find_most_similar_cohort(og_cohort, comparison_cohorts):
    #   returns dict of compared cohorts with the count of similar org_ids
    results = {}
    results_raw_count = {}
    for key in comparison_cohorts:
        curr_comparison_cohort = comparison_cohorts[key]
        merge_df = og_cohort.merge(
            curr_comparison_cohort, on="org_id", how="outer", indicator=True
        )
        results_raw_count[key] = merge_df.groupby("_merge").count()["org_id"].both

        removed_org_ids = merge_df[merge_df["_merge"] == "left_only"]
        added_org_ids = merge_df[merge_df["_merge"] == "right_only"]

        results[key] = {
            "left_df_count": og_cohort["org_id"].count(),
            "right_df_count": comparison_cohorts[key]["org_id"].count(),
            "left_only": merge_df.groupby("_merge").count()["org_id"].left_only,
            "right_only": merge_df.groupby("_merge").count()["org_id"].right_only,
            "both": merge_df.groupby("_merge").count()["org_id"].both,
            "removed_org_ids": merge_df[merge_df["_merge"] == "left_only"],
            "added_org_ids": merge_df[merge_df["_merge"] == "right_only"],
        }

    return results, results_raw_count


def match_cohort_results(org_cohort_run1, org_cohort_run2):
    raw_results = {}
    results = {}
    comparison_cohorts1 = construct_comparison_cohorts(org_cohort_run1)
    comparison_cohorts2 = construct_comparison_cohorts(org_cohort_run2)

    # Create a list that contains the similar orgs for each cohort id between the og and new data.
    # We take the max below to find the most simlilar org
    for cohort_id in comparison_cohorts1:
        comparisons, comparison_counts = find_most_similar_cohort(
            comparison_cohorts1[cohort_id], comparison_cohorts2
        )
        raw_results[cohort_id] = comparisons
        results[cohort_id] = comparison_counts

    cohort_ids = []
    new_cohort_id = []
    old_cohort_count = []
    new_cohort_count = []
    orgs_removed_count = []
    orgs_removed_reassigned_cohort = []
    orgs_removed_dropped_from_cohorts = []
    orgs_added_count = []
    orgs_added_reassigned_cohort = []
    orgs_added_previously_no_cohort = []
    orgs_same_count = []
    orgs_same_percentage = []

    raw_cohort_results = {}
    for cohort_id in results:
        # Note: PySpark lib overwrites the max function which causes error
        most_similar_org = max(results[cohort_id].items(), key=operator.itemgetter(1))[
            0
        ]  # most similar cohort id in new data
        new_cohort_results = raw_results[cohort_id][most_similar_org]

        (
            removed_reassigned_orgs,
            removed_orgs,
            removed_reassigned_orgs_df,
        ) = compare_removed_cohorts_to_new_org_cohorts(
            new_cohort_results["removed_org_ids"], org_cohort_run2
        )
        (
            added_reassigned_orgs,
            orgs_previously_without_cohort,
            added_reassigned_orgs_df,
        ) = compare_added_cohorts_to_old_org_cohorts(
            new_cohort_results["added_org_ids"], org_cohort_run1
        )

        #   Add to dict with all supporting data
        new_cohort_results["removed_org_ids"] = removed_reassigned_orgs_df
        new_cohort_results["added_org_ids"] = added_reassigned_orgs_df
        raw_cohort_results[cohort_id] = new_cohort_results

        cohort_ids.append(cohort_id)
        new_cohort_id.append(most_similar_org)
        old_cohort_count.append(new_cohort_results["left_df_count"])
        new_cohort_count.append(new_cohort_results["right_df_count"])
        orgs_removed_count.append(new_cohort_results["left_only"])
        orgs_removed_reassigned_cohort.append(removed_reassigned_orgs)
        orgs_removed_dropped_from_cohorts.append(removed_orgs)
        orgs_added_count.append(new_cohort_results["right_only"])
        orgs_added_reassigned_cohort.append(added_reassigned_orgs)
        orgs_added_previously_no_cohort.append(orgs_previously_without_cohort)
        orgs_same_count.append(new_cohort_results["both"])
        orgs_same_percentage.append(
            (new_cohort_results["both"] / new_cohort_results["left_df_count"]) * 100
        )

    result_summary = pd.DataFrame(
        data={
            "cohort_id": cohort_ids,
            "new_cohort_id": new_cohort_id,
            "prev_count": old_cohort_count,
            "new_count": new_cohort_count,
            #     "orgs_removed_count": orgs_removed_count,
            "reassigned_to_another_cohort": orgs_removed_reassigned_cohort,
            #     "reassigned_to_orgs": list(new_cohort_results['removed_org_ids']),
            "orgs_dropped_no_assignment": orgs_removed_dropped_from_cohorts,
            #     "orgs_added_count": orgs_added_count,
            "reassigned_to_this_cohort": orgs_added_reassigned_cohort,
            #     "reassigned_from_orgs": list(new_cohort_results['added_org_ids']),
            "orgs_added_previously_no_cohort": orgs_added_previously_no_cohort,
            #     "orgs_same_count": orgs_same_count,
            "orgs_same_percentage": orgs_same_percentage,
        }
    )

    relabel_mapping = pd.DataFrame(
        data={"original_cohort_id": cohort_ids, "flipped_cohort_id": new_cohort_id}
    )

    relabel_mapping = relabel_mapping.set_index("flipped_cohort_id").to_dict()[
        "original_cohort_id"
    ]

    return result_summary, raw_cohort_results, relabel_mapping


def compare_removed_cohorts_to_new_org_cohorts(removed_cohorts_df, new_org_cohorts):
    del removed_cohorts_df["_merge"]
    merge_df = removed_cohorts_df.merge(
        new_org_cohorts, on="org_id", how="left", indicator=True
    )
    orgs_reassigned_cohort = merge_df.groupby("_merge").count()["org_id"].both
    orgs_now_without_cohort = removed_cohorts_df.org_id.count() - orgs_reassigned_cohort
    del merge_df["cohort_id_y"]
    merge_df.columns = ["org_id", "origin_cohort", "dest_cohort", "_merge"]
    return orgs_reassigned_cohort, orgs_now_without_cohort, merge_df


def compare_added_cohorts_to_old_org_cohorts(added_cohorts_df, old_org_cohorts):
    del added_cohorts_df["_merge"]
    merge_df = old_org_cohorts.merge(
        added_cohorts_df, on="org_id", how="right", indicator=True
    )
    orgs_reassigned_cohort = merge_df.groupby("_merge").count()["org_id"].both
    orgs_previously_without_cohort = (
        added_cohorts_df.org_id.count() - orgs_reassigned_cohort
    )
    del merge_df["cohort_id_x"]
    merge_df.columns = ["org_id", "origin_cohort", "dest_cohort", "_merge"]
    return orgs_reassigned_cohort, orgs_previously_without_cohort, merge_df


# COMMAND ----------

# K-Means clustering is non-deterministic so we apply a similarity algorithm based on majority
# similar orgs matched within each cohort between original and new to get the correct label
prod_org_segments = spark.table("dataproducts.org_segments_v2").toPandas()
new_org_segments = spark.sql("select org_id, cohort_id from working_copy").toPandas()
_, _, relabel_mapping = match_cohort_results(prod_org_segments, new_org_segments)

# Apply relabeling
new_org_segments = new_org_segments.replace({"cohort_id": relabel_mapping})
new_org_segments_sdf = spark.createDataFrame(new_org_segments)
new_org_segments_sdf.createOrReplaceTempView("new_org_segments")

# COMMAND ----------

##########################
# Save tables
##########################

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save previous org segments to enable comparison stats
# MAGIC drop table if exists playground.org_segments_v2_prev;
# MAGIC create table if not exists playground.org_segments_v2_prev as (
# MAGIC   select * from playground.org_segments_v2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists playground.org_segments_v2;
# MAGIC create table if not exists playground.org_segments_v2 as (
# MAGIC   select * from new_org_segments
# MAGIC )

# COMMAND ----------

#################
# Cohort Stats
#################

# COMMAND ----------

# base_data_copy = base_data.copy()
# base_data_copy = base_data_copy.set_index("org_id")
# working_copy = working_copy.set_index("org_id")
# working_copy.head()

# COMMAND ----------

# Num orgs in each cohort
# working_copy.groupby(["cohort_id"]).count()

# COMMAND ----------

# Rejoin orgs with base data
# cohort_base_data = pd.merge(
#     base_data_copy, working_copy, left_index=True, right_index=True
# )
# cohort_base_data.head()

# COMMAND ----------

# cohort_base_data.groupby("cohort_id").describe()
