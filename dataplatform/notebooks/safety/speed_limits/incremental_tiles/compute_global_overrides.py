# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

"""
Compute Global Speed Limit Overrides

This notebook computes global speed limit overrides based on customer consensus.
A global override is computed when:
1. 2+ non-internal customers have overridden the same way_id with the same speed limit
2. The override value is within 5 mph delta of the p85 speed from the confidence table

Output table: safety_map_data.global_overrides_YYYYMMDD
"""

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

ARG_OSM_DATASET_VERSION = "osm_dataset_version"
"""
* ARG_OSM_DATASET_VERSION:
String OSM dataset version (format: YYYYMMDD) - used to find the confidence table
"""
dbutils.widgets.text(ARG_OSM_DATASET_VERSION, "")
osm_dataset_version = dbutils.widgets.get(ARG_OSM_DATASET_VERSION)
print(f"{ARG_OSM_DATASET_VERSION}: {osm_dataset_version}")

# COMMAND ----------

# Constants
DELTA_THRESHOLD_MPH = 5  # Maximum delta between override and p85 speed
MIN_CONSENSUS_ORGS = 2  # Minimum number of orgs for consensus
MILLIKNOTS_TO_MPH = 0.00115078  # Conversion factor

# COMMAND ----------


def find_confidence_table_by_osm_version(osm_dataset_version: str) -> str:
    """
    Find the confidence table based on OSM dataset version.
    The confidence table name follows the pattern:
    safety_map_data.{resolved_table_name}_confidence

    e.g. safety_map_data.osm_can_20251215_eur_20251215_mex_20251215_usa_20251215__tomtom_202512__resolved_speed_limits_confidence
    """
    resolved_table_name = find_tomtom_osm_resolved_speed_limit_table_by_version(
        osm_dataset_version
    )
    # Remove schema prefix if present
    if "." in resolved_table_name:
        resolved_table_name = resolved_table_name.split(".")[-1]

    confidence_table = f"safety_map_data.{resolved_table_name}_confidence"
    return confidence_table


# COMMAND ----------


def compute_global_overrides(
    osm_dataset_version: str,
    delta_threshold_mph: float = DELTA_THRESHOLD_MPH,
    min_consensus_orgs: int = MIN_CONSENSUS_ORGS,
):
    """
    Compute global speed limit overrides based on customer consensus.

    Logic:
    1. Filter customer overrides (org_id > 0) from non-internal orgs (internal_type = 0)
    2. Find way_ids where 2+ customers have the same override value
    3. Join with p85 confidence data to calculate delta
    4. Filter to overrides where delta < 5 mph
    5. For each way_id, pick the override with lowest delta (highest consensus as tiebreaker)

    Returns DataFrame with columns:
    - way_id: OSM way ID
    - global_override_speed_limit: Speed limit in string format (e.g., "65 kph")
    - global_override_milliknots: Speed limit in milliknots
    - p85_speed: P85 speed from confidence table (mph)
    - delta_mph: Absolute delta between override and p85
    - num_orgs: Number of orgs with this consensus
    """

    # Find confidence table
    confidence_table = find_confidence_table_by_osm_version(osm_dataset_version)
    print(f"Using confidence table: {confidence_table}")

    if not DBX_TABLE.is_table_exists(confidence_table):
        print(f"WARNING: Confidence table {confidence_table} does not exist.")
        print("Returning empty DataFrame - no global overrides will be computed.")
        # Return empty DataFrame with expected schema
        return spark.createDataFrame(
            [],
            schema="way_id BIGINT, global_override_speed_limit STRING, global_override_milliknots INT, p85_speed DOUBLE, delta_mph DOUBLE, num_orgs INT",
        )

    # Step 1: Get non-internal org IDs (internal_type <> 1 excludes generic internal orgs)
    non_internal_orgs_df = spark.sql(
        """
        SELECT id as org_id 
        FROM clouddb.organizations 
        WHERE internal_type <> 1
    """
    )
    print(f"Found {non_internal_orgs_df.count()} non-internal organizations")

    # Step 2: Find customer overrides with consensus
    # Customer overrides have org_id > 0 (org_id = -1 is global/system)
    customer_overrides_df = spark.sql(
        """
        SELECT 
            way_id,
            override_speed_limit_milliknots,
            org_id
        FROM speedlimitsdb.speed_limit_overrides
        WHERE org_id > 0
    """
    )

    # Filter to only non-internal orgs
    customer_overrides_df = customer_overrides_df.join(
        non_internal_orgs_df, on="org_id", how="inner"
    )

    # Group by way_id and override value to find consensus
    consensus_df = (
        customer_overrides_df.groupBy("way_id", "override_speed_limit_milliknots")
        .agg(F.countDistinct("org_id").alias("num_orgs"))
        .filter(F.col("num_orgs") >= min_consensus_orgs)
    )

    print(
        f"Found {consensus_df.count()} way_id/override combinations with {min_consensus_orgs}+ org consensus"
    )

    # Step 3: Load confidence table and join
    confidence_df = spark.table(confidence_table).select(
        F.col("way_id"), F.col("p85_speed")
    )

    # Join consensus with confidence data
    with_p85_df = consensus_df.join(confidence_df, on="way_id", how="inner")

    # Step 4: Calculate delta and filter
    with_delta_df = with_p85_df.withColumn(
        "override_mph",
        F.round(F.col("override_speed_limit_milliknots") * F.lit(MILLIKNOTS_TO_MPH)),
    ).withColumn("delta_mph", F.abs(F.col("override_mph") - F.col("p85_speed")))

    # Filter to overrides within delta threshold
    filtered_df = with_delta_df.filter(F.col("delta_mph") < delta_threshold_mph)

    print(
        f"Found {filtered_df.count()} overrides within {delta_threshold_mph} mph of p85 speed"
    )

    # Step 5: For each way_id, pick the override with lowest delta
    # Use num_orgs DESC as tiebreaker (prefer higher consensus)
    window = Window.partitionBy("way_id").orderBy(
        F.col("delta_mph").asc(), F.col("num_orgs").desc()
    )

    ranked_df = filtered_df.withColumn("rank", F.row_number().over(window))
    best_override_df = ranked_df.filter(F.col("rank") == 1).drop("rank")

    # Convert to output format
    # Speed limit in kph string format to match existing max_speed_limit format
    result_df = (
        best_override_df.withColumn(
            "override_kph",
            F.round(F.col("override_speed_limit_milliknots") * F.lit(0.001852)).cast(
                "int"
            ),
        )
        .withColumn(
            "global_override_speed_limit",
            F.concat(F.col("override_kph"), F.lit(" kph")),
        )
        .withColumn(
            "global_override_milliknots",
            F.col("override_speed_limit_milliknots").cast("int"),
        )
        .select(
            "way_id",
            "global_override_speed_limit",
            "global_override_milliknots",
            "p85_speed",
            "delta_mph",
            "num_orgs",
        )
    )

    print(f"Final global overrides count: {result_df.count()}")
    return result_df


# COMMAND ----------

# Main execution
print("=" * 60)
print("Computing Global Speed Limit Overrides")
print("=" * 60)

global_overrides_df = compute_global_overrides(
    osm_dataset_version=osm_dataset_version,
    delta_threshold_mph=DELTA_THRESHOLD_MPH,
    min_consensus_orgs=MIN_CONSENSUS_ORGS,
)

# COMMAND ----------

# Save to output table
today_date = datetime.today().strftime("%Y%m%d")
output_table_name = f"safety_map_data.global_overrides_{today_date}"

print(f"Writing global overrides to {output_table_name}")

global_overrides_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(output_table_name)

print(
    f"✅ Successfully saved {global_overrides_df.count()} global overrides to {output_table_name}"
)

# COMMAND ----------

# Output the table name for downstream consumption
dbutils.notebook.exit(output_table_name)
