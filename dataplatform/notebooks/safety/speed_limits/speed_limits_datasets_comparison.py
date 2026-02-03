# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

ARG_PREVIOUS_SPEED_LIMIT_DATASET_TABLE_NAME = "previous_speed_limit_dataset_table_name"
ARG_CURRENT_SPEED_LIMIT_DATASET_TABLE_NAME = "current_speed_limit_dataset_table_name"
ARG_HISTOGRAM_DATASET_TABLE_NAME = "histogram_dataset_table_name"

# COMMAND ----------

"""
* ARG_PREVIOUS_SPEED_LIMIT_DATASET_TABLE_NAME:
String previous speed limit dataset name
"""
dbutils.widgets.text(ARG_PREVIOUS_SPEED_LIMIT_DATASET_TABLE_NAME, "")
previous_speed_limit_dataset_table_name = dbutils.widgets.get(
    ARG_PREVIOUS_SPEED_LIMIT_DATASET_TABLE_NAME
)
print(
    f"{ARG_PREVIOUS_SPEED_LIMIT_DATASET_TABLE_NAME}: {previous_speed_limit_dataset_table_name}"
)

"""
* ARG_CURRENT_SPEED_LIMIT_DATASET_TABLE_NAME:
String current speed limit dataset table name
"""
dbutils.widgets.text(ARG_CURRENT_SPEED_LIMIT_DATASET_TABLE_NAME, "")
current_speed_limit_dataset_table_name = dbutils.widgets.get(
    ARG_CURRENT_SPEED_LIMIT_DATASET_TABLE_NAME
)
print(
    f"{ARG_CURRENT_SPEED_LIMIT_DATASET_TABLE_NAME}: {current_speed_limit_dataset_table_name}"
)

"""
* ARG_HISTOGRAM_DATASET_TABLE_NAME:
String histogram dataset table name
"""
dbutils.widgets.text(ARG_HISTOGRAM_DATASET_TABLE_NAME, "")
histogram_dataset_table_name = dbutils.widgets.get(ARG_HISTOGRAM_DATASET_TABLE_NAME)
print(f"{ARG_HISTOGRAM_DATASET_TABLE_NAME}: {histogram_dataset_table_name}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

# COMMAND ----------

previous_speed_limit_dataset = spark.table(previous_speed_limit_dataset_table_name)
current_speed_limit_dataset = spark.table(current_speed_limit_dataset_table_name)
histogram_dataset = spark.table(histogram_dataset_table_name)

# COMMAND ----------


def compute_speed_limit_confidence(
    speed_limit_dataset: DataFrame,
    histogram_dataset: DataFrame,
    dataset_name: str,
    tolerance: int = 10,
) -> DataFrame:
    """
    Compute confidence scores for speed limit dataset using histogram data.

    Args:
        speed_limit_dataset: DataFrame containing speed limit data with columns:
            - osm_way_id: way ID
            - tomtom_state_code: state code
            - osm_tomtom_updated_passenger_limit: speed limit string (e.g., "65 mph")
        histogram_dataset: DataFrame containing histogram data with columns:
            - way_id: way ID
            - speed_histogram: array of speed counts (array_size is always 100)
        dataset_name: Name of the dataset for output table naming
        tolerance: Tolerance in mph for confidence calculation (default: 5)

    Returns:
        DataFrame with confidence scores containing:
            - way_id: way ID
            - speed_limit_mph: speed limit in mph
            - confidence: confidence score (0-1)
            - state: state code
            - updated_at: timestamp
    """

    # Process speed limits - extract speed values and convert to mph
    limits_df = (
        speed_limit_dataset.withColumn(
            "limit_value",
            F.split(F.col("osm_tomtom_updated_passenger_limit"), " ")
            .getItem(0)
            .cast("double"),
        )
        .withColumn(
            "limit_unit",
            F.lower(
                F.split(F.col("osm_tomtom_updated_passenger_limit"), " ").getItem(1)
            ),
        )
        .withColumn(
            "speed_limit_mph",
            F.when(F.col("limit_unit") == "mph", F.col("limit_value"))
            .when(
                F.col("limit_unit").isin("kph", "km/h"),
                F.col("limit_value") * F.lit(0.621371),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("speed_limit_mph", F.round("speed_limit_mph").cast("int"))
        .select(
            F.col("osm_way_id").alias("way_id"), "speed_limit_mph", "tomtom_state_code"
        )
    )

    # Aggregate histograms per way_id (element-wise sum of arrays)
    # Array size is always 100 for histogram datasets
    array_size = 100
    zero_array = F.array(*[F.lit(0).cast("bigint") for _ in range(array_size)])
    agg_hist_df = histogram_dataset.groupBy("way_id").agg(
        F.aggregate(
            F.collect_list("speed_histogram"),
            zero_array,
            lambda acc, x: F.zip_with(acc, x, lambda a, b: a + b),
        ).alias("agg_hist")
    )

    # Join with limits (inner)
    df = agg_hist_df.join(limits_df, on="way_id", how="inner")

    # Confidence score within ±tolerance mph of posted limit
    df = df.withColumn(
        "total",
        F.aggregate("agg_hist", F.lit(0).cast("bigint"), lambda acc, x: acc + x),
    )

    df = (
        df.withColumn(
            "lower_idx",
            F.greatest(F.col("speed_limit_mph") - F.lit(tolerance), F.lit(0)),
        )
        .withColumn(
            "upper_idx",
            F.least(F.col("speed_limit_mph") + F.lit(tolerance), F.lit(array_size - 1)),
        )
        .withColumn("upper_idx", F.greatest(F.col("upper_idx"), F.col("lower_idx")))
    )

    df = df.withColumn(
        "window_slice",
        F.slice(
            "agg_hist",
            F.col("lower_idx") + F.lit(1),
            F.greatest(F.col("upper_idx") - F.col("lower_idx") + F.lit(1), F.lit(0)),
        ),
    ).withColumn(
        "match_count",
        F.aggregate("window_slice", F.lit(0).cast("bigint"), lambda acc, x: acc + x),
    )

    result_df = df.withColumn(
        "confidence",
        F.when(F.col("total") > 0, F.col("match_count") / F.col("total")).otherwise(
            F.lit(None)
        ),
    ).select(
        F.col("way_id"),
        F.col("speed_limit_mph"),
        F.col("confidence"),
        F.col("tomtom_state_code").alias("state"),
        F.current_timestamp().alias("updated_at"),
    )

    return result_df


# COMMAND ----------


def compute_speed_limit_confidence_using_p85_speed_value(
    speed_limit_dataset: DataFrame,
    histogram_dataset: DataFrame,
) -> DataFrame:
    """
    Compute confidence scores for speed limit dataset using histogram data.
    """
    from pyspark.sql.window import Window as W

    # If your bins aren't 1 mph per index, adjust these:
    BIN_SIZE = 1.0  # mph per bin
    BIN_OFFSET = 0.0  # mph of index 0

    # Flatten histogram -> (way_id, speed_bin, n), then sum across all rows for each way_id
    bins = (
        histogram_dataset.select(
            "way_id", F.posexplode_outer("speed_histogram").alias("speed_bin", "n")
        )
        .where(F.col("n").isNotNull())
        .groupBy("way_id", "speed_bin")
        .agg(F.sum("n").cast("long").alias("n"))
    )

    # Running total per way_id ordered by speed_bin
    w_ordered = W.partitionBy("way_id").orderBy("speed_bin")
    with_cum = (
        bins.withColumn("cum_n", F.sum("n").over(w_ordered))
        .withColumn("total_n", F.sum("n").over(W.partitionBy("way_id")))
        .where(F.col("total_n") > 0)
        .withColumn("threshold", F.col("total_n") * F.lit(0.85))
    )

    # First bin whose cumulative count meets/exceeds the 85% threshold
    p85_idx = (
        with_cum.where(F.col("cum_n") >= F.col("threshold"))
        .groupBy("way_id")
        .agg(F.min("speed_bin").alias("p85_bin"))
    )

    # Convert bin index -> speed value (center of bin, if needed adjust BIN_OFFSET/SIZE)
    p85_speed_values = p85_idx.withColumn(
        "p85_speed",
        (F.col("p85_bin") * F.lit(BIN_SIZE) + F.lit(BIN_OFFSET)).cast("double"),
    ).select("way_id", "p85_speed")

    T = 40.0  # mph: confidence goes to 0 when |p85 - limit| >= T

    limits = (
        speed_limit_dataset.withColumn(
            "limit_value",
            F.split(F.col("osm_tomtom_updated_passenger_limit"), " ")
            .getItem(0)
            .cast("double"),
        )
        .withColumn(
            "limit_unit",
            F.lower(
                F.split(F.col("osm_tomtom_updated_passenger_limit"), " ").getItem(1)
            ),
        )
        .withColumn(
            "speed_limit_mph",
            F.when(F.col("limit_unit") == "mph", F.col("limit_value"))
            .when(
                F.col("limit_unit").isin("kph", "km/h"),
                F.col("limit_value") * F.lit(0.621371),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("speed_limit_mph", F.round("speed_limit_mph").cast("int"))
        .select(
            F.col("osm_way_id").alias("way_id"),
            "speed_limit_mph",
            F.col("tomtom_state_code").alias("state"),
        )
    )

    result = (
        p85_speed_values.join(limits, "way_id", "inner")
        .withColumn("diff", F.abs(F.col("p85_speed") - F.col("speed_limit_mph")))
        .withColumn("confidence_raw", 1 - (F.col("diff") / F.lit(T)))
        .withColumn(
            "confidence",
            F.greatest(F.lit(0.0), F.least(F.col("confidence_raw"), F.lit(1.0))),
        )
        .select(
            "way_id",
            "speed_limit_mph",
            "p85_speed",
            "confidence",
            "state",
            F.current_timestamp().alias("updated_at"),
        )
    )

    return result


# COMMAND ----------


def compare_speed_limit_datasets(
    current_dataset: DataFrame,
    previous_dataset: DataFrame,
    current_table_name: str,
    previous_table_name: str,
) -> DataFrame:
    """
    Compare current and previous speed limit datasets to analyze changes.

    Args:
        current_dataset: Current speed limit dataset DataFrame
        previous_dataset: Previous speed limit dataset DataFrame
        current_table_name: Name of current table for output naming
        previous_table_name: Name of previous table for output naming

    Returns:
        DataFrame with comparison results containing:
            - way_id: way ID
            - current_speed_limit_mph: speed limit in current dataset
            - previous_speed_limit_mph: speed limit in previous dataset (null if missing)
            - change_type: "new", "increased", "decreased", "same", or "removed"
            - speed_difference: difference in speed limits (current - previous)
    """

    # Process current dataset - extract speed values and convert to mph
    current_processed = (
        current_dataset.withColumn(
            "limit_value",
            F.split(F.col("osm_tomtom_updated_passenger_limit"), " ")
            .getItem(0)
            .cast("double"),
        )
        .withColumn(
            "limit_unit",
            F.lower(
                F.split(F.col("osm_tomtom_updated_passenger_limit"), " ").getItem(1)
            ),
        )
        .withColumn(
            "speed_limit_mph",
            F.when(F.col("limit_unit") == "mph", F.col("limit_value"))
            .when(
                F.col("limit_unit").isin("kph", "km/h"),
                F.col("limit_value") * F.lit(0.621371),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("speed_limit_mph", F.round("speed_limit_mph").cast("int"))
        .select(
            F.col("osm_way_id").alias("way_id"),
            F.col("speed_limit_mph").alias("current_speed_limit_mph"),
            F.col("tomtom_state_code"),
            F.col("tomtom_country_code"),
        )
    )

    # Process previous dataset - extract speed values and convert to mph
    previous_processed = (
        previous_dataset.withColumn(
            "limit_value",
            F.split(F.col("osm_tomtom_updated_passenger_limit"), " ")
            .getItem(0)
            .cast("double"),
        )
        .withColumn(
            "limit_unit",
            F.lower(
                F.split(F.col("osm_tomtom_updated_passenger_limit"), " ").getItem(1)
            ),
        )
        .withColumn(
            "speed_limit_mph",
            F.when(F.col("limit_unit") == "mph", F.col("limit_value"))
            .when(
                F.col("limit_unit").isin("kph", "km/h"),
                F.col("limit_value") * F.lit(0.621371),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("speed_limit_mph", F.round("speed_limit_mph").cast("int"))
        .select(
            F.col("osm_way_id").alias("way_id"),
            F.col("speed_limit_mph").alias("previous_speed_limit_mph"),
            F.col("tomtom_state_code"),
            F.col("tomtom_country_code"),
        )
    )

    # Full outer join to get all way_ids from both datasets
    comparison_df = (
        current_processed.alias("current")
        .join(
            previous_processed.alias("previous"),
            current_processed["way_id"] == previous_processed["way_id"],
            "full_outer",
        )
        .select(
            F.coalesce(F.col("current.way_id"), F.col("previous.way_id")).alias(
                "way_id"
            ),
            F.col("current.current_speed_limit_mph"),
            F.col("previous.previous_speed_limit_mph"),
            F.coalesce(
                F.col("current.tomtom_state_code"), F.col("previous.tomtom_state_code")
            ).alias("state"),
            F.coalesce(
                F.col("current.tomtom_country_code"),
                F.col("previous.tomtom_country_code"),
            ).alias("country"),
        )
    )

    # Determine change type and calculate differences
    result_df = (
        comparison_df.withColumn(
            "change_type",
            F.when(F.col("current_speed_limit_mph").isNull(), F.lit("removed"))
            .when(
                F.col("previous_speed_limit_mph").isNull()
                | (F.col("previous_speed_limit_mph") == 0),
                F.lit("new"),
            )
            .when(
                F.col("current_speed_limit_mph") > F.col("previous_speed_limit_mph"),
                F.lit("increased"),
            )
            .when(
                F.col("current_speed_limit_mph") < F.col("previous_speed_limit_mph"),
                F.lit("decreased"),
            )
            .otherwise(F.lit("same")),
        )
        .withColumn(
            "speed_difference",
            F.when(
                F.col("current_speed_limit_mph").isNotNull()
                & F.col("previous_speed_limit_mph").isNotNull(),
                F.col("current_speed_limit_mph") - F.col("previous_speed_limit_mph"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    return result_df


# COMMAND ----------

# Compute confidence for both current and previous datasets
print("Computing confidence for current dataset...")
current_confidence_df = compute_speed_limit_confidence_using_p85_speed_value(
    current_speed_limit_dataset,
    histogram_dataset,
)

print("Computing confidence for previous dataset...")
previous_confidence_df = compute_speed_limit_confidence_using_p85_speed_value(
    previous_speed_limit_dataset,
    histogram_dataset,
)

# Write results to Delta tables
print("Writing confidence results to Delta tables...")

# Extract table names from the full table paths
current_table_name = current_speed_limit_dataset_table_name.split(".")[-1]
previous_table_name = previous_speed_limit_dataset_table_name.split(".")[-1]

# Current dataset confidence
(
    current_confidence_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"safety_map_data.{current_table_name}_confidence")
)

# Previous dataset confidence
(
    previous_confidence_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"safety_map_data.{previous_table_name}_confidence")
)

print("✅ Successfully computed and saved confidence scores for both datasets")

# COMMAND ----------

# Perform comparison analysis between current and previous datasets
print("Performing comparison analysis between current and previous datasets...")
comparison_df = compare_speed_limit_datasets(
    current_speed_limit_dataset,
    previous_speed_limit_dataset,
    current_table_name,
    previous_table_name,
)

# Write comparison results to Delta table
comparison_table_name = (
    f"safety_map_data.{current_table_name}_vs_{previous_table_name}_comparison"
)
(
    comparison_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(comparison_table_name)
)

print(f"✅ Comparison results saved to {comparison_table_name}")

# Generate summary statistics
print("\n=== COMPARISON SUMMARY ===")

# Count by change type
change_summary = (
    comparison_df.groupBy("change_type").count().orderBy("count", ascending=False)
)

print("Changes by type:")
change_summary.show()

# Calculate total way_ids in previous dataset for percentage calculations
total_previous_way_ids = comparison_df.filter(
    F.col("previous_speed_limit_mph").isNotNull()
).count()
total_current_way_ids = comparison_df.filter(
    F.col("current_speed_limit_mph").isNotNull()
).count()

print(f"\nDataset sizes:")
print(f"Previous dataset: {total_previous_way_ids} way_ids")
print(f"Current dataset: {total_current_way_ids} way_ids")

# Count new way_ids (present in current but missing/zero in previous)
new_way_ids_count = comparison_df.filter(F.col("change_type") == "new").count()
new_pct = (
    (new_way_ids_count / total_previous_way_ids * 100)
    if total_previous_way_ids > 0
    else 0
)
print(
    f"\nNew way_ids with speed limits: {new_way_ids_count} ({new_pct:.1f}% of previous dataset)"
)

# Count removed way_ids (present in previous but missing in current)
removed_way_ids_count = comparison_df.filter(F.col("change_type") == "removed").count()
removed_pct = (
    (removed_way_ids_count / total_previous_way_ids * 100)
    if total_previous_way_ids > 0
    else 0
)
print(
    f"Removed way_ids: {removed_way_ids_count} ({removed_pct:.1f}% of previous dataset)"
)

# Count increased speed limits
increased_count = comparison_df.filter(F.col("change_type") == "increased").count()
increased_pct = (
    (increased_count / total_previous_way_ids * 100)
    if total_previous_way_ids > 0
    else 0
)
print(
    f"Increased speed limits: {increased_count} ({increased_pct:.1f}% of previous dataset)"
)

# Count decreased speed limits
decreased_count = comparison_df.filter(F.col("change_type") == "decreased").count()
decreased_pct = (
    (decreased_count / total_previous_way_ids * 100)
    if total_previous_way_ids > 0
    else 0
)
print(
    f"Decreased speed limits: {decreased_count} ({decreased_pct:.1f}% of previous dataset)"
)

# Count unchanged speed limits
same_count = comparison_df.filter(F.col("change_type") == "same").count()
same_pct = (
    (same_count / total_previous_way_ids * 100) if total_previous_way_ids > 0 else 0
)
print(f"Unchanged speed limits: {same_count} ({same_pct:.1f}% of previous dataset)")

print("\n✅ Comparison analysis completed!")

# COMMAND ----------

# State-level statistics
print("\n=== STATE-LEVEL STATISTICS ===")

# Filter comparison data to USA only
usa_comparison_df = comparison_df.filter(F.col("country") == "USA")

# Get all unique states from USA datasets
all_states = (
    usa_comparison_df.select("state")
    .distinct()
    .filter(F.col("state").isNotNull())
    .orderBy("state")
    .collect()
)

for state_row in all_states:
    state = state_row["state"]
    print(f"\n--- {state} ---")

    # Filter data for this state
    state_comparison = usa_comparison_df.filter(F.col("state") == state)
    state_current_confidence = current_confidence_df.filter(F.col("state") == state)
    state_previous_confidence = previous_confidence_df.filter(F.col("state") == state)

    # Total way_ids in current and previous datasets for this state
    total_current_way_ids = state_comparison.filter(
        F.col("current_speed_limit_mph").isNotNull()
    ).count()
    total_previous_way_ids = state_comparison.filter(
        F.col("previous_speed_limit_mph").isNotNull()
    ).count()

    # Change type statistics
    change_type_stats = state_comparison.groupBy("change_type").count().collect()

    change_type_dict = {row["change_type"]: row["count"] for row in change_type_stats}

    new_count = change_type_dict.get("new", 0)
    removed_count = change_type_dict.get("removed", 0)
    increased_count = change_type_dict.get("increased", 0)
    decreased_count = change_type_dict.get("decreased", 0)
    same_count = change_type_dict.get("same", 0)

    # Calculate percentages relative to previous dataset size
    new_pct = (
        (new_count / total_previous_way_ids * 100) if total_previous_way_ids > 0 else 0
    )
    removed_pct = (
        (removed_count / total_previous_way_ids * 100)
        if total_previous_way_ids > 0
        else 0
    )
    increased_pct = (
        (increased_count / total_previous_way_ids * 100)
        if total_previous_way_ids > 0
        else 0
    )
    decreased_pct = (
        (decreased_count / total_previous_way_ids * 100)
        if total_previous_way_ids > 0
        else 0
    )
    same_pct = (
        (same_count / total_previous_way_ids * 100) if total_previous_way_ids > 0 else 0
    )

    print(f"Total way_ids in current dataset: {total_current_way_ids}")
    print(f"New way_ids (Recall increased): {new_count} ({new_pct:.1f}%)")
    print(f"Removed way_ids: {removed_count} ({removed_pct:.1f}%)")
    print(f"Increased speed limits: {increased_count} ({increased_pct:.1f}%)")
    print(f"Decreased speed limits: {decreased_count} ({decreased_pct:.1f}%)")
    print(f"Unchanged speed limits: {same_count} ({same_pct:.1f}%)")

    # Confidence statistics
    current_low_confidence = state_current_confidence.filter(
        F.col("confidence") < 0.7
    ).count()
    previous_low_confidence = state_previous_confidence.filter(
        F.col("confidence") < 0.7
    ).count()

    total_with_confidence = state_current_confidence.count()
    current_low_conf_pct = (
        (current_low_confidence / total_current_way_ids * 100)
        if total_current_way_ids > 0
        else 0
    )

    previous_low_conf_pct = (
        (previous_low_confidence / total_previous_way_ids * 100)
        if total_previous_way_ids > 0
        else 0
    )

    print(f"\nConfidence < 0.7:")
    print(f"  Current: {current_low_confidence} ({current_low_conf_pct:.1f}%)")
    print(f"  Previous: {previous_low_confidence} ({previous_low_conf_pct:.1f}%)")

    # Confidence change
    if previous_low_confidence > 0:
        confidence_change = current_low_confidence - previous_low_confidence
        confidence_change_pct = (
            (confidence_change / previous_low_confidence * 100)
            if previous_low_confidence > 0
            else 0
        )

        if confidence_change > 0:
            print(
                f"  Change: +{confidence_change} (+{confidence_change_pct:.1f}% increase)"
            )
        elif confidence_change < 0:
            print(
                f"  Change: {confidence_change} ({confidence_change_pct:.1f}% decrease)"
            )
        else:
            print(f"  Change: No change")
    else:
        print(f"  Change: N/A (no previous data)")

    # Speed limits less than (p85 - 10 mph) analysis
    current_low_speed_count = state_current_confidence.filter(
        F.col("speed_limit_mph") < (F.col("p85_speed") - F.lit(10))
    ).count()
    previous_low_speed_count = state_previous_confidence.filter(
        F.col("speed_limit_mph") < (F.col("p85_speed") - F.lit(10))
    ).count()

    current_low_speed_pct = (
        (current_low_speed_count / total_current_way_ids * 100)
        if total_current_way_ids > 0
        else 0
    )
    previous_low_speed_pct = (
        (previous_low_speed_count / total_previous_way_ids * 100)
        if total_previous_way_ids > 0
        else 0
    )

    print(f"\nSpeed limits < (p85 - 10 mph):")
    print(f"  Current: {current_low_speed_count} ({current_low_speed_pct:.1f}%)")
    print(f"  Previous: {previous_low_speed_count} ({previous_low_speed_pct:.1f}%)")

    # Low speed change
    if previous_low_speed_count > 0:
        low_speed_change = current_low_speed_count - previous_low_speed_count
        low_speed_change_pct = (
            (low_speed_change / previous_low_speed_count * 100)
            if previous_low_speed_count > 0
            else 0
        )

        if low_speed_change > 0:
            print(
                f"  Change: +{low_speed_change} (+{low_speed_change_pct:.1f}% increase)"
            )
        elif low_speed_change < 0:
            print(
                f"  Change: {low_speed_change} ({low_speed_change_pct:.1f}% decrease)"
            )
        else:
            print(f"  Change: No change")
    else:
        print(f"  Change: N/A (no previous data)")

print("\n✅ State-level statistics completed!")

# COMMAND ----------
