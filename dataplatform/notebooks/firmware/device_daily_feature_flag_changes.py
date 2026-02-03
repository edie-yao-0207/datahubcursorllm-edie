# Databricks notebook source
# This notebook detects daily feature flag changes (rollouts/rollbacks) per device.
#
# Optimization vs original SQL:
# - Original: explodes arrays (~150 keys/device) BEFORE window functions,
#   creating ~1B+ rows to shuffle/sort across ~1B partitions (org_id, gateway_id, key).
# - Optimized: runs window functions on ~7M compact rows, then uses map lookups
#   for key comparisons. Still explodes for output, but avoids the expensive
#   shuffle on the exploded dataset.

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Get date range from arguments (default: yesterday to today)
start_date, end_date = get_start_end_date(1, 0)

# Earliest date for feature flag capture
FF_CAPTURE_START_DATE = "2024-03-18"

# COMMAND ----------

# Load feature flag states from source table
# Match the original SQL: load from start_date to end_date
feature_flag_states_df = spark.sql(
    """--sql
    SELECT date, org_id, gateway_id, enabled_feature_flag_values
    FROM dataprep_firmware.gateway_feature_flag_states_daily
    WHERE
        date >= '{start_date}'
        AND date <= '{end_date}'
        AND date >= '{ff_start}'
        AND org_id != 1
    --endsql
    """.format(
        start_date=start_date,
        end_date=end_date,
        ff_start=FF_CAPTURE_START_DATE,
    )
)

# COMMAND ----------

# Apply window function BEFORE exploding - this is the key optimization.
# Original SQL: explodes first (~150 keys per device), then windows by (org_id, gateway_id, pair.key)
# With ~7M devices/day, that's ~7M * 150 = ~1B window partitions on ~1B+ exploded rows.
# Optimized: window by (org_id, gateway_id) on ~7M rows, then explode only for comparisons.
# This reduces shuffle volume significantly by operating on the compact pre-exploded data.
window_spec = Window.partitionBy("org_id", "gateway_id").orderBy("date")

feature_flag_states_with_lag_lead_df = (
    feature_flag_states_df.withColumn(
        "prev_feature_flag_values",
        F.lag("enabled_feature_flag_values").over(window_spec),
    )
    .withColumn("prev_date", F.lag("date").over(window_spec))
    .withColumn(
        "next_feature_flag_values",
        F.lead("enabled_feature_flag_values").over(window_spec),
    )
    .withColumn("next_date", F.lead("date").over(window_spec))
)

# COMMAND ----------

# Convert arrays of structs to maps for O(1) key lookup.
# With ~150 keys per device, map lookups are faster than linear array scans.
feature_flag_maps_df = (
    feature_flag_states_with_lag_lead_df.withColumn(
        "cur_map", F.map_from_entries("enabled_feature_flag_values")
    )
    .withColumn(
        "prev_map",
        F.when(
            F.col("prev_feature_flag_values").isNotNull(),
            F.map_from_entries("prev_feature_flag_values"),
        ),
    )
    .withColumn(
        "next_map",
        F.when(
            F.col("next_feature_flag_values").isNotNull(),
            F.map_from_entries("next_feature_flag_values"),
        ),
    )
)

# COMMAND ----------

# Get all unique keys from current and previous arrays for rollout detection
# Get all unique keys from current and next arrays for rollback detection
feature_flag_with_keys_df = (
    feature_flag_maps_df.withColumn("cur_keys", F.map_keys("cur_map"))
    .withColumn("prev_keys", F.coalesce(F.map_keys("prev_map"), F.array()))
    .withColumn("next_keys", F.coalesce(F.map_keys("next_map"), F.array()))
    # All keys for rollout comparison (cur vs prev)
    .withColumn("rollout_keys", F.array_union("cur_keys", "prev_keys"))
    # All keys for rollback comparison (cur vs next)
    .withColumn("rollback_keys", F.array_union("cur_keys", "next_keys"))
)

# COMMAND ----------

# --- ROLLOUTS ---
# Filter to valid dates for rollout detection.
# Exclude start_date itself because prev_date would be NULL, causing all keys to appear as rollouts.
# This matches the original SQL: date > greatest("2024-03-18", start_date)
rollout_base_df = feature_flag_with_keys_df.filter(
    (F.col("date") > F.lit(FF_CAPTURE_START_DATE)) & (F.col("date") > F.lit(start_date))
)

# Explode the union of current and previous keys for comparison
rollout_exploded_df = rollout_base_df.select(
    "date",
    "org_id",
    "gateway_id",
    "prev_date",
    "cur_map",
    "prev_map",
    F.explode("rollout_keys").alias("key"),
)

# Compute rollout change_int
rollouts_df = (
    rollout_exploded_df.withColumn("cur_value", F.element_at("cur_map", F.col("key")))
    .withColumn("prev_value", F.element_at("prev_map", F.col("key")))
    .withColumn(
        "change_int",
        F.when(
            # Only consider keys present in current (rollouts are for current keys)
            F.col("cur_value").isNotNull(),
            F.when(
                # prev_value is null → new key, rolled out
                F.col("prev_value").isNull(),
                F.lit(1),
            )
            .when(
                # date gap of 2+ days → key was rolled back then rolled out again
                F.col("prev_date") <= F.date_sub("date", 2),
                F.lit(1),
            )
            .when(
                # value changed from previous day (prev_date = date - 1)
                (F.col("prev_value") != F.col("cur_value"))
                & (F.col("prev_date") == F.date_sub("date", 1)),
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        ).otherwise(F.lit(0)),
    )
    .filter(F.col("cur_value").isNotNull())  # Only keep keys present in current
    .select(
        "date",
        "org_id",
        "gateway_id",
        "key",
        F.col("cur_value").alias("value"),
        "change_int",
    )
)

# COMMAND ----------

# --- ROLLBACKS ---
# Filter to valid dates for rollback detection (date < end_date)
rollback_base_df = feature_flag_with_keys_df.filter(F.col("date") < F.lit(end_date))

# Explode the union of current and next keys for comparison
rollback_exploded_df = rollback_base_df.select(
    "date",
    "org_id",
    "gateway_id",
    "next_date",
    "cur_map",
    "next_map",
    F.explode("rollback_keys").alias("key"),
)

# Compute rollback change_int
# Note: rollback date is date + 1 (the day when the rollback took effect)
rollbacks_df = (
    rollback_exploded_df.withColumn("cur_value", F.element_at("cur_map", F.col("key")))
    .withColumn("next_value", F.element_at("next_map", F.col("key")))
    .withColumn(
        "change_int",
        F.when(
            # Only consider keys present in current (rollbacks are for keys that existed)
            F.col("cur_value").isNotNull(),
            F.when(
                # next_value is null → key disappeared, rolled back
                F.col("next_value").isNull(),
                F.lit(-1),
            )
            .when(
                # date gap of 2+ days → key disappeared then reappeared
                F.col("next_date") >= F.date_add("date", 2),
                F.lit(-1),
            )
            .otherwise(F.lit(0)),
        ).otherwise(F.lit(0)),
    )
    .filter(F.col("cur_value").isNotNull())  # Only keep keys present in current
    .withColumn("date", F.date_add("date", 1))  # Rollback date is date + 1
    .select(
        "date",
        "org_id",
        "gateway_id",
        "key",
        F.col("cur_value").alias("value"),
        "change_int",
    )
)

# COMMAND ----------

# Union rollouts and rollbacks (like the original SQL)
# PySpark union() does NOT deduplicate, but SQL UNION does.
# Add distinct() to match the original SQL UNION behavior.
ff_key_value_change_df = rollouts_df.union(rollbacks_df).distinct()

# COMMAND ----------

# Target table name
target_table = "dataprep_firmware.device_daily_feature_flag_changes"

# COMMAND ----------

# Safety check: verify we have data before writing.
# Unlike MERGE (which only updates/inserts), replaceWhere will DELETE partitions
# even if the DataFrame is empty. An empty result with ~7M devices almost certainly
# indicates an upstream data issue, not a legitimate "no changes" scenario.
source_count = feature_flag_states_df.count()
output_count = ff_key_value_change_df.count()

assert source_count > 0, (
    f"Source table gateway_feature_flag_states_daily returned 0 rows for "
    f"date range {start_date} to {end_date}. Aborting to prevent data loss."
)

assert output_count > 0, (
    f"Output DataFrame is empty despite {source_count:,} source rows. "
    f"This likely indicates a logic error. Aborting to prevent data loss."
)

print(f"Source rows: {source_count:,}, Output rows: {output_count:,}")

# COMMAND ----------

# Write to table using partition overwrite (more efficient than MERGE).
# Since output is deterministic per date, we can simply replace the affected partitions.
#
# Note: The output DataFrame contains dates from max(start_date, FF_CAPTURE_START_DATE) + 1
# to end_date because:
# - Rollouts filter: date > FF_CAPTURE_START_DATE AND date > start_date
# - Rollbacks shift dates by +1, and source data starts from FF_CAPTURE_START_DATE
# We must pass the correct output date range to avoid deleting partitions without replacement.
ff_capture_date = datetime.datetime.strptime(FF_CAPTURE_START_DATE, "%Y-%m-%d")
# Handle start_date being either datetime.date or datetime.datetime
if isinstance(start_date, datetime.date) and not isinstance(
    start_date, datetime.datetime
):
    start_date_cmp = datetime.datetime.combine(start_date, datetime.time.min)
else:
    start_date_cmp = start_date
output_start_date = max(start_date_cmp, ff_capture_date) + datetime.timedelta(days=1)

create_or_replace_by_date(
    target_table, ff_key_value_change_df, output_start_date, end_date
)
