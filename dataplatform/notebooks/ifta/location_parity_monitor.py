# Databricks notebook source
# Configuration
ALERT_THRESHOLD_PERCENT = 0.3

# Compare tables with scope filtering (exclude mobile app locations)
comparison_query = """
WITH old_filtered AS (
  SELECT date, org_id, device_id, time
  FROM kinesisstats.location
  WHERE date >= date_sub(current_date(), 5)
    AND date < date_sub(current_date(), 1)
    AND (scope IS NULL OR scope NOT IN (2))
),

new_filtered AS (
  SELECT date, org_id, object_id, time
  FROM kinesisstats.osdlocationwithroadcontext
  WHERE date >= date_sub(current_date(), 5)
    AND date < date_sub(current_date(), 1)
),

missing_in_new AS (
  SELECT old.*
  FROM old_filtered old
  LEFT JOIN new_filtered new
    ON old.org_id = new.org_id
    AND old.device_id = new.object_id
    AND old.time = new.time
    AND old.date = new.date
  WHERE new.object_id IS NULL
)

SELECT
  (SELECT COUNT(*) FROM old_filtered) as old_vehicle_locations,
  (SELECT COUNT(*) FROM new_filtered) as new_table_count,
  (SELECT COUNT(*) FROM missing_in_new) as missing_in_new,
  COALESCE(
    ROUND((SELECT COUNT(*) FROM missing_in_new) * 100.0 / NULLIF((SELECT COUNT(*) FROM old_filtered), 0), 2),
    0.0
  ) as missing_pct
"""

result_df = spark.sql(comparison_query)
result = result_df.collect()[0]

old_count = result["old_vehicle_locations"]
new_count = result["new_table_count"]
missing_count = result["missing_in_new"]
percent_difference = result["missing_pct"]

difference = missing_count

print(f"Old table (vehicle locations, 5 days excluding yesterday): {old_count:,} rows")
print(f"New table (5 days excluding yesterday): {new_count:,} rows")
print(f"Missing in new table: {difference:,} rows ({percent_difference:.2f}%)")

# COMMAND ----------

# Check parity and raise exception if threshold exceeded
if new_count == 0 and old_count > 0:
    raise Exception("New table has NO rows while old table has data")
elif percent_difference > ALERT_THRESHOLD_PERCENT:
    raise Exception(
        f"New table missing {percent_difference:.2f}% of rows (old: {old_count:,}, new: {new_count:,}, missing: {difference:,})"
    )
else:
    print(f"   Parity check passed - tables are in sync")
    print(f"   Difference: {difference:,} rows ({percent_difference:.2f}%)")
