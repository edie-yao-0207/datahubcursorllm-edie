# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Safety Event Review FP Rates Aggregation
# MAGIC
# MAGIC This notebook computes daily False Positive (FP) rates for Safety Event Review jobs.
# MAGIC The FP rates are used by dynamic policy configuration to determine which event types
# MAGIC should skip human review based on their detection accuracy.
# MAGIC
# MAGIC **FP Rate = (dismissed_jobs / total_jobs) * 100**
# MAGIC
# MAGIC Where:
# MAGIC - dismissed_jobs = jobs with result_type IN (2, 3) (TRIGGER_INCORRECT variants)
# MAGIC - total_jobs = all completed jobs (status=100) with a result_type
# MAGIC
# MAGIC The aggregation uses a 14-day rolling window.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

# Use UTC explicitly since the job is scheduled at 8 AM UTC (cron: "0 0 8 * * ?")
# This ensures consistent partition dates regardless of cluster timezone
today_utc = datetime.now(timezone.utc).date()

# Queue name constant for filtering review jobs
CUSTOMER_QUEUE = "CUSTOMER"

# partition_date: the date this aggregation is computed for (used as partition key)
# window_start: start of the 14-day rolling window (today - 13 days = 14 days inclusive)
partition_date = today_utc.strftime("%Y-%m-%d")
window_start = (today_utc - timedelta(days=13)).strftime("%Y-%m-%d")

print(f"Computing FP rates for partition date: {partition_date}")
print(f"Using 14-day window from {window_start} to {partition_date}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the aggregated FP rates table
# MAGIC CREATE TABLE IF NOT EXISTS product_analytics.agg_safety_event_review_fp_rates (
# MAGIC   date STRING NOT NULL COMMENT 'The date this aggregation was computed, in YYYY-MM-DD format.',
# MAGIC   detection_type STRING NOT NULL COMMENT 'The event type that triggered the safety review (e.g., haDistractedDriving).',
# MAGIC   total_jobs BIGINT NOT NULL COMMENT 'Total number of completed review jobs in the 14-day window.',
# MAGIC   dismissed_jobs BIGINT NOT NULL COMMENT 'Number of jobs marked as false positive (result_type IN 2,3) in the 14-day window.',
# MAGIC   fp_rate_pct DOUBLE NOT NULL COMMENT 'False positive rate as a percentage: (dismissed_jobs / total_jobs) * 100.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date)
# MAGIC COMMENT 'Aggregated False Positive (FP) rates for Safety Event Review jobs. Used by dynamic policy configuration.'

# COMMAND ----------

# Build and execute the FP rate computation query
fp_rates_query = f"""
WITH job_with_type AS (
    SELECT
        j.uuid,
        j.result_type,
        CASE
            WHEN hat.enum = 6 THEN 'haSharpTurn'
            ELSE COALESCE(hat.event_type, 'unknown')
        END AS detection_type
    FROM safetyeventreviewdb.jobs j
    INNER JOIN safetyeventreviewdb.review_request_metadata rrm
        ON j.uuid = rrm.job_uuid AND j.org_id = rrm.org_id
    LEFT JOIN definitions.harsh_accel_type_enums hat
        ON rrm.accel_type = hat.enum
    INNER JOIN datamodel_core.dim_organizations o
        ON j.org_id = o.org_id AND j.date = o.date
    WHERE j.date BETWEEN '{window_start}' AND '{partition_date}'
        AND o.internal_type = 0              -- non-internal orgs only
        AND rrm.queue_name = '{CUSTOMER_QUEUE}'
        AND j.status = 100                   -- completed jobs only
        AND j.result_type IS NOT NULL        -- must have a result
)
SELECT
    '{partition_date}' AS date,
    detection_type,
    COUNT(DISTINCT uuid) AS total_jobs,
    COUNT(DISTINCT CASE WHEN result_type IN (2, 3) THEN uuid END) AS dismissed_jobs,
    CAST(ROUND(COUNT(DISTINCT CASE WHEN result_type IN (2, 3) THEN uuid END) * 100.0 / COUNT(DISTINCT uuid), 2) AS DOUBLE) AS fp_rate_pct
FROM job_with_type
GROUP BY detection_type
HAVING COUNT(DISTINCT uuid) > 0
ORDER BY total_jobs DESC
"""

fp_rates_df = spark.sql(fp_rates_query)
display(fp_rates_df)

# COMMAND ----------

# Write to the Delta table, overwriting the partition for today's date
fp_rates_df.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"date = '{partition_date}'"
).saveAsTable("product_analytics.agg_safety_event_review_fp_rates")

print(f"Successfully wrote {fp_rates_df.count()} rows for partition {partition_date}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data was written correctly
# MAGIC SELECT * FROM product_analytics.agg_safety_event_review_fp_rates
# MAGIC WHERE date = (SELECT MAX(date) FROM product_analytics.agg_safety_event_review_fp_rates)
# MAGIC ORDER BY total_jobs DESC
