# Databricks notebook source
# DBTITLE 1,frequency of counts vs gauges
# MAGIC %sql
# MAGIC
# MAGIC WITH per_object_stat AS (
# MAGIC   SELECT
# MAGIC     t.org_id,
# MAGIC     t.object_id,
# MAGIC     t.time AS event_ts,
# MAGIC
# MAGIC     SUM(CASE WHEN m.metric_type = 1 THEN 1 ELSE 0 END) AS count_metric_count,
# MAGIC     SUM(CASE WHEN m.metric_type = 2 THEN 1 ELSE 0 END) AS gauge_metric_count,
# MAGIC
# MAGIC     -- NOTE: if you suspect NULLs are the issue, consider COALESCE(m.value, 0) here
# MAGIC     SUM(CASE WHEN m.metric_type = 1 AND m.value = 0 THEN 1 ELSE 0 END) AS zero_value_count_metric_count
# MAGIC
# MAGIC   FROM kinesisstats.osdfirmwaremetrics t
# MAGIC   LATERAL VIEW explode(t.value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(t.`date`) = DATE('2025-12-12')
# MAGIC   GROUP BY t.org_id, t.object_id, t.time
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*) AS object_stat_count,
# MAGIC   SUM(count_metric_count) AS total_count_metrics,
# MAGIC   SUM(gauge_metric_count) AS total_gauge_metrics,
# MAGIC
# MAGIC   percentile_approx(count_metric_count, 0.5)  AS p50_count,
# MAGIC   percentile_approx(count_metric_count, 0.9)  AS p90_count,
# MAGIC   percentile_approx(count_metric_count, 0.99) AS p99_count,
# MAGIC
# MAGIC   AVG(zero_value_count_metric_count)                 AS avg_zero_value_count_metrics,
# MAGIC   percentile_approx(zero_value_count_metric_count, 0.5)  AS p50_zero_count,
# MAGIC   percentile_approx(zero_value_count_metric_count, 0.9)  AS p90_zero_count,
# MAGIC   percentile_approx(zero_value_count_metric_count, 0.99) AS p99_zero_count,
# MAGIC
# MAGIC   percentile_approx(gauge_metric_count, 0.5)  AS p50_gauge,
# MAGIC   percentile_approx(gauge_metric_count, 0.9)  AS p90_gauge,
# MAGIC   percentile_approx(gauge_metric_count, 0.99) AS p99_gauge
# MAGIC FROM per_object_stat;
# MAGIC

# COMMAND ----------

# DBTITLE 1,value distribution of metrics
# MAGIC %sql
# MAGIC WITH exploded AS (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     object_id,
# MAGIC     time AS event_ts,
# MAGIC     m.name        AS metric_name,
# MAGIC     m.metric_type AS metric_type,
# MAGIC     m.value       AS metric_value
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC ),
# MAGIC
# MAGIC count_metrics AS (
# MAGIC   SELECT *
# MAGIC   FROM exploded
# MAGIC   WHERE metric_type = 1   -- 1 for count, 2 for gauge
# MAGIC ),
# MAGIC
# MAGIC binned AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN metric_value IS NULL THEN 'NULL'
# MAGIC       WHEN isnan(metric_value) THEN 'NaN'
# MAGIC       WHEN metric_value = 0 THEN '0'
# MAGIC       WHEN metric_value BETWEEN 1 AND 127 THEN '1-127'
# MAGIC       WHEN metric_value > 127 THEN '>127'
# MAGIC       ELSE 'OTHER'  -- e.g., negative values
# MAGIC     END AS value_bin
# MAGIC   FROM count_metrics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   value_bin,
# MAGIC   COUNT(*) AS metric_entry_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_count_metrics
# MAGIC FROM binned
# MAGIC GROUP BY value_bin
# MAGIC ORDER BY
# MAGIC   CASE value_bin
# MAGIC     WHEN '0' THEN 1
# MAGIC     WHEN '1-127' THEN 2
# MAGIC     WHEN '>127' THEN 3
# MAGIC     WHEN 'NULL' THEN 4
# MAGIC     WHEN 'NaN' THEN 5
# MAGIC     ELSE 6
# MAGIC   END;
# MAGIC

# COMMAND ----------

# DBTITLE 1,percent of metric counts with non-integer values
# MAGIC %sql
# MAGIC WITH counts AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN m.value IS NULL THEN 'NULL_VALUE'
# MAGIC       WHEN m.value < 0 THEN 'NEGATIVE'
# MAGIC       WHEN m.value = FLOOR(m.value) THEN 'INTEGER_VALUE'
# MAGIC       ELSE 'NON_INTEGER_VALUE'
# MAGIC     END AS value_category,
# MAGIC     COUNT(*) AS metric_count
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC     AND m.metric_type = 1
# MAGIC   GROUP BY
# MAGIC     CASE
# MAGIC       WHEN m.value IS NULL THEN 'NULL_VALUE'
# MAGIC       WHEN m.value < 0 THEN 'NEGATIVE'
# MAGIC       WHEN m.value = FLOOR(m.value) THEN 'INTEGER_VALUE'
# MAGIC       ELSE 'NON_INTEGER_VALUE'
# MAGIC     END
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   value_category,
# MAGIC   metric_count,
# MAGIC   ROUND(
# MAGIC     100.0 * metric_count / SUM(metric_count) OVER (),
# MAGIC     2
# MAGIC   ) AS pct_of_count_metrics
# MAGIC FROM counts
# MAGIC ORDER BY value_category;
# MAGIC

# COMMAND ----------

# DBTITLE 1,show metric counts with non-integer values
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   m.name,
# MAGIC   count(*)
# MAGIC FROM kinesisstats.osdfirmwaremetrics
# MAGIC LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC   AND m.metric_type = 1
# MAGIC   AND m.value IS NOT NULL
# MAGIC   AND m.value != FLOOR(m.value)
# MAGIC GROUP BY m.name

# COMMAND ----------

# DBTITLE 1,example counts with non-integer
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     org_id,
# MAGIC     object_id,
# MAGIC     m.name  AS metric_name,
# MAGIC     m.value AS metric_value,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY m.name
# MAGIC       ORDER BY time   -- arbitrary; could also use NULL or m.value
# MAGIC     ) AS rn
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC     AND m.metric_type = 1
# MAGIC     AND m.value IS NOT NULL
# MAGIC     AND m.value <> FLOOR(m.value)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   org_id,
# MAGIC   object_id,
# MAGIC   metric_name,
# MAGIC   metric_value
# MAGIC FROM ranked
# MAGIC WHERE rn = 1
# MAGIC LIMIT 1000;
# MAGIC

# COMMAND ----------

# DBTITLE 1,metric name distribution
# MAGIC %sql
# MAGIC
# MAGIC WITH exploded AS (
# MAGIC   SELECT
# MAGIC     m.name        AS metric_name,
# MAGIC     m.metric_type AS metric_type
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   metric_name,
# MAGIC   CASE metric_type
# MAGIC     WHEN 1 THEN 'COUNT'
# MAGIC     WHEN 2 THEN 'GAUGE'
# MAGIC     ELSE 'OTHER'
# MAGIC   END AS metric_type,
# MAGIC   COUNT(*) AS metric_entry_count,
# MAGIC   ROUND(
# MAGIC     100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
# MAGIC     2
# MAGIC   ) AS pct_of_all_metrics
# MAGIC FROM exploded
# MAGIC GROUP BY metric_name, metric_type
# MAGIC ORDER BY metric_entry_count DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,tag set vs individual tag cardinality
# MAGIC %sql
# MAGIC
# MAGIC WITH metrics AS (
# MAGIC   SELECT
# MAGIC     m.name AS metric_name,
# MAGIC     m.metric_type,
# MAGIC     m.tags AS tags
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC ),
# MAGIC
# MAGIC tag_rows AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_type,
# MAGIC     t AS tag
# MAGIC   FROM metrics
# MAGIC   LATERAL VIEW explode(tags) t AS t
# MAGIC ),
# MAGIC
# MAGIC tag_sets AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_type,
# MAGIC     concat_ws('|', array_sort(coalesce(tags, array()))) AS tag_set_key
# MAGIC   FROM metrics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT COUNT(DISTINCT tag) FROM tag_rows) AS unique_tags,
# MAGIC   (SELECT COUNT(DISTINCT tag_set_key) FROM tag_sets) AS unique_tag_sets;
# MAGIC

# COMMAND ----------

# DBTITLE 1,canonicalize and store tagsets
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH exploded AS (
# MAGIC   SELECT
# MAGIC     array_sort(coalesce(m.tags, array())) AS sorted_tags
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   concat_ws('|', sorted_tags) AS tag_set_key,
# MAGIC   size(sorted_tags)           AS tag_count,
# MAGIC   COUNT(*)                    AS metric_entry_count
# MAGIC FROM exploded
# MAGIC GROUP BY
# MAGIC   concat_ws('|', sorted_tags),
# MAGIC   size(sorted_tags);
# MAGIC

# COMMAND ----------

# DBTITLE 1,show individual tags counts
# MAGIC %sql
# MAGIC WITH exploded_tags AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     metric_entry_count
# MAGIC   FROM firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC   LATERAL VIEW explode(split(tag_set_key, '\\|')) tag AS tag
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   tag,
# MAGIC   SUM(metric_entry_count) AS total_metric_entries
# MAGIC FROM exploded_tags
# MAGIC GROUP BY tag
# MAGIC ORDER BY total_metric_entries DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,tag count distribution
# MAGIC %sql
# MAGIC WITH totals AS (
# MAGIC   SELECT SUM(metric_entry_count) AS total
# MAGIC   FROM firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   tag_count,
# MAGIC   SUM(metric_entry_count) AS total_metric_entries,
# MAGIC   ROUND(
# MAGIC     100.0 * SUM(metric_entry_count) / t.total,
# MAGIC     2
# MAGIC   ) AS pct_of_metrics
# MAGIC FROM firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC CROSS JOIN totals t
# MAGIC GROUP BY tag_count, t.total
# MAGIC ORDER BY tag_count;
# MAGIC

# COMMAND ----------

# DBTITLE 1,longest tags
# MAGIC %sql
# MAGIC WITH exploded_tags AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     len(tag) as tag_len
# MAGIC   FROM firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC   LATERAL VIEW explode(split(tag_set_key, '\\|')) tag AS tag
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   tag_len,
# MAGIC   tag
# MAGIC FROM exploded_tags
# MAGIC ORDER BY tag_len DESC
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,calculate savings per tag
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE firmware_dev.jimrowson_tag_enum_byte_model
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH tag_counts AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     SUM(metric_entry_count) AS tag_occurrences
# MAGIC   FROM firmware_dev.jimrowson_firmwaremetrics_tagsets
# MAGIC   LATERAL VIEW explode(split(tag_set_key, '\\|')) tag AS tag
# MAGIC   WHERE tag IS NOT NULL AND tag <> '' -- exclude empty tag
# MAGIC   GROUP BY tag
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     tag_occurrences,
# MAGIC
# MAGIC     -- Original encoding, adjusted for gzip/zlib:
# MAGIC     -- assume compressed payload is 70% of string bytes; keep 1 byte for length
# MAGIC     (CAST(CEIL(0.70 * LENGTH(tag)) AS BIGINT) + 1) AS orig_bytes_per_occurrence,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (ORDER BY tag_occurrences DESC, tag) AS freq_rank
# MAGIC   FROM tag_counts
# MAGIC ),
# MAGIC
# MAGIC sized AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     tag_occurrences,
# MAGIC     orig_bytes_per_occurrence,
# MAGIC     freq_rank,
# MAGIC     CASE WHEN freq_rank <= 100 THEN 1 ELSE 2 END AS enum_bytes_per_occurrence
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   tag,
# MAGIC   tag_occurrences,
# MAGIC   freq_rank,
# MAGIC   orig_bytes_per_occurrence,
# MAGIC   enum_bytes_per_occurrence,
# MAGIC
# MAGIC   (orig_bytes_per_occurrence - enum_bytes_per_occurrence) AS bytes_saved_per_occurrence,
# MAGIC
# MAGIC   tag_occurrences * orig_bytes_per_occurrence             AS total_orig_bytes,
# MAGIC   tag_occurrences * enum_bytes_per_occurrence             AS total_enum_bytes,
# MAGIC   tag_occurrences * (orig_bytes_per_occurrence - enum_bytes_per_occurrence) AS total_bytes_saved
# MAGIC FROM sized;
# MAGIC

# COMMAND ----------

# DBTITLE 1,total savings if enumify all tags
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(total_orig_bytes)      AS total_orig_bytes,
# MAGIC   SUM(total_enum_bytes)      AS total_enum_bytes,
# MAGIC   SUM(total_bytes_saved)     AS total_bytes_saved,
# MAGIC   ROUND(100.0 * SUM(total_bytes_saved) / SUM(total_orig_bytes), 2) AS pct_savings
# MAGIC FROM firmware_dev.jimrowson_tag_enum_byte_model;
# MAGIC

# COMMAND ----------

# DBTITLE 1,total savings if enumify a subset of tags
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_bytes_saved DESC, tag) AS savings_rank
# MAGIC   FROM firmware_dev.jimrowson_tag_enum_byte_model
# MAGIC ),
# MAGIC
# MAGIC all_tags_totals AS (
# MAGIC   SELECT
# MAGIC     SUM(total_orig_bytes) AS total_orig_bytes,
# MAGIC     SUM(tag_occurrences)  AS total_tag_occurrences
# MAGIC   FROM ranked
# MAGIC ),
# MAGIC
# MAGIC convert_top AS (
# MAGIC   SELECT
# MAGIC     SUM(total_orig_bytes)  AS orig_bytes_for_converted,
# MAGIC     SUM(total_enum_bytes)  AS enum_bytes_for_converted,
# MAGIC     SUM(total_bytes_saved) AS bytes_saved_for_converted,
# MAGIC     SUM(tag_occurrences)   AS tag_occurrences_converted
# MAGIC   FROM ranked
# MAGIC   WHERE savings_rank <= 200
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.total_orig_bytes AS total_orig_bytes,
# MAGIC
# MAGIC   -- New total = (enum bytes for converted) + (original bytes for everything else)
# MAGIC   (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted)) AS total_new_bytes,
# MAGIC
# MAGIC   -- Savings = original total - new total
# MAGIC   (a.total_orig_bytes - (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted))) AS total_bytes_saved,
# MAGIC
# MAGIC   ROUND(
# MAGIC     100.0 * (a.total_orig_bytes - (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted)))
# MAGIC     / a.total_orig_bytes,
# MAGIC     2
# MAGIC   ) AS pct_savings,
# MAGIC
# MAGIC   -- Percent of tag occurrences that would use enums
# MAGIC   c.tag_occurrences_converted AS tag_occurrences_using_enums,
# MAGIC   a.total_tag_occurrences     AS total_tag_occurrences,
# MAGIC   ROUND(
# MAGIC     100.0 * c.tag_occurrences_converted / a.total_tag_occurrences,
# MAGIC     2
# MAGIC   ) AS pct_tag_occurrences_using_enums
# MAGIC
# MAGIC FROM all_tags_totals a
# MAGIC CROSS JOIN convert_top c;
# MAGIC

# COMMAND ----------

# DBTITLE 1,tag length distribution for best tags
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_bytes_saved DESC, tag) AS savings_rank
# MAGIC   FROM firmware_dev.jimrowson_tag_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   LENGTH(tag) AS tag_length,
# MAGIC   COUNT(*)    AS num_unique_tags
# MAGIC FROM ranked
# MAGIC WHERE savings_rank <= 200
# MAGIC GROUP BY LENGTH(tag)
# MAGIC ORDER BY tag_length;
# MAGIC

# COMMAND ----------

# DBTITLE 1,output tag strings for use in code
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     enum_bytes_per_occurrence,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       ORDER BY enum_bytes_per_occurrence ASC, total_bytes_saved DESC, tag
# MAGIC     ) AS mixed_rank
# MAGIC   FROM firmware_dev.jimrowson_tag_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   enum_bytes_per_occurrence AS enum_bytes,
# MAGIC   concat_ws('\n', collect_list(CONCAT('"', tag, '",'))) AS go_list
# MAGIC FROM ranked
# MAGIC WHERE mixed_rank <= 200
# MAGIC GROUP BY enum_bytes_per_occurrence
# MAGIC ORDER BY enum_bytes_per_occurrence;
# MAGIC

# COMMAND ----------

# DBTITLE 1,list top tags
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     tag,
# MAGIC     enum_bytes_per_occurrence,
# MAGIC     total_bytes_saved,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       ORDER BY enum_bytes_per_occurrence ASC, total_bytes_saved DESC, tag
# MAGIC     ) AS mixed_rank
# MAGIC   FROM firmware_dev.jimrowson_tag_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   tag,
# MAGIC   enum_bytes_per_occurrence,
# MAGIC   total_bytes_saved
# MAGIC FROM ranked
# MAGIC WHERE mixed_rank <= 300
# MAGIC
# MAGIC -- SELECT sum(total_bytes_saved) from ranked where mixed_rank >= 0 and mixed_rank <= 200

# COMMAND ----------

# DBTITLE 1,calculate savings per name
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH name_counts AS (
# MAGIC   SELECT
# MAGIC     m.name        AS metric_name,
# MAGIC     m.metric_type AS metric_type,
# MAGIC     COUNT(*)      AS metric_occurrences
# MAGIC   FROM kinesisstats.osdfirmwaremetrics
# MAGIC   LATERAL VIEW explode(value.proto_value.firmware_metrics.metrics) m AS m
# MAGIC   WHERE DATE(`date`) = DATE('2025-12-12')
# MAGIC     AND m.name IS NOT NULL
# MAGIC     AND m.name <> ''
# MAGIC   GROUP BY m.name, m.metric_type
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_type,
# MAGIC     metric_occurrences,
# MAGIC
# MAGIC     -- Original encoding, adjusted for gzip/zlib:
# MAGIC     -- assume compressed payload is 70% of string bytes; keep 1 byte for length
# MAGIC     (CAST(CEIL(0.70 * LENGTH(metric_name)) AS BIGINT) + 1) AS orig_bytes_per_occurrence,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (ORDER BY metric_occurrences DESC, metric_name) AS freq_rank
# MAGIC   FROM name_counts
# MAGIC ),
# MAGIC
# MAGIC sized AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_type,
# MAGIC     metric_occurrences,
# MAGIC     orig_bytes_per_occurrence,
# MAGIC     freq_rank,
# MAGIC     CASE WHEN freq_rank <= 100 THEN 1 ELSE 2 END AS enum_bytes_per_occurrence
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   metric_name,
# MAGIC   metric_type,
# MAGIC   metric_occurrences,
# MAGIC   freq_rank,
# MAGIC   orig_bytes_per_occurrence,
# MAGIC   enum_bytes_per_occurrence,
# MAGIC
# MAGIC   (orig_bytes_per_occurrence - enum_bytes_per_occurrence) AS bytes_saved_per_occurrence,
# MAGIC
# MAGIC   metric_occurrences * orig_bytes_per_occurrence             AS total_orig_bytes,
# MAGIC   metric_occurrences * enum_bytes_per_occurrence             AS total_enum_bytes,
# MAGIC   metric_occurrences * (orig_bytes_per_occurrence - enum_bytes_per_occurrence) AS total_bytes_saved
# MAGIC FROM sized;
# MAGIC

# COMMAND ----------

# DBTITLE 1,total savings if enumify all names
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(total_orig_bytes)      AS total_orig_bytes,
# MAGIC   SUM(total_enum_bytes)      AS total_enum_bytes,
# MAGIC   SUM(total_bytes_saved)     AS total_bytes_saved,
# MAGIC   ROUND(100.0 * SUM(total_bytes_saved) / SUM(total_orig_bytes), 2) AS pct_savings
# MAGIC FROM firmware_dev.jimrowson_metricname_enum_byte_model;
# MAGIC

# COMMAND ----------

# DBTITLE 1,total savings if enumify a subset of names
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_bytes_saved DESC, metric_name) AS savings_rank
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC ),
# MAGIC
# MAGIC all_names_totals AS (
# MAGIC   SELECT
# MAGIC     SUM(total_orig_bytes)     AS total_orig_bytes,
# MAGIC     SUM(metric_occurrences)   AS total_metric_occurrences
# MAGIC   FROM ranked
# MAGIC ),
# MAGIC
# MAGIC convert_top AS (
# MAGIC   SELECT
# MAGIC     SUM(total_orig_bytes)     AS orig_bytes_for_converted,
# MAGIC     SUM(total_enum_bytes)     AS enum_bytes_for_converted,
# MAGIC     SUM(total_bytes_saved)    AS bytes_saved_for_converted,
# MAGIC     SUM(metric_occurrences)   AS metric_occurrences_converted
# MAGIC   FROM ranked
# MAGIC   WHERE savings_rank <= 200
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.total_orig_bytes AS total_orig_bytes,
# MAGIC
# MAGIC   -- New total = (enum bytes for converted) + (original bytes for everything else)
# MAGIC   (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted)) AS total_new_bytes,
# MAGIC
# MAGIC   -- Savings = original total - new total
# MAGIC   (a.total_orig_bytes
# MAGIC     - (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted))
# MAGIC   ) AS total_bytes_saved,
# MAGIC
# MAGIC   ROUND(
# MAGIC     100.0
# MAGIC     * (a.total_orig_bytes
# MAGIC         - (c.enum_bytes_for_converted + (a.total_orig_bytes - c.orig_bytes_for_converted))
# MAGIC       )
# MAGIC     / a.total_orig_bytes,
# MAGIC     2
# MAGIC   ) AS pct_savings,
# MAGIC
# MAGIC   -- Percent of metric-name occurrences that would use enums
# MAGIC   c.metric_occurrences_converted AS metric_occurrences_using_enums,
# MAGIC   a.total_metric_occurrences     AS total_metric_occurrences,
# MAGIC   ROUND(
# MAGIC     100.0 * c.metric_occurrences_converted / a.total_metric_occurrences,
# MAGIC     2
# MAGIC   ) AS pct_metric_occurrences_using_enums
# MAGIC
# MAGIC FROM all_names_totals a
# MAGIC CROSS JOIN convert_top c;
# MAGIC

# COMMAND ----------

# DBTITLE 1,name length distribution for best names
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_bytes_saved DESC, metric_name) AS savings_rank
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   LENGTH(metric_name) AS name_length,
# MAGIC   COUNT(*)    AS num_unique_names
# MAGIC FROM ranked
# MAGIC WHERE savings_rank <= 200
# MAGIC GROUP BY LENGTH(metric_name)
# MAGIC ORDER BY name_length;
# MAGIC

# COMMAND ----------

# DBTITLE 1,output name strings for use in code
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     enum_bytes_per_occurrence,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       ORDER BY enum_bytes_per_occurrence ASC, total_bytes_saved DESC, metric_name
# MAGIC     ) AS mixed_rank
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   enum_bytes_per_occurrence AS enum_bytes,
# MAGIC   concat_ws('\n', collect_list(CONCAT('"', metric_name, '",'))) AS go_list
# MAGIC FROM ranked
# MAGIC WHERE mixed_rank <= 200
# MAGIC GROUP BY enum_bytes_per_occurrence
# MAGIC ORDER BY enum_bytes_per_occurrence;

# COMMAND ----------

# DBTITLE 1,list top names
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     enum_bytes_per_occurrence,
# MAGIC     total_bytes_saved,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       ORDER BY enum_bytes_per_occurrence ASC, total_bytes_saved DESC, metric_name
# MAGIC     ) AS mixed_rank
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC )
# MAGIC
# MAGIC --SELECT
# MAGIC --  metric_name,
# MAGIC --  enum_bytes_per_occurrence,
# MAGIC --  total_bytes_saved
# MAGIC --FROM ranked
# MAGIC --WHERE mixed_rank <= 300
# MAGIC
# MAGIC SELECT sum(total_bytes_saved) from ranked where mixed_rank >= 201 and mixed_rank <= 500

# COMMAND ----------

# DBTITLE 1,gauge type frequency
# MAGIC %sql
# MAGIC WITH classified AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_occurrences,
# MAGIC     CASE
# MAGIC       WHEN endswith(metric_name, '.min') THEN 'min'
# MAGIC       WHEN endswith(metric_name, '.max') THEN 'max'
# MAGIC       WHEN endswith(metric_name, '.avg') THEN 'avg'
# MAGIC       WHEN endswith(metric_name, '.var') THEN 'var'
# MAGIC       ELSE 'last_val'
# MAGIC     END AS gauge_kind
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC   WHERE metric_type = 2   -- GAUGE only
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   gauge_kind,
# MAGIC   SUM(metric_occurrences) AS metric_name_occurrences,
# MAGIC   ROUND(100.0 * SUM(metric_occurrences) / SUM(SUM(metric_occurrences)) OVER (), 2) AS pct_of_occurrences
# MAGIC FROM classified
# MAGIC GROUP BY gauge_kind
# MAGIC ORDER BY gauge_kind;

# COMMAND ----------

# DBTITLE 1,count of gauges with multiple types
# MAGIC %sql
# MAGIC WITH normalized AS (
# MAGIC   SELECT
# MAGIC     metric_name,
# MAGIC     metric_occurrences,
# MAGIC     CASE
# MAGIC       WHEN endswith(metric_name, '.min') THEN substr(metric_name, 1, length(metric_name) - 4)
# MAGIC       WHEN endswith(metric_name, '.max') THEN substr(metric_name, 1, length(metric_name) - 4)
# MAGIC       WHEN endswith(metric_name, '.avg') THEN substr(metric_name, 1, length(metric_name) - 4)
# MAGIC       WHEN endswith(metric_name, '.var') THEN substr(metric_name, 1, length(metric_name) - 4)
# MAGIC       ELSE metric_name
# MAGIC     END AS base_metric_name
# MAGIC   FROM firmware_dev.jimrowson_metricname_enum_byte_model
# MAGIC   WHERE metric_type = 2   -- GAUGE only
# MAGIC ),
# MAGIC
# MAGIC variants_per_base AS (
# MAGIC   SELECT
# MAGIC     base_metric_name,
# MAGIC     COUNT(*)                   AS num_variants,
# MAGIC     SUM(metric_occurrences)    AS total_metric_occurrences
# MAGIC   FROM normalized
# MAGIC   GROUP BY base_metric_name
# MAGIC ),
# MAGIC
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     SUM(total_metric_occurrences) AS all_metric_occurrences
# MAGIC   FROM variants_per_base
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   v.num_variants,
# MAGIC   COUNT(*)                      AS num_base_metrics,
# MAGIC   SUM(v.total_metric_occurrences) AS total_metric_occurrences,
# MAGIC
# MAGIC   -- percent of total gauge occurrences
# MAGIC   ROUND(
# MAGIC     100.0 * SUM(v.total_metric_occurrences) / t.all_metric_occurrences,
# MAGIC     2
# MAGIC   ) AS pct_of_total_occurrences
# MAGIC FROM variants_per_base v
# MAGIC CROSS JOIN totals t
# MAGIC GROUP BY v.num_variants, t.all_metric_occurrences
# MAGIC ORDER BY v.num_variants;

# COMMAND ----------

# MAGIC %md
# MAGIC
