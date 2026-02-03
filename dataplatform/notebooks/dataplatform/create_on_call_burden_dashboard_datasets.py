# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, rlike, avg

num_weeks_in_window = 13
window_spec = Window.orderBy("week_id").rowsBetween(-(num_weeks_in_window - 1), 0)

# COMMAND ----------


def make_metric_table(spark_df, in_metric_name, out_metric_name, out_table_name):
    temp_spark_df = (
        spark_df.groupBy("week_id")
        .sum(in_metric_name)
        .withColumnRenamed(f"sum({in_metric_name})", out_metric_name)
    )
    temp_spark_df = temp_spark_df.sort("week_id").select("week_id", out_metric_name)
    temp_spark_df.write.mode("overwrite").format("delta").saveAsTable(out_table_name)


# COMMAND ----------


def make_metric_rolling_average_table(
    spark_df, in_metric_name, out_metric_name, out_table_name
):
    temp_spark_df = spark_df.groupBy("week_id").sum(in_metric_name)
    temp_spark_df = temp_spark_df.withColumn(
        out_metric_name, avg(f"sum({in_metric_name})").over(window_spec)
    )
    temp_spark_df = temp_spark_df.select("week_id", out_metric_name)
    temp_spark_df.write.mode("overwrite").format("delta").saveAsTable(out_table_name)


# COMMAND ----------

import re


def to_tokens(title):
    if title is None:
        title = ""

    title = re.sub(r"\d+", "(.*)", title)
    title = re.sub(r"\[.*\]", "(.*)", title)

    pattern = "\s+|:|,"
    parts = re.split(f"{pattern}", title)
    delims = [x for x in re.split(f"[^{pattern}]", title) if len(x) > 0]
    tokens = [p + d for p, d in zip(parts, delims + [""])]
    return tokens


def extract_patterns(spark_df):
    unique_titles = [
        row["title"] for row in spark_df.select("title").distinct().collect()
    ]

    G = {}
    for title in unique_titles:
        tokens = to_tokens(title)

        for p, c in zip(tokens, tokens[1:]):
            if p not in G:
                G.update({p: {c: c}})
            elif c not in G[p]:
                G[p].update({c: c})

    wildcards = [k for k, v in G.items() if len(v) > 6]
    for wildcard in wildcards:
        if wildcard in G:
            G[wildcard] = {k: "(.*)" for k, _ in G[wildcard].items()}

    unique_patterns = set()
    for title in unique_titles:
        tokens = to_tokens(title)

        out_pattern = [tokens[0]]
        for p, c in zip(tokens, tokens[1:]):
            out_pattern.append(G[p][c])

        t = [
            p
            for p, c in zip(out_pattern, out_pattern[1:])
            if not (p.strip() == "(.*)" and c.strip() == "(.*)")
        ]
        t.append(out_pattern[-1])
        out_pattern = t

        out_pattern = "".join(out_pattern)
        out_pattern = out_pattern.replace("[", "\\[").replace("]", "\\]")

        try:
            re.match(out_pattern, "test string")
            if "{" in out_pattern or "}" in out_pattern:
                raise ValueError()
        except Exception:
            print("Rejecting", out_pattern)
            continue

        unique_patterns.update([out_pattern])

    print(
        "unique_titles:", len(unique_titles), "unique_patterns:", len(unique_patterns)
    )

    return [pattern for pattern in unique_patterns]


# COMMAND ----------


def match_patterns(spark_df, patterns):
    others_spark_df = spark_df.withColumn("pattern", lit("Other"))
    all_matched_spark_df = None

    for pattern in patterns:
        c1 = col("title").rlike(pattern)
        pattern_matched_spark_df = others_spark_df.filter(c1)
        others_spark_df = others_spark_df.filter(~c1)

        pattern_matched_spark_df = pattern_matched_spark_df.replace(
            "Other", pattern.replace(".", "_"), "pattern"
        )

        if all_matched_spark_df is None:
            all_matched_spark_df = pattern_matched_spark_df
        else:
            all_matched_spark_df = all_matched_spark_df.unionAll(
                pattern_matched_spark_df
            )

    if all_matched_spark_df is None:
        all_matched_spark_df = others_spark_df
    else:
        all_matched_spark_df = all_matched_spark_df.unionAll(others_spark_df)

    return all_matched_spark_df


# COMMAND ----------


def make_metric_by_pattern_table(
    spark_df, in_metric_name, out_metric_name, out_table_name
):
    temp_spark_df = (
        spark_df.groupBy("week_id").pivot("pattern").sum(in_metric_name).fillna(0)
    )

    temp_spark_df = temp_spark_df.unpivot(
        ids=[temp_spark_df.columns[0]],
        values=temp_spark_df.columns[1:],
        variableColumnName="pattern",
        valueColumnName=out_metric_name,
    )

    temp_spark_df.write.mode("overwrite").format("delta").saveAsTable(out_table_name)


# COMMAND ----------


def make_metric_rolling_average_by_pattern_table(
    spark_df, in_metric_name, out_metric_name, out_table_name
):
    temp_spark_df = (
        spark_df.groupBy("week_id").pivot("pattern").sum(in_metric_name).fillna(0)
    )

    pattern_columns = temp_spark_df.columns[1:]

    new_columns = []
    for pattern_column in pattern_columns:
        new_column = f"mean_{pattern_column}"
        new_columns.append(new_column)

        temp_spark_df = temp_spark_df.withColumn(
            new_column, avg(pattern_column).over(window_spec)
        )

    new_columns.insert(0, "week_id")

    temp_spark_df = temp_spark_df.select(new_columns)

    new_columns.pop(0)

    temp_spark_df = temp_spark_df.withColumnsRenamed(
        {old: old.replace("mean_", "") for old in new_columns}
    )

    temp_spark_df = temp_spark_df.unpivot(
        ids=[temp_spark_df.columns[0]],
        values=temp_spark_df.columns[1:],
        variableColumnName="pattern",
        valueColumnName=out_metric_name,
    )

    temp_spark_df.write.mode("overwrite").format("delta").saveAsTable(out_table_name)


# COMMAND ----------

high_urgency_spark_df = spark.sql(
    """
  SELECT
      year(created_at) || '_' || to_char(weekofyear(created_at), "00") as week_id,
      title,
      1 as count,
      datediff(minute, created_at, last_status_change_at) / 60 / 24 / 3.5 as response_effort_eng_weeks
  FROM
    default.pagerduty_bronze.incident
  where
    service_id = 'PCBW9WG'
    and urgency = "high"
    and date_diff(MONTH, created_at, now()) < 12
  ORDER BY
    week_id
"""
).repartition(4)

# COMMAND ----------

make_metric_table(
    high_urgency_spark_df,
    "count",
    "pages_per_week",
    "default.dataplatform.pagerduty_high_urgency_pages_per_week",
)

# COMMAND ----------

make_metric_table(
    high_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks",
    "default.dataplatform.pagerduty_high_urgency_response_effort_eng_weeks_per_week",
)

# COMMAND ----------

make_metric_rolling_average_table(
    high_urgency_spark_df,
    "count",
    "pages_per_week_rolling_average",
    "default.dataplatform.pagerduty_high_urgency_pages_per_week_rolling_average",
)

# COMMAND ----------

make_metric_rolling_average_table(
    high_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks_rolling_average",
    "default.dataplatform.pagerduty_high_urgency_response_effort_eng_weeks_per_week_rolling_average",
)

# COMMAND ----------

high_urgency_patterns = extract_patterns(high_urgency_spark_df)

# COMMAND ----------

all_matched_high_urgency_spark_df = match_patterns(
    high_urgency_spark_df, high_urgency_patterns
)

# COMMAND ----------

make_metric_by_pattern_table(
    all_matched_high_urgency_spark_df,
    "count",
    "pages_per_week",
    "default.dataplatform.pagerduty_high_urgency_pages_per_week_by_pattern",
)

# COMMAND ----------

make_metric_by_pattern_table(
    all_matched_high_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks",
    "default.dataplatform.pagerduty_high_urgency_response_effort_eng_weeks_by_pattern",
)

# COMMAND ----------

make_metric_rolling_average_by_pattern_table(
    all_matched_high_urgency_spark_df,
    "count",
    "pages_per_week_rolling_average",
    "default.dataplatform.pagerduty_high_urgency_pages_per_week_rolling_average_by_pattern",
)

# COMMAND ----------

make_metric_rolling_average_by_pattern_table(
    all_matched_high_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks_rolling_average",
    "default.dataplatform.pagerduty_high_urgency_response_effort_eng_weeks_rolling_average_by_pattern",
)

# COMMAND ----------

low_urgency_spark_df = spark.sql(
    """
  SELECT
      year(created_at) || '_' || to_char(weekofyear(created_at), "00") as week_id,
      title,
      1 as count,
      datediff(minute, created_at, last_status_change_at) / 60 / 24 / 3.5 as response_effort_eng_weeks
  FROM
    default.pagerduty_bronze.incident
  where
    service_id = 'PGNHN96'
    and urgency = "low"
    and date_diff(MONTH, created_at, now()) < 12
  ORDER BY
    week_id
"""
).repartition(8)

# COMMAND ----------

make_metric_table(
    low_urgency_spark_df,
    "count",
    "pages_per_week",
    "default.dataplatform.pagerduty_low_urgency_pages_per_week",
)

# COMMAND ----------


make_metric_table(
    low_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks",
    "default.dataplatform.pagerduty_low_urgency_response_effort_eng_weeks_per_week",
)

# COMMAND ----------


make_metric_rolling_average_table(
    low_urgency_spark_df,
    "count",
    "pages_per_week_rolling_average",
    "default.dataplatform.pagerduty_low_urgency_pages_per_week_rolling_average",
)

# COMMAND ----------


make_metric_rolling_average_table(
    low_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks_rolling_average",
    "default.dataplatform.pagerduty_low_urgency_response_effort_eng_weeks_per_week_rolling_average",
)

# COMMAND ----------

low_urgency_patterns = extract_patterns(low_urgency_spark_df)

# COMMAND ----------

all_matched_low_urgency_spark_df = match_patterns(
    low_urgency_spark_df, low_urgency_patterns
)

# COMMAND ----------


make_metric_by_pattern_table(
    all_matched_low_urgency_spark_df,
    "count",
    "pages_per_week",
    "default.dataplatform.pagerduty_low_urgency_pages_per_week_by_pattern",
)


# COMMAND ----------

make_metric_by_pattern_table(
    all_matched_low_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks",
    "default.dataplatform.pagerduty_low_urgency_response_effort_eng_weeks_by_pattern",
)


# COMMAND ----------

make_metric_rolling_average_by_pattern_table(
    all_matched_low_urgency_spark_df,
    "count",
    "pages_per_week_rolling_average",
    "default.dataplatform.pagerduty_low_urgency_pages_per_week_rolling_average_by_pattern",
)


# COMMAND ----------

make_metric_rolling_average_by_pattern_table(
    all_matched_low_urgency_spark_df,
    "response_effort_eng_weeks",
    "response_effort_eng_weeks_rolling_average",
    "default.dataplatform.pagerduty_low_urgency_response_effort_eng_weeks_rolling_average_by_pattern",
)
