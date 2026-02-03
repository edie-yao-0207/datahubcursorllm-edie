WITH deduped AS (
  SELECT * FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY date, table ORDER BY exportId DESC) AS row_number
      FROM delta.`s3://samsara-databricks-kinesisstats-diffs/diff-output-aggregates`
      WHERE exportId LIKE 'dataplatcron%'
  )
  WHERE row_number = 1
),

deduped_grouped_by_week AS (
    SELECT
        year(date) as year,
        weekofyear(date) as week_number,
        table as table_name,
        min(date) as start_date,
        max(date) as end_date,
        sum(changedCount + deletedCount + insertedCount) as weekly_incorrect_count,
        sum(ksCount) as weekly_total_count
    FROM deduped
    GROUP BY 1, 2, 3
)

SELECT
    year,
    week_number,
    start_date,
    end_date,
    table_name,
    1 - (weekly_incorrect_count / weekly_total_count) as weekly_accuracy,
    weekly_total_count
FROM deduped_grouped_by_week
