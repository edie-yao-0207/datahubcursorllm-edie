with t1 as (
  SELECT
    get_json_object(
      ConfigJson,
      '$.preprocessing_group.pipeline_name'
    ) AS pipeline_name,
    StartedAtMs,
    UpdatedAtMs
  FROM
    dynamodb.workflow_coordinator_status
  WHERE
    WorkflowName IN (
      "asset-detection-workflow",
      "asset-based-detection-and-augmentation-workflow",
      "detection-workflow",
      "detection-asset-workflow",
      "detection-and-augmentation-workflow",
      "detection-and-asset-augmentation-workflow",
      "processing-workflow"
    )
    AND FROM_UNIXTIME(StartedAtMs / 1000, 'yyyy-MM-dd') >= :date_range.start
    AND FROM_UNIXTIME(StartedAtMs / 1000, 'yyyy-MM-dd') <=  :date_range.end 
    AND ConfigJson IS NOT NULL
    AND status IN (2, 3)
)
select
  date_format(from_unixtime(StartedAtMs/1000),'yyyy-MM-dd') as date,
  round(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
      ORDER BY
        ((UpdatedAtMs - StartedAtMs) / 1000)
    ),
    2
  ) as p50_latency_secs,
  round(
    PERCENTILE_CONT(0.95) WITHIN GROUP (
      ORDER BY
        ((UpdatedAtMs - StartedAtMs) / 1000)
    ),
    2
  ) as p95_latency_secs,
  round(
    PERCENTILE_CONT(0.99) WITHIN GROUP (
      ORDER BY
        ((UpdatedAtMs - StartedAtMs) / 1000)
    ),
    2
  ) as p99_latency_secs
from
  t1
GROUP BY
  1
