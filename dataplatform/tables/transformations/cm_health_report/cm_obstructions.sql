WITH cm_obstructions AS (
  SELECT
    org_id,
    device_id,
    max(
      CASE
        WHEN detail_proto.accel_type = '25' THEN event_ms
      END
    ) AS latest_inward_detection_ms,
    max(
      CASE
        WHEN detail_proto.accel_type = '29' THEN event_ms
      END
    ) AS latest_outward_detection_ms,
    max(
      CASE
        WHEN detail_proto.accel_type = '31' THEN event_ms
      END
    ) AS latest_misaligned_detection_ms
  FROM
    safetydb_shards.safety_events
  WHERE
    date >= date_sub(${start_date}, 5)
    AND date < ${end_date}
  GROUP BY
    org_id,
    device_id
)

SELECT
  org_id,
  device_id,
  latest_inward_detection_ms,
  latest_outward_detection_ms,
  latest_misaligned_detection_ms
FROM
  cm_obstructions
WHERE
  latest_outward_detection_ms IS NOT NULL
  OR latest_inward_detection_ms IS NOT NULL
  OR latest_misaligned_detection_ms IS NOT NULL
