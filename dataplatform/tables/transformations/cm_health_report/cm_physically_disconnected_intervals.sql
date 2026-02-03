WITH cm_physically_disconnected_intervals_3x AS (
  SELECT
    cm_power_ranges.org_id,
    cm_power_ranges.linked_cm_id,
    cm_power_ranges.device_id,
    GREATEST(
      cm_power_ranges.start_ms,
      attached_cm_usb_ranges.start_ms
    ) AS start_ms,
    LEAST(
      cm_power_ranges.end_ms,
      attached_cm_usb_ranges.end_ms
    ) AS end_ms,
    attached_cm_usb_ranges.date
  FROM
    cm_health_report.cm_power_intervals AS cm_power_ranges
    JOIN cm_health_report.cm_attached_intervals AS attached_cm_usb_ranges ON cm_power_ranges.org_id = attached_cm_usb_ranges.org_id
    AND cm_power_ranges.linked_cm_id = attached_cm_usb_ranges.linked_cm_id
    AND cm_power_ranges.device_id = attached_cm_usb_ranges.device_id
    AND NOT (
      attached_cm_usb_ranges.end_ms <= cm_power_ranges.start_ms
      OR attached_cm_usb_ranges.start_ms >= cm_power_ranges.end_ms
    )
  WHERE
    cm_power_ranges.has_power = false
    AND attached_cm_usb_ranges.attached_to_cm = false
    AND cm_power_ranges.date < ${end_date}
    AND cm_power_ranges.date >= ${start_date}
    AND attached_cm_usb_ranges.date < ${end_date}
    AND attached_cm_usb_ranges.date >= ${start_date}
),
cm_physically_disconnected_intervals_2x AS (
  SELECT
    org_id,
    linked_cm_id,
    device_id,
    start_ms,
    end_ms,
    date
  FROM
    cm_health_report.cm_attached_intervals AS attached_2x_ranges
  WHERE
    linked_cm_id IS NULL
    AND attached_to_cm = false
    AND date < ${end_date}
    AND date >= ${start_date}
)
SELECT
  *
FROM
  cm_physically_disconnected_intervals_2x
UNION
SELECT
  *
FROM
  cm_physically_disconnected_intervals_3x
