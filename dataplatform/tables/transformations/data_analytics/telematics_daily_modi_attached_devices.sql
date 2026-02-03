--Get device data for all VGs
WITH devices AS (
 SELECT
   a.id AS gateway_id,
   a.device_id,
   a.org_id
  FROM productsdb.gateways AS a
  JOIN productsdb.devices AS b ON
    a.device_id = b.id
  WHERE a.product_id in (24,35,178)
),

--Add dates column to devices list
device_dates AS (
  SELECT dev.*,
    EXPLODE(SEQUENCE(DATE_SUB(${start_date},31), DATE_SUB(${end_date}, 1), INTERVAL 1 day)) AS date
  FROM devices AS dev
),

-- osdattachedusbdevices only gets reported when the vg changes power states or a usb device is plugged/unplugged. So
-- we'll need to fetch the first and last time that objectStat was reported. We'll need to be mindful of the instances where devices
-- last reported osdattachedusbdevices greater than three days ago

-- Fetch the first time the device shows up in the objectStat table witin the last 6 months but not including the last 3 days.
first_last_attached_usb AS (
  SELECT
    org_id,
    object_id AS device_id,
    MIN(date) AS first_reported_date,
    MAX(date) AS last_reported_date,
    MAX((time, value.proto_value.attached_usb_devices.usb_id)).usb_id AS last_reported_usb_id
  FROM kinesisstats.osdattachedusbdevices
  WHERE date >= DATE_SUB(${start_date}, 180)
    AND date < DATE_SUB(${end_date}, 3)
    AND value.is_databreak = false
  GROUP BY
    org_id,
    object_id
),


-- Fetch the last reported date within the time window that overlaps our scheduled refresh. This accounts for
-- an edge case where a device that was was previously offline comes online after the first day of the schedule refresh window.
-- This means we'll need to fetch the last reported start prior to the days considered in the refresh window.
last_attached_usb_overlap AS (
  SELECT
    org_id,
    object_id AS device_id,
    MAX(date) AS last_reported_date_overlap
  FROM kinesisstats.osdattachedusbdevices
  WHERE date >= DATE_SUB(${end_date}, 3)
    AND date < ${end_date}
    AND value.is_databreak = false
  GROUP BY
    org_id,
    object_id
),

-- Fetch count the number of times each device reported the modi usb value.
modi_attached_count AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    SUM(IF (EXISTS(value.proto_value.attached_usb_devices.usb_id, x -> x == 281315232), 1, 0) ) AS modi_attached_count
  FROM kinesisstats.osdattachedusbdevices
  WHERE date >= DATE_SUB(${start_date}, 30)
    AND value.is_databreak = false
  GROUP BY
    date,
    org_id,
    object_id
),

-- Join modi attached counts onto daily intervals table.
modi_devices AS (
  SELECT
    dev.date,
    dev.org_id,
    dev.device_id,
    CASE
      WHEN usb_date.first_reported_date IS NULL THEN false                                                                  -- Flag false if device hasn't reported objectStat within last 6 months
      WHEN dev.date < usb_date.first_reported_date THEN false                                                               -- Flag false if device hasn't yet reported objectStat at given date
      WHEN dev.date > usb_date.last_reported_date THEN IF(EXISTS(usb_date.last_reported_usb_id, x -> x == 281315232), true, false)   -- Flag last reported state if current date is greater than the last reported date
      WHEN overlap.last_reported_date_overlap IS NOT NULL AND dev.date < overlap.last_reported_date_overlap THEN IF(EXISTS(usb_date.last_reported_usb_id, x -> x == 281315232), true, false) -- Edge case when date is less than overlapping last reported date
      WHEN mod.modi_attached_count > 0 THEN true                                                                            -- Flag true if modi_attached_count is greater than 0
      WHEN mod.modi_attached_count = 0 THEN false                                                                           -- Flag false if objectStat was reported and modi device wasn't attached
      ELSE NULL                                                                                                             -- Explicitly marking NULL if above conditions aren't met, this will be filled with the LAST_VALUE() in next code block
    END AS has_modi
  FROM device_dates AS dev
  LEFT JOIN first_last_attached_usb AS usb_date ON
    dev.org_id = usb_date.org_id
    AND dev.device_id = usb_date.device_id
  LEFT JOIN last_attached_usb_overlap AS overlap ON
    dev.org_id = overlap.org_id
    AND dev.device_id = overlap.device_id
  LEFT JOIN modi_attached_count AS mod ON
    dev.date = mod.date
    AND dev.org_id = mod.org_id
    AND dev.device_id = mod.device_id
),

-- To handle devices that parke offline for several days at a time, we'll use the last_value() window function
-- to grab the last reported state.
daily_modi_devices AS (
  SELECT
    date,
    org_id,
    device_id,
    LAST_VALUE(has_modi, true) OVER (PARTITION BY org_id, device_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS has_modi
  FROM modi_devices
)
SELECT *
FROM daily_modi_devices
WHERE date >= ${start_date}
  AND date < ${end_date}
