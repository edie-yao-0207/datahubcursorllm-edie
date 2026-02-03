-- PTO is an acronym for Power Take Off.
-- PTO is when a vehicle uses the mechanical power from the engine to power a piece of auxiliary equipment like a tow bed or crane.
-- Each vehicle implements PTO differently. Turning PTO on/off is done through a different auxiliary port in each vehicle.
-- Customers must configure each vehicle in the Samsara web application to define which port is used for PTO.
-- PTO signals are on/off signals that are sent to the Samsara device when the PTO is turned on or off. (research showed they also appear to be reported at regular intervals of about 10 minutes in some cases)
-- There is no stream of data on the PTO state at any given time.
-- In order to determine periods when PTO is off, there are three steps:
-- 1. Query the vehicle configuration to determine which port is used for PTO for any given vehicle.
-- 2. Query the on/off signals for that port on that vehicle
-- 3. Use some look-back logic to find corresponding on/off signals to derive periods where PTO was on
-- Additionally, engine control unit (ECU) PTO is handled entirely differently.
-- ECU PTO is not controlled by an auxiliary port on the vehicle, but rather by the vehicle's engine control unit.
-- Therefore, there is a separate stream of object stats (called osdpowertakeoff) for reporting ECU PTO on/off events.


-- Digital input port on/off values
WITH digio AS (
  SELECT *, 1 AS port FROM kinesisstats.osddigioinput1
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 2 AS port FROM kinesisstats.osddigioinput2
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 3 AS port FROM kinesisstats.osddigioinput3
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 4 AS port FROM kinesisstats.osddigioinput4
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 5 AS port FROM kinesisstats.osddigioinput5
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 6 AS port FROM kinesisstats.osddigioinput6
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 7 AS port FROM kinesisstats.osddigioinput7
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 8 AS port FROM kinesisstats.osddigioinput8
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 9 AS port FROM kinesisstats.osddigioinput9
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
  UNION
  SELECT *, 10 AS port FROM kinesisstats.osddigioinput10
  WHERE TO_DATE(date) >= DATE_SUB(${start_date}, 14)
    AND TO_DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

-- Expand digital input configurations.
expanded_devices AS (
  SELECT
    org_id,
    id AS device_id,
    digi1_type_id,
    digi2_type_id,
    EXPLODE_OUTER(device_settings_proto.digi_inputs.inputs) AS input
  FROM productsdb.devices
  WHERE org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

-- Find all devices and PTO ports and combine port with input type.
pto_devices AS (
  SELECT org_id, device_id, 1 AS port, digi1_type_id AS input_type FROM expanded_devices
  UNION
  SELECT org_id, device_id, 2 AS port, digi2_type_id AS input_type FROM expanded_devices
  UNION
  SELECT org_id, device_id, input.port AS port, input.input.input_type AS input_type FROM expanded_devices
),

ecu_pto_orgs AS (
SELECT
  id
FROM clouddb.organizations
WHERE settings_proto.organization_gateway_settings.ecu_pto_enabled = true
  AND id NOT IN (SELECT * FROM helpers.ignored_org_ids)
),

ecu_pto AS (
  SELECT
    DATE(date) as date,
    org_id,
    object_id as device_id,
    -- ECU PTO doesn't have an associated port and their input device types cannot be configured
    -- on the device configuration page hence we assign -1 to port and input type.
    -1 as port,
    -1 as input_type,
    time,
    value.int_value as value,
    "ecu" as source
  FROM kinesisstats.osdpowertakeoff
  -- Look back 14 days from the start so we can find the previous value
  WHERE DATE(date) >= DATE_SUB(${start_date}, 14)
    AND DATE(date) < ${end_date}
    AND org_id IN (SELECT * FROM ecu_pto_orgs)
),

-- Filter digio on/off values by valid PTO ports and include ecu pto for orgs that have it enabled.
pto_states_with_input_devices AS (
  SELECT
    DATE(date) as date,
    pd.org_id,
    pd.device_id,
    pd.port,
    pd.input_type,
    d.time,
    d.value.int_value AS value,
    "digio" as source
  FROM pto_devices AS pd
  JOIN digio as d
  ON pd.port = d.port
  AND pd.org_id = d.org_id
  AND pd.device_id = d.object_id
  UNION
  -- Combine ECU PTO
  SELECT
    *
  FROM ecu_pto
)

SELECT
  date,
  org_id,
  device_id,
  port,
  time,
  input_type,
  source,
  value,
  is_productive
FROM (
  SELECT
    DATE(date) as date,
    org_id,
    device_id,
    port,
    input_type,
    time,
    value,
    LAG(value) OVER (PARTITION BY org_id, device_id, port ORDER BY time ASC) AS prev_value,
    source,
    CASE
      /*
        The following PTO input device types are considered to be productive since its believed that
        some type of work is being done by the vehicle when they are active.
        Definitions copied from: /go/src/samsaradev.io/hubproto/config.pb.go
        4: PTO
        5: Plow
        6: Sweeper
        7: Salter
        9: Door
        10: Boom
        11: Engine
        20: Virtual ECU PTO
      */
      WHEN input_type IN (4,5,6,7,9,10,11,20) THEN true -- productive PTO input types
      WHEN source = "ecu" THEN true
      ELSE false
    END AS is_productive
  FROM pto_states_with_input_devices
)
WHERE value != prev_value
AND date >= ${start_date}
AND date < ${end_date}
