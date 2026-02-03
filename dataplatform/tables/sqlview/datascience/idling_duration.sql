WITH ignored_org_ids AS (
  SELECT
    EXPLODE(
      ARRAY(
        1,
        43810,
        43509,
        43811,
        43518,
        43519,
        43511,
        43512,
        43513,
        43514,
        43801,
        43515,
        43516,
        43517,
        5289,
        1782
      )
    ) AS org_id
),
engine_states AS (
  SELECT
    es.org_id,
    object_id,
    time,
    date,
    CASE
      -- A databreak means the previous value should be considered an "end" which corresponds with the OFF state (0).
      WHEN LEAD(value.is_databreak) OVER (
        PARTITION BY es.org_id,
        object_id
        ORDER BY
          time ASC
      ) THEN 0 -- Otherwise, just use the int_value.
      ELSE value.int_value
    END AS int_value,
    LAG(value.int_value) OVER (
      PARTITION BY es.org_id,
      object_id
      ORDER BY
        time ASC
    ) AS prev_int_value,
    value.is_databreak AS is_databreak
  FROM
    kinesisstats.osdenginestate as es
    join ignored_org_ids as i on es.org_id != i.org_id
  WHERE
    date >= add_months(current_date(), -12)
),
-- Skip engine state points with no state changes.
engine_state_transitions AS (
  SELECT
    org_id,
    object_id,
    time,
    date,
    int_value
  FROM
    engine_states
  WHERE
    int_value != prev_int_value
    AND NOT is_databreak
),
-- Map engine state transitions to intervals.
engine_state_intervals AS (
  SELECT
    org_id,
    object_id,
    date,
    time AS start_ms,
    CASE
      WHEN int_value = 0 THEN "OFF"
      WHEN int_value = 1 THEN "ON"
      WHEN int_Value = 2 THEN "IDLE"
    END AS state,
    COALESCE(
      LEAD(time) OVER (
        PARTITION BY org_id,
        object_id
        ORDER BY
          time ASC
      ),
      CAST(
        unix_timestamp(CAST (current_date() AS TIMESTAMP)) * 1000 AS BIGINT
      )
    ) AS end_ms
  FROM
    engine_state_transitions
),
-- Expand engine state intervals by covering hours.
engine_state_intervals_by_covering_hour AS (
  SELECT
    org_id,
    object_id,
    state,
    date,
    start_ms,
    end_ms,
    EXPLODE(
      SEQUENCE(
        DATE_TRUNC('hour', from_unixtime(start_ms / 1000)),
        DATE_TRUNC('hour', from_unixtime(end_ms / 1000)),
        -- SEQUENCE() takes inclusive end time.
        INTERVAL 1 hour
      )
    ) AS hour_start
  FROM
    engine_state_intervals
  WHERE
    start_ms < end_ms
),
-- Split engine state intervals by hour.
engine_state_intervals_by_hour AS (
  SELECT
    org_id,
    object_id,
    state,
    date,
    GREATEST(
      start_ms,
      CAST(unix_timestamp(hour_start) * 1000 AS BIGINT)
    ) AS start_ms,
    LEAST(
      end_ms,
      CAST(
        unix_timestamp(hour_start + INTERVAL 1 HOUR) * 1000 AS BIGINT
      )
    ) AS end_ms
  FROM
    engine_state_intervals_by_covering_hour
),
engine_duration_by_day as (
  select
    org_id,
    date,
    sum(
      case
        when state = "ON"
        or state = "IDLE" then end_ms - start_ms
        else 0
      end
    ) as on_duration_ms,
    sum(
      case
        when state = "IDLE" then end_ms - start_ms
        else 0
      end
    ) as idle_duration_ms
  from
    engine_state_intervals_by_hour
  group by
    org_id,
    date
), -- ==========================
-- = Calculate PTO Duration =
-- ==========================
-- Valid PTO equipment types.
pto_device_types AS (
  SELECT
    EXPLODE(ARRAY(4, 5, 6, 7, 9, 10, 11)) as type_id
),
-- Expand digital input configurations.
expanded_devices AS (
  SELECT
    d.org_id AS org_id,
    d.id AS device_id,
    d.digi1_type_id,
    d.digi2_type_id,
    EXPLODE_OUTER(d.device_settings_proto.digi_inputs.inputs) AS input
  FROM
    productsdb.devices AS d
),
-- Find all devices and PTO ports.
pto_devices AS (
  SELECT
    org_id,
    device_id,
    1 AS port
  FROM
    expanded_devices e
    JOIN pto_device_types p ON e.digi1_type_id = p.type_id
  UNION
  SELECT
    org_id,
    device_id,
    2 AS port
  FROM
    expanded_devices e
    JOIN pto_device_types p ON e.digi2_type_id = p.type_id
  UNION
  SELECT
    org_id,
    device_id,
    input.port AS port
  FROM
    expanded_devices e
    JOIN pto_device_types p ON e.input.input.input_type = p.type_id
),
-- Digital input port on/off values.
digio AS (
  SELECT
    *,
    1 AS port
  FROM
    kinesisstats.osddigioinput1
  UNION
  SELECT
    *,
    2 AS port
  FROM
    kinesisstats.osddigioinput2
  UNION
  SELECT
    *,
    3 AS port
  FROM
    kinesisstats.osddigioinput3
  UNION
  SELECT
    *,
    4 AS port
  FROM
    kinesisstats.osddigioinput4
  UNION
  SELECT
    *,
    5 AS port
  FROM
    kinesisstats.osddigioinput5
  UNION
  SELECT
    *,
    6 AS port
  FROM
    kinesisstats.osddigioinput6
  UNION
  SELECT
    *,
    7 AS port
  FROM
    kinesisstats.osddigioinput7
  UNION
  SELECT
    *,
    8 AS port
  FROM
    kinesisstats.osddigioinput8
  UNION
  SELECT
    *,
    9 AS port
  FROM
    kinesisstats.osddigioinput9
  UNION
  SELECT
    *,
    10 AS port
  FROM
    kinesisstats.osddigioinput10
),
-- Filter digio on/off values by valid PTO ports.
pto_port_states AS (
  SELECT
    pd.org_id,
    pd.device_id,
    pd.port,
    d.date,
    d.time,
    d.value.int_value AS value,
    LAG(d.value.int_value) OVER (
      PARTITION BY pd.org_id,
      pd.device_id,
      pd.port
      ORDER BY
        d.time ASC
    ) AS prev_value
  FROM
    pto_devices AS pd
    JOIN digio as d ON pd.port = d.port
    AND pd.org_id = d.org_id
    AND pd.device_id = d.object_id
    JOIN ignored_org_ids as i ON d.org_id != i.org_id
  WHERE
    date >= add_months(current_date(), -12)
),
-- Filter PTO values for ON/OFF transitions per port.
pto_port_state_transitions AS (
  SELECT
    org_id,
    device_id,
    port,
    date,
    time,
    value
  FROM
    pto_port_states
  WHERE
    value != prev_value
),
-- Combine PTO intervals.
-- For example, a device has two equipments:
--   equipment A is in use on port 1 between (10, 50),
--   equipment B is in use on port 2 between (30, 70),
-- we want to find the union of these intervals, i.e. (10, 70),
-- when we calculate PTO intervals, so we don't double count
-- duration (30, 50) when we report total ON time.
--
-- Perform a running sum of "transition" for each device.
-- The total represents "number of overlapping intervals".
-- When total=1, start an interval. When total=0, close the interval.
pto_transition_running_sum AS (
  SELECT
    org_id,
    device_id,
    date,
    time,
    SUM(
      CASE
        WHEN value = 1 THEN 1 -- When the first value is not equal to 1, avoid starting the total at -1 by returning 0 instead.
        WHEN time = FIRST_VALUE(time) OVER (
          PARTITION BY org_id,
          device_id
          ORDER BY
            time
        ) THEN 0
        ELSE -1
      END
    ) OVER (
      PARTITION BY org_id,
      device_id
      ORDER BY
        time ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS total
  FROM
    pto_port_state_transitions
),
-- Filter open and close events.
pto_open_close AS (
  SELECT
    org_id,
    device_id,
    date,
    time,
    total
  FROM
    pto_transition_running_sum
  WHERE
    total = 0
    OR total = 1
),
pto_open_close_with_next_time AS (
  SELECT
    org_id,
    device_id,
    total,
    date,
    time AS start_ms,
    COALESCE(
      LEAD(time) OVER (
        PARTITION BY org_id,
        device_id
        ORDER BY
          time ASC
      ),
      CAST(
        unix_timestamp(CAST (current_date() AS TIMESTAMP)) * 1000 AS BIGINT
      )
    ) AS end_ms
  FROM
    pto_open_close
),
pto_on_intervals AS (
  SELECT
    org_id,
    device_id,
    date,
    start_ms,
    end_ms
  FROM
    pto_open_close_with_next_time
  WHERE
    total = 1
),
-- Filter engine state intervals by IDLE state.
engine_idle_intervals AS (
  SELECT
    *
  FROM
    engine_state_intervals
  WHERE
    state = "IDLE"
),
-- Find overlap between engine idle intervals and PTO intervals.
aux_engine_state_intervals AS (
  SELECT
    /*+ RANGE_JOIN(idle, 3600000) */
    pto.org_id,
    pto.device_id,
    pto.date,
    GREATEST(pto.start_ms, idle.start_ms) AS interval_start,
    LEAST(pto.end_ms, idle.end_ms) AS interval_end
  FROM
    pto_on_intervals pto
    JOIN engine_idle_intervals idle ON pto.org_id = idle.org_id
    AND pto.device_id = idle.object_id
    AND pto.start_ms <= idle.end_ms
    AND pto.end_ms >= idle.start_ms
),
-- Expand aux_engine_state_intervals by covering hours.
aux_engine_state_intervals_by_covering_hour AS (
  SELECT
    org_id,
    device_id,
    date,
    interval_start,
    interval_end,
    EXPLODE(
      SEQUENCE(
        DATE_TRUNC('hour', FROM_UNIXTIME(interval_start / 1000)),
        DATE_TRUNC('hour', FROM_UNIXTIME(interval_end / 1000)),
        -- SEQUENCE() takes inclusive end time.
        INTERVAL 1 hour
      )
    ) AS hour_start
  FROM
    aux_engine_state_intervals
  WHERE
    interval_start < interval_end
),
-- Calculate aux_engine_state intervals, split at hour boundary.
aux_engine_state_intervals_by_hour AS (
  SELECT
    org_id,
    device_id,
    date,
    hour_start AS interval_start,
    GREATEST(
      interval_start,
      UNIX_TIMESTAMP(hour_start) * 1000
    ) AS aux_during_idle_start_ms,
    LEAST(
      interval_end,
      (UNIX_TIMESTAMP(hour_start) + 3600) * 1000
    ) AS aux_during_idle_end_ms
  FROM
    aux_engine_state_intervals_by_covering_hour
),
aux_engine_state_duration_by_day as (
  select
    org_id,
    date,
    sum(
      aux_during_idle_end_ms - aux_during_idle_start_ms
    ) as aux_during_idle_ms
  from
    aux_engine_state_intervals_by_hour
  group by
    org_id,
    date
),
idling_duration_by_org_by_day as (
  select
    ed.org_id,
    ed.date,
    on_duration_ms,
    -- subtract aux_during_idle_ms because it represents PTO usage time. In other
    -- words, the vehicle is not moving, but the engine is in use powering
    -- external equipment like a boom arm.
    idle_duration_ms - coalesce(aux_during_idle_ms, 0) as idle_duration_ms
  from
    engine_duration_by_day as ed
    left join aux_engine_state_duration_by_day as ad on ed.org_id = ad.org_id
    and ed.date = ad.date
  where
    on_duration_ms > 0
)
select
  o.id as org_id,
  c.date,
  coalesce(on_duration_ms, 0) as on_duration_ms,
  coalesce(idle_duration_ms, 0) as idle_duration_ms
from
  clouddb.organizations as o
  join definitions.445_calendar as c
  left join idling_duration_by_org_by_day as m on m.org_id = o.id
  and m.date = c.date
where
  c.date between add_months(current_date(), -12)
  and current_date()
