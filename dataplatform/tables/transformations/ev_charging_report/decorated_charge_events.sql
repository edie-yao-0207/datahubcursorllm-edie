WITH
  charge_events_within_timeframe AS (
    SELECT
      *
    FROM
      ev_charging_report.charge_events
    WHERE
      date >= ${start_date}
      AND date < ${end_date}
  ),
  started_charging_transitions_filtered AS (
    SELECT
      object_id,
      start_ms as time,
      "STARTED_CHARGING" AS transition
    FROM
      charge_events_within_timeframe
  ),
  stopped_charging_transitions_filtered AS (
    SELECT
      object_id,
      end_ms as time,
      "STOPPED_CHARGING" AS transition
    FROM
      charge_events_within_timeframe
  ),


  valid_soc_stats AS (
    SELECT
      *
    FROM
      kinesisstats.osdevusablestateofchargemillipercent
    WHERE
      date >= DATE_SUB(${start_date}, 1)
      AND date < DATE_ADD(${end_date}, 1)
      AND value.is_end != true
      AND value.is_databreak != true
  ),
  started_charging_transitions_x_earlier_socs AS (
    SELECT
      ROW_NUMBER() OVER (
        PARTITION BY transitions.object_id,
        transitions.time
        ORDER BY
          valid_soc_stats.time DESC
      ) AS row_num,
      transitions.*,
      valid_soc_stats.value.int_value AS soc_percent,
      valid_soc_stats.time AS soc_time
    FROM
      started_charging_transitions_filtered as transitions
      INNER JOIN valid_soc_stats ON valid_soc_stats.object_id = transitions.object_id
      AND valid_soc_stats.time <= transitions.time
  ),
  started_charging_transitions_with_start_soc AS (
    SELECT
      object_id,
      transition,
      time,
      soc_percent / 1000 AS soc_percent
    FROM
      started_charging_transitions_x_earlier_socs
    WHERE
      row_num = 1
  ),

  stopped_charging_transitions_x_later_socs AS (
    SELECT
      ROW_NUMBER() OVER (
        PARTITION BY transitions.object_id,
        transitions.time
        ORDER BY
          valid_soc_stats.time ASC
      ) AS row_num,
      transitions.*,
      valid_soc_stats.value.int_value AS soc_percent,
      valid_soc_stats.time AS soc_time
    FROM
      stopped_charging_transitions_filtered as transitions
      INNER JOIN valid_soc_stats ON valid_soc_stats.object_id = transitions.object_id
      AND valid_soc_stats.time >= transitions.time
  ),
  stopped_charging_transitions_with_end_soc AS (
    SELECT
      object_id,
      transition,
      time,
      soc_percent / 1000 AS soc_percent
    FROM
      stopped_charging_transitions_x_later_socs
    WHERE
      row_num = 1
  ),

 events_with_max_soc AS (
    SELECT
      charge_event.org_id,
      charge_event.object_id,
      charge_event.start_ms,
      charge_event.end_ms,
      MAX(valid_soc_stats.value.int_value) / 1000 AS max_soc_in_event
    FROM
      charge_events_within_timeframe AS charge_event
      LEFT JOIN valid_soc_stats
      ON valid_soc_stats.object_id = charge_event.object_id
      AND valid_soc_stats.time <= charge_event.end_ms
      AND valid_soc_stats.time >= charge_event.start_ms
    GROUP BY charge_event.org_id, charge_event.object_id, charge_event.start_ms, charge_event.end_ms
  ),


  valid_delta_energy_stats AS (
    SELECT
      *
    FROM
      kinesisstats.osddeltaevchargingenergymicrowh
    WHERE
      date >= ${start_date}
      AND date < ${end_date}
      AND value.is_end != true
      AND value.is_databreak != true
  ),

 events_with_energy_charged AS (
    SELECT
      charge_event.org_id,
      charge_event.object_id,
      charge_event.start_ms,
      charge_event.end_ms,
      SUM(valid_delta_energy_stats.value.int_value) AS energy_charged_in_event
    FROM
      charge_events_within_timeframe AS charge_event
      LEFT JOIN valid_delta_energy_stats
      ON valid_delta_energy_stats.object_id = charge_event.object_id
      AND valid_delta_energy_stats.time < charge_event.end_ms + 300000 -- 5 minute charging grace period, aligns with energy accumulator grace period.
      AND valid_delta_energy_stats.time >= charge_event.start_ms
    GROUP BY charge_event.org_id, charge_event.object_id, charge_event.start_ms, charge_event.end_ms
  ),


  recent_location_stats AS (
    SELECT
      device_id,
      time,
      value.longitude,
      value.latitude,
      rtrim(
        concat(
          concat_ws(
            ' ',
            value.revgeo_street,
            value.revgeo_city
          ),
          ', ',
          concat_ws(
            ' ',
            value.revgeo_state,
            value.revgeo_postcode
          )
        )
      ) as address
    FROM
      kinesisstats.location
    WHERE
      date >= DATE_SUB(${start_date}, 1)
      AND date < ${end_date}
  ),
  started_charging_transitions_x_earlier_locations AS (
    SELECT
      ROW_NUMBER() OVER (
        PARTITION BY transitions.object_id,
        transitions.time
        ORDER BY
          recent_location_stats.time DESC
      ) AS row_num,
      transitions.*,
      recent_location_stats.longitude,
      recent_location_stats.latitude,
      recent_location_stats.address
    FROM
      started_charging_transitions_filtered AS transitions
      INNER JOIN recent_location_stats ON recent_location_stats.device_id = transitions.object_id
      AND recent_location_stats.time <= transitions.time
  ),
  started_charging_transitions_with_location AS (
    SELECT
      object_id,
      transition,
      time,
      latitude,
      longitude,
      address
    FROM
      started_charging_transitions_x_earlier_locations
    WHERE
      row_num = 1
  ),
  decorated_charge_events AS (
    SELECT
      charge_events.date,
      charge_events.org_id,
      charge_events.object_id as device_id,
      charge_events.start_ms,
      charge_events.end_ms,
      s_soc.soc_percent AS start_soc,
      CASE
        WHEN (e_soc.soc_percent IS NULL) THEN max_soc.max_soc_in_event
        WHEN (max_soc.max_soc_in_event IS NULL) THEN e_soc.soc_percent
        WHEN (max_soc.max_soc_in_event > e_soc.soc_percent) THEN max_soc.max_soc_in_event
        ELSE e_soc.soc_percent
      END AS end_soc,
      loc.address,
      loc.latitude,
      loc.longitude,
      energy_charged.energy_charged_in_event / 1000000000 AS energy_charged_kwh
    FROM
      charge_events_within_timeframe AS charge_events
      INNER JOIN events_with_max_soc AS max_soc ON max_soc.object_id = charge_events.object_id
      AND max_soc.start_ms = charge_events.start_ms
      AND max_soc.end_ms = charge_events.end_ms
      INNER JOIN events_with_energy_charged AS energy_charged ON energy_charged.object_id = charge_events.object_id
      AND energy_charged.start_ms = charge_events.start_ms
      AND energy_charged.end_ms = charge_events.end_ms
      LEFT JOIN stopped_charging_transitions_with_end_soc AS e_soc ON e_soc.object_id = charge_events.object_id
      AND e_soc.time = charge_events.end_ms
      LEFT JOIN started_charging_transitions_with_start_soc AS s_soc ON s_soc.object_id = charge_events.object_id
      AND s_soc.time = charge_events.start_ms
      LEFT JOIN started_charging_transitions_with_location AS loc ON loc.object_id = charge_events.object_id
      AND loc.time = charge_events.start_ms
    ORDER BY
      device_id,
      start_ms
  )

SELECT
  *
FROM
  decorated_charge_events
