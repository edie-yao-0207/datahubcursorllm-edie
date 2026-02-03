WITH
  valid_status_stats AS (
    SELECT
      *
    FROM
      kinesisstats.osdevchargingstatus
    WHERE
      date >= DATE_SUB(${start_date}, 2)
      AND date < ${end_date}
      AND value.is_end != true
      AND value.is_databreak != true
  ),
  status_stats_with_previous AS (
    SELECT
      date,
      org_id,
      object_id,
      time,
      value.int_value as current_status,
      LAG(value.int_value) OVER (
        PARTITION BY object_id
        ORDER BY
          time
      ) AS prev_status,
      LAG(time) OVER (
        PARTITION BY object_id
        ORDER BY
          time
      ) AS prev_time
    FROM
      valid_status_stats
  ),
  status_transitions AS (
    SELECT
      date,
      org_id,
      object_id,
      time,
      CASE
        WHEN current_status = 2
        AND prev_status != 2 THEN "STARTED_CHARGING"
        WHEN current_status != 2
        AND prev_status = 2 THEN "STOPPED_CHARGING"
        ELSE "UNKNOWN_TRANSITION"
      END AS transition
    FROM
      status_stats_with_previous
    WHERE
      current_status != prev_status
  ),
  transitions_with_next AS (
    SELECT
      date,
      org_id,
      object_id,
      transition,
      time AS start_ms,
      LEAD(time) OVER (
        PARTITION BY object_id
        ORDER BY
          time
      ) AS end_ms
    FROM
      status_transitions
  ),
  started_charging_transitions AS (
    SELECT
      *
    FROM
      transitions_with_next
    WHERE
      transition = "STARTED_CHARGING"
  ),
  charge_events AS (
    SELECT
      date,
      org_id,
      object_id,
      start_ms,
      end_ms
    FROM
      started_charging_transitions
    WHERE
      end_ms IS NOT NULL
    ORDER BY
      start_ms
  ),
  charge_events_with_previous_time AS (
    SELECT
      *,
      LAG (end_ms) OVER (
        PARTITION BY object_id
        ORDER BY
          start_ms ASC
      ) AS last_event_time
    FROM
      charge_events
  ),
  charge_events_with_should_stitch_with_previous AS (
    SELECT
      date,
      org_id,
      object_id,
      start_ms,
      end_ms,
      CASE
        WHEN (start_ms - last_event_time < 300000) THEN true
        ELSE false
      END AS should_stitch_with_previous
    FROM
      charge_events_with_previous_time
    ORDER BY
      object_id,
      start_ms
  ),
  -- The below statement solves the islands and gaps problem
  -- In this case we have charge events that occur so close together (<90s) that they should be combined into one charge event
  -- The below statement combines multiple events that should be stitched into the previous event, into one event that should be stitched into the earliest event in the chain
  -- It does this by partitioning by object_id, ordering by start_ms and getting the row number of each event
  -- It then partitions by object_id and should_stitch, ordering by start_ms and getting the row number of each event
  -- If two rows are within the same "island", then the difference in these two row numbers that we found will be the same for both rows. This gives us a group number.
  -- We can then group by this group number and get the earliest time and latest time within the group to form the start and end time for the combined event.
  contiguous_should_stitches_stitched_to_one_should_stitch AS (
    SELECT
      MIN(date) as date,
      org_id,
      object_id,
      MIN(start_ms) AS start_ms,
      MAX(end_ms) AS end_ms,
      should_stitch_with_previous
    FROM
      (
        SELECT
          t.*,
          row_number() OVER (
            PARTITION BY object_id
            ORDER BY
              start_ms
          ) AS event_count,
          row_number() OVER (
            PARTITION BY object_id,
            should_stitch_with_previous
            ORDER BY
              start_ms
          ) AS event_in_group_count
        FROM
          charge_events_with_should_stitch_with_previous t
      ) t
    GROUP BY
      org_id,
      object_id,
      should_stitch_with_previous,
      event_count - event_in_group_count
  ),
  -- While we combined the should_stitches, we may also have combined events where should_stitch is false. We do not want to do that is these events are distinct.
  -- So we discard them.
  discrete_should_stitches AS (
    SELECT
      *
    FROM
      contiguous_should_stitches_stitched_to_one_should_stitch
    WHERE
      should_stitch_with_previous = true
  ),
  -- We then recombine the combined should stitch events with the uncorrupted should_stitch false events.
  recombined_charge_event_parents_and_should_stitches AS (
    SELECT
      *
    FROM
      discrete_should_stitches
    UNION
    SELECT
      *
    FROM
      charge_events_with_should_stitch_with_previous
    WHERE
      should_stitch_with_previous == false
  ),
  -- We can then combine should stitch events with their parent event as we now know a parent event will only have one child event to stitch with.
  stitched_charge_events_with_should_stich_artifacts AS (
    SELECT
      date,
      org_id,
      object_id,
      start_ms,
      should_stitch_with_previous,
      CASE
        WHEN (
          LEAD (should_stitch_with_previous) OVER (
            PARTITION BY object_id
            ORDER BY
              start_ms ASC
          )
        ) THEN (
          LEAD (end_ms) OVER (
            PARTITION BY object_id
            ORDER BY
              start_ms ASC
          )
        )
        ELSE end_ms
      END as end_ms
    FROM
      recombined_charge_event_parents_and_should_stitches
  ),
  charge_events_over_five_minutes AS (
    SELECT
      date,
      org_id,
      object_id,
      start_ms,
      end_ms
    FROM
      stitched_charge_events_with_should_stich_artifacts
    WHERE
      should_stitch_with_previous == false
      AND (end_ms - start_ms) > 300000
  ),
  charge_events_within_timeframe AS (
    SELECT
      *
    FROM
      charge_events_over_five_minutes
    WHERE
      date >= ${start_date}
      AND date < ${end_date}
  )

SELECT
  *
FROM
  charge_events_within_timeframe
