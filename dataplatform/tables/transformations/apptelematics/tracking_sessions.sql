with telematics_tracking_logs as (
  select
    *
  from
    datastreams.mobile_logs
  where
    date >= ${start_date}
    AND date < ${end_date}
    and (
      event_type = "DRIVER_TELEMATICS_LOCATION_TRACKING_STARTED"
      or event_type = "DRIVER_TELEMATICS_LOCATION_TRACKING_STOPPED"
    )
),

tracking_sessions as (
  SELECT
    *
  FROM
    (
      SELECT
        date,
        org_id,
        driver_id,
        cast(
          GET_JSON_OBJECT(json_params, "$$.deviceId") as LONG
        ) device_id,
        event_type,
        timestamp AS tracking_start,
        LEAD(event_type) OVER (
          PARTITION by org_id,
          driver_id
          ORDER BY
            timestamp ASC
        ) AS next_event_type,
        LEAD(timestamp) OVER (
          PARTITION by org_id,
          driver_id
          ORDER BY
            timestamp ASC
        ) AS tracking_end
      FROM
        telematics_tracking_logs
    )
  WHERE
    event_type = "DRIVER_TELEMATICS_LOCATION_TRACKING_STARTED"
    and next_event_type = "DRIVER_TELEMATICS_LOCATION_TRACKING_STOPPED"
),

locations as (
  select
    *
  from
    kinesisstats.location
  where
    date >= ${start_date}
    AND date < ${end_date}
    and org_id in (
      select
        org_id
      from
        tracking_sessions
    )
    and device_id in (
      select
        device_id
      from
        tracking_sessions
    )
)

select
  ts.date,
  ts.org_id org_id,
  ts.driver_id driver_id,
  ts.device_id device_id,
  tracking_start,
  tracking_end,
  count(time) num_loc_points
from
  tracking_sessions ts
  left join locations on ts.org_id = locations.org_id
  and ts.device_id = locations.device_id
  and from_unixtime(locations.time / 1000) >= tracking_start
  and from_unixtime(locations.time / 1000) <= tracking_end
  and locations.date >= date(tracking_start)
  and locations.date <= date(tracking_end)
group by
  ts.date,
  ts.org_id,
  ts.driver_id,
  ts.device_id,
  tracking_start,
  tracking_end
