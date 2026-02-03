with app_opens as (
    select
      org_id,
      driver_id,
      WINDOW(timestamp, "1 hour").start AS interval_start,
      count(*) as num_app_opens
    from
      datastreams.mobile_logs
    where
        (
          -- change to foreground (active)
          (event_type = "GLOBAL_UPDATE_APP_STATE" and json_params = "\"active\"")
          -- first open
          or event_type = "DRIVER_WAIT_FOR_APP_TO_LOAD"
        )
        and date >= DATE_SUB(${start_date}, 1) -- to capture rows where date comes earlier than timestamp, which is rare and at most 1 day difference
        and date < DATE_ADD(${end_date}, 7) -- to capture rows where date comes much later than timestamps. This is more common.
        and driver_id is not NULL
        and driver_id != 0
        and org_id != 0
    group by
        driver_id, org_id, WINDOW
)

select
  to_date(interval_start) as date,
  org_id,
  driver_id,
  interval_start,
  sum(num_app_opens) as num_app_opens
from
    app_opens
where
    to_date(interval_start) >= ${start_date}
        and to_date(interval_start) < ${end_date}
group by
    date, org_id, driver_id, interval_start
