-- Split intervals by calendar hour.
with intervals_by_hour as (
  select
    org_id,
    object_id as device_id,
    explode(
      sequence(
        floor(start_ms / 3600000) * 3600000,
        floor((end_ms - 1) / 3600000) * 3600000, -- Sequence stop is inclusive, but end_ms is exclusive.
        3600000
      )
    ) as hour_start_ms,
    start_ms as interval_start_ms,
    end_ms as interval_end_ms,
    fuel_consumed_ml as interval_fuel_consumption_ml,
    gaseous_fuel_consumed_grams as interval_gaseous_fuel_consumed_grams
  from
    engine_state_report.engine_state_pto_air_temp_fuel_intervals
  where
    date >= ${start_date}
    and date < ${end_date}
    and engine_state = 'IDLE'
    and org_id is not null
    and object_id is not null
    and start_ms is not null
    and end_ms is not null
    and (fuel_consumed_ml is not null
    or gaseous_fuel_consumed_grams is not null)
    and COALESCE(original_engine_state_interval_duration_ms, 0) <= 86400000 -- EVEC-2486: Filter events of more than a day. This can because the vehicle lost connectivity or the vehicle is reporting the signals wrong.
)

select
  to_date(from_unixtime(hour_start_ms / 1000)) as date,
  org_id,
  device_id,
  hour_start_ms as start_ms,
  sum(
    least(
      hour_start_ms + 3600000,
      interval_end_ms
    ) - greatest(
      hour_start_ms,
      interval_start_ms
    ) -- The intersection of the hour and the interval.
  ) AS duration_ms,
  sum(
    (
      least(hour_start_ms + 3600000, interval_end_ms) - greatest(hour_start_ms, interval_start_ms)
    ) / (interval_end_ms - interval_start_ms) * interval_fuel_consumption_ml
  ) AS fuel_consumption_ml,
  sum(
    (
      least(hour_start_ms + 3600000, interval_end_ms) - greatest(hour_start_ms, interval_start_ms)
    ) / (interval_end_ms - interval_start_ms) * interval_gaseous_fuel_consumed_grams
  ) AS gaseous_fuel_consumed_grams
from
  intervals_by_hour
group by
  org_id,
  device_id,
  hour_start_ms
