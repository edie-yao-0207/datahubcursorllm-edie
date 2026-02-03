select
  a.org_id,
  a.date,
  total_harsh_accel as harsh_accel,
  total_harsh_brake as harsh_brake,
  total_harsh_turn as harsh_turn,
  total_harsh_accel + total_harsh_brake + total_harsh_turn as harsh_events_all,
  total_distance_mi/1000 as total_dist_1000mi
from
  datascience.harsh_accel as a
  join datascience.harsh_brake as b on a.org_id = b.org_id
  and a.date = b.date
  join datascience.harsh_turn as t on a.org_id = t.org_id
  and a.date = t.date
  join datascience.org_trips as o on a.org_id = o.org_id
  and a.date = o.date
