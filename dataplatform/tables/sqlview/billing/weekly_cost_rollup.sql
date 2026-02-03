select
  cast(date_trunc('WEEK', date) as date) as week,
  team,
  service,
  sum(aws_cost + dbx_cost) as total_cost
from
  billing.dataplatform_costs
group by
  1,
  2,
  3
