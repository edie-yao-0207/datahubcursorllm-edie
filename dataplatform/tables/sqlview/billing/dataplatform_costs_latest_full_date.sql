-- Look at the last date that has both aws and databricks cost,
-- and subtract 1. It's possible for a day to have partial aws
-- (and maybe partial dbx) data, so we want to err on removing
-- the most recent day.
select
  date_sub(max(date), 1) as date
from
  billing.dataplatform_costs
where
  aws_cost > 0
  and dbx_cost > 0
