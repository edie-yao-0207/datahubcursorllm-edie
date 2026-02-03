-- take the latest complete date from billing.dataplatform_costs_latest_date,
-- subtract 7 and round down to the beginning of the week to get the start date
-- of the last week with complete data.
select
  cast(
    date_trunc('week', date_sub(date, 7)) as date
  ) as date
from
  billing.dataplatform_costs_latest_full_date
