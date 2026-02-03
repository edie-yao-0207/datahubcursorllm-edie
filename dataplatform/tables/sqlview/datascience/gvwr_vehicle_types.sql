with vin_make_model_weight_counts as (
-- Make model weights from vindb.
  select
    trim(lower(make)) as make,
    trim(lower(model)) as model,
    max_weight_lbs,
    count(*) as count
  from
    vindb_shards.device_vin_metadata
  group by
    trim(lower(make)),
    trim(lower(model)),
    max_weight_lbs
),
vin_most_common_make_model_weights as (
-- When there are multiple weights for a single make and model in vindb, pick the weight with the most devices.
  select
    make,
    model,
    max_by(max_weight_lbs, count) as max_weight_lbs
  from
    vin_make_model_weight_counts
  group by
    make,
    model
),
cloud_make_model_weights as (
-- Make model weights from clouddb.
  select
    trim(lower(make)) as make,
    trim(lower(model)) as model,
    max_weight_lbs
  from
    clouddb.vehicle_make_model_weights
),
all_make_model_weights as (
-- Union clouddb and vindb make model weights.
  select
    *
  from
    vin_most_common_make_model_weights
  union all
  select
    *
  from
    cloud_make_model_weights
)
-- Group the vindb and clouddb make and model and take the max weight between vindb and clouddb.
-- This accounts for cases where vindb has a make and model that clouddb does not or visa versa.
-- This also accounts for cases where both vindb and clouddb have a make model, but one weight is zero and the other is non-zero.
select
  make,
  model,
  case
    when max(max_weight_lbs) < 6000
    and max(max_weight_lbs) > 0 then "passenger"
    when max(max_weight_lbs) >= 6000
    and max(max_weight_lbs) < 26000 then "light"
    when max(max_weight_lbs) >= 26000 then "heavy"
    else "unknown"
  end as gvwr_vehicle_type
from
  all_make_model_weights
group by
  make,
  model
