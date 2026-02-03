spark.sql(
    """
create table if not exists datascience.org_hev2_opt_in using delta as (
  select
    o.id as org_id,
    fps.updated_at as date,
    fps.enabled as hev2_enabled,
    current_date() as _written
  FROM
    clouddb.organizations as o
    left outer join releasemanagementdb_shards.feature_package_self_serve as fps on fps.org_id = o.id
  where
    lower(fps.feature_package_uuid) = 'af02640685ae4549af78fbc5584db713' --hev2
    and o.internal_type = 0 -- Customer Orgs
    and fps.user_id = 0
)
"""
)


spark.sql(
    """
with updates as (
  SELECT
    o.id as org_id,
    fps.updated_at as date,
    fps.enabled as hev2_enabled,
    current_date() as _written
  FROM
    clouddb.organizations as o
    JOIN releasemanagementdb_shards.feature_package_self_serve as fps on fps.org_id = o.id
    AND lower(fps.feature_package_uuid) = 'af02640685ae4549af78fbc5584db713' --hev2
    AND o.internal_type = 0 -- Customer Orgs
    AND fps.user_id = 0
  WHERE
    fps.updated_at >= date_sub(current_date(), 7) -- use 7 days in case there is a lag in the snapshot table
)

MERGE INTO datascience.org_hev2_opt_in AS target USING updates ON target.date = updates.date
AND target.hev2_enabled = updates.hev2_enabled
AND target.org_id = updates.org_id
WHEN NOT MATCHED THEN
INSERT
  *
"""
)
