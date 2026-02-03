SELECT
  org_id,
  date,
  uuid,
  driver_id AS user_id
FROM coachingdb_shards.coachable_item
WHERE item_type = 7  -- EU_HOS_INFRINGEMENT_EVENT
  AND coachable_item_type = 63  -- EU_HOS_INFRINGEMENT
  AND coaching_state IN (10, 100)  -- NEEDS_COACHING or COACHED
