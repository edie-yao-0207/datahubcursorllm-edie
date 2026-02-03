SELECT DISTINCT
  org_id,
  date,
  object_id AS device_id
FROM
  kinesisstats_history.osdtachographvudownload
WHERE
  org_id IS NOT NULL
