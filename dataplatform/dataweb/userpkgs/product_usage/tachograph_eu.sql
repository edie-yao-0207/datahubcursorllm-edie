SELECT DISTINCT
  org_id,
  date,
  object_id AS device_id
FROM
  delta.`s3://samsara-eu-kinesisstats-delta-lake/table/deduplicated/osDTachographVUDownload`
WHERE
  org_id IS NOT NULL
