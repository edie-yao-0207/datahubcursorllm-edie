(
  select string(org_id) as org_id
  from dataprep_firmware.all_org_ids
)
union
(
  select 'all' as org_id
)
order by org_id desc
