(
  select cm_product_program_id
  from dataprep_firmware.all_cm_product_program_ids
)
union
(
  select 'all' as cm_product_program_id
)
order by cm_product_program_id desc
