(
  select firmware_build
  from dataprep_firmware.all_cm_firmware_builds
)
union
(
  select 'all' as firmware_build
)
order by firmware_build desc
