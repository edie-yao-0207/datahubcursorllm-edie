(
  select firmware_build
  from dataprep_firmware.all_vg_firmware_builds
)
union
(
  select 'all' as firmware_build
)
order by firmware_build desc
