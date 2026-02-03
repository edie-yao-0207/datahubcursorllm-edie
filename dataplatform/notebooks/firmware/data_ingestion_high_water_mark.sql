create or replace temp view jobs as (
  select
    max(change_time) as change_time,
    max_by(job_id, change_time) as job_id,
    lower(name) as name
  from system.lakeflow.jobs
  where lower(name) like "%kinesisstats%"
  group by name
);

create or replace temp view job_runs as (
  select
    name,
    max_by(period_start_time, period_end_time) as period_start_time
  from system.lakeflow.job_run_timeline join jobs using (job_id)
  where termination_code = 'SUCCESS'
  group by
    name
);

create or replace table dataprep_firmware.data_ingestion_high_water_mark as (
  select
    min(unix_millis(period_start_time)) as time_ms
  from job_runs
  where name in (
    -- US
    "kinesisstats-merge-osdaccelerometer-all-us","kinesisstats-merge-osdanomalyevent-all-us","kinesisstats-merge-osdattachedusbdevices-all-us","kinesisstats-merge-osdcm3xaudioalertinfo-all-us","kinesisstats-merge-osdcm3xsystemstats-all-us","kinesisstats-merge-osdcurrentmaptile-all-us","kinesisstats-merge-osddashcamconnected-all-us","kinesisstats-merge-osddashcamstate-all-us","kinesisstats-merge-osddashcamtargetstate-all-us","kinesisstats-merge-osddriverirledenabled-all-us","kinesisstats-merge-osddriversigninstate-all-us","kinesisstats-merge-osdedgespeedlimitv2-all-us","kinesisstats-merge-osdenginegauge-all-us","kinesisstats-merge-osdenginestate-all-us","kinesisstats-merge-osdfirmwaremetrics-all-us","kinesisstats-merge-osdfirmwaretripstate-all-us","kinesisstats-merge-osdgatewayupgradeattempt-all-us","kinesisstats-merge-osdhev1crashdetectionrunning-all-us","kinesisstats-merge-osdhev1harsheventdetectionrunning-all-us","kinesisstats-merge-osdhev2crashdetectionrunning-all-us","kinesisstats-merge-osdhev2harsheventdetectionrunning-all-us","kinesisstats-merge-osdhubserverdeviceheartbeat-all-us","kinesisstats-merge-osdincrementalcellularusage-all-us","kinesisstats-merge-osdlowbridgestrikewarningevent-all-us","kinesisstats-merge-osdmulticamconnected-all-us","kinesisstats-merge-osdobdcableid-all-us","kinesisstats-merge-osdparkingmodestate-all-us","kinesisstats-merge-osdpowerstate-all-us","kinesisstats-merge-osdqrcodescan-all-us","kinesisstats-merge-osdqrcodescandebug-all-us","kinesisstats-merge-osdqrcodescanningstate-all-us","kinesisstats-merge-osdreporteddeviceconfig-all-us","kinesisstats-merge-osdupgradedurations-all-us","kinesisstats-merge-osdvoltageignitiondetections-all-us",
    --EU
    "kinesisstats-merge-osdaccelerometer-all-eu","kinesisstats-merge-osdanomalyevent-all-eu","kinesisstats-merge-osdattachedusbdevices-all-eu","kinesisstats-merge-osdcm3xaudioalertinfo-all-eu","kinesisstats-merge-osdcm3xsystemstats-all-eu","kinesisstats-merge-osdcurrentmaptile-all-eu","kinesisstats-merge-osddashcamconnected-all-eu","kinesisstats-merge-osddashcamstate-all-eu","kinesisstats-merge-osddashcamtargetstate-all-eu","kinesisstats-merge-osddriverirledenabled-all-eu","kinesisstats-merge-osddriversigninstate-all-eu","kinesisstats-merge-osdedgespeedlimitv2-all-eu","kinesisstats-merge-osdenginegauge-all-eu","kinesisstats-merge-osdenginestate-all-eu","kinesisstats-merge-osdfirmwaremetrics-all-eu","kinesisstats-merge-osdfirmwaretripstate-all-eu","kinesisstats-merge-osdgatewayupgradeattempt-all-eu","kinesisstats-merge-osdhev1crashdetectionrunning-all-eu","kinesisstats-merge-osdhev1harsheventdetectionrunning-all-eu","kinesisstats-merge-osdhev2crashdetectionrunning-all-eu","kinesisstats-merge-osdhev2harsheventdetectionrunning-all-eu","kinesisstats-merge-osdhubserverdeviceheartbeat-all-eu","kinesisstats-merge-osdincrementalcellularusage-all-eu","kinesisstats-merge-osdlowbridgestrikewarningevent-all-eu","kinesisstats-merge-osdmulticamconnected-all-eu","kinesisstats-merge-osdobdcableid-all-eu","kinesisstats-merge-osdparkingmodestate-all-eu","kinesisstats-merge-osdpowerstate-all-eu","kinesisstats-merge-osdqrcodescan-all-eu","kinesisstats-merge-osdqrcodescandebug-all-eu","kinesisstats-merge-osdqrcodescanningstate-all-eu","kinesisstats-merge-osdreporteddeviceconfig-all-eu","kinesisstats-merge-osdupgradedurations-all-eu","kinesisstats-merge-osdvoltageignitiondetections-all-eu")
);