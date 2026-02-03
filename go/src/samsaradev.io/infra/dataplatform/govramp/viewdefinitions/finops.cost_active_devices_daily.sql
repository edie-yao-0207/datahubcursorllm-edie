SELECT
  active_date,
  eom,
  fiscal_year,
  quarter,
  eoq,
  device_region,
  product_id,
  name,
  product_family,
  active_device_days
from
  finops.cost_active_devices_daily_pre_aug_2024
where
  active_date < '2024-08-01'
UNION
--  beginning in August 2024 used cross regional dataset (excludes internal devices) but includes both NA/EU devices in the cloud with metrics_repo.count_devices_active https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,metrics_api.count_devices_active,PROD)/Schema?is_lineage_mode=false&schemaFilter= which is based on this definition of active (trips + heartbeats for VG/CM) - https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,datamodel_core.lifetime_device_activity,PROD)/Documentation?is_lineage_mode=false
SELECT
  to_date(cad.date) as active_date,
  -- Specify the table alias here
  eom,
  fiscal_year,
  quarter,
  eoq,
  region as device_region,
  product_id,
  device_type as name,
  SUBSTRING(device_type, 1, 2) AS product_family,
  count_devices as active_device_days
from
  (
    SELECT
      date,
      product_id,
      device_type,
      region,
      COUNT(DISTINCT device_id) AS count_devices
    FROM
      dataengineering.device_activity_global
    WHERE
      is_device_active = 1
      and date between '2024-08-01' and current_date()
    GROUP BY
      1,
      2,
      3,
      4
  ) cad
    left join definitions.445_calendar cal
      on cad.date = cal.date
