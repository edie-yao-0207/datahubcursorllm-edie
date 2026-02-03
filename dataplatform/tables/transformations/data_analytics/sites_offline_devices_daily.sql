WITH sites_gateway_devices AS (
--cmd2
-- Sites Devices Metadata
  SELECT
    ad.date,
    ad.org_id AS org_id,
    cm.sam_number,
    o.name AS org_name,
    o.internal_type AS internal_org,
    nvl(d.name, d.serial) AS device_name,
    ad.device_id,
    ad.product_id,
    case when d.variant_id = 0 then "VariantNone"
    when d.variant_id = 1 then "VariantVg34M"
    when d.variant_id = 2 then "VariantVg54NaE"
    when d.variant_id = 3 then "VariantVg54Fn"
    when d.variant_id = 4 then "VariantVms32TB"
    when d.variant_id = 5 then "VariantSg1x"
    when d.variant_id = 6 then "VariantSg1g"
    when d.variant_id = 7 then "VariantIgAiVin60"
    when d.variant_id = 8 then "VariantIgDioVin60"
    else d.variant_id end as variant_type
  FROM dataprep.active_devices ad
    LEFT JOIN clouddb.organizations o
      ON ad.org_id = o.id
    LEFT JOIN productsdb.devices d
      ON ad.org_id = d.org_id AND
        ad.device_id = d.id
    LEFT JOIN dataprep.customer_metadata cm
      ON o.id = cm.org_id
  WHERE d.product_id = 55 -- product_id '55' is only id for sites devices?
     AND ad.date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
  ),

dates AS (
  SELECT date
  FROM definitions.445_calendar
  WHERE date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
),

heartbeats_hub AS (
  SELECT
    sgd.date,
    sgd.org_id,
    sgd.sam_number,
    sgd.org_name,
    sgd.internal_org,
    sgd.device_id,
    sgd.device_name,
    sgd.product_id,
    sgd.variant_type,
    hhb.value.proto_value.hub_server_device_heartbeat.connection.device_hello.build AS fw,
    hhb.time AS heartbeat_time_ms
  FROM sites_gateway_devices AS sgd
  LEFT JOIN kinesisstats.osdhubserverdeviceheartbeat hhb
    ON sgd.org_id = hhb.org_id
      AND sgd.device_id = hhb.object_id
      AND sgd.date = to_date(from_utc_timestamp(to_timestamp(from_unixtime(hhb.time/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles')) -- may want to get rid of ts conversion but doesn't save time/performance
      -- AND hhb.date >= DATE_SUB(CURRENT_DATE(),61) --may want to remove but doesn't save time/performance
      -- AND to_date(from_utc_timestamp(to_timestamp(from_unixtime(hhb.time/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles')) >= DATE_SUB(CURRENT_DATE(),61)
      AND hhb.time < (unix_timestamp() * 1000) - 21600000 -- time earlier than 6 hrs ago
      AND hhb.value.is_end = 'false'
      AND hhb.value.is_databreak = 'false'
  WHERE
    hhb.date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
  ),

heartbeats_hub_gaps AS (
  SELECT
    date,
    org_id,
    org_name,
    internal_org,
    product_id,
    variant_type,
    device_id,
    device_name,
    fw,
    sam_number,
    heartbeat_time_ms,
    LEAD(heartbeat_time_ms) OVER (PARTITION BY org_id, device_id ORDER BY heartbeat_time_ms) AS next_heartbeat_time_ms
  FROM heartbeats_hub
),

offline_devices_hub AS (
  SELECT
    date,
    org_id,
    org_name,
    internal_org,
    product_id,
    variant_type,
    device_id,
    device_name,
    fw,
    sam_number,
    heartbeat_time_ms,
    IFNULL(next_heartbeat_time_ms, ((unix_timestamp() * 1000) - 21600000)) AS next_heartbeat_time_ms, --If next heartbeat is null replace with timestamp 6hrs ago
    from_utc_timestamp(to_timestamp(from_unixtime(heartbeat_time_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') AS human_heartbeat_time,
    from_utc_timestamp(to_timestamp(from_unixtime(IFNULL(next_heartbeat_time_ms, ((unix_timestamp() * 1000) - 21600000))/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') AS human_next_heartbeat_time --If next heartbeat is null replace with timestamp 6hrs ago
  FROM heartbeats_hub_gaps
  WHERE (next_heartbeat_time_ms - heartbeat_time_ms) >= 7200000
    OR (((unix_timestamp() * 1000) - 21600000) - heartbeat_time_ms >= 7200000 AND next_heartbeat_time_ms IS NULL) -- if next is null and hb is over 6 hours ago then include as offline
  ORDER BY org_name,
    heartbeat_time_ms
  ),

odh_dates AS (
  SELECT
    d.date,
    odh.date AS odh_date,
    odh.org_id,
    odh.org_name,
    odh.internal_org,
    odh.product_id,
    odh.variant_type,
    odh.device_id,
    odh.device_name,
    odh.fw,
    odh.sam_number,
    odh.heartbeat_time_ms,
    odh.next_heartbeat_time_ms,
    odh.human_heartbeat_time,
    odh.human_next_heartbeat_time
  FROM dates d
  LEFT JOIN offline_devices_hub odh
  ON d.date BETWEEN DATE(odh.human_heartbeat_time) AND DATE(odh.human_next_heartbeat_time)
),

daily_offline_devices_hub AS (
  SELECT
    date,
    org_id,
    device_id,
    fw,
    '1' AS is_offline
  FROM odh_dates
  GROUP BY date, org_id, device_id, fw
),

daily_offline_devices_hub_joined AS (
  SELECT
    sgd.date,
    sgd.org_id,
    sgd.sam_number,
    sgd.org_name,
    sgd.internal_org,
    sgd.device_name,
    sgd.device_id,
    sgd.product_id,
    sgd.variant_type,
    COALESCE(dodh.fw, db.latest_build_on_day) AS fw,
    IFNULL(dodh.is_offline,0) AS is_offline
  FROM sites_gateway_devices sgd
  LEFT JOIN daily_offline_devices_hub dodh
    ON sgd.date = dodh.date AND
      sgd.org_id = dodh.org_id AND
      sgd.device_id = dodh.device_id
  LEFT JOIN dataprep.device_builds db
    ON sgd.org_id = db.org_id AND
      sgd.device_id = db.device_id AND
      sgd.date = db.date
  WHERE
    db.date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
  ),

heartbeats_grpc AS (
  SELECT
    sgd.date,
    sgd.org_id,
    sgd.sam_number,
    sgd.org_name,
    sgd.internal_org,
    sgd.device_id,
    sgd.device_name,
    sgd.product_id,
    sgd.variant_type,
    ghb.time AS heartbeat_time_ms
  FROM sites_gateway_devices AS sgd
  LEFT JOIN kinesisstats.osdgrpchubserverdeviceheartbeat ghb
    ON sgd.org_id = ghb.org_id
      AND sgd.device_id = ghb.object_id
      AND sgd.date = to_date(from_utc_timestamp(to_timestamp(from_unixtime(ghb.time/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles')) --may want to get rid of ts conversion but doesn't save time/performance
      -- AND ghb.date >= DATE_SUB(CURRENT_DATE(),61) --may want to remove but doesn't save time/performance
      -- AND to_date(from_utc_timestamp(to_timestamp(from_unixtime(ghb.time/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles')) >= DATE_SUB(CURRENT_DATE(),61)
      AND ghb.time < (unix_timestamp() * 1000) - 21600000 -- time earlier than 6 hrs ago
      AND ghb.value.is_end = 'false'
      AND ghb.value.is_databreak = 'false'
  WHERE
    ghb.date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
  ),

heartbeats_grpc_gaps AS (
  SELECT
    date,
    org_id,
    org_name,
    internal_org,
    product_id,
    variant_type,
    device_id,
    device_name,
    sam_number,
    heartbeat_time_ms,
    LEAD(heartbeat_time_ms) OVER (PARTITION BY org_id, device_id ORDER BY heartbeat_time_ms) AS next_heartbeat_time_ms
  FROM heartbeats_grpc
),

offline_devices_grpc AS (
  SELECT
    date,
    org_id,
    org_name,
    internal_org,
    product_id,
    variant_type,
    device_id,
    device_name,
    sam_number,
    heartbeat_time_ms,
    IFNULL(next_heartbeat_time_ms, ((unix_timestamp() * 1000) - 21600000)) AS next_heartbeat_time_ms, --If next heartbeat is null replace with timestamp 6hrs ago
    from_utc_timestamp(to_timestamp(from_unixtime(heartbeat_time_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') AS human_heartbeat_time,
    from_utc_timestamp(to_timestamp(from_unixtime(IFNULL(next_heartbeat_time_ms, ((unix_timestamp() * 1000) - 21600000))/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') AS human_next_heartbeat_time --If next heartbeat is null replace with timestamp 6hrs ago
  FROM heartbeats_grpc_gaps
  WHERE (next_heartbeat_time_ms - heartbeat_time_ms) >= 7200000
    OR (((unix_timestamp() * 1000) - 21600000) - heartbeat_time_ms >= 7200000 AND next_heartbeat_time_ms IS NULL) -- if next is null and hb is over 6 hours ago then include as offline
  ORDER BY org_name,
  heartbeat_time_ms
  ),

grpc_dates AS (
  SELECT
    d.date,
    odg.date AS odh_date,
    odg.org_id,
    odg.org_name,
    odg.internal_org,
    odg.product_id,
    odg.variant_type,
    odg.device_id,
    odg.device_name,
    odg.sam_number,
    odg.heartbeat_time_ms,
    odg.next_heartbeat_time_ms,
    odg.human_heartbeat_time,
    odg.human_next_heartbeat_time
  FROM dates d
  LEFT JOIN offline_devices_grpc odg
  ON d.date BETWEEN DATE(odg.human_heartbeat_time) AND DATE(odg.human_next_heartbeat_time)
),

daily_offline_devices_grpc AS (
  SELECT
    gd.date,
    gd.org_id,
    gd.device_id,
    '1' AS is_offline
  FROM grpc_dates gd
  GROUP BY 1,2,3
),

daily_offline_devices_grpc_joined AS (
  SELECT
    sgd.date,
    sgd.org_id,
    sgd.sam_number,
    sgd.org_name,
    sgd.internal_org,
    sgd.device_name,
    sgd.device_id,
    sgd.product_id,
    sgd.variant_type,
    db.latest_build_on_day AS fw,
    IFNULL(dodg.is_offline,0) AS is_offline
  FROM sites_gateway_devices sgd
  LEFT JOIN daily_offline_devices_grpc dodg
    ON sgd.date = dodg.date AND
      sgd.org_id = dodg.org_id AND
      sgd.device_id = dodg.device_id
  LEFT JOIN dataprep.device_builds db
    ON sgd.org_id = db.org_id AND
      sgd.device_id = db.device_id AND
      sgd.date = db.date
  WHERE
    db.date BETWEEN DATE_SUB(${end_date},61) AND ${end_date}
  ),

daily_offline_devices_joined AS (
  SELECT a.*,
    ROW_NUMBER() OVER(PARTITION BY a.date, a.org_id, a.device_id ORDER BY a.is_offline DESC) rnk
  FROM(
    SELECT *
    FROM daily_offline_devices_hub_joined

    UNION

    SELECT *
    FROM daily_offline_devices_grpc_joined
    ) a
)

  SELECT
    date,
    org_id,
    sam_number,
    org_name,
    internal_org,
    device_name,
    device_id,
    product_id,
    variant_type,
    fw,
    is_offline
  FROM daily_offline_devices_joined
  WHERE rnk = 1
