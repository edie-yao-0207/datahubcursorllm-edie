WITH latest_gateways_per_device AS (
  SELECT
    CAST(CURRENT_DATE() AS STRING) as date,
    org_id,
    object_id AS device_id,
    MAX((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id AS gateway_id)).gateway_id AS latest_gateway_id
  FROM kinesisstats.osdhubserverdeviceheartbeat AS hb
  WHERE
    value.is_databreak = 'false'
    AND value.is_end = 'false'
    AND date >= DATE_ADD(CURRENT_DATE(), -1)
    AND date <= CURRENT_DATE()
  GROUP BY
    date,
    org_id,
    device_id
),

device_builds_with_gateway_product_program_id AS (
  SELECT
    t1.*,
    t2.product_program_id AS product_program_id_gateway,
    t2._timestamp
  FROM latest_gateways_per_device t1
  LEFT JOIN firmwaredb.gateway_program_override_rollout_stages t2
    ON t1.latest_gateway_id = t2.gateway_id
    AND t1.org_id = t2.org_id
),

device_builds_with_gateway_and_org_product_program_id AS (
  SELECT
    t1.*,
    t2.product_program_id AS product_program_id_org,
    t2._timestamp as _timestamp_org
  FROM device_builds_with_gateway_product_program_id t1
  LEFT JOIN firmwaredb.org_program_override_rollout_stages t2
    ON t1.org_id = t2.org_id
),

device_product_programs AS (
SELECT *
FROM
  (
    SELECT
      date,
      org_id,
      device_id,
      latest_gateway_id,
      COALESCE(product_program_id_gateway, product_program_id_org) AS product_program_id,
      CASE
        WHEN product_program_id_gateway IS NULL AND product_program_id_org IS NULL THEN NULL
        WHEN product_program_id_gateway IS NOT NULL THEN "gateway"
        ELSE "org"
      END AS product_program_id_type,
      ROW_NUMBER() OVER (PARTITION BY date, org_id, device_id, latest_gateway_id ORDER BY COALESCE(_timestamp, _timestamp_org) DESC) as rnk
    FROM device_builds_with_gateway_and_org_product_program_id
  ) anon1
WHERE rnk = 1
)

SELECT
  date,
  org_id,
  device_id,
  latest_gateway_id,
  product_program_id,
  product_program_id_type
FROM device_product_programs
