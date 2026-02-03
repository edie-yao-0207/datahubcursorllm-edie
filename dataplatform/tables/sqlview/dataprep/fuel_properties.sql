SELECT
  ft.org_id,
  ft.device_id,
  ft.created_at,
  ft.source as source_id,
  fst.name as source_name,
  fg.id as fuel_group_id,
  fg.name as fuel_group_name,
  pt.id as power_train_id,
  pt.name as power_train_name
FROM
  (
  SELECT
    org_id,
    device_id,
    MAX(created_at) AS max_created_at
  FROM
    fueldb_shards.fuel_types
  GROUP BY
    org_id,
    device_id
  ) AS device_max_created_at
LEFT JOIN fueldb_shards.fuel_types AS ft
  ON device_max_created_at.org_id = ft.org_id
  AND device_max_created_at.device_id = ft.device_id
  AND device_max_created_at.max_created_at = ft.created_at
JOIN definitions.canonical_fuel_source_type AS fst ON ft.source = fst.id
JOIN definitions.properties_fuel_fuelgroup AS fg ON fg.id = CASE
    WHEN ft.engine_type = 1 THEN 4 -- FUEL_GROUP_ELECTRICITY
    WHEN ft.engine_type = 4 THEN 5 -- FUEL_GROUP_HYDROGEN
    ELSE
      CASE
        WHEN ft.gasoline_type > 0 THEN 1 -- FUEL_GROUP_GASOLINE
        WHEN ft.diesel_type > 0 THEN 2 -- FUEL_GROUP_DIESEL
        WHEN ft.gaseous_type > 0 THEN 3 -- FUEL_GROUP_GASEOUS
        ELSE 0 -- FUEL_GROUP_UNKNOWN
      END
  END
JOIN definitions.properties_fuel_powertrain AS pt ON pt.id = CASE
    WHEN ft.engine_type = 0 THEN 1 -- POWERTRAIN_INTERNAL_COMBUSTION_ENGINE
    WHEN ft.engine_type = 1 THEN 2 -- POWERTRAIN_BATTERY_ELECTRIC_VEHICLE
    WHEN ft.engine_type = 2 THEN 3 -- POWERTRAIN_PLUG_IN_HYBRID_ELECTRIC_VEHICLE
    WHEN ft.engine_type = 3 THEN 4 -- POWERTRAIN_HYBRID
    WHEN ft.engine_type = 4 THEN 5 -- POWERTRAIN_HYDROGEN
    ELSE 0 -- POWERTRAIN_UNKNOWN
  END
