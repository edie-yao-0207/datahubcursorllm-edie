-- Databricks notebook source
-- Devices in productsdb.devices can be either customer devices or cm's
CREATE OR REPLACE TEMPORARY VIEW devices_vgs AS (
  SELECT
    dv.org_id
    ,dv.id AS device_id -- this is the customer device id (not vehicle gateway (vg) id)
    ,dc.id AS camera_id
    ,pv.name AS vg_product_name
    ,pc.name AS cm_product_name
  FROM productsdb.devices dv
  -- Not all vg's have associated cm's, hence the left and not inner join
  LEFT JOIN productsdb.devices dc ON upper(replace(replace(dv.camera_serial, '-', ''), ' ', '')) = dc.serial
  JOIN definitions.products pv ON dv.product_id = pv.product_id
  LEFT JOIN definitions.products pc ON dc.product_id = pc.product_id
  WHERE upper(pv.name) RLIKE 'VG'
);
CREATE OR REPLACE TEMPORARY VIEW devices_cms AS (
  SELECT
    dv.org_id -- multiple vg's can have the same camera_serial
    ,dc.id AS camera_id -- for cm's in productsdb.devices, id represents the cm's id (not the customer device_id)
    ,dv.id AS device_id -- this is the customer device_id (not vehicle gateway (vg) id)
    ,pc.name AS cm_product_name
    ,pv.name AS vg_product_name
  FROM productsdb.devices dc
  -- A small number of cm's don't have associated vg's, hence the left and not inner join
  LEFT JOIN productsdb.devices dv ON dc.serial = upper(replace(replace(dv.camera_serial, '-', ''), ' ', ''))
  JOIN definitions.products pc ON dc.product_id = pc.product_id
  LEFT JOIN definitions.products pv ON dv.product_id = pv.product_id
  WHERE upper(pc.name) RLIKE 'CM'
    AND upper(pv.name) RLIKE 'VG' -- some cm's can be paired with ag's and other non-vg devices
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW safety_event_surveys as (
  select date,
  org_id,
  device_id,
  harsh_accel_type,
  event_ms,
  firmware_detected_event,
  detail_proto.is_useful,
  rank() OVER(
    PARTITION BY date,
                event_ms,
                org_id,
                device_id,
                harsh_accel_type
    ORDER BY created_at desc) as rank_useful
  from safetydb_shards.harsh_event_surveys
  WHERE `date` >= COALESCE(nullif(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),7))
    AND `date` <= COALESCE(nullif(getArgument("end_date"), ''),  CURRENT_DATE())
);

-- COMMAND ----------

-- The inner join - se.device_id = devices_vgs.device_id - will result in only those events where the
-- se.device_id represents a customer device_id.
CREATE OR REPLACE TEMPORARY VIEW backend_events_vgs AS (
  SELECT
    se.date
    ,se.event_ms
    ,se.org_id
    ,se.device_id AS device_id_from_safety
    ,se.detail_proto.event_id
    ,se.detail_proto.hidden_to_customer
    ,CASE WHEN se.release_stage = 0 THEN "Unset"
      WHEN se.release_stage = 1 THEN "Dark Launched"
      WHEN se.release_stage = 2 THEN "Closed Beta"
      WHEN se.release_stage = 3 THEN "Open Beta"
      WHEN se.release_stage = 4 THEN "GA"
    END AS release_stage
    ,dv.device_id
    ,dv.vg_product_name
    ,dv.camera_id
    ,dv.cm_product_name
    ,db_vg.latest_build_on_day AS vg_build
    ,db_cm.latest_build_on_day AS cm_build
    ,ad.total_distance AS distance_traveled
    ,CASE WHEN o.release_type_enum = 0 THEN "Phase 1"
      WHEN o.release_type_enum = 1 THEN "Phase 2"
      WHEN o.release_type_enum = 2 THEN "Early Adopter"
    END AS customer_phase
    ,CASE WHEN o.internal_type = 0 THEN "Customer Org"
      WHEN o.internal_type = 1 THEN "Internal Org"
      WHEN o.internal_type = 2 THEN "Lab Org"
      WHEN o.internal_type = 3 THEN "Developer Portal Test Org"
      WHEN o.internal_type = 4 THEN "Developer Portal Dev Org"
    END AS customer_type
    ,ha.event_type AS harsh_event_type
    ,ss.firmware_detected_event
    ,ss.is_useful
FROM safetydb_shards.safety_events as se
-- **Note that devices_vgs.device_id will always be a customer device_id (due to product filter in devices_vgs).**
JOIN devices_vgs dv on se.device_id = dv.device_id
  AND se.org_id = dv.org_id
LEFT JOIN dataprep.device_builds db_vg ON dv.device_id = db_vg.device_id
    AND se.`date` = db_vg.`date`
    AND se.org_id = db_vg.org_id
LEFT JOIN dataprep.device_builds db_cm ON dv.camera_id = db_cm.device_id
    AND se.`date` = db_cm.`date`
    AND se.org_id = db_cm.org_id
LEFT JOIN dataprep.active_devices ad ON dv.device_id = ad.device_id
    AND se.`date` = ad.`date`
    AND se.org_id = ad.org_id
LEFT JOIN clouddb.organizations o ON se.org_id = o.id
JOIN definitions.harsh_accel_type_enums ha -- enums may be present in safety_events, but not in harsh_accel_type_enums
  ON se.detail_proto.accel_type = ha.enum

left join (select * from safety_event_surveys where rank_useful = 1) as ss on ss.org_id = se.org_id
  and ss.event_ms = se.event_ms
  and ss.date = se.date
  and ss.device_id = se.device_id
  and ss.harsh_accel_type =  se.detail_proto.accel_type

WHERE se.`date` >= COALESCE(nullif(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),7))
  AND se.`date` <= COALESCE(nullif(getArgument("end_date"), ''),  CURRENT_DATE())
  AND isnotnull(se.detail_proto.accel_type)
);

-- COMMAND ----------


-- The inner join - se.device_id = devices_cms.camera_id - will result in only those events where the
-- se.device_id represents a camera_id.
CREATE OR REPLACE TEMPORARY VIEW backend_events_cms AS (
  SELECT
    se.date
    ,se.event_ms
    ,se.org_id
    ,se.device_id AS device_id_from_safety
    ,se.detail_proto.event_id
    ,se.detail_proto.hidden_to_customer
    ,CASE WHEN se.release_stage = 0 THEN "Unset"
      WHEN se.release_stage = 1 THEN "Dark Launched"
      WHEN se.release_stage = 2 THEN "Closed Beta"
      WHEN se.release_stage = 3 THEN "Open Beta"
      WHEN se.release_stage = 4 THEN "GA"
    END AS release_stage
    ,dc.device_id
    ,dc.vg_product_name
    ,dc.camera_id
    ,dc.cm_product_name
    ,db_vg.latest_build_on_day AS vg_build
    ,db_cm.latest_build_on_day AS cm_build
    ,ad.total_distance AS distance_traveled
    ,CASE WHEN o.release_type_enum = 0 THEN "Phase 1"
      WHEN o.release_type_enum = 1 THEN "Phase 2"
      WHEN o.release_type_enum = 2 THEN "Early Adopter"
    END AS customer_phase
    ,CASE WHEN o.internal_type = 0 THEN "Customer Org"
      WHEN o.internal_type = 1 THEN "Internal Org"
      WHEN o.internal_type = 2 THEN "Lab Org"
      WHEN o.internal_type = 3 THEN "Developer Portal Test Org"
      WHEN o.internal_type = 4 THEN "Developer Portal Dev Org"
    END AS customer_type
    ,ha.event_type AS harsh_event_type
    ,ss.firmware_detected_event
    ,ss.is_useful
FROM safetydb_shards.safety_events as se
-- **Note the difference in join here as compared to backend_events_vgs.**
JOIN devices_cms dc on se.device_id = dc.camera_id
  AND se.org_id = dc.org_id
LEFT JOIN dataprep.device_builds db_vg ON dc.device_id = db_vg.device_id
    AND se.`date` = db_vg.`date`
    AND se.org_id = db_vg.org_id
LEFT JOIN dataprep.device_builds db_cm ON dc.camera_id = db_cm.device_id
    AND se.`date` = db_cm.`date`
    AND se.org_id = db_cm.org_id
LEFT JOIN dataprep.active_devices ad ON dc.device_id = ad.device_id
    AND se.`date` = ad.`date`
    AND se.org_id = ad.org_id
LEFT JOIN clouddb.organizations o ON se.org_id = o.id
JOIN definitions.harsh_accel_type_enums ha -- enums may be present in safety_events, but not in harsh_accel_type_enums
  ON se.detail_proto.accel_type = ha.enum

left join (select * from safety_event_surveys where rank_useful = 1) as ss on ss.org_id = se.org_id
  and ss.event_ms = se.event_ms
  and ss.date = se.date
  and ss.device_id = se.device_id
  and ss.harsh_accel_type =  se.detail_proto.accel_type

WHERE se.`date` >= COALESCE(nullif(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),7))
  AND se.`date` <= COALESCE(nullif(getArgument("end_date"), ''),  CURRENT_DATE())
  AND isnotnull(se.detail_proto.accel_type)
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW backend_events AS (
  SELECT * FROM backend_events_vgs
  UNION
  SELECT * FROM backend_events_cms
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataengineering.backend_events
USING DELTA
PARTITIONED BY (`date`)
AS SELECT * FROM backend_events;

-- COMMAND ----------

MERGE INTO dataengineering.backend_events AS target
USING backend_events AS source ON target.date = source.date
  AND target.event_ms = source.event_ms
  AND target.org_id = source.org_id
  AND target.device_id_from_safety = source.device_id_from_safety
  AND target.event_id = source.event_id
  -- Do not use camera_id as a merge condition since it can be null
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
