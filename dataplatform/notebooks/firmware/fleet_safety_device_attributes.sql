-- Databricks notebook source
-- MAGIC %md
-- MAGIC The goal of this notebook is to generate a dataset of VG devices and attributes for our firmware team to sample from when releasing new firmware.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 0: Define Org Rollout Stages

-- COMMAND ----------

--In order to join the correct rollout stage for each device, we'll need to add in product ids to the firmwaredb.product_group_rollout_stage_by_org table.
-- Product Group Id Enums:
-- Fleet Safety = 1
-- Assets = 2
-- Industrial = 3
-- Connected Sites = 4

CREATE OR REPLACE TEMP VIEW org_rollout_stage_add_product_id AS (
SELECT
  org_id,
  product_group_id,
  CASE
    WHEN product_group_id = 1 THEN array(3,4,7,17,24,25,30,31,33,35,40,43,44,46,47,48,50,53,56,57,58,60,64,69,70,71,72,73,74,89,90,91,94,95,96,97,98,99,100,104,105,106,107)
    WHEN product_group_id = 2 THEN array(1,2,8,9,19,20,21,22,23,27,29,32,36,38,41,42,54,62,63,65,68,83,84,85,103)
    WHEN product_group_id = 3 THEN array(5,6,10,11,12,13,14,15,16,18,26,28,34,37,39,45,49,51,52,61,66,67,75,76,77,78,79,80,86,87,88,93)
    WHEN product_group_id = 4 THEN array(55,59,81,82,92,101,102,108,109)
  END AS product_id_array,
  rollout_stage_id
FROM firmwaredb.product_group_rollout_stage_by_org
);

CREATE OR REPLACE TEMP VIEW org_rollout_stage_by_product AS (
  SELECT
    org_id,
    product_group_id,
    EXPLODE(product_id_array) AS product_id,
    rollout_stage_id
  FROM org_rollout_stage_add_product_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 1: Define Device List

-- COMMAND ----------

-- Grabbing all VG devices, their respective customer data of interest, and attached CM product id.
CREATE OR REPLACE TEMP VIEW vg_devices AS (
  SELECT
    gw.org_id,
    gw.product_id AS vg_product_id,
    gw.id AS vg_gateway_id,
    dev.id AS vg_device_id,
    dev.camera_product_id AS cm_product_id,
    REPLACE(dev.camera_serial,'-','') as camera_serial,
    org.name AS org_name,
    CASE
      WHEN dev.org_id IN (1,0,18103) THEN 1
      ELSE org.internal_type
    END AS internal_type,
    org.release_type_enum,
    CASE
      WHEN grp.wifi_ap_enabled = 1 THEN true
      WHEN grp.wifi_ap_enabled = 0 THEN false
    END AS wifi_ap_enabled,
    vg_stage.rollout_stage_id AS fleet_safety_org_rollout_stage_enrollment,
    COALESCE(grs.rollout_stage_id, 'None') AS vg_device_stage_enrollment,
    CASE
      WHEN cmd.org_size = '1 -99' THEN '1 - 99'
      WHEN cmd.org_size = '1-99' THEN '1 - 99'
      WHEN cmd.org_size = 'Jan-99' THEN '1 - 99'
      ELSE COALESCE(cmd.org_size,'Not_Listed')
      END AS org_size,
    cmd.billingcountry AS org_billing_country,
    COALESCE(cmd.segment,'Not_Listed') AS customer_segment,
    COALESCE(cmd.csm_tier,'Not_Listed') AS csm_tier
  FROM clouddb.gateways AS gw
  JOIN clouddb.devices AS dev ON
    gw.org_id = dev.org_id
    AND gw.device_id = dev.id
  LEFT JOIN clouddb.organizations AS org ON
    gw.org_id = org.id
  LEFT JOIN clouddb.groups AS grp ON
    gw.org_id = grp.organization_id
  LEFT JOIN org_rollout_stage_by_product AS vg_stage ON
    gw.org_id = vg_stage.org_id
    AND gw.product_id = vg_stage.product_id
  LEFT JOIN dataprep.customer_metadata AS cmd ON
    gw.org_id = cmd.org_id
  LEFT JOIN firmwaredb.gateway_rollout_stages AS grs ON
    gw.id = grs.gateway_id
  WHERE gw.product_id IN (24,35,53,89)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 2: Add Safety Attributes and Perseus Devices

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW org_settings AS (
  SELECT
    id AS org_id,
    settings_proto.forward_collision_warning_enabled,
    settings_proto.distracted_driving_detection_enabled,
    settings_proto.following_distance_enabled,
    settings_proto.safety.policy_violation_settings.audio_alert_settings.enabled AS policy_violation_settings_enabled,
    settings_proto.safety.livestream_settings.enabled AS livestream_settings_enabled,
    settings_proto.face_detection_enabled,
    settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled AS in_cab_stop_sign_violation_alert_settings_enabled,
    settings_proto.safety.in_cab_railroad_crossing_violation_alert_settings.enabled AS in_cab_railroad_crossing_violation_alert_settings_enabled,
    settings_proto.safety.in_cab_speed_limit_alert_settings.enabled AS in_cab_speed_limit_alert_settings_enabled,
    --settings_proto.forward_collision_warning_audio_alerts_enabled,
    --settings_proto.distracted_driving_audio_alerts_enabled,
    --settings_proto.following_distance_audio_alerts_enabled,
    CASE
      WHEN audio_recording_enabled = 1 THEN true
      ELSE false
      END AS audio_recording_enabled
  FROM clouddb.organizations
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg_devices_extended AS(
  SELECT
    vg.*,
    cm.id AS cm_device_id,
    cmhb.last_reported_gateway_id AS cm_gateway_id,
    cmhb.last_heartbeat_date AS cm_last_heartbeat_date,
    o.face_detection_enabled,
    COALESCE(cm.device_settings_proto.forward_collision_warning_audio_alerts_device_enabled, o.forward_collision_warning_enabled)  AS fcw_enabled,
    COALESCE(cm.device_settings_proto.distracted_driving_detection_audio_alerts_device_enabled, o.distracted_driving_detection_enabled) AS ddd_enabled,
    COALESCE(cm.device_settings_proto.following_distance_audio_alerts_device_enabled, o.following_distance_enabled) AS fd_enabled,
    COALESCE(grs.rollout_stage_id, 'None') AS cm_device_stage_enrollment,
    COALESCE(cm.device_settings_proto.safety.policy_violation_settings.enabled, o.policy_violation_settings_enabled) AS policy_violations_enabled,
    COALESCE(cm.device_settings_proto.safety.livestream_settings.enabled, o.livestream_settings_enabled) AS livestream_enabled,
    COALESCE(cm.device_settings_proto.audio_recording_device_enabled, CASE WHEN o.audio_recording_enabled = 1 THEN true ELSE false end) AS audio_recording_enabled,
    CASE
      WHEN COALESCE(cm.device_settings_proto.forward_collision_warning_audio_alerts_device_enabled, o.forward_collision_warning_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.distracted_driving_detection_audio_alerts_device_enabled, o.distracted_driving_detection_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.following_distance_audio_alerts_device_enabled, o.following_distance_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.enabled, o.in_cab_speed_limit_alert_settings_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.safety.in_cab_railroad_crossing_violation_alert_settings.enabled, o.in_cab_railroad_crossing_violation_alert_settings_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled, o.in_cab_stop_sign_violation_alert_settings_enabled) = TRUE
      OR COALESCE(cm.device_settings_proto.safety.policy_violation_settings.enabled, o.policy_violation_settings_enabled) = TRUE
      THEN TRUE
      ELSE FALSE
    END AS in_cab_alerting_enabled,
    CASE
      WHEN COALESCE(cm.device_settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled, o.in_cab_stop_sign_violation_alert_settings_enabled) = TRUE
      OR  COALESCE(cm.device_settings_proto.safety.in_cab_railroad_crossing_violation_alert_settings.enabled, o.in_cab_railroad_crossing_violation_alert_settings_enabled) = TRUE
      OR  COALESCE(cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.enabled, o.in_cab_speed_limit_alert_settings_enabled) = TRUE
    THEN TRUE
    ELSE FALSE
    END AS tiling_features_enabled,
    CASE
      WHEN cm.config_override_json IS NOT NULL THEN true
      ELSE false
    END AS custom_json_override
  FROM vg_devices AS vg
  LEFT JOIN clouddb.devices AS cm ON
    vg.camera_serial = cm.serial
  LEFT JOIN dataprep.device_heartbeats_extended AS cmhb ON
    vg.org_id = cmhb.org_id
    AND cm.id = cmhb.device_id
  LEFT JOIN org_settings AS o ON
    vg.org_id = o.org_id
  LEFT JOIN firmwaredb.gateway_rollout_stages AS grs ON
    cmhb.last_reported_gateway_id = grs.gateway_id
)

-- COMMAND ----------

-- In order to identify which devices are attached to perseus we need to first grab the perseus devices and then join them with the deviceassociationsdb_shards.device_collection_associations to get their respective VG and CM devices. This is based off the logic originaly developed and reviewed by Brian Westpah. Notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/4424097529181695/command/4424097529181696
CREATE OR REPLACE TEMP VIEW perseus_devices AS (
  SELECT
    device.org_id,
    device.id AS device_id,
    device.name AS device_name,
    gateway.id AS gateway_id
  FROM clouddb.devices AS device
  JOIN clouddb.organizations AS o ON
    device.org_id = o.id
  LEFT JOIN clouddb.gateways AS gateway ON
    gateway.device_id = device.id
    AND gateway.org_id = device.org_id
  WHERE device.product_id IN (60)
);

CREATE OR REPLACE TEMP VIEW perseus_collections AS (
  SELECT
    perseus_devices.org_id,
    perseus_devices.device_id as perseus_device_id,
    perseus_devices.device_name as perseus_device_name,
    perseus_devices.gateway_id as perseus_gateway_id,
    associations.collection_uuid
  FROM perseus_devices
  JOIN deviceassociationsdb_shards.device_collection_associations AS associations ON
    perseus_devices.device_id = associations.device_id
    AND end_at IS NULL
);

-- Grab the Perseus <> VG pairings via collection_uuid
CREATE OR REPLACE TEMP VIEW vg_collections AS (
  SELECT
    device_id AS vg_id,
    org_id,
    product_id AS vg_product_id,
    collection_uuid
  FROM deviceassociationsdb_shards.device_collection_associations
  WHERE product_id IN (24,35,53,89)
  AND collection_uuid IN (SELECT collection_uuid FROM perseus_collections)
  AND end_at IS NULL
);

-- Grab the Perseus <> CM Paring through the VG
CREATE OR REPLACE TEMP VIEW cm_collections AS (
  SELECT
    a.vg_id,
    a.org_id,
    b.cm_product_id AS cm_product_id,
    b.linked_cm_id AS cm_id
  FROM vg_collections AS a
  LEFT JOIN dataprep_safety.cm_linked_vgs AS b ON
    a.org_id = b.org_id
    AND a.vg_id = b.vg_device_id
);

-- Join Perseus device associations together
CREATE OR REPLACE TEMP VIEW perseus_devices_extended AS (
  SELECT
    perseus.org_id,
    perseus.perseus_device_id,
    perseus.perseus_device_name,
    perseus.perseus_gateway_id,
    perseus.collection_uuid,
    vg_c.vg_product_id,
    vg_c.vg_id,
    cm_c.cm_product_id,
    cm_c.cm_id,
    true AS has_perseus
  FROM perseus_collections AS perseus
  LEFT JOIN vg_collections AS vg_c ON
    perseus.org_id = vg_c.org_id
    AND perseus.collection_uuid = vg_c.collection_uuid
  LEFT JOIN cm_collections AS cm_c ON
    vg_c.org_id = cm_c.org_id
    AND vg_c.vg_id = cm_c.vg_id
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    a.*,
    COALESCE(per.has_perseus, false) AS perseus
  FROM vg_devices_extended as a
  LEFT JOIN perseus_devices_extended AS per ON
    a.org_id = per.org_id
    AND a.vg_device_id = per.vg_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 2: Add Baxter Attribute

-- COMMAND ----------

--This is referencing a table created by Ava Oneil to idenitfy devices that have baxters attached. Notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/3231081252082727/command/3231081252082728
CREATE OR REPLACE TEMP VIEW baxter_devices AS (
  SELECT
    org_id,
    vg_device_id,
    cm_device_id,
    true AS baxter
  FROM baxter.baxter_devices
)

-- COMMAND ----------

--Join tables to add has_baxter col to original device list
CREATE OR REPLACE TEMP VIEW devices_add_baxter AS (
  SELECT a.*,
  COALESCE(bax.baxter, false) AS baxter
  FROM devices AS a
  LEFT JOIN baxter_devices AS bax ON
    a.org_id = bax.org_id
    AND a.vg_device_id = bax.vg_device_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 3: Add Modi Attribute

-- COMMAND ----------

--This is referencing a table created by Chris Fiore to idenitfy devices that have Modi attached. Notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/3817162871370290/command/3817162871370291
CREATE OR REPLACE TEMP VIEW modi_devices AS (
  SELECT
    org_id,
    device_id,
    MAX((date, has_modi)).has_modi AS has_modi
  FROM data_analytics.dataprep_vg_modi_days
  GROUP BY
    org_id,
    device_id
)

-- COMMAND ----------

--Join tables to add has_modi col to device list
CREATE OR REPLACE TEMP VIEW devices_add_modi AS (
  SELECT a.*,
  COALESCE(mod.has_modi, false) AS modi
  FROM devices_add_baxter AS a
  LEFT JOIN modi_devices AS mod ON
    a.org_id = mod.org_id
    AND a.vg_device_id = mod.device_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 4: Add Tachometer Attribute

-- COMMAND ----------

-- Currently there is not another notebook in production that creates a devices with tachometer dataset. The only way we can currently determine if a device has tachograph is by seeing if the device has reported any of the below objectStats. The logic below has reviwed and approved by Zaid on our EU Fleet team.
CREATE OR REPLACE TEMP VIEW tacho_download_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS download_count
  FROM kinesisstats.osDTachographVUDownload as a
  JOIN dataprep_firmware.data_ingestion_high_water_mark as b
  WHERE a.time <= b.time_ms
  GROUP BY
    org_id,
    object_id
);

CREATE OR REPLACE TEMP VIEW tacho_dinfo_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS dinfo_count
  FROM kinesisstats.osDTachographDriverInfo as a
  JOIN dataprep_firmware.data_ingestion_high_water_mark as b
  WHERE a.time <= b.time_ms
  GROUP BY
    org_id,
    object_id
);

CREATE OR REPLACE TEMP VIEW tacho_dstate_devices AS (
  SELECT
    org_id,
    object_id AS device_id,
    count(*) AS dstate_count
  FROM kinesisstats.osDTachographDriverState as a
  JOIN dataprep_firmware.data_ingestion_high_water_mark as b
  WHERE a.time <= b.time_ms
  GROUP BY
    org_id,
    object_id
);

CREATE OR REPLACE TEMP VIEW devices_joined AS (
  SELECT
    a.org_id,
    a.vg_device_id,
    COALESCE(b.download_count, 0) AS download_count,
    COALESCE(c.dinfo_count, 0) AS dinfo_count,
    COALESCE(d.dstate_count, 0) AS dstate_count
  FROM devices AS a
  LEFT JOIN tacho_download_devices AS b ON
    a.org_id = b.org_id
    AND a.vg_device_id = b.device_id
  LEFT JOIN tacho_dinfo_devices AS c ON
    a.org_id = c.org_id
    AND a.vg_device_id = c.device_id
  LEFT JOIN tacho_dstate_devices AS d ON
    a.org_id = d.org_id
    AND a.vg_device_id = d.device_id
);

-- Join has_tacho to device list
CREATE OR REPLACE TEMP VIEW tacho_devices AS (
  SELECT DISTINCT
    org_id,
    vg_device_id,
    true AS has_tacho
  FROM devices_joined
  WHERE
    download_count > 0
    OR dinfo_count > 0
    OR dstate_count > 0
)

-- COMMAND ----------

--Join tables to add has_tacho col to device list
CREATE OR REPLACE TEMP VIEW devices_add_tacho AS (
  SELECT a.*,
  COALESCE(tach.has_tacho, false) AS tacho
  FROM devices_add_modi AS a
  LEFT JOIN tacho_devices AS tach ON
    a.org_id = tach.org_id
    AND a.vg_device_id = tach.vg_device_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 5: Add ELD Attribute

-- COMMAND ----------

--Below is the logic developed by our compliance team to determine if a device is being used for HOS/ELD. See slack thread: https://samsara-net.slack.com/archives/C66L7ELHW/p1622155996031500
CREATE OR REPLACE TEMP VIEW hos_devices AS (
  SELECT
    h.vehicle_id AS device_id,
    h.org_id,
    count(*) AS record_count
  FROM compliancedb_shards.driver_hos_logs AS h
  LEFT JOIN clouddb.drivers AS d ON
    h.driver_id = d.id
  WHERE d.eld_exempt <> 1
  GROUP BY
    h.vehicle_id,
    h.org_id
);

-- The assumption is that a device is being used for ELD if it's record count is greater than one
CREATE OR REPLACE TEMP VIEW hos_enabled AS (
  SELECT
    device_id,
    org_id,
    CASE WHEN record_count > 0 THEN true ELSE false END AS hos_enabled
  FROM hos_devices
);

-- Join hos_enabled to device list
CREATE OR REPLACE TEMP VIEW devices_add_hos AS (
  SELECT a.*,
  COALESCE(hos.hos_enabled, false) AS hos_enabled
  FROM devices_add_tacho AS a
  LEFT JOIN hos_enabled AS hos ON
    a.org_id = hos.org_id
    AND a.vg_device_id = hos.device_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 6: Add Cable Attribute

-- COMMAND ----------

-- This is referencing a data table that fills in the last reported cable value on the days where the device didn't connect to our backend. (e.g device was sitting off in a parking lot).
-- Raw objectStat could be referenced but that is a much, much larger table that would be more expensive to query at scale.
CREATE OR REPLACE TEMP VIEW last_reported_device_cable AS (
  SELECT
    a.org_id,
    b.vg_product_id,
    a.device_id,
    MAX((a.date, a.cable_type)).cable_type AS last_reported_cable_id
  FROM data_analytics.device_cable_clean AS a
  JOIN devices AS b ON
    a.org_id = b.org_id
    AND a.device_id = b.vg_device_id
  GROUP BY
    a.org_id,
    b.vg_product_id,
    a.device_id
);

-- Join cable name from enum table in our backend. Coalescing in case a new cable enum is added that's not in the definitions table yet
CREATE OR REPLACE TEMP VIEW device_cable_name AS (
  SELECT
    a.org_id,
    a.device_id,
    a.vg_product_id,
    COALESCE(b.type, STRING(a.last_reported_cable_id))AS cable_type
  FROM last_reported_device_cable AS a
  LEFT JOIN definitions.cable_type_mappings AS b ON
    a.vg_product_id = b.product_id
    AND a.last_reported_cable_id = b.cable_id
);

-- Join cable name to device list
CREATE OR REPLACE TEMP VIEW devices_add_cable AS (
  SELECT a.*,
  cab.cable_type
  FROM devices_add_hos AS a
  LEFT JOIN device_cable_name AS cab ON
    a.org_id = cab.org_id
    AND a.vg_device_id = cab.device_id
    AND a.vg_product_id = cab.vg_product_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 7: Add Canbus Type

-- COMMAND ----------

-- This is referencing a data table that fills in the last reported canbus value on the days where the device didn't connect to our backend. (e.g device was sitting off in a parking lot).
-- Raw objectStat could be referenced but that is a much, much larger table that would be more expensive to query at scale.
CREATE OR REPLACE TEMP VIEW last_reported_device_canbus AS (
  SELECT
    a.org_id,
    a.device_id,
    MAX((a.date, a.can_bus_type)).can_bus_type AS last_reported_canbus_id
  FROM data_analytics.device_canbus_clean AS a
  JOIN devices AS b ON
    a.org_id = b.org_id
    AND a.device_id = b.vg_device_id
  GROUP BY
    a.org_id,
    a.device_id
);

-- Join canbus name from enum table in our backend. Coalescing in case a new canbus enum is added that's not in the definitions table yet
CREATE OR REPLACE TEMP VIEW device_canbus_name AS (
  SELECT
    a.org_id,
    a.device_id,
    COALESCE(b.type, STRING(a.last_reported_canbus_id))AS canbus_type
  FROM last_reported_device_canbus AS a
  LEFT JOIN definitions.canbus_type_mappings AS b ON
    a.last_reported_canbus_id = b.canbus_id
);

-- Join canbus name to device list
CREATE OR REPLACE TEMP VIEW devices_add_canbus AS (
  SELECT a.*,
  can.canbus_type
  FROM devices_add_cable AS a
  LEFT JOIN device_canbus_name AS can ON
    a.org_id = can.org_id
    AND a.vg_device_id = can.device_id
 )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 8: Add Accessory Attachments

-- COMMAND ----------

-- Objectstat is reported on change
CREATE OR REPLACE TEMP VIEW last_reported_osdattachedusbdevices AS (
    SELECT
      hub.org_id,
      hub.object_id AS vg_device_id,
      max((hub.time, hub.value.proto_value.attached_usb_devices.usb_id)).usb_id AS last_reported_attachments
    FROM kinesisstats.osdattachedusbdevices AS hub
    INNER JOIN devices AS dev ON
      hub.org_id = dev.org_id
      AND hub.object_id = dev.vg_device_id
    JOIN dataprep_firmware.data_ingestion_high_water_mark as hwm
    WHERE hub.date >= to_date(current_date() - interval 6 months)
      AND hub.value.is_databreak = false
      AND hub.value.is_end = false
      AND hub.time <= hwm.time_ms
    GROUP BY
      hub.org_id,
      hub.object_id
);

-- COMMAND ----------

--grabbed USB connection list from atlassian page
CREATE OR REPLACE TEMP VIEW exploded_device_usb_list AS (
  SELECT
    org_id,
    vg_device_id,
    explode(last_reported_attachments) AS usb_id
  FROM last_reported_osdattachedusbdevices
);

CREATE OR REPLACE TEMP VIEW vg_accesories AS (
SELECT
  org_id,
  vg_device_id,
  CASE
    WHEN exists(last_reported_attachments, x -> x = 491806831) = true THEN true
    ELSE false
  END AS falko,
  CASE
    WHEN exists(last_reported_attachments, x -> x = 491806832) = true THEN true
    WHEN exists(last_reported_attachments, x -> x = 491806833) = true THEN true
    WHEN exists(last_reported_attachments, x -> x = 491806834) = true THEN true
    ELSE false
  END AS luft,
  CASE
    WHEN exists(last_reported_attachments, x -> x = 706283520) = true THEN true
    WHEN exists(last_reported_attachments, x -> x = 318844929) = true THEN true
    ELSE false
  END AS aux_expander,
  CASE
    WHEN exists(last_reported_attachments, x -> x = 67330049) = true THEN true
    ELSE false
  END AS salt_spreader,
  CASE
    WHEN exists(last_reported_attachments, x -> x = 318844928) = true THEN true
    WHEN exists(last_reported_attachments, x -> x = 381683167) = true THEN true
  ELSE false
  END AS engine_immobilizer
FROM last_reported_osdattachedusbdevices
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW devices_add_names AS (
  SELECT
    vg.org_id,
    vg.org_name,
    vg.fleet_safety_org_rollout_stage_enrollment,
    vg.vg_device_stage_enrollment,
    vg.cm_device_stage_enrollment,
    vg.org_billing_country,
    vg.org_size,
    vg.customer_segment,
    vg.csm_tier,
    CASE
      WHEN vg.internal_type = 1 THEN 'Internal'
      ELSE 'Customer'
    END AS org_type,
    CASE
      WHEN vg.release_type_enum = 0 THEN 'Phase 1'
      WHEN vg.release_type_enum = 1 THEN 'Phase 2'
      WHEN vg.release_type_enum = 2 THEN 'Early Adopter'
      ELSE 'Error'
    END AS org_release_type,
    COALESCE(vg_name.name, vg.vg_product_id) AS vg_product_type,
    vg.vg_device_id,
    vg.vg_gateway_id,
    COALESCE(cm_name.name, vg.cm_product_id) AS cm_product_type,
    vg.cm_device_id,
    vg.cm_gateway_id,
    vg.cm_last_heartbeat_date,
    COALESCE(vg.ddd_enabled, false) as ddd_enabled,
    COALESCE(vg.fcw_enabled, false) as fcw_enabled,
    COALESCE(vg.fd_enabled, false) as fd_enabled,
    COALESCE(vg.face_detection_enabled, false) as face_detection_enabled,
    COALESCE(vg.policy_violations_enabled, false) as policy_violations_enabled,
    COALESCE(vg.livestream_enabled, false) as livestream_enabled,
    COALESCE(vg.audio_recording_enabled, false) as audio_recording_enabled,
    COALESCE(vg.in_cab_alerting_enabled, false) as in_cab_alerting_enabled,
    COALESCE(vg.tiling_features_enabled, false) as tiling_features_enabled,
    COALESCE(vg.custom_json_override, false) as custom_json_override,
    COALESCE(vg.wifi_ap_enabled, false) as wifi_ap_enabled,
    COALESCE(vg.hos_enabled, false) as hos_enabled,
    COALESCE(vg.perseus, false) as perseus,
    COALESCE(vg.baxter, false) as baxter,
    COALESCE(vg.modi, false) as modi,
    COALESCE(vg.tacho, false) as tacho,
    vg.cable_type,
    vg.canbus_type,
    COALESCE(acc.falko, false) AS falko,
    COALESCE(acc.luft, false) AS luft,
    COALESCE(acc.aux_expander, false) AS aux_expander,
    COALESCE(acc.salt_spreader, false) AS salt_spreader,
    COALESCE(acc.engine_immobilizer, false) AS engine_immobilizer
  FROM devices_add_canbus AS vg
  LEFT JOIN definitions.products AS vg_name ON
    vg.vg_product_id = vg_name.product_id
  LEFT JOIN definitions.products AS cm_name ON
    vg.cm_product_id = cm_name.product_id
  LEFT JOIN vg_accesories AS acc ON
    vg.org_id = acc.org_id
    AND vg.vg_device_id = acc.vg_device_id
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_firmware.fleet_device_attributes
USING DELTA
SELECT * FROM devices_add_names

-- COMMAND ----------

INSERT OVERWRITE TABLE dataprep_firmware.fleet_device_attributes (SELECT * FROM devices_add_names)
