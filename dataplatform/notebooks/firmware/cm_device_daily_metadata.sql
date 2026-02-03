-- Databricks notebook source
-- Enable dynamic partition overwrite mode so only affected date partitions are replaced
SET spark.sql.sources.partitionOverwriteMode = dynamic;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cm_device_daily_metadata_view AS (
  SELECT
    assoc.date,
    cm_gws.org_id,
    case
      when orgs.internal_type = 0 then "customer"
      when orgs.internal_type = 1 then "internal"
      else string(orgs.internal_type)
    end as org_type,
    case
      when orgs.release_type_enum = 0 then "Phase 1"
      when orgs.release_type_enum = 1 then "Phase 2"
      when orgs.release_type_enum = 2 then "Early Adopter"
    end as org_release_track,
    cell.cell_id as org_cell,
    assoc.vg_device_id,
    assoc.vg_gateway_id,
    case
      when vg_gws.product_id = 24 then "VG34"
      when vg_gws.product_id = 35 then "VG34EU"
      when vg_gws.product_id = 90 then "VG34FN"
      when vg_gws.product_id = 53 then "VG54"
      when vg_gws.product_id = 89 then "VG54EU"
      when vg_gws.product_id = 178 and vg_gws.variant_id not in (16, 17) then "VG55NA"
      when vg_gws.product_id = 178 and vg_gws.variant_id in (16) then "VG55EU"
      when vg_gws.product_id = 178 and vg_gws.variant_id in (17) then "VG55FN"
      else string(vg_gws.product_id)
    end as vg_product_name,
    vg_gws.variant_id as vg_variant_id,
    vg_stages.product_program_id as vg_product_program_id,
    vg_stages.rollout_stage_id as vg_rollout_stage_id,
    coalesce(vg_fw.latest_build_on_day, "") as last_reported_vg_build,
    assoc.cm_device_id,
    assoc.cm_gateway_id,
    case
      when cm_gws.product_id = 43 then "CM32"
      when cm_gws.product_id = 44 then "CM31"
      when cm_gws.product_id = 155 then "CM34"
      when cm_gws.product_id = 167 then "CM33"
      when cm_gws.product_id = 126 then "Octo-1"
      when cm_gws.product_id = 213 then "Octo-4"
      else string(cm_gws.product_id)
    end as cm_product_name,
    cm_gws.variant_id as cm_variant_id,
    cm_stages.product_program_id as cm_product_program_id,
    cm_stages.rollout_stage_id as cm_rollout_stage_id,
    coalesce(cm_fw.latest_build_on_day, "") as last_reported_cm_build,
    vg_ff.enabled_feature_flag_values as vg_feature_flags,
    cm_ff.enabled_feature_flag_values as cm_feature_flags,
    case
      when vg_devs.asset_type = 0 then "Unknown"
      when vg_devs.asset_type = 1 then "Trailer"
      when vg_devs.asset_type = 2 then "Equipment"
      when vg_devs.asset_type = 3 then "UnpoweredAsset"
      when vg_devs.asset_type = 4 then "Vehicle"
      when vg_devs.asset_type = 5 then "Uncategorized"
      else string(vg_devs.asset_type)
    end as vg_asset_type
  from dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
  join clouddb.gateways as vg_gws
    on assoc.vg_gateway_id = vg_gws.id
  join clouddb.gateways as cm_gws
    on assoc.cm_gateway_id = cm_gws.id
  join clouddb.devices as vg_devs
    on assoc.vg_device_id = vg_devs.id
  join clouddb.devices as cm_devs
    on assoc.cm_device_id = cm_devs.id
  left join dataprep_firmware.device_daily_firmware_builds as vg_fw
    on assoc.date = vg_fw.date
    and assoc.vg_gateway_id = vg_fw.gateway_id
  left join dataprep_firmware.device_daily_firmware_builds as cm_fw
    on assoc.date = cm_fw.date
    and assoc.cm_gateway_id = cm_fw.gateway_id
  join dataprep_firmware.gateway_daily_rollout_stages as vg_stages
    on assoc.date = vg_stages.date
    and vg_devs.org_id = vg_stages.org_id
    and assoc.vg_device_id = vg_stages.device_id
    and assoc.vg_gateway_id = vg_stages.gateway_id
  join dataprep_firmware.gateway_daily_rollout_stages as cm_stages
    on assoc.date = cm_stages.date
    and cm_devs.org_id = cm_stages.org_id
    and assoc.cm_device_id = cm_stages.device_id
    and assoc.cm_gateway_id = cm_stages.gateway_id
  join clouddb.org_cells as cell
    on cm_gws.org_id = cell.org_id
  join clouddb.organizations as orgs
    on cm_devs.org_id = orgs.id
  left join internaldb.simulated_orgs as sim_orgs
    on cm_gws.org_id = sim_orgs.org_id
  left join dataprep_firmware.gateway_feature_flag_states_daily as vg_ff
    on assoc.date = vg_ff.date
    and vg_devs.org_id = vg_ff.org_id
    and assoc.vg_gateway_id = vg_ff.gateway_id
  left join dataprep_firmware.gateway_feature_flag_states_daily as cm_ff
    on assoc.date = cm_ff.date
    and cm_devs.org_id = cm_ff.org_id
    and assoc.cm_gateway_id = cm_ff.gateway_id
  where sim_orgs.org_id is null
    and assoc.date >= date_sub(current_date(), 364)
);

-- COMMAND ----------

-- Create the table if it doesn't exist (with partitioning)
CREATE TABLE IF NOT EXISTS dataprep_firmware.cm_device_daily_metadata
USING DELTA
PARTITIONED BY (date)
AS SELECT * FROM cm_device_daily_metadata_view;

-- COMMAND ----------

-- Overwrite only the affected date partitions on subsequent runs
INSERT OVERWRITE TABLE dataprep_firmware.cm_device_daily_metadata
SELECT * FROM cm_device_daily_metadata_view;
