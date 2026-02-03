-- Databricks notebook source
create or replace temp view current_day_rollout_stages as (
  select
    a.org_id,
    a.id as gateway_id,
    a.device_id,
    b.product_group_id,
    a.product_id,
    -- Gateway and org overrides make the product_program_id moot.
    -- Then the heirarchy is gw non-default program, org non-default program, defualt program (which is 0).
    -- This matches the logic in the backend.
    case
      when h.product_firmware_id is not null then "gateway_override"
      when g.product_firmware_id is not null then "org_override"
      else coalesce(c.product_program_id, d.product_program_id, 0)
    end as product_program_id,
    -- Type of program enrollment can be gateway-level or org-level, or "override" if
    -- there's a gateway-level or org-level firmware override.
    case
      when (g.product_firmware_id is not null) or (h.product_firmware_id is not null) then "override"
      when c.rollout_stage_id is not null then "gateway"
      when d.rollout_stage_id is not null then "org"
      when e.rollout_stage_id is not null then "gateway"
      else "org" -- also includes the case where f.rollout_stage_id is not null
    end as product_program_id_type,
    -- Gateway and org overrides make the product_firmware_id moot.
    -- Then the hierarchy is gw non-default program, org non-default program, gw default program, product group default program and stage 600.
    -- This matches the logic in the backend.
    case
      when h.product_firmware_id is not null then "gateway_override"
      when g.product_firmware_id is not null then "org_override"
      else coalesce(c.rollout_stage_id, d.rollout_stage_id, e.rollout_stage_id, f.rollout_stage_id, 600)
    end as rollout_stage_id
  from productsdb.gateways as a
  join definitions.products as b
    on a.product_id = b.product_id
  left join firmwaredb.gateway_program_override_rollout_stages as c
    on a.product_id = c.product_id
    and a.org_id = c.org_id
    and a.id = c.gateway_id
  left join firmwaredb.org_program_override_rollout_stages as d
    on a.product_id = d.product_id
    and a.org_id = d.org_id
  left join firmwaredb.gateway_rollout_stages as e
    on a.product_id = e.product_id
    and a.org_id = e.org_id
    and a.id = e.gateway_id
  left join firmwaredb.product_group_rollout_stage_by_org as f
    on b.product_group_id = f.product_group_id
    and a.org_id = f.org_id
  left join firmwaredb.organization_firmwares as g
    on a.product_id = g.product_id
    and a.org_id = g.organization_id
  left join firmwaredb.item_firmwares as h
    on a.product_id = h.product_id
    and a.org_id = h.organization_id
    and a.id = h.item_id
);

-- COMMAND ----------

create or replace temp view dates as (
  select explode(sequence(date_sub(current_date(), 30), current_date(), interval 1 day)) as date
);

-- COMMAND ----------

create or replace temp view thirty_day_rollout_stages as (
  select * from current_day_rollout_stages
  cross join dates
);

-- COMMAND ----------

create table if not exists dataprep_firmware.gateway_daily_rollout_stages
using delta
partitioned by (date)
as
select * from thirty_day_rollout_stages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enable schema automerge so that our new product_program_id_type column can be added automatically. Can be removed once this has run at least once.
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

-- COMMAND ----------

merge into dataprep_firmware.gateway_daily_rollout_stages as target
using thirty_day_rollout_stages as updates
on target.org_id = updates.org_id
and target.gateway_id = updates.gateway_id
and target.date = updates.date
when not matched then insert * ;

