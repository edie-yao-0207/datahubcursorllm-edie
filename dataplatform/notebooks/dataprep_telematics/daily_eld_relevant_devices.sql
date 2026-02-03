-- Rules for ELD relevancy and related owners are here
-- https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4648066/HoS+ELD+Vehicle+Definitions+Working+Doc#ELD-Relevant-Vehicles

-- For backfills, note that there aren't any snapshot/historical datasets available atm for devices and configs (overrides below)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # MMs explicitly marked as ELD relevant by legal/product
-- MAGIC import boto3
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC region = boto3.session.Session().region_name
-- MAGIC if region == 'us-west-2':
-- MAGIC     s3_bucket = 'samsara-databricks-workspace'
-- MAGIC elif region == 'eu-west-1':
-- MAGIC     s3_bucket = 'samsara-eu-databricks-workspace'
-- MAGIC s3_path = f's3://{s3_bucket}/dataanalytics/Fleet MMY vs MM current sheet - S3 Bucket Upload.csv'
-- MAGIC
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC df = spark.read.csv(s3_path, header=True, inferSchema=True)
-- MAGIC df.createOrReplaceTempView("make_model_ELD_relevance")

-- COMMAND ----------

-- device metadata + override information
create or replace temp view all_devices as (
  select
    d.org_id,
    d.id as device_id,
    d.product_id,
    d.serial,
    lower(trim(d.make)) as make,
    lower(trim(d.model)) as model,
    lower(trim(d.year)) as year,
    ifnull(d.device_settings_proto.unregulated_vehicle, false) as is_unregulated_vehicle,

    case when
      --Widget dropdown (OBD set to OFF) vehicle list
      obd_type = 3                                 OR
      --Probe type = 3 via device level
      config_override_json LIKE '%probe_type": 3%' OR
      config_override_json LIKE '%probe_type":3%'  OR
      --Cable ID = 0 (VG34 power only cable override) via device level
      config_override_json LIKE '%cable_id": 0%'   OR
      config_override_json LIKE '%cable_id":0%'    OR
      --Cable ID = 8 (VG54 power only cable override) via device level
      config_override_json LIKE '%cable_id": 8%'   OR
      config_override_json LIKE '%cable_id":8%'
    then true
    else false
    end as has_override
  from productsdb.devices d
    join clouddb.organizations o
      on o.id = d.org_id
  where 1=1
    and d.product_id in (24,53,35,89,178) --- 35 and 89 are EU variants
    and o.internal_type = 0
);

-- COMMAND ----------

-- explode device metadata over dates that need to be processed
create or replace temp view all_device_dates AS (
  select
    org_id,
    device_id,
    product_id,
    serial,
    make,
    model,
    year,
    is_unregulated_vehicle,
    has_override,

    EXPLODE(SEQUENCE(DATE(COALESCE(NULLIF(getArgument('start_date'), ''), CURRENT_DATE - 3)), DATE(COALESCE(NULLIF(getArgument('end_date'), ''), CURRENT_DATE)))) AS date
  from all_devices
);

-- COMMAND ----------

-- mark devices that had an active heartbeat within the trailing year
create or replace temporary view all_device_dates_heartbeats_l366d as(
  select
    ad.date,
    ad.org_id,
    ad.device_id,
    ad.product_id,
    ad.serial,
    ad.make,
    ad.model,
    ad.year,
    ad.is_unregulated_vehicle,
    has_override,
    ifnull(max(hb.date >= date_sub(ad.date, 366)), false) as has_heartbeat_l366d
  from all_device_dates ad
  left join (
    select
    date,
    org_id,
    object_id as device_id
    from kinesisstats_history.osdhubserverdeviceheartbeat
    where value.is_databreak = 'false'
      and value.is_end = 'false'
      and date >= date_sub(COALESCE(NULLIF(getArgument('start_date'), ''), CURRENT_DATE - 3), 366)
    group by 1,2,3) hb
  on ad.org_id = hb.org_id
  and ad.device_id = hb.device_id
  and ad.date >= hb.date
  group by 1,2,3,4,5,6,7,8,9,10
);

-- COMMAND ----------

-- mark devices that had an hos log within the trailing year
create or replace temporary view all_device_dates_hos_logs_l183d as(
  select
    ad.date,
    ad.org_id,
    ad.device_id,
    ifnull(max(hl.date >= date_sub(ad.date, 183)), false) as has_hos_logs_l183d
  from all_device_dates ad
  left join (
    select
      date,
      org_id,
      vehicle_id as device_id
    from
      compliancedb_shards.driver_hos_logs
    where 1=1
      and (log_proto.event_record_status <> 2 or log_proto.event_record_status is null) --- filter out outdated rows for a given log
      and date >= date_sub(COALESCE(NULLIF(getArgument('start_date'), ''), CURRENT_DATE - 3), 183)
    group by 1,2,3) hl
  on ad.org_id = hl.org_id
  and ad.device_id = hl.device_id
  and ad.date >= hl.date
  group by 1,2,3
);

-- COMMAND ----------

-- mark devices that had an assigned hos log ever
create or replace temp view first_assigned_hos_log_dates as (
select
  vehicle_id as device_id,
  org_id,
  min(date) as first_assigned_hos_log_date
from
  compliancedb_shards.driver_hos_logs h
where 1=1
  and (log_proto.event_record_status <> 2 or log_proto.event_record_status is null) --- filter out outdated rows for a given log
  and (driver_id > 0 or (notes is not null and trim(notes) <> '')) -- assigned logs only
group by 1,2
);

-- COMMAND ----------

-- list of orgs that have overrides applied
create or replace temp view orgs_currently_with_overrides as (
SELECT
  g.organization_id as org_id
FROM
  clouddb.groups g
WHERE
    --Probe type = 3 via device level
    g.group_config_override LIKE '%probe_type": 3%' OR
    g.group_config_override LIKE '%probe_type":3%'  OR
    --Cable ID = 0 (VG34 power only cable override) via device level
    g.group_config_override LIKE '%cable_id": 0%'   OR
    g.group_config_override LIKE '%cable_id":0%'    OR
    --Cable ID = 8 (VG54 power only cable override) via device level
    g.group_config_override LIKE '%cable_id": 8%'   OR
    g.group_config_override LIKE '%cable_id":8%'
);

-- COMMAND ----------

-- list of orgs where 'compliance-na-reporting-enabled' feature config is disabled,
-- this indicates that they do not have a license which includes the compliance feature set
create or replace temp view orgs_without_compliance_feature_config as (
  SELECT
    OrgId as org_id
  FROM dynamodb.cached_feature_configs_v2
  WHERE
    FeatureConfigKey = 'compliance-na-reporting-enabled' AND
    Value = 'false'
);

-- COMMAND ----------

-- join everything together to attribute devices with the eld relevance tag based on combination of criteria
create or replace temp view daily_eld_relevant_devices as (
select

  adhb.date,
  adhb.org_id,
  adhb.device_id,
  adhb.product_id,
  adhb.serial,
  adhb.make,
  adhb.model,
  adhb.year,
  lower(trim(vin.engine_model)) as engine_model,
  lower(trim(vin.primary_fuel_type)) as primary_fuel_type,

  adhb.has_heartbeat_l366d, -- true = blue_bubble
  h183.has_hos_logs_l183d, -- (adhb.has_heartbeat_l366d and has_hos_logs_l183d) = purple_bubble
  r.group as eld_relevancy_group,
  fhd.first_assigned_hos_log_date,
  adhb.is_unregulated_vehicle,
  adhb.has_override,
  (oo.org_id is not null) as has_org_override,

  case
    when
      adhb.has_heartbeat_l366d -- filter out devices without a heartbeat in the last year
      and adhb.year >= 2000 --- filter out pre 2000 vehicles
      and r.group in ('Group 1', 'Group 2', 'Group 3') --- filter out vehicles not explicitly marked as ELD relevant in the MM csv from legal/product
      and has_hos_logs_l183d --- filter out devices without logs in half year
      and adhb.date >= fhd.first_assigned_hos_log_date --- filtering out vehicles that have never had an assigned HoS log
      and not(adhb.is_unregulated_vehicle) --- filtering out explicitly unregulated vehicles
      and not(adhb.has_override)
      and oo.org_id is null --- filtering out devices and orgs with explicit data overrides
      and ofc.org_id is null --- filtering out devices belonging to orgs that don't have the compliance feature set
    then true else false
  end as is_eld_relevant

from
  all_device_dates_heartbeats_l366d adhb
  left join vindb_shards.device_vin_metadata vin
    on adhb.device_id = vin.device_id
    and adhb.org_id = vin.org_id
  left join make_model_ELD_relevance r
    on r.make = adhb.make
    and r.model = adhb.model
  left join all_device_dates_hos_logs_l183d h183
    on h183.device_id = adhb.device_id
    and h183.org_id = adhb.org_id
    and h183.date = adhb.date
  left join first_assigned_hos_log_dates fhd
    on fhd.device_id = adhb.device_id
    and fhd.org_id = adhb.org_id
  left join orgs_currently_with_overrides oo
    on oo.org_id = adhb.org_id
  left join orgs_without_compliance_feature_config ofc
    on ofc.org_id = adhb.org_id
where 1=1
);

-- COMMAND ----------

create table if not exists dataprep_telematics.daily_eld_relevant_devices
using delta
partitioned by (date)
as
select * from daily_eld_relevant_devices;

-- COMMAND ----------

merge into dataprep_telematics.daily_eld_relevant_devices as target
using daily_eld_relevant_devices as updates
on target.device_id = updates.device_id
and target.org_id = updates.org_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;
