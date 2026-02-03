-- Databricks notebook source
create or replace temporary view org_first_heartbeat as (
  select
    orgs.id as org_id,
    min(first_heartbeat_date) as first_heartbeat_date
  from dataprep.device_heartbeats_extended
  join clouddb.organizations orgs on id = org_id
  where internal_type != 1
  group by orgs.id
);

-- COMMAND ----------

create or replace temporary view org_metadata_heartbeat as (
  select
    sam_number,
    min(first_heartbeat_date) as first_heartbeat_date
  from org_first_heartbeat heartbeats
  join dataprep.customer_metadata metadata on metadata.org_id = heartbeats.org_id
  group by sam_number
);

-- COMMAND ----------

create or replace temporary view org_metadata_dates as (
  -- explode sequence from very first customer's heartbeat
  select
    *,
    explode(sequence(to_date(first_heartbeat_date), current_date())) as date
  from org_metadata_heartbeat
);

-- COMMAND ----------

create table if not exists dataprep.customer_dates using delta partitioned by (date) select * from org_metadata_dates;

-- COMMAND ----------

create or replace temporary view org_dates_updates as (
  select *
  from org_metadata_dates
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

-- COMMAND ----------

merge into dataprep.customer_dates as target
using org_dates_updates as updates
on target.sam_number = updates.sam_number
and target.date = updates.date
when matched then update set *
when not matched then insert *;
