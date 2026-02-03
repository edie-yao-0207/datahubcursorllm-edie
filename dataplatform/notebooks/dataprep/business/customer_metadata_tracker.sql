-- Databricks notebook source
create or replace temporary view metadata_tracker as (
  select 
    metadata.*,
    current_date() as date
  from dataprep.customer_metadata metadata
);

-- COMMAND ----------

create table if not exists dataprep.customer_health_tracking 
using delta partitioned by (date) 
select * from metadata_tracker;

-- COMMAND ----------

merge into dataprep.customer_health_tracking as target 
using metadata_tracker as updates
on target.org_id = updates.org_id
and target.sam_number = updates.sam_number
and target.date = updates.date
when matched then update set *
when not matched then insert *;
