-- Databricks notebook source
create or replace temporary view route_loads as (
  select
    org_dates.sam_number,
    org_dates.date,
    COUNT(distinct user_id) as dashboard_users,
    COUNT(formatted_url) as usage_ct
  from dataprep.customer_dates org_dates
  join dataprep.customer_metadata sam_orgs on sam_orgs.sam_number = org_dates.sam_number
  left join dataprep.routeload_data routeloads
    on routeloads.org_id = sam_orgs.org_id
    and routeloads.date = org_dates.date
  group by
    org_dates.sam_number,
    org_dates.date
);

-- COMMAND ----------

create or replace temporary view total_users as (
  select
    sam_orgs.sam_number as sam_number,
    COUNT(distinct user_id) as num_users
  from dataprep.routeload_data rld
  left join dataprep.customer_metadata sam_orgs
    on sam_orgs.org_id = rld.org_id
  group by sam_orgs.sam_number
);

-- COMMAND ----------

create or replace temporary view pageloads_users as (
  select
    route_loads.*,
    num_users as total_dashboard_users
  from route_loads
  join total_users
    on route_loads.sam_number = total_users.sam_number
);

-- COMMAND ----------

create table if not exists dataprep.customer_routeloads
using delta partitioned by (date)
select * from pageloads_users;

-- COMMAND ----------

create or replace temporary view routeload_updates as (
  select *
  from pageloads_users
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

-- COMMAND ----------

merge into dataprep.customer_routeloads as target
using routeload_updates as updates
on target.sam_number = updates.sam_number
and target.date = updates.date
when matched then update set *
when not matched then insert *;
