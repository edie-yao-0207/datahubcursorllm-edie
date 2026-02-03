-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Purpose
-- MAGIC This notebook is used to calculate the RND-allocation values for the Delta lake S3 buckets
-- MAGIC We take rows from the s3inventories data to join it with `dataplatform.table_classifications` to
-- MAGIC determine production vs non-production assets and output a final table that produces the percentage
-- MAGIC of data in each bucket for production data (COGS) vs non-production data (OpEx)
-- MAGIC
-- MAGIC We commit these values into the repo that updates the tags in the buckets and is reflected in
-- MAGIC the cost dashboards.
-- MAGIC
-- MAGIC Refer to this link for more information: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5187912/Runbook+How+to+update+COGS+vs+OpEx+allocation+values+for+Delta+Lake+buckets
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # KS Replication

-- COMMAND ----------

create
or replace temporary view ks_inventory as (
  select
    *
  from
    s3inventories_delta.samsara_kinesisstats_delta_lake
  where
    inventory_date = (
      select
        max(inventory_date)
      from
        s3inventories_delta.samsara_kinesisstats_delta_lake
    )
);

-- KS data is located in the bucket under the following folders
-- - table/deduplicated/<tablename>...
-- - table/s3files/<tablename>...
create
or replace temporary view ks_inventory_data_files as (
  select
    *
  from
    ks_inventory k
  where
    k.key like 'table/deduplicated/%'
    or k.key like 'table/s3files/%'
);

-- We infer the table names by the folder following either deduplicated or s3files
create
or replace temporary view ks_inventory_data_files_with_table_name as (
  select
    *,
    regexp_extract(key, 'table/(deduplicated|s3files)/([^/]+)/.*', 2) as tableName
  from
    ks_inventory_data_files
);

-- We join with table_classifications to get production/nonproduction information
create
or replace temporary view ks_inventory_with_prod_status as (
  select
    *
  from
    ks_inventory_data_files_with_table_name k
    left outer join dataplatform.table_classifications c on lower(k.tableName) = lower(c.`table`) 
    where c.db = 'kinesisstats'
)

-- COMMAND ----------

-- We join the prod/nonprod information with the entire bucket and 
-- aggregate the size per prod/nonproduction
-- data that exists outside of the standard folder structure has production
-- value set to null and categorized as 'production_null_opex'. 
-- We add production_null_opex and opex values to determine the rnd-allocation
select
  case
    when production = false then 'opex'
    when production = true then 'cogs'
    else 'production_null_opex'
  end as cost_allocation,
  sum(t1.size) / 1e12 AS size_tb,
  round(
    (sum(t1.size) / sum(sum(t1.size)) over ()) * 100,
    2
  ) as percent_of_total
from
  ks_inventory t1
  left outer join ks_inventory_with_prod_status t2 on t1.key = t2.key
  and t1.version_id = t2.version_id
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # RDS Replication
-- MAGIC

-- COMMAND ----------

create
or replace temporary view rds_inventory as (
  select
    *
  from
    s3inventories_delta.samsara_rds_delta_lake
  where
    inventory_date = (
      select
        max(inventory_date)
      from
        s3inventories_delta.samsara_rds_delta_lake
    )
);

-- RDS data is located in the bucket under the following folders
-- - table-parquet/<prod_db_name>/<db_name_unclean>/<table_name_unclean>
-- - table/...
-- - s3files/...
-- - checkpoint/...
-- - s3listcheckpoints/...
-- Note: some of these folders (such as table) correspond to old CSV 
-- data locations. While leaving it as it won't break anything, you may
-- be able to delete them from the query.
create
or replace temporary view rds_inventory_data_files as (
  select
    *
  from
    rds_inventory k
  where
    k.key like 'table/%'
    or k.key like 'checkpoint/%'
    or k.key like 'table-parquet/%'
    or k.key like 's3files/%'
    or k.key like 's3listcheckpoints/%'
);

create
or replace temporary view rds_inventory_data_files_with_table_name as (
  select
    *,
    split(key, '/') [1] as prod_db_name,
    split(key, '/') [2] as db_name_unclean,
    split(key, '/') [3] as table_name_unclean
  from
    rds_inventory_data_files
);

-- COMMAND ----------

-- We sanitize the unclean dbnames for unsharded DBs
create
or replace temporary view rds_inventory_data_files_with_table_name_clean as (
  select
    *,
    case
      when prod_db_name = 'prod-clouddb' then 'clouddb'
      when prod_db_name = 'prod-statsdb' then 'statsdb'
      when prod_db_name = 'prod-productsdb' then 'productsdb'
      when prod_db_name = 'prod-alertsdb' then 'alertsdb'
      else db_name_unclean
    end as db_name_clean,
    substring(
      table_name_unclean,
      1,
      length(table_name_unclean) - 3
    ) as table_name_clean
  from
    rds_inventory_data_files_with_table_name
);

-- We join with table_classifications to get prod status
create
or replace temporary view rds_inventory_with_prod_status as (
  select
    *
  from
    rds_inventory_data_files_with_table_name_clean k
    left outer join dataplatform.table_classifications c on lower(k.table_name_clean) = lower(c.`table`)
    and (
      (lower(k.db_name_clean) = lower(c.db))
      or (lower(k.db_name_clean || '_shards') = lower(c.db))
    )
);

-- COMMAND ----------

-- We join the prod/nonprod information with the entire bucket and 
-- aggregate the size per prod/nonproduction
-- data that exists outside of the standard folder structure has production
-- value set to null and categorized as 'production_null_opex'. 
-- We add production_null_opex and opex values to determine the rnd-allocation
select
  case
    when production = false then 'opex'
    when production = true then 'cogs'
    else 'production_null_opex'
  end as cost_allocation,
  sum(t1.size) / 1e12 AS size_tb,
  round(
    (sum(t1.size) / sum(sum(t1.size)) over ()) * 100,
    2
  ) as percentage_of_total
from
  rds_inventory t1
  left outer join rds_inventory_with_prod_status t2 on t1.key = t2.key
  and t1.version_id = t2.version_id
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Pipelines

-- COMMAND ----------

create
or replace temporary view data_pipelines_inventory as (
  select
    *
  from
    s3inventories_delta.samsara_data_pipelines_delta_lake
  where
    inventory_date = (
      select
        max(inventory_date)
      from
        s3inventories_delta.samsara_data_pipelines_delta_lake
    )
);

-- data pipelines data lives in either tmp/ folder for legacy reasons
--  or at the top level followed by the db name and then table name
-- - db_name/table_name
-- - tmp/db_name/table_name
create
or replace temporary view data_pipelines_inventory_data_files_with_table_name as (
  select
    *,
    case
      when key like 'tmp/%' then split(split(t1.key, '/') [1], '\\.') [0]
      else split(t1.key, '/') [0]
    end as db_name,
    case
      when key like 'tmp/%' then split(split(t1.key, '/') [1], '\\.') [1]
      else split(t1.key, '/') [1]
    end as table_name
  from
    data_pipelines_inventory t1
);

-- We join with table_classifications to get production/nonproduction information
create
or replace temporary view data_pipelines_inventory_with_prod_status as (
  select
    *
  from
    data_pipelines_inventory_data_files_with_table_name k
    left outer join dataplatform.table_classifications c on lower(k.table_name) = lower(c.`table`)
    and lower(k.db_name) = lower(c.db)
)

-- COMMAND ----------

-- We join the prod/nonprod information with the entire bucket and 
-- aggregate the size per prod/nonproduction
-- data that exists outside of the standard folder structure has production
-- value set to null and categorized as 'production_null_opex'. 
-- We add production_null_opex and opex values to determine the rnd-allocation
select
  case
    when production = false then 'opex'
    when production = true then 'cogs'
    else 'production_null_opex'
  end as cost_allocation,
  sum(t1.size) / 1e12 AS size_tb,
  round(
    (sum(t1.size) / sum(sum(t1.size)) over ()) * 100,
    2
  ) as percentage_of_total
from
  data_pipelines_inventory t1
  left outer join data_pipelines_inventory_with_prod_status t2 on t1.key = t2.key
  and t1.version_id = t2.version_id
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Streams

-- COMMAND ----------

create
or replace temporary view data_streams_inventory as (
  select
    *
  from
    s3inventories_delta.samsara_data_stream_lake
  where
    inventory_date = (
      select
        max(inventory_date)
      from
        s3inventories_delta.samsara_data_stream_lake
    )
);

-- data streams data lives on the top level of the bucket
-- /<table_name>
create
or replace temporary view data_streams_inventory_data_files_with_table_name as (
  select
    *,
    split(key, '/') [0] as table_name
  from
    data_streams_inventory
);

-- the prod/nonprod status for datastreams isn't part of table_classifications (TODO: add to registry)
-- so we simply set the production value to true if table is api_logs else false
create
or replace temporary view data_streams_inventory_data_files_with_prod_status as (
  select
    *,
    case
      when table_name = 'api_logs' then true
      else false
    end as production
  from
    data_streams_inventory_data_files_with_table_name
)

-- COMMAND ----------

-- We join the prod/nonprod information with the entire bucket and 
-- aggregate the size per prod/nonproduction
select
  case
    when production = false then 'opex'
    when production = true then 'cogs'
    else 'unknown'
  end as cost_allocation,
  sum(t1.size) / 1e12 AS size_tb,
  round(
    (sum(t1.size) / sum(sum(t1.size)) over ()) * 100,
    2
  ) as percentage_of_total
from
  data_streams_inventory t1
  left outer join data_streams_inventory_data_files_with_prod_status t2 on t1.key = t2.key
  and t1.version_id = t2.version_id
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Streams Delta Lake
-- MAGIC

-- COMMAND ----------

create
or replace temporary view data_streams_delta_lake_inventory as (
  select
    *
  from
    s3inventories_delta.samsara_data_streams_delta_lake
  where
    inventory_date = (
      select
        max(inventory_date)
      from
        s3inventories_delta.samsara_data_streams_delta_lake
    )
);

-- data streams data lives on the top level of the bucket
-- /<table_name>
create
or replace temporary view data_streams_delta_lake_inventory_data_files_with_table_name as (
  select
    *,
    split(key, '/') [0] as table_name
  from
    data_streams_delta_lake_inventory
);

-- the prod/nonprod status for datastreams isn't part of table_classifications (TODO: add to registry)
-- so we simply set the production value to true if table is api_logs else false
create
or replace temporary view data_streams_delta_lake_inventory_data_files_with_prod_status as (
  select
    *,
    case
      when table_name = 'api_logs' then true
      else false
    end as production
  from
    data_streams_delta_lake_inventory_data_files_with_table_name
)

-- COMMAND ----------

-- We join the prod/nonprod information with the entire bucket and 
-- aggregate the size per prod/nonproduction
select
  case
    when production = false then 'opex'
    when production = true then 'cogs'
    else 'unknown'
  end as cost_allocation,
  sum(t1.size) / 1e12 AS size_tb,
  round(
    (sum(t1.size) / sum(sum(t1.size)) over ()) * 100,
    2
  ) as percentage_of_total
from
  data_streams_delta_lake_inventory t1
  left outer join data_streams_delta_lake_inventory_data_files_with_prod_status t2 on t1.key = t2.key
  and t1.version_id = t2.version_id
group by
  1

-- COMMAND ----------


