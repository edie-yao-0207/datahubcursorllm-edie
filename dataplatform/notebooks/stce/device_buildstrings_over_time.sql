-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Readme
-- MAGIC
-- MAGIC This note book will create a lookup table to use a device_id and timestamp to determine what build was (most likely) running at the time using the `device.hello` object stat uploaded every ping which contains the reported build string of the currently running firmware.
-- MAGIC
-- MAGIC This supports both normal "good" build strings as well as fallback buildstrings (see notes in first query), and should cover 99% of all pertinent cases.
-- MAGIC
-- MAGIC Note, some stats recorded early at boot before the given device checks in and creates the `device.hello` stat will be reported incorrectly on the previous version, but these are few and hopefully not critical.
-- MAGIC
-- MAGIC When better matching on a given build is needed, the `approx_device_buildnum` field should be used to match on the desired build being investigated, specific fields for the 5340 and 9160 running versions are also available.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,[DB 10.4 version] Creates temporary view table for device_id & timestamp => buildstring
create or replace temporary view ag5x_reported_build_history as (

with first_time_on_build as (
    select
        object_id as device_id,
        min(time) as start_time, -- Time first on new build string
        value.proto_value.hub_server_device_heartbeat.connection.device_hello.build as approx_buildstring

    from kinesisstats.osdhubserverdeviceheartbeat as heartbeat
      left join productsdb.devices as devices on heartbeat.object_id = devices.id
    where date BETWEEN date_sub(current_date(), 180) and current_date() -- constrain to the last 6 months of information
          --and object_id in (281474985147536, 281474985251949, 281474984073951) -- for debugging
          and devices.product_id in (124, 125, 142, 143, 140, 144) -- limit to AG5x products
    group by object_id, value.proto_value.hub_server_device_heartbeat.connection.device_hello.build
),

first_time_on_build_with_buildnums as (
    select
        device_id,
        start_time, -- Time first on new build string
        coalesce(lead(start_time) over (partition by device_id order by start_time), unix_timestamp(current_timestamp)*1000) as end_time,
        approx_buildstring,

        -- Note the fallback string parsing will break on very old verisons of code where the formatting was different, e.g. ~v400 or older we will just ignore those units.

        -- 📱Full Devide Version, this self references 'approx_buildstring' from above
        case
          when approx_buildstring like "202%"  -- for "normal builds strings" which all start with the date (year), e.g. "2023-08-18_crevasse-13.7_v771-13-11221-gbec28463"
            then SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', -2), '-', 1)
          when approx_buildstring like "fa=%" -- for "fallback strings" which all start with "fa=", e.g. "fa=v771-k11221-gbec28463_na=v771-k11221-gbec28463_fm=v771-k11221_fn=k8150_nn=mfw_nrf9160_1.3.3"
            -- tokenize on '-' then strip off the leading 'k' to get the 5340 running image build number which we'll consider the "device" version upon a fallback string
            then substring(SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', 2), '-', -1),2)
          else -- if its on some really unexepcted build string, then NULL?
            NULL
        end as approx_device_buildnum,

        -- 🦊 5340 SoC Version, this self references 'approx_buildstring' from above
        case
          when approx_buildstring like "202%"  -- for "normal builds strings" which all start with the date (year), e.g. "2023-08-18_crevasse-13.7_v771-13-11221-gbec28463"
            then SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', -2), '-', 1) -- we assume all manifests use "fresh" 5340 & 9160 builds, where the manifest build number IS the 5340 & 9160 build number
          when approx_buildstring like "fa=%" -- for "fallback strings" which all start with "fa=", e.g. "fa=v771-k11221-gbec28463_na=v771-k11221-gbec28463_fm=v771-k11221_fn=k8150_nn=mfw_nrf9160_1.3.3"
            -- tokenize on '-' then strip off the leading 'k' to get the 5340 running image build number
            then substring(SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', 2), '-', -1),2)
          else -- if its on some really unexepcted build string, then NULL?
            NULL
        end as approx_5340_buildnum,

        -- 🐬 9160 SoC Version, this self references 'approx_buildstring' from above
        case
          when approx_buildstring like "202%"  -- for "normal builds strings" which all start with the date (year), e.g. "2023-08-18_crevasse-13.7_v771-13-11221-gbec28463"
            then SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', -2), '-', 1) -- we assume all manifests use "fresh" 5340 & 9160 builds, where the manifest build number IS the 5340 & 9160 build number
          when approx_buildstring like "fa=%" -- for "fallback strings" which all start with "fa=", e.g. "fa=v771-k11221-gbec28463_na=v771-k11221-gbec28463_fm=v771-k11221_fn=k8150_nn=mfw_nrf9160_1.3.3"
            -- tokenize on '-' then strip off the leading 'k' to get the 9160 running image build number
            then substring(SUBSTRING_INDEX(SUBSTRING_INDEX(approx_buildstring, '-', 4), '-', -1),2)
          else -- if its on some really unexepcted build string, then NULL?
            NULL
        end as approx_9160_buildnum

    from first_time_on_build
),

build_filter_table as (
    select
        device_id,
        approx_buildstring,
        approx_device_buildnum,
        approx_5340_buildnum,
        approx_9160_buildnum,
        start_time,
        end_time,
        date_format(convert_timezone('UTC', 'America/Los_Angeles', FROM_UNIXTIME(start_time/1000) ), 'yyyy-MM-dd hh:mm:ss a') as startPacific, --helpful for humanz
        date_format(convert_timezone('UTC', 'America/Los_Angeles', FROM_UNIXTIME(end_time/1000) ), 'yyyy-MM-dd hh:mm:ss a') as endPacific, --helpful for humanz
        date_format(FROM_UNIXTIME(end_time/1000), 'yyyy-MM-dd') as end_date -- for delta table partitioning

    from first_time_on_build_with_buildnums
)

select * from build_filter_table
sort by end_time desc

);

-- COMMAND ----------

-- DBTITLE 1,Write that big query to a permanent database
-- This generally takes less than 5 minutes to run fully, so we'll do that for now instead of delta updates.

create or replace table firmware.ag5x_reported_build_history using delta partitioned by (end_date) AS (
--create table if not exists firmware.ag5x_reported_build_history using delta partitioned by (end_time) AS (
  select *
  from ag5x_reported_build_history
)
