-- Databricks notebook source
set vdp.lookback_days = 365

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_samsara_supported_tachographs
create or replace table firmware_dev.telematics_samsara_supported_tachographs (
  select
    distinct(value.proto_value.tachograph_vu_download.part_number) as tacho_part_number
    , value.proto_value.tachograph_vu_download.manufacturer_name as manufacturer
    , value.proto_value.tachograph_vu_download.generation as generation
  from
    kinesisstats.osdtachographvudownload
  where
    date between date_sub(now(), ${vdp.lookback_days}) and date(now())
)

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_most_prevalent_errors
create or replace table firmware_dev.telematics_most_prevalent_error as
  with

    data as (
      select
        org_id
        , object_id as device_id
        , histogram_numeric(value.proto_value.tachograph_download_error.error_code, 40) as histogram
      from
        kinesisstats.osdtachographdownloaderror
      where
        date between date_sub(now(), 21) and date(now())
      group by
        object_id
        , org_id
    )

    , exploded as (
      select
        org_id
        , device_id
        , entry.x
        , sum(entry.y) as y

      from
        data

      lateral view explode(histogram) as entry

      where
        entry.x not in (255, 254, 31)

      group by
        org_id
        , device_id
        , entry.x
    )

    , most_freq as (
      select
        org_id
        , device_id
        , max((x, y)) as max
      from
        exploded
      group by
        org_id
        , device_id
    )

  select
    org_id
    , device_id
    , tachograph_download_error_code.name as error
    , round(max.x) as error_code
    , max.y as error_count

    from
      most_freq

    left join
      definitions.tachograph_download_error_code
      on
        round(most_freq.max.x) = tachograph_download_error_code.id

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tachograph_last_build
create or replace table firmware_dev.telematics_tachograph_last_build as (
  select
    org_id
    , device_id
    , max((date, firmware_build)).date as date
    , max((date, firmware_build)).firmware_build as build

  from
    datamodel_core.dim_devices_settings

  where
    date between date_sub(now(), ${vdp.lookback_days}) and date(now())
    and firmware_build is not null
    and public_product_name like "VG%"

  group by all
)

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tachograph_last_cable
 create or replace table firmware_dev.telematics_tachograph_last_cable as (
  select
    org_id
    , device_id
    , max((date, last_cable_id)).last_cable_id as cable_id
    , max((date, last_cable_id)).date as date

  from
     datamodel_core.dim_devices_fast

  where
    date between date_sub(now(), ${vdp.lookback_days}) and date(now())
    and last_cable_id is not null

  group by all
)

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.dims_telematics_tachograph
create or replace table firmware_dev.dims_telematics_tachograph as (
  select
    telematics_tachograph_last_build.org_id
    , telematics_tachograph_last_build.device_id
    , devices.product_id
    , devices.make
    , devices.model
    , devices.year
    , organizations.locale
    , organizations.name as org_name
    , customer_metadata.lifetime_acv
    , devices.name as device_name
    , devices.config_override_json
    , telematics_tachograph_last_build.build
    , telematics_tachograph_last_build.date as last_build_date
    , telematics_tachograph_last_cable.cable_id as last_cable_id
    , telematics_tachograph_last_cable.date as last_cable_id_date

  from
    firmware_dev.telematics_tachograph_last_build

  left join
    firmware_dev.telematics_tachograph_last_cable
    on
      telematics_tachograph_last_build.org_id = telematics_tachograph_last_cable.org_id
      and telematics_tachograph_last_build.device_id = telematics_tachograph_last_cable.device_id

  inner join
    clouddb.organizations
    on
      organizations.id = telematics_tachograph_last_build.org_id

  left join
    productsdb.devices
    on
      telematics_tachograph_last_build.device_id = devices.id

  left join
    dataprep.customer_metadata
    on
      telematics_tachograph_last_build.org_id = customer_metadata.org_id

  where
    telematics_tachograph_last_build.date between date_sub(now(), 365) and date(now())
    -- Customer orgs only
    and organizations.internal_type == 0

  group by all
)

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tachograph_last_can_connected
create or replace table firmware_dev.telematics_tachograph_last_can_connected as (
  select
    org_id
    , object_id as device_id
    , max(date) as date

  from
    kinesisstats.osDCanConnected

  inner join
    definitions.can_bus_events
    on value.int_value = id

  where
    date between date_sub(now(), ${vdp.lookback_days}) and date(now())
    and not value.is_databreak
    and not value.is_end
    and name = "CANBUS_CONNECT"

  group by all
)

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tachograph_versions
create or replace table firmware_dev.telematics_tachograph_versions as
with
  driverfile as (
    select
      object_id as device_id
      , org_id
      , max(date) as last_driver_download_date

    FROM
      kinesisstats.osdtachographdriverinfo

    where
      date between date_sub(now(), ${vdp.lookback_days}) and date(now())
      and not value.is_databreak
      and not value.is_end

    group by all
  )

  , vu as (
      select
        org_id
        , object_id as device_id
        , max((date, time, value.proto_value.tachograph_vu_download)).tachograph_vu_download as tachograph_vu_download
        , max(date) as last_vu_download_date

      from
        kinesisstats.osdtachographvudownload

      where
        date between date_sub(now(), ${vdp.lookback_days}) and date(now())
        and not value.is_databreak
        and not value.is_end

      group by all
  )

  , falko as (
      select
        object_id as device_id,
        org_id,
        max(date) as last_falko_connection

      from
        kinesisstats.osdfalkoconnected

      where
        date between date_sub(now(), ${vdp.lookback_days}) and date(now())
        and not value.is_databreak
        and not value.is_end

      group by all
  )

  , metadata as (
      SELECT
        org_id
        , object_id as device_id
        , max((date, time, value.proto_value.tachograph_metadata)).tachograph_metadata as metadata

      from
        kinesisstats.osdtachographmetadata

      where
        date between date_sub(now(), ${vdp.lookback_days}) and date(now())
        and not value.is_databreak
        and not value.is_end

      group by all
  )

  , errors as (
      select
        org_id
        , object_id as device_id
        , max((date, time, value.proto_value.tachograph_download_error)) as error

      from
        kinesisstats.osdtachographdownloaderror

      where
        date between date_sub(now(), ${vdp.lookback_days}) and date(now())
        and not value.is_databreak
        and not value.is_end
        and contains(value.proto_value.tachograph_download_error.error_text, 'Locked-in with card:')

      group by all
  )

  , can_connection as (
      select
        org_id
        , object_id as device_id
        , max(date) as last

      from
        kinesisstats.osDCanConnected

      inner join
        definitions.can_bus_events
        on value.int_value = id

      where
        date between date_sub(now(), ${vdp.lookback_days}) and date(now())
        and not value.is_databreak
        and not value.is_end
        and name = "CANBUS_CONNECT"

      group by all
  )

  , trips as (
      select
        org_id
        , device_id
        , max((date, proto.trip_distance.distance_meters)) as last

      from
        trips2db_shards.trips

      where
        proto.trip_distance.distance_meters > 5000
        and date between date_sub(now(), ${vdp.lookback_days}) and date(now())

      group by all
  )

  , online_company_card_readers AS (
      select
        org_id
        , array_agg(card_number) as online_cards
      from
        tachographdb_shards.company_card_assignments

      where
        card_connected = 1 AND card_number LIKE '%0%'

      group by all
  )

select
  dims_telematics_tachograph.org_id
  , dims_telematics_tachograph.device_id
  , dims_telematics_tachograph.product_id
  , telematics_tachograph_last_build.build as firmware_version
  , dims_telematics_tachograph.last_cable_id as cable_id
  , dims_telematics_tachograph.org_name as org_name
  , dims_telematics_tachograph.device_name as device_name
  , dims_telematics_tachograph.locale
  , dims_telematics_tachograph.make
  , dims_telematics_tachograph.model
  , dims_telematics_tachograph.year
  , dims_telematics_tachograph.config_override_json as config_override_json
  , dims_telematics_tachograph.lifetime_acv
  , vu.last_vu_download_date
  , vu.tachograph_vu_download.part_number
  , vu.tachograph_vu_download.manufacturer_name
  , (
      case
        when vu.tachograph_vu_download.manufacturer_name = 'Continental Automotive Technologies' then 'Gen2v2'
        when vu.tachograph_vu_download.part_number like '900773R%' then 'Gen2v2' -- Stonerdige Gen2v2 all start with partnumber 900773
        when vu.tachograph_vu_download.generation = 1 then "Gen 1"
        when vu.tachograph_vu_download.generation = 2 then "Gen 2"
        else "Unknown"
      end
    ) as generation
  , driverfile.last_driver_download_date
  , falko.last_falko_connection
  , telematics_tachograph_last_can_connected.date as last_can_connected
  , trips.last.date as last_trip_date
  , trips.last.distance_meters as last_trip_distance_meters
  , substring(metadata.supplier_id FROM 0 FOR 36) as manufacturer_uds
  , replace(substring(metadata.part_number FROM 0 FOR 20), '  ', '') as part_number_uds
  , (
      case
        when manufacturer_uds = 'Continental Automotive Technologies' then 'Gen2v2'
        when manufacturer_uds = 'Continental Automotive GmbH' then 'Gen1/Gen2' -- To identify Gen1/Gen2 I need to create a hardcoded list of 188 part numbers
        when manufacturer_uds like 'Stoneridge%' and part_number_uds like '900773R%' then 'Gen2v2'
        when manufacturer_uds like 'Stoneridge%' and part_number_uds like '%900588%' then 'Gen2'
        when manufacturer_uds like 'Stoneridge%' and part_number_uds like '%900208%' then 'Gen1'
        else "Unknown"
      end
    ) as uds_generation
  , metadata.card_writing_enabled as card_writing_enabled
  , metadata.rdl_can_a_enabled as rdl_can_a
  , metadata.rdl_can_c_enabled as rdl_can_c
  , metadata.tachograph_vehicle_registration as vehicle_registration
  , metadata.card_id_slot_1 as driver_id_1_uds
  , errors.error.date as last_date_cardlock
  , regexp_extract(errors.error.tachograph_download_error.error_text,'Locked-in with card:(.*)', 1) as last_error_cardlock
  , array_contains(online_company_card_readers.online_cards,regexp_extract(last_error_cardlock,'\((.{13})\).$', 1)) as company_card_match
  , errors.error.tachograph_download_error.can_interface as last_error_interface
  , telematics_most_prevalent_error.error as most_prevalent_error
  , telematics_most_prevalent_error.error_code as most_prevalent_error_code
  , telematics_most_prevalent_error.error_count as most_prevalent_error_count

from
  firmware_dev.dims_telematics_tachograph

left join
  vu
  on
    dims_telematics_tachograph.org_id = vu.org_id
    and dims_telematics_tachograph.device_id = vu.device_id

left join
  driverfile
  on
    dims_telematics_tachograph.org_id = driverfile.org_id
    and dims_telematics_tachograph.device_id = driverfile.device_id

left join
  falko
  on
    dims_telematics_tachograph.org_id = falko.org_id
    and dims_telematics_tachograph.device_id = falko.device_id

left join
  metadata
  on
    dims_telematics_tachograph.org_id = metadata.org_id
    and dims_telematics_tachograph.device_id = metadata.device_id

left join
  errors
  on
    errors.org_id = dims_telematics_tachograph.org_id
    and errors.device_id = dims_telematics_tachograph.device_id

left join
  trips
  on
    trips.org_id = dims_telematics_tachograph.org_id
    and trips.device_id = dims_telematics_tachograph.device_id

left join
  firmware_dev.telematics_tachograph_last_can_connected
  on
    telematics_tachograph_last_can_connected.org_id = dims_telematics_tachograph.org_id
    and telematics_tachograph_last_can_connected.device_id = dims_telematics_tachograph.device_id

left join
  firmware_dev.telematics_most_prevalent_error
  on
    telematics_most_prevalent_error.org_id = dims_telematics_tachograph.org_id
    and telematics_most_prevalent_error.device_id = dims_telematics_tachograph.device_id

left join
  firmware_dev.telematics_tachograph_last_build
  on
    telematics_tachograph_last_build.org_id = dims_telematics_tachograph.org_id
    and telematics_tachograph_last_build.device_id = dims_telematics_tachograph.device_id

left join
    online_company_card_readers
   on
    online_company_card_readers.org_id = dims_telematics_tachograph.org_id

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tachograph_derived_state
create or replace table firmware_dev.telematics_tachograph_derived_state
  select
    telematics_tachograph_versions.*

    , products.name as product_name
    , coalesce(product_to_cables.cable_name, if(telematics_tachograph_versions.cable_id = 21, "CBL-VG-CPWR Power Only Cable V2", "N/A")) as cable_name
    , telematics_samsara_supported_tachographs.tacho_part_number is not null as tachograph_supported

    , (
      CASE
        WHEN last_vu_download_date >= DATE(NOW() - INTERVAL 10 DAY) AND last_driver_download_date >= DATE(NOW() - INTERVAL 7 DAY) THEN "HEALTHY"
        WHEN last_trip_date < DATE(NOW() - INTERVAL 7 DAY) THEN "NEEDS TRIP"
        ELSE "UNHEALTHY"
      END
    ) AS tacho_state


    , (
        CASE
          WHEN telematics_tachograph_versions.cable_id in (2, 14, 15, 25, 32, 33, 34) THEN 'OK for RTD' -- xFMS, xHGV and xTDC-Y0 cables
          WHEN telematics_tachograph_versions.cable_id in (4, 7) THEN (  --BOBD and BHGV needs BTUH (FALKO) to download
            CASE
              WHEN last_falko_connection >= DATE(NOW() - INTERVAL 2 DAY) THEN 'OK for RTD'
              ELSE 'FALKO issue - Install BTUH or VG55 SWAP'
            END
          )
          ELSE "NOK for RTD"
        END
    ) as cable_recommendation


    , (
        CASE
          WHEN part_number_uds IS null THEN 'Unable to get settings, no UDS data available'
          WHEN part_number_uds IS not null THEN (
            CASE
              -- Stoneridge
              WHEN startswith(manufacturer_uds, 'Stone') AND rdl_can_c = 'TRUE' AND card_writing_enabled is null THEN 'Settings OK'
              WHEN startswith(manufacturer_uds, 'Stone') AND card_writing_enabled = 'TRUE' THEN ( -- Devices with remote card writing enabled
                CASE
                  WHEN rdl_can_c = 'TRUE' THEN 'Remote card writing enabled, CAN C enabled'
                  WHEN rdl_can_c is null THEN 'Remote card writing enabled, CAN C disabled'
                END
              )
              -- Other
              WHEN rdl_can_c THEN 'Settings OK'
              WHEN rdl_can_a = 'TRUE' AND rdl_can_c IS null THEN 'CAN A enabled, CAN C not enabled'
              WHEN rdl_can_a is null AND rdl_can_c IS null THEN 'CAN A not enabled, CAN C not enabled'
              ELSE 'Unable to get settings, check for tachograph support'
            END
          )
          ELSE "Unexpected settings result 2"
        END
    ) as settings

from
  firmware_dev.telematics_tachograph_versions

left join
  definitions.products
  on telematics_tachograph_versions.product_id = products.product_id

left join
  definitions.product_to_cables
  on
    telematics_tachograph_versions.cable_id = product_to_cables.cable_id
    and (
      products.name = product_to_cables.product_name
      or (products.name like "VG34%" and product_to_cables.product_name like "VG34%")
      or (products.name like "VG54%" and product_to_cables.product_name like "VG54%")
      or (products.name like "VG55%" and product_to_cables.product_name like "VG55%")
    )

left join
  firmware_dev.telematics_samsara_supported_tachographs
  on
    trim(part_number_uds) = trim(telematics_samsara_supported_tachographs.tacho_part_number)

where
  products.name like "VG%"

-- COMMAND ----------

-- DBTITLE 1,TB: firmware_dev.telematics_tacho_health_analyzer
create or replace table firmware_dev.telematics_tacho_health_analyzer
  select
    telematics_tachograph_derived_state.*

    , (
        case
          -- Healthy tacho state, no further action required
          when tacho_state = 'HEALTHY' THEN 'No action required'
          when tacho_state = 'NEEDS TRIP' THEN 'Vehicle trip required'
          -- Unhealthy tacho state
          -- Unhealthy VG54
          when cable_recommendation = 'OK for RTD' then (
              case
                -- Found tachograph connected
                when part_number_uds is not null then (
                  case
                    -- Tacho supported & settings OK | NON FMS cables
                    when tachograph_supported and settings = 'Settings OK' then (
                      case
                        when (
                          cable_id in (7, 14, 25, 32, 34)
                          and (
                            last_error_interface = 'can0'
                            or last_error_interface = 'vcan0'
                          )
                        ) then 'Settings OK, but no connection on CAN-C'
                        when company_card_match = 'FALSE' then 'Customer to insert other card or change company card lock'
                        when company_card_match is null then 'Check company card lock or no company card readers connected'
                        else 'Tacho supported and settings OK | No recommendation available [Debug 1]'
                      end
                    )
                    -- Tacho supported & settings NOK
                    when tachograph_supported = 'TRUE' and settings != 'Settings OK' then
                    case
                     when settings = 'CAN A enabled, CAN C not enabled' then 'Customer to enable CAN C'
                     when settings = 'CAN A not enabled, CAN C not enabled' then 'Customer to enable CAN C'
                     when settings = 'Remote card writing enabled, CAN C disabled' then 'Customer to disable remote card writing and enable CAN C'
                     when settings = 'Remote card writing enabled, CAN C enabled' then 'Customer to disable remote card writing'
                     else 'No recommendation available - [Debug 2]'
                    end

                    -- Tacho possibly unsupported
                    when tachograph_supported = 'FALSE' and settings = 'Unable to get settings' then 'Check tacho coverage, settings unknown'
                    when tachograph_supported = 'FALSE' and settings = 'CAN A enabled, CAN C not enabled' then 'Check tacho coverage, most likely not supported'
                    when tachograph_supported = 'FALSE' and settings = 'CAN A not enabled, CAN C not enabled' then 'Check tacho coverage, CAN C not enabled'
                    when tachograph_supported = 'FALSE' and settings = 'Remote card writing enabled, CAN C enabled' then 'Check tacho coverage, disable card writing'
                    when tachograph_supported = 'FALSE' and settings = 'Remote card writing enabled, CAN C disabled' then 'Check tacho coverage, disable card writing end enable CAN C'
                    else 'Tachograph connected - check tacho coverage [Debug 3]'
                  END
                )
            -- No tachograph connected
            WHEN part_number IS NOT NULL THEN 'Downloaded in the past, but lost tacho connection / no UDS commincation'
            ELSE 'No tachograph installed or no connection with tachograp possible'
          END
        )

      WHEN (
        --Unhealthy VG34 without FALKO
        cable_recommendation = 'FALKO issue - Install BTUH or VG55 SWAP'
      ) THEN (
          CASE
            -- Tacho communcicating by UDS
            WHEN part_number_uds IS NOT NULL and settings = 'Settings OK' then 'Install BTUH / VG55'
            WHEN part_number_uds IS NOT NULL and settings like '%CAN C not enabled' then 'Install BTUH / VG55 Swap and enable CAN-C'
            -- No tacho connected
            WHEN part_number_uds is null and settings = 'Unable to get settings, no UDS data available' then 'No tachograph installed or no connection with tachograp possible '
            ELSE 'No tachograph installed or no connection with tachograp possible [Debug 4]'
          END
      )

      -- Installed with wrong cable or lost cable ID
      WHEN cable_recommendation = 'NOK for RTD' THEN 'No tachograph installed or wrong cable used'
      WHEN cable_recommendation = 'FALKO issue - Install BTUH or VG55 SWAP' and rdl_can_c = 'TRUE' then 'Install BTUH / VG swap'
      WHEN cable_recommendation = 'FALKO issue - Install BTUH or VG55 SWAP' and rdl_can_c is null then 'Install BTUH / VG swap and enable CAN C'
      ELSE 'No recommendation [Debug 5]'
    END
  ) AS final_recommendation

  FROM
    firmware_dev.telematics_tachograph_derived_state

-- COMMAND ----------

select * from firmware_dev.telematics_tacho_health_analyzer

-- COMMAND ----------

select count(*) from firmware_dev.telematics_tacho_health_analyzer

-- COMMAND ----------
