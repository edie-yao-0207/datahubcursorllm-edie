SELECT
  active_date,
  eom,
  fiscal_year,
  quarter,
  eoq,
  locale,
  device_region,
  product_id,
  name,
  product_family,
  active_device_days,
  total_devices_heartbeats,
  total_devices_trips
FROM
  (
    --active_device_day field calc to count a VG or CM as active if it was on a trip in a given day and an AG/IG/SG as active if it had a heartbeat on a given day
    SELECT
      *,
      CASE
        WHEN product_family = 'VG' THEN total_devices_trips
        WHEN product_family = 'CM' THEN total_devices_trips
        WHEN product_family = 'AG' THEN total_devices_heartbeats
        WHEN product_family = 'IG' THEN total_devices_heartbeats
        WHEN product_family = 'SG' THEN total_devices_heartbeats
      END as active_device_days,
      --device_region field calc to categorize NA vs EU devices per logic listed here: https://paper.dropbox.com/doc/Eurofleet-Quick-Reference-Guide--A64B4opSPA_u3K9UMHkkcOP8Ag-ySKzu5Ypm3P3ZKS8xhAVb#:uid=887667006238826231731273&h2=Finding-out-EU-customers-in-th
      CASE
        WHEN
          locale != "us"
          AND locale != "ca"
          AND locale != "mx"
        THEN
          'EU'
        ELSE 'NA'
      END as device_region
    from
      (
        --product_family field calc to determine product line using first 2 characters in name field
        SELECT
          *,
          SUBSTRING(name, 1, 2) AS product_family
        from
          (
            --get active devices data by date from dataprep.active_devices table, count number of distinct device heartbeats and trips for each unique product/locale combination
            select
              to_date(ad.date) as active_date,
              ad.product_id as prod_id,
              o.locale,
              count(distinct
                case
                  when active_heartbeat = true then ad.device_id
                end
              ) as total_devices_heartbeats,
              count(distinct
                case
                  when
                    trip_count is not null
                    and trip_count <> 0
                  then
                    ad.device_id
                end
              ) as total_devices_trips
            from
              dataprep.active_devices ad
                --join active devices (ad) to organizations to get region data
                left join clouddb.organizations o
                  on o.id = ad.org_id
            group by
              ad.date,
              ad.product_id,
              o.locale
          ) cad
            --join to definitions.products table to get product names
            left join definitions.products p
              on p.product_id = cad.prod_id
            --join to definitions.445_calendar to get fiscal calendar dates
            left join definitions.445_calendar cal
              on cal.date = cad.active_date
      ) cad_d
    --filter to after 2019 and product families (VG, AG, CM, IG)
    WHERE
      cad_d.active_date >= to_date('2018-01-01')
      and cad_d.active_date < date '2024-08-01'
      and cad_d.product_family IN ('VG', 'AG', 'CM', 'IG', 'SG')
  )
