SELECT
  id,
  name,
  CASE
    WHEN internal_type = 1 THEN "internal,all"
    ELSE "external,all"
  END as org_type
FROM
  clouddb.organizations
where
  id in (
    select
      org_id
    from
      productsdb.devices
    where
      devices.id in (
        select
          *
        from
          apptelematics.device_ids
      )
  )
