select
    l.device_uuid as device_uuid,
    l.org_id as org_id,
    l.driver_id as driver_id,
    l.timestamp as timestamp,
    i.platform as platform,
    l.date as date,
    (get_json_object(l.json_params, '$.cleanDestination')) as destination
from
    datastreams.mobile_logs l
left outer join datastreams.mobile_device_info i
    on l.device_uuid = i.device_uuid
where
    l.event_type = 'GLOBAL_NAVIGATE'
    and l.date >= date_sub(current_date(), 1)
    and l.driver_id > 0
    and l.org_id > 0
    and i.timestamp = (
        select max(timestamp) from datastreams.mobile_device_info
            where device_uuid = i.device_uuid
    )

