with active_drivers as (
    select
        d.id as id,
        d.name as name,
        d.org_id as org_id,
        (
            select count(*)
            from datastreams.mobile_logs l
            where
                d.id = l.driver_id
                and l.event_type = 'GLOBAL_NAVIGATE'
                and get_json_object(l.json_params, '$.cleanDestination') = '/driver/hos/home'
                and l.date > date_sub(current_date(), 7)
        ) as hos_navigation_count,
        (
            select count(*)
            from datastreams.mobile_logs l
            where
                d.id = l.driver_id
                and l.event_type = 'GLOBAL_NAVIGATE'
                and get_json_object(l.json_params, '$.cleanDestination') = '/driver/messages'
                and l.date > date_sub(current_date(), 7)
        ) as messaging_navigation_count,
        (
            select count(*)
            from datastreams.mobile_logs l
            where
                d.id = l.driver_id
                and l.event_type = 'GLOBAL_NAVIGATE'
                and get_json_object(l.json_params, '$.cleanDestination') like '/driver/dispatch%'
                and l.date > date_sub(current_date(), 7)
        ) as routes_navigation_count,
        (
            select count(*)
            from datastreams.mobile_logs l
            where
                d.id = l.driver_id
                and l.event_type = 'GLOBAL_NAVIGATE'
                and get_json_object(l.json_params, '$.cleanDestination') = '/driver/dvir/list'
                and l.date > date_sub(current_date(), 7)
        ) as dvir_navigation_count,
        (
            select count(*)
            from datastreams.mobile_logs l
            where
                d.id = l.driver_id
                and l.event_type = 'GLOBAL_NAVIGATE'
                and get_json_object(l.json_params, '$.cleanDestination') = '/driver/documents'
                and l.date > date_sub(current_date(), 7)
        ) as documents_navigation_count
    from clouddb.drivers d
        where
            (
                select count(*)
                from datastreams.mobile_logs l
                where
                    d.id = l.driver_id
                    and l.date > date_sub(current_date(), 7)
            ) > 0
)
select
    id,
    name,
    org_id,
    case when (hos_navigation_count > 0) then 1 else 0 end as uses_hos,
    case when (messaging_navigation_count > 0) then 1 else 0 end as uses_messaging,
    case when (dvir_navigation_count > 0) then 1 else 0 end as uses_dvir,
    case when (routes_navigation_count > 0) then 1 else 0 end as uses_routes,
    case when (documents_navigation_count > 0) then 1 else 0 end as uses_documents
from active_drivers

