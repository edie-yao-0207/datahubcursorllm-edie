with final_data as (
select ad.*, case when dd.date is null then 'Not Detected' else 'Detected' end as type 
from (select y.date, x.*
    from (select  peripheral_asset_id, min(date) as first_date
        from hardware_analytics.at11_data as han
        join clouddb.organizations as o1
        on han.peripheral_org_id = o1.id
        join clouddb.organizations as o2
        on han.scanner_org_id = o2.id
        join hardware_analytics.at11_devices as d
        on han.peripheral_org_id = d.org_id
        and han.peripheral_asset_id = d.gateway_id
        and han.peripheral_device_id = d.object_id
        where o1.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{org_type}})
        and o2.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{scan_org_type}})
        and (han.fw_version in ({{fw}}) or 'all' in ({{fw}}))
        and han.date between '{{date_range.start}}' and '{{date_range.end}}'
        group by peripheral_asset_id )  as x
    join (select * from definitions.`445_calendar` where date > '2023-01-18' and date < current_date()) as y
    where y.`date` >= x.first_date) as ad
left join (select  peripheral_asset_id, date
        from hardware_analytics.at11_data as han
        join clouddb.organizations as o1
        on han.peripheral_org_id = o1.id
        join clouddb.organizations as o2
        on han.scanner_org_id = o2.id
        join hardware_analytics.at11_devices as d
        on han.peripheral_org_id = d.org_id
        and han.peripheral_asset_id = d.gateway_id
        and han.peripheral_device_id = d.object_id
        where o1.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{org_type}})
        and o2.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{scan_org_type}})
        and (han.fw_version in ({{fw}}) or 'all' in ({{fw}}))
        and han.date between '{{date_range.start}}' and '{{date_range.end}}'
        group by peripheral_asset_id,  date) as dd
on ad.date = dd.`date`
and ad.peripheral_asset_id = dd.peripheral_asset_id 
)

select date, type, count(distinct(peripheral_asset_id)) as total_assets
from final_data
group by date, type
