select cast(date as date) as date, scanner_product_id, dp.name, count(distinct(scanner_object_id)) as total_scanners
from hardware_analytics.at11_data as han
join clouddb.organizations as o1
on han.peripheral_org_id = o1.id
join clouddb.organizations as o2
on han.scanner_org_id = o2.id
join hardware_analytics.at11_devices as d
on han.peripheral_org_id = d.org_id
and han.peripheral_asset_id = d.gateway_id
and han.peripheral_device_id = d.object_id
left join definitions.products as dp
on han.scanner_product_id = dp.product_id
where o1.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{org_type}})
and o2.internal_type in (SELECT case when col1 = 'Customer' then 0 when col1='Internal' then 1 end as col FROM VALUES {{scan_org_type}})
and (han.fw_version in ({{fw}}) or 'all' in ({{fw}}))
and han.date between '{{date_range.start}}' and '{{date_range.end}}'
group by date, scanner_product_id, dp.name
