select cast(han.date as date) as date, 
        percentile_approx(han.rssi, 0.01) as p01,
        percentile_approx(han.rssi, 0.25) as p25,
        percentile_approx(han.rssi, 0.50) as p50,
        percentile_approx(han.rssi, 0.75) as p75,
        percentile_approx(han.rssi, 0.90) as p90,
        percentile_approx(han.rssi, 0.95) as p95,
        percentile_approx(han.rssi, 0.99) as p99
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
group by date
