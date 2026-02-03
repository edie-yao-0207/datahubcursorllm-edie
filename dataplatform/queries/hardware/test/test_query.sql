select cast(date as date) as date, peripheral_product_id, count(distinct(asset_id)) as total_assets
from hardware_analytics.hansel_data as han
join clouddb.organizations as o
on han.peripheral_org_id = o.id
where peripheral_product_id = 172
and o.internal_type in ({{org_type}})
and han.fw_version in ({{fw}})
and han.date between '{{date_range.start}}' and '{{date_range.end}}'
group by date, peripheral_product_id