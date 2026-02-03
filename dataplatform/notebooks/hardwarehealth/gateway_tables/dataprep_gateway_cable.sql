create or replace table hardware_dev.meenu_ag52_cable_date_ranges as (
select org_id, object_id as gateway_id, cable_id, date as from_date, date_sub(next_date, 1) as to_date
from (select *, lead(date) over(PARTITION BY object_id, org_id ORDER BY date ASC) as next_date
      from ((SELECT min(date) as date, c.org_id, object_id, min_by(value.int_value, time) as cable_id, null as next_cable -- very first cable
              FROM kinesisstats_history.osgobdcableid as c
              join productsdb.gateways as g
              on c.object_id = g.id
              where g.product_id in (125, 140, 143, 144)
              group by c.org_id, c.object_id)
            union
            (select * -- from the first transition
            from (SELECT date, org_id, object_id, cable_id, 
                  lead(cable_id) over(PARTITION BY object_id, org_id ORDER BY date ASC) as next_cable
                  FROM (SELECT date, c.org_id, object_id, max_by(value.int_value, time) as cable_id
                        FROM kinesisstats_history.osgobdcableid as c
                        join clouddb.gateways as g
                        on c.object_id = g.id
                        where g.product_id in (125, 140, 143, 144)
                        group by date, c.org_id, c.object_id)
                  )
            where cable_id <> next_cable or next_cable is null)
      )
)
);

create or replace table hardware_analytics.ag52_gateway_cable as 
(select x.date, x.org_id, x.object_id, x.gateway_id, x.serial, x.product_id, y.cable_id
from (select * from hardware.active_gateways where product_id in (125, 140, 143, 144)) as x
left join (select a.date, a.org_id, a.gateway_id, c.cable_id
        from (select * from hardware.active_gateways where product_id in (125, 140, 143, 144)) as a
        join (select org_id, gateway_id, cable_id, from_date, coalesce(to_date, current_date) as to_date
        from hardware_dev.meenu_ag5x_cable_date_ranges) as c
        on a.gateway_id = c.gateway_id
        and a.org_id = c.org_id
        where a.date between c.from_date and c.to_date) as y
on x.date = y.date
and x.gateway_id = y.gateway_id
and x.org_id = y.org_id);