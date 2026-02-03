create or replace table hardware_analytics.ag51_devices as 
(
  select g.*
  from (select a.object_id, 
        a.gateway_id, 
        a.org_id,
        a.product_id,
        a.first_heartbeat_date,
        a.last_heartbeat_date,
        max_by(hwrev.hw_rev, hwrev.date) as hw_rev
      from hardware.gateways_heartbeat as a
      left join (select date, org_id, object_id, max_by(value.proto_value.nordic_factory_config_debug.hw_rev, time) as hw_rev
                from kinesisstats.osdnordicfactoryconfigdebug
                group by date, org_id, object_id ) as hwrev
      on a.object_id = hwrev.object_id
      and a.org_id = hwrev.org_id
      where gateway_id is not null
      and hwrev.date between a.first_heartbeat_date and a.last_heartbeat_date
      group by a.object_id, a.gateway_id, a.org_id, a.product_id, a.first_heartbeat_date, a.last_heartbeat_date) as g
  join clouddb.organizations as o
  on g.org_id = o.id
  where o.internal_type == 0
  and org_id not in (select org_id from internaldb.simulated_orgs)
  and g.product_id in (124, 142)
);

create or replace table hardware_analytics.ag52_devices as 
(
  select g.*
  from (select a.object_id, 
        a.gateway_id, 
        a.org_id,
        a.product_id,
        a.first_heartbeat_date,
        a.last_heartbeat_date,
        max_by(hwrev.hw_rev, hwrev.date) as hw_rev
      from hardware.gateways_heartbeat as a
      left join (select date, org_id, object_id, max_by(value.proto_value.nordic_factory_config_debug.hw_rev, time) as hw_rev
                from kinesisstats.osdnordicfactoryconfigdebug
                group by date, org_id, object_id ) as hwrev
      on a.object_id = hwrev.object_id
      and a.org_id = hwrev.org_id
      where gateway_id is not null
      and hwrev.date between a.first_heartbeat_date and a.last_heartbeat_date
      group by a.object_id, a.gateway_id, a.org_id, a.product_id, a.first_heartbeat_date, a.last_heartbeat_date) as g
  join clouddb.organizations as o
  on g.org_id = o.id
  where o.internal_type == 0
  and org_id not in (select org_id from internaldb.simulated_orgs)
  and g.product_id in (125, 143)
);

create or replace table hardware_analytics.ag5x_powered_devices as 
(
  select g.*
  from (select a.object_id, 
        a.gateway_id, 
        a.org_id,
        a.product_id,
        a.first_heartbeat_date,
        a.last_heartbeat_date,
        max_by(hwrev.hw_rev, hwrev.date) as hw_rev
      from hardware.gateways_heartbeat as a
      left join (select date, org_id, object_id, max_by(value.proto_value.nordic_factory_config_debug.hw_rev, time) as hw_rev
                from kinesisstats.osdnordicfactoryconfigdebug
                group by date, org_id, object_id ) as hwrev
      on a.object_id = hwrev.object_id
      and a.org_id = hwrev.org_id
      where gateway_id is not null
      and hwrev.date between a.first_heartbeat_date and a.last_heartbeat_date
      group by a.object_id, a.gateway_id, a.org_id, a.product_id, a.first_heartbeat_date, a.last_heartbeat_date) as g
  join clouddb.organizations as o
  on g.org_id = o.id
  where o.internal_type == 0
  and org_id not in (select org_id from internaldb.simulated_orgs)
  and g.product_id in (125, 143, 140, 144)
);

