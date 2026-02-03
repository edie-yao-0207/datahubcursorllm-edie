-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Eligibility Criteria
-- MAGIC No need to run this again unless you intentionally want to regenerate the eligible orgs.
-- MAGIC
-- MAGIC The rest of the notebook uses `dataproducts.eligible_orgs` which was last run 12/17 and
-- MAGIC contains 15241 orgs.

-- COMMAND ----------

/* Unique active vehicles since August 1st */
create or replace temp view org_active_vehicles_since_august as
select
  ad.org_id,
  count(distinct case when trip_count is not null and trip_count <> 0 then ad.device_id end) as unique_active_vehicles
from dataprep.active_devices ad
left join productsdb.gateways gw on
  gw.org_id = ad.org_id and
  gw.device_id = ad.device_id
left join clouddb.organizations o on
  o.id = ad.org_id
where
  o.internal_type != 1 and
--   o.quarantine_enabled != 1 and
  gw.product_id in (7,24,17,35) and
  ad.date >= to_date("2020-08-01") and
  ad.date <= to_date('2020-11-30')
group by
  ad.org_id;

-- COMMAND ----------

/* Total distance driven this year by org */
create or replace temp view distance_driven_this_year as
select
  trips.org_id,
  sum(proto.trip_distance.distance_meters) * 0.000621371 as total_trip_distance_miles
from trips2db_shards.trips trips
left join productsdb.gateways gw on
  gw.org_id = trips.org_id and
  gw.device_id = trips.device_id
left join clouddb.organizations o on
  o.id = trips.org_id
where
  o.internal_type != 1 and
--   o.quarantine_enabled != 1 and
  gw.product_id in (7,24,17,35) and
  trips.date >= to_date("2020-01-01") and
  trips.date <= to_date('2020-11-30') and
  (proto.end.time-proto.start.time)<(24*60*60*1000) and /*Filter out very long trips*/
  proto.start.time != proto.end.time and /* legit trips only */
  trips.version = 101
group by trips.org_id;

-- COMMAND ----------

create or replace temp view eligible_orgs as
select
  dd.org_id,
  unique_active_vehicles,
  total_trip_distance_miles
from org_active_vehicles_since_august as av
inner join distance_driven_this_year as dd
  on av.org_id = dd.org_id
where unique_active_vehicles >= 3      /* These filters determine the eligibility criteria */
and total_trip_distance_miles >= 1000; /* They were determined by observing distributions and funnel analysis */

cache table eligible_orgs;

-- COMMAND ----------

-- Uncomment if eligible criteria changes to save the table for analysis
-- drop table if exists dataproducts.eligible_orgs;
-- create table if not exists dataproducts.eligible_orgs as (
--   select * from eligible_orgs
-- )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- 15040 as of 7:08pm 12/16
-- MAGIC -- 15025 as of 10:56am 12/17
-- MAGIC select count(1) from eligible_orgs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Total Miles Stats

-- COMMAND ----------

/* Query for total miles driven, total hours driven, total number of trips, and longest trip */
create or replace temp view trip_stats as
select
  org_id,
  sum(proto.trip_distance.distance_meters) * 0.000621371 as total_trip_distance_miles,
  sum(proto.end.time-proto.start.time) / 1000 / 60 / 60 as total_trip_duration_hours,
  count(1) as total_num_trips,
  max(proto.trip_distance.distance_meters) * 0.000621371 as longest_trip_distance_miles
from trips2db_shards.trips trips
where date >= to_date('2020-01-01')
  and date <= to_date('2020-11-30')
  and (proto.end.time-proto.start.time)<(24*60*60*1000) /*Filter out very long trips*/
  and proto.start.time != proto.end.time /* legit trips only */
  and (proto.trip_distance.distance_meters) * 0.000621371 <= 1440 /*Filter out impossibly long distance trips*/
  and org_id in (select org_id from dataproducts.eligible_orgs)
  and trips.version = 101
group by org_id;

-- COMMAND ----------

/* Get active devices per org */
create or replace temp view org_active_vehicles_this_year as
select
  ad.org_id,
  count(distinct case when trip_count is not null and trip_count <> 0 then ad.device_id end) as unique_active_vehicles
from dataprep.active_devices ad
left join productsdb.gateways gw on
  gw.org_id = ad.org_id and
  gw.device_id = ad.device_id
left join clouddb.organizations o on
  o.id = ad.org_id
where
  o.internal_type != 1 and
  o.quarantine_enabled != 1 and
  gw.product_id in (7,24,17,35) and
  ad.date >= to_date("2020-01-01") and
  ad.date <= to_date('2020-11-30') and
  ad.org_id in (select org_id from dataproducts.eligible_orgs)
group by
  ad.org_id;

-- COMMAND ----------

/* Combine trips and active devices to get stats for Total Miles */
create or replace temp view total_miles_stats as
select
  ts.org_id,
  total_trip_distance_miles,
  total_trip_duration_hours,
  total_num_trips,
  longest_trip_distance_miles,
  total_trip_distance_miles/unique_active_vehicles as avg_miles_per_vehicle
from trip_stats as ts
left join org_active_vehicles_this_year as oav
  on ts.org_id = oav.org_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Popular Sites
-- MAGIC https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3299941134683274/command/3299941134689093

-- COMMAND ----------

/* Query TOS report to get amount of time on site per org/addr */
create or replace temporary view tos_2020_by_site as
select
  org_id,
  address_id,
  sum(time_on_site_ms) as total_time_on_site_ms,
  sum(visit_start_count) as visit_count
from delta.`s3://samsara-report-staging-tables/report_aggregator/time_on_site_report/`
where date >= to_date('2020-01-01')
and date <= to_date('2020-11-30')
and org_id in (select org_id from dataproducts.eligible_orgs)
group by org_id, address_id
ORDER BY total_time_on_site_ms desc;

-- COMMAND ----------

/* Get top 5 highest dwell time addresses per org */
create or replace temporary view tos_2020_org_top_5 as
WITH t AS
(
   SELECT *,
     ROW_NUMBER() OVER
     (PARTITION BY org_id ORDER BY total_time_on_site_ms DESC) as rn
   FROM tos_2020_by_site
)
SELECT *
  FROM t
  WHERE rn <= 5
  ORDER BY org_id, total_time_on_site_ms DESC;

-- COMMAND ----------

create or replace temporary view tos_2020_org_top_5_by_org as
select *
from
(
  select org_id, address_id, rn
  from tos_2020_org_top_5
) t
pivot
(
  first(address_id)
  for rn in (1 as address_1 , 2 as address_2, 3 as address_3, 4 as address_4, 5 as address_5)
)
order by org_id;

select * from tos_2020_org_top_5_by_org;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Dashboard Usage Stats
-- MAGIC https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/3878763834928583/command/3299941134693029

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Most Frequent Visited Reports

-- COMMAND ----------

-- QUERY TO MAP ROUTES TO REPORT NAMES
create or replace temporary view report_route_loads as
select
  OrgId,
  ResourceName,
  case
    when ResourceName = "/o/:org_id/fleet/reports/activity" then "activity"
    when (
        ResourceName = "/o/:org_id/fleet/reports/asset/billing" or
        ResourceName = "/o/:org_id/fleet/reports/asset/billing/site"
    ) then "billing"
    when ResourceName = "/o/:org_id/fleet/reports/asset/dormancy" then "asset_dormancy"
    when ResourceName = "/o/:org_id/fleet/reports/asset/schedule" then "asset_schedule"
    when ResourceName = "/o/:org_id/fleet/reports/camera_health" then "camera_health"
    when (
        ResourceName = "/o/:org_id/fleet/reports/cameras" or
        ResourceName = "/o/:org_id/fleet/reports/cameras/:deviceId/trips" or
        ResourceName = "/o/:org_id/fleet/reports/cameras/visual_review" or
        ResourceName = "/o/:org_id/fleet/reports/multicam" or
        ResourceName = "/o/:org_id/fleet/reports/multicam/trip/:deviceId"
    ) then "cameras"
    when (
        ResourceName = "/o/:org_id/fleet/reports/coaching" or
        ResourceName = "/o/:org_id/fleet/reports/coaching/driver/:driverId/events/:eventIdentifiers" or
        ResourceName = "/o/:org_id/fleet/reports/coaching/manager/:safetyManagerId"
    ) then "coaching"
    when (
        ResourceName = "/o/:org_id/fleet/reports/colocation" or
        ResourceName = "/o/:org_id/fleet/reports/colocation/:uuid" or
        ResourceName = "/o/:org_id/fleet/reports/colocation/:uuid/driver/:driverId" or
        ResourceName = "/o/:org_id/fleet/reports/colocation/create"
    ) then "co-location"
    when ResourceName = "/o/:org_id/fleet/reports/compliance_dashboard" then "compliance_dashboard"
    when ResourceName = "/o/:org_id/fleet/reports/safety/crash" then "crash"
    when (
        ResourceName = "/o/:org_id/fleet/reports/custom" or
        ResourceName = "/o/:org_id/fleet/reports/custom/:configUuid"
    ) then "custom"
    when (
        ResourceName = "/o/:org_id/fleet/reports/dashcam" or
        ResourceName = "/o/:org_id/fleet/reports/dashcam/:deviceId" or
        ResourceName = "/o/:org_id/fleet/reports/dashcam/:deviceId/trip/:startMs"
    ) then "dashcam"
    when (
        ResourceName = "/o/:org_id/fleet/reports/asset/detention" or
        ResourceName = "/o/:org_id/fleet/reports/asset/detention/site"
    ) then "detention"
    when (
        ResourceName = "/o/:org_id/fleet/reports/driver_assignment" or
        ResourceName = "/o/:org_id/fleet/reports/driver_assignment/driver/:driverId/training_images"
    ) then "driver_assignment"
    when (
        ResourceName = "/o/:org_id/fleet/reports/documents" or
        ResourceName = "/o/:org_id/fleet/reports/documents/:driverId/:driverCreatedAtMs" or
        ResourceName = "/o/:org_id/fleet/reports/documents/:driverId/:driverCreatedAtMs/edit" or
        ResourceName = "/o/:org_id/fleet/reports/documents/type/:uuid/edit" or
        ResourceName = "/o/:org_id/fleet/reports/documents/type/create"
    ) then "driver_documents"
    when ResourceName = "/o/:org_id/fleet/reports/driver_efficiency" then "driver_efficiency"
    when (
        ResourceName = "/o/:org_id/fleet/reports/hos_audit" or
        ResourceName = "/o/:org_id/fleet/reports/hos_audit/report" or
        ResourceName = "/o/:org_id/fleet/reports/hos_transfer"
    ) then "driver_hours_of_service_audit"
    when ResourceName = "/o/:org_id/fleet/reports/driver_qualifications/dashboard" then "driver_qualifications"
    when (
        ResourceName = "/o/:org_id/fleet/reports/drivers_hours" or
        ResourceName = "/o/:org_id/fleet/reports/drivers_hours/:driverId"
    ) then "driver_hours"
    when ResourceName = "/o/:org_id/fleet/reports/hos/status" then "duty_status_summary"
    when ResourceName = "/o/:org_id/fleet/reports/digio" then "equipment_report"
    when (
        ResourceName = "/o/:org_id/fleet/reports/ev_charging" or
        ResourceName = "/o/:org_id/fleet/reports/ev_charging/location" or
        ResourceName = "/o/:org_id/fleet/reports/ev_charging/vehicle" or
        ResourceName = "/o/:org_id/fleet/reports/ev_efficiency"
    ) then "ev_charging"
    when (
        ResourceName = "/o/:org_id/fleet/reports/travel_expenses" or
        ResourceName = "/o/:org_id/fleet/reports/travel_expenses/:driverId"
    ) then "expense"
    when ResourceName = "/o/:org_id/fleet/reports/fleet_benchmarks" then "fleet_benchmarks"
    when ResourceName = "/o/:org_id/fleet/reports/fleet_electrification" then "fleet_electrification"
    when (
        ResourceName = "/o/:org_id/fleet/reports/fuel" or
        ResourceName = "/o/:org_id/fleet/reports/fuel_energy"
    ) then "fuel_&_energy"
    when (
        ResourceName = "/o/:org_id/fleet/reports/fuel_purchases" or
        ResourceName = "/o/:org_id/fleet/reports/fuel_purchases/purchase/:uuid"
    ) then "fuel_purchases"
    when ResourceName = "/o/:org_id/fleet/reports/gateway_health" then "gateway_health"
    when ResourceName = "/o/:org_id/fleet/reports/safety/harsh_event/:harshEventLabel" then "harsh_driving"
    when ResourceName = "/o/:org_id/fleet/reports/asset/historic_diagnostic" then "historic_diagnostic"
    when (
        ResourceName = "/o/:org_id/fleet/reports/hos/driver" or
        ResourceName = "/o/:org_id/fleet/reports/hos/driver/driver/:driver_id" or
        ResourceName = "/o/:org_id/fleet/reports/hos/driver_classic" or
        ResourceName = "/o/:org_id/fleet/reports/hos/driver_v2"
    ) then "hours_of_service"
    when (
        ResourceName = "/o/:org_id/fleet/reports/hos/violations" or
        ResourceName = "/o/:org_id/fleet/reports/hos/violations_v2"
    ) then "hours_of_service_violations"
    when ResourceName = "/o/:org_id/fleet/reports/ifta" then "IFTA"
    when (
        ResourceName = "/o/:org_id/fleet/reports/infringement" or
        ResourceName = "/o/:org_id/fleet/reports/infringement/:driverId"
    ) then "infringement"
    when (
        ResourceName = "/o/:org_id/fleet/reports/asset/inventory" or
        ResourceName = "/o/:org_id/fleet/reports/asset/inventory/:siteAddressId/show" or
        ResourceName = "/o/:org_id/fleet/reports/asset/inventory/search" or
        ResourceName = "/o/:org_id/fleet/reports/asset/inventory/search/report"
    ) then "inventory"
    when (
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/address/:addressId" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/driver/:driverId" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/vehicle/:vehicleId" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees/driver/:driverId" or
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees/vehicle/:vehicleId"
    ) then "planned_vs_actual"
    when (
        ResourceName = "/o/:org_id/fleet/reports/privacy_sessions" or
        ResourceName = "/o/:org_id/fleet/reports/privacy_sessions/:deviceId"
    ) then "privacy_sessions"
    when (
        ResourceName = "/o/:org_id/fleet/reports/route_runs" or
        ResourceName = "/o/:org_id/fleet/reports/route_runs/:recurringRouteId"
    ) then "recurring_routes"
    when (
        ResourceName = "/o/:org_id/fleet/reports/asset/reefer" or
        ResourceName = "/o/:org_id/fleet/reports/asset/reefer/:deviceId"
    ) then "reefer"
    when (
        ResourceName = "/o/:org_id/fleet/reports/edu/rides" or
        ResourceName = "/o/:org_id/fleet/reports/edu/rides/routes" or
        ResourceName = "/o/:org_id/fleet/reports/edu/rides/scans" or
        ResourceName = "/o/:org_id/fleet/reports/edu/rides/students" or
        ResourceName = "/o/:org_id/fleet/reports/edu/rides/vehicles"
    ) then "rides"
    when (
        ResourceName = "/o/:org_id/fleet/reports/safety" or
        ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/driver/:driverId" or
        ResourceName = "/o/:org_id/fleet/reports/safety/driver/:driverId"
    ) then "safety_dashboard"
    when ResourceName = "/o/:org_id/fleet/reports/safety_inbox" then "safety_inbox"
    when (
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/collision_risk" or
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/distracted_driving" or
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/harsh_events" or
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/policy_violations" or
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/speeding" or
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/traffic_signs" or
        ResourceName = "/o/:org_id/fleet/reports/safety/tag" or
        ResourceName = "/o/:org_id/fleet/reports/safety/vehicle/:vehicleId" or
        ResourceName = "/o/:org_id/fleet/reports/safety/vehicle/:vehicleId/incident/:atMs"
    ) then "safety"
    when ResourceName = "/o/:org_id/fleet/reports/sensor_health" then "sensor_health"
    when ResourceName = "/o/:org_id/fleet/reports/vehicle_timesheet" then "start/stop_report"
    when ResourceName = "/o/:org_id/fleet/reports/tachograph_explorer" then "tachograph_explorer"
    when (
        ResourceName = "/o/:org_id/fleet/reports/remote_tachograph" or
        ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/driver/:cardNumber" or
        ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/driver/:cardNumber/analysis/:timestamp" or
        ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/vehicle/:deviceId"
    ) then "tachograph_file_downloads"
    when (
        ResourceName = "/o/:org_id/fleet/reports/site" or
        ResourceName = "/o/:org_id/fleet/reports/site/:addressId/show" or
        ResourceName = "/o/:org_id/fleet/reports/site/address" or
        ResourceName = "/o/:org_id/fleet/reports/site/unknown" or
        ResourceName = "/o/:org_id/fleet/reports/site/vehicle" or
        ResourceName = "/o/:org_id/fleet/reports/site/vehicle/:deviceId/show" or
        ResourceName = "/o/:org_id/fleet/reports/site/vehicle_v2"
    ) then "time_on-site"
    when ResourceName = "/o/:org_id/fleet/reports/trips" then "trip_history"
    when (
        ResourceName = "/o/:org_id/fleet/reports/unassigned_tachograph_driving" or
        ResourceName = "/o/:org_id/fleet/reports/unassigned_tachograph_driving/:deviceId"
    ) then "unassigned_driving"
    when (
        ResourceName = "/o/:org_id/fleet/reports/hos/unassigned" or
        ResourceName = "/o/:org_id/fleet/reports/hos/unassigned/:vehicleId" or
        ResourceName = "/o/:org_id/fleet/reports/hos/unassigned_v2"
    ) then "unassigned_HOS"
    when ResourceName = "/o/:org_id/fleet/reports/asset/utilization" then "utilization"
    when (
        ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments" or
        ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments/driver/:driverId" or
        ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments/vehicle/:vehicleId"
    ) then "vehicle_assignments"
    when (
        ResourceName = "/o/:org_id/fleet/reports/video_retrieval" or
        ResourceName = "/o/:org_id/fleet/reports/video_retrieval/v2"
    ) then "video_retrieval"
  end as report_name
from routeload_logs.routeload
where ResourceName not like "/o/:org_id/fleet/reports/index" and
ResourceName like "/o/:org_id/fleet/reports/%" and
UserEmail not like "%@samsara.com" and
date >= to_date("2020-01-01") and
date <= to_date('2020-11-30');

-- COMMAND ----------

-- QUERY FOR REPORT ROUTE LOADS BY ORG ID RANKED
create or replace temporary view org_route_loads as
select * from (
  select
    OrgId as org_id,
    report_name,
    count(*) as count,
    row_number() over (partition by OrgId order by count(*) desc) as route_rank
  from report_route_loads
  where report_name is not null and
  OrgId in (select org_id from dataproducts.eligible_orgs)
  group by OrgId, report_name
) ranks
where route_rank <= 5;

-- COMMAND ----------

create or replace temporary view top_report_names_by_org as
select *
from (
  select org_id, report_name, route_rank
  from org_route_loads
) t
pivot (
  first(report_name)
  for route_rank in (1 as report_1 , 2 as report_2, 3 as report_3, 4 as report_4, 5 as report_5)
)
order by org_id;

-- COMMAND ----------

create or replace temporary view top_report_count_by_org as
select *
from (
  select org_id, count, route_rank
  from org_route_loads
) t
pivot (
  first(count)
  for route_rank in (1 as report_1_count , 2 as report_2_count, 3 as report_3_count, 4 as report_4_count, 5 as report_5_count)
)
order by org_id;

-- COMMAND ----------

create or replace temp view total_page_view_count as
select
  OrgId as org_id,
  count(1) as total_page_view_count
from routeload_logs.routeload
where UserEmail not like "%@samsara.com" and
  date >= to_date("2020-01-01") and
  date <= to_date('2020-11-30') and
  OrgId in (select org_id from dataproducts.eligible_orgs)
group by org_id;

-- COMMAND ----------

create or replace temporary view top_reports_by_org as
select
  t1.org_id,
  report_1,
  report_2,
  report_3,
  report_4,
  report_5,
  report_1_count,
  report_2_count,
  report_3_count,
  report_4_count,
  report_5_count,
  total_page_view_count
from top_report_names_by_org as t1
join top_report_count_by_org as t2 on t1.org_id = t2.org_id
join total_page_view_count as t3 on t1.org_id = t3.org_id;

select * from top_reports_by_org;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top Alerts Configured

-- COMMAND ----------

-- Get all alert ids
create or replace temp view alert_ids as
select id as alert_id, group_id
from clouddb.alerts
where deleted_at is null and
is_admin is false;

-- COMMAND ----------

create or replace temp view alert_individual_devices as
select
  organization_id as org_id,
  count(*) as configure_count,
  definitions.alert_types.name
from alert_ids
join clouddb.groups
  on clouddb.groups.id = group_id
join clouddb.alert_conditions
  on clouddb.alert_conditions.alert_id = alert_ids.alert_id
join definitions.alert_types
  on  clouddb.alert_conditions.type = definitions.alert_types.id
group by org_id, definitions.alert_types.name

-- COMMAND ----------

create or replace temporary view alert_device_count_by_org as
select * from (
  select
    org_id,
    configure_count,
    name as alert_name,
    row_number() over (partition by org_id order by configure_count desc) as alert_rank
  from alert_individual_devices
  where org_id in (select org_id from dataproducts.eligible_orgs)
) ranks
where alert_rank <= 3;

-- COMMAND ----------

/* Top alerts by name */
create or replace temporary view top_alert_names_by_org as
select *
from (
  select org_id, alert_name, alert_rank
  from alert_device_count_by_org
) t
pivot (
  first(alert_name)
  for alert_rank in (1 as alert_1 , 2 as alert_2, 3 as alert_3)
)
order by org_id;

-- COMMAND ----------

create or replace temporary view top_alert_count_by_org as
select *
from (
  select org_id, configure_count as count, alert_rank
  from alert_device_count_by_org
) t
pivot (
  first(count)
  for alert_rank in (1 as alert_1_count , 2 as alert_2_count, 3 as alert_3_count)
)
order by org_id;

-- COMMAND ----------

create or replace temp view alert_total_count_by_org as
select
  organization_id as org_id,
  count(*) as total_alerts_configured
from alert_ids
join clouddb.groups
  on clouddb.groups.id = group_id
where organization_id in (select org_id from dataproducts.eligible_orgs)
group by organization_id;

-- COMMAND ----------

create or replace temporary view top_alerts_by_org as
select
  t1.org_id,
  alert_1,
  alert_2,
  alert_3,
  alert_1_count,
  alert_2_count,
  alert_3_count,
  total_alerts_configured
from top_alert_names_by_org as t1
join top_alert_count_by_org as t2 on t1.org_id = t2.org_id
join alert_total_count_by_org as t3 on t1.org_id = t3.org_id;

select * from top_alerts_by_org;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Safety Stats

-- COMMAND ----------

create or replace temp view safety_events as
select
  *
from safetydb_shards.safety_events events
where events.date >= to_date('2020-01-01')
  and events.date <= to_date('2020-11-30')
  and
      (
        /*
            Filter out events that happen under 5 mph unless
            they are haCrash (5) or haRolledStopSign (11)
        */
        detail_proto.start.speed_milliknots >= 4344.8812095 AND
        detail_proto.stop.speed_milliknots >= 4344.8812095 OR
        detail_proto.accel_type IN (5, 11)
      )
--   and
--       (
--         /* harsh accel, brake, crash, turn, rolling stop, near collision, distracted driving, tailgating */
--         detail_proto.accel_type IN (1,2,5,6,11,12,13,14)
--       )
  and events.org_id in (select org_id from dataproducts.eligible_orgs);

-- COMMAND ----------

create or replace temp view trips as
select
  org_id,
  device_id,
  proto.start.time AS start_ms,
  proto.end.time AS end_ms
from trips2db_shards.trips
where date >= to_date('2019-12-30') /* trips can take up to two days to be ingested */
  and date <= to_date('2020-11-30')
  and org_id in (select org_id from dataproducts.eligible_orgs)
  and version = 101;

-- COMMAND ----------

/* Join safety events and trips to get only events that happen on trips */
create or replace temp view safety_events_trips_only as
select
  se.org_id,
  se.device_id,
  event_ms,
  t.end_ms as trip_end_ms,
  detail_proto,
  additional_labels
from safety_events as se
inner join trips as t
  on se.org_id = t.org_id
  and se.device_id = t.device_id
  and se.event_ms between t.start_ms and t.end_ms;

-- COMMAND ----------

create or replace temp view safety_events_trips_only_with_metadata as
select
  safety_events_trips_only.org_id,
  safety_events_trips_only.device_id,
  safety_events_trips_only.event_ms,
  safety_events_trips_only.trip_end_ms,
  detail_proto,
  additional_labels,
  safetydb_shards.safety_event_metadata.inbox_state,
  safetydb_shards.safety_event_metadata.coaching_state
from safety_events_trips_only
left join safetydb_shards.safety_event_metadata
  on safety_events_trips_only.org_id = safetydb_shards.safety_event_metadata.org_id
  and safety_events_trips_only.device_id = safetydb_shards.safety_event_metadata.device_id
  and safety_events_trips_only.event_ms = safetydb_shards.safety_event_metadata.event_ms;

-- COMMAND ----------

create or replace temp view filtered_safety_events as
select
  *
from safety_events_trips_only_with_metadata
where
    /*
    Filter out events that are DISMISSED (2) inbox state AND
    coaching state in one of DISMISSED (3) MANUAL_REVIEW (6), AUTO_DISMISSED (8)
    */
  (coaching_state is NULL and inbox_state is NULL)
   or
  (inbox_state != 2 and coaching_state not in (3, 6, 8));

-- COMMAND ----------

/* Aggregate filtered safety events to get total number of safety events per org */
create or replace temp view safety_events_total as
select
  org_id,
  count(distinct detail_proto.event_id) as num_events_total
from filtered_safety_events
group by org_id;

-- COMMAND ----------

create or replace temp view safety_events_coached as
select
  events.org_id,
  coalesce(count(distinct detail_proto.event_id), 0) as num_events_coached
from filtered_safety_events as events
-- join definitions.harsh_accel_type_enums event_enum
--   on events.detail_proto.accel_type = event_enum.enum
left join playground.ava_coaching_state cs
  on cs.enum = coaching_state
where cs.enum in (2) -- coached
group by events.org_id;

-- COMMAND ----------

create or replace temp view safety_events_reviewed as
select
  events.org_id,
  coalesce(count(distinct detail_proto.event_id), 0) as num_events_reviewed
from filtered_safety_events as events
join definitions.harsh_accel_type_enums event_enum
  on events.detail_proto.accel_type = event_enum.enum
left join playground.ava_coaching_state cs
  on cs.enum = coaching_state
where cs.enum in (4) -- reviewed
group by events.org_id;

-- COMMAND ----------

create or replace temp view safety_events_recognized as
select
  events.org_id,
  coalesce(count(distinct detail_proto.event_id), 0) as num_events_recognized
from filtered_safety_events as events
join definitions.harsh_accel_type_enums event_enum
  on events.detail_proto.accel_type = event_enum.enum
left join playground.ava_coaching_state cs
  on cs.enum = coaching_state
where cs.enum in (10) -- recognized
group by events.org_id;

-- COMMAND ----------

/* Join num events total, num events coached, num events recognized, and num events reviewed */
create or replace temp view safety_stats as
select
  a.org_id,
  num_events_total,
  nullif(num_events_coached, 0) as num_events_coached,
  nullif(coalesce(num_events_reviewed,0) + coalesce(num_events_recognized,0) + coalesce(num_events_coached,0), 0) as num_events_reviewed
from safety_events_total as a
left join safety_events_coached as b
  on a.org_id = b.org_id
left join safety_events_reviewed as c
  on a.org_id = c.org_id
left join safety_events_recognized as d
  on a.org_id = d.org_id
where num_events_total > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Safety Video Uploads
-- MAGIC Defined as # video retrievals + # of safety event video uploads

-- COMMAND ----------

-- Get all event ids that are mp4s this year
create or replace temp view uploaded_mp4s as
select
  org_id,
  value.proto_value.uploaded_file_set.event_id as event_id
from kinesisstats.osDUploadedFileSet
where date >= to_date('2020-01-01')
  and date <= to_date('2020-11-30')
  and org_id in (select org_id from dataproducts.eligible_orgs)
  and value.proto_value.uploaded_file_set.event_id is not null
  and EXISTS(value.proto_value.uploaded_file_set.s3urls, url -> url like '%.mp4');

-- COMMAND ----------

-- Join safety events (using filtered safety events from above) and mp4 event ids together to get count and duration of safety events that sent up a video for each org
create or replace temporary view safety_events_uploads as
select
  fse.org_id,
  event_id,
  case when detail_proto.accel_type = 12 then 7 else 10 end as upload_duration_s -- Assume distracted driving is 7s and other safety event types are 10s
from filtered_safety_events as fse
left join uploaded_mp4s as u on
  fse.org_id = u.org_id and
  fse.detail_proto.event_id = u.event_id
where event_id is not null;

-- COMMAND ----------

-- Aggregate safety events uploads to get total number and duration
create or replace temporary view safety_events_uploads_metrics as
select
  org_id,
  count(event_id) as num_safety_event_uploads,
  sum(upload_duration_s)/60 as safety_event_uploads_duration_minutes
from safety_events_uploads
group by org_id;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create or replace temp view video_request_metrics_raw as (
-- MAGIC select
-- MAGIC   d.org_id,
-- MAGIC   vr.device_id,
-- MAGIC   vr.date,
-- MAGIC   case
-- MAGIC     when vr.state = 2 then 1
-- MAGIC     else 0
-- MAGIC   end as successful,
-- MAGIC   (vr.end_ms - vr.start_ms) / 1000 / 60 / 60 as requested_video_duration_minutes
-- MAGIC from
-- MAGIC   clouddb.historical_video_requests as vr
-- MAGIC   join productsdb.devices as d on vr.device_id = d.id
-- MAGIC where
-- MAGIC   vr.date >= '2020-01-01'
-- MAGIC   and vr.date <= '2020-11-30'
-- MAGIC   and vr.created_at_ms != 0
-- MAGIC   and d.org_id in (select org_id from dataproducts.eligible_orgs)
-- MAGIC );

-- COMMAND ----------

create or replace temp view video_request_metrics as (
select
  org_id,
  nullif(SUM(successful),0) as successful_video_requests,
  nullif(SUM(
    case
      when successful = 1 then requested_video_duration_minutes
      else 0
    end
  ), 0) as successful_video_duration_minutes
from
  video_request_metrics_raw
group by
  org_id
)

-- COMMAND ----------

create or replace temp view video_upload_stats as
select
  se.org_id,
  successful_video_requests,
  successful_video_duration_minutes,
  num_safety_event_uploads,
  safety_event_uploads_duration_minutes,
  coalesce(successful_video_requests, 0) + coalesce(num_safety_event_uploads, 0) as total_num_video_uploads,
  coalesce(successful_video_duration_minutes, 0) + coalesce(safety_event_uploads_duration_minutes,0) as total_video_uploads_duration_minutes
from safety_events_uploads_metrics as se
full outer join video_request_metrics as vr
  on se.org_id = vr.org_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fuel Stats
-- MAGIC Calculate avergage mpg for an org if the org has Fuel reporting for 90% of vehicles in fleet

-- COMMAND ----------

create or replace temp view org_device_fuel_consumed as
select
  org_id,
  object_id,
  sum(value.int_value) as fuel_consumed_ml
from kinesisstats.osdderivedfuelconsumed
where date >= to_date('2020-01-01')
  and date <= to_date('2020-11-30')
group by org_id, object_id;

-- COMMAND ----------

create or replace temp view org_device_energy_consumed as
select
  org_id,
  object_id,
  value.int_value - (
    LAG(value.int_value) OVER (PARTITION BY org_id, object_id ORDER BY time ASC)) as energy_consumed_uwh
from kinesisstats.osdevenergyconsumedmicrowh
where date >= to_date('2020-01-01')
  and date <= to_date('2020-11-30');

create or replace temp view org_device_energy_consumed as
select
  org_id,
  object_id,
  sum(energy_consumed_uwh) as energy_consumed_uwh
from org_device_energy_consumed
group by org_id, object_id;

-- COMMAND ----------

create or replace temp view org_device_energy_or_fuel as
select
  f.org_id,
  f.object_id,
  case when fuel_consumed_ml > 0 or energy_consumed_uwh > 0 then 1 else 0 end as is_fuel_or_energy
from org_device_fuel_consumed as f
left join org_device_energy_consumed as e
  on f.org_id = e.org_id
  and f.object_id = e.object_id
where f.org_id in (select org_id from dataproducts.eligible_orgs);

create or replace temp view org_energy_or_fuel_device_count as
select
  org_id,
  count(distinct object_id) as num_fuel_or_energy_devices
from org_device_energy_or_fuel
where is_fuel_or_energy = 1
group by org_id;

-- COMMAND ----------

/* Number unique active vehicles since January 1st by org*/
create or replace temp view eligible_org_active_vgs_this_year as
select
  ad.org_id,
  count(distinct case when trip_count is not null and trip_count <> 0 then ad.device_id end) as unique_active_vehicles
from dataprep.active_devices ad
left join productsdb.gateways gw on
  gw.org_id = ad.org_id and
  gw.device_id = ad.device_id
left join clouddb.organizations o on
  o.id = ad.org_id
where
  o.internal_type != 1 and
  o.quarantine_enabled != 1 and
  gw.product_id in (7,24,17,35) and
  ad.date >= to_date("2020-01-01") and
  ad.date <= to_date('2020-11-30') and
  ad.org_id in (select org_id from dataproducts.eligible_orgs)
group by
  ad.org_id;

-- COMMAND ----------

create or replace temp view org_fuel_energy_and_total_vgs as
select
  oav.org_id,
  coalesce(num_fuel_or_energy_devices,0) as num_fuel_or_energy_devices,
  unique_active_vehicles,
  num_fuel_or_energy_devices/unique_active_vehicles * 100 as percent_fuel_or_energy_vgs
from eligible_org_active_vgs_this_year as oav
left join org_energy_or_fuel_device_count as ef
  on oav.org_id = ef.org_id
where oav.org_id in (select org_id from eligible_org_active_vgs_this_year);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from org_fuel_energy_and_total_vgs where org_id = 14083;

-- COMMAND ----------

/* Now that we know how much of an org's VG is composed of fuel VGs, use FEER to get average MPG */
create or replace temp view feer_aggregated as
select
  org_id,
  object_id,
  sum(fuel_consumed_ml) as fuel_consumed_ml,
  sum(distance_traveled_m_gps) as distance_traveled_m_gps,
  sum(energy_consumed_kwh) as energy_consumed_kwh,
  sum(electric_distance_traveled_m_odo) as electric_distance_traveled_m_odo
from delta.`s3://samsara-report-staging-tables/report_aggregator/fuel_energy_efficiency_report/`
where date >= to_date('2020-01-01')
  and date <= to_date('2020-11-30')
  and org_id in (select org_id from org_fuel_energy_and_total_vgs where percent_fuel_or_energy_vgs >= 90) /* Threshold set to 90% fuel energy based to include */
group by org_id, object_id;

-- COMMAND ----------

create or replace temp view feer_stats as
select
  org_id,
  object_id,
  (distance_traveled_m_gps * 0.000621371)/(fuel_consumed_ml * 0.000264172) as mpg,
   case
      when fuel_consumed_ml > 0 and energy_consumed_kwh > 0
        then electric_distance_traveled_m_odo / distance_traveled_m_gps * 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m_odo * 0.0006213711922)) +
            (1 - electric_distance_traveled_m_odo / distance_traveled_m_gps) * (distance_traveled_m_gps - electric_distance_traveled_m_odo) * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
      when fuel_consumed_ml = 0 and energy_consumed_kwh > 0
        then 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m_odo * 0.0006213711922))
      when fuel_consumed_ml > 0 and energy_consumed_kwh = 0
        then distance_traveled_m_gps * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
      else 0
  end mpge
from feer_aggregated;

-- COMMAND ----------

create or replace temp view fuel_efficiency_stats as
select
  org_id,
  avg(mpge) as avg_mpg
from feer_stats
where mpge > 0 and
mpge <= 150 and
org_id not in (31577, 8936, 857, 46467, 28592, 962, 21651)
group by org_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # API Stats
-- MAGIC Total number of API calls and tokens used by orgs

-- COMMAND ----------

create or replace temporary view api_call_count_by_org as
select
    OrgId as org_id,
    count(1) as api_total_count
  from api_logs.api
  where date >= '2020-01-01' and
    date < '2020-11-12' and
    UrlPath is not null and
    OrgId in (select org_id from dataproducts.eligible_orgs)
  group by
    OrgId
  union all
  select
    org_id,
    count(1) as api_total_count
  from datastreams.api_logs
  where
    date >= '2020-11-12' and
    date <= '2020-11-30' and
    org_id in (select org_id from dataproducts.eligible_orgs)
  group by
    org_id;

create or replace temporary view api_call_count_by_org as
select
  org_id,
  sum(api_total_count) as api_total_count
from api_call_count_by_org
group by org_id;

-- COMMAND ----------

/* This query is deprecated and not used */
create or replace temporary view api_token_count_by_org as
select
  OrgId as org_id,
  count(distinct AccessTokenName) as api_token_count
from api_logs.api
where UrlPath is not null and
  OrgId in (select org_id from dataproducts.eligible_orgs) and
  date >= to_date("2020-01-01") and
  date <= to_date('2020-11-30')
group by OrgId;

-- COMMAND ----------

create or replace temp view api_call_token_count_by_org as
select
  t.org_id,
  api_token_count,
  api_total_count,
  percent_rank() over (order by api_total_count)*100 as api_total_count_percentile
from api_token_count_by_org as t
join api_call_count_by_org as c
  on t.org_id = c.org_id
where api_token_count > 0 and
  api_total_count >0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Join together

-- COMMAND ----------

create or replace temp view your_2020_samsara_data as
select
  a.org_id,
  a.total_trip_distance_miles,
  a.total_trip_duration_hours,
  a.total_num_trips,
  a.longest_trip_distance_miles,
  b.avg_mpg,
  c.address_1,
  c.address_2,
  c.address_3,
  c.address_4,
  c.address_5,
  d.num_events_total,
  d.num_events_coached,
  d.num_events_reviewed,
  e.successful_video_requests,
  e.successful_video_duration_minutes,
  f.report_1,
  f.report_2,
  f.report_3,
  f.report_1_count,
  f.report_2_count,
  f.report_3_count,
  g.alert_1,
  g.alert_2,
  g.alert_3,
  g.alert_1_count,
  g.alert_2_count,
  g.alert_3_count,
  g.total_alerts_configured,
  f.total_page_view_count,
  h.api_token_count,
  h.api_total_count,
  h.api_total_count_percentile,
  a.avg_miles_per_vehicle,
  f.report_4,
  f.report_5,
  e.total_num_video_uploads,
  e.total_video_uploads_duration_minutes
from total_miles_stats as a
left join fuel_efficiency_stats as b
  on a.org_id = b.org_id
left join tos_2020_org_top_5_by_org as c
  on a.org_id = c.org_id
left join safety_stats as d
  on a.org_id = d.org_id
left join video_upload_stats as e
  on a.org_id = e.org_id
left join top_reports_by_org as f
  on a.org_id = f.org_id
left join top_alerts_by_org as g
  on a.org_id = g.org_id
left join api_call_token_count_by_org as h
  on a.org_id = h.org_id;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC cache table your_2020_samsara_data;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from your_2020_samsara_data;

-- COMMAND ----------

-- Uncomment to save the table for analysis
drop table if exists dataproducts.your_2020_samsara_data;
create table if not exists dataproducts.your_2020_samsara_data as (
  select * from your_2020_samsara_data
)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dataproducts.your_2020_samsara_data where org_id = 9086;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Write to s3

-- COMMAND ----------

-- MAGIC %python
-- MAGIC s3_bucket = f"s3://samsara-year-review-metrics/year_review"
-- MAGIC
-- MAGIC query = "select * from dataproducts.your_2020_samsara_data"
-- MAGIC
-- MAGIC metrics_sdf = spark.sql(query)
-- MAGIC metrics_df = metrics_sdf.toPandas()
-- MAGIC metrics_df.head()
-- MAGIC
-- MAGIC metrics_sdf.coalesce(1).write.format("csv").mode('overwrite').option("header", True).save(f"{s3_bucket}/data_2020_prod")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Check Ranges

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import numpy as np

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('select * from dataproducts.your_2020_samsara_data').toPandas()
-- MAGIC df.describe()

-- COMMAND ----------

select
  org_id,
  total_trip_distance_miles
from dataproducts.your_2020_samsara_data
order by total_trip_distance_miles desc;

-- COMMAND ----------


