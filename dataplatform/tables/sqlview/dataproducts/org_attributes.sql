select
  fdp.org_id,
   -- total_devices_trips is the number of unique vehciles that were active in the queried timeframe
   -- org 1: 5 veh drive 5 mi for 30 days, org 2: 5 veh drive 5 miles for 1 day
  (total_trip_distance_miles / unique_active_all_vehicles)/60    as distance_driven_miles_per_vehicle,
  total_trip_distance_miles / num_trips                          as trip_length,

  unique_active_all_vehicles                                     as unique_active_vehicles,
  unique_active_passenger_vehicles/unique_active_all_vehicles    as percent_passenger,
  percent_trips_start_end_city                                   as percent_trips_city
from dataproducts.fleet_driving_patterns as fdp
left join dataproducts.org_active_vgs as navo
  on fdp.org_id = navo.org_id
left join dataproducts.org_trip_city_count as otcc
  on fdp.org_id = otcc.org_id
