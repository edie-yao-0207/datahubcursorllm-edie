# Databricks notebook source
# MAGIC %run /backend/dataproducts/cityinsights/utils

# COMMAND ----------


def generate_harsh_event_density_dataset(
    city, state, tz, start_date=None, end_date=None
):
    city = city.lower()

    if start_date and end_date:
        print("Querying {} data for # of harsh events by road segment...".format(city))
        # This query gets all harsh accel, brake, and crash events for a certain city/state
        # over the 1/1/2020-6/30-2020 time range
        query = """
    create or replace temporary view safety_events_accel as
      select
        case
          when value.proto_value.accelerometer_event.harsh_accel_type = 1 then "HARSH_ACCEL"
          when value.proto_value.accelerometer_event.harsh_accel_type = 2 then "HARSH_BRAKE"
          when value.proto_value.accelerometer_event.harsh_accel_type = 5 then "CRASH"
        end as event_type,
        value.proto_value.accelerometer_event.last_gps.latitude/1e9 as lat,
        value.proto_value.accelerometer_event.last_gps.longitude/1e9 as lng,
        time as ts
      from kinesisstats.osdAccelerometer as a
      left join clouddb.organizations as b on
        a.org_id = b.id
      where date >= to_date('{}') and
      date <= to_date('{}') and
      value.proto_value.accelerometer_event.harsh_accel_type in (1, 2, 5) and
      value.proto_value.accelerometer_event.last_gps.speed >= 4344.8812095 and
      value.proto_value.accelerometer_event.hidden_to_customer != true and
      value.is_databreak = 'false' and
      value.is_end = 'false' and
      b.internal_type != 1 and
      a.org_id not in (29664, 9547, 47846, 18883, 23975, 18892, 25869, 658, 14481)
    """.format(
            start_date, end_date
        )

        sqlContext.sql(query)

        safety_events_sdf = spark.sql("select * from safety_events_accel")

        # Convert all harsh events into h3. Resolution 12 was found to be the best
        # hex size for road segments and is somewhere between city block size (res 10) and a coffee
        # table (res 15)
        RES = 12
        safety_events_sdf_h3 = safety_events_sdf.withColumn(
            "hex_id", lat_lng_to_h3_str_udf(col("lat"), col("lng"), lit(RES))
        )
        safety_events_sdf_h3 = safety_events_sdf_h3.withColumn(
            "h3_index", lat_lng_to_h3_int_udf(col("lat"), col("lng"), lit(RES))
        )
        safety_events_sdf_h3.createOrReplaceTempView("safety_events_h3")

        # Join our harsh event data with our road segment h3s on h3 index to get
        # the street for each harsh event data point
        query = """
    create or replace temporary view se_rs as
    select
      event_type,
      ts,
      first(street_id) as street_id,
      first(street_name) as street_name,
      first(is_interstate) as is_interstate,
      first(geometry) as geometry,
      a.h3_index,
      a.hex_id
    from safety_events_h3 as a
    inner join playground.road_segments_{}_h3 as b
      on a.h3_index = b.h3_index
    group by event_type, ts, a.h3_index, a.hex_id
    """.format(
            state
        )

        sqlContext.sql(query)

        # Augment data with potential cuts
        sers_sdf = spark.sql("select * from se_rs")
        sers_sdf = sers_sdf.withColumn(
            "is_post_covid", get_post_covid(col("ts"), lit(tz))
        )
        sers_sdf = sers_sdf.withColumn("day_of_week", get_day(col("ts"), lit(tz)))
        sers_sdf = sers_sdf.withColumn("month", get_month(col("ts"), lit(tz)))
        sers_sdf = sers_sdf.withColumn("rush_hour", is_rush_hour(col("ts"), lit(tz)))

        sers_sdf.createOrReplaceTempView("se_rs_pre_agg")

        # Aggregate number of harsh events by street and all cuts
        query = """
    create or replace temporary view se_rs_agg as
    select
      event_type,
      month,
      day_of_week,
      is_post_covid,
      rush_hour,
      count(1) as num_harsh_events,
      street_id,
      is_interstate,
      geometry
    from se_rs_pre_agg
    group by event_type, month, day_of_week, is_post_covid, rush_hour, street_id, is_interstate, geometry
    """

        sqlContext.sql(query)

        create_table = """
    create or replace table playground.harsh_event_road_segments_{} USING DELTA as
      select * from se_rs_agg
    """.format(
            city
        )
        sqlContext.sql(create_table)

        print("Done!")

        # free up memory
        safety_events_sdf.unpersist()
        safety_events_sdf_h3.unpersist()
        sers_sdf.unpersist()
        del safety_events_sdf
        del safety_events_sdf_h3
        del sers_sdf
        spark.catalog.clearCache()

        print(
            "Querying {} data for # of vehicles by road segment (this will take a while)...".format(
                city
            )
        )
        # This query gets all non-interal org location data points for a certain city/state
        query = """
    create or replace temporary view location_data as
    select
      date,
      org_id,
      device_id,
      time,
      value.latitude as lat,
      value.longitude as lng
    from kinesisstats.location as a
    left join clouddb.organizations as b on
      a.org_id = b.id
    where date >= to_date('{}') and
    date <= to_date('{}') and
    lower(value.revgeo_city) = "{}" and
    lower(value.revgeo_state) = "{}" and
    b.internal_type != 1 and
    org_id not in (29664, 9547, 47846, 18883, 23975, 18892, 25869, 658, 14481)
    """.format(
            start_date, end_date, city.replace("_", " "), state
        )

        sqlContext.sql(query)

        location_data_sdf = spark.sql("select * from location_data")

        # Convert all location speed data points into h3. Resolution 12 was found to be the best
        # hex size for road segments and is somewhere between city block size (res 10) and a coffee
        # table (res 15)
        RES = 12
        location_data_sdf_h3 = location_data_sdf.withColumn(
            "hex_id", lat_lng_to_h3_str_udf(col("lat"), col("lng"), lit(RES))
        )
        location_data_sdf_h3 = location_data_sdf_h3.withColumn(
            "h3_index", lat_lng_to_h3_int_udf(col("lat"), col("lng"), lit(RES))
        )
        location_data_sdf_h3.createOrReplaceTempView("location_data_h3")

        # Join our location data with our road segment h3s on h3 index to get
        # the street for each location speed data point.
        query = """
    create or replace temporary view ld_rs as
    select
      first(org_id) as org_id,
      first(device_id) as device_id,
      time,
      first(lat) as lat,
      first(lng) as lng,
      a.h3_index,
      a.hex_id,
      first(street_id) as street_id,
      first(street_name) as street_name,
      first(is_interstate) as is_interstate
    from location_data_h3 as a
    inner join playground.road_segments_{}_h3 as b
      on a.h3_index = b.h3_index
    group by time, a.h3_index, a.hex_id
    """.format(
            state
        )

        sqlContext.sql(query)

        ld_rs_sdf = spark.sql(
            "select org_id, device_id, time, street_id, street_name, is_interstate from ld_rs"
        )
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "rounded_hour_ts", snap_1_hour(col("time"), lit(tz))
        )
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "is_post_covid", get_post_covid(col("time"), lit(tz))
        )
        ld_rs_sdf = ld_rs_sdf.withColumn("day_of_week", get_day(col("time"), lit(tz)))
        ld_rs_sdf = ld_rs_sdf.withColumn("month", get_month(col("time"), lit(tz)))
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "rush_hour", is_rush_hour(col("time"), lit(tz))
        )
        ld_rs_sdf.createOrReplaceTempView("ld_rs_pre_agg")

        # Get a list of all streets with only one org in this time period
        query = """
    create or replace temp view num_orgs_per_street as
    select
      street_id,
      count(distinct org_id) as num_orgs
    from ld_rs_pre_agg
    group by street_id
    """
        sqlContext.sql(query)

        create_table = """
    create or replace table playground.streets_with_more_than_one_org_{} USING DELTA as
      select street_id
      from num_orgs_per_street
      where num_orgs > 3
    """.format(
            city
        )
        sqlContext.sql(create_table)

        # Aggregate distinct devices on the hour level granularity. This gives us the number of unique vehicles
        # by street for each hour
        query = """
    create or replace temporary view ld_rs_agg_by_date_hour as
    select
      rounded_hour_ts,
      month,
      day_of_week,
      is_post_covid,
      rush_hour,
      street_id,
      street_name,
      is_interstate,
      count(distinct device_id) as num_vehicles
    from ld_rs_pre_agg
    group by rounded_hour_ts, month, day_of_week, is_post_covid, rush_hour, street_id, street_name, is_interstate
    """

        sqlContext.sql(query)

        cache_table = """
    cache table ld_rs_agg_by_date_hour
    """

        sqlContext.sql(cache_table)

        # Now that we have number of distinct vehicles by hour, we can sum the number of vehicles
        # to aggregate on the month granularity by street
        query = """
    create or replace temporary view ld_rs_agg as
    select
      month,
      day_of_week,
      is_post_covid,
      rush_hour,
      street_id,
      street_name,
      is_interstate,
      sum(num_vehicles) as num_vehicles
    from ld_rs_agg_by_date_hour
    group by month, day_of_week, is_post_covid, rush_hour, street_id, street_name,is_interstate
    """

        sqlContext.sql(query)

        create_table = """
    create or replace table playground.num_vehicles_per_road_segment_{} USING DELTA as
      select *
      from ld_rs_agg
    """.format(
            city
        )
        sqlContext.sql(create_table)

        print("Done!")

    print("Generating Harsh Event Density datasets...")
    # Join together our harsh events per road segment data and # of vehicles per road segment data
    # to get harsh events/traffic density metric
    query = """
  create or replace temporary view normalized_harsh_events_dataset as
  select
    a.month,
    a.day_of_week,
    a.is_post_covid,
    a.rush_hour,
    b.event_type,
    a.street_id,
    a.street_name,
    a.is_interstate,
    b.geometry,
    b.num_harsh_events,
    a.num_vehicles,
    b.num_harsh_events/a.num_vehicles as normalized_harsh_events
  from playground.num_vehicles_per_road_segment_{} as a
  inner join playground.harsh_event_road_segments_{} as b
    on a.street_id = b.street_id
    and a.month = b.month
    and a.day_of_week = b.day_of_week
    and a.is_post_covid = b.is_post_covid
    and a.rush_hour = b.rush_hour
  """.format(
        city, city
    )

    sqlContext.sql(query)

    # Queries for harsh event density by road segments.

    # First we aggregate by street to get the normalized harsh events per street. We then
    # apply a p90 confidence interval to our normalized harsh events in order to account
    # for streets where we might not have enough data to confidently make conclusions.

    # The logic is as follows: if a street has less than 150 vehicles all time we use
    # the lower bound confidence interval value, else we use the mean of the lower and
    # upper bound which is just the original normalized harsh events value.

    # Finally, we apply a percent rank on the adjusted normalized harsh event values
    # to get values from 0 to 1 for Kepler to easily process them to quintiles.

    query = """
  create or replace temp view he_rs as
  select
    street_id,
    street_name,
    geometry,
    sum(num_harsh_events) as total_harsh_events,
    sum(num_vehicles) as total_num_vehicles,
    sum(num_harsh_events)/sum(num_vehicles) as normalized_harsh_events
  from normalized_harsh_events_dataset
  group by street_id,street_name,geometry
  """
    sqlContext.sql(query)

    query = """
  create or replace temp view he_rs_remove_one_org_streets as
  select
    a.street_id,
    street_name,
    geometry,
    total_harsh_events,
    total_num_vehicles,
    normalized_harsh_events
  from he_rs as a
  inner join playground.streets_with_more_than_one_org_{} as b
    on a.street_id = b.street_id
  """.format(
        city
    )
    sqlContext.sql(query)

    # Construct confidence intervals
    query = """
  create or replace temp view he_rs_with_confidence_intervals as
  select
    *,
    normalized_harsh_events + 1.64*sqrt((normalized_harsh_events*(1-normalized_harsh_events))/total_num_vehicles) as confidence_interval_high,
    normalized_harsh_events - 1.64*sqrt((normalized_harsh_events*(1-normalized_harsh_events))/total_num_vehicles) as confidence_interval_low
  from he_rs_remove_one_org_streets
  """
    sqlContext.sql(query)

    # Add heuristics for final dataset
    query = """
  create or replace temp view he_rs_adjusted as
  select
    street_id,
    street_name,
    geometry,
    case
      when total_num_vehicles < 150 then confidence_interval_low else (confidence_interval_high+confidence_interval_low)/2
    end as normalized_harsh_events_adjusted,
    total_harsh_events,
    total_num_vehicles,
    normalized_harsh_events as normalized_harsh_events_original,
    confidence_interval_high,
    confidence_interval_low
  from he_rs_with_confidence_intervals
  where total_num_vehicles >= 10
  """

    sqlContext.sql(query)

    # Apply percent rank
    query = """
  create or replace temp view he_rs_adjusted_percent_rank as
  select
    street_id,
    street_name,
    geometry,
    percent_rank() over (order by normalized_harsh_events_adjusted)*100 as normalized_harsh_events,
    total_harsh_events,
    total_num_vehicles,
    normalized_harsh_events_adjusted,
    normalized_harsh_events_original,
    confidence_interval_high,
    confidence_interval_low
  from he_rs_adjusted
  """

    sqlContext.sql(query)

    # Queries for harsh event density by neighborhood.

    # Get geometries for each neighborhood
    query = """
  create or replace temp view {}_neighborhood_id_geometry_map as
  select distinct neighborhood_id, geometry
  from playground.neighborhoods_{}_res10_h3
  """.format(
        city, city
    )

    sqlContext.sql(query)

    # Tag each road segment with which neighborhood it belongs to and aggregate
    # by neighborhood to calculate the avg harsh event density for that neighborhood
    query = """
  create or replace temp view {}_neighborhood_harsh_event_density as
  select
    n.neighborhood_id,
    n.neighborhood,
    m.geometry,
    avg(normalized_harsh_events_adjusted) as harsh_event_density,
    sum(total_num_vehicles) as total_num_vehicles
  from he_rs_adjusted as rs
  inner join playground.streetid_to_neighborhoodid_map_{} as n
    on rs.street_id = n.street_id
  inner join {}_neighborhood_id_geometry_map as m
    on n.neighborhood_id = m.neighborhood_id
  group by n.neighborhood_id, n.neighborhood, m.geometry
  """.format(
        city, city, city
    )

    sqlContext.sql(query)

    # Create final dataset tables in playground
    if city == "boston":
        print("~Boston specific data set~")
        create_table = """
    create or replace table playground.{}_nhe_rs USING DELTA as
    select
      street_name,
      geometry,
      normalized_harsh_events as harsh_event_density,
      linestring_to_gmap_url_udf(geometry) as street_view
    from he_rs_adjusted_percent_rank
    where street_id != 302988
    """.format(
            city
        )
        sqlContext.sql(create_table)
    else:
        create_table = """
    create table if not exists playground.{}_nhe_rs USING DELTA as
    select
      street_name,
      geometry,
      normalized_harsh_events as harsh_event_density,
      linestring_to_gmap_url_udf(geometry) as street_view
    from he_rs_adjusted_percent_rank
    """.format(
            city
        )
        sqlContext.sql(create_table)

    if city == "boston":
        # Filter out certain neighborhoods based on number of vehicles and specfic neighborhood ids
        query = """
    create or replace temp view boston_neighborhood_harsh_event_density_filter as
    select *
    from boston_neighborhood_harsh_event_density
    where neighborhood_id not in (52, 445, 1024, 228, 1023, 752, 1065, 280, 1016, 58, 220, 320, 713, 1193, 914, 21, 846, 1099, 1010, 27, 1022, 846, 900, 763, 760, 1005, 277, 528, 1002)
    and total_num_vehicles >= 2000
    """
        sqlContext.sql(query)
    else:
        # Filter out certain neighborhoods based on number of vehicles
        query = """
    create or replace temp view {}_neighborhood_harsh_event_density_filter as
    select *
    from {}_neighborhood_harsh_event_density
    where total_num_vehicles >= 100
    """.format(
            city, city
        )
        sqlContext.sql(query)

    create_table = """
    create or replace table playground.{}_nhe_n USING DELTA as
    select
      neighborhood,
      geometry,
      percent_rank() over (order by harsh_event_density)*100 as harsh_event_density
    from {}_neighborhood_harsh_event_density_filter
    """.format(
        city, city
    )
    sqlContext.sql(create_table)

    location_data_sdf.unpersist()
    location_data_sdf.unpersist()
    ld_rs_sdf.unpersist()
    del location_data_sdf
    del location_data_sdf_h3
    del ld_rs_sdf
    spark.catalog.clearCache()

    print("Done!")


# COMMAND ----------


def generate_speed_dataset(city, state, tz, start_date=None, end_date=None):
    city = city.lower()

    if start_date and end_date:
        print("Querying {} data for speed by road segment...".format(city))

        # This query gets all non-interal org location data points for a certain city/state
        # over the time range. We use the ECU speed when it exists, otherwise
        # we fallback to the gps speed.
        query = """
    create or replace temporary view location_data as
    select
      date,
      org_id,
      device_id,
      time,
      value.latitude as lat,
      value.longitude as lng,
      case
        when value.ecu_speed_meters_per_second is not null
          then value.ecu_speed_meters_per_second*2.23694
        when value.gps_speed_meters_per_second is not null
          then value.gps_speed_meters_per_second*2.23694
      end as speed_mph
    from kinesisstats.location as a
    left join clouddb.organizations as b on
        a.org_id = b.id
    where date >= to_date('{}') and
    date <= to_date('{}') and
    lower(value.revgeo_city) = "{}" and
    lower(value.revgeo_state) = "{}" and
    value.gps_speed_meters_per_second is not null and
    b.internal_type != 1 and
    org_id not in (29664, 9547, 47846, 18883, 23975, 18892, 25869, 658, 14481)
    """.format(
            start_date, end_date, city.replace("_", " "), state
        )

        sqlContext.sql(query)

        # Filter out speeds >= 5mph and <= 120mph
        location_data_sdf = spark.sql(
            "select * from location_data where speed_mph>=5 and speed_mph<=120"
        )

        # Convert all location speed data points into h3. Resolution 12 was found to be the best
        # hex size for road segments and is somewhere between city block size (res 10) and a coffee
        # table (res 15)
        RES = 12
        location_data_sdf_h3 = location_data_sdf.withColumn(
            "hex_id", lat_lng_to_h3_str_udf(col("lat"), col("lng"), lit(RES))
        )
        location_data_sdf_h3 = location_data_sdf_h3.withColumn(
            "h3_index", lat_lng_to_h3_int_udf(col("lat"), col("lng"), lit(RES))
        )
        location_data_sdf_h3.createOrReplaceTempView("location_data_h3")

        # Join our location data with our road segment h3s on h3 index to get
        # the street for each location speed data point. We also round
        # each speed to the nearest 5mph.
        query = """
    create or replace temporary view ld_rs as
    select
      first(org_id) as org_id,
      first(device_id) as device_id,
      time,
      first(lat) as lat,
      first(lng) as lng,
      first(speed_mph) as speed_mph,
      round(first(speed_mph)/5,0)*5 as rounded_speed,
      a.h3_index,
      a.hex_id,
      first(street_id) as street_id,
      first(street_name) as street_name,
      first(is_interstate) as is_interstate,
      first(geometry) as geometry
    from location_data_h3 as a
    inner join playground.road_segments_{}_h3 as b
      on a.h3_index = b.h3_index
    group by time, a.h3_index, a.hex_id
    """.format(
            state
        )

        sqlContext.sql(query)

        # Augment data with columns for potential aggregation on different cuts
        ld_rs_sdf = spark.sql(
            "select org_id,device_id, time, street_id, street_name, is_interstate, geometry,speed_mph,rounded_speed from ld_rs"
        )
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "rounded_hour_ts", snap_1_hour(col("time"), lit(tz))
        )
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "is_post_covid", get_post_covid(col("time"), lit(tz))
        )
        ld_rs_sdf = ld_rs_sdf.withColumn("day_of_week", get_day(col("time"), lit(tz)))
        ld_rs_sdf = ld_rs_sdf.withColumn("month", get_month(col("time"), lit(tz)))
        ld_rs_sdf = ld_rs_sdf.withColumn(
            "rush_hour", is_rush_hour(col("time"), lit(tz))
        )
        ld_rs_sdf.createOrReplaceTempView("ld_rs_pre_agg")

        # Get a list of all streets with only one org in this time period
        query = """
    create or replace temp view num_orgs_per_street as
    select
      street_id,
      count(distinct org_id) as num_orgs
    from ld_rs_pre_agg
    group by street_id
    """
        sqlContext.sql(query)

        create_table = """
    create or replace table playground.streets_with_more_than_one_org_{} USING DELTA as
      select street_id
      from num_orgs_per_street
      where num_orgs > 3
    """.format(
            city
        )
        sqlContext.sql(create_table)

        # Aggregate data by road segment and rounded speed.
        # Count frequency of each rounded speed which will be used in our mode calculation.
        query = """
    create or replace temporary view ld_rs_agg as
    select
      street_id,
      street_name,
      geometry,
      rounded_speed,
      count(rounded_speed) as rounded_speed_freq,
      count(speed_mph) as num_speed_points,
      sum(speed_mph) as sum_speed,
      avg(speed_mph) as avg_speed
    from ld_rs_pre_agg
    group by street_id, street_name, geometry,rounded_speed
    """
        sqlContext.sql(query)

        cache_table = """
    cache table ld_rs_agg
    """

        sqlContext.sql(cache_table)

        create_table = """
    create or replace table playground.mode_speed_per_road_segment_{} USING DELTA as
      select *
      from ld_rs_agg
    """.format(
            city
        )
        sqlContext.sql(create_table)

        print("Done!")

    print("Generating Speed datasets...")

    # These queries calculate the most frequent rounded speed (mode) to represent the speed
    # of a road segment. It also buckets them into 10 mph increments so Kepler is able to
    # automatically quantize them and display the correct labels. We also filter out streets
    # with only one org represented at the beginning.
    # NOTE: This requires you to run through the commands in the harsh event density notebook first!
    query = """
  create or replace temp view speed_freq_sorted as
  select
    a.street_id,
    street_name,
    geometry,
    rounded_speed,
    rounded_speed_freq,
    row_number() over (partition by a.street_id order by rounded_speed_freq desc) as seq_num
  from playground.mode_speed_per_road_segment_{} as a
  order by a.street_id,rounded_speed
  """.format(
        city, city
    )

    sqlContext.sql(query)

    # Pick mode for speed
    query = """
  create or replace temp view speed_mode as
  select
    street_id,
    street_name,
    geometry,
    rounded_speed as mode_speed_mph,
    rounded_speed_freq
  from speed_freq_sorted
  where seq_num=1
  """

    sqlContext.sql(query)

    # Bucket speed into 10 mph buckets to make it easier for Kepler quantizing
    query = """
  create or replace temp view speed_mode_buckets as
  select
    street_name,
    geometry,
    case
      when mode_speed_mph <10 then 0
      when mode_speed_mph>=10 and mode_speed_mph<20 then 11
      when mode_speed_mph>=20 and mode_speed_mph<30 then 21
      when mode_speed_mph>=30 and mode_speed_mph<40 then 31
      when mode_speed_mph>=40 and mode_speed_mph<50 then 41
      when mode_speed_mph>=50 then 60
    end as speed
  from speed_mode
  where rounded_speed_freq >= 10
  """

    sqlContext.sql(query)

    # Queries for speed by neighborhood.

    # Get geometries for each neighborhood
    query = """
  create or replace temp view {}_neighborhood_id_geometry_map as
  select distinct neighborhood_id, geometry
  from playground.neighborhoods_{}_res10_h3
  """.format(
        city, city
    )

    sqlContext.sql(query)

    # Tag each road segment with which neighborhood it belongs to and aggregate
    # by neighborhood to calculate the avg harsh event density for that neighborhood
    query = """
  create or replace temp view {}_neighborhood_speed as
  select
    n.neighborhood_id,
    n.neighborhood,
    m.geometry,
    avg(mode_speed_mph) as speed,
    sum(rounded_speed_freq) as total_points,
    count(distinct s.street_id) as num_streets
  from speed_mode as s
  inner join playground.streetid_to_neighborhoodid_map_{} as n
    on s.street_id = n.street_id
  inner join {}_neighborhood_id_geometry_map as m
    on n.neighborhood_id = m.neighborhood_id
  group by n.neighborhood_id, n.neighborhood, m.geometry
  """.format(
        city, city, city
    )

    sqlContext.sql(query)

    query = """
  create or replace temp view {}_neighborhood_speed_buckets as
  select
    neighborhood_id,
    neighborhood,
    geometry,
    case
      when speed <10 then 0
      when speed>=10 and speed<20 then 11
      when speed>=20 and speed<30 then 21
      when speed>=30 and speed<40 then 31
      when speed>=40 and speed<50 then 41
      when speed>=50 then 60
    end as speed,
    total_points,
    num_streets
  from {}_neighborhood_speed
  """.format(
        city, city
    )

    sqlContext.sql(query)

    # Create final dataset tables in playground

    create_table = """
  create or replace table playground.{}_speed_rs USING DELTA as
  select *, linestring_to_gmap_url_udf(geometry) as street_view
  from speed_mode_buckets
  """.format(
        city
    )
    sqlContext.sql(create_table)

    if city == "boston":
        query = """
    create or replace temp view {}_neighborhood_speed_buckets as
    select *
    from {}_neighborhood_speed_buckets
    where neighborhood_id not in (52, 445, 1024, 228, 1023, 752, 1065, 280, 1016, 58, 220, 320, 713, 1193, 914, 21, 846, 1002)
    and total_points>=300000
    """.format(
            city, city
        )
        sqlContext.sql(query)

    create_table = """
  create or replace table playground.{}_speed_n USING DELTA as
  select
    neighborhood,
    geometry,
    speed
  from {}_neighborhood_speed_buckets
  union
    select "" as neighborhood, null as geometry, 60 as speed
  union
    select "" as neighborhood, null as geometry, 0 as speed
  """.format(
        city, city
    )
    sqlContext.sql(create_table)

    location_data_sdf.unpersist()
    location_data_sdf_h3.unpersist()
    ld_rs_sdf.unpersist()
    del location_data_sdf
    del location_data_sdf_h3
    del ld_rs_sdf
    spark.catalog.clearCache()

    print("Done!")


# COMMAND ----------

# rs_ma_h3_only_sdf.coalesce(1).write.format("csv").option("header", True).save("s3://samsara-databricks-playground/joanne/roads_ma_interp_line_h3_res12_3.csv")

# COMMAND ----------

# test.coalesce(1).write.format("csv").option("header", True).save("s3://samsara-databricks-playground/alvin/test.csv")
