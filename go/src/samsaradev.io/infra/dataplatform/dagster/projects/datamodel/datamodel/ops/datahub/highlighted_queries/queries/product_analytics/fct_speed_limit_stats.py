queries = {
    "Find backend-computed speed limit stats for a specific org with source and matching method names": """
            SELECT
              date,
              org_id,
              device_id,
              way_id,
              time,
              backend_speed_limit_source,
              b.value as backend_speed_limit_source_name, -- speed limit source name, e.g. OSRM, CUSTOMER
              map_matching_method,
              m.value as map_matching_method_name, -- e.g. OSRM_MATCH, OSRM_NEAREST
              speed_limit_meters_per_second,
              speed_limit_meters_per_second * 2.237 as speed_limits_miles_per_hour,
              gps_speed_meters_per_second,
              ecu_speed_meters_per_second,
              latitude_degrees,
              longitude_degrees,
              heading_degrees,
              revgeo_state,
              revgeo_country
            FROM product_analytics.fct_speed_limit_stats s
            JOIN definitions.backend_speed_limit_source_type_enums b
              ON s.backend_speed_limit_source = b.enum
            JOIN definitions.map_matching_method_type_enums m
              ON s.map_matching_method = m.enum
            WHERE date = date_sub(current_date(), 1)
              AND org_id IN (32663)
              AND has_speed_limit
              AND location_speed_limit_source IS NULL -- return only speed limit stats from backend source
            """,
    "Find the top-5 most-traveled way_ids by org from the past week": """
          SELECT
            org_id,
            way_id,
            ANY_VALUE(speed_limit_meters_per_second) AS speed_limit_meters_per_second,
            COUNT(1) AS trip_count
          FROM
            product_analytics.fct_speed_limit_stats
          WHERE
            date BETWEEN date_sub(current_date(), 7) AND date_sub(current_date(), 1)
          GROUP BY
            1,
            2
          QUALIFY
            ROW_NUMBER() OVER (PARTITION BY org_id, way_id ORDER BY trip_count DESC) <= 5
          """,
}
