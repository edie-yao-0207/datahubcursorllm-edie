WITH primary_jurisdiction_mappings AS (
  VALUES
    ('AB', 'Alberta'),
    ('AGU', 'Aguascalientes'),
    ('AK', 'Alaska'),
    ('AL', 'Alabama'),
    ('AR', 'Arkansas'),
    ('AS', 'American Samoa'),
    ('AZ', 'Arizona'),
    ('BC', 'British Columbia'),
    ('BCN', 'Baja California'),
    ('BCS', 'Baja California Sur'),
    ('CA', 'California'),
    ('CAM', 'Campeche'),
    ('CHH', 'Chihuahua'),
    ('CHP', 'Chiapas'),
    ('CMX', 'Ciudad de México'),
    ('CO', 'Colorado'),
    ('COA', 'Coahuila de Zaragoza'),
    ('COL', 'Colima'),
    ('CT', 'Connecticut'),
    ('DC', 'District of Columbia'),
    ('DE', 'Delaware'),
    ('DUR', 'Durango'),
    ('FL', 'Florida'),
    ('GA', 'Georgia'),
    ('GRO', 'Guerrero'),
    ('GU', 'Guam'),
    ('GUA', 'Guanajuato'),
    ('HI', 'Hawaii'),
    ('HID', 'Hidalgo'),
    ('IA', 'Iowa'),
    ('ID', 'Idaho'),
    ('IL', 'Illinois'),
    ('IN', 'Indiana'),
    ('JAL', 'Jalisco'),
    ('KS', 'Kansas'),
    ('KY', 'Kentucky'),
    ('LA', 'Louisiana'),
    ('MA', 'Massachusetts'),
    ('MB', 'Manitoba'),
    ('MD', 'Maryland'),
    ('ME', 'Maine'),
    ('MEX', 'México'),
    ('MI', 'Michigan'),
    ('MIC', 'Michoacán de Ocampo'),
    ('MN', 'Minnesota'),
    ('MO', 'Missouri'),
    ('MOR', 'Morelos'),
    ('MP', 'Northern Mariana Islands '),
    ('MS', 'Mississippi'),
    ('MT', 'Montana'),
    ('NAY', 'Nayarit'),
    ('NB', 'New Brunswick'),
    ('NC', 'North Carolina'),
    ('ND', 'North Dakota'),
    ('NE', 'Nebraska'),
    ('NH', 'New Hampshire'),
    ('NJ', 'New Jersey'),
    ('NL', 'New Foundland and Labrador'),
    ('NLE', 'Nuevo León'),
    ('NM', 'New Mexico'),
    ('NS', 'Nova Scotia'),
    ('NT', 'Northwest Territories'),
    ('NU', 'Nunavut'),
    ('NV', 'Nevada'),
    ('NY', 'New York'),
    ('OAX', 'Oaxaca'),
    ('OH', 'Ohio'),
    ('OK', 'Oklahoma'),
    ('ON', 'Ontario'),
    ('OR', 'Oregon'),
    ('PA', 'Pennsylvania'),
    ('PE', 'Prince Edward Island'),
    ('PR', 'Puerto Rico'),
    ('PUE', 'Puebla'),
    ('QC', 'Quebec'),
    ('QUE', 'Querétaro'),
    ('RI', 'Rhode Island'),
    ('ROO', 'Quintana Roo'),
    ('SC', 'South Carolina'),
    ('SD', 'South Dakota'),
    ('SIN', 'Sinaloa'),
    ('SK', 'Saskatchewan'),
    ('SLP', 'San Luis Potosí'),
    ('SON', 'Sonora'),
    ('TAB', 'Tabasco'),
    ('TAM', 'Tamaulipas'),
    ('TLA', 'Tlaxcala'),
    ('TN', 'Tennessee'),
    ('TX', 'Texas'),
    ('UM', 'United States Minor Outlying Islands'),
    ('UT', 'Utah'),
    ('VA', 'Virginia'),
    ('VER', 'Veracruz de Ignacio de la Llave'),
    ('VI', 'U.S. Virgin Islands'),
    ('VI', 'United States Virgin Islands'),
    ('VT', 'Vermont'),
    ('WA', 'Washington'),
    ('WI', 'Wisconsin'),
    ('WV', 'West Virginia'),
    ('WY', 'Wyoming'),
    ('YT', 'Yukon'),
    ('YUC', 'Yucatán'),
    ('ZAC', 'Zacatecas')
  AS (code, name)
),

-- Other anomalies that may occur in location points revgeo.
additional_jurisdiction_mappings AS (
  VALUES
    -- Alaska
    ('AK', 'Anchorage'),
    ('AK', 'Unorganized Borough'),
    ('AK', 'Matanuska-Susitna'),
    ('AK', 'North Slope'),
    ('AK', 'Kenai Peninsula'),
    ('AK', 'Fairbanks North Star'),
    ('AK', 'Denali'),
    ('AK', 'Lake and Peninsula'),
    ('AK', 'Northwest Arctic'),
    ('AK', 'Ketchikan Gateway'),
    ('AK', 'Yakutat'),
    ('AK', 'Petersburg Borough'),
    ('AK', 'Kodiak Island'),
    ('AK', 'Sitka'),
    ('AK', 'Wrangell'),
    ('AK', 'Juneau'),
    ('AK', 'Aleutians East'),
    ('AK', 'Haines'),
    ('AK', 'Skagway'),
    ('AK', 'Bristol Bay'),
    ('AK', 'Bethel'),
    ('AK', 'Nome'),
    ('AK', 'Kusilvak Census Area'),
    ('AK', 'Dillingham'),

    -- Ontario
    ('ON', 'Southwestern Ontario'),
    ('ON', 'Northeastern Ontario'),
    ('ON', 'Eastern Ontario'),
    ('ON', 'Central Ontario'),
    ('ON', 'Peel Region'),
    ('ON', 'Toronto'),
    ('ON', 'Halton Region'),
    ('ON', 'York Region'),
    ('ON', 'Durham Region'),
    ('ON', 'Northwestern Ontario'),
    ('ON', 'Hamilton'),

    -- Mexican
    ('YUC', 'Progreso'), -- city YUC
    ('ROO', 'Othón P. Blanco'), -- municipality ROO
    ('ROO', 'Isla Mujeres'), -- island ROO
    ('ROO', 'Felipe Carrillo Puerto'), -- municipality ROO
    ('CAM', 'Calkiní'), -- city CAM
    ('YUC', 'Celestún'), -- town YUC
    ('OAX', 'San Mateo del Mar'), -- municipality OAX
    ('JAL', 'La Huerta'), -- town & municipality JAL
    ('NAY', 'María Madre'), -- island NAY
    ('CAM', 'Hecelchakán'), -- city CAM
    ('NAY', 'Santiago Ixcuintla'), -- municipality NAY
    ('NAY', 'María Magdalena'), -- island NAY
    ('VER', 'Tecolutla'), -- town VER
    ('OAX', 'Santa María Huatulco'), -- town OAX
    ('VER', 'Tamiahua'), -- municipality VER
    ('SIN', 'Mazatlán'), -- city SIN
    ('BCS', 'Gaviota Island'), -- island BCS
    ('NAY', 'Compostela'), -- municipality NAY
    ('VER', 'Veracruz'), -- Isla de Sacrificios, VER
    ('ROO', 'Cozumel'), -- island ROO
    ('BCS', 'Isla San Damia'), -- island BCS
    ('JAL', 'Puerto Vallarta'), -- city JAL
    ('OAX', 'Santiago Astata'), -- town OAX
    ('JAL', 'Puerto Vallarta'), -- city JAL
    ('OAX', 'Santiago Astata'), -- town OAX
    ('VER', 'Papantla'), -- city VER
    ('OAX', 'Villa de Tututepec de Melchor Ocampo'), -- town OAX
    ('COA', 'Coahuila'), -- Local variant of Coahuila de Zaragoza
    ('MIC', 'Michoacán'), -- Local variant of Michoacán de Ocampo
    ('VER', 'Veracruz'), -- Local variant of Veracruz de Ignacio de la Llave

    -- Indian Reservations
    ('MI', 'Grand Traverse Reservation'), -- MI, US
    ('MI', 'Little Traverse Bay Bands of Odawas Reservation'), -- MI, US
    ('NB', 'Devon Indian Reserve NO. 30'), -- NB, CA
    ('WA', 'Puyallup Tribe'), -- WA, US
    ('MI', 'Little Traverse Bay Band of Odawa Reservation'), -- MI, US
    ('MI', 'Little Traverse Bay Band of Odawas Reservation'), -- MI, US
    ('MI', 'Little Traverse Bay Bands Of Odawa Reservation'),  -- MI, US
    ('MI', 'Little Traverse Bay Band of Odawas'), -- MI, US
    ('WA', 'Puyallup Tribe Reservation'), -- WA, US
    ('NB', 'Woodstock First Nation'), -- NB, CA

    -- Misc
    ('AK', 'Walrus Islands State Game Sanctuary'), -- Bristol Bay, AK
    ('NL', 'Moose Management Area 2'), -- Portland Creek, NL
    ('PE', 'Prince County'), -- Off coast of PE
    ('NY', 'Liberty Island'), -- NY
    ('PE', 'PEI') -- Prince Edward Island (PE) = PEI
  AS (code, name)
),

jurisdiction_mappings AS (
  SELECT * FROM primary_jurisdiction_mappings
  UNION
  SELECT * FROM additional_jurisdiction_mappings
),

operating_authority_orgs AS (
  SELECT org_id FROM VALUES
  64368 -- UniGroup prod
  ,80820 -- UniGroup non-prod
  ,73963 -- Sirva
  ,11000966 -- Atlas World Group
  ,51582 -- Golden Van Lines, Inc
  ,81768 -- Ace Worldwide Moving & Storage
  ,23679 -- Paxton Van Lines, Inc
  ,3846 -- Internal test org
  ,4000170
  AS (org_id)
),

operating_authority_updates AS (
  SELECT
    *,
    UNIX_TIMESTAMP(log_at) * 1000 AS start_ms,
    UNIX_TIMESTAMP(next_log_at) * 1000 AS end_ms
  FROM (
    SELECT
      org_id,
      operating_authority,
      driver_id,
      log_at,
      COALESCE(LEAD(log_at) OVER(w), TIMESTAMP(${end_date})) AS next_log_at
    FROM ifta_report.operating_authority_updates
    WHERE date < ${end_date}
      AND org_id IN (
        SELECT org_id from operating_authority_orgs
       )
    WINDOW w AS (PARTITION BY org_id, driver_id ORDER BY log_at)
  )
),

vehicles AS (
  SELECT
    org_id,
    id AS device_id
  FROM productsdb.devices AS devices
    JOIN definitions.products AS products
      ON devices.product_id = products.product_id
      AND (products.name LIKE '%VG%') OR (products.name = 'OEMV')
),

valid_locations_without_road_context AS (
  SELECT
    loc.date,
    loc.org_id,
    loc.device_id,
    loc.time,
    STRUCT(
      value.time AS time,
      value.has_fix AS has_fix,
      value.latitude AS latitude,
      value.longitude AS longitude,
      value.gps_speed_meters_per_second AS gps_speed_meters_per_second,
      value.heading_degrees AS heading_degrees,
      value.has_revgeo AS has_revgeo,
      value.revgeo_city AS revgeo_city,
      value.revgeo_state AS revgeo_state,
      value.revgeo_country AS revgeo_country,
      value.has_speed_limit AS has_speed_limit,
      value.speed_limit_meters_per_second AS speed_limit_meters_per_second,
      value.has_toll AS has_toll,
      value.toll AS toll,
      false AS private_road,
      false AS off_highway
    ) AS value
  FROM kinesisstats.location as loc
  WHERE loc.date >= DATE_SUB(${start_date}, 1)
    AND loc.date < ${end_date}
    AND loc.date < TO_DATE("2025-12-15")
    AND value.revgeo_state IS NOT NULL
    AND value.has_fix
    AND loc.org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    AND loc.time >= unix_timestamp(DATE_SUB(${start_date}, 1)) * 1000
    AND loc.time < unix_timestamp(to_timestamp(${end_date})) * 1000
),

valid_locations_with_road_context AS (
  SELECT
    loc.date,
    loc.org_id,
    loc.object_id as device_id,
    loc.time,
    STRUCT(
      value.time AS time,
      value.proto_value.location_with_road_context.has_fix AS has_fix,
      value.proto_value.location_with_road_context.latitude AS latitude,
      value.proto_value.location_with_road_context.longitude AS longitude,
      value.proto_value.location_with_road_context.gps_speed_meters_per_second AS gps_speed_meters_per_second,
      value.proto_value.location_with_road_context.heading_degrees AS heading_degrees,
      value.proto_value.location_with_road_context.has_revgeo AS has_revgeo,
      value.proto_value.location_with_road_context.revgeo_city AS revgeo_city,
      value.proto_value.location_with_road_context.revgeo_state AS revgeo_state,
      value.proto_value.location_with_road_context.revgeo_country AS revgeo_country,
      value.proto_value.location_with_road_context.has_speed_limit AS has_speed_limit,
      value.proto_value.location_with_road_context.speed_limit_meters_per_second AS speed_limit_meters_per_second,
      value.proto_value.location_with_road_context.has_toll AS has_toll,
      value.proto_value.location_with_road_context.toll AS toll,
      COALESCE(ARRAY_CONTAINS(value.proto_value.location_with_road_context.ifta_geofence_exemption_types, 1), false) AS private_road, -- COALESCE is used because if the array is null then private_road should be false.
      CASE
        WHEN ARRAY_CONTAINS(value.proto_value.location_with_road_context.ifta_geofence_exemption_types, 1) THEN false
        ELSE COALESCE(ARRAY_CONTAINS(value.proto_value.location_with_road_context.ifta_geofence_exemption_types, 2), false) -- COALESCE is used because if the array is null then off_highway should be false.
      END AS off_highway -- Private road takes precedence over off-highway to prevent double-counting when both exemption types are present.
    ) AS value
  FROM kinesisstats.osdlocationwithroadcontext as loc
  WHERE loc.date >= DATE_SUB(${start_date}, 1)
    AND loc.date < ${end_date}
    AND loc.date >= TO_DATE("2025-12-15")  -- On/after cutover
    AND value.proto_value.location_with_road_context.revgeo_state IS NOT NULL
    AND value.proto_value.location_with_road_context.has_fix
    AND loc.org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    AND loc.time >= unix_timestamp(DATE_SUB(${start_date}, 1)) * 1000
    AND loc.time < unix_timestamp(to_timestamp(${end_date})) * 1000
    -- Keep rows where hdop is unknown OR good
    AND (value.proto_value.location_with_road_context.hdop IS NULL
      OR value.proto_value.location_with_road_context.hdop <= 3.0)
    -- Keep rows where accuracy is unknown OR accuracy is good
    AND (value.proto_value.location_with_road_context.accuracy_invalid = TRUE
      OR value.proto_value.location_with_road_context.accuracy_millimeters IS NULL
      OR value.proto_value.location_with_road_context.accuracy_millimeters <= 30000)
),

-- COMBINED
-- Migration from kinesisstats.location to kinesisstats.osdlocationwithroadcontext
-- Cutover Date: 2025-12-15
-- Old Behavior (before 2025-12-15):
--   - Source: kinesisstats.location table
--   - Schema: Direct access to location fields (e.g., value.latitude, value.toll)
--   - Handled by: valid_locations_without_road_context CTE
-- New Behavior (on/after 2025-12-15):
--   - Source: kinesisstats.osdlocationwithroadcontext objectstats table
--   - Schema: Nested access via value.proto_value.location_with_road_context.*
--   - Device ID mapping: object_id → device_id
--   - Handled by: valid_locations_with_road_context CTE
-- Both CTEs produce identical 18-field STRUCTs to enable UNION ALL:
--   {time, has_fix, latitude, longitude, gps_speed_meters_per_second, heading_degrees,
--    has_revgeo, revgeo_city, revgeo_state, revgeo_country, has_speed_limit,
--    speed_limit_meters_per_second, has_toll, toll, private_road, off_highway}
valid_locations AS (
  SELECT * FROM valid_locations_without_road_context
  UNION ALL
  SELECT * FROM valid_locations_with_road_context
),

vehicle_locations AS (
  SELECT
    loc.date,
    loc.org_id,
    loc.device_id,
    loc.time,
    loc.value
  FROM valid_locations as loc
    -- Filter down to just vehicles
  JOIN vehicles
    ON loc.org_id = vehicles.org_id
    AND loc.device_id = vehicles.device_id
),

filtered_driver_assignments AS (
  SELECT * FROM fuel_energy_efficiency_report.driver_assignments
    WHERE org_id IN (SELECT org_id FROM operating_authority_orgs)
    AND date >= DATE_SUB(${start_date}, 1)
    AND date < ${end_date}
),

locations_with_driver_assigned AS (
  -- Add Range Join optimization hint. Bin size was computed as the 90th percentile of difference between start_time and end_time.
  SELECT /*+ RANGE_JOIN(loc, 34111054) */
    loc.date,
    loc.org_id,
    loc.device_id,
    loc.time,
    loc.value,
    MAX(driver_assignments.driver_id) as driver_id
  FROM vehicle_locations as loc
  LEFT JOIN filtered_driver_assignments as driver_assignments
  -- fuel_energy_efficiency_report.driver_assignments explicitly splits driver assignments that overlap days into multiple driver assignments, each bound to the time window of a day.
    ON loc.date = driver_assignments.date
    AND loc.org_id = driver_assignments.org_id
    AND loc.device_id = driver_assignments.device_id
    AND loc.time >= driver_assignments.start_time AND loc.time < driver_assignments.end_time
  GROUP BY loc.date, loc.org_id, loc.device_id, loc.time, loc.value
),

-- Associate location points with a sanitized jurisdiction code ("Missouri" -> "MO").
-- Assign the known operating authority and driver at each location point's time.
-- Add Range Join optimization hint. Bin size was computed as the 90th percentile of difference between start_ms and end_ms
-- from joining operating_authority_updates entity (see https://docs.databricks.com/delta/join-performance/range-join.html#choose-the-bin-size).
decorated_location_points AS (
  SELECT
    /*+ RANGE_JOIN(loc, 19650741000) */
    loc.date,
    loc.org_id,
    loc.device_id,
    loc.time,
    STRUCT(
      value.time,
      value.has_fix,
      value.latitude,
      value.longitude,
      value.gps_speed_meters_per_second,
      value.heading_degrees,
      value.has_revgeo,
      -- Sometimes both the city and state appear in the city field.
      -- For example, "Hockessin, Delaware" is provided as the name of a city.
      -- Fix this by keeping only the city name if what's after the comma
      -- is the name associated with what's in the state field.
      IF(LENGTH(
        REGEXP_EXTRACT(value.revgeo_city,
          CONCAT("(.*), ", jurisdiction_with_matching_code.name),
          1)
      ) > 0,
      REGEXP_EXTRACT(value.revgeo_city,
        CONCAT("(.*), ", jurisdiction_with_matching_code.name),
        1),
      loc.value.revgeo_city) AS revgeo_city,
      value.revgeo_state,
      value.revgeo_country,
      value.has_speed_limit,
      value.speed_limit_meters_per_second,
      value.has_toll,
      value.toll,
      value.off_highway,
      value.private_road,
      COALESCE(jurisdiction_with_matching_name.code, value.revgeo_state) AS jurisdiction,
      CASE
      -- IN/EXISTS predicate sub-queries can only be used in Filter/Join, so manually enumerate them here.
      -- This should match operating_authority_orgs.
        WHEN loc.org_id NOT IN
          (64368 -- UniGroup prod
          ,80820 -- UniGroup non-prod
          ,73963 -- Sirva
          ,11000966 -- Atlas World Group
          ,51582 -- Golden Van Lines, Inc
          ,81768 -- Ace Worldwide Moving & Storage
          ,23679 -- Paxton Van Lines, Inc
          ,3846 -- Internal test org
          ,4000170)
        THEN NULL
        WHEN COALESCE(oau.operating_authority, CAST(groups.carrier_us_dot_number AS INT)) = 0 THEN NULL
        ELSE COALESCE(oau.operating_authority, CAST(groups.carrier_us_dot_number AS INT))
      END AS operating_authority,
      loc.driver_id
    ) AS value,
  -- Put each location point into eighth hour (7.5 minute) buckets
  WINDOW(FROM_UNIXTIME(loc.time / 1000), "450 second").start AS eighth_hour
  FROM locations_with_driver_assigned as loc
  -- Map jurisdiction names to codes since sometimes the name is provided.
  LEFT JOIN operating_authority_updates oau
    -- don't join on date here since HOS log updates are sparse and not propagated for each
    -- date.
    ON loc.org_id = oau.org_id
    AND loc.driver_id = oau.driver_id
    AND loc.time >= oau.start_ms AND loc.time < oau.end_ms
  JOIN clouddb.groups as groups
    ON loc.org_id = groups.organization_id
  LEFT JOIN jurisdiction_mappings jurisdiction_with_matching_name
    ON loc.value.revgeo_state = jurisdiction_with_matching_name.name
  -- Also join with matching codes to solve a specific issue (see above).
  -- Don't use the "additional" mappings to avoid duplicates that could result,
  -- since it maps additional names to existing codes.
  LEFT JOIN primary_jurisdiction_mappings jurisdiction_with_matching_code
    ON loc.value.revgeo_state = jurisdiction_with_matching_code.code
),

-- Filter out all location points that don't
-- update jurisdiction or toll status
location_points_with_updates AS (
  SELECT *
  FROM (
    -- This subquery is a workaround for the fact that
    -- you can't use WINDOW in a WHERE clause.
    SELECT
      *,
      -- Filter out points that don't have a change in jurisdiction, toll, operating authority, or driver.
      value.jurisdiction <> LAG(value.jurisdiction) OVER (w)
      OR COALESCE(value.toll, false) <> COALESCE(LAG(value.toll) OVER (w), false)
      OR COALESCE(value.off_highway, false) <> COALESCE(LAG(value.off_highway) OVER (w), false)
      OR COALESCE(value.private_road, false) <> COALESCE(LAG(value.private_road) OVER (w), false)
      OR COALESCE(value.operating_authority, 0) <> COALESCE(LAG(value.operating_authority) OVER(w), 0)
      OR COALESCE(value.driver_id, 0) <> COALESCE(LAG(value.driver_id) OVER(w), 0)
        AS keep_point
    FROM decorated_location_points
    WINDOW w AS (PARTITION BY org_id, device_id ORDER BY time ASC)
  )
  WHERE keep_point
),

-- Get the first point for each eighth hour bucket to ensure a gap
-- of no more than 15 minutes if at all possible
first_eighth_hour_location_points AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(first_point_window) as first_point_rowid
    FROM decorated_location_points
    WINDOW first_point_window AS (PARTITION BY org_id, device_id, eighth_hour ORDER BY time ASC)
  )
  WHERE first_point_rowid = 1
),

-- Keep the last point for each device so the
-- end of the report can be as accurate as possible.
last_location_points AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(last_point_window) as last_point_rowid
    FROM decorated_location_points
    WINDOW last_point_window AS (PARTITION BY org_id, device_id ORDER BY time DESC)
  )
  WHERE last_point_rowid = 1
),

-- Copy the last point for each device
-- to the end the report period.
-- This way the whole report period
-- is covered by a segment.
end_location_points AS (
  SELECT
    CAST(UNIX_TIMESTAMP(CAST(${end_date} AS TIMESTAMP)) * 1000 AS BIGINT)
      AS time,
    value,
    date,
    device_id,
    org_id
  FROM last_location_points
),

filtered_location_points AS (
  SELECT time, value, date, device_id, org_id FROM location_points_with_updates
  UNION -- Discards duplicates
  SELECT time, value, date, device_id, org_id FROM first_eighth_hour_location_points
  UNION
  SELECT time, value, date, device_id, org_id FROM last_location_points
  UNION
  SELECT time, value, date, device_id, org_id FROM end_location_points
),

-- Make segments out of the location points
location_segments AS (
  SELECT *
  FROM (
    SELECT
      TO_DATE(LAG(date) OVER (w)) AS date, -- Use the start date and convert its type
      org_id,
      device_id,
      LAG(time) OVER (w) AS start_ms,
      time AS end_ms,
      LAG(value) OVER (w) AS value
    FROM filtered_location_points loc
    WINDOW w AS (PARTITION BY org_id, device_id ORDER BY time ASC)
  )
  WHERE start_ms IS NOT NULL
),

-- Split intervals that overlap an hour to avoid
-- counting distance data in the wrong hourly bucket.
-- This also allows us to avoid intervals that overlap with
-- a day outside of [start_date, end_date).
location_segments_split_by_hour AS (
  SELECT
    -- Make sure date is consistent with the subsegment start time
    TO_DATE(FROM_UNIXTIME(start_ms / 1000)) AS date,
    *
  FROM (
    SELECT
      org_id,
      device_id,
      GREATEST(start_ms, CAST(UNIX_TIMESTAMP(hour_start) * 1000 AS BIGINT)) AS start_ms,
      LEAST(end_ms, CAST(UNIX_TIMESTAMP(hour_start + INTERVAL 1 HOUR) * 1000 AS BIGINT)) AS end_ms,
      value
    FROM (
      SELECT
        *,
        EXPLODE(
          SEQUENCE(
            DATE_TRUNC('hour', FROM_UNIXTIME(start_ms/1000)),
            DATE_TRUNC('hour', FROM_UNIXTIME((end_ms-1)/1000)),
            INTERVAL 1 hour
          )
        ) AS hour_start
      FROM location_segments
    )
  )
)

SELECT * FROM location_segments_split_by_hour
WHERE date >= ${start_date} AND date < ${end_date}
