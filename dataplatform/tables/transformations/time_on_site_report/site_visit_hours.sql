WITH location_data AS (
  SELECT
    org_id,
    device_id,
    date,
    value.time as time_ms,
    ST_Transform(
      ST_Point(
        CAST(value.longitude AS decimal(38, 10)),
        CAST(value.latitude AS decimal(38, 10))
      ),
      'epsg:4326', 'epsg:102003', true, true
    ) AS geom
  FROM kinesisstats.location
  WHERE date >= DATE_SUB(${start_date}, 2)
  AND date < ${end_date}
  AND value.longitude IS NOT NULL
  AND value.latitude IS NOT NULL
  AND value.longitude > -180 AND value.longitude < 180
  AND value.latitude > -90 AND value.latitude < 90
),

addresses AS (
  SELECT
    grp.organization_id AS org_id,
    grp.id AS group_id,
    addr.id AS address_id,
    addr.name,
    addr.address,
    CAST(addr.latitude AS DECIMAL(38, 10)) AS latitude,
    CAST(addr.longitude AS DECIMAL(38, 10)) AS longitude,
    CAST(addr.radius AS DECIMAL(38, 10)) AS radius,
    addr.error,
    CASE WHEN addr.points IS NOT NULL
      THEN
        ST_AsText( -- geom -> WKT
          ST_PolygonFromText( -- "x1,y1,x2,y2,..." -> geom
            CONCAT_WS(',', FLATTEN( -- ARRAY((x1, y1), (x2, y2), ...) -> "x1,y1,x2,y2,..."
              CONCAT(addr.points, SLICE(addr.points,1,1)) -- Close open polygon
            )),
            ','
          )
        )
      ELSE NULL
    END AS geofence_wkt
  FROM
    clouddb.groups AS grp
    JOIN
    (
      SELECT
        id, group_id, name, address, latitude, longitude, radius, error,
        TRANSFORM(geofence_proto.geofence_polygon.vertices, v -> ARRAY(CAST(v.lng AS STRING), CAST(v.lat AS STRING))) AS points
      FROM clouddb.addresses
    ) AS addr
    ON addr.group_id = grp.id
  WHERE grp.organization_id NOT IN (
    SELECT org_id FROM helpers.ignored_org_ids
  )
),

-- epsg:4326 https://epsg.io/4326: coordinate system used by gps
-- epsg:102003 https://epsg.io/102003: USA Contiguous Albers Equal Area Conic. General purpose, low distortion. May not work well for EU.
polygons AS (
  SELECT
    org_id,
    address_id,
    -- First true specifies lng/lat ordering and second true optional parameters required to disable "Bursa wolf parameters required" error from ST_Transform.
    -- https://datasystemslab.github.io/GeoSpark/api/sql/GeoSparkSQL-Function/#st_transform
    ST_Transform(ST_GeomFromWKT(geofence_wkt), 'epsg:4326', 'epsg:102003', true, true) as geom
  FROM addresses
  WHERE geofence_wkt IS NOT null
),

-- Filter out rows with non-positive or very large radius so ST_Envelope doesn't break.
circles AS (
  SELECT
    org_id,
    address_id,
    ST_Buffer(ST_Transform(ST_Point(longitude, latitude), 'epsg:4326', 'epsg:102003', true, true), radius) as geom
  FROM addresses
  WHERE geofence_wkt IS null
    AND longitude IS NOT null AND latitude IS NOT null
    AND longitude > -180 AND longitude < 180
    AND latitude > -90 AND latitude < 90
    AND radius > 0 AND radius < 10000 * 10000
),

geofences AS(
  SELECT *
  FROM polygons
  WHERE ST_IsValid(geom)
  UNION ALL
  SELECT *
  FROM circles
  WHERE ST_IsValid(geom)
),

-- Envelope of a polygon is another polygon (typically a rectangle) which fully encompasses the polygon in question.
envelopes AS (
  SELECT
    org_id,
    address_id,
    geom,
    ST_AsText(ST_Envelope(geom)) AS env,
    ST_Area(ST_Envelope(geom)) / (1000 * 1000) AS area_km2
  FROM geofences
),

-- (x1, y1) is the bottom left corner of the envelope.
-- (x2, y2) is the rop right corer of the envelope.
-- x dimension increases towards the right.
-- y dimension increases upwards.
bounds AS (
  SELECT
    org_id,
    address_id,
    geom,
    area_km2,
    -- geospark doesn't have a function to extract points from polygon
    -- note that `\` in regex is replaced with `\\`, see example at
    -- https://spark.apache.org/docs/latest/api/sql/index.html#regexp_extract
    float(regexp_extract(env, 'POLYGON \\(\\(([^\\s]+) ([^,]+), [^\\s]+ [^,]+, ([^\\s]+) ([^,]+), [^\\s]+ [^,]+, [^\\s]+ [^)]+\\)\\)', 1)) AS x1,
    float(regexp_extract(env, 'POLYGON \\(\\(([^\\s]+) ([^,]+), [^\\s]+ [^,]+, ([^\\s]+) ([^,]+), [^\\s]+ [^,]+, [^\\s]+ [^)]+\\)\\)', 2)) AS y1,
    float(regexp_extract(env, 'POLYGON \\(\\(([^\\s]+) ([^,]+), [^\\s]+ [^,]+, ([^\\s]+) ([^,]+), [^\\s]+ [^,]+, [^\\s]+ [^)]+\\)\\)', 3)) AS x2,
    float(regexp_extract(env, 'POLYGON \\(\\(([^\\s]+) ([^,]+), [^\\s]+ [^,]+, ([^\\s]+) ([^,]+), [^\\s]+ [^,]+, [^\\s]+ [^)]+\\)\\)', 4)) AS y2
  FROM envelopes
  WHERE area_km2 < 10000 * 1000
),

-- Categorize each geofence with its corresponding cell level based on its area.
levels AS (
  SELECT
    org_id,
    address_id,
    geom,
    x1, y1, x2, y2,
    -- pick a cell level (1 km^2, 10 km^2, 100 km^2 or 1000 km^2)
    CASE
      WHEN area_km2 > 1000 THEN 1000 * 1000
      WHEN area_km2 > 100 THEN 100 * 1000
      WHEN area_km2 > 10 THEN 10 * 1000
      ELSE 1000
    END AS cell_length
  FROM bounds
),

split_x AS (
  SELECT
    org_id,
    address_id,
    geom,
    cell_length,
    -- split geofence into covering segments of `cell_length` meters west to east
    EXPLODE(
      SEQUENCE(
        CAST(floor(x1 / cell_length) AS BIGINT) * cell_length,
        CAST(floor(x2 / cell_length) AS BIGINT) * cell_length,
        cell_length
      )
    ) AS x,
    y1,
    y2
  FROM levels
),

split_y AS (
  SELECT
    org_id,
    address_id,
    geom,
    cell_length,
    x,
    -- split geofence into covering segments of `cell_length` meters south to north
    EXPLODE(
      SEQUENCE(
        CAST(floor(y1 / cell_length) AS BIGINT) * cell_length,
        CAST(floor(y2 / cell_length) AS BIGINT) * cell_length,
        cell_length
      )
    ) AS y
  FROM split_x
),

geofence_cells AS (
  SELECT
    org_id,
    address_id,
    geom,
    struct(x, y, cell_length) AS cell
  FROM split_y
),

location_xy AS (
  SELECT
    org_id,
    device_id,
    date,
    time_ms,
    geom,
    float(regexp_extract(ST_AsText(geom), 'POINT \\(([^\\s]+) ([^\\)]+)\\)', 1)) as x,
    float(regexp_extract(ST_AsText(geom), 'POINT \\(([^\\s]+) ([^\\)]+)\\)', 2)) as y
  FROM location_data
),

-- We expand each location row into a row for each cell. This allows us to match address cells with locations from any cell.
location_cells AS (
  SELECT
    org_id,
    device_id,
    date,
    time_ms,
    geom,
    EXPLODE(
      array(
        struct(
          CAST(floor(x / 1000) AS BIGINT) * 1000 AS x,
          CAST(floor(y / 1000) AS BIGINT) * 1000 AS y,
          1000 AS cell_length
        ),
        struct(
          CAST(floor(x / (10 * 1000)) AS BIGINT) * 10 * 1000 AS x,
          CAST(floor(y / (10 * 1000)) AS BIGINT) * 10 * 1000 AS y,
          10 * 1000 AS cell_length
        ),
        struct(
          CAST(floor(x / (100 * 1000)) AS BIGINT) * 100 * 1000 AS x,
          CAST(floor(y / (100 * 1000)) AS BIGINT) * 100 * 1000 AS y,
          100 * 1000 AS cell_length
        ),
        struct(
          CAST(floor(x / (1000 * 1000)) AS BIGINT) * 1000 * 1000 AS x,
          CAST(floor(y / (1000 * 1000)) AS BIGINT) * 1000 * 1000 AS y,
          1000 * 1000 AS cell_length
        )
      )
    ) AS cell
  FROM location_xy
),

location_entries_with_address AS (
  SELECT
    l.org_id,
    l.device_id,
    l.date,
    l.time_ms,
    first(struct(l.*)) AS location,
    first(struct(g.*)) AS address
  FROM
    location_cells l
    JOIN geofence_cells g
    ON l.org_id = g.org_id
    AND l.cell = g.cell
    AND ST_Contains(g.geom, l.geom)
  GROUP BY l.org_id, l.device_id, l.date, l.time_ms, g.address_id
  HAVING l.date >= ${start_date}
  AND l.date < ${end_date}
),

neighboring_geofence_times AS (
  SELECT
    location.org_id,
    location.device_id,
    date,
    address.address_id,
    location.time_ms,
    LAG(location.time_ms) OVER (PARTITION BY location.org_id, location.device_id, address.address_id ORDER BY location.time_ms) AS prev_geofence_time,
    LEAD(location.time_ms) OVER (PARTITION BY location.org_id, location.device_id, address.address_id ORDER BY location.time_ms) AS next_geofence_time
  FROM location_entries_with_address
  WHERE address.address_id IS NOT null
),

location_entries_with_neighboring_time AS (
  SELECT
    org_id,
    device_id,
    date,
    value.time AS time_ms,
    previous.time AS prev_time_ms,
    next.time AS next_time_ms
  FROM kinesisstats_window.location
  WHERE date >= DATE_SUB(${start_date}, 2)
  AND date < ${end_date}
),

geofence_events AS (
  SELECT
    g.org_id,
    g.device_id,
    g.date,
    g.address_id,
    g.time_ms,
    (g.prev_geofence_time IS null OR (g.prev_geofence_time != l.prev_time_ms)) AND l.prev_time_ms IS NOT null AS is_entry,
    (g.next_geofence_time IS null OR (g.next_geofence_time != l.next_time_ms)) AND l.next_time_ms IS NOT null AS is_exit
  FROM neighboring_geofence_times g
    JOIN location_entries_with_neighboring_time l
    ON g.org_id = l.org_id
    AND g.device_id = l.device_id
    AND g.time_ms = l.time_ms
    AND g.date = l.date
),

geofence_transitions AS (
  SELECT
    *,
    LAG(time_ms) OVER (PARTITION BY org_id, device_id, address_id ORDER BY time_ms) AS prev_time_ms
  FROM geofence_events
  WHERE (is_entry = true AND is_exit = false) OR (is_entry = false AND is_exit = true)
),

geofence_exits AS (
  SELECT
    org_id,
    device_id,
    date,
    address_id,
    prev_time_ms AS start_ms,
    time_ms AS end_ms
  FROM geofence_transitions
  WHERE is_exit = true
  AND prev_time_ms IS NOT null
),

geofence_interval_hours AS (
  SELECT
    *,
    hour AS interval_start,
    hour + interval 1 hour AS interval_end
  FROM (
    SELECT
      *,
      EXPLODE(
        SEQUENCE(
          date_trunc('hour', from_unixtime(start_ms / 1000)),
          date_trunc('hour', from_unixtime(end_ms / 1000)),
          interval 1 hour
        )
      ) as hour
    FROM geofence_exits
  )
)

SELECT
  org_id,
  device_id,
  date,
  address_id,
  interval_start,
  interval_end,
  start_ms AS visit_start_ms,
  end_ms AS visit_end_ms,
  CASE WHEN start_ms > to_unix_timestamp(interval_start) * 1000 THEN start_ms ELSE to_unix_timestamp(interval_start) * 1000 END AS start_ms,
  CASE WHEN end_ms < to_unix_timestamp(interval_end) * 1000 THEN end_ms ELSE to_unix_timestamp(interval_end) * 1000 END AS end_ms
FROM geofence_interval_hours
WHERE end_ms - start_ms >= 1000*60*2
AND date >= ${start_date}
AND date < ${end_date}
