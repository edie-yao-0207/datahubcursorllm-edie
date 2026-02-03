WITH sam_orgs AS (
    SELECT
        o.org_id,
        s.sam_number
    FROM clouddb.org_sfdc_accounts o
    LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
),

driving_activity AS (
    SELECT
        org_dates.org_id,
        sam_orgs.sam_number,
        org_dates.date,
        trips.device_id,
        COUNT(trips.device_id) AS num_trips,
        SUM((proto.end.time - proto.start.time)) AS trip_duration,
        SUM(proto.trip_distance.distance_meters) AS trip_distance_meters,
        SUM(proto.trip_fuel.fuel_consumed_ml) AS fuel_consumed_ml,
        SUM(proto.trip_speeding_mph.not_speeding_ms) AS not_speeding_ms,
        SUM(proto.trip_speeding_mph.light_speeding_ms) AS light_speeding_ms,
        SUM(proto.trip_speeding_mph.moderate_speeding_ms) AS moderate_speeding_ms,
        SUM(proto.trip_speeding_mph.heavy_speeding_ms) AS heavy_speeding_ms,
        SUM(proto.trip_speeding_mph.severe_speeding_ms) AS severe_speeding_ms,
        COUNT(events.detail_proto.accel_type) AS event_count
    FROM customer360.org_dates org_dates
    JOIN sam_orgs ON org_dates.sam_number = sam_orgs.sam_number
    LEFT JOIN trips2db_shards.trips trips
        ON org_dates.date = trips.date
        AND sam_orgs.org_id = trips.org_id
        AND trips.version = 101
    LEFT JOIN safetydb_shards.safety_events events
        ON trips.device_id = events.device_id
        AND trips.org_id = events.org_id
        AND events.event_ms BETWEEN proto.start.time AND proto.end.time
        AND events.date = org_dates.date
    WHERE
        (proto.end.time-proto.start.time)<(24*60*60*1000) AND -- Filter out very lONg trips
        proto.start.time != proto.end.time -- actual trips
    GROUP BY
        org_dates.org_id,
        sam_orgs.sam_number,
        org_dates.date,
        trips.device_id
),

coaching_state AS (
    SELECT
        org_dates.org_id,
        sam_orgs.sam_number,
        org_dates.date,
        REPLACE(LOWER(cs.coaching_type), ' ','_') AS coaching_type,
        COUNT(distinct detail_proto.event_id) AS num_events
    FROM customer360.org_dates org_dates
    JOIN sam_orgs ON org_dates.sam_number = sam_orgs.sam_number
    LEFT JOIN safetydb_shards.safety_events events
        ON sam_orgs.org_id = events.org_id
        AND events.date = org_dates.date
    JOIN definitions.harsh_accel_type_enums event_enum
        ON events.detail_proto.accel_type = event_enum.enum
    LEFT JOIN safetydb_shards.safety_event_metadata metadata
        ON events.org_id = metadata.org_id
        AND events.device_id = metadata.device_id
        AND events.event_ms = metadata.event_ms
        AND events.date = metadata.date
    LEFT JOIN definitions.coaching_state_enums cs ON cs.enum = metadata.coaching_state
    group by
        org_dates.org_id,
        sam_orgs.sam_number,
        org_dates.date,
        replace(lower(cs.coaching_type), ' ','_')
),

coaching_state_pivoted AS (
    SELECT * FROM (
        SELECT *
        FROM coaching_state
    )
    PIVOT (
        SUM(num_events)
        for coaching_type IN ('needs_review', 'needs_coaching', 'needs_recognition', 'reviewed', 'coached', 'recognized', 'dismissed')
    )
),

org_data_usage AS ( 
  SELECT
    d.org_id,
    att.record_received_date AS date,
    CAST(att.data_usage AS double) AS bytes
  FROM dataprep_cellular.att_daily_usage att
  JOIN productsdb.devices d ON att.iccid = d.iccid
),

sam_data_usage AS (
    SELECT
        o.date,
        o.org_id,
        s.sam_number,
        SUM(bytes) AS bytes
    FROM org_data_usage o
    LEFT JOIN sam_orgs s ON o.org_id = s.org_id
    GROUP BY
        o.date,
        o.org_id,
        s.sam_number
),

org_active_devices AS (
    SELECT
        ad.date,
        ad.org_id,
        ad.product_id,
        COUNT(distinct d.serial) AS num_active_devices
    FROM dataprep.active_devices ad
    LEFT JOIN productsdb.devices d ON ad.device_id = d.id AND ad.org_id = d.org_id
    group by
        ad.date,
        ad.product_id,
        ad.org_id
),

sam_active_devices AS (
    SELECT
        date,
        o.org_id,
        s.sam_number,
        SUM(cASe when product_id in (7, 17, 24, 35, 53) then num_active_devices else 0 end) AS num_active_vg,
        SUM(cASe when product_id in (25, 30, 31, 43, 44, 46) then num_active_devices else 0 end) AS num_active_cm,
        SUM(cASe when product_id in (27, 32, 36, 38, 42, 54, 62, 65, 68) then num_active_devices else 0 end) AS num_active_ag,
        SUM(cASe when product_id in (28, 34, 45, 49, 61, 75) then num_active_devices else 0 end) AS num_active_ig
    FROM org_active_devices sr
    LEFT JOIN clouddb.org_sfdc_accounts o ON sr.org_id = o.org_id
    LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
    group by
        date,
        o.org_id,
        s.sam_number
),

org_videos_retrieved AS (
    SELECT
        date,
        organization_id AS org_id,
        count(1) AS num_videos_retrieved
    FROM clouddb.historical_video_requests vr
    LEFT JOIN clouddb.groups g ON
        vr.group_id = g.id
    GROUP BY
        date,
        org_id
),

sam_videos_retrieved AS (
    SELECT
        vr.date,
        vr.org_id,
        s.sam_number,
        sum(vr.num_videos_retrieved) AS num_videos_retrieved
    FROM org_videos_retrieved vr
    LEFT JOIN clouddb.org_sfdc_accounts o ON vr.org_id = o.org_id
    LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
    GROUP BY
        vr.date,
        vr.org_id,
        s.sam_number
)

SELECT
  org_dates.org_id,
  org_dates.sam_number,
  org_dates.date,
  SUM(driving_activity.num_trips) AS num_trips,
  SUM(driving_activity.trip_duratiON) AS total_trip_duration,
  SUM(driving_activity.trip_distance_meters) AS total_trip_distance_meters,
  SUM(driving_activity.fuel_consumed_ml) AS total_fuel_consumed_ml,
  SUM(driving_activity.not_speeding_ms) AS total_not_speeding_ms,
  SUM(driving_activity.light_speeding_ms) AS total_light_speeding_ms,
  SUM(driving_activity.moderate_speeding_ms) AS total_moderate_speeding_ms,
  SUM(driving_activity.heavy_speeding_ms) AS total_heavy_speeding_ms,
  SUM(driving_activity.severe_speeding_ms) AS total_severe_speeding_ms,
  SUM(driving_activity.event_count) AS total_harsh_events,
  SUM(du.bytes) AS total_data_usage,
  MAX(ad.num_active_vg) AS num_active_vg,
  MAX(ad.num_active_cm) AS num_active_cm,
  MAX(ad.num_active_ag) AS num_active_ag,
  MAX(ad.num_active_ig) AS num_active_ig,
  MAX(he.haBraking) AS haBraking,
  MAX(he.haAccel) AS haAccel,
  MAX(he.hASharpTurn) AS haSharpTurn,
  MAX(he.haCrash) AS haCrash,
  MAX(he.haDistractedDriving) AS haDistractedDriving,
  MAX(he.haTailgating) AS haTailgating,
  MAX(needs_review) AS needs_review,
  MAX(needs_coaching) AS needs_coaching,
  MAX(needs_recognition) AS needs_recognition,
  MAX(reviewed) AS reviewed,
  MAX(coached) AS coached,
  MAX(recognized) AS recognized,
  MAX(dismissed) AS dismissed,
  MAX(vr.num_videos_retrieved) AS num_videos_retrieved
FROM customer360.org_dates org_dates
LEFT JOIN driving_activity
  ON org_dates.org_id = driving_activity.org_id
  AND org_dates.date = driving_activity.date
LEFT JOIN sam_data_usage du
  ON org_dates.org_id = du.org_id
  AND org_dates.date = du.date
LEFT JOIN sam_active_devices ad
  ON org_dates.date = ad.date
  AND org_dates.org_id = ad.org_id
LEFT JOIN coaching_state_pivoted cs
  ON org_dates.date = cs.date
  AND org_dates.org_id = cs.org_id
LEFT JOIN customer360.customer_harsh_events he
  ON org_dates.date = he.date
  AND org_dates.org_id = he.org_id
LEFT JOIN sam_videos_retrieved vr
  ON org_dates.date = vr.date
  AND org_dates.org_id = vr.org_id
WHERE
    org_dates.date IS NOT NULL AND
    org_dates.org_id IS NOT NULL AND
    org_dates.date >= ${start_date} AND
    org_dates.date < ${end_date}
GROUP BY
  org_dates.org_id,
  org_dates.sam_number,
  org_dates.date
