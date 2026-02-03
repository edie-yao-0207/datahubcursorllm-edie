select
  a.event_type as event_type,
  round(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
      ORDER BY
        (
          to_unix_timestamp(s.created_at) - s.event_ms / 1000
        )
    ) / 60,
    2
  ) as p50_latency_mins,
  round(
    PERCENTILE_CONT(0.95) WITHIN GROUP (
      ORDER BY
        (
          to_unix_timestamp(s.created_at) - s.event_ms / 1000
        )
    ) / 60,
    2
  ) as p95_latency_mins,
  round(
    PERCENTILE_CONT(0.99) WITHIN GROUP (
      ORDER BY
        (
          to_unix_timestamp(s.created_at) - s.event_ms / 1000
        )
    ) / 60,
    2
  ) as p99_latency_mins,
  count(*)
from
  safetydb_shards.safety_events s
  inner join definitions.harsh_accel_type_enums a on a.enum = s.detail_proto.accel_type
  inner join clouddb.organizations o on o.id = s.org_id
  and o.internal_type <> 1
where
  s.created_at >= :date_range.start
  and s.created_at <= :date_range.end 
-- only include event types of interest, those Konmari deals with
  and a.event_type in (
    'haAccel',
    'haBraking',
    'haCameraMisaligned',
    'haCrash',
    'haDistractedDriving',
    'haDriverObstructionPolicy',
    'haDrowsinessDetection',
    'haLaneDeparture',
    'haNearCollision',
    'haOutwardObstructionPolicy',
    'haPhonePolicy',
    'haSeatbeltPolicy',
    'haSharpTurn',
    'haTailgating',
    'haTileRollingStopSign'
  ) -- ignore manually created events as they will skew the metric massively
  and element_at(s.additional_labels.additional_labels, 1).label_source <> 3
group by 1
