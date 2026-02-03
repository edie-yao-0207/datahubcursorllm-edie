-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW viewed_events AS (
  WITH us_users AS (
    SELECT *,
        CASE WHEN email ilike '%@samsara.com' THEN TRUE ELSE FALSE END AS is_samsara_email
    FROM datamodel_platform_bronze.raw_clouddb_users
    WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
  )
  SELECT sae.date,
      sae.org_id,
      sae.device_id,
      sae.event_ms,
      sae.activity_name,
      sae.created_at,
      unix_millis(TIMESTAMP(sae.created_at)) AS created_at_ms,
      (unix_millis(TIMESTAMP(sae.created_at)) - event_ms)/(1000*60*60*24) AS days_to_view
  FROM datamodel_safety_silver.stg_activity_events sae
  INNER JOIN us_users du
    ON sae.detail_proto.user_id = du.id
    AND sae.`date` = du.`date`
  INNER JOIN datamodel_core.dim_organizations do ON sae.org_id = do.org_id
    AND sae.`date` = do.`date`
  WHERE sae.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND do.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND du.is_samsara_email = FALSE
    AND do.internal_type = 0
    AND sae.activity_enum IN (8,4,1,11,9,16,14)
    -- ('BehaviorLabelActivityType', 'CoachingStateActivityType', 'CommentActivityType', 'VideoDownloadActivityType', 'ViewedByActivityType', 'ShareSafetyEventActivityType', 'DriverCommentActivityType')
    --https://github.com/samsara-dev/backend/blob/067d3e46149d1276fbd39f8464961c75187f0b41/go/src/samsaradev.io/fleet/safety/safetyproto/safetyactivity.proto#L16
    --https://docs.google.com/spreadsheets/d/1VGJBamCSeICLyeOlxo4eb0hV_4OT2WRDEAbb8eKMysk/edit#gid=359523161
  QUALIFY ROW_NUMBER() OVER(PARTITION BY sae.event_ms, sae.device_id, sae.org_id ORDER BY sae.created_at ASC) = 1 -- first viewed qualifying activity
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW speeding_events AS (
  SELECT a.date,
        a.org_id,
        do.org_name,
        do.account_size_segment_name,
        do.account_industry,
        a.device_id,
        a.driver_id,
        a.start_ms,
        a.end_ms,
        a.trigger_label,
        blte.behavior_label_type,
        a.coaching_state,
        a.triage_state,
        CASE
          WHEN a.triage_state = 0 THEN 'Invalid'
          WHEN a.triage_state = 1 THEN 'Needs Review'
          WHEN a.triage_state = 2 THEN 'Reviewed'
          WHEN a.triage_state = 3 THEN 'In Coaching'
          WHEN a.triage_state = 4 THEN 'Dismissed'
          WHEN a.triage_state = 5 THEN 'Manual Review'
          WHEN a.triage_state = 6 THEN 'Manual Review Dismissed'
          WHEN a.triage_state = 7 THEN 'Needs Recognition'
          WHEN a.triage_state = 8 THEN 'Recognized'
          ELSE 'ERROR'
        END as triage_state_english,
        CASE WHEN triage_state = 4 THEN 1 ELSE 0 END AS is_dismissed,
        CASE WHEN a.triage_state IN (0,1) THEN 0 ELSE 1 END AS is_status_change,
        CASE WHEN ve.event_ms IS NULL THEN 0 ELSE 1 END AS is_viewed,
        COALESCE(loc.value.revgeo_state,'Unknown') AS revgeo_state,
        COALESCE(loc.value.revgeo_country,'Unknown') AS revgeo_country--,
        -- COALESCE(abs(a.start_ms - loc.time),99999) AS millis_from_event
  FROM safetyeventtriagedb_shards.triage_events_v2 a
  INNER JOIN definitions.behavior_label_type_enums blte
    ON a.trigger_label = blte.enum
  INNER JOIN datamodel_core.dim_organizations do
    ON a.org_id = do.org_id
    AND a.date = do.date
  LEFT JOIN kinesisstats.location loc
    ON a.device_id = loc.device_id
    AND a.date = loc.date
    AND a.start_ms = loc.time
  LEFT JOIN viewed_events ve
    ON a.device_id = ve.device_id
    AND a.org_id = ve.org_id
    AND a.start_ms = ve.event_ms
  WHERE a.trigger_label IN (6,32,33)
    AND a.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND do.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND do.is_paid_safety_customer
);

-- SevereSpeeding 33
-- MaxSpeed 32
-- Speeding 6
-- triage_states: https://github.com/samsara-dev/backend/blob/b45df232b1ffaa86b38152f441d38526bc6d0447/go/src/samsaradev.io/fleet/safety/infra/safetyeventtriagemodels/triage_state_metadata.go#L29

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.speeding_events
USING delta
PARTITIONED BY (date)
AS
SELECT * FROM speeding_events

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW speeding_events_updates AS (
  SELECT *
  FROM speeding_events
  WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),14)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
);

MERGE INTO data_analytics.speeding_events AS target
USING speeding_events_updates AS updates
ON target.date = updates.date
AND target.device_id = updates.device_id
AND target.start_ms = updates.start_ms
AND target.trigger_label = updates.trigger_label
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;

-- COMMAND ----------

