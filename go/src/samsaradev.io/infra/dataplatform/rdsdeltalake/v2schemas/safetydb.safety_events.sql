`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`event_ms` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`detail_proto` STRUCT<
  `start`: STRUCT<
    `latitude`: BIGINT,
    `longitude`: BIGINT,
    `speed_milliknots`: BIGINT,
    `at_ms`: BIGINT
  >,
  `stop`: STRUCT<
    `latitude`: BIGINT,
    `longitude`: BIGINT,
    `speed_milliknots`: BIGINT,
    `at_ms`: BIGINT
  >,
  `duration_ms`: BIGINT,
  `at_ms`: BIGINT,
  `accel_type`: INT,
  `max_accel_gs`: DOUBLE,
  `brake_thresh_gs`: DOUBLE,
  `device_id`: BIGINT,
  `target_gateway_id`: BIGINT,
  `event_id`: BIGINT,
  `crash_thresh_gs`: DOUBLE,
  `reviewed_by_samsara_at_ms`: BIGINT,
  `ingestion_tag`: DECIMAL(20, 0),
  `customer_visible_ingestion_tag`: DECIMAL(20, 0),
  `hidden_to_customer`: BOOLEAN
>,
`_raw_detail_proto` STRING,
`trigger_reason` SHORT,
`additional_labels` STRUCT<
  `additional_labels`: ARRAY<
    STRUCT<
      `label_type`: INT,
      `label_source`: INT,
      `last_added_ms`: BIGINT,
      `user_id`: BIGINT
    >
  >
>,
`_raw_additional_labels` STRING,
`release_stage` SHORT,
`generated_in_release_stage` SHORT,
`include_in_reporting` BYTE,
`include_in_scoring` BYTE,
`include_in_coaching` BYTE,
`automatically_add_to_coaching_queue` BYTE,
`reviewed_by_samsara_at_ms` BIGINT,
`driver_id` BIGINT,
`trip_start_ms` BIGINT,
`uuid` STRING,
`event_metadata` STRUCT<
  `contexts`: ARRAY<
    STRUCT<
      `context_label`: INT,
      `aux_input_detail`: STRUCT<`placeholder`: STRING>,
      `asset_mode_detail`: STRUCT<
        `primary_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `driver_facing_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `external_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `analog_1_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `analog_2_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `analog_3_camera_detail`: STRUCT<`disabled`: BOOLEAN>,
        `analog_4_camera_detail`: STRUCT<`disabled`: BOOLEAN>
      >,
      `ai_generated_context_detail`: STRUCT<
        `event_summary`: STRUCT<
          `en_us`: STRING,
          `de_de`: STRING,
          `en_gb`: STRING,
          `es_es`: STRING,
          `es_us`: STRING,
          `fr_fr`: STRING,
          `it_it`: STRING,
          `nl_nl`: STRING,
          `pt_pt`: STRING,
          `cs_cz`: STRING,
          `pl_pl`: STRING,
          `en_ca`: STRING,
          `fr_ca`: STRING,
          `ro_ro`: STRING
        >,
        `labels`: ARRAY<
          STRUCT<
            `key`: STRING,
            `val`: STRING
          >
        >,
        `version`: STRING
      >,
      `drowsiness_context_detail`: STRUCT<`severity`: INT>,
      `severity_detail`: INT,
      `speed_limit_verified_context_detail`: STRUCT<
        `way_id_verified_data_sources`: ARRAY<
          STRUCT<
            `way_id`: BIGINT,
            `speed_limit_milliknots`: BIGINT,
            `verified_data_source`: INT
          >
        >,
        `dataset_org_id`: BIGINT,
        `dataset_version_id`: BIGINT,
        `dataset_version`: BIGINT
      >,
      `safety_event_review_context_detail`: STRUCT<
        `reviewed_labels`: ARRAY<
          STRUCT<
            `label_id`: STRING,
            `selected`: BOOLEAN
          >
        >
      >,
      `seatbelt_context_detail`: STRUCT<`status`: INT>,
      `mobile_usage_context_detail`: STRUCT<`type`: INT>,
      `detection_mode_context_detail`: STRUCT<`mode`: INT>,
      `construction_zone_context_detail`: STRUCT<`type`: INT>
    >
  >,
  `ml_mode`: INT,
  `is_shadow_event`: BOOLEAN,
  `detection_entity_type`: INT,
  `outward_obstruction_metadata`: STRUCT<
    `asset_ms`: BIGINT,
    `media_input`: INT,
    `camera_role_v2`: INT
  >,
  `detected_streams`: ARRAY<
    STRUCT<
      `media_input`: INT,
      `camera_role_v2`: INT,
      `device_id`: BIGINT
    >
  >,
  `is_power_loss_event`: BOOLEAN,
  `hide_ai_insight`: BOOLEAN,
  `is_manual_promotion`: BOOLEAN,
  `is_driverless_nudge`: BOOLEAN,
  `media_metadata`: STRUCT<`msp_client_asset_id`: STRING>,
  `sub_detections`: STRUCT<
    `sub_detections`: ARRAY<
      STRUCT<
        `start_ms`: BIGINT,
        `end_ms`: BIGINT,
        `event_id`: BIGINT,
        `time_ms`: BIGINT
      >
    >,
    `msp_client_asset_id`: STRING
  >,
  `is_backfill_event`: BOOLEAN,
  `is_internal_promotion`: BOOLEAN,
  `form_submission_uuid`: STRING,
  `backfilled_at_ms`: BIGINT,
  `org_engagement_tier`: INT,
  `ml_event_upgrade`: STRUCT<
    `source_behavior`: INT,
    `detected_behavior`: INT
  >,
  `ml_detection_result`: STRUCT<
    `expected_precision`: FLOAT,
    `model_version`: STRING,
    `inference_type`: STRING
  >,
  `speeding_confidence`: STRUCT<
    `sources`: ARRAY<
      STRUCT<
        `source_type`: INT,
        `score`: DOUBLE,
        `metadata`: MAP<STRING, DOUBLE>,
        `eval_mode`: BOOLEAN
      >
    >,
    `overall_score`: DOUBLE,
    `weights`: STRUCT<
      `map_matching_weight`: DOUBLE,
      `speed_limit_accuracy_weight`: DOUBLE,
      `detection_quality_weight`: DOUBLE
    >,
    `decision`: INT,
    `explanation`: STRING,
    `thresholds`: STRUCT<
      `high_confidence_threshold`: DOUBLE,
      `medium_confidence_threshold`: DOUBLE
    >,
    `eval_inclusive_score`: DOUBLE
  >
>,
`_raw_event_metadata` STRING,
`end_ms` BIGINT,
`date` STRING
