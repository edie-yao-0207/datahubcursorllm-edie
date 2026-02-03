`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `deep_stream_metadata`: STRUCT<
      `video_processing_pipeline_run_id`: BIGINT,
      `stream_width`: BIGINT,
      `stream_height`: BIGINT,
      `detections`: ARRAY<
        STRUCT<
          `deep_stream_object_id`: BIGINT,
          `instances`: ARRAY<
            STRUCT<
              `delta_time_offset_ms`: INT,
              `delta_duration_ms`: INT,
              `delta_upper_left`: STRUCT<
                `delta_x`: INT,
                `delta_y`: INT
              >,
              `delta_lower_right`: STRUCT<
                `delta_x`: INT,
                `delta_y`: INT
              >,
              `diagnostics`: STRUCT<`delta_confidence_percent`: BIGINT>
            >
          >,
          `type`: INT,
          `secondary_classifier_metadata`: STRUCT<
            `person`: STRUCT<
              `clothing`: STRUCT<
                `upper_color`: INT,
                `lower_color`: INT
              >
            >
          >,
          `motion_metadata`: STRUCT<`motion_value`: BIGINT>,
          `confidence_percent`: BIGINT
        >
      >,
      `primary_object_detector_model_version`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
