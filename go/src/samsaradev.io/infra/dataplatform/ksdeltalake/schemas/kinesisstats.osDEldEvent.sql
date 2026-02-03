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
    `eld_events`: ARRAY<
      STRUCT<
        `type`: INT,
        `records`: ARRAY<
          STRUCT<
            `metadata`: STRUCT<
              `unique_id`: BINARY,
              `creation`: STRUCT<
                `utc_ms`: DECIMAL(20, 0),
                `op_id`: DECIMAL(20, 0),
                `eld_id`: STRING
              >,
              `last_modification`: STRUCT<
                `utc_ms`: DECIMAL(20, 0),
                `op_id`: DECIMAL(20, 0),
                `eld_id`: STRING
              >,
              `recorded_in_jurisdiction`: INT,
              `debug`: STRUCT<
                `eld_state_json`: STRING,
                `vehicle_signals_json`: STRING,
                `reason`: STRING
              >,
              `status_override`: STRUCT<
                `ucc_event_code`: INT,
                `dsc_event_code`: INT
              >,
              `edit_proposal_relationships`: STRUCT<
                `originated_in_id`: BINARY,
                `originated_by`: STRUCT<
                  `id`: BIGINT,
                  `user_type`: INT,
                  `username`: STRING,
                  `first_name`: STRING,
                  `last_name`: STRING
                >
              >,
              `initial_origin`: STRUCT<
                `origin`: INT,
                `from_unidentified_driver`: BOOLEAN
              >
            >,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `driver_ids`: ARRAY<BIGINT>,
            `sequence_id_usa`: STRUCT<`value`: BIGINT>,
            `sequence_id_ca`: STRUCT<`value`: BIGINT>,
            `event_type`: INT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `duty_status_change`: STRUCT<
              `event_code`: INT,
              `status`: INT,
              `origin`: INT,
              `malfunction_active`: BOOLEAN,
              `diagnostic_active`: BOOLEAN,
              `accumulated_distance_meters`: DECIMAL(20, 0),
              `elapsed_engine_seconds`: DECIMAL(20, 0),
              `distance_slvc_meters`: DECIMAL(20, 0),
              `location`: STRUCT<
                `latitude`: BIGINT,
                `longitude`: BIGINT,
                `validity`: INT,
                `geolocation_usa`: STRING,
                `geolocation_ca`: STRING
              >,
              `location_description`: STRING,
              `comment`: STRING,
              `event_data_check_usa`: BIGINT,
              `event_data_check_ca`: BIGINT,
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >
            >,
            `intermediate_log`: STRUCT<
              `event_code`: INT,
              `status`: INT,
              `origin`: INT,
              `malfunction_active`: BOOLEAN,
              `diagnostic_active`: BOOLEAN,
              `accumulated_distance_meters`: DECIMAL(20, 0),
              `elapsed_engine_seconds`: DECIMAL(20, 0),
              `distance_slvc_meters`: DECIMAL(20, 0),
              `location`: STRUCT<
                `latitude`: BIGINT,
                `longitude`: BIGINT,
                `validity`: INT,
                `geolocation_usa`: STRING,
                `geolocation_ca`: STRING
              >,
              `comment`: STRING,
              `event_data_check_usa`: BIGINT,
              `event_data_check_ca`: BIGINT,
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >
            >,
            `usage_categorization_change`: STRUCT<
              `event_code`: INT,
              `status`: INT,
              `origin`: INT,
              `malfunction_active`: BOOLEAN,
              `diagnostic_active`: BOOLEAN,
              `accumulated_distance_meters`: DECIMAL(20, 0),
              `elapsed_engine_seconds`: DECIMAL(20, 0),
              `total_distance_meters`: DECIMAL(20, 0),
              `distance_slvc_meters`: DECIMAL(20, 0),
              `location`: STRUCT<
                `latitude`: BIGINT,
                `longitude`: BIGINT,
                `validity`: INT,
                `geolocation_usa`: STRING,
                `geolocation_ca`: STRING
              >,
              `location_description`: STRING,
              `comment`: STRING,
              `event_data_check_usa`: BIGINT,
              `event_data_check_ca`: BIGINT,
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >
            >,
            `logs_certified`: STRUCT<
              `event_code`: BIGINT,
              `timezone_offset_minutes`: INT,
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >,
              `certified_date`: STRING,
              `day_start_type`: INT
            >,
            `driver_login_status_change`: STRUCT<
              `event_code`: INT,
              `total_distance_meters`: DECIMAL(20, 0),
              `total_engine_seconds`: DECIMAL(20, 0),
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >
            >,
            `engine_status_change`: STRUCT<
              `event_code`: INT,
              `total_distance_meters`: DECIMAL(20, 0),
              `total_engine_seconds`: DECIMAL(20, 0),
              `distance_slvc_meters`: DECIMAL(20, 0),
              `location`: STRUCT<
                `latitude`: BIGINT,
                `longitude`: BIGINT,
                `validity`: INT,
                `geolocation_usa`: STRING,
                `geolocation_ca`: STRING
              >,
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >,
              `trailers`: ARRAY<
                STRUCT<
                  `id`: BIGINT,
                  `name`: STRING,
                  `license_plate`: STRING
                >
              >
            >,
            `diagnostic_or_malfunction`: STRUCT<
              `event_code`: INT,
              `diagnostic_or_malfunction_code`: STRING,
              `total_distance_meters`: DECIMAL(20, 0),
              `total_engine_seconds`: DECIMAL(20, 0),
              `shipping_documents`: ARRAY<
                STRUCT<`document_number`: STRING>
              >
            >,
            `off_duty_deferral_ca`: STRUCT<`event_code`: INT>,
            `cycle_change_ca`: STRUCT<`event_code`: INT>,
            `operating_zone_change_ca`: STRUCT<`event_code`: INT>,
            `additional_hours_not_recorded_ca`: STRUCT<`event_code`: INT>
          >
        >,
        `exemption_declarations`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `exemption_claim_id`: BINARY,
            `declaration_type`: INT,
            `exemption_type`: INT,
            `carrier_id`: BIGINT,
            `driver_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `remark`: STRING,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING
          >
        >,
        `prompt_lifecycle_changes`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `prompt_instance_id`: BINARY,
            `prompt_type`: INT,
            `prompt_lifecycle_stage`: INT,
            `carrier_id`: BIGINT,
            `driver_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `expires_at_utc_ms`: DECIMAL(20, 0),
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING
          >
        >,
        `team_driving_configuration_changes`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `team_driving_session_id`: BINARY,
            `change_trigger`: INT,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `active_driver_id`: BIGINT,
            `co_driver_ids`: ARRAY<BIGINT>,
            `added_driver_ids`: ARRAY<BIGINT>,
            `removed_driver_ids`: ARRAY<BIGINT>,
            `previous_active_driver_id`: BIGINT,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING
          >
        >,
        `border_crossings`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `active_driver_id`: BIGINT,
            `co_driver_ids`: ARRAY<BIGINT>,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `prev_reverse_geocode`: STRUCT<
              `country`: INT,
              `state`: INT,
              `changed_at_utc_ms`: DECIMAL(20, 0)
            >,
            `next_reverse_geocode`: STRUCT<
              `country`: INT,
              `state`: INT,
              `changed_at_utc_ms`: DECIMAL(20, 0)
            >,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by_eld_id`: STRING
          >
        >,
        `edit_proposal_lifecycle_changes`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `proposal_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_ids`: ARRAY<BIGINT>,
            `driver_ids`: ARRAY<BIGINT>,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by_eld_id`: STRING,
            `edit_proposal_type`: INT,
            `edit_proposal_status`: INT,
            `comment`: STRING
          >
        >,
        `shipping_document_association_declarations`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `driver_id`: BIGINT,
            `shipping_documents`: ARRAY<
              STRUCT<`document_number`: STRING>
            >,
            `remark`: STRING,
            `status`: INT,
            `source`: INT,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING,
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by`: BIGINT,
            `updated_at_op_id`: DECIMAL(20, 0),
            `updated_by_eld_id`: STRING,
            `updated_at_utc_ms`: DECIMAL(20, 0)
          >
        >,
        `trailer_association_declarations`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `driver_id`: BIGINT,
            `trailers`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `name`: STRING,
                `license_plate`: STRING
              >
            >,
            `remark`: STRING,
            `status`: INT,
            `source`: INT,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING,
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by`: BIGINT,
            `updated_at_op_id`: DECIMAL(20, 0),
            `updated_by_eld_id`: STRING,
            `updated_at_utc_ms`: DECIMAL(20, 0)
          >
        >,
        `unassigned_segment_attribution_declarations`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `driver_id`: BIGINT,
            `start_at_utc_ms`: DECIMAL(20, 0),
            `end_at_utc_ms`: DECIMAL(20, 0),
            `status`: INT,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by_eld_id`: STRING,
            `created_in_jurisdiction`: INT,
            `updated_at_op_id`: DECIMAL(20, 0),
            `updated_at_utc_ms`: DECIMAL(20, 0),
            `updated_by_eld_id`: STRING
          >
        >,
        `vehicle_association_declarations`: ARRAY<
          STRUCT<
            `unique_id`: BINARY,
            `carrier_id`: BIGINT,
            `vehicle_id`: BIGINT,
            `event_at_utc_ms`: DECIMAL(20, 0),
            `driver_id`: BIGINT,
            `session_id`: BINARY,
            `driver_role`: INT,
            `status`: INT,
            `source`: INT,
            `created_at_op_id`: DECIMAL(20, 0),
            `created_in_jurisdiction`: INT,
            `created_by_eld_id`: STRING,
            `created_at_utc_ms`: DECIMAL(20, 0),
            `created_by`: BIGINT,
            `updated_at_op_id`: DECIMAL(20, 0),
            `updated_by_eld_id`: STRING,
            `updated_at_utc_ms`: DECIMAL(20, 0),
            `declaration_type`: INT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
