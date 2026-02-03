`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`created_at` TIMESTAMP,
`proto` STRUCT<
  `template_versions`: ARRAY<
    STRUCT<
      `field_types`: ARRAY<
        STRUCT<
          `media_upload_field_type`: STRUCT<
            `label`: STRING,
            `is_a_required_field`: BOOLEAN
          >,
          `string_field_type`: STRUCT<
            `label`: STRING,
            `is_a_required_field`: BOOLEAN,
            `pre_populated_data`: INT
          >,
          `number_field_type`: STRUCT<
            `label`: STRING,
            `num_decimal_places`: BIGINT,
            `is_a_required_field`: BOOLEAN
          >,
          `option_select_field_type`: STRUCT<
            `label`: STRING,
            `option_labels`: ARRAY<
              STRUCT<`label`: STRING>
            >,
            `is_a_required_field`: BOOLEAN
          >,
          `signature_field_type`: STRUCT<
            `label`: STRING,
            `legal_text`: STRING,
            `is_a_required_field`: BOOLEAN
          >,
          `date_time_field_type`: STRUCT<
            `label`: STRING,
            `is_a_required_field`: BOOLEAN
          >,
          `scanned_document_field_type`: STRUCT<
            `label`: STRING,
            `is_a_required_field`: BOOLEAN
          >,
          `barcode_field_type`: STRUCT<
            `label`: STRING,
            `is_a_required_field`: BOOLEAN
          >
        >
      >,
      `version_number`: BIGINT,
      `created_at_ms`: BIGINT,
      `conditional_field_sections`: ARRAY<
        STRUCT<
          `triggering_field_index`: DECIMAL(20, 0),
          `triggering_field_value`: STRING,
          `conditional_field_index_first`: DECIMAL(20, 0),
          `conditional_field_index_last`: DECIMAL(20, 0)
        >
      >
    >
  >
>,
`_raw_proto` STRING,
`deleted` BYTE,
`date` STRING
