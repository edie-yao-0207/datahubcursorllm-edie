`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`driver_document_uuid` STRING,
`modified_at_ms` TIMESTAMP,
`modified_by_user_id` BIGINT,
`modified_by_user_type` BYTE,
`modification_proto` STRUCT<
  `previous_state`: INT,
  `new_state`: INT,
  `changed_fields`: ARRAY<
    STRUCT<
      `field_index`: BIGINT,
      `previous_value`: STRUCT<
        `media_upload_field_input`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `uuid`: STRING,
              `org_id`: BIGINT
            >
          >
        >,
        `string_field_input`: STRUCT<`value`: STRING>,
        `number_field_input`: STRUCT<`value`: DOUBLE>,
        `option_select_field_input`: STRUCT<
          `option_values`: ARRAY<
            STRUCT<
              `value`: STRING,
              `selected`: BOOLEAN
            >
          >
        >,
        `signature_field_input`: STRUCT<
          `name`: STRING,
          `uuid`: STRING,
          `signed_at_ms`: BIGINT,
          `org_id`: BIGINT
        >,
        `date_time_field_input`: STRUCT<`date_time_ms`: BIGINT>,
        `notes_field_input`: STRUCT<`value`: STRING>,
        `scanned_document_field_input`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `uuid`: STRING,
              `org_id`: BIGINT
            >
          >
        >,
        `barcode_field_input`: STRUCT<
          `barcodes`: ARRAY<
            STRUCT<
              `barcode_value`: STRING,
              `barcode_type`: STRING
            >
          >
        >
      >,
      `new_value`: STRUCT<
        `media_upload_field_input`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `uuid`: STRING,
              `org_id`: BIGINT
            >
          >
        >,
        `string_field_input`: STRUCT<`value`: STRING>,
        `number_field_input`: STRUCT<`value`: DOUBLE>,
        `option_select_field_input`: STRUCT<
          `option_values`: ARRAY<
            STRUCT<
              `value`: STRING,
              `selected`: BOOLEAN
            >
          >
        >,
        `signature_field_input`: STRUCT<
          `name`: STRING,
          `uuid`: STRING,
          `signed_at_ms`: BIGINT,
          `org_id`: BIGINT
        >,
        `date_time_field_input`: STRUCT<`date_time_ms`: BIGINT>,
        `notes_field_input`: STRUCT<`value`: STRING>,
        `scanned_document_field_input`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `uuid`: STRING,
              `org_id`: BIGINT
            >
          >
        >,
        `barcode_field_input`: STRUCT<
          `barcodes`: ARRAY<
            STRUCT<
              `barcode_value`: STRING,
              `barcode_type`: STRING
            >
          >
        >
      >
    >
  >
>,
`_raw_modification_proto` STRING,
`date` STRING
