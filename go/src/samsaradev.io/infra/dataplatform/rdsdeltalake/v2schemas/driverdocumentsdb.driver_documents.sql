`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`driver_id` BIGINT,
`driver_created_at` TIMESTAMP,
`submission_latitude` DOUBLE,
`submission_longitude` DOUBLE,
`notes` STRING,
`proto` STRUCT<
  `field_inputs`: ARRAY<
    STRUCT<
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
  >,
  `template_version_number`: BIGINT
>,
`_raw_proto` STRING,
`server_created_at` TIMESTAMP,
`driver_document_template_uuid` STRING,
`dispatch_job_id` BIGINT,
`state` INT,
`server_updated_at` TIMESTAMP,
`uuid` STRING,
`name` STRING,
`date` STRING
