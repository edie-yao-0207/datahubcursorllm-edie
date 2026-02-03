`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`form_submission_uuid` STRING,
`proto` STRUCT<
  `org_id`: BIGINT,
  `form_submission_uuid`: BINARY,
  `form_template_uuid`: BINARY,
  `form_template_revision_uuid`: BINARY,
  `field_inputs`: ARRAY<
    STRUCT<
      `text_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<`value`: STRING>,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `number_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<`value`: DOUBLE>,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `date_time_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<
          `time_ms`: BIGINT,
          `timezone`: STRING
        >,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `single_select_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<`selected_option_uuid`: BINARY>,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `multi_select_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<
          `selected_options_uuids`: ARRAY<BINARY>
        >,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `person_field_input`: STRUCT<
        `field_uuid`: BINARY,
        `notes`: STRING,
        `value`: STRUCT<
          `polymorphic_user_id`: STRING,
          `manual_entry`: STRING
        >,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `signature_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<
          `media_item`: STRUCT<
            `uuid`: BINARY,
            `client_asset_id_override`: BINARY,
            `namespace_override`: INT,
            `captured_at_ms`: BIGINT
          >,
          `signed_by_polymorphic`: STRING
        >
      >,
      `media_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<
          `media_items`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >
        >
      >,
      `asset_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<
          `asset_id`: BIGINT,
          `asset_name`: STRING,
          `asset_location`: STRUCT<
            `location`: STRUCT<
              `latitude`: DOUBLE,
              `longitude`: DOUBLE
            >,
            `timeMs`: BIGINT
          >
        >
      >,
      `address_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<
          `address_id`: BIGINT,
          `manually_entered_address_name`: STRING
        >
      >,
      `relationship_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<
          `object_type`: INT,
          `object_id`: STRING
        >
      >,
      `entity_field_input`: STRUCT<
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >,
        `value`: STRUCT<`filler_for_databricks`: BOOLEAN>
      >,
      `tag_field_input`: STRUCT<
        `value`: STRUCT<
          `tag_ids`: ARRAY<BIGINT>,
          `tag_names`: ARRAY<STRING>
        >,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >,
      `attribute_field_input`: STRUCT<
        `value`: STRUCT<
          `attribute_value_uuids`: ARRAY<STRING>,
          `attribute_value_names`: ARRAY<STRING>
        >,
        `common`: STRUCT<
          `field_uuid`: BINARY,
          `notes`: STRING,
          `media_attachments`: ARRAY<
            STRUCT<
              `uuid`: BINARY,
              `client_asset_id_override`: BINARY,
              `namespace_override`: INT,
              `captured_at_ms`: BIGINT
            >
          >,
          `auto_fill_info`: STRUCT<
            `type`: INT,
            `auto_filled_at_ms`: BIGINT,
            `manually_overridden`: BOOLEAN,
            `source_fields`: ARRAY<
              STRUCT<`field_uuid`: BINARY>
            >,
            `source_entities`: STRUCT<
              `entities`: ARRAY<
                STRUCT<
                  `entity_name`: STRING,
                  `entity_id`: STRING
                >
              >
            >
          >,
          `key`: STRING,
          `readonly`: BOOLEAN
        >
      >
    >
  >,
  `has_resolved_field_keys`: BOOLEAN
>,
`_raw_proto` STRING,
`partition` STRING
