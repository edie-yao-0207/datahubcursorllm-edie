`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`gateway_id` BIGINT,
`updated_at` TIMESTAMP,
`enabled_ffs_hash` STRING,
`enabled_feature_flag_values` STRUCT<
  `values`: ARRAY<
    STRUCT<
      `key`: STRING,
      `value`: STRING
    >
  >
>,
`_raw_enabled_feature_flag_values` STRING,
`partition` STRING
