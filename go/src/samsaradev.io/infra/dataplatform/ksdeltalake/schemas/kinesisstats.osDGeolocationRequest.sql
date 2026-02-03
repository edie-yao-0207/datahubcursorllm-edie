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
    `geolocation_request`: STRUCT<
      `gps`: STRUCT<
        `latitude_nd`: BIGINT,
        `longitude_nd`: BIGINT
      >,
      `cellular`: STRUCT<
        `towers`: ARRAY<
          STRUCT<
            `cell_id`: DECIMAL(20, 0),
            `area_code`: BIGINT,
            `mcc`: BIGINT,
            `mnc`: BIGINT,
            `rsrp_dbm`: INT,
            `radio_type`: INT
          >
        >
      >,
      `wifi`: STRUCT<
        `access_points`: ARRAY<
          STRUCT<
            `ssid`: STRING,
            `bssid`: DECIMAL(20, 0),
            `rssi_dbm`: INT,
            `channel`: BIGINT
          >
        >
      >,
      `gps_scan`: BOOLEAN,
      `cellular_scan`: BOOLEAN,
      `wifi_scan`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
