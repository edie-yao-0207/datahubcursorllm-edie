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
    `ev_charging_status_info`: STRUCT<
      `speed_kmph`: DECIMAL(20, 0),
      `charge_rate_kw`: BIGINT,
      `battery_current_amps`: BIGINT,
      `monitoring_state`: INT,
      `charge_in_progress_state`: INT,
      `charge_rate_state`: INT,
      `battery_current_state`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
