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
    `victron_smart_shunt_battery_monitor_debug`: STRUCT<
      `state_of_charge_percentage`: FLOAT,
      `battery_voltage_millivolts`: INT,
      `battery_current_milliamps`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
