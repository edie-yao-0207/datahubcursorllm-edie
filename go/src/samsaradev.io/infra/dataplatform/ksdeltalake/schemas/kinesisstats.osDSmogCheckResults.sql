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
    `smog_check_results`: STRUCT<
      `comm_protocol`: INT,
      `firmware_number`: STRING,
      `odometer_meters`: DECIMAL(20, 0),
      `ecu_smog_info`: ARRAY<
        STRUCT<
          `tx_id`: BIGINT,
          `ecu_name`: STRING,
          `vin`: STRING,
          `cal_id`: BINARY,
          `cvn`: BINARY,
          `engine_rpm`: DECIMAL(20, 0),
          `mil_on_dist_km`: BIGINT,
          `mil_on_runtime_min`: BIGINT,
          `time_since_dtcs_cleared_min`: BIGINT,
          `dlc_pin_voltage_milliv`: BIGINT,
          `dist_since_dtcs_cleared_km`: BIGINT,
          `num_warmups_since_dtcs_cleared`: BIGINT,
          `ecu_name_valid`: BOOLEAN,
          `vin_valid`: BOOLEAN,
          `cal_id_valid`: BOOLEAN,
          `cvn_valid`: BOOLEAN,
          `engine_rpm_valid`: BOOLEAN,
          `mil_on_dist_km_valid`: BOOLEAN,
          `mil_on_runtime_min_valid`: BOOLEAN,
          `time_since_dtcs_cleared_min_valid`: BOOLEAN,
          `dlc_pin_voltage_milliv_valid`: BOOLEAN,
          `dist_since_dtcs_cleared_km_valid`: BOOLEAN,
          `num_warmups_since_dtcs_cleared_valid`: BOOLEAN,
          `supported_pids`: ARRAY<BIGINT>,
          `supported_pids_valid`: BOOLEAN
        >
      >,
      `odometer_meters_valid`: BOOLEAN,
      `firmware_verison`: STRING,
      `gateway_serial`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
