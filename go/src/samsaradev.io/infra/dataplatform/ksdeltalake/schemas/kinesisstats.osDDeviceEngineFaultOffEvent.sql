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
    `vehicle_fault_edge_event`: STRUCT<
      `j1939_faults`: ARRAY<
        STRUCT<
          `fault_code`: STRUCT<
            `spn`: INT,
            `fmi`: INT
          >,
          `lamp`: INT,
          `data`: STRUCT<
            `tx_id`: INT,
            `mil_status`: INT,
            `red_lamp_status`: INT,
            `amber_lamp_status`: INT,
            `protect_lamp_status`: INT,
            `spn`: INT,
            `fmi`: INT,
            `occurance_count`: INT,
            `backend_only_j1939_fault_data`: STRUCT<
              `fmi_description`: STRING,
              `dtc_description`: STRING,
              `volvo_repair_instructions_url`: STRING
            >,
            `fault_protocol_source`: INT,
            `deprecated_spn_version`: BOOLEAN
          >
        >
      >,
      `passenger_faults`: ARRAY<
        STRUCT<
          `fault_code`: STRUCT<`dtc`: INT>,
          `lamp`: INT,
          `data`: STRUCT<
            `tx_id`: INT,
            `mil_status`: BOOLEAN,
            `dtcs`: ARRAY<INT>,
            `pending_dtcs`: ARRAY<INT>,
            `permanent_dtcs`: ARRAY<INT>,
            `monitor_status`: ARRAY<
              STRUCT<
                `name`: INT,
                `status`: INT
              >
            >,
            `ignition_type`: INT,
            `mil_valid`: BOOLEAN,
            `dtcs_valid`: BOOLEAN,
            `pending_dtcs_valid`: BOOLEAN,
            `permanent_dtcs_valid`: BOOLEAN,
            `monitor_status_valid`: BOOLEAN,
            `ignition_type_valid`: BOOLEAN,
            `fault_protocol_source`: INT,
            `dtcs_with_severity_and_class`: ARRAY<
              STRUCT<
                `dtc`: INT,
                `severity`: STRUCT<
                  `maintenance_only`: BOOLEAN,
                  `check_at_next_halt`: BOOLEAN,
                  `check_immediately`: BOOLEAN
                >,
                `class`: STRUCT<
                  `class_0`: BOOLEAN,
                  `class_1`: BOOLEAN,
                  `class_2`: BOOLEAN,
                  `class_3`: BOOLEAN,
                  `class_4`: BOOLEAN
                >
              >
            >,
            `pending_dtcs_with_severity_and_class`: ARRAY<
              STRUCT<
                `dtc`: INT,
                `severity`: STRUCT<
                  `maintenance_only`: BOOLEAN,
                  `check_at_next_halt`: BOOLEAN,
                  `check_immediately`: BOOLEAN
                >,
                `class`: STRUCT<
                  `class_0`: BOOLEAN,
                  `class_1`: BOOLEAN,
                  `class_2`: BOOLEAN,
                  `class_3`: BOOLEAN,
                  `class_4`: BOOLEAN
                >
              >
            >
          >
        >
      >,
      `oem_faults`: ARRAY<
        STRUCT<
          `fault_code`: STRUCT<
            `code_identifier`: STRING,
            `code_description`: STRING,
            `code_severity`: STRING,
            `code_source`: STRING
          >,
          `data`: STRUCT<
            `code_identifier`: STRING,
            `code_description`: STRING,
            `code_severity`: STRING,
            `code_source`: STRING
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
