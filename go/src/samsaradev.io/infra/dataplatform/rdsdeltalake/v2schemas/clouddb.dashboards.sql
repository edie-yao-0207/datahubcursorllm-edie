`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`group_id` BIGINT,
`id` BIGINT,
`name` STRING,
`config_proto` STRUCT<
  `components`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `name`: STRING,
      `component_type`: INT,
      `sources`: ARRAY<
        STRUCT<
          `machine_input`: STRUCT<`id`: BIGINT>,
          `gateway_digital_output`: STRUCT<
            `device_id`: BIGINT,
            `pin_number`: BIGINT
          >,
          `modbus_output`: STRUCT<
            `slave_device_id`: BIGINT,
            `function_code`: BIGINT,
            `address`: BIGINT,
            `message`: STRUCT<`value`: BIGINT>
          >,
          `machine_output`: STRUCT<`uuid`: BINARY>,
          `asset_report`: STRUCT<`uuid`: STRING>,
          `data_group`: STRUCT<`data_group_id`: BIGINT>,
          `label`: STRING
        >
      >,
      `position`: STRUCT<
        `top`: BIGINT,
        `left`: BIGINT,
        `height`: BIGINT,
        `width`: BIGINT,
        `zIndex`: BIGINT
      >,
      `configuration`: STRUCT<
        `single_value`: STRUCT<
          `comparator`: INT,
          `color_mappings`: ARRAY<
            STRUCT<
              `value`: BIGINT,
              `operation`: INT,
              `color`: STRING,
              `double_value`: STRUCT<`value`: DOUBLE>
            >
          >,
          `sparkline`: STRUCT<
            `duration_ms`: BIGINT,
            `increase_color`: STRING,
            `decrease_color`: STRING
          >,
          `threshold_colors`: STRUCT<
            `use_alert_threshold`: BOOLEAN,
            `alert_id`: BIGINT,
            `default_color`: STRUCT<`color`: INT>
          >
        >,
        `progress_bar`: STRUCT<
          `label_low`: STRING,
          `label_high`: STRING,
          `min`: BIGINT,
          `max`: BIGINT,
          `color_mappings`: ARRAY<
            STRUCT<
              `value`: BIGINT,
              `operation`: INT,
              `color`: STRING,
              `double_value`: STRUCT<`value`: DOUBLE>
            >
          >
        >,
        `symbol`: STRUCT<`type`: INT>,
        `text`: STRUCT<
          `text`: STRING,
          `color`: STRING
        >,
        `image`: STRUCT<`url`: STRING>,
        `single_status_light`: STRUCT<
          `color_mappings`: ARRAY<
            STRUCT<
              `value`: BIGINT,
              `operation`: INT,
              `color`: STRING,
              `double_value`: STRUCT<`value`: DOUBLE>
            >
          >
        >,
        `double_status_light`: STRUCT<
          `vertical`: BOOLEAN,
          `first_light_color`: STRUCT<
            `value`: BIGINT,
            `operation`: INT,
            `color`: STRING,
            `double_value`: STRUCT<`value`: DOUBLE>
          >,
          `second_light_color`: STRUCT<
            `value`: BIGINT,
            `operation`: INT,
            `color`: STRING,
            `double_value`: STRUCT<`value`: DOUBLE>
          >
        >,
        `toggle`: STRUCT<
          `type`: INT,
          `label_off`: STRING,
          `label_on`: STRING
        >,
        `button`: STRUCT<
          `type`: INT,
          `label`: STRING,
          `value`: BIGINT,
          `color`: STRING,
          `presets`: ARRAY<
            STRUCT<
              `label`: STRING,
              `value`: STRING
            >
          >
        >,
        `mes_line`: STRUCT<`line_id`: BIGINT>,
        `line_graph`: STRUCT<
          `inputs`: ARRAY<
            STRUCT<`id`: BIGINT>
          >,
          `second_axis_enabled`: BOOLEAN,
          `secondary_inputs`: ARRAY<
            STRUCT<`id`: BIGINT>
          >,
          `scale_axis_enabled`: BOOLEAN,
          `axis_scale_type`: INT,
          `data_groups`: STRUCT<
            `data_group_ids`: ARRAY<BIGINT>
          >,
          `secondary_data_groups`: STRUCT<
            `data_group_ids`: ARRAY<BIGINT>
          >
        >,
        `number_display`: STRUCT<`num_decimals`: BIGINT>,
        `bar_graph`: STRUCT<
          `inputs`: ARRAY<
            STRUCT<`id`: BIGINT>
          >,
          `bar_interval`: BIGINT,
          `data_groups`: STRUCT<
            `data_group_ids`: ARRAY<BIGINT>
          >
        >,
        `hmi_iframe`: STRUCT<
          `device_id`: BIGINT,
          `ip_address_and_port`: STRING
        >,
        `gauge`: STRUCT<
          `warning_threshold`: DOUBLE,
          `alarm_threshold`: DOUBLE,
          `low_warning_threshold`: DOUBLE,
          `low_alarm_threshold`: DOUBLE,
          `threshold_colors`: STRUCT<
            `use_alert_threshold`: BOOLEAN,
            `alert_id`: BIGINT,
            `default_color`: STRUCT<`color`: INT>
          >,
          `minimum_value`: DOUBLE
        >,
        `slider`: STRUCT<
          `min`: DOUBLE,
          `max`: DOUBLE
        >,
        `links`: STRUCT<
          `dashboard_link`: STRUCT<
            `dashboard_id`: BIGINT,
            `dynamic_dashboard_uuid`: STRING
          >,
          `asset_link`: STRUCT<`asset_uuid`: STRING>
        >,
        `embedded_report`: STRUCT<
          `report_type`: INT,
          `show_only_asset_data`: BOOLEAN
        >,
        `control_form`: STRUCT<`individual_buttons`: BOOLEAN>,
        `alarms`: STRUCT<`title`: STRING>
      >,
      `layout`: STRUCT<
        `x`: BIGINT,
        `y`: BIGINT,
        `width`: BIGINT,
        `height`: BIGINT
      >
    >
  >,
  `dashboard_type`: INT,
  `heightPixels`: BIGINT,
  `widthPixels`: BIGINT,
  `default_time_range`: STRUCT<
    `end_ms`: BIGINT,
    `duration_ms`: BIGINT,
    `preset`: INT
  >
>,
`_raw_config_proto` STRING,
`created_at` TIMESTAMP,
`created_by` BIGINT,
`updated_at` TIMESTAMP,
`updated_by` BIGINT,
`graphical_config_proto` STRUCT<
  `nodes`: ARRAY<
    STRUCT<
      `id`: STRING,
      `position`: STRUCT<
        `x`: BIGINT,
        `y`: BIGINT
      >,
      `properties`: STRUCT<
        `module_config`: STRUCT<
          `name`: STRING,
          `rows`: ARRAY<
            STRUCT<
              `row_id`: STRING,
              `components`: ARRAY<
                STRUCT<
                  `id`: STRING,
                  `config`: STRUCT<
                    `text_config`: STRUCT<
                      `value`: STRING,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >
                    >,
                    `meter_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `max`: BIGINT,
                      `threshold`: BIGINT,
                      `type`: INT,
                      `title`: STRING,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `show_title`: STRUCT<`show_title`: BOOLEAN>,
                      `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `tank_meter_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `max`: BIGINT,
                      `threshold`: BIGINT,
                      `title`: STRING,
                      `show_title`: BOOLEAN,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `line_chart_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `title`: STRING,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `y_axis_scale`: STRUCT<
                        `should_scale_y_axis`: BOOLEAN,
                        `axis_scale_type`: INT
                      >,
                      `show_title`: STRUCT<`show_title`: BOOLEAN>,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `sources`: STRUCT<
                        `data_input_ids`: ARRAY<BIGINT>,
                        `data_groups`: STRUCT<
                          `data_group_ids`: ARRAY<BIGINT>
                        >,
                        `source_labels`: STRUCT<
                          `labels`: ARRAY<STRING>
                        >,
                        `source_data_output_uuids`: ARRAY<STRING>
                      >,
                      `second_y_axis`: STRUCT<
                        `second_axis_enabled`: BOOLEAN,
                        `data_input_ids`: ARRAY<BIGINT>,
                        `data_groups`: STRUCT<
                          `data_group_ids`: ARRAY<BIGINT>
                        >
                      >
                    >,
                    `link_terminal_config`: STRUCT<
                      `text`: STRING,
                      `position`: STRING,
                      `type`: INT,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >
                    >,
                    `shutoff_valve_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `control_valve_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `single_value_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `show_title`: STRUCT<`show_title`: BOOLEAN>,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `title`: STRUCT<`title`: STRING>,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `discrete_chart_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `sources`: STRUCT<
                        `data_input_ids`: ARRAY<BIGINT>,
                        `data_groups`: STRUCT<
                          `data_group_ids`: ARRAY<BIGINT>
                        >,
                        `source_labels`: STRUCT<
                          `labels`: ARRAY<STRING>
                        >,
                        `source_data_output_uuids`: ARRAY<STRING>
                      >
                    >,
                    `bar_chart_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `interval_ms`: BIGINT,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `sources`: STRUCT<
                        `data_input_ids`: ARRAY<BIGINT>,
                        `data_groups`: STRUCT<
                          `data_group_ids`: ARRAY<BIGINT>
                        >,
                        `source_labels`: STRUCT<
                          `labels`: ARRAY<STRING>
                        >,
                        `source_data_output_uuids`: ARRAY<STRING>
                      >
                    >,
                    `dynamic_icon_config`: STRUCT<
                      `machine_input_id`: BIGINT,
                      `icon_type`: INT,
                      `title`: STRING,
                      `show_title`: BOOLEAN,
                      `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `gauge_config`: STRUCT<
                      `data_input_id`: BIGINT,
                      `max`: BIGINT,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `threshold_colors`: STRUCT<
                        `threshold_colors`: ARRAY<
                          STRUCT<
                            `threshold_value`: BIGINT,
                            `hex_color`: STRING,
                            `double_value`: STRUCT<`value`: FLOAT>
                          >
                        >
                      >,
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `alert_thresholds`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>,
                      `min`: DOUBLE
                    >,
                    `shared_config`: STRUCT<
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `asset_link`: STRUCT<`asset_uuid`: STRING>,
                      `font_size`: STRUCT<`value`: BIGINT>
                    >,
                    `toggle_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `data_output_uuid`: BINARY,
                      `label_on`: STRING,
                      `label_off`: STRING,
                      `output_source`: STRUCT<`data_output_uuid`: STRING>,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `button_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `data_output_uuid`: BINARY,
                      `output_source`: STRUCT<`data_output_uuid`: STRING>,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `dropdown_select_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `data_output_uuid`: BINARY,
                      `options`: ARRAY<STRING>,
                      `output_source`: STRUCT<`data_output_uuid`: STRING>,
                      `mapping_colors`: STRUCT<
                        `colors`: ARRAY<
                          STRUCT<
                            `value`: BIGINT,
                            `color`: STRING
                          >
                        >
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `table_config`: STRUCT<
                      `data_input_ids`: ARRAY<BIGINT>,
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `data_groups`: STRUCT<
                        `data_group_ids`: ARRAY<BIGINT>
                      >
                    >,
                    `slider_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `output_source`: STRUCT<`data_output_uuid`: STRING>,
                      `digits_of_precision`: STRUCT<
                        `round_enabled`: BOOLEAN,
                        `num_digits`: BIGINT
                      >,
                      `min`: FLOAT,
                      `max`: FLOAT,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `single_value_button_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `output_source`: STRUCT<`data_output_uuid`: STRING>,
                      `data_output_mapping`: STRUCT<
                        `label`: STRING,
                        `value`: FLOAT
                      >,
                      `custom_mapping`: STRUCT<
                        `label`: STRING,
                        `value`: FLOAT
                      >,
                      `data_group`: STRUCT<`data_group_id`: BIGINT>
                    >,
                    `control_form_config`: STRUCT<
                      `show_title`: BOOLEAN,
                      `title`: STRING,
                      `sources`: STRUCT<
                        `data_input_ids`: ARRAY<BIGINT>,
                        `data_groups`: STRUCT<
                          `data_group_ids`: ARRAY<BIGINT>
                        >,
                        `source_labels`: STRUCT<
                          `labels`: ARRAY<STRING>
                        >,
                        `source_data_output_uuids`: ARRAY<STRING>
                      >,
                      `individual_buttons`: BOOLEAN
                    >,
                    `alarms_config`: STRUCT<`title`: STRING>
                  >,
                  `type`: STRING,
                  `graphical_size`: STRUCT<
                    `width`: BIGINT,
                    `height`: BIGINT
                  >,
                  `orientation`: STRUCT<
                    `rotate_degrees`: BIGINT,
                    `flip_config`: STRUCT<
                      `flip_horizontal`: BOOLEAN,
                      `flip_vertical`: BOOLEAN
                    >
                  >
                >
              >
            >
          >,
          `source`: STRUCT<
            `machine_input`: STRUCT<`id`: BIGINT>
          >,
          `font_size`: STRUCT<`value`: BIGINT>
        >,
        `component_config`: STRUCT<
          `id`: STRING,
          `config`: STRUCT<
            `text_config`: STRUCT<
              `value`: STRING,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >
            >,
            `meter_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `max`: BIGINT,
              `threshold`: BIGINT,
              `type`: INT,
              `title`: STRING,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `show_title`: STRUCT<`show_title`: BOOLEAN>,
              `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `tank_meter_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `max`: BIGINT,
              `threshold`: BIGINT,
              `title`: STRING,
              `show_title`: BOOLEAN,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `line_chart_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `title`: STRING,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `y_axis_scale`: STRUCT<
                `should_scale_y_axis`: BOOLEAN,
                `axis_scale_type`: INT
              >,
              `show_title`: STRUCT<`show_title`: BOOLEAN>,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `sources`: STRUCT<
                `data_input_ids`: ARRAY<BIGINT>,
                `data_groups`: STRUCT<
                  `data_group_ids`: ARRAY<BIGINT>
                >,
                `source_labels`: STRUCT<
                  `labels`: ARRAY<STRING>
                >,
                `source_data_output_uuids`: ARRAY<STRING>
              >,
              `second_y_axis`: STRUCT<
                `second_axis_enabled`: BOOLEAN,
                `data_input_ids`: ARRAY<BIGINT>,
                `data_groups`: STRUCT<
                  `data_group_ids`: ARRAY<BIGINT>
                >
              >
            >,
            `link_terminal_config`: STRUCT<
              `text`: STRING,
              `position`: STRING,
              `type`: INT,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >
            >,
            `shutoff_valve_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `control_valve_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `single_value_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `show_title`: STRUCT<`show_title`: BOOLEAN>,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `title`: STRUCT<`title`: STRING>,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `discrete_chart_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `sources`: STRUCT<
                `data_input_ids`: ARRAY<BIGINT>,
                `data_groups`: STRUCT<
                  `data_group_ids`: ARRAY<BIGINT>
                >,
                `source_labels`: STRUCT<
                  `labels`: ARRAY<STRING>
                >,
                `source_data_output_uuids`: ARRAY<STRING>
              >
            >,
            `bar_chart_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `interval_ms`: BIGINT,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `sources`: STRUCT<
                `data_input_ids`: ARRAY<BIGINT>,
                `data_groups`: STRUCT<
                  `data_group_ids`: ARRAY<BIGINT>
                >,
                `source_labels`: STRUCT<
                  `labels`: ARRAY<STRING>
                >,
                `source_data_output_uuids`: ARRAY<STRING>
              >
            >,
            `dynamic_icon_config`: STRUCT<
              `machine_input_id`: BIGINT,
              `icon_type`: INT,
              `title`: STRING,
              `show_title`: BOOLEAN,
              `show_data_value`: STRUCT<`show_data_value`: BOOLEAN>,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `gauge_config`: STRUCT<
              `data_input_id`: BIGINT,
              `max`: BIGINT,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `threshold_colors`: STRUCT<
                `threshold_colors`: ARRAY<
                  STRUCT<
                    `threshold_value`: BIGINT,
                    `hex_color`: STRING,
                    `double_value`: STRUCT<`value`: FLOAT>
                  >
                >
              >,
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `alert_thresholds`: STRUCT<
                `use_alert_threshold`: BOOLEAN,
                `alert_id`: BIGINT,
                `default_color`: STRUCT<`color`: INT>
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>,
              `min`: DOUBLE
            >,
            `shared_config`: STRUCT<
              `dashboard_link`: STRUCT<
                `dashboard_id`: BIGINT,
                `dynamic_dashboard_uuid`: STRING
              >,
              `asset_link`: STRUCT<`asset_uuid`: STRING>,
              `font_size`: STRUCT<`value`: BIGINT>
            >,
            `toggle_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `data_output_uuid`: BINARY,
              `label_on`: STRING,
              `label_off`: STRING,
              `output_source`: STRUCT<`data_output_uuid`: STRING>,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `button_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `data_output_uuid`: BINARY,
              `output_source`: STRUCT<`data_output_uuid`: STRING>,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `dropdown_select_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `data_output_uuid`: BINARY,
              `options`: ARRAY<STRING>,
              `output_source`: STRUCT<`data_output_uuid`: STRING>,
              `mapping_colors`: STRUCT<
                `colors`: ARRAY<
                  STRUCT<
                    `value`: BIGINT,
                    `color`: STRING
                  >
                >
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `table_config`: STRUCT<
              `data_input_ids`: ARRAY<BIGINT>,
              `show_title`: BOOLEAN,
              `title`: STRING,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `data_groups`: STRUCT<
                `data_group_ids`: ARRAY<BIGINT>
              >
            >,
            `slider_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `output_source`: STRUCT<`data_output_uuid`: STRING>,
              `digits_of_precision`: STRUCT<
                `round_enabled`: BOOLEAN,
                `num_digits`: BIGINT
              >,
              `min`: FLOAT,
              `max`: FLOAT,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `single_value_button_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `output_source`: STRUCT<`data_output_uuid`: STRING>,
              `data_output_mapping`: STRUCT<
                `label`: STRING,
                `value`: FLOAT
              >,
              `custom_mapping`: STRUCT<
                `label`: STRING,
                `value`: FLOAT
              >,
              `data_group`: STRUCT<`data_group_id`: BIGINT>
            >,
            `control_form_config`: STRUCT<
              `show_title`: BOOLEAN,
              `title`: STRING,
              `sources`: STRUCT<
                `data_input_ids`: ARRAY<BIGINT>,
                `data_groups`: STRUCT<
                  `data_group_ids`: ARRAY<BIGINT>
                >,
                `source_labels`: STRUCT<
                  `labels`: ARRAY<STRING>
                >,
                `source_data_output_uuids`: ARRAY<STRING>
              >,
              `individual_buttons`: BOOLEAN
            >,
            `alarms_config`: STRUCT<`title`: STRING>
          >,
          `type`: STRING,
          `graphical_size`: STRUCT<
            `width`: BIGINT,
            `height`: BIGINT
          >,
          `orientation`: STRUCT<
            `rotate_degrees`: BIGINT,
            `flip_config`: STRUCT<
              `flip_horizontal`: BOOLEAN,
              `flip_vertical`: BOOLEAN
            >
          >
        >,
        `connected_to_graphql`: BOOLEAN
      >,
      `type`: STRING,
      `ports`: ARRAY<
        STRUCT<
          `id`: STRING,
          `type`: STRING,
          `position`: STRUCT<
            `x`: BIGINT,
            `y`: BIGINT
          >
        >
      >,
      `graphical_size`: STRUCT<
        `width`: BIGINT,
        `height`: BIGINT
      >
    >
  >,
  `links`: ARRAY<
    STRUCT<
      `id`: STRING,
      `source`: STRUCT<
        `node_port_config`: STRUCT<
          `node_id`: STRING,
          `port_id`: STRING
        >,
        `position`: STRUCT<
          `x`: BIGINT,
          `y`: BIGINT
        >
      >,
      `target`: STRUCT<
        `node_port_config`: STRUCT<
          `node_id`: STRING,
          `port_id`: STRING
        >,
        `position`: STRUCT<
          `x`: BIGINT,
          `y`: BIGINT
        >
      >,
      `properties`: STRUCT<
        `color`: INT,
        `type`: INT
      >
    >
  >,
  `layer_order`: STRUCT<
    `id_layer_order`: ARRAY<STRING>
  >,
  `grid_size`: STRUCT<`grid_size`: BIGINT>
>,
`_raw_graphical_config_proto` STRING,
`partition` STRING
