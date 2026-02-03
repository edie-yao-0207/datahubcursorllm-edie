raw_productdb_devices_schema = [
    {
        "name": "_timestamp",
        "type": "timestamp",
        "nullable": True,
        "metadata": {},
    },
    {"name": "_filename", "type": "string", "nullable": True, "metadata": {}},
    {"name": "_rowid", "type": "string", "nullable": True, "metadata": {}},
    {"name": "id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {},
    },
    {"name": "lat", "type": "double", "nullable": True, "metadata": {}},
    {"name": "lng", "type": "double", "nullable": True, "metadata": {}},
    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
    {"name": "group_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "config_override_json",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {"name": "public_key", "type": "string", "nullable": True, "metadata": {}},
    {
        "name": "auto_sleep_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {"name": "product_id", "type": "long", "nullable": True, "metadata": {}},
    {"name": "obd_type", "type": "long", "nullable": True, "metadata": {}},
    {"name": "driver_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "chipset_serial",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {"name": "serial", "type": "string", "nullable": True, "metadata": {}},
    {"name": "order_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "samsara_note",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {"name": "user_note", "type": "string", "nullable": True, "metadata": {}},
    {"name": "digi1_type_id", "type": "long", "nullable": True, "metadata": {}},
    {"name": "digi2_type_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "ibeacon_major_minor",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "manual_odometer_meters",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "manual_odometer_updated_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "front_ultrasonic_widget_id",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "middle_ultrasonic_widget_id",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "back_ultrasonic_widget_id",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {"name": "vin", "type": "string", "nullable": True, "metadata": {}},
    {"name": "make", "type": "string", "nullable": True, "metadata": {}},
    {"name": "model", "type": "string", "nullable": True, "metadata": {}},
    {"name": "year", "type": "long", "nullable": True, "metadata": {}},
    {"name": "imei", "type": "string", "nullable": True, "metadata": {}},
    {"name": "iccid", "type": "string", "nullable": True, "metadata": {}},
    {
        "name": "camera_last_connected_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "harsh_accel_setting",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "aobrd_mode_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "device_color",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "camera_serial",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "camera_product_id",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "camera_first_connected_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "manual_engine_hours",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "manual_engine_hours_updated_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {},
    },
    {"name": "is_ecm_vin", "type": "integer", "nullable": True, "metadata": {}},
    {
        "name": "quarantine_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "device_settings_proto",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "ethernet_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "ethernet_mode",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "static_ipv4_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "cidr_ip",
                                            "type": "string",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "default_gateway",
                                            "type": "string",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "route_subnet_traffic_only",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "dns_config",
                                "type": {
                                    "type": "array",
                                    "elementType": "string",
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "interface_num",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "router_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "dhcpd_config",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "static_leases",
                                                        "type": {
                                                            "type": "array",
                                                            "elementType": {
                                                                "type": "struct",
                                                                "fields": [
                                                                    {
                                                                        "name": "mac_address",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                    {
                                                                        "name": "ipv4",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                ],
                                                            },
                                                            "containsNull": True,
                                                        },
                                                        "nullable": True,
                                                        "metadata": {},
                                                    }
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "connectivity_check_interval_seconds",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "io_module_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "dio_configs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "pin_number",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "dio_config",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "module_id",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "ai_configs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "pin_number",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "ai_config",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "module_id",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "ao_configs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "pin_number",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "ao_config",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "value",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "value",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "module_id",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "plc_url",
                                "type": "string",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "logging_period_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "analog_threshold_mv",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "counter_configs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "counter_filter_us",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "module_id",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "programs_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "plc_programs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "program_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "program_hash",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "variables",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "machineInputId",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "machine_output_uuid",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "local_variable_id",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "gateway_io",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "pin_number",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "module_id",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "module_gateway_id",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "type",
                                                                "type": "integer",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "metadata",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "statType",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "objectType",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "objectId",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "formula",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "variable_type",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "program_uuid",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "audio_recording_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "mes_settings",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "mes_recipes",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "org_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "name",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "has_image",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "device_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "recipe_actions",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "start_actions",
                                                            "type": {
                                                                "type": "array",
                                                                "elementType": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "machine_output_uuid",
                                                                            "type": "binary",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "machine_output_value",
                                                                            "type": "double",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "machine_output_name",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "containsNull": True,
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "end_actions",
                                                            "type": {
                                                                "type": "array",
                                                                "elementType": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "machine_output_uuid",
                                                                            "type": "binary",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "machine_output_value",
                                                                            "type": "double",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "machine_output_name",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "containsNull": True,
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "image_hash",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "product_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "target_count_rate",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "rate_period_ms",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "unit_config",
                                                            "type": {
                                                                "type": "struct",
                                                                "fields": [
                                                                    {
                                                                        "name": "units",
                                                                        "type": {
                                                                            "type": "array",
                                                                            "elementType": {
                                                                                "type": "struct",
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "scale_factor",
                                                                                        "type": "long",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                    {
                                                                                        "name": "name",
                                                                                        "type": "string",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                ],
                                                                            },
                                                                            "containsNull": True,
                                                                        },
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                    {
                                                                        "name": "default_unit",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                    {
                                                                        "name": "reporting_unit",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                ],
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "machine_input_scale",
                                                "type": "double",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "units",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "scalar_configs",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "configs",
                                                            "type": {
                                                                "type": "array",
                                                                "elementType": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "machine_input_id",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "scale",
                                                                            "type": "double",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "units",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "line_config",
                                                                            "type": {
                                                                                "type": "struct",
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "line_id",
                                                                                        "type": "long",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                    {
                                                                                        "name": "line_name",
                                                                                        "type": "string",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                    {
                                                                                        "name": "target_count_rate",
                                                                                        "type": "long",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                    {
                                                                                        "name": "rate_period_ms",
                                                                                        "type": "long",
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                    {
                                                                                        "name": "data_inputs_scales",
                                                                                        "type": {
                                                                                            "type": "array",
                                                                                            "elementType": {
                                                                                                "type": "struct",
                                                                                                "fields": [
                                                                                                    {
                                                                                                        "name": "machine_input_id",
                                                                                                        "type": "long",
                                                                                                        "nullable": True,
                                                                                                        "metadata": {},
                                                                                                    },
                                                                                                    {
                                                                                                        "name": "scale",
                                                                                                        "type": "double",
                                                                                                        "nullable": True,
                                                                                                        "metadata": {},
                                                                                                    },
                                                                                                    {
                                                                                                        "name": "units",
                                                                                                        "type": "string",
                                                                                                        "nullable": True,
                                                                                                        "metadata": {},
                                                                                                    },
                                                                                                ],
                                                                                            },
                                                                                            "containsNull": True,
                                                                                        },
                                                                                        "nullable": True,
                                                                                        "metadata": {},
                                                                                    },
                                                                                ],
                                                                            },
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "containsNull": True,
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "mes_work_orders",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "org_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "name",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "recipe_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "quantity",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "complete",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "line",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "date_info",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "production_date",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "mes_lines",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "org_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "name",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "inputs",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "id",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "shift_schedule",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "shift_configs",
                                                            "type": {
                                                                "type": "array",
                                                                "elementType": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "key",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "relative_start_ms",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "duration_ms",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "day_of_week",
                                                                            "type": "integer",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "downtime_allowances",
                                                                            "type": {
                                                                                "type": "array",
                                                                                "elementType": {
                                                                                    "type": "struct",
                                                                                    "fields": [
                                                                                        {
                                                                                            "name": "reason",
                                                                                            "type": "string",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "duration_ms",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "count",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                    ],
                                                                                },
                                                                                "containsNull": True,
                                                                            },
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "scheduled_runs",
                                                                            "type": {
                                                                                "type": "array",
                                                                                "elementType": {
                                                                                    "type": "struct",
                                                                                    "fields": [
                                                                                        {
                                                                                            "name": "product_id",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "relative_start_ms",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "duration_ms",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "target_product_count",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                    ],
                                                                                },
                                                                                "containsNull": True,
                                                                            },
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "containsNull": True,
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "timezone",
                                                            "type": "string",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "form_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "sections",
                                                            "type": {
                                                                "type": "array",
                                                                "elementType": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "label",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "fields",
                                                                            "type": {
                                                                                "type": "array",
                                                                                "elementType": {
                                                                                    "type": "struct",
                                                                                    "fields": [
                                                                                        {
                                                                                            "name": "label",
                                                                                            "type": "string",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "input_id",
                                                                                            "type": "long",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "data_type",
                                                                                            "type": "integer",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "enum_options",
                                                                                            "type": {
                                                                                                "type": "array",
                                                                                                "elementType": {
                                                                                                    "type": "struct",
                                                                                                    "fields": [
                                                                                                        {
                                                                                                            "name": "name",
                                                                                                            "type": "string",
                                                                                                            "nullable": True,
                                                                                                            "metadata": {},
                                                                                                        }
                                                                                                    ],
                                                                                                },
                                                                                                "containsNull": True,
                                                                                            },
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                    ],
                                                                                },
                                                                                "containsNull": True,
                                                                            },
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "containsNull": True,
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "input_assignments",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "assignment_type",
                                                                "type": "integer",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "formula",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "input_sources",
                                                                "type": {
                                                                    "type": "array",
                                                                    "elementType": {
                                                                        "type": "struct",
                                                                        "fields": [
                                                                            {
                                                                                "name": "input_id",
                                                                                "type": "long",
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                            {
                                                                                "name": "variable",
                                                                                "type": "string",
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                        ],
                                                                    },
                                                                    "containsNull": True,
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "interval_ms",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "counter_type",
                                                                "type": "integer",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "minimum_downtime_ms",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "ig_tunnel_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "forwards",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "local_port",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "remote_gateway_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "remote_host",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "remote_port",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "secondary_ethernet_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "ethernet_mode",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "static_ipv4_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "cidr_ip",
                                            "type": "string",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "default_gateway",
                                            "type": "string",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "route_subnet_traffic_only",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "dns_config",
                                "type": {
                                    "type": "array",
                                    "elementType": "string",
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "interface_num",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "router_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "dhcpd_config",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "static_leases",
                                                        "type": {
                                                            "type": "array",
                                                            "elementType": {
                                                                "type": "struct",
                                                                "fields": [
                                                                    {
                                                                        "name": "mac_address",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                    {
                                                                        "name": "ipv4",
                                                                        "type": "string",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                ],
                                                            },
                                                            "containsNull": True,
                                                        },
                                                        "nullable": True,
                                                        "metadata": {},
                                                    }
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "connectivity_check_interval_seconds",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "machine_vision_snapshot_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "snapshot_id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "turing_io_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "mod_config",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "slot_num",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "mod_type",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "min_log_interval_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "slave_server_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "registers",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "data",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "type",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "object_type",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "object_stat",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "object_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "pin_number",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "machine_input_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "machine_output_uuid",
                                                            "type": "string",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "connections",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "protocol",
                                                                "type": "integer",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "rest_settings",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "url",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "key",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "modbus_settings",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "type",
                                                                            "type": "integer",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "slave_addr",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "code",
                                                                            "type": "integer",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "register",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "num_registers",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "data_type",
                                                                            "type": "integer",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "word_swap",
                                                                            "type": "boolean",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "byte_swap",
                                                                            "type": "boolean",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "mqtt_settings",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "slave_device_id",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "slave_pin_id",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "data_output_settings",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "machine_output_uuid",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "interval_ms",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "serial_cfg",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "baud_rate",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "data_bits",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "stop_bits",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "parity",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "set_rs485_bias",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "rs485_termination_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "protocol",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "port",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "enabled_protocols",
                                "type": {
                                    "type": "array",
                                    "elementType": "integer",
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "serial_port_protocol",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "modbus_rtu_slave_id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "serial_port",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "stat_scaler_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "stat_mappings",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "machine_input_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "object_stat_key",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "object_type",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "object_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "stat_type",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "formula",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "modbus_scale_factor",
                                                "type": "double",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "modbus_data_type",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "modbus_word_swap",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "modbus_byte_swap",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "is_remote",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "out_mappings",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "output_type",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "slave_pin_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "modbus_settings",
                                                            "type": {
                                                                "type": "struct",
                                                                "fields": [
                                                                    {
                                                                        "name": "unchanged_bits_mask",
                                                                        "type": "long",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                    {
                                                                        "name": "use_value_as_bool_for_all_bits",
                                                                        "type": "boolean",
                                                                        "nullable": True,
                                                                        "metadata": {},
                                                                    },
                                                                ],
                                                            },
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "local_variable_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        }
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "gateway_channel_analog_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "mode",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "module_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "gateway_channel_digital_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "module_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "uuid",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "rest_api_config",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "url",
                                                            "type": "string",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "variable_key",
                                                            "type": "string",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "device_id",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "label_value_mappings",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "label",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "value",
                                                                "type": "double",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "widget_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "use_standard_units_for_analog_data_outputs",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "voice_coaching_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "forward_collision_warning_audio_alerts_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "distracted_driving_detection_audio_alerts_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "digi_inputs",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "inputs",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "port",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "input",
                                                "type": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "input_type",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "registered_at",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "uuid",
                                                            "type": "string",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "use_gps_speed_for_auto_duty",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "unregulated_vehicle",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "forward_collision_warning_roi_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "vehicle_center_diff_x_px",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "horizon_line_percent",
                                "type": "float",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "oriented_harsh_event",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_accel_x_threshold_gs",
                                "type": "float",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "harsh_brake_x_threshold_gs",
                                "type": "float",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "harsh_turn_x_threshold_gs",
                                "type": "float",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "internal_low_power_mode_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "voice_coaching_speed_threshold_milliknots",
                    "type": "long",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "device_audio_language",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "voice_coaching_alert_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "seat_belt_unbuckled_alert_disabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "maximum_speed_alert_disabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "harsh_driving_alert_disabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "crash_alert_disabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "nvr_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "camera_devices",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "uuid",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "streams",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "uuid",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "rtsp_config",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "uri",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "username",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                        {
                                                                            "name": "password",
                                                                            "type": "string",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        },
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "serdes_config",
                                                                "type": {
                                                                    "type": "struct",
                                                                    "fields": [
                                                                        {
                                                                            "name": "device_port",
                                                                            "type": "long",
                                                                            "nullable": True,
                                                                            "metadata": {},
                                                                        }
                                                                    ],
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "id",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "extra_channels",
                                                                "type": {
                                                                    "type": "array",
                                                                    "elementType": {
                                                                        "type": "struct",
                                                                        "fields": [
                                                                            {
                                                                                "name": "channel",
                                                                                "type": "integer",
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                            {
                                                                                "name": "rtsp_config",
                                                                                "type": {
                                                                                    "type": "struct",
                                                                                    "fields": [
                                                                                        {
                                                                                            "name": "uri",
                                                                                            "type": "string",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "username",
                                                                                            "type": "string",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                        {
                                                                                            "name": "password",
                                                                                            "type": "string",
                                                                                            "nullable": True,
                                                                                            "metadata": {},
                                                                                        },
                                                                                    ],
                                                                                },
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                            {
                                                                                "name": "store_to_disk",
                                                                                "type": "integer",
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                            {
                                                                                "name": "id",
                                                                                "type": "long",
                                                                                "nullable": True,
                                                                                "metadata": {},
                                                                            },
                                                                        ],
                                                                    },
                                                                    "containsNull": True,
                                                                },
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "default_channel_id",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "mac_address",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "enable_audio",
                                                "type": "boolean",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "storage_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "cleanup_threshold_percentage",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "do_stream_with_initial_h264parse",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "nvr_streamer_with_h264parse_embedded_enabled",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "forward_collision_warning_safety_inbox_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "distracted_driving_detection_safety_inbox_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "vms_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "camera_devices",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "name",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "streams",
                                                "type": {
                                                    "type": "array",
                                                    "elementType": {
                                                        "type": "struct",
                                                        "fields": [
                                                            {
                                                                "name": "id",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "object_detection_confidence_min_percent_override",
                                                                "type": "long",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "generate_low_res_channel",
                                                                "type": "boolean",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                            {
                                                                "name": "uri",
                                                                "type": "string",
                                                                "nullable": True,
                                                                "metadata": {},
                                                            },
                                                        ],
                                                    },
                                                    "containsNull": True,
                                                },
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "camera_type",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "hostname",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "serial",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "username",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "password",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "mac_address",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "object_detection_confidence_min_percent_gateway_override",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "detection_analytics_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "upload_interval_ms",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "min_instance_interval_ms",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "enable_diagnostics",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "ipcamera_mananger_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "network_scan_config",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "enabled",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "scan_local_network",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "additional_netblocks",
                                                        "type": {
                                                            "type": "array",
                                                            "elementType": "string",
                                                            "containsNull": True,
                                                        },
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "scan_interval_seconds",
                                                        "type": "long",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "scan_timeout_seconds",
                                                        "type": "long",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "following_distance_safety_inbox_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "following_distance_audio_alerts_device_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "camera_height_meters",
                    "type": "float",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "safety",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "in_cab_speed_limit_alert_settings",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "enable_audio_alerts",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "kmph_over_limit_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "kmph_over_limit_threshold",
                                            "type": "float",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "percent_over_limit_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "percent_over_limit_threshold",
                                            "type": "float",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "time_before_alert_ms",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "recording",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "storage_hours_setting",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "vanishing_point_estimation",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "calibration_version",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "policy_violation_settings",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "smoking_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "phone_usage_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "food_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "drink_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "seatbelt_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "mask_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "audio_alert_settings",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "smoking_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "phone_usage_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "food_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "drink_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "seatbelt_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "mask_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "camera_obstruction_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "inward_obstruction_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "outward_obstruction_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "camera_obstruction_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "minimum_speed",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "inward_obstruction_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "outward_obstruction_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "in_cab_stop_sign_violation_alert_settings",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "enable_audio_alerts",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "stop_threshold_kmph",
                                            "type": "float",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "in_cab_railroad_crossing_violation_alert_settings",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "enable_audio_alerts",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "stop_threshold_kmph",
                                            "type": "float",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "livestream_settings",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "outward_camera_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "inward_camera_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "audio_stream_enabled",
                                            "type": "boolean",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "multicam",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "camera_info",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "stream_uuid",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "camera_rotation",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "stream_id",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "camera_monitor_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "default_stream_uuid",
                                            "type": "string",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "default_stream_id",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "vulcan_module_config",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "module_id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "module_type",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "ai_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "ai_chan_setting_pairs",
                                            "type": {
                                                "type": "array",
                                                "elementType": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "channel",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "setting",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "containsNull": True,
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "ao_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "ao_chan_setting_pairs",
                                            "type": {
                                                "type": "array",
                                                "elementType": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "channel",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "setting",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "containsNull": True,
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "minimum_log_interval",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "dio_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "dio_bank_setting_pairs",
                                            "type": {
                                                "type": "array",
                                                "elementType": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {
                                                            "name": "bank",
                                                            "type": "long",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                        {
                                                            "name": "setting",
                                                            "type": "integer",
                                                            "nullable": True,
                                                            "metadata": {},
                                                        },
                                                    ],
                                                },
                                                "containsNull": True,
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        }
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "sys_config",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "serial_config",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "mux_control",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "serial_mode",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "rs485_bias_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "rs485_termination_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "can_config",
                                            "type": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "mux_control",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "can_mode",
                                                        "type": "integer",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                    {
                                                        "name": "termination_enabled",
                                                        "type": "boolean",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    },
                                                ],
                                            },
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "industrial",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "ig15_octopus_cable_with_no_peripheral",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "vehicle",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "vehicle_type",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "gross_vehicle_weight",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {
                                            "name": "weight",
                                            "type": "long",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                        {
                                            "name": "unit",
                                            "type": "integer",
                                            "nullable": True,
                                            "metadata": {},
                                        },
                                    ],
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "replay_mode",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "recordings",
                                "type": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "sensor_type",
                                                "type": "integer",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "url",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "md5_hex",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "containsNull": True,
                                },
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "source_org_id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "source_device_id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "source_start_unix_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "device_sim_settings",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "sim_slot_failover_disabled",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {},
                            },
                            {
                                "name": "sim_slot_failover_disabled_set",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "syslog_retrieval",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "upload_syslogs_before_unix_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
            ],
        },
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "sleep_override",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {"name": "mpg_city", "type": "long", "nullable": True, "metadata": {}},
    {"name": "mpg_hw", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "fuel_capacity",
        "type": "double",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "is_multicam",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "asset_serial",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "carrier_name_override",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "carrier_address_override",
        "type": "string",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "carrier_us_dot_number_override",
        "type": "long",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "proto",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "LicensePlate",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                }
            ],
        },
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "sleep_override_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {"name": "cable_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "ignition_disabled",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {"name": "asset_type", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "enabled_for_mobile",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {"name": "order_uuid", "type": "string", "nullable": True, "metadata": {}},
    {"name": "variant_id", "type": "integer", "nullable": True, "metadata": {}},
    {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
    {
        "name": "camera_variant_id",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {
        "name": "contains_internal_rma_required",
        "type": "integer",
        "nullable": True,
        "metadata": {},
    },
    {"name": "partition", "type": "string", "nullable": True, "metadata": {}},
    {"name": "date", "type": "string", "nullable": False, "metadata": {}},
]
