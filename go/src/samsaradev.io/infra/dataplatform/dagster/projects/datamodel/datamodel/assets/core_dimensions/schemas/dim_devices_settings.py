from ....common import constants

schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "A device's settings/configs in this table, will be the latest as of this date."
        },
    },
    {
        "name": "latest_config_date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The date of the latest config for the device."},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": constants.org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": constants.device_id_default_description},
    },
    {
        "name": "latest_config_time",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The UNIX timestamp of the latest config for the device."
        },
    },
    {
        "name": "public_product_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name of the product."},
    },
    {
        "name": "firmware_build",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The firmware build the device is running."},
    },
    {
        "name": "harsh_event_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "distracted_driving_detection",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "legacy_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not legacy harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {"comment": "The harsh event settings for the device."},
                },
                {
                    "name": "mobile_usage",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "legacy_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not legacy harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The mobile usage settings for the device."
                    },
                },
                {
                    "name": "seatbelt",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "legacy_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not legacy harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {"comment": "The seatbelt settings for the device."},
                },
                {
                    "name": "forward_collision_warning",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "legacy_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not legacy harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow events (events detected but not reported to the user) are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow events alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "model_registry_key",
                                "type": "string",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The model registry key for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The forward collision warning settings for the device."
                    },
                },
                {
                    "name": "following_distance",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "legacy_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not legacy harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The following distance settings for the device."
                    },
                },
                {
                    "name": "lane_departure_warning",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The lane departure warning settings for the device."
                    },
                },
                {
                    "name": "drowsiness",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "model_registry_key",
                                "type": "string",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The model registry key for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {"comment": "The drowsiness settings for the device."},
                },
                {
                    "name": "inward_safety",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "shadow_events_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "model_registry_key",
                                "type": "string",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The model registry key for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The inward safety settings for the device."
                    },
                },
                {
                    "name": "outward_safety",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "shadow_events_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events are enabled for the device."
                                },
                            },
                            {
                                "name": "shadow_events_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not shadow (detected but not reported) events alerts are enabled for the device."
                                },
                            },
                            {
                                "name": "model_registry_key",
                                "type": "string",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The model registry key for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The outward safety settings for the device."
                    },
                },
                {
                    "name": "tile_rolling_stop",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The tile rolling stop settings for the device."
                    },
                },
                {
                    "name": "rolling_stop",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": False,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The rolling stop settings for the device."
                    },
                },
                {
                    "name": "inward_obstruction",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The inward obstruction settings for the device. Inward obstruction refers to objects or conditions that block the driver-facing camera's view of the driver, such as a hand covering the lens or an object placed in front of the camera."
                    },
                },
                {
                    "name": "outward_obstruction",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "harsh_event_name",
                                "type": "string",
                                "nullable": False,
                                "metadata": {"comment": "The name of the harsh event."},
                            },
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not harsh event detection is enabled for the device."
                                },
                            },
                            {
                                "name": "audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for the device."
                                },
                            },
                        ],
                    },
                    "nullable": False,
                    "metadata": {
                        "comment": "The outward obstruction settings for the device. Outward obstruction refers to objects or conditions that block the road facing camera and obstruct the driver's view, such as a hand or an object blocking the camera's view."
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {"comment": "The harsh event settings for the device."},
    },
    {
        "name": "audio_alert_configs",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "event_type",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "The type of event that the audio alert is for."
                        },
                    },
                    {
                        "name": "severity",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {"comment": "The severity of the audio alert."},
                    },
                    {
                        "name": "enabled",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether or not the audio alert is enabled for the device."
                        },
                    },
                    {
                        "name": "audio_file_path",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "The path to the audio file for the audio alert."
                        },
                    },
                    {
                        "name": "volume",
                        "type": "long",
                        "nullable": True,
                        "metadata": {"comment": "The volume of the audio alert."},
                    },
                    {
                        "name": "priority",
                        "type": "long",
                        "nullable": True,
                        "metadata": {"comment": "The priority of the audio alert."},
                    },
                    {
                        "name": "repeat_config",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "enabled",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether or not the audio alert should repeat."
                                    },
                                },
                                {
                                    "name": "count",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The number of times the audio alert should repeat."
                                    },
                                },
                                {
                                    "name": "interval_ms",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The interval between repeats of the audio alert in milliseconds."
                                    },
                                },
                            ],
                        },
                        "nullable": True,
                        "metadata": {
                            "comment": "The repeat configuration for the audio alert."
                        },
                    },
                    {
                        "name": "is_voiceless",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether or not the audio alert is voiceless."
                        },
                    },
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {"comment": "The raw audio alert configurations for the device."},
    },
]
