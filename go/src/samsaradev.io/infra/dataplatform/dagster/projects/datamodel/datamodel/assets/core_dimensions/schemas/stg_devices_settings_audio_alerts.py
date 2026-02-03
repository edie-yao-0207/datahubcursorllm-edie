from ....common.constants import (
    device_id_default_description,
    org_id_default_description,
)

schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "latest_config_date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Latest date that config was set."},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "latest_config_time",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Latest time where config was set."},
    },
    {
        "name": "public_product_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Product name for device"},
    },
    {
        "name": "firmware_build",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Which firmware version is running"},
    },
    {
        "name": "audio_alerts",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "boolean",
            "valueContainsNull": True,
        },
        "nullable": False,
        "metadata": {"comment": "Configured audio alerts for device."},
    },
]
