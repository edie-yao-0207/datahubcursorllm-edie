import json
import re
from collections import defaultdict
from datetime import datetime, timedelta

import numpy
import numpy as np
import pandas as pd
import requests
from dagster import (
    AssetDep,
    AssetKey,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    LastPartitionMapping,
    Nothing,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    date_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    TableType,
    WarehouseWriteMode,
    apply_db_overrides,
    build_table_description,
    get_code_location,
)

GROUP_NAME = "fct_weather_alerts"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
DAILY_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-06-15", end_offset=1)

databases = {
    "database_silver": Database.DATAMODEL_TELEMATICS_SILVER,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)


@asset(
    name="stg_weather_alerts",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    description="Staging table for weather alerts fetched from the National Weather Service (NWS) API (api.weather.gov). Each row represents a single weather alert in CAP (Common Alerting Protocol) format. Data is fetched for land-based regions covering the past 2 days. Refreshed daily with overwrite mode.",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "code_location": get_code_location(),
        "schema": [
            {
                "name": "date",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": date_default_description},
            },
            {
                "name": "id",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Unique alert identifier (URN) from the NWS API, with the 'https://api.weather.gov/alerts/' prefix stripped."
                },
            },
            {
                "name": "geometry",
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "type",
                            "type": "string",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "coordinates",
                            "type": {
                                "type": "array",
                                "elementType": {
                                    "type": "array",
                                    "elementType": {
                                        "type": "array",
                                        "elementType": "double",
                                        "containsNull": True,
                                    },
                                    "containsNull": True,
                                },
                                "containsNull": True,
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                    ],
                },
                "nullable": True,
                "metadata": {
                    "comment": "GeoJSON geometry object defining the alert area. Contains nested fields: 'type' (e.g., 'Polygon', 'MultiPolygon') and 'coordinates' (nested array of coordinate rings where each ring is an array of [longitude, latitude] pairs in WGS84 decimal degrees). NULL if the alert has no explicit geometry."
                },
            },
            {
                "name": "geometry_type",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "GeoJSON geometry type extracted from geometry.type for convenience. Values: 'Polygon', 'MultiPolygon', or empty string if no geometry is provided."
                },
            },
            {
                "name": "coordinates",
                "type": {
                    "type": "array",
                    "elementType": {
                        "type": "array",
                        "elementType": "double",
                        "containsNull": True,
                    },
                    "containsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "Flattened array of [longitude, latitude] coordinate pairs in WGS84 decimal degrees for the alert area boundary. Note: follows GeoJSON convention (longitude first, NOT latitude). Extracted from geometry.coordinates for simpler access. Empty array [[]] if no geometry."
                },
            },
            {
                "name": "area_desc",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Human-readable description of the affected geographic area. Typically includes county names, zones, or regions (e.g., 'Alameda; Contra Costa; San Francisco')."
                },
            },
            {
                "name": "geocode",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {
                        "type": "array",
                        "elementType": "string",
                        "containsNull": True,
                    },
                    "valueContainsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "Map of geocode systems to their codes for the affected area. Keys are geocode types (e.g., 'UGC' for Universal Geographic Code, 'SAME' for Specific Area Message Encoding). Values are arrays of codes within that system."
                },
            },
            {
                "name": "affected_zones",
                "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "Array of NWS zone URLs (api.weather.gov/zones/...) affected by this alert. Each URL identifies a specific forecast zone."
                },
            },
            {
                "name": "references",
                "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "Array of references to related alerts (e.g., previous versions of this alert being updated/cancelled). Each reference contains identifier, sender, and sent timestamp. Empty array if no references."
                },
            },
            {
                "name": "sent",
                "type": "timestamp",
                "nullable": True,
                "metadata": {
                    "comment": "Timestamp when the alert was originally sent/issued by the NWS. Timezone-aware (UTC)."
                },
            },
            {
                "name": "effective",
                "type": "timestamp",
                "nullable": True,
                "metadata": {
                    "comment": "Timestamp when the alert becomes effective. Timezone-aware (UTC). Often the same as 'sent' for immediate alerts."
                },
            },
            {
                "name": "onset",
                "type": "timestamp",
                "nullable": True,
                "metadata": {
                    "comment": "Expected timestamp when the hazardous conditions are expected to begin. Timezone-aware (UTC). NULL if the onset time is unknown or the alert is already in effect."
                },
            },
            {
                "name": "expires",
                "type": "timestamp",
                "nullable": True,
                "metadata": {
                    "comment": "Timestamp when the alert information expires and should no longer be displayed. Timezone-aware (UTC). After this time, the alert is considered stale."
                },
            },
            {
                "name": "ends",
                "type": "timestamp",
                "nullable": True,
                "metadata": {
                    "comment": "Expected timestamp when the hazardous conditions are expected to end. Timezone-aware (UTC). NULL if the end time is unknown or indefinite."
                },
            },
            {
                "name": "message_type",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP message type indicating the nature of this alert message. Values: 'Alert' (new alert), 'Update' (updated alert), 'Cancel' (cancellation of previous alert)."
                },
            },
            {
                "name": "category",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP alert category. Values: 'Met' (meteorological). All alerts from NWS are meteorological."
                },
            },
            {
                "name": "severity",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP severity level indicating the potential impact. Values in decreasing severity: 'Extreme' (extraordinary threat to life/property), 'Severe' (significant threat), 'Moderate' (possible threat), 'Minor' (minimal threat), 'Unknown' (severity unknown)."
                },
            },
            {
                "name": "certainty",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP certainty level indicating confidence in the occurrence. Values in decreasing certainty: 'Observed' (already occurring), 'Likely' (>50% probability), 'Possible' (probability <= 50%), 'Unknown' (certainty unknown)."
                },
            },
            {
                "name": "urgency",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP urgency level indicating time-sensitivity of the alert. Values in decreasing urgency: 'Immediate' (responsive action should be taken now), 'Expected' (action should be taken within the next hour), 'Future' (action should be taken in the near future), 'Past' (event has ended), 'Unknown' (urgency unknown)."
                },
            },
            {
                "name": "event",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Type of weather event (e.g., 'Tornado Warning', 'Flash Flood Watch', 'Winter Storm Warning', 'Heat Advisory', 'Red Flag Warning'). Full list of ~100+ event types defined by NWS."
                },
            },
            {
                "name": "sender",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Email address or identifier of the NWS office that issued the alert (e.g., 'w-nws.webmaster@noaa.gov')."
                },
            },
            {
                "name": "sender_name",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Human-readable name of the NWS office that issued the alert (e.g., 'NWS Spokane WA', 'NWS Honolulu HI')."
                },
            },
            {
                "name": "headline",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Short summary headline for the alert (e.g., 'Red Flag Warning issued August 6 at 8:04PM PDT until August 7 at 8:00PM PDT'). May indicate cancellation status."
                },
            },
            {
                "name": "description",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Detailed text description of the weather event, including conditions, timing, and potential impacts. May contain multiple paragraphs."
                },
            },
            {
                "name": "instruction",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Recommended protective actions and safety instructions for the public (e.g., 'Move to higher ground immediately'). NULL if no specific instructions are provided."
                },
            },
            {
                "name": "response",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "CAP recommended response type. Values: 'Shelter' (take shelter), 'Evacuate' (relocate), 'Prepare' (make preparations), 'Execute' (execute pre-planned response), 'Avoid' (avoid the area), 'Monitor' (monitor conditions), 'AllClear' (threat has passed), 'None' (no specific response). NULL if not specified."
                },
            },
        ],
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
)
def stg_weather_alerts(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    base_url = "https://api.weather.gov/alerts"
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=2)
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {
        "start": start_time_str,
        "end": end_time_str,
        "region_type": "land",
    }

    def fetch_alerts(base_url, params):
        alerts = []
        while True:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                alerts = alerts + data["features"]
                # Check if there is a next page
                next_url = data.get("pagination", {}).get("next")
                if next_url:
                    base_url = next_url
                    params = {}
                else:
                    break
            else:
                context.log.info(
                    f"Failed to retrieve alerts. HTTP Status code: {response.status_code}"
                )
                context.log.info(response.text)
                break
        return alerts

    alerts = fetch_alerts(base_url, params)
    unnested_list = []
    for alert in alerts:
        row = {}
        row["date"] = partition_date_str
        row["id"] = alert["id"].replace("https://api.weather.gov/alerts/", "")
        row["geometry_type"] = ""
        row["coordinates"] = [[]]
        row["geometry"] = {}
        if alert["geometry"] != None:
            row["geometry_type"] = alert["geometry"]["type"]
            if len(alert["geometry"]["coordinates"]) > 0:
                coordinates = (
                    np.array(alert["geometry"]["coordinates"][0], dtype=object)
                    .reshape(-1, 2)
                    .tolist()
                )
                coordinates = [
                    [float(coord) for coord in coords] for coords in coordinates
                ]
                row["coordinates"] = coordinates
                row["geometry"] = {
                    "type": row["geometry_type"],
                    "coordinates": [coordinates],
                }
        row["type"] = alert["properties"]["@type"]
        row["area_desc"] = alert["properties"]["areaDesc"]
        row["geocode"] = alert["properties"]["geocode"]
        row["affected_zones"] = alert["properties"]["affectedZones"]
        row["references"] = alert["properties"]["references"]
        if alert["properties"]["sent"] is not None:
            row["sent"] = datetime.strptime(
                alert["properties"]["sent"], "%Y-%m-%dT%H:%M:%S%z"
            )
        if alert["properties"]["effective"] is not None:
            row["effective"] = datetime.strptime(
                alert["properties"]["effective"], "%Y-%m-%dT%H:%M:%S%z"
            )
        if alert["properties"]["onset"] is not None:
            row["onset"] = datetime.strptime(
                alert["properties"]["onset"], "%Y-%m-%dT%H:%M:%S%z"
            )
        if alert["properties"]["expires"] is not None:
            row["expires"] = datetime.strptime(
                alert["properties"]["expires"], "%Y-%m-%dT%H:%M:%S%z"
            )
        if alert["properties"]["ends"] is not None:
            row["ends"] = datetime.strptime(
                alert["properties"]["ends"], "%Y-%m-%dT%H:%M:%S%z"
            )
        row["status"] = alert["properties"]["status"]
        row["message_type"] = alert["properties"]["messageType"]
        row["category"] = alert["properties"]["category"]
        row["severity"] = alert["properties"]["severity"]
        row["certainty"] = alert["properties"]["certainty"]
        row["urgency"] = alert["properties"]["urgency"]
        row["event"] = alert["properties"]["event"]
        row["sender"] = alert["properties"]["sender"]
        row["sender_name"] = alert["properties"]["senderName"]
        row["headline"] = alert["properties"]["headline"]
        row["description"] = alert["properties"]["description"]
        row["instruction"] = alert["properties"]["instruction"]
        row["response"] = alert["properties"]["response"]
        unnested_list.append(row)

    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("id", StringType(), True),
            StructField(
                "geometry",
                StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField(
                            "coordinates",
                            ArrayType(ArrayType(ArrayType(DoubleType()))),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField("geometry_type", StringType(), True),
            StructField("coordinates", ArrayType(ArrayType(DoubleType())), True),
            StructField("area_desc", StringType(), True),
            StructField(
                "geocode", MapType(StringType(), ArrayType(StringType())), True
            ),
            StructField("affected_zones", ArrayType(StringType()), True),
            StructField("references", ArrayType(StringType()), True),
            StructField("sent", TimestampType(), True),
            StructField("effective", TimestampType(), True),
            StructField("onset", TimestampType(), True),
            StructField("expires", TimestampType(), True),
            StructField("ends", TimestampType(), True),
            StructField("message_type", StringType(), True),
            StructField("category", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("certainty", StringType(), True),
            StructField("urgency", StringType(), True),
            StructField("event", StringType(), True),
            StructField("sender", StringType(), True),
            StructField("sender_name", StringType(), True),
            StructField("headline", StringType(), True),
            StructField("description", StringType(), True),
            StructField("instruction", StringType(), True),
            StructField("response", StringType(), True),
        ]
    )

    df = spark.createDataFrame(unnested_list, schema=schema)
    context.log.info(df)
    return df
