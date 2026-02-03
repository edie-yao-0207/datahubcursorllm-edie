import json
import re
from collections import defaultdict

# Define the topics we want to use
required_topics = [
    "active CMs",
    "active VGs",
    "active devices",
    "activity",
    "activity log",
    "alerts",
    "cable voltage",
    "customer360",
    "devices",
    "diagnostics",
    "drivers",
    "ecu speed",
    "engine hours",
    "engine state",
    "firmware",
    "fuel level",
    "gateways",
    "hardware",
    "heartbeats",
    "incident",
    "licenses",
    "locations",
    "mixpanel",
    "odometer",
    "opportunity",
    "org config",
    "organizations",
    "safety events",
    "salesforce",
    "seatbelt",
    "vehicle name",
    "voltage",
    "widget",
    "zendesk",
]

# Path to datahub.json file
datahub_file = "datahub.json"

# Load datahub.json
with open(datahub_file, "r") as f:
    datahub_data = json.load(f)

# Create a semantic map with topics to tables
semantic_map = defaultdict(list)

# Define topic-specific keywords to help with matching
topic_keywords = {
    "active CMs": ["cm", "camera", "dashcam", "cm3x", "cm2x"],
    "active VGs": ["vg", "vehicle gateway", "gateway"],
    "zendesk": ["ticket", "support", "customer service", "help desk"],
    "org config": [
        "org setting",
        "organization config",
        "org configuration",
        "organization setting",
    ],
    "activity log": ["activity log", "audit", "user action", "log"],
    "engine hours": ["engine hour", "engine time", "engine duration"],
    "vehicle name": ["vehicle name", "vehicle label", "vehicle title"],
    "cable voltage": ["cable voltage", "voltage", "power", "electrical"],
}


# Function to check if a table is related to a topic
def is_related_to_topic(table_name, table_info, topic):
    # Check table name for relation to topic
    if re.search(r"\b" + re.escape(topic.lower()) + r"\b", table_name.lower()):
        return True

    # Check table description for relation to topic
    description = table_info.get("description", "").lower()
    if re.search(r"\b" + re.escape(topic.lower()) + r"s?\b", description):
        return True

    # Check schema fields for relation to topic
    schema = table_info.get("datahub_schema", {})
    for field_name, field_info in schema.items():
        if re.search(r"\b" + re.escape(topic.lower()) + r"\b", field_name.lower()):
            return True
        field_desc = field_info.lower()
        if re.search(r"\b" + re.escape(topic.lower()) + r"\b", field_desc):
            return True

    # Check with topic-specific keywords if available
    if topic in topic_keywords:
        for keyword in topic_keywords[topic]:
            # Check in table name
            if re.search(
                r"\b" + re.escape(keyword.lower()) + r"\b", table_name.lower()
            ):
                return True

            # Check in table description
            if re.search(r"\b" + re.escape(keyword.lower()) + r"\b", description):
                return True

            # Check in schema fields
            for field_name, field_info in schema.items():
                if re.search(
                    r"\b" + re.escape(keyword.lower()) + r"\b", field_name.lower()
                ):
                    return True
                field_desc = field_info.lower()
                if re.search(r"\b" + re.escape(keyword.lower()) + r"\b", field_desc):
                    return True

    return False


# Special case handling for certain topics
def handle_special_cases(table_name, table_info):
    # "active CMs" - tables related to Camera Modules that are active
    if any(
        keyword in table_name.lower()
        for keyword in ["cm", "camera", "dashcam", "cm3x", "cm2x"]
    ):
        if (
            "active" in table_name.lower()
            or "active" in table_info.get("description", "").lower()
        ):
            semantic_map["active CMs"].append(table_name)
        # Also add tables that might contain CM health/status information
        elif any(
            term in table_name.lower()
            for term in ["health", "status", "connected", "recording", "uptime"]
        ):
            semantic_map["active CMs"].append(table_name)

    # "active VGs" - tables related to Vehicle Gateways that are active
    if any(keyword in table_name.lower() for keyword in ["vg", "gateway", "vehicle"]):
        if (
            "active" in table_name.lower()
            or "active" in table_info.get("description", "").lower()
        ):
            semantic_map["active VGs"].append(table_name)
        # Also add tables that might contain VG health/status information
        elif any(
            term in table_name.lower()
            for term in ["health", "status", "connected", "uptime"]
        ):
            semantic_map["active VGs"].append(table_name)

    # Additional specific cases for topics with no tables
    # Zendesk
    if (
        "ticket" in table_name.lower()
        or "zendesk" in table_name.lower()
        or "support" in table_name.lower()
    ):
        semantic_map["zendesk"].append(table_name)

    # org config
    if "org_setting" in table_name.lower() or "org_config" in table_name.lower():
        semantic_map["org config"].append(table_name)
    elif "settings" in table_name.lower() and "organization" in table_name.lower():
        semantic_map["org config"].append(table_name)

    # activity log
    if (
        "audit" in table_name.lower()
        or "activity" in table_name.lower()
        or "log" in table_name.lower()
    ):
        semantic_map["activity log"].append(table_name)

    # engine hours
    if "engine" in table_name.lower() and any(
        term in table_name.lower() for term in ["hour", "time", "duration"]
    ):
        semantic_map["engine hours"].append(table_name)
    elif "engine" in table_name.lower() and "stat" in table_name.lower():
        semantic_map["engine hours"].append(table_name)

    # vehicle name
    if "vehicle" in table_name.lower() and any(
        term in table_name.lower() for term in ["name", "label", "info"]
    ):
        semantic_map["vehicle name"].append(table_name)

    # cable voltage
    if (
        "voltage" in table_name.lower()
        or "power" in table_name.lower()
        or "electrical" in table_name.lower()
    ):
        semantic_map["cable voltage"].append(table_name)
    elif "battery" in table_name.lower() and "volt" in table_name.lower():
        semantic_map["cable voltage"].append(table_name)


# Add some manually curated mappings for hard-to-match topics
manual_mappings = {
    "active CMs": [
        "dataprep_safety.cm_recording_durations",
        "dataprep_safety.cm_device_health_daily",
        "dataprep_safety.cm_device_health_intervals",
        "cm_health_report.cm_2x_3x_linked_vgs",
        "datamodel_core.lifetime_device_activity",
        "datamodel_core.lifetime_device_online",
        "dataengineering.device_dormancy_global",
    ],
    "active VGs": [
        "dataprep.active_devices",
        "datamodel_core.lifetime_device_activity",
        "datamodel_core.lifetime_device_online",
        "dataengineering.device_dormancy_global",
    ],
    "activity log": [
        "datastreams.mobile_logs",
        "datastreams.api_logs",
        "auditlog.fct_datahub_events",
    ],
    "zendesk": [
        "datastreams.webhook_logs",  # May contain webhook data related to Zendesk
        "dataengineering.cloud_dashboard_user_activity_global",  # May include support activity
    ],
    "org config": [
        "datamodel_core.dim_organizations_settings",
        "dataprep.customer_metadata",
    ],
    "engine hours": ["kinesisstats.osdengineseconds", "engine_state.intervals"],
    "vehicle name": [
        "datamodel_telematics.fct_trips",  # Likely contains vehicle names
        "datamodel_core.dim_devices",  # May contain vehicle naming info
    ],
    "cable voltage": [
        "kinesisstats.osdbatteryinfo",
        "kinesisstats.osdauxiliaryvoltages",
        "kinesisstats.osdevhighcapacitybatterymillivolt",
    ],
}

# Process each table in datahub.json
for table_name, table_info in datahub_data.items():
    # Handle special cases first
    handle_special_cases(table_name, table_info)

    # Check if the table is related to any of the topics
    for topic in required_topics:
        if is_related_to_topic(table_name, table_info, topic):
            semantic_map[topic].append(table_name)

# Add manual mappings for topics that might still have no tables
for topic, tables in manual_mappings.items():
    for table in tables:
        if table in datahub_data:  # Make sure the table actually exists
            semantic_map[topic].append(table)

# Sort the tables in each topic and remove duplicates
for topic in semantic_map:
    semantic_map[topic] = sorted(list(set(semantic_map[topic])))

# Add datamodel_core.lifetime_device_activity to active devices if not already there
if "datamodel_core.lifetime_device_activity" not in semantic_map["active devices"]:
    semantic_map["active devices"].append("datamodel_core.lifetime_device_activity")
semantic_map["active devices"] = sorted(semantic_map["active devices"])

# Convert defaultdict to regular dict for JSON serialization
semantic_map_dict = dict(semantic_map)

# Check if we have all required topics
for topic in required_topics:
    if topic not in semantic_map_dict or not semantic_map_dict[topic]:
        print(f"Warning: Topic '{topic}' has no associated tables.")

# Write the semantic map to a file
with open("semantic_map.json", "w") as f:
    json.dump(semantic_map_dict, f, indent=2)

print("Semantic map created and saved to semantic_map.json")
