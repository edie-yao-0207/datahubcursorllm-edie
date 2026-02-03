"""SQL parameter detection and definition building utilities."""

import re


# Default parameter definitions for common SQL parameters
# These can be overridden in dashboard_config.json via the "parameters" section
DEFAULT_PARAMETERS = {
    "date": {
        "pattern": r":date\.(min|max)",
        "definition": {
            "displayName": "date",
            "keyword": "date",
            "dataType": "DATE",
            "complexType": "RANGE",
            "defaultSelection": {
                "range": {
                    "dataType": "DATE",
                    "min": {"value": "now-90d/d"},
                    "max": {"value": "now/d"},
                }
            },
        },
    },
    "limit": {
        "pattern": r":limit\b",
        "definition": {
            "displayName": "limit",
            "keyword": "limit",
            "dataType": "DECIMAL",
            "defaultSelection": {
                "values": {"dataType": "DECIMAL", "values": [{"value": "-1"}]}
            },
        },
    },
}


def build_param_definition(param_name: str, config: dict) -> dict:
    """Build a Databricks parameter definition from config.

    Args:
        param_name: The parameter name
        config: Config dict with optional keys:
            - type: "string" | "number" | "date" | "date_range" | "select" | "multi_select"
            - default: Default value (default: "" for string, "*" for multi_select)

    Returns:
        Databricks parameter definition dict
    """
    param_type = config.get("type", "string")
    
    # Multi-select defaults to "null" if not specified
    if param_type == "multi_select":
        default_value = config.get("default", "null")
    else:
        default_value = config.get("default", "")

    type_map = {
        "string": "STRING",
        "number": "DECIMAL",
        "date": "DATE",
        "date_range": "DATE",
        "select": "STRING",
        "multi_select": "STRING",
    }
    data_type = type_map.get(param_type, "STRING")

    definition = {
        "displayName": param_name,
        "keyword": param_name,
        "dataType": data_type,
    }

    # Add complexType for multi-select (array) parameters
    if param_type == "multi_select":
        definition["complexType"] = "MULTI"

    if param_type == "date_range":
        definition["complexType"] = "RANGE"
        min_val = config.get("default_min", "now-90d/d")
        max_val = config.get("default_max", "now/d")
        definition["defaultSelection"] = {
            "range": {
                "dataType": "DATE",
                "min": {"value": min_val},
                "max": {"value": max_val},
            }
        }
    else:
        definition["defaultSelection"] = {
            "values": {"dataType": data_type, "values": [{"value": str(default_value)}]}
        }

    return definition


def detect_sql_parameters(sql: str, custom_params: dict = None) -> dict:
    """Detect parameters used in SQL.

    Args:
        sql: The SQL query string
        custom_params: Optional dict of custom parameter definitions from config.
            Format: { "param_name": { "type": "string", "default": "value" } }

    Returns:
        Dict of parameter definitions for detected parameters
    """
    params = {}
    custom_params = custom_params or {}
    
    # Detect which parameters are used with array_contains (need MULTI type)
    # Pattern matches: array_contains(:param_name, ...) or array_contains(split(:param_name, ...
    array_params = set()
    for match in re.finditer(r'array_contains\s*\(\s*(?:split\s*\(\s*)?:(\w+)', sql, re.IGNORECASE):
        array_params.add(match.group(1))

    # Check for default parameters
    for param_name, param_info in DEFAULT_PARAMETERS.items():
        if re.search(param_info["pattern"], sql):
            params[param_name] = param_info["definition"].copy()

    # Check for custom parameters defined in config
    for param_name, param_config in custom_params.items():
        # Skip if already handled by defaults (unless explicitly overriding)
        pattern = param_config.get("pattern", rf":{param_name}\b")
        if re.search(pattern, sql):
            # Only use multi_select type if the SQL actually uses array_contains for this param
            effective_config = param_config.copy()
            if param_config.get("type") == "multi_select" and param_name not in array_params:
                # Downgrade to simple string if not used with array_contains
                effective_config["type"] = "string"
            params[param_name] = build_param_definition(param_name, effective_config)

    # Auto-detect any :param patterns not already defined
    # This catches custom parameters used in SQL even without config
    for match in re.finditer(r":(\w+)(?:\.\w+)?", sql):
        param_name = match.group(1)
        if param_name not in params:
            # Use config if available, otherwise use default STRING type
            if param_name in custom_params:
                effective_config = custom_params[param_name].copy()
                if effective_config.get("type") == "multi_select" and param_name not in array_params:
                    effective_config["type"] = "string"
                params[param_name] = build_param_definition(
                    param_name, effective_config
                )
            else:
                # Default to STRING type for unknown parameters
                params[param_name] = {
                    "displayName": param_name,
                    "keyword": param_name,
                    "dataType": "STRING",
                    "defaultSelection": {
                        "values": {"dataType": "STRING", "values": [{"value": ""}]}
                    },
                }

    return params

