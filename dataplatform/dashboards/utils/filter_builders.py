"""Filter widget builders for dashboard filters."""

from .sql_utils import generate_id


def normalize_date_value(value: str) -> str:
    """Convert date shorthand to Databricks date format.
    
    Examples:
        "-60d" -> "now-60d/d"
        "today" -> "now/d"
        "now-30d/d" -> "now-30d/d" (already normalized)
    """
    if not value:
        return "now/d"
    
    # Already in Databricks format
    if value.startswith("now"):
        return value
    
    # Shorthand formats
    if value == "today":
        return "now/d"
    if value == "yesterday":
        return "now-1d/d"
    
    # Relative format like "-60d" or "-7d"
    if value.startswith("-") and value.endswith("d"):
        days = value[1:-1]  # Extract number
        return f"now-{days}d/d"
    
    # Otherwise return as-is (might be a literal date)
    return value


def build_filter_widget(
    filter_config: dict,
    position: dict,
    datasets_info: list,
    datasets: dict,
    named_datasets: dict,
    get_or_create_dataset_func,
) -> dict:
    """Build a filter widget based on config.

    Args:
        filter_config: Filter definition from global_filters config
        position: Widget position dict with x, y, width, height
        datasets_info: List of tuples (dataset_name, dataset_id) for datasets with this param
        datasets: Dict mapping sql_path -> dataset dict
        named_datasets: Dict mapping dataset_name -> sql_path
        get_or_create_dataset_func: Function to get or create a dataset (sql_path -> dataset_id)
    """
    param_name = filter_config.get("name", "date")
    filter_type = filter_config.get("type", "date_range")
    title = filter_config.get("title", param_name.replace("_", " ").title())

    # Handle field-based filters (filter on a column value, not SQL parameter)
    if filter_type in ("field_select", "field_multi_select", "range_slider"):
        return build_field_filter_widget(filter_config, position, datasets_info)

    # Build internal queries that link to each dataset
    queries = []
    fields = []
    
    # Check if filter has a dedicated lookup dataset for populating dropdown values
    lookup_dataset_name = filter_config.get("lookup_dataset")
    lookup_field = filter_config.get("lookup_field", param_name)  # Field name in lookup dataset
    
    if lookup_dataset_name:
        # Ensure the lookup dataset is loaded
        lookup_sql_path = named_datasets.get(lookup_dataset_name)
        if lookup_sql_path:
            lookup_ds_id = get_or_create_dataset_func(lookup_sql_path)
            if lookup_ds_id:
                # Add query to extract values from lookup dataset
                lookup_query_name = f"lookup_{lookup_ds_id}_{param_name}"
                queries.append(
                    {
                        "name": lookup_query_name,
                        "query": {
                            "datasetName": lookup_ds_id,
                            "fields": [
                                {"name": param_name, "expression": f"`{lookup_field}`"},
                                {
                                    "name": f"{param_name}_associativity",
                                    "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                },
                            ],
                            "disaggregated": False,
                        },
                    }
                )
                fields.append({"fieldName": param_name, "queryName": lookup_query_name})

    # Add parameter bindings for each dataset that uses this param
    for ds_name, ds_id in datasets_info:
        query_name = f"param_{ds_id}_{param_name}"

        # Check if this dataset uses the param as a SQL parameter
        # by looking at the parameters list in the dataset
        ds_info = datasets.get(ds_name, {})
        ds_params = {p.get("keyword"): p for p in ds_info.get("parameters", [])}
        uses_param = param_name in ds_params
        
        # For multi_select filters, only bind to datasets where the param has complexType MULTI
        # This prevents conflicts when widgets override parameters on datasets using simple equality
        if filter_type == "multi_select" and uses_param:
            param_def = ds_params.get(param_name, {})
            if param_def.get("complexType") != "MULTI":
                # Dataset uses simple equality for this param, skip binding
                continue

        if uses_param:
            # Dataset uses this as a SQL parameter - create parameter binding
            queries.append(
                {
                    "name": query_name,
                    "query": {
                        "datasetName": ds_id,
                        "parameters": [{"name": param_name, "keyword": param_name}],
                        "disaggregated": False,
                    },
                }
            )
            fields.append({"parameterName": param_name, "queryName": query_name})
        elif not lookup_dataset_name:
            # Dataset outputs this as a field - extract values for dropdown
            # Only do this if no dedicated lookup dataset is specified
            queries.append(
                {
                    "name": query_name,
                    "query": {
                        "datasetName": ds_id,
                        "fields": [
                            {"name": param_name, "expression": f"`{param_name}`"},
                            {
                                "name": f"{param_name}_associativity",
                                "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                            },
                        ],
                        "disaggregated": False,
                    },
                }
            )
            fields.append({"fieldName": param_name, "queryName": query_name})

    # Map filter type to widget type
    # Note: Databricks Lakeview doesn't support a text input filter widget.
    # Text filters fall back to filter-single-select (dropdown).
    widget_type_map = {
        "date_range": "filter-date-range-picker",
        "date": "filter-date-picker",
        "select": "filter-single-select",
        "multi_select": "filter-multi-select",
        "number": "filter-slider",
        # "text" intentionally omitted - Databricks doesn't support filter-text-input
    }
    widget_type = widget_type_map.get(filter_type, "filter-single-select")

    spec = {
        "version": 2,
        "widgetType": widget_type,
        "encodings": {"fields": fields},
        "frame": {"showTitle": True, "title": title},
    }

    # Add default value if specified (must be nested inside "selection" object)
    default_value = filter_config.get("default")
    if default_value is not None:
        if filter_type == "date_range":
            # Date range uses min/max values
            if isinstance(default_value, dict):
                min_val = default_value.get("min", "now-90d/d")
                max_val = default_value.get("max", "now/d")
            else:
                # Fallback for simple string default
                min_val = "now-90d/d"
                max_val = "now/d"
            # Convert shorthand formats to Databricks format
            min_val = normalize_date_value(min_val)
            max_val = normalize_date_value(max_val)
            spec["selection"] = {
                "defaultSelection": {
                    "range": {
                        "dataType": "DATE",
                        "min": {"value": min_val},
                        "max": {"value": max_val},
                    }
                }
            }
        elif filter_type == "multi_select":
            # Multi-select uses array of values
            values_list = default_value if isinstance(default_value, list) else [default_value]
            spec["selection"] = {
                "defaultSelection": {
                    "values": {
                        "dataType": "STRING",
                        "values": [{"value": str(v)} for v in values_list],
                    }
                }
            }
        elif filter_type in ("select", "text"):
            # Single select uses single value
            spec["selection"] = {
                "defaultSelection": {
                    "values": {"dataType": "STRING", "values": [{"value": str(default_value)}]}
                }
            }

    return {
        "name": generate_id(f"filter-{param_name}"),
        "position": position,
        "widget": {
            "name": generate_id(f"filter-{param_name}-widget"),
            "queries": queries,
            "spec": spec,
        },
    }


def build_field_filter_widget(
    filter_config: dict,
    position: dict,
    datasets_info: list,
) -> dict:
    """Build a field-based filter widget (filters on column value, not SQL parameter).

    Args:
        filter_config: Filter definition with 'field' property
        position: Widget position dict with x, y, width, height
        datasets_info: List of tuples (dataset_name, dataset_id) for datasets to filter
    """
    field_name = filter_config.get("field", filter_config.get("name", "signal"))
    title = filter_config.get("title", field_name.replace("_", " ").title())
    disallow_all = filter_config.get("disallow_all", True)
    
    # Filter datasets if specific dataset(s) are specified
    # This allows linking a filter to only datasets that have the field
    target_datasets = filter_config.get("datasets")
    if target_datasets:
        if isinstance(target_datasets, str):
            target_datasets = [target_datasets]
        # Filter to only matching datasets
        datasets_info = [
            (ds_name, ds_id) for ds_name, ds_id in datasets_info
            if any(target in ds_name for target in target_datasets)
        ]

    # Build queries that extract unique values from each dataset
    queries = []
    fields = []

    for ds_name, ds_id in datasets_info:
        query_name = f"field_{ds_id}_{field_name}"

        queries.append(
            {
                "name": query_name,
                "query": {
                    "datasetName": ds_id,
                    "fields": [
                        {"name": field_name, "expression": f"`{field_name}`"},
                        {
                            "name": f"{field_name}_associativity",
                            "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                        },
                    ],
                    "disaggregated": False,
                },
            }
        )

        fields.append({"fieldName": field_name, "queryName": query_name})

    # Determine widget type based on filter config
    filter_type = filter_config.get("type", "field_select")
    if filter_type == "field_multi_select":
        widget_type = "filter-multi-select"
    elif filter_type == "range_slider":
        widget_type = "range-slider"
    else:
        widget_type = "filter-single-select"

    spec = {
        "version": 2,
        "widgetType": widget_type,
        "encodings": {"fields": fields},
        "frame": {"showTitle": True, "title": title},
    }

    if disallow_all and widget_type != "range-slider":
        spec["disallowAll"] = True

    # Support default value for field filters (must be nested inside selection object)
    default_value = filter_config.get("default")
    if default_value:
        if widget_type == "range-slider":
            # Range slider uses range format
            min_val = default_value.get("min", "") if isinstance(default_value, dict) else default_value
            max_val = default_value.get("max", "") if isinstance(default_value, dict) else ""
            spec["selection"] = {
                "defaultSelection": {
                    "range": {
                        "dataType": "INTEGER",
                        "min": {"value": str(min_val) if min_val else ""},
                        "max": {"value": str(max_val) if max_val else ""},
                    }
                }
            }
        elif widget_type == "filter-multi-select":
            # Multi-select uses array of values
            values_list = default_value if isinstance(default_value, list) else [default_value]
            spec["selection"] = {
                "defaultSelection": {
                    "values": {
                        "dataType": "STRING",
                        "values": [{"value": str(v)} for v in values_list],
                    }
                }
            }
        else:
            # Single select uses single value
            spec["selection"] = {
                "defaultSelection": {
                    "values": {"dataType": "STRING", "values": [{"value": default_value}]}
                }
            }

    return {
        "name": generate_id(f"filter-{field_name}"),
        "position": position,
        "widget": {
            "name": generate_id(f"filter-{field_name}-widget"),
            "queries": queries,
            "spec": spec,
        },
    }


def build_date_filter_widget(
    position: dict,
    datasets_info: list,
    datasets: dict,
    named_datasets: dict,
    get_or_create_dataset_func,
) -> dict:
    """Build date range filter widget (legacy method for backward compatibility)."""
    return build_filter_widget(
        {"name": "date", "type": "date_range", "title": "Date Range"},
        position,
        datasets_info,
        datasets,
        named_datasets,
        get_or_create_dataset_func,
    )

