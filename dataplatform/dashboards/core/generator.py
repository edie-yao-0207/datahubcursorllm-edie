"""Main ConfigDrivenGenerator class for dashboard generation."""

import json
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict

from ..utils.sql_utils import generate_id, parse_select_columns, parse_sql_description
from ..utils.parameter_utils import detect_sql_parameters
from ..utils.widget_specs import (
    build_counter_spec,
    build_line_spec,
    build_bar_spec,
    build_area_spec,
    build_pie_spec,
    build_heatmap_spec,
    build_histogram_spec,
    build_table_spec,
)
from ..utils.filter_builders import (
    normalize_date_value,
    build_filter_widget,
    build_field_filter_widget,
    build_date_filter_widget,
)


@dataclass
class WidgetConfig:
    """Configuration for a single widget."""

    sql_path: str
    widget_type: str
    title: str
    description: str = ""
    width: int = 6
    height: int = 6
    config: dict = field(default_factory=dict)

    # Derived from SQL
    columns: list = field(default_factory=list)
    sql_content: str = ""
    dataset_id: str = ""


# Shared datasets registry - allows multiple widgets to reference the same dataset
# Maps dataset name -> sql_path
SHARED_DATASETS: Dict[str, str] = {}


class ConfigDrivenGenerator:
    """Generates dashboard from JSON config."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        # If config is in a 'dashboard' subdirectory, use parent as base_dir
        # This allows structure: dashboard_name/dashboard/config.json + dashboard_name/queries/
        if config_path.parent.name == "dashboard":
            self.base_dir = config_path.parent.parent
        else:
            self.base_dir = config_path.parent
        self.config = self._load_config()
        self.datasets: dict[str, dict] = {}  # sql_path -> dataset dict
        self.named_datasets: dict[str, str] = {}  # dataset_name -> sql_path (for shared datasets)
        # Load custom parameter definitions from config
        # Merge from both "parameters" and "global_filters" so defaults propagate to datasets
        self.custom_params = self.config.get("parameters", {})
        for filter_cfg in self.config.get("global_filters", []):
            param_name = filter_cfg.get("name")
            if param_name and param_name not in self.custom_params:
                # Convert filter config to parameter config format
                filter_type = filter_cfg.get("type", "string")
                param_cfg = {"type": filter_type}
                default_val = filter_cfg.get("default", "")
                # Handle date_range defaults which use {"min": ..., "max": ...} format
                if filter_type == "date_range" and isinstance(default_val, dict):
                    # Convert shorthand date values to Databricks format
                    min_val = default_val.get("min", "now-90d/d")
                    max_val = default_val.get("max", "now/d")
                    param_cfg["default_min"] = normalize_date_value(min_val)
                    param_cfg["default_max"] = normalize_date_value(max_val)
                else:
                    param_cfg["default"] = default_val
                self.custom_params[param_name] = param_cfg
        
        # Load shared dataset definitions from config
        # Format: "datasets": { "name": "queries/file.sql", ... }
        for ds_name, sql_path in self.config.get("datasets", {}).items():
            self.named_datasets[ds_name] = sql_path

    def _load_config(self) -> dict:
        """Load JSON configuration."""
        with open(self.config_path) as f:
            return json.load(f)

    def _load_sql(self, sql_path: str) -> str:
        """Load SQL file content."""
        full_path = self.base_dir / sql_path
        if not full_path.exists():
            print(f"Warning: SQL file not found: {full_path}", file=sys.stderr)
            return ""
        return full_path.read_text()

    def _get_or_create_dataset(self, sql_path: str) -> str:
        """Get dataset ID, creating if needed."""
        if sql_path in self.datasets:
            return self.datasets[sql_path]["name"]

        sql = self._load_sql(sql_path)
        if not sql:
            return ""

        dataset_id = generate_id(sql_path)
        display_name = sql_path.replace("/", ".").replace(".sql", "")

        # Split SQL into lines for queryLines format
        query_lines = [line + "\n" for line in sql.split("\n")]

        # Detect parameters (pass custom definitions from config)
        params = detect_sql_parameters(sql, self.custom_params)

        # Parse description from SQL comments
        description = parse_sql_description(sql)

        self.datasets[sql_path] = {
            "name": dataset_id,
            "displayName": display_name,
            "queryLines": query_lines,
            "parameters": list(params.values()),
            "_columns": parse_select_columns(sql),  # Internal use
            "_description": description,  # Internal use
        }

        return dataset_id

    def _build_widget(self, widget_cfg: dict, position: dict, widget_idx: int = 0) -> dict:
        """Build widget JSON from config.
        
        Widget config options:
            sql: str - Path to SQL file (creates/uses dataset from this file)
            dataset: str - Name of a shared dataset defined in config's "datasets" section
            parameters: dict - Widget-level parameter overrides for the query
                Example: {"metric_name": "gps_speed"} to filter shared dataset
            type: str - Widget type (table, line, bar, area, counter, text, etc.)
            title: str - Widget title
            description: str - Widget description
            width: int - Widget width in grid units
            height: int - Widget height in grid units
            config: dict - Type-specific visualization config
        """
        widget_type = widget_cfg.get("type", "table")
        
        # Handle text/markdown widgets - no SQL required
        if widget_type == "text":
            return self._build_text_widget(widget_cfg, position, widget_idx)
        
        # Support both "sql" (file path) and "dataset" (shared dataset name)
        sql_path = widget_cfg.get("sql", "")
        dataset_name = widget_cfg.get("dataset", "")
        
        # If dataset name is specified, look it up in named_datasets
        if dataset_name and dataset_name in self.named_datasets:
            sql_path = self.named_datasets[dataset_name]
        elif dataset_name:
            print(f"Warning: Unknown dataset '{dataset_name}', available: {list(self.named_datasets.keys())}", file=sys.stderr)
            return None
            
        dataset_id = self._get_or_create_dataset(sql_path)

        if not dataset_id:
            return None
        
        title = widget_cfg.get("title", sql_path)
        # Use config description, fall back to SQL-parsed description
        description = widget_cfg.get("description", "") or self.datasets[sql_path].get(
            "_description", ""
        )
        config = widget_cfg.get("config", {})
        
        # Widget-level parameter overrides (e.g., metric_name filter)
        widget_params = widget_cfg.get("parameters", {})

        # Get columns from dataset
        columns = self.datasets[sql_path].get("_columns", [])

        # Build fields list - if auto_columns is enabled, use empty fields
        # to let Databricks auto-discover all columns from the query result
        auto_columns = config.get("auto_columns", False)
        if auto_columns:
            fields = []
        elif widget_type == "histogram":
            # Histograms need special aggregate fields for Databricks
            x_field = config.get("x_field") or config.get("x_axis") or (columns[0] if columns else "value")
            fields = [
                {"name": "count(*)", "expression": "COUNT(`*`)"},
                {"name": x_field, "expression": f"`{x_field}`"},
                {"name": f"count({x_field})", "expression": f"COUNT(`{x_field}`)"},
            ]
        elif widget_type == "area" and config.get("stacked", True) and not config.get("color_by"):
            # Stacked area charts without color_by need SUM() wrappers to match Databricks export format
            # When color_by is used, we use a single y_field with stackType instead of multiple fields
            y_fields = config.get("y_fields", [])
            x_axis = config.get("x_axis", "date")
            fields = []
            for col in columns:
                if col == x_axis:
                    fields.append({"name": col, "expression": f"`{col}`"})
                elif col in y_fields:
                    # Wrap y_fields with SUM() for stacked area charts
                    fields.append({"name": f"sum({col})", "expression": f"SUM(`{col}`)"})
                else:
                    fields.append({"name": col, "expression": f"`{col}`"})
        else:
            fields = [{"name": col, "expression": f"`{col}`"} for col in columns]

        # Build query
        query = {
            "name": "main_query",
            "query": {
                "datasetName": dataset_id,
                "fields": fields,
                "disaggregated": widget_type in ["table", "counter"],
            },
        }
        
        # Add widget-level parameter overrides if specified
        # This allows filtering a shared dataset with different parameter values per widget
        # Format matches Databricks Lakeview: parameterValues with selection object
        if widget_params:
            param_values = []
            for param_name, param_val in widget_params.items():
                # Support single value or list of values
                values_list = param_val if isinstance(param_val, list) else [param_val]
                param_values.append({
                    "keyword": param_name,
                    "selection": {
                        "values": {
                            "dataType": "STRING",
                            "values": [{"value": str(v)} for v in values_list]
                        }
                    }
                })
            query["query"]["parameterValues"] = param_values

        # Build spec based on type
        if widget_type == "counter":
            spec = build_counter_spec(title, description, columns, config)
        elif widget_type == "line":
            spec = build_line_spec(title, description, columns, config)
        elif widget_type == "bar":
            spec = build_bar_spec(title, description, columns, config)
        elif widget_type == "area":
            spec = build_area_spec(title, description, columns, config)
        elif widget_type == "pie":
            spec = build_pie_spec(title, description, columns, config)
        elif widget_type == "heatmap":
            spec = build_heatmap_spec(title, description, columns, config)
        elif widget_type == "histogram":
            spec = build_histogram_spec(title, description, columns, config)
        else:
            spec = build_table_spec(title, description, columns, config)

        # Include parameters and widget index in ID to ensure uniqueness for widgets with same title but different filters
        # or same widget appearing multiple times on the same page
        params_seed = "-".join(f"{k}={v}" for k, v in sorted(widget_params.items())) if widget_params else ""
        return {
            "widget": {
                "name": generate_id(f"widget-{sql_path}-{title}-{params_seed}-{widget_idx}"),
                "queries": [query],
                "spec": spec,
            },
            "position": position,
        }

    def _build_text_widget(self, widget_cfg: dict, position: dict, widget_idx: int = 0) -> dict:
        """Build text/markdown widget (no SQL query required).

        Config options:
            content: str - Markdown content to display
            title: str - Optional title (not typically used for text widgets)
        """
        content = widget_cfg.get("content", "")
        
        # Split content into lines for multilineTextboxSpec format
        # Each line needs trailing \n except possibly the last
        if content:
            lines = [line + "\n" for line in content.split("\n")]
        else:
            lines = [""]

        # Include widget index in ID to ensure uniqueness when same text widget appears multiple times on same page
        return {
            "widget": {
                "name": generate_id(f"text-{content}-{widget_idx}"),
                "multilineTextboxSpec": {
                    "lines": lines,
                },
            },
            "position": position,
        }

    def _build_filter_widget(
        self, filter_config: dict, position: dict, datasets_info: list
    ) -> dict:
        """Build a filter widget based on config (wrapper to pass class state)."""
        return build_filter_widget(
            filter_config,
            position,
            datasets_info,
            self.datasets,
            self.named_datasets,
            self._get_or_create_dataset,
        )

    def generate(self) -> dict:
        """Generate the full dashboard JSON."""
        pages = []

        # Get global filters from config
        global_filters = self.config.get(
            "global_filters",
            [{"name": "date", "type": "date_range", "title": "Date Range"}],
        )

        # Track which parameters exist in datasets
        datasets_by_param = {}  # param_name -> [(sql_path, dataset_id)]

        for tab_idx, tab_cfg in enumerate(self.config.get("tabs", [])):
            tab_name = tab_cfg.get("name", "Untitled")
            widgets = tab_cfg.get("widgets", [])
            # Support both "filters" and "tab_filters" keys for backward compatibility
            tab_filters = tab_cfg.get("filters", tab_cfg.get("tab_filters", []))

            layout = []
            y = 0
            x = 0
            row_height = 0
            # Allow tabs to specify a custom row width (default: 6-column grid)
            max_row_width = tab_cfg.get("row_width", 6)

            # Track which datasets are used in this tab for tab-level filters
            tab_datasets_by_param = {}

            # First pass: build widgets and track datasets
            widget_layouts = []
            for widget_idx, widget_cfg in enumerate(widgets):
                width = widget_cfg.get("width", 3)
                height = widget_cfg.get("height", 4)

                # Validate widget width (max is 6 for Databricks 6-column grid)
                if width > max_row_width:
                    widget_title = widget_cfg.get("title", f"Widget {widget_idx + 1}")
                    raise ValueError(
                        f"Invalid widget width {width} for widget '{widget_title}' in tab '{tab_name}'. "
                        f"Maximum width is {max_row_width} (Databricks uses a {max_row_width}-column grid). "
                        f"Common mistake: using 12-column grid values. Use widths 1-{max_row_width} only."
                    )

                # Check if we need to wrap to next row
                if x + width > max_row_width:
                    x = 0
                    y += row_height
                    row_height = 0

                position = {"x": x, "y": y, "width": width, "height": height}

                widget_json = self._build_widget(widget_cfg, position, widget_idx)
                if widget_json:
                    widget_layouts.append(widget_json)
                    x += width
                    row_height = max(row_height, height)

                    # Track datasets used in this tab
                    # Note: Only track widgets with explicit "sql" path, not those using "dataset" name
                    # This preserves the original behavior where widgets using dataset weren't tracked
                    # (which prevented filters from being built when tab_datasets_by_param["_all"] was empty)
                    sql_path = widget_cfg.get("sql")
                    if sql_path and sql_path in self.datasets:
                        ds = self.datasets[sql_path]
                        # Track all datasets on this tab
                        if "_all" not in tab_datasets_by_param:
                            tab_datasets_by_param["_all"] = []
                        tab_datasets_by_param["_all"].append((sql_path, ds["name"]))
                        # Track by parameter
                        for param in ds.get("parameters", []):
                            keyword = param.get("keyword")
                            if keyword:
                                if keyword not in tab_datasets_by_param:
                                    tab_datasets_by_param[keyword] = []
                                tab_datasets_by_param[keyword].append(
                                    (sql_path, ds["name"])
                                )

            # Helper function to build filters
            def build_filter_widgets(filters_to_build, start_y):
                """Build filter widgets and return (widgets, total_height)."""
                filter_widgets = []
                filter_x = 0
                filter_y = start_y
                filter_row_height = 0

                for filter_cfg in filters_to_build:
                    filter_type = filter_cfg.get("type", "select")
                    param_name = filter_cfg.get("name")

                    # Check if explicit datasets are specified
                    explicit_datasets = filter_cfg.get("datasets")
                    if explicit_datasets:
                        # Use explicitly specified datasets
                        if isinstance(explicit_datasets, str):
                            explicit_datasets = [explicit_datasets]
                        # Match explicit dataset names to actual datasets on this tab
                        datasets_for_filter = [
                            (sql_path, ds_name)
                            for sql_path, ds_name in tab_datasets_by_param.get("_all", [])
                            if any(target in sql_path or target in ds_name for target in explicit_datasets)
                        ]
                    elif filter_type in ("field_select", "field_multi_select", "range_slider"):
                        # Field-based filters apply to all datasets on the tab
                        datasets_for_filter = tab_datasets_by_param.get("_all", [])
                    else:
                        # Parameter-based filters only apply to datasets with that param
                        datasets_for_filter = tab_datasets_by_param.get(param_name, [])

                    if datasets_for_filter:
                        filter_width = filter_cfg.get("width", 2)
                        filter_height = filter_cfg.get("height", 2)

                        # Validate filter width (max is 6 for Databricks 6-column grid)
                        if filter_width > max_row_width:
                            filter_title = filter_cfg.get("title", param_name or "Unknown Filter")
                            raise ValueError(
                                f"Invalid filter width {filter_width} for filter '{filter_title}' in tab '{tab_name}'. "
                                f"Maximum width is {max_row_width} (Databricks uses a {max_row_width}-column grid). "
                                f"Use widths 1-{max_row_width} only."
                            )

                        # Wrap filter row if needed
                        if filter_x + filter_width > max_row_width:
                            filter_x = 0
                            filter_y += filter_row_height
                            filter_row_height = 0

                        filter_widget = self._build_filter_widget(
                            filter_cfg,
                            {
                                "x": filter_x,
                                "y": filter_y,
                                "width": filter_width,
                                "height": filter_height,
                            },
                            datasets_for_filter,
                        )
                        filter_widgets.append(filter_widget)
                        filter_x += filter_width
                        filter_row_height = max(filter_row_height, filter_height)

                total_height = (filter_y - start_y) + filter_row_height if filter_widgets else 0
                return filter_widgets, total_height

            # Split filters by filter_row property (default 0 = top, 1 = after first widget group)
            top_filters = [f for f in (tab_filters or []) if f.get("filter_row", 0) == 0]
            mid_filters = [f for f in (tab_filters or []) if f.get("filter_row", 0) == 1]

            # Add top filters
            top_filter_widgets, top_filter_height = build_filter_widgets(top_filters, 0)
            layout.extend(top_filter_widgets)

            # Track current y position
            current_y = top_filter_height

            # If we have mid-filters, find where to insert them (after first widget or group)
            if mid_filters:
                # Get the first widget's height to place mid-filters after it
                first_widget_height = widget_layouts[0]["position"]["height"] if widget_layouts else 0
                
                # Offset first widget below top filters
                if widget_layouts:
                    widget_layouts[0]["position"]["y"] += current_y
                    layout.append(widget_layouts[0])
                    current_y += first_widget_height
                
                # Add mid-filters after first widget
                mid_filter_widgets, mid_filter_height = build_filter_widgets(mid_filters, current_y)
                layout.extend(mid_filter_widgets)
                current_y += mid_filter_height
                
                # Offset remaining widgets
                for wl in widget_layouts[1:]:
                    wl["position"]["y"] += current_y
                layout.extend(widget_layouts[1:])
            else:
                # No mid-filters, just offset all widgets below top filters
                for wl in widget_layouts:
                    wl["position"]["y"] += current_y
                layout.extend(widget_layouts)

            pages.append(
                {
                    "name": generate_id(f"page-{tab_name}"),
                    "displayName": tab_name,
                    "layout": layout,
                    "pageType": "PAGE_TYPE_CANVAS",
                }
            )

        # Track which datasets have which parameters (for filter bindings)
        # Also collect all datasets for field-based filters
        all_datasets_list = []  # List of all (sql_path, dataset_id) tuples
        for sql_path, ds in self.datasets.items():
            all_datasets_list.append((sql_path, ds["name"]))
            # Track which datasets have which parameters
            for param in ds.get("parameters", []):
                keyword = param.get("keyword")
                if keyword:
                    if keyword not in datasets_by_param:
                        datasets_by_param[keyword] = []
                    datasets_by_param[keyword].append((sql_path, ds["name"]))

        # Create a dedicated Global Filters page (PAGE_TYPE_GLOBAL_FILTERS)
        # This page doesn't show as a tab but applies to all widgets
        if global_filters:
            filter_layout = []
            filter_x = 0
            filter_y = 0
            filter_row_height = 0
            max_row_width = 6  # Default row width for global filters

            for filter_cfg in global_filters:
                filter_type = filter_cfg.get("type", "select")
                param_name = filter_cfg.get("name")
                datasets_for_filter = []

                # Handle field-based filters (field_select, field_multi_select, range_slider)
                if filter_type in ("field_select", "field_multi_select", "range_slider"):
                    # Check if explicit datasets are specified
                    explicit_datasets = filter_cfg.get("datasets")
                    if explicit_datasets:
                        # Use explicitly specified datasets
                        if isinstance(explicit_datasets, str):
                            explicit_datasets = [explicit_datasets]
                        # Match explicit dataset names to actual datasets
                        datasets_for_filter = [
                            (sql_path, ds_name)
                            for sql_path, ds_name in all_datasets_list
                            if any(target in sql_path or target in ds_name for target in explicit_datasets)
                        ]
                    else:
                        # Field-based filters without explicit datasets apply to all datasets
                        datasets_for_filter = all_datasets_list
                # Handle parameter-based filters (date_range, select, multi_select, etc.)
                elif param_name and param_name in datasets_by_param:
                    datasets_for_filter = datasets_by_param[param_name]

                if datasets_for_filter:
                    # Get width from config or default to 2
                    filter_width = filter_cfg.get("width", 2)
                    filter_height = filter_cfg.get("height", 2)

                    # Validate global filter width (max is 6 for Databricks 6-column grid)
                    if filter_width > max_row_width:
                        filter_title = filter_cfg.get("title", param_name or "Unknown Filter")
                        raise ValueError(
                            f"Invalid global filter width {filter_width} for filter '{filter_title}'. "
                            f"Maximum width is {max_row_width} (Databricks uses a {max_row_width}-column grid). "
                            f"Use widths 1-{max_row_width} only."
                        )

                    # Wrap filter row if needed
                    if filter_x + filter_width > max_row_width:
                        filter_x = 0
                        filter_y += filter_row_height
                        filter_row_height = 0

                    filter_widget = self._build_filter_widget(
                        filter_cfg,
                        {
                            "x": filter_x,
                            "y": filter_y,
                            "width": filter_width,
                            "height": filter_height,
                        },
                        datasets_for_filter,
                    )
                    filter_layout.append(filter_widget)
                    filter_x += filter_width
                    filter_row_height = max(filter_row_height, filter_height)

            if filter_layout:
                global_filters_page = {
                    "name": generate_id("global-filters"),
                    "displayName": "Global Filters",
                    "layout": filter_layout,
                    "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
                }
                pages.append(global_filters_page)

        # Build final datasets list (after global filters have loaded any lookup datasets)
        datasets = []
        for sql_path, ds in self.datasets.items():
            ds_clean = {k: v for k, v in ds.items() if not k.startswith("_")}
            datasets.append(ds_clean)

        # Collect all unique parameters from datasets
        all_params = {}
        for ds in self.datasets.values():
            for param in ds.get("parameters", []):
                keyword = param.get("keyword")
                if keyword and keyword not in all_params:
                    all_params[keyword] = param

        dashboard = {
            "datasets": datasets,
            "pages": pages,
            "uiSettings": {
                "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
                "applyModeEnabled": False,
            },
        }

        # Add global parameters if any exist
        if all_params:
            dashboard["parameters"] = list(all_params.values())

        return dashboard

