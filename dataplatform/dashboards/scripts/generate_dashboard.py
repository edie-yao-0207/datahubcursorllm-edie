#!/usr/bin/env python3
"""
Databricks AI/BI Dashboard Generator (Auto-Discovery)

Generates a .lvdash.json file from SQL files in a directory structure.
Reads visualization hints from SQL file comments to configure charts.

Usage:
    python generate_dashboard.py <dashboard_dir> [--title "Dashboard Title"]
    python generate_dashboard.py dce > "DCE Dashboard.lvdash.json"
    
SQL File Comment Format (parsed for visualization config):
    -- Tab: <tab_name>
    -- View: <view_name>
    -- Visualization: <type> (Counter, Line Chart, Bar Chart, Table, etc.)
    -- Title: "<title>"
    -- Description: <description>
    -- X-Axis: <field_name>
    -- Y-Axis: <field_name> (stacked)
    -- Series: <field1>, <field2>, ...
"""

import json
import os
import re
import hashlib
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def generate_id(seed: str) -> str:
    """Generate 8-char hex ID from seed string."""
    return hashlib.md5(seed.encode()).hexdigest()[:8]


@dataclass
class VisualizationConfig:
    """Parsed visualization config from SQL comments."""
    tab: str = "Overview"
    view: str = "Untitled"
    viz_type: str = "table"  # counter, line, bar, table, area
    title: str = ""
    description: str = ""
    x_axis: str = ""
    y_axis: str = ""
    y_fields: list = field(default_factory=list)
    series_by: str = ""
    stacked: bool = False
    columns: list = field(default_factory=list)


def parse_select_columns(sql: str) -> List[str]:
    """Extract column names from SQL SELECT clause."""
    columns = []
    
    # Find the main SELECT (not in CTEs)
    # Look for final SELECT that's not inside WITH
    sql_upper = sql.upper()
    
    # Find the last SELECT that produces output
    # Simple heuristic: find SELECT after last CTE or the only SELECT
    select_match = None
    for match in re.finditer(r'\bSELECT\b', sql, re.IGNORECASE):
        select_match = match
    
    if not select_match:
        return columns
    
    # Get text from SELECT to FROM (handle string literals and identifiers)
    start = select_match.end()
    main_sql = sql[start:]
    
    # Find FROM while tracking string literals and ensuring word boundary
    depth = 0
    in_string = False
    string_char = None
    from_pos = None
    for i in range(len(main_sql)):
        char = main_sql[i]
        
        # Track string literals (single or double quotes)
        if char in ("'", '"') and (i == 0 or main_sql[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
            continue
        
        if in_string:
            continue
            
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and main_sql[i : i + 4].upper() == "FROM":
            # Ensure FROM is a complete word (not part of identifier like from_stage)
            prev_ok = i == 0 or not (main_sql[i - 1].isalnum() or main_sql[i - 1] == "_")
            next_pos = i + 4
            next_ok = next_pos >= len(main_sql) or not (main_sql[next_pos].isalnum() or main_sql[next_pos] == "_")
            if prev_ok and next_ok:
                from_pos = i
                break
    
    if from_pos is None:
        return columns
    
    select_clause = main_sql[:from_pos]
    
    # Parse column expressions (handle nested parens and string literals)
    depth = 0
    in_string = False
    string_char = None
    current = ""
    for i, char in enumerate(select_clause):
        # Track string literals (single or double quotes)
        if char in ("'", '"') and (i == 0 or select_clause[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
            current += char
        elif in_string:
            current += char
        elif char == "(":
            depth += 1
            current += char
        elif char == ")":
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            columns.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        columns.append(current.strip())
    
    # Extract final column names (alias or last identifier)
    result = []
    for col in columns:
        col = col.strip()
        if not col:
            continue
        # Check for AS alias
        as_match = re.search(r'\bAS\s+[`"]?(\w+)[`"]?\s*$', col, re.IGNORECASE)
        if as_match:
            result.append(as_match.group(1))
        else:
            # Get last identifier (handles table.column)
            ident_match = re.search(r'[`"]?(\w+)[`"]?\s*$', col)
            if ident_match:
                result.append(ident_match.group(1))
    
    return result


def parse_sql_comments(sql: str) -> VisualizationConfig:
    """Extract visualization config from SQL file comments."""
    config = VisualizationConfig()
    
    # Parse tab
    match = re.search(r'--\s*Tab:\s*(.+)', sql)
    if match:
        config.tab = match.group(1).strip()
    
    # Parse view name
    match = re.search(r'--\s*View:\s*(.+)', sql)
    if match:
        config.view = match.group(1).strip()
    
    # Parse visualization type
    match = re.search(r'--\s*Visualization:\s*(\w+)', sql, re.IGNORECASE)
    if match:
        viz_type = match.group(1).lower()
        type_map = {
            'counter': 'counter',
            'scorecards': 'counter',
            'line': 'line',
            'bar': 'bar',
            'stacked': 'bar',
            'area': 'area',
            'table': 'table',
            'combination': 'line',
        }
        config.viz_type = type_map.get(viz_type, 'table')
    
    # Parse title
    match = re.search(r'--\s*Title:\s*["\']?([^"\'\n]+)["\']?', sql)
    if match:
        config.title = match.group(1).strip()
    
    # Parse description  
    match = re.search(r'--\s*Description:\s*(.+?)(?=\n--|$)', sql, re.DOTALL)
    if match:
        config.description = ' '.join(match.group(1).split())
    
    # Parse X-Axis
    match = re.search(r'--\s*X-Axis:\s*(\w+)', sql)
    if match:
        config.x_axis = match.group(1).strip()
    
    # Parse Y-Axis
    match = re.search(r'--\s*Y-Axis:\s*(.+)', sql)
    if match:
        y_spec = match.group(1).strip()
        config.stacked = 'stacked' in y_spec.lower()
        # Extract field name
        y_field = re.sub(r'\s*\(.*\)', '', y_spec).strip()
        config.y_axis = y_field
    
    # Parse Series/Stack by
    match = re.search(r'--\s*(?:Series|Stack by).*?:\s*(.+)', sql)
    if match:
        fields = [f.strip() for f in match.group(1).split(',')]
        config.y_fields = fields
    
    # Auto-detect columns from SQL
    config.columns = parse_select_columns(sql)
    
    # Auto-set x_axis to 'date' if not specified and date column exists
    if not config.x_axis and 'date' in config.columns:
        config.x_axis = 'date'
    
    # Auto-set y_fields for charts if not specified
    if not config.y_fields and config.viz_type in ['line', 'bar', 'area']:
        # Use numeric-looking columns (exclude date, strings)
        numeric_cols = [c for c in config.columns 
                       if c not in ['date', 'database', 'table', 'service', 'team', 
                                   'region', 'name', 'metric', 'status', 'alert_type',
                                   'hierarchy_label', 'grouping_columns', 'time_horizon']]
        config.y_fields = numeric_cols[:3]  # Limit to first 3
    
    return config


@dataclass
class Dataset:
    """A SQL dataset for the dashboard."""
    name: str
    display_name: str
    sql: str
    has_date_param: bool = True
    has_limit_param: bool = False


@dataclass  
class Widget:
    """A dashboard widget."""
    name: str
    dataset_name: str
    config: VisualizationConfig
    position: dict


class DashboardGenerator:
    """Generates Databricks AI/BI Dashboard JSON."""
    
    def __init__(self, title: str = "DCE Dashboard"):
        self.title = title
        self.datasets: List[Dataset] = []
        self.pages: Dict[str, List[Widget]] = {}  # tab_name -> widgets
        
    def add_sql_file(self, sql_path: Path) -> None:
        """Add a SQL file as a dataset and widget."""
        sql = sql_path.read_text()
        
        # Generate display name from path
        rel_path = sql_path.relative_to(sql_path.parent.parent.parent)
        display_name = str(rel_path).replace('/', '.').replace('.sql', '')
        
        # Generate unique ID
        dataset_id = generate_id(display_name)
        
        # Parse visualization config
        config = parse_sql_comments(sql)
        if not config.title:
            config.title = config.view
            
        # Check for parameters
        has_date = ':date.min' in sql or ':date.max' in sql
        has_limit = ':limit' in sql
        
        # Create dataset
        dataset = Dataset(
            name=dataset_id,
            display_name=display_name,
            sql=sql,
            has_date_param=has_date,
            has_limit_param=has_limit,
        )
        self.datasets.append(dataset)
        
        # Create widget
        widget = Widget(
            name=generate_id(f"widget-{display_name}"),
            dataset_name=dataset_id,
            config=config,
            position={},  # Will be computed later
        )
        
        # Add to appropriate tab
        tab_name = config.tab
        if tab_name not in self.pages:
            self.pages[tab_name] = []
        self.pages[tab_name].append(widget)
    
    def _build_dataset_json(self, dataset: Dataset) -> dict:
        """Build dataset JSON structure."""
        # Split SQL into lines for queryLines format
        query_lines = [line + '\n' for line in dataset.sql.split('\n')]
        
        params = []
        
        # Add date range parameter
        if dataset.has_date_param:
            params.append({
                "displayName": "date",
                "keyword": "date",
                "dataType": "DATE",
                "complexType": "RANGE",
                "defaultSelection": {
                    "range": {
                        "dataType": "DATE",
                        "min": {"value": "now-90d/d"},
                        "max": {"value": "now/d"}
                    }
                }
            })
        
        # Add limit parameter
        if dataset.has_limit_param:
            params.append({
                "displayName": "limit",
                "keyword": "limit",
                "dataType": "DECIMAL",
                "defaultSelection": {
                    "values": {
                        "dataType": "DECIMAL",
                        "values": [{"value": "-1"}]
                    }
                }
            })
        
        return {
            "name": dataset.name,
            "displayName": dataset.display_name,
            "queryLines": query_lines,
            "parameters": params
        }
    
    def _build_widget_json(self, widget: Widget) -> dict:
        """Build widget JSON structure."""
        config = widget.config
        
        # Build fields list from detected columns
        fields = []
        for col in config.columns:
            fields.append({
                "name": col,
                "expression": f"`{col}`"
            })
        
        # Base query reference
        query = {
            "name": "main_query",
            "query": {
                "datasetName": widget.dataset_name,
                "fields": fields,
                "disaggregated": config.viz_type in ['table', 'counter']
            }
        }
        
        # Build spec based on widget type
        if config.viz_type == 'counter':
            spec = self._build_counter_spec(config)
        elif config.viz_type == 'line':
            spec = self._build_line_spec(config)
        elif config.viz_type == 'bar':
            spec = self._build_bar_spec(config)
        elif config.viz_type == 'area':
            spec = self._build_area_spec(config)
        else:  # table
            spec = self._build_table_spec(config)
        
        return {
            "widget": {
                "name": widget.name,
                "queries": [query],
                "spec": spec
            },
            "position": widget.position
        }
    
    def _build_counter_spec(self, config: VisualizationConfig) -> dict:
        """Build counter widget spec."""
        # Try to find a value column
        value_field = "value"
        for col in config.columns:
            if col in ['value', 'total', 'count', 'sum', 'avg', 'metric']:
                value_field = col
                break
            if 'count' in col.lower() or 'total' in col.lower():
                value_field = col
                break
        
        # If no good value field, use first numeric-looking one
        if value_field == "value" and config.columns:
            for col in config.columns:
                if col not in ['date', 'name', 'metric', 'status', 'type']:
                    value_field = col
                    break
        
        return {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {
                    "fieldName": value_field,
                    "rowNumber": 1
                }
            },
            "frame": {
                "showTitle": True,
                "title": config.title
            }
        }
    
    def _build_line_spec(self, config: VisualizationConfig) -> dict:
        """Build line chart spec."""
        y_fields = config.y_fields or ([config.y_axis] if config.y_axis else [])
        
        # If no y fields specified, auto-detect from columns
        if not y_fields and config.columns:
            y_fields = [c for c in config.columns 
                       if c not in ['date', 'database', 'table', 'service', 'team', 
                                   'region', 'name', 'status', 'alert_type']][:3]
        
        return {
            "version": 3,
            "widgetType": "line",
            "encodings": {
                "x": {
                    "fieldName": config.x_axis or "date",
                    "scale": {"type": "temporal"}
                },
                "y": {
                    "scale": {"type": "quantitative"},
                    "fields": [{"fieldName": f} for f in y_fields]
                }
            },
            "frame": {
                "showTitle": True,
                "title": config.title,
                "showDescription": bool(config.description),
                "description": config.description
            }
        }
    
    def _build_bar_spec(self, config: VisualizationConfig) -> dict:
        """Build bar chart spec."""
        y_fields = config.y_fields or ([config.y_axis] if config.y_axis else [])
        
        # If no y fields specified, auto-detect from columns
        if not y_fields and config.columns:
            y_fields = [c for c in config.columns 
                       if c not in ['date', 'database', 'table', 'service', 'team', 
                                   'region', 'name', 'status', 'alert_type']][:3]
        
        return {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {
                    "fieldName": config.x_axis or "date",
                    "scale": {"type": "categorical"},
                    "axis": {"labelAngle": 45}
                },
                "y": {
                    "scale": {"type": "quantitative"},
                    "fields": [{"fieldName": f} for f in y_fields]
                }
            },
            "frame": {
                "showTitle": True,
                "title": config.title
            }
        }
    
    def _build_area_spec(self, config: VisualizationConfig) -> dict:
        """Build area chart spec."""
        spec = self._build_line_spec(config)
        spec["widgetType"] = "area"
        return spec
    
    def _build_table_spec(self, config: VisualizationConfig) -> dict:
        """Build table widget spec."""
        # Build column definitions from detected columns
        columns = []
        for i, col in enumerate(config.columns):
            col_def = {
                "fieldName": col,
                "type": "string",
                "displayAs": "string",
                "visible": True,
                "order": 100000 + i,
                "title": col,
                "alignContent": "left"
            }
            # Adjust for numeric-looking columns
            if any(x in col.lower() for x in ['count', 'cost', 'queries', 'users', 
                                               'total', 'avg', 'sum', 'percent', 'pct',
                                               'score', 'days', 'num_']):
                col_def["type"] = "number"
                col_def["displayAs"] = "number"
                col_def["alignContent"] = "right"
            columns.append(col_def)
        
        return {
            "version": 1,
            "widgetType": "table",
            "encodings": {
                "columns": columns
            },
            "invisibleColumns": [],
            "itemsPerPage": 25,
            "condensed": True,
            "frame": {
                "showTitle": True,
                "title": config.title
            }
        }
    
    def _compute_layout(self) -> None:
        """Compute widget positions within each page using row-based packing."""
        GRID_WIDTH = 12  # Dashboard grid is 12 columns wide
        
        for tab_name, widgets in self.pages.items():
            current_y = 0          # Current vertical position
            current_row_x = 0      # Current horizontal position in row
            current_row_height = 0 # Max height of widgets in current row
            
            for widget in widgets:
                # Determine widget dimensions based on viz type
                viz_type = widget.config.viz_type
                
                if viz_type == 'counter':
                    width, height = 3, 2
                elif viz_type == 'table':
                    width, height = 6, 8
                else:  # line, bar, area charts
                    width, height = 6, 6
                
                # Check if widget fits in current row
                if current_row_x + width > GRID_WIDTH:
                    # Move to next row
                    current_y += current_row_height
                    current_row_x = 0
                    current_row_height = 0
                
                # Place widget at current position
                widget.position = {
                    "x": current_row_x,
                    "y": current_y,
                    "width": width,
                    "height": height
                }
                
                # Update row tracking
                current_row_x += width
                current_row_height = max(current_row_height, height)
    
    def generate(self) -> dict:
        """Generate the full dashboard JSON."""
        self._compute_layout()
        
        # Build datasets
        datasets_json = [self._build_dataset_json(d) for d in self.datasets]
        
        # Build pages
        pages_json = []
        for tab_name, widgets in self.pages.items():
            page = {
                "name": generate_id(f"page-{tab_name}"),
                "displayName": tab_name,
                "layout": [self._build_widget_json(w) for w in widgets],
                "pageType": "PAGE_TYPE_CANVAS"
            }
            pages_json.append(page)
        
        return {
            "datasets": datasets_json,
            "pages": pages_json,
            "uiSettings": {
                "theme": {
                    "widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"
                },
                "applyModeEnabled": False
            }
        }


def discover_sql_files(base_dir: Path) -> List[Path]:
    """Find all SQL files in directory tree."""
    return sorted(base_dir.glob("**/*.sql"))


def main():
    """Generate dashboard from SQL files."""
    import sys
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python generate_dashboard.py <dashboard_dir> [--title \"Dashboard Title\"]", file=sys.stderr)
        print("", file=sys.stderr)
        print("Example:", file=sys.stderr)
        print("  python generate_dashboard.py dce > dce_dashboard.lvdash.json", file=sys.stderr)
        sys.exit(1)
    
    dashboard_dir = Path(sys.argv[1])
    
    # Parse optional title
    title = "Dashboard"
    for i, arg in enumerate(sys.argv):
        if arg == "--title" and i + 1 < len(sys.argv):
            title = sys.argv[i + 1]
            break
    
    if not dashboard_dir.exists():
        print(f"Error: Directory not found: {dashboard_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Create generator
    gen = DashboardGenerator(title=title)
    
    # Add all SQL files from the directory
    sql_files = discover_sql_files(dashboard_dir)
    for sql_file in sql_files:
        gen.add_sql_file(sql_file)
    
    # Generate and print JSON
    dashboard = gen.generate()
    print(json.dumps(dashboard, indent=2))


if __name__ == "__main__":
    main()

