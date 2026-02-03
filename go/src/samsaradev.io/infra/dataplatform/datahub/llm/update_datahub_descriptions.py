#!/usr/bin/env python3
"""
Update DataHub Column Descriptions Tool

This tool updates column descriptions for data assets. It handles two cases:
1. Dagster assets (datamodel_*, dataweb_*): Provides instructions for updating the dagster asset file
2. Non-dagster assets: Updates datahub_descriptions.py directly

Usage:
    python update_datahub_descriptions.py <table_name> '<descriptions_json>'

Examples:
    # Non-dagster asset (updates datahub_descriptions.py):
    python update_datahub_descriptions.py default.data_analytics.api_mappings \
        '{"api_category": "Type of API", "http_method": "HTTP method"}'

    # Dagster asset (provides file location and update instructions):
    python update_datahub_descriptions.py datamodel_core.dim_devices \
        '{"device_id": "Unique device identifier", "org_id": "FK to dim_organizations"}'
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple


def find_backend_root() -> str:
    """Find the backend repository root directory."""
    # Check environment variable first
    backend_root = os.environ.get("BACKEND_ROOT", "")
    if backend_root and os.path.isdir(backend_root):
        return backend_root

    # Walk up from this file to find go/src/samsaradev.io
    current = Path(__file__).resolve().parent
    while current != current.parent:
        candidate = current / "go" / "src" / "samsaradev.io"
        if candidate.is_dir():
            return str(current)
        current = current.parent

    return ""


def get_datahub_descriptions_path(backend_root: str) -> str:
    """Get the path to datahub_descriptions.py."""
    return os.path.join(
        backend_root,
        "go/src/samsaradev.io/infra/dataplatform/dagster/projects/"
        "datamodel/datamodel/common/datahub_descriptions.py",
    )


def get_source_code_map_path(backend_root: str) -> Optional[str]:
    """Get the path to source_code_map.json if it exists."""
    # Check multiple possible locations
    paths = [
        os.path.join(os.path.dirname(__file__), "source_code_map.json"),
        os.path.join(backend_root, "..", "datahubcursorllm", "source_code_map.json"),
    ]
    for path in paths:
        if os.path.exists(path):
            return path
    return None


def get_table_project(table_name: str) -> str:
    """
    Determine which project a table belongs to.

    Returns:
        'datamodel' - Assets from datamodel project (update dagster asset schema)
        'dataweb' - Assets from dataweb project (update dagster asset schema)
        'other' - Non-dagster assets (update datahub_descriptions.py)
    """
    # Extract database from table name
    parts = table_name.split(".")
    if len(parts) >= 2:
        database = parts[-2] if len(parts) == 3 else parts[0]
    else:
        database = table_name

    # datamodel project: datamodel_* databases (core, platform, telematics, etc.)
    # These have dagster asset schemas that should be updated directly in the asset file
    datamodel_prefixes = [
        "datamodel_core",
        "datamodel_platform",
        "datamodel_telematics",
        "datamodel_safety",
        "datamodel_dev",
    ]
    if any(database.startswith(prefix) for prefix in datamodel_prefixes):
        return "datamodel"

    # dataweb project: dataweb_* databases (update dagster asset schema)
    # These also have dagster asset schemas
    dataweb_prefixes = [
        "dataweb_",
    ]
    if any(database.startswith(prefix) for prefix in dataweb_prefixes):
        return "dataweb"

    # Assets managed via datahub_descriptions.py (not dagster asset schemas)
    # These include analytics, metrics, ML outputs, staging tables, and ops-based tables
    datahub_descriptions_prefixes = [
        "product_analytics",
        "product_analytics_staging",
        "dataengineering",
        "metrics_api",
        "metrics_repo",
        "feature_store",
        "inference_store",
        "dataplatform_",
    ]
    if any(database.startswith(prefix) for prefix in datahub_descriptions_prefixes):
        return "other"

    # Everything else (default.*, accord_*, mixpanel_*, etc.)
    return "other"


def uses_dagster_schema(table_name: str) -> bool:
    """
    Check if an asset should have its descriptions in dagster schema metadata.

    Only datamodel and dataweb project assets use dagster schemas.
    Other assets (including ML ops-based outputs) use datahub_descriptions.py.
    """
    project = get_table_project(table_name)
    return project in ("datamodel", "dataweb")


def escape_python_string(s: str) -> str:
    """
    Escape a string for use in generated Python code with double quotes.

    Handles backslashes, newlines, and quotes to produce valid Python strings.
    """
    # Order matters: escape backslashes first, then other special chars
    s = s.replace("\\", "\\\\")
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    s = s.replace("\t", "\\t")
    s = s.replace('"', '\\"')
    return s


def normalize_table_name(table_name: str) -> Tuple[str, str, str]:
    """
    Normalize table name to (full_name, database, table) tuple.

    Handles formats:
    - database.table -> (default.database.table, database, table)
    - default.database.table -> (default.database.table, database, table)
    """
    parts = table_name.split(".")
    if len(parts) == 2:
        database, table = parts
        full_name = f"default.{database}.{table}"
    elif len(parts) == 3:
        _, database, table = parts
        full_name = table_name
    else:
        # Assume it's just a table name, try common databases
        full_name = table_name
        database = ""
        table = table_name
    return full_name, database, table


def find_dagster_asset_file(
    table_name: str, backend_root: str
) -> Optional[str]:
    """
    Find the dagster asset file for a given table.

    Uses source_code_map.json if available, otherwise searches the codebase.
    """
    parts = table_name.split(".")
    if len(parts) < 2:
        print(f"Error: Invalid table name '{table_name}'. Expected format: database.table or default.database.table")
        return None
    database, table = parts[-2:]

    # Try source_code_map.json first
    source_map_path = get_source_code_map_path(backend_root)
    if source_map_path:
        try:
            with open(source_map_path, "r") as f:
                source_map = json.load(f)
            # Try various key formats
            for key in [table_name, f"{database}.{table}", table]:
                if key in source_map:
                    return source_map[key]
        except (json.JSONDecodeError, IOError):
            pass

    # Search common dagster asset directories
    asset_dirs = [
        os.path.join(
            backend_root,
            "go/src/samsaradev.io/infra/dataplatform/dagster/projects/"
            "datamodel/datamodel/assets",
        ),
        os.path.join(
            backend_root,
            "go/src/samsaradev.io/infra/dataplatform/dagster/projects/"
            "dataweb/dataweb/assets",
        ),
    ]

    # Search for the table name in Python files
    for asset_dir in asset_dirs:
        if not os.path.isdir(asset_dir):
            continue
        for root, _, files in os.walk(asset_dir):
            for filename in files:
                if not filename.endswith(".py"):
                    continue
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "r") as f:
                        content = f.read()
                    # Look for function definitions matching the table name
                    if re.search(rf"def\s+{re.escape(table)}\s*\(", content):
                        return filepath
                    # Also check for asset name in metadata
                    if f'name="{table}"' in content or f"name='{table}'" in content:
                        return filepath
                except IOError:
                    continue

    return None


def update_datahub_descriptions_file(
    table_name: str,
    descriptions: Dict[str, str],
    backend_root: str,
    table_description: str = "",
) -> bool:
    """
    Update the datahub_descriptions.py file with new column descriptions.

    Returns True on success, False on failure.
    """
    filepath = get_datahub_descriptions_path(backend_root)

    if not os.path.exists(filepath):
        print(f"Error: datahub_descriptions.py not found at {filepath}")
        return False

    # Read the current file
    with open(filepath, "r") as f:
        content = f.read()

    # Normalize table name for the descriptions dict
    full_name, _, _ = normalize_table_name(table_name)

    # Check if table already exists in the file
    table_pattern = rf'"{re.escape(full_name)}":\s*\{{'
    table_exists = re.search(table_pattern, content)

    if table_exists:
        # Update existing entry - this is complex, so we'll provide instructions
        print(f"Table {full_name} already exists in datahub_descriptions.py")
        print("Please manually update the schema section with these descriptions:")
        print()
        for col, desc in sorted(descriptions.items()):
            escaped_desc = escape_python_string(desc)
            print(f'            "{col}": "{escaped_desc}",')
        return True

    # Find the end of the descriptions dict to insert new entry
    # The dict ends with a closing brace before the final closing brace
    # Look for the pattern of the last entry

    # Build the new entry
    escaped_table_desc = escape_python_string(table_description)
    new_entry_lines = [f'    "{full_name}": {{']
    new_entry_lines.append(f'        "description": "{escaped_table_desc}",')
    new_entry_lines.append('        "schema": {')
    for col, desc in sorted(descriptions.items()):
        escaped_desc = escape_python_string(desc)
        new_entry_lines.append(f'            "{col}": "{escaped_desc}",')
    new_entry_lines.append("        },")
    new_entry_lines.append("    },")
    new_entry = "\n".join(new_entry_lines)

    # Find where to insert (before the final closing brace of descriptions dict)
    # Look for the pattern: },\n} at the end of the descriptions dict
    # We need to insert alphabetically to maintain sorted order

    # Parse to find insertion point based on alphabetical order
    # Find all existing table names with their line positions
    lines = content.split("\n")
    table_line_indices = []  # (table_name, line_index)

    for i, line in enumerate(lines):
        match = re.match(r'\s*"(default\.[^"]+)":\s*\{', line)
        if match:
            table_line_indices.append((match.group(1), i))

    table_line_indices.sort(key=lambda x: x[0])

    # Find where our table should go alphabetically
    insert_before_line = None
    insert_after_table = None
    for table, line_idx in table_line_indices:
        if table > full_name:
            insert_before_line = line_idx
            break
        insert_after_table = table

    if insert_before_line is not None:
        # Insert before the next table entry
        # Find the line index and insert there
        new_lines = lines[:insert_before_line] + [new_entry] + lines[insert_before_line:]
        content = "\n".join(new_lines)
    elif insert_after_table:
        # Insert at the end (after the last table)
        # Find the closing of the descriptions dict
        # Look for the line with just "}" that closes the dict
        last_table_line = None
        for table, line_idx in table_line_indices:
            if table == insert_after_table:
                last_table_line = line_idx
                break

        if last_table_line is not None:
            # Find the closing brace of this entry by counting braces
            brace_count = 0
            entry_end_line = last_table_line
            for i in range(last_table_line, len(lines)):
                brace_count += lines[i].count("{") - lines[i].count("}")
                if brace_count == 0:
                    entry_end_line = i
                    break

            # Insert after this entry
            new_lines = (
                lines[: entry_end_line + 1] + [new_entry] + lines[entry_end_line + 1 :]
            )
            content = "\n".join(new_lines)
        else:
            print(f"Could not find insertion point after {insert_after_table}")
            print("Please manually add this entry to datahub_descriptions.py:")
            print()
            print(new_entry)
            return True
    else:
        # Insert at the beginning of the descriptions dict
        # Find "descriptions: dict[str, dict[str, str]] = {"
        pattern = r'(descriptions:\s*dict\[str,\s*dict\[str,\s*str\]\]\s*=\s*\{)'
        match = re.search(pattern, content)
        if match:
            insert_pos = match.end()
            content = content[:insert_pos] + "\n" + new_entry + content[insert_pos:]
        else:
            print("Could not find descriptions dict in file")
            print("Please manually add this entry to datahub_descriptions.py:")
            print()
            print(new_entry)
            return True

    # Write the updated content
    with open(filepath, "w") as f:
        f.write(content)

    print(f"✅ Successfully updated {filepath}")
    print(f"   Added descriptions for table: {full_name}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Update column descriptions for data tables"
    )
    parser.add_argument(
        "table_name",
        help="Full table name (e.g., datamodel_core.dim_devices or default.data_analytics.api_mappings)",
    )
    parser.add_argument(
        "descriptions",
        help="JSON string mapping column names to descriptions",
    )
    parser.add_argument(
        "--table-description",
        default="",
        help="Optional table-level description",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )

    args = parser.parse_args()

    # Parse descriptions JSON
    try:
        descriptions = json.loads(args.descriptions)
        if not isinstance(descriptions, dict):
            raise ValueError("Descriptions must be a JSON object")
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error: Invalid JSON in descriptions: {e}")
        sys.exit(1)

    if not descriptions:
        print("Error: No descriptions provided")
        sys.exit(1)

    # Validate all values are non-empty strings
    invalid_entries = []
    for key, value in descriptions.items():
        if not isinstance(key, str) or not key:
            invalid_entries.append(f"  - Key {key!r}: must be a non-empty string")
        elif not isinstance(value, str):
            invalid_entries.append(
                f"  - '{key}': expected string, got {type(value).__name__} ({value!r})"
            )
        elif not value.strip():
            invalid_entries.append(f"  - '{key}': description cannot be empty or whitespace")
    if invalid_entries:
        print("Error: Invalid description entries:")
        for entry in invalid_entries:
            print(entry)
        sys.exit(1)

    # Find backend root
    backend_root = find_backend_root()
    if not backend_root:
        print("Error: Could not find backend repository root")
        print("Set BACKEND_ROOT environment variable or run from within the repository")
        sys.exit(1)

    table_name = args.table_name
    project = get_table_project(table_name)

    # Determine asset type and handle accordingly
    if uses_dagster_schema(table_name):
        project_name = "datamodel" if project == "datamodel" else "dataweb"
        print(f"🔍 Detected {project_name} project asset: {table_name}")
        print("   → Descriptions should be added to dagster asset schema metadata")
        print()

        # Try to find the asset file
        asset_file = find_dagster_asset_file(table_name, backend_root)

        if asset_file:
            print(f"📁 Found asset file: {asset_file}")
        else:
            print("⚠️  Could not locate asset file automatically")
            if project == "datamodel":
                print("   Search for the table definition in:")
                print("   - go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/assets/")
            else:
                print("   Search for the asset definition in:")
                print("   - go/src/samsaradev.io/infra/dataplatform/dagster/projects/dataweb/dataweb/assets/")

        print()
        print("📝 Update the 'schema' metadata in the asset definition with these column descriptions:")
        print()
        print("```python")
        print("schema=[")
        for col_name, description in sorted(descriptions.items()):
            escaped_desc = escape_python_string(description)
            print(f'    {{"name": "{col_name}", "type": "<TYPE>", "nullable": True, "metadata": {{"comment": "{escaped_desc}"}}}},')
        print("]")
        print("```")
        print()
        print("Replace <TYPE> with the actual column type (string, long, double, etc.)")
        print()
        print("For nested struct fields, use the full nested structure:")
        print("```python")
        print('{')
        print('    "name": "struct_col",')
        print('    "type": {')
        print('        "type": "struct",')
        print('        "fields": [')
        print('            {"name": "nested_field", "type": "string", "nullable": True, "metadata": {"comment": "description"}}')
        print('        ]')
        print('    },')
        print('    "metadata": {"comment": "struct column description"}')
        print('}')
        print("```")

    else:
        print(f"📊 Detected non-dagster asset: {table_name}")
        print("   → Descriptions should be added to datahub_descriptions.py")
        print()

        if args.dry_run:
            print("DRY RUN - Would add to datahub_descriptions.py:")
            print()
            full_name, _, _ = normalize_table_name(table_name)
            escaped_table_desc = escape_python_string(args.table_description)
            print(f'    "{full_name}": {{')
            print(f'        "description": "{escaped_table_desc}",')
            print('        "schema": {')
            for col, desc in sorted(descriptions.items()):
                escaped_desc = escape_python_string(desc)
                print(f'            "{col}": "{escaped_desc}",')
            print("        },")
            print("    },")
        else:
            success = update_datahub_descriptions_file(
                table_name,
                descriptions,
                backend_root,
                args.table_description,
            )
            if not success:
                sys.exit(1)


if __name__ == "__main__":
    main()
