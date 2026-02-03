"""Test that product usage JSON files comply with the defined schema.

This test validates all JSON files in dataplatform/dataweb/product_usage/ against
the schema defined in dataplatform/_json_schemas/product_usage.json.

The test will fail if any JSON file:
1. Contains invalid JSON syntax
2. Does not comply with the schema structure
3. Has missing required fields
4. Has invalid field combinations

These files need to be updated to follow the schema's enablement_array (and eventually usage_array) rules.
"""

import json
import os
from pathlib import Path
from typing import List

import jsonschema
import pytest


def load_json_schema() -> dict:
    """Load the product usage JSON schema."""
    # Use BACKEND_ROOT environment variable for reliable path resolution
    backend_root = os.getenv("BACKEND_ROOT")
    if backend_root:
        schema_path = (
            Path(backend_root) / "dataplatform" / "_json_schemas" / "product_usage.json"
        )
    else:
        # Fallback to relative path for local development
        schema_path = (
            Path(
                __file__
            ).parent.parent.parent.parent.parent.parent.parent.parent.parent.parent
            / "dataplatform"
            / "_json_schemas"
            / "product_usage.json"
        )

    with open(schema_path, "r") as f:
        return json.load(f)


def get_product_usage_json_files() -> List[Path]:
    """Get all JSON files in the product_usage directory, excluding templates."""
    # Use BACKEND_ROOT environment variable for reliable path resolution
    backend_root = os.getenv("BACKEND_ROOT")
    if backend_root:
        product_usage_dir = (
            Path(backend_root)
            / "dataplatform"
            / "dataweb"
            / "userpkgs"
            / "product_usage"
        )
    else:
        # Fallback to relative path for local development
        product_usage_dir = (
            Path(
                __file__
            ).parent.parent.parent.parent.parent.parent.parent.parent.parent.parent
            / "dataplatform"
            / "dataweb"
            / "userpkgs"
            / "product_usage"
        )

    # Only get JSON files directly in the product_usage directory, not in subdirectories
    # This excludes the templates/ folder which contains example files
    return [f for f in product_usage_dir.glob("*.json") if f.name != "CODEREVIEW"]


def load_json_file(file_path: Path) -> dict:
    """Load a JSON file and return its contents."""
    with open(file_path, "r") as f:
        return json.load(f)


@pytest.mark.parametrize("json_file", get_product_usage_json_files())
def test_product_usage_json_schema_compliance(json_file: Path):
    """Test that each product usage JSON file complies with the schema."""
    schema = load_json_schema()

    try:
        json_data = load_json_file(json_file)
        jsonschema.validate(instance=json_data, schema=schema)
    except jsonschema.ValidationError as e:
        # Provide more detailed error information
        error_details = []
        if hasattr(e, "absolute_path") and e.absolute_path:
            error_details.append(
                f"Path: {' -> '.join(str(p) for p in e.absolute_path)}"
            )
        if hasattr(e, "context") and e.context:
            error_details.append(f"Context: {[c.message for c in e.context]}")
        error_msg = (
            f"JSON file {json_file.name} does not comply with schema: {e.message}"
        )
        if error_details:
            error_msg += f"\nDetails: {'; '.join(error_details)}"
        pytest.fail(error_msg)
    except json.JSONDecodeError as e:
        pytest.fail(f"JSON file {json_file.name} contains invalid JSON: {e}")
    except Exception as e:
        pytest.fail(f"Unexpected error validating {json_file.name}: {e}")
