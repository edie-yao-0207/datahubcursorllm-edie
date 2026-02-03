"""Tests to validate job configuration files against JSON schema."""

import json
import os
from pathlib import Path

import jsonschema
import pytest


def get_jobs_dir():
    """Get the path to the jobs directory."""
    return Path(__file__).parent.parent / "ml" / "jobs"


def get_schema():
    """Load the JSON schema for job configurations."""
    schema_path = get_jobs_dir() / "job_config_schema.json"
    with open(schema_path, "r") as f:
        return json.load(f)


def get_job_config_files():
    """Get all job configuration JSON files (excluding the schema)."""
    jobs_dir = get_jobs_dir()
    return [f for f in jobs_dir.glob("*.json") if f.name != "job_config_schema.json"]


@pytest.mark.parametrize("config_file", get_job_config_files())
def test_job_config_validates_against_schema(config_file):
    """Test that each job configuration file validates against the schema."""
    schema = get_schema()

    with open(config_file, "r") as f:
        config = json.load(f)

    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.ValidationError as e:
        pytest.fail(
            f"Job config {config_file.name} failed schema validation:\n"
            f"  Error: {e.message}\n"
            f"  Path: {'.'.join(str(p) for p in e.path)}\n"
            f"  Schema path: {'.'.join(str(p) for p in e.schema_path)}"
        )


def test_job_name_matches_filename():
    """Test that the 'name' field in each config matches its filename."""
    for config_file in get_job_config_files():
        with open(config_file, "r") as f:
            config = json.load(f)

        expected_name = config_file.stem  # filename without .json
        actual_name = config.get("name")

        assert actual_name == expected_name, (
            f"Config file {config_file.name} has name='{actual_name}' "
            f"but should be '{expected_name}' to match the filename."
        )


def test_region_specific_jobs_have_correct_region():
    """Test that region-specific job files specify the correct region."""
    region_suffixes = {
        "_eu": "eu-west-1",
        "_ca": "ca-central-1",
    }

    for config_file in get_job_config_files():
        with open(config_file, "r") as f:
            config = json.load(f)

        job_name = config_file.stem
        regions = config.get("regions", [])

        # Check if job name has a region suffix
        for suffix, expected_region in region_suffixes.items():
            if job_name.endswith(suffix):
                assert len(regions) == 1, (
                    f"Job {job_name} has region suffix '{suffix}' but "
                    f"specifies multiple regions: {regions}. "
                    f"Region-specific jobs should specify exactly one region."
                )
                assert expected_region in regions, (
                    f"Job {job_name} has region suffix '{suffix}' but "
                    f"doesn't specify region '{expected_region}'. "
                    f"Found regions: {regions}"
                )


def test_ops_array_has_no_op_suffix():
    """Test that ops in the 'ops' array don't include the '_op' suffix."""
    for config_file in get_job_config_files():
        with open(config_file, "r") as f:
            config = json.load(f)

        ops = config.get("ops", [])
        for op_name in ops:
            assert not op_name.endswith("_op"), (
                f"Config file {config_file.name} has op '{op_name}' in 'ops' array. "
                "Op names in the config should NOT include the '_op' suffix. "
                f"Use '{op_name.replace('_op', '')}' instead."
            )


def test_op_name_has_no_op_suffix():
    """Test that 'op_name' field doesn't include the '_op' suffix."""
    for config_file in get_job_config_files():
        with open(config_file, "r") as f:
            config = json.load(f)

        op_name = config.get("op_name")
        if op_name:
            assert not op_name.endswith("_op"), (
                f"Config file {config_file.name} has op_name='{op_name}'. "
                "The 'op_name' field should NOT include the '_op' suffix. "
                f"Use '{op_name.replace('_op', '')}' instead."
            )


def test_schema_is_valid():
    """Test that the schema file itself is valid JSON Schema."""
    schema = get_schema()

    # Validate that it's a valid JSON Schema Draft 7 schema
    try:
        # This will raise an exception if the schema is invalid
        jsonschema.Draft7Validator.check_schema(schema)
    except jsonschema.SchemaError as e:
        pytest.fail(f"Job config schema is invalid: {e.message}")


def test_all_json_files_have_configs():
    """Test that we found at least some job config files."""
    config_files = get_job_config_files()
    assert len(config_files) > 0, (
        "No job configuration files found in ml/jobs/. "
        "Expected to find at least one .json file."
    )
