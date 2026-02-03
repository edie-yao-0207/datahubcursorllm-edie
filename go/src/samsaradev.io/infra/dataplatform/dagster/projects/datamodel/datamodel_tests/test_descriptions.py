import warnings
from datetime import datetime

import pytest
from dagster import AssetsDefinition
from datamodel import defs
from datamodel.common.datahub_utils import is_high_quality_description
from datamodel.common.utils import AWSRegions, TableType, build_table_description


def test_build_table_description():
    custom_string = "Custom string"
    row_meaning = "Meaning of each row"
    table_type = TableType.TRANSACTIONAL_FACT
    freshness_slo_hours = 24
    related_table_info = {
        "table1": "Description of table1",
        "table2": "Description of table2",
    }
    caveats = "Additional caveats"

    expected_output = """**Table Description**:
Custom string

**Meaning of each row**:
Meaning of each row

**Table Type**:
This is a transactional fact table - a transactional fact table is a

table where each date partition stores the transactions that happened on a given date.

To query activity on a time period, filter on the date column

For example - A fact trips table will show all the trips taken on a date or a range of dates

**SLO**:
This table will be updated at least once every 24 hours.
Breaches to this guarantee will be monitored and responded to during business days (9am-5pm PT).

**Related Tables**:
- table1 => Description of table1.
- table2 => Description of table2.

Additional caveats"""

    observed_output = build_table_description(
        table_desc=custom_string,
        row_meaning=row_meaning,
        table_type=table_type,
        freshness_slo_hours=freshness_slo_hours,
        related_table_info=related_table_info,
        caveats=caveats,
    )

    assert observed_output == expected_output


def test_all_asset_description():
    def _test_description(
        description: str, asset_key: str, asset_context: dict
    ) -> bool:
        if description is None:
            return False

        for SENTINEL_PHRASE in SENTINEL_PHRASES:
            if SENTINEL_PHRASE not in description:
                return False

        return True

    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        assert (
            len(asset_key.path) == 3
        ), f"{asset_key} must have length of 3 in format (region, database, table)."

        region, database, table = asset_key.path

        if (
            table.startswith("dq_")
            or region == AWSRegions.EU_WEST_1.value
            or not database.startswith("datamodel_")
            or database.endswith("_bronze")
            or database.endswith("_silver")
            or database == "datamodel_dev"
        ):
            continue

        asset_def: AssetsDefinition = repo_def.assets_defs_by_key[asset_key]

        asset_context = asset_def.metadata_by_key[asset_key]

        SENTINEL_PHRASES = [
            "Breaches to this guarantee will be monitored and responded to during business days",
            "This table will be updated",
            "**SLO**",
            "**Meaning of each row**:",
            "**Table Description**:",
        ]

        description = asset_context.get("description")

        test_description_result = _test_description(
            description, asset_key, asset_context
        )

        assert (
            test_description_result
        ), f"Description for {database}.{table} is not templated correctly. Must use build_table_description function."

    assert True


def test_imported_from_with_few_tokens():
    assert not is_high_quality_description("Imported from table")


def test_sourced_from_with_few_tokens():
    assert not is_high_quality_description("Sourced from table")


def test_short_description():
    assert not is_high_quality_description("short")


def test_empty_description():
    assert not is_high_quality_description("")


def test_spaces_only_description():
    assert not is_high_quality_description("     ")


def test_valid_description():
    assert is_high_quality_description("This is a valid description.")


def test_imported_from_with_enough_tokens():
    assert is_high_quality_description("imported from table with many tokens")


def test_sourced_from_with_enough_tokens():
    assert is_high_quality_description("sourced from table with many tokens")


def test_column_descriptions():

    repo_def = defs.get_repository_def()
    for asset_key, _ in repo_def.assets_defs_by_key.items():
        region, database, table = asset_key.path

        if (
            table.startswith("dq_")
            or region == AWSRegions.EU_WEST_1.value
            or not (
                database.startswith("datamodel_")
                or database
                in [
                    "product_analytics",
                    "dataengineering",
                    "metrics_api",
                    "metrics_repo",
                ]
            )
            or database.endswith("_bronze")
            or database.endswith("_silver")
            or database.endswith("_dev")
        ):
            continue

        assert (
            len(asset_key.path) == 3
        ), f"{asset_key} must have length of 3 in format (region, database, table)."

        asset_def: AssetsDefinition = repo_def.assets_defs_by_key[asset_key]

        asset_context = asset_def.metadata_by_key[asset_key]

        for column_metadata in asset_context.get("schema", {}):
            assert isinstance(
                column_metadata, dict
            ), f"Column metadata for {database}.{table} is not a dictionary."

            assert (
                "metadata" in column_metadata
            ), f"Column metadata for {database}.{table} does not have metadata."

            assert (
                "comment" in column_metadata["metadata"]
            ), f"Column metadata for {database}.{table} does not have a comment."
