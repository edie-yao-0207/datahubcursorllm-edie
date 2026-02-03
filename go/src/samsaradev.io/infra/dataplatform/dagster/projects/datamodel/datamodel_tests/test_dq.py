import pandas as pd
from dagster import Definitions, InMemoryIOManager, load_assets_from_modules
from dagster_duckdb_pandas import DuckDBPandasIOManager
from datamodel.common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from datamodel.common.utils import DQGroup, build_dq_asset

from . import sample_data

all_assets = load_assets_from_modules([sample_data])

# Definitions
defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_io_manager": DuckDBPandasIOManager(database="my_db.duckdb"),
        "in_memory_io_manager": InMemoryIOManager(),
    },
)


def test_table1_asset():
    df1 = sample_data.table1()

    # Assert that the DataFrame is not empty
    assert not df1.empty

    # Assert that the DataFrame has 10 rows
    assert len(df1) == 10


def test_table2_asset():
    df1 = sample_data.table1()
    df2 = sample_data.table2(df1)

    # Assert that the DataFrame is not empty
    assert not df2.empty

    # Reset index for comparison
    df2.reset_index(drop=True, inplace=True)

    # You can add more assertions to test the DataFrame content
    assert df2.equals(
        pd.DataFrame(
            {
                "id": [6, 7, 8, 9, 10],
                "name": ["Fig", "Grape", "Honeydew", "Ice plant", "Jackfruit"],
                "shape": ["Oval", "Round", "Round", "Leafy", "Spiky"],
            }
        )
    )


def test_table3_asset():
    df1 = sample_data.table1()
    df2 = sample_data.table2(df1)
    df3 = sample_data.table3(df2)

    # Assert that the DataFrame is not empty
    assert df3.empty


def test_import_dq_assets():
    dq_assets = [
        build_dq_asset(
            dq_name,
            dq_values,
            "dq_test",
            None,
            SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
            step_launcher_resource="duckdb_io_manager",
        )
        for dq_name, dq_values in sample_data.dqs.items()
    ]

    dq_assets_from_builder = DQGroup(
        "dq_test",
        None,
        SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
        step_launcher_resource="duckdb_io_manager",
    )

    for dq_name, dq_values in sample_data.dqs.items():
        dq_assets_from_builder[dq_name] = dq_values
    # TODO - revisit for better test of equality, but for now it's helpful to document these methods succeed
    # and we get similar results
    assert len(dq_assets) == len(dq_assets_from_builder.generate())
