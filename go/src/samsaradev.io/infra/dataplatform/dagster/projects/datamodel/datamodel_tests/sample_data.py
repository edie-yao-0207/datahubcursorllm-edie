from collections import defaultdict

import pandas as pd
from dagster import asset
from datamodel.common.utils import NonEmptyDQCheck

dqs = defaultdict(list)


# First asset that creates a DuckDB table with some sample data
@asset(key_prefix=["my_schema"])
def table1() -> pd.DataFrame:  # the name of the asset will be the table name
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": [
                "Apple",
                "Banana",
                "Cherry",
                "Date",
                "Elderberry",
                "Fig",
                "Grape",
                "Honeydew",
                "Ice plant",
                "Jackfruit",
            ],
            "shape": [
                "Round",
                "Long",
                "Round",
                "Oval",
                "Round",
                "Oval",
                "Round",
                "Round",
                "Leafy",
                "Spiky",
            ],
        }
    )


@asset(key_prefix=["my_schema"])
def table2(table1: pd.DataFrame) -> pd.DataFrame:
    result_df = table1.query("id > 5")
    return result_df


@asset(key_prefix=["my_schema"])
def table3(table2: pd.DataFrame) -> pd.DataFrame:
    result_df = table2.query("id < 5")
    return result_df


dqs["table2"].append(
    NonEmptyDQCheck(
        name="blocking_empty_partition_check",
        database="my_schema",
        table="table2",
        blocking=True,
    )
)

dqs["table3"].append(
    NonEmptyDQCheck(
        name="blocking_empty_partition_check",
        database="my_schema",
        table="table3",
        blocking=True,
    )
)
