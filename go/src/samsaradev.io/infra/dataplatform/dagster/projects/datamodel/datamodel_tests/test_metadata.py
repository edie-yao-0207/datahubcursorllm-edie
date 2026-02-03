import os

from datamodel.common import datahub_descriptions
from datamodel.common.datahub_utils import get_db_and_table, load_highlighted_queries
from datamodel.common.llm.lineage import find_downstream_tables, find_upstream_tables


def test_loading_highlighted_queries():
    if os.getenv("MODE", "") == "PYTEST_RUN":
        directory_prefix = "/datamodel/datamodel"
    else:
        directory_prefix = f"{os.getenv('BACKEND_ROOT', '/code')}/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel"

    directory = f"{directory_prefix}/ops/datahub/highlighted_queries/queries"

    highlighted_query_map = load_highlighted_queries(directory)

    assert (
        len(highlighted_query_map) >= 5
    ), "Highlighted query map have at least 5 queries"

    for table, queries in highlighted_query_map.items():
        for key, query in queries.items():
            assert key, "Key is not empty"
            assert query, "Query is not empty"
            assert isinstance(table, str), f"table: {table} is not a string"
            assert isinstance(query, str), f"Query: {query} is not a string"
            assert isinstance(key, str), f"Key: {key} is not a string"


def test_loading_datahub_descriptions():
    datahub_descriptions_dict = datahub_descriptions.descriptions

    for table, table_info in datahub_descriptions_dict.items():
        assert isinstance(table, str), f"Table name {table} should be a string"
        assert isinstance(
            table_info, dict
        ), f"Table info for {table} should be a dictionary"

        assert isinstance(
            table_info.get("description", ""), str
        ), f"Description for {table} should be a string"
        assert isinstance(
            table_info.get("schema", {}), dict
        ), f"Schema for {table} should be a dictionary"

        for column, description in table_info.get("schema", {}).items():
            assert isinstance(
                column, str
            ), f"Column name {column} in {table} should be a string"
            assert isinstance(
                description, str
            ), f"Description for column {column} in {table} should be a string"

            unpacked_table_and_catalog = table.split(".")

            assert (
                len(unpacked_table_and_catalog) == 3
            ), f"Table {table} should have 3 parts"
            assert isinstance(
                unpacked_table_and_catalog[0], str
            ), f"Catalog for {table} should be a string"
            assert isinstance(
                unpacked_table_and_catalog[1], str
            ), f"Database for {table} should be a string"
            assert isinstance(
                unpacked_table_and_catalog[2], str
            ), f"Table for {table} should be a string"

    assert datahub_descriptions_dict, "Datahub descriptions dict is not empty"


def test_datahub_descriptions_sorted_by_table_name():
    datahub_descriptions_dict = datahub_descriptions.descriptions

    sorted_table_names = list(sorted(datahub_descriptions_dict.keys()))

    assert sorted_table_names == list(
        datahub_descriptions_dict.keys()
    ), f"Table names must be sorted: {', '.join(list(datahub_descriptions_dict.keys()))}"


lineage_dict = {
    "a": [],
    "b": ["a"],
    "c": ["b"],
    "d": ["b", "c"],
    "e": ["c", "d"],
    "f": ["b", "c", "d"],
    "g": ["a"],
}


def test_find_upstream_tables():

    upstream_tables = find_upstream_tables(lineage_dict, "e")
    assert upstream_tables == {
        "a",
        "b",
        "c",
        "d",
    }, f"Upstream tables for e should be a, b, c, d but got {sorted(upstream_tables)}"

    upstream_tables = find_upstream_tables(lineage_dict, "g")
    assert upstream_tables == {
        "a"
    }, f"Upstream tables for g should be a, but got {sorted(upstream_tables)}"


def test_find_downstream_tables():
    downstream_tables = find_downstream_tables(lineage_dict, "a")
    assert downstream_tables == {
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
    }, f"Downstream tables for a should be b, c, d, e, f, g, but got {sorted(downstream_tables)}"

    downstream_tables = find_downstream_tables(lineage_dict, "g")
    assert (
        downstream_tables == set()
    ), f"Downstream tables for g should be empty, but got {downstream_tables}"

    downstream_tables = find_downstream_tables(lineage_dict, "b")
    assert downstream_tables == {
        "c",
        "d",
        "e",
        "f",
    }, f"Downstream tables for b should be c, d, e, f, but got {sorted(downstream_tables)}"


def test_get_db_and_table():
    catalog, db, table = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:databricks,datamodel_core.dim_devices,PROD)"
    )
    assert catalog is None
    assert db == "datamodel_core"
    assert table == "dim_devices"

    catalog, db, table = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:dbt,edw.finance_strategy_gold.agg_analytics_arr,PROD)"
    )
    assert catalog == "edw"
    assert db == "finance_strategy_gold"
    assert table == "agg_analytics_arr"

    catalog, db, table = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:dbt,databricks.edw.finance_strategy_gold.agg_analytics_arr,PROD)"
    )
    assert catalog == "edw"
    assert db == "finance_strategy_gold"
    assert table == "agg_analytics_arr"

    res = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:dbt,databricks.edw.finance_strategy_gold.agg_analytics_arr,PROD)",
        return_type="tuple",
    )
    assert res == ("edw", "finance_strategy_gold", "agg_analytics_arr")

    res = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:dbt,bted_edw.edw.finance_strategy_gold.agg_analytics_arr,PROD)",
        return_type="tuple",
    )
    assert res == ("edw", "finance_strategy_gold", "agg_analytics_arr")

    res = get_db_and_table(
        "urn:li:dataset:(urn:li:dataPlatform:dbt,MarketingAnalytics.edw.gtm_gold.attribution_gold,PROD)",
        return_type="tuple",
    )
    assert res == ("edw", "gtm_gold", "attribution_gold")
