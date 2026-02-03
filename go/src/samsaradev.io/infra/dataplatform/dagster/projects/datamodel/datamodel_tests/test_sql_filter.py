import pytest
from datamodel.common.utils import add_sql_partition_filter, replace_timetravel_string

DEFAULT_DATE_FILTER = "date = '2023-01-05'"


def test_basic_where_clause():
    input_query = "SELECT * FROM table_1 WHERE b = 2 AND c = 3"
    expected_output = (
        "SELECT * FROM table_1 WHERE date = '2023-01-05' and b = 2 AND c = 3"
    )
    assert (
        add_sql_partition_filter(input_query, DEFAULT_DATE_FILTER).lower()
        == expected_output.lower()
    )


def test_single_where_clause():
    input_query = "SELECT * FROM table_1 WHERE d = 4"
    expected_output = "SELECT * FROM table_1 WHERE date = '2023-01-05' and d = 4"
    assert (
        add_sql_partition_filter(input_query, DEFAULT_DATE_FILTER).lower()
        == expected_output.lower()
    )


def test_no_where_clause():
    input_query = "SELECT * FROM table_1"
    expected_output = "SELECT * FROM table_1 WHERE date = '2023-01-05'"
    assert (
        add_sql_partition_filter(input_query, DEFAULT_DATE_FILTER).lower()
        == expected_output.lower()
    )


def test_case_insensitive():
    input_query = "select * from table_1 where b = 2 and c = 3"
    expected_output = (
        "select * from table_1 where date = '2023-01-05' and b = 2 and c = 3"
    )
    assert (
        add_sql_partition_filter(input_query, DEFAULT_DATE_FILTER).lower()
        == expected_output.lower()
    )


# Run this test case with semicolon at the end of the query
def test_no_where_clause_with_semicolon():
    input_query = "SELECT * FROM table_1;"
    expected_output = "SELECT * FROM table_1 WHERE date = '2023-01-05';"
    assert (
        add_sql_partition_filter(input_query, DEFAULT_DATE_FILTER).lower()
        == expected_output.lower()
    )


def test_replace_timetravel_string():
    sql_query = "SELECT * FROM table@2022-01-01 WHERE column = 'value'@2022-02-02"
    expected_result = "SELECT * FROM table WHERE column = 'value'"
    assert replace_timetravel_string(sql_query) == expected_result

    sql_query = "SELECT * FROM table WHERE column = 'value'@2022-03-03"
    expected_result = "SELECT * FROM table WHERE column = 'value'"
    assert replace_timetravel_string(sql_query) == expected_result

    sql_query = "SELECT * FROM table@2022-04-04"
    expected_result = "SELECT * FROM table"
    assert replace_timetravel_string(sql_query) == expected_result

    sql_query = "SELECT * FROM table"
    expected_result = "SELECT * FROM table"
    assert replace_timetravel_string(sql_query) == expected_result
