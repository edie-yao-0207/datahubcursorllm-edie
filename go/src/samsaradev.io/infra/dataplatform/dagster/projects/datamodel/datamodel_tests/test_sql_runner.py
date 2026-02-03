import pytest
from datamodel.common.llm.sql_runner import validate_query


def test_valid_select_query():
    query = "SELECT * FROM users"
    validate_query(query)  # Should not raise an exception


def test_invalid_non_select_query():
    query = "DELETE FROM users"
    with pytest.raises(ValueError) as excinfo:
        validate_query(query)
    assert "Query must be a SELECT statement" in str(excinfo.value)


def test_forbidden_statement_in_query():
    query = "SELECT * FROM users; DROP TABLE users"
    with pytest.raises(ValueError) as excinfo:
        validate_query(query)
    assert "Query contains a forbidden DROP statement" in str(excinfo.value)


def test_multiple_statements():
    query = "SELECT * FROM users; SELECT * FROM orders"
    with pytest.raises(ValueError) as excinfo:
        validate_query(query)
    assert "Multiple SQL statements are not allowed" in str(excinfo.value)


def test_valid_select_with_semicolon():
    query = "SELECT * FROM users;"
    validate_query(query)  # Should not raise an exception


def test_with_cte():
    query = """
    WITH users_cte AS (
        SELECT * FROM users
    )
    SELECT * FROM users_cte;
    """
    validate_query(query)  # Should not raise an exception
