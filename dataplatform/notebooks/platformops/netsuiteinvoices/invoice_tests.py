# MAGIC %run ./invoice_functions

# COMMAND ----------

from datetime import datetime
from inspect import isclass
import unittest
from unittest.mock import MagicMock

import pandas as pd
from pyspark.sql import SparkSession

# tests do not need to log metrics
global LOG_METRICS_TO_DATADOG
LOG_METRICS_TO_DATADOG = False


# We're using the following decorators to mock functions from the
# invoice_functions file. The decorators are helpful because in
# order to mock the functions we have to overwrite their value.
# The decorators always set the function back to its original value
# so that other tests can use the function without it being mocked.
def mock_get_invoice_data(f):
    def wrap(*args, **kwargs):
        global get_invoice_data
        orig = get_invoice_data
        get_invoice_data = MagicMock(name="get_invoice_data")
        out = f(*args, **kwargs)
        get_invoice_data = orig
        return out

    return wrap


def mock_get_consolidated_invoice_data(f):
    def wrap(*args, **kwargs):
        global get_consolidated_invoice_data
        orig = get_consolidated_invoice_data
        get_consolidated_invoice_data = MagicMock(name="get_consolidated_invoice_data")
        out = f(*args, **kwargs)
        get_consolidated_invoice_data = orig
        return out

    return wrap


def mock_get_current_region_org_samnumbers_set(f):
    def wrap(*args, **kwargs):
        global get_current_region_org_samnumbers_set
        orig = get_current_region_org_samnumbers_set
        get_current_region_org_samnumbers_set = MagicMock(
            name="get_current_region_org_samnumbers_set"
        )
        out = f(*args, **kwargs)
        get_current_region_org_samnumbers_set = orig
        return out

    return wrap


def mock_update_ci_invoice_map(f):
    def wrap(*args, **kwargs):
        global update_ci_invoice_map
        orig = update_ci_invoice_map
        update_ci_invoice_map = MagicMock(name="update_ci_invoice_map")
        out = f(*args, **kwargs)
        update_ci_invoice_map = orig
        return out

    return wrap


def mock_process_invoices_for_sam(f):
    def wrap(*args, **kwargs):
        global process_invoices_for_sam
        orig = process_invoices_for_sam
        process_invoices_for_sam = MagicMock(name="process_invoices_for_sam")
        out = f(*args, **kwargs)
        process_invoices_for_sam = orig
        return out

    return wrap


# The main idea for each testCase is to define the tests for
# a single function. So 1 test case class maps to a single
# function we're testing. Each method in the class defines a
# specific case we want to check.
class TestUpdateInvoices(unittest.TestCase):
    @mock_get_consolidated_invoice_data
    @mock_get_invoice_data
    def test_only_ci_only_call_get_cis(self):
        global ONLY_CONSOLIDATED_INVOICES
        orig = ONLY_CONSOLIDATED_INVOICES
        ONLY_CONSOLIDATED_INVOICES = True

        update_invoices(MagicMock(spec=SparkSession))
        get_consolidated_invoice_data.assert_called_once()
        self.assertFalse(get_invoice_data.called)

        ONLY_CONSOLIDATED_INVOICES = orig

    @mock_get_consolidated_invoice_data
    @mock_get_invoice_data
    def test_no_options_call_get_invoices_and_get_cis(self):
        update_invoices(MagicMock(spec=SparkSession))
        get_consolidated_invoice_data.assert_called_once()
        get_invoice_data.assert_called_once()

    @mock_get_consolidated_invoice_data
    @mock_get_invoice_data
    def test_skip_ci_only_call_get_invoices(self):
        global SKIP_CONSOLIDATED_INVOICES
        orig = SKIP_CONSOLIDATED_INVOICES
        SKIP_CONSOLIDATED_INVOICES = True

        update_invoices(MagicMock(spec=SparkSession))
        get_invoice_data.assert_called_once()
        self.assertFalse(get_consolidated_invoice_data.called)

        SKIP_CONSOLIDATED_INVOICES = orig

    @mock_get_consolidated_invoice_data
    @mock_get_invoice_data
    @mock_get_current_region_org_samnumbers_set
    @mock_update_ci_invoice_map
    @mock_process_invoices_for_sam
    def test_invoices_not_in_region_doesnt_process_invoices(self):
        current_region_sam = "us-sam"
        other_region_sam = "eu-sam"
        now = datetime.now()

        mock_data = {
            "sam_number": [other_region_sam],
            "_fivetran_synced": [now],
            "create_date": [now],
        }
        mock_invoices = pd.DataFrame(mock_data)
        mock_cis = pd.DataFrame(mock_data)
        get_current_region_org_samnumbers_set.return_value = set([current_region_sam])
        get_invoice_data.return_value = mock_invoices
        get_consolidated_invoice_data.return_value = mock_cis

        update_invoices(MagicMock(spec=SparkSession))

        get_current_region_org_samnumbers_set.assert_called_once()
        update_ci_invoice_map.assert_called_once()

        ci_df = process_invoices_for_sam.call_args[0][0]
        i_df = process_invoices_for_sam.call_args[0][1]

        self.assertTrue(ci_df.empty and i_df.empty)

    @mock_get_consolidated_invoice_data
    @mock_get_invoice_data
    @mock_get_current_region_org_samnumbers_set
    @mock_update_ci_invoice_map
    @mock_process_invoices_for_sam
    def test_invoices_in_region_does_process_invoices(self):
        current_region_sam = "us-sam"
        now = datetime.now()
        mock_data = {
            "sam_number": [current_region_sam],
            "_fivetran_synced": [now],
            "create_date": [now],
        }
        mock_invoices = pd.DataFrame(mock_data)
        mock_cis = pd.DataFrame(mock_data)
        get_current_region_org_samnumbers_set.return_value = set([current_region_sam])
        get_invoice_data.return_value = mock_invoices
        get_consolidated_invoice_data.return_value = mock_cis

        update_invoices(MagicMock(spec=SparkSession))

        get_current_region_org_samnumbers_set.assert_called_once()
        update_ci_invoice_map.assert_called_once()

        ci_df = process_invoices_for_sam.call_args[0][0]
        i_df = process_invoices_for_sam.call_args[0][1]

        self.assertTrue(len(ci_df.index) == 1)
        self.assertTrue(len(i_df.index) == 1)


class TestGetConsolidatedInvoiceData(unittest.TestCase):
    def test_query_throws_exception_returns_empty_dataframe(self):
        spark = MagicMock(spec=SparkSession)
        mock_sql = MagicMock()

        mock_sql.toPandas = MagicMock(side_effect=StopIteration)
        spark.sql.return_value = mock_sql

        actual_result = get_consolidated_invoice_data(spark)
        mock_sql.toPandas.assert_called_once()
        self.assertTrue(actual_result.empty)

    def test_query_succeeds_returns_results(self):
        spark = MagicMock(spec=SparkSession)
        mock_sql = MagicMock()
        expected_result = pd.DataFrame(
            {"transaction_id": [1, 2, 3], "amount_remaining": [4, 5, 6]}
        )
        mock_sql.toPandas = MagicMock(return_value=expected_result)
        spark.sql.return_value = mock_sql

        actual_result = get_consolidated_invoice_data(spark)
        mock_sql.toPandas.assert_called_once()
        self.assertTrue(expected_result.equals(actual_result))


class TestGetInvoiceData(unittest.TestCase):
    def test_query_throws_exception_return_empty_dataframe(self):
        spark = MagicMock(spec=SparkSession)
        mock_sql = MagicMock()
        mock_sql.toPandas = MagicMock(side_effect=StopIteration)
        spark.sql.return_value = mock_sql

        actual_result = get_invoice_data(spark)
        mock_sql.toPandas.assert_called_once()
        self.assertTrue(actual_result.empty)

    def test_query_succeeds_returns_results(self):
        spark = MagicMock(spec=SparkSession)
        mock_sql = MagicMock()
        expected_result = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        mock_sql.toPandas = MagicMock(return_value=expected_result)
        spark.sql.return_value = mock_sql

        actual_result = get_invoice_data(spark)
        mock_sql.toPandas.assert_called_once()
        self.assertTrue(expected_result.equals(actual_result))


fs = list(
    globals().values()
)  # Do this to hack around the "dict size changed during iteration"


tests = [
    unittest.TestLoader().loadTestsFromTestCase(t)
    for t in fs
    if isclass(t) and issubclass(t, unittest.TestCase)
]
suite = unittest.TestSuite(tests)
runner = unittest.TextTestRunner()
runner_output = runner.run(suite)

failed_tests = (
    len(runner_output.errors)
    + len(runner_output.failures)
    + len(runner_output.unexpectedSuccesses)
)
assert failed_tests == 0
