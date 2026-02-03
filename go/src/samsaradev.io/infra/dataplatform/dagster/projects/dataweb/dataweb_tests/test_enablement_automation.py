"""Tests for enablement automation logic in product_usage_utils.py"""

import json
import os

import pytest
from dataweb.userpkgs.product_usage_utils import get_final_relevant_orgs


def format_sql(sql_string):
    """Simple SQL formatting function"""
    # Basic formatting - add newlines after major keywords
    formatted = sql_string.replace(" AS (", "\n    AS (")
    formatted = formatted.replace("SELECT", "\nSELECT")
    formatted = formatted.replace("FROM", "\nFROM")
    formatted = formatted.replace("WHERE", "\nWHERE")
    formatted = formatted.replace("UNION", "\nUNION")
    formatted = formatted.replace("JOIN", "\nJOIN")
    formatted = formatted.replace("LEFT OUTER JOIN", "\nLEFT OUTER JOIN")
    formatted = formatted.replace("AND", "\n    AND")
    formatted = formatted.replace("OR", "\n    OR")

    # Clean up extra whitespace
    lines = [line.strip() for line in formatted.split("\n") if line.strip()]
    return "\n".join(lines)


def load_expected_sql(test_name):
    """Load expected SQL from testfiles directory"""
    testfiles_dir = os.path.join(os.path.dirname(__file__), "testfiles")
    sql_file = os.path.join(testfiles_dir, f"expected_{test_name}.sql")

    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"Expected SQL file not found: {sql_file}")

    with open(sql_file, "r") as f:
        return f.read().strip()


class TestEnablementAutomation:
    """Test class for enablement automation functionality"""

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_serves_all_enablement(self):
        """Test enablement logic when serves_all is True"""
        """If updating the config below, update the corresponding SQL in expected_serves_all_enablement.sql"""
        # Test configuration where the product serves all organizations
        test_config = {
            "feature_name": "ACR",
            "enablement_array": [{"effective_date": "2024-01-01", "serves_all": True}],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("serves_all_enablement")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_closed_beta_launchdarkly_only(self):
        """Test enablement logic for Closed Beta with LaunchDarkly only"""
        """If updating the config below, update the corresponding SQL in expected_closed_beta_launchdarkly.sql"""
        test_config = {
            "feature_name": "Jamming Maps",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "feature_flag_name": ["show-gps-jamming-overlay"],
                    "release_phase": "Closed Beta",
                    "release_types": ["Early Adopter"],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("closed_beta_launchdarkly")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_open_beta_with_licenses(self):
        """Test enablement logic for Open Beta with license requirements"""
        """If updating the config below, update the corresponding SQL in expected_open_beta_with_licenses.sql"""
        test_config = {
            "feature_name": "Engine Immobilizer",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "feature_flag_name": [
                        "expose-workflow-action-engine-immobilization"
                    ],
                    "release_phase": "Open Beta",
                    "licenses": {"skus": ["LIC-EI-SEC"], "license_required": True},
                    "release_types": ["Early Adopter", "Phase 1"],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("open_beta_with_licenses")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_ga_with_licenses_and_feature_flag(self):
        """Test enablement logic for GA with licenses and feature flag exclusions"""
        """If updating the config below, update the corresponding SQL in expected_ga_with_licenses_and_feature_flag.sql"""
        test_config = {
            "feature_name": "GA Feature with Licenses and FF",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "feature_flag_name": ["unified-coaching-enabled"],
                    "licenses": {
                        "skus": [
                            "LIC-VG-ENT",
                            "LIC-VG-EXPRESS",
                            "LIC-VG-LD",
                            "LIC-VG-NOELD",
                            "LIC-VG-PS",
                            "LIC-VG-ESS-PLUS-PLTFM-ESS",
                            "LIC-VG-ESS-PLUS-PLTFM-PREM",
                            "LIC-VG-ENT-PLTFM-PREM",
                            "LIC-VG-ENT-PLTFM-ESS",
                            "LIC-VG-COMPLIANCE",
                        ],
                        "license_required": True,
                        "add_on_skus": ["LIC-VG-COMPLIANCE"],
                    },
                    "release_phase": "GA",
                    "release_types": ["Early Adopter", "Phase 1"],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("ga_with_licenses_and_feature_flag")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_ga_license_only_no_feature_flag(self):
        """Test enablement logic for GA with licenses only (no feature flag)"""
        """If updating the config below, update the corresponding SQL in expected_ga_license_only_no_feature_flag.sql"""
        test_config = {
            "feature_name": "GA Feature License Only",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "licenses": {
                        "skus": [
                            "LIC-VG-ENT",
                            "LIC-VG-ENT-PLTFM-ESS",
                            "LIC-VG-ENT-PLTFM-PREM",
                            "LIC-VG-ENTERPRISE",
                            "LIC-VG-EXPRESS",
                            "LIC-VG-LD",
                            "LIC-VG-NOELD",
                            "LIC-VG-PREM-PLTFM-ESS",
                            "LIC-VG-PREM-PLTFM-PREM",
                            "LIC-VG-PS",
                        ],
                        "license_required": True,
                    },
                    "release_phase": "GA",
                    "locales": ["us", "ca"],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("ga_license_only_no_feature_flag")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_relevant_orgs is None,
        reason="get_final_relevant_orgs function not available",
    )
    def test_open_beta_license_not_required(self):
        """Test enablement logic for Open Beta where license is not required"""
        """If updating the config below, update the corresponding SQL in expected_open_beta_license_not_required.sql"""
        test_config = {
            "feature_name": "Open Beta No License Required",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "licenses": {
                        "skus": [
                            "LIC-VG-ENT",
                            "LIC-VG-ENT-PLTFM-ESS",
                            "LIC-VG-ENT-PLTFM-PREM",
                            "LIC-VG-ENTERPRISE",
                            "LIC-VG-ESS-PLTFM-ESS",
                            "LIC-VG-ESS-PLTFM-PREM",
                            "LIC-VG-ESS-PLUS-PLTFM-ESS",
                            "LIC-VG-ESS-PLUS-PLTFM-PREM",
                            "LIC-VG-EXPRESS",
                            "LIC-VG-LD",
                            "LIC-VG-NOELD",
                            "LIC-VG-PREM-PLTFM-ESS",
                            "LIC-VG-PREM-PLTFM-PREM",
                            "LIC-VG-PREMIER-PLTFM-ESS",
                            "LIC-VG-PREMIER-PLTFM-PREM",
                            "LIC-VG-PREMIER-PS",
                            "LIC-VG-PS",
                        ],
                        "license_required": False,
                    },
                    "feature_flag_name": ["asset-utilization-v2-enabled"],
                    "release_phase": "Open Beta",
                    "release_types": ["Early Adopter", "Phase 1"],
                }
            ],
        }

        partition_start = "2024-01-15"
        result_sql = get_final_relevant_orgs(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("open_beta_license_not_required")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"
