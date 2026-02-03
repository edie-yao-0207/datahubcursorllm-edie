"""Tests for usage automation logic in product_usage_utils.py"""

import json
import os

import pytest
from dataweb.userpkgs.product_usage_utils import get_final_usage_sql


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


class TestUsageAutomation:
    """Test class for usage automation functionality"""

    @pytest.mark.skipif(
        get_final_usage_sql is None,
        reason="get_final_usage_sql function not available",
    )
    def test_advanced_idling_mixpanel_events(self):
        """Test usage logic for Advanced Idling with Mixpanel events"""
        """If updating the config below, update the corresponding SQL in expected_advanced_idling_mixpanel_events.sql"""
        # Test configuration for Advanced Idling with Mixpanel events
        test_config = {
            "feature_name": "Advanced Idling",
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
                            "LIC-VG-PREMIER",
                            "LIC-VG-PREMIER-PLTFM-ESS",
                            "LIC-VG-PREMIER-PLTFM-PREM",
                            "LIC-VG-PREMIER-PS",
                            "LIC-VG-PS",
                        ],
                        "license_required": True,
                    },
                    "feature_flag_name": ["advanced-idling-report"],
                    "release_phase": "GA",
                    "release_types": ["Early Adopter", "Phase 1"],
                }
            ],
            "usage_array": [
                {
                    "effective_date": "2024-01-01",
                    "mixpanel_metric_names": [
                        "fleet_reports_advanced_idling_filter_change",
                        "fleet_reports_advanced_idling_heat_map_zoom",
                        "fleet_reports_advanced_idling_tab_change",
                        "org_config_fuel_advanced_idling_settings_saved",
                    ],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_usage_sql(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("advanced_idling_mixpanel_events")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_usage_sql is None,
        reason="get_final_usage_sql function not available",
    )
    def test_driver_efficiency_cloud_routes(self):
        """Test usage logic for Driver Efficiency with Cloud routes"""
        """If updating the config below, update the corresponding SQL in expected_driver_efficiency_cloud_routes.sql"""
        # Test configuration for Driver Efficiency with Cloud routes
        test_config = {
            "feature_name": "Driver Efficiency Report",
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
                            "LIC-VG-PREMIER",
                            "LIC-VG-PREMIER-PLTFM-ESS",
                            "LIC-VG-PREMIER-PLTFM-PREM",
                            "LIC-VG-PREMIER-PS",
                            "LIC-VG-PS",
                        ],
                        "license_required": True,
                    },
                    "release_phase": "GA",
                }
            ],
            "usage_array": [
                {
                    "effective_date": "2024-01-01",
                    "route_names": [
                        "fleet_driver_efficiency_profile_bounds",
                        "fleet_driver_efficiency_profile",
                        "fleet_driver_efficiency_assign_vehicles",
                        "fleet_driver_efficiency_profile_weights",
                        "driver_efficiency",
                        "driver_efficiency_weights",
                        "driver_efficiency_bounds",
                        "driver_efficiency_profile",
                        "driver_efficiency_assign_vehicles",
                        "driver_efficiency_profile_weights",
                        "driver_efficiency_profile_bounds",
                    ],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_usage_sql(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("driver_efficiency_cloud_routes")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_usage_sql is None,
        reason="get_final_usage_sql function not available",
    )
    def test_performance_overview_both_routes_and_mixpanel(self):
        """Test usage logic for Performance Overview with both Cloud routes and Mixpanel events"""
        """If updating the config below, update the corresponding SQL in expected_performance_overview_both_routes_and_mixpanel.sql"""
        # Test configuration for Performance Overview with both Cloud routes and Mixpanel events
        test_config = {
            "feature_name": "Performance Overview",
            "enablement_array": [
                {
                    "effective_date": "2024-01-01",
                    "feature_flag_name": [
                        "show-coaching-intelligence-supervisor-focus-tab"
                    ],
                    "release_phase": "Closed Beta",
                }
            ],
            "usage_array": [
                {
                    "effective_date": "2024-01-01",
                    "route_names": ["fleet_supervisor_insights"],
                    "mixpanel_metric_names": [
                        "supervisor_view_safety_score_navigation_click",
                        "supervisor_view_ecodriving_score_navigation_click",
                        "supervisor_insights_drivers_to_recognize_click_driver_name",
                        "supervisor_insights_drivers_requiring_attention_click_send_to_coaching",
                        "supervisor_insights_drivers_requiring_attention_click_send_message",
                        "supervisor_insights_drivers_requiring_attention_click_driver_name",
                        "supervisor_insights_coaching_timeliness_click_send_notification",
                        "supervisor_insights_coaching_effectiveness_click_view_drivers",
                        "supervisor_insights_training_timeliness_click_send_message",
                        "positive_recognition_kudos_send_kudos",
                    ],
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_usage_sql(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql(
            "performance_overview_both_routes_and_mixpanel"
        )

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"

    @pytest.mark.skipif(
        get_final_usage_sql is None,
        reason="get_final_usage_sql function not available",
    )
    def test_acr_custom_query(self):
        """Test usage logic for ACR with custom query"""
        """If updating the config below, update the corresponding SQL in expected_acr_custom_query.sql"""
        # Test configuration for ACR with custom query
        test_config = {
            "feature_name": "ACR",
            "enablement_array": [{"effective_date": "2024-01-01", "serves_all": True}],
            "usage_array": [
                {
                    "effective_date": "2024-01-01",
                    "custom_query": {
                        "sql_file": "acr.sql",
                        "eu_sql_file": "acr_eu.sql",
                        "filter_internal_usage": True,
                        "has_user_id_field": True,
                        "usage_field": "usage_id",
                    },
                }
            ],
        }

        partition_start = "2025-09-15"
        result_sql = get_final_usage_sql(test_config, partition_start)

        # Format the SQL for better readability
        formatted_sql = format_sql(result_sql)

        # Load expected SQL from testfiles directory
        expected_sql = load_expected_sql("acr_custom_query")

        # Compare the full SQL output
        assert (
            formatted_sql == expected_sql
        ), f"SQL mismatch:\nExpected:\n{expected_sql}\n\nActual:\n{formatted_sql}"
