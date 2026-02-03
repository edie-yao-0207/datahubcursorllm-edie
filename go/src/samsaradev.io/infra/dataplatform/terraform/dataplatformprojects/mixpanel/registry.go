package mixpanel

type MixpanelTableSpec struct {
	S3Directory string `json:"s3_directory"`
	TableName   string `json:"table_name"`
}

var MixpanelTableRegistry = []MixpanelTableSpec{
	{
		S3Directory: "cloud_route",
		TableName:   "cloud_route",
	},
	{
		S3Directory: "fleet_safety_coaching_sessions_click_begin_sessions",
		TableName:   "fleet_safety_coaching_sessions_click_begin_sessions",
	},
	{
		S3Directory: "fleet_safety_coaching_sessions_complete_session",
		TableName:   "fleet_safety_coaching_sessions_complete_session",
	},
	{
		S3Directory: "fleet_safety_coaching_sessions_click_complete_behavior",
		TableName:   "fleet_safety_coaching_sessions_click_complete_behavior",
	},
	{
		S3Directory: "vr4_modalbeingviewed_count",
		TableName:   "vr4_modalbeingviewed_count",
	},
	{
		S3Directory: "vr4_retrievalconfirmclicked_count",
		TableName:   "vr4_retrievalConfirmClicked_count",
	},
	{
		S3Directory: "troy_admin_global_navigate",
		TableName:   "troy_admin_global_navigate",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_click",
		TableName:   "fleet_benchmarks_edit_peer_group_click",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_create_segment_from_tag",
		TableName:   "fleet_benchmarks_edit_peer_group_create_segment_from_tag",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_edit_org",
		TableName:   "fleet_benchmarks_edit_peer_group_edit_org",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_edit_existing_tag_group",
		TableName:   "fleet_benchmarks_edit_peer_group_edit_existing_tag_group",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_open_category",
		TableName:   "fleet_benchmarks_edit_peer_group_open_category",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_category_value_selected",
		TableName:   "fleet_benchmarks_edit_peer_group_category_value_selected",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_next_clicked",
		TableName:   "fleet_benchmarks_edit_peer_group_next_clicked",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_back_clicked",
		TableName:   "fleet_benchmarks_edit_peer_group_back_clicked",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_close",
		TableName:   "fleet_benchmarks_edit_peer_group_close",
	},
	{
		S3Directory: "fleet_benchmarks_edit_peer_group_submit",
		TableName:   "fleet_benchmarks_edit_peer_group_submit",
	},
	{
		S3Directory: "fleet_benchmarks_change_peer_group_click",
		TableName:   "fleet_benchmarks_change_peer_group_click",
	},
	{
		S3Directory: "fleet_benchmarks_change_peer_group_submit",
		TableName:   "fleet_benchmarks_change_peer_group_submit",
	},
	{
		S3Directory: "fleet_benchmarks_change_peer_group_close",
		TableName:   "fleet_benchmarks_change_peer_group_close",
	},
	{
		S3Directory: "fleet_benchmarks_metric_cards_peer_group_distribution_clicked",
		TableName:   "fleet_benchmarks_metric_cards_peer_group_distribution_clicked",
	},
	{
		S3Directory: "fleet_benchmarks_metric_cards_trend_clicked",
		TableName:   "fleet_benchmarks_metric_cards_trend_clicked",
	},
	{
		S3Directory: "fleet_benchmarks_metric_cards_view_report_clicked",
		TableName:   "fleet_benchmarks_metric_cards_view_report_clicked",
	},
	{
		S3Directory: "fleet_benchmarks_filters_change_date",
		TableName:   "fleet_benchmarks_filters_change_date",
	},
	{
		S3Directory: "fleet_benchmarks_compare_compare_modal_appeared",
		TableName:   "fleet_benchmarks_compare_compare_modal_appeared",
	},
	{
		S3Directory: "fleet_benchmarks_compare_compare_occurred",
		TableName:   "fleet_benchmarks_compare_compare_occurred",
	},
	{
		S3Directory: "fleet_benchmarks_compare_compare_modal_cancelled",
		TableName:   "fleet_benchmarks_compare_compare_modal_cancelled",
	},
	{
		S3Directory: "fleet_benchmarks_compare_clear_selections",
		TableName:   "fleet_benchmarks_compare_clear_selections",
	},
	{
		S3Directory: "fleet_reports_safety_inbox_click_safety_event",
		TableName:   "fleet_reports_safety_inbox_click_safety_event",
	},
	{
		S3Directory: "fleet_safety_safety_media_display_viewed",
		TableName:   "fleet_safety_safety_media_display_viewed",
	},
	{
		S3Directory: "browsev2togglenodeevent",
		TableName:   "browsev2togglenodeevent",
	},
	{
		S3Directory: "createviewevent",
		TableName:   "createviewevent",
	},
	{
		S3Directory: "entitysectionviewevent",
		TableName:   "entitysectionviewevent",
	},
	{
		S3Directory: "entityviewevent",
		TableName:   "entityviewevent",
	},
	{
		S3Directory: "homepagebrowseresultclickevent",
		TableName:   "homepagebrowseresultclickevent",
	},
	{
		S3Directory: "homepagesearchevent",
		TableName:   "homepagesearchevent",
	},
	{
		S3Directory: "homepageviewevent",
		TableName:   "homepageviewevent",
	},
	{
		S3Directory: "recommendationclickevent",
		TableName:   "recommendationclickevent",
	},
	{
		S3Directory: "recommendationimpressionevent",
		TableName:   "recommendationimpressionevent",
	},
	{
		S3Directory: "searchacrosslineageresultsviewevent",
		TableName:   "searchacrosslineageresultsviewevent",
	},
	{
		S3Directory: "searchevent",
		TableName:   "searchevent",
	},
	{
		S3Directory: "selectautocompleteoption",
		TableName:   "selectautocompleteoption",
	},
	{
		S3Directory: "updateviewevent",
		TableName:   "updateviewevent",
	},
	{
		S3Directory: "visuallineageexpandgraphevent",
		TableName:   "visuallineageexpandgraphevent",
	},
	{
		S3Directory: "visuallineageviewevent",
		TableName:   "visuallineageviewevent",
	},
	{
		S3Directory: "searchresultstryagainclickevent",
		TableName:   "searchresultstryagainclickevent",
	},
	{
		S3Directory: "searchresultsviewevent",
		TableName:   "searchresultsviewevent",
	},
	{
		S3Directory: "searchresultclickevent",
		TableName:   "searchresultclickevent",
	},
	{
		S3Directory: "cloud_components_banner_load",
		TableName:   "cloud_components_banner_load",
	},
	{
		S3Directory: "cloud_components_banner_dismiss",
		TableName:   "cloud_components_banner_dismiss",
	},
	{
		S3Directory: "ui_containers_page_header_star_page",
		TableName:   "ui_containers_page_header_star_page",
	},
	{
		S3Directory: "ui_containers_page_header_unstar_page",
		TableName:   "ui_containers_page_header_unstar_page",
	},
	{
		S3Directory: "fleet_benchmarks_cohorts_panel_toggle",
		TableName:   "fleet_benchmarks_cohorts_panel_toggle",
	},
	{
		S3Directory: "fleet_benchmarks_compare_create_peer_groups",
		TableName:   "fleet_benchmarks_compare_create_peer_groups",
	},
	{
		S3Directory: "fleet_benchmarks_filters_open_benchmark_dropdown",
		TableName:   "fleet_benchmarks_filters_open_benchmark_dropdown",
	},
	{
		S3Directory: "fleet_benchmarks_metric_cards_peer_group_distribution_clicked",
		TableName:   "fleet_benchmarks_metric_cards_peer_group_distribution_clicked",
	},
	{
		S3Directory: "report_export_download_csv",
		TableName:   "report_export_download_csv",
	},
	{
		S3Directory: "report_export_email_csv",
		TableName:   "report_export_email_csv",
	},
	{
		S3Directory: "spotlight_close",
		TableName:   "spotlight_close",
	},
	{
		S3Directory: "spotlight_open",
		TableName:   "spotlight_open",
	},
	{
		S3Directory: "spotlight_select",
		TableName:   "spotlight_select",
	},
	{
		S3Directory: "spotlight_select_preview",
		TableName:   "spotlight_select_preview",
	},
	{
		S3Directory: "spotlight_search",
		TableName:   "spotlight_search",
	},
	{
		S3Directory: "spotlight_preview",
		TableName:   "spotlight_preview",
	},
	{
		S3Directory: "global_search_focus",
		TableName:   "global_search_focus",
	},
	{
		S3Directory: "global_search_dismiss",
		TableName:   "global_search_dismiss",
	},
	{
		S3Directory: "global_search_search",
		TableName:   "global_search_search",
	},
	{
		S3Directory: "global_search_preview",
		TableName:   "global_search_preview",
	},
	{
		S3Directory: "global_search_select",
		TableName:   "global_search_select",
	},
	{
		S3Directory: "ai_chat_floating_init",
		TableName:   "ai_chat_floating_init",
	},
	{
		S3Directory: "ai_chat_send",
		TableName:   "ai_chat_send",
	},
	{
		S3Directory: "ai_chat_floating_open",
		TableName:   "ai_chat_floating_open",
	},
	{
		S3Directory: "in_context_help_submit_ticket_click",
		TableName:   "in_context_help_submit_ticket_click",
	},
	{
		S3Directory: "in_context_help_call_click",
		TableName:   "in_context_help_call_click",
	},
	{
		S3Directory: "in_context_help_visit_help_center_click",
		TableName:   "in_context_help_visit_help_center_click",
	},
	{
		S3Directory: "ai_chat_request_cancelled",
		TableName:   "ai_chat_request_cancelled",
	},
	{
		S3Directory: "ai_chat_first_token",
		TableName:   "ai_chat_first_token",
	},
	{
		S3Directory: "fleet_reports_acr_created_by_samsara",
		TableName:   "fleet_reports_acr_created_by_samsara",
	},
	{
		S3Directory: "fleet_reports_acr_custom_reports",
		TableName:   "fleet_reports_acr_custom_reports",
	},
	{
		S3Directory: "fleet_reports_acr_scheduled_reports",
		TableName:   "fleet_reports_acr_scheduled_reports",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_open_create_modal",
		TableName:   "fleet_reports_advanced_custom_reports_open_create_modal",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_save_report",
		TableName:   "fleet_reports_advanced_custom_reports_save_report",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_select_dataset",
		TableName:   "fleet_reports_advanced_custom_reports_select_dataset",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_select_template",
		TableName:   "fleet_reports_advanced_custom_reports_select_template",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_create_report_from_ai",
		TableName:   "fleet_reports_advanced_custom_reports_create_report_from_ai",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_reports_run_report",
		TableName:   "fleet_reports_advanced_custom_reports_run_report",
	},
	{
		S3Directory: "fleet_scheduled_reports_save_report_config",
		TableName:   "fleet_scheduled_reports_save_report_config",
	},
	{
		S3Directory: "fleet_vehicle_show_faults_open_fault_description_modal",
		TableName:   "fleet_vehicle_show_faults_open_fault_description_modal",
	},
	{
		S3Directory: "fleet_maintenance_status_open_fault_description_modal",
		TableName:   "fleet_maintenance_status_open_fault_description_modal",
	},
	{
		S3Directory: "self_serve_feature",
		TableName:   "self_serve_feature",
	},
	{
		S3Directory: "fleet_reports_safety_aggregated_inbox_switchtooldview",
		TableName:   "fleet_reports_safety_aggregated_inbox_switchtooldview",
	},
	{
		S3Directory: "fleet_reports_safety_inbox_switchtonewview",
		TableName:   "fleet_reports_safety_inbox_switchtonewview",
	},
	{
		S3Directory: "fleet_reports_safety_speeding_inbox_switchtonewview",
		TableName:   "fleet_reports_safety_speeding_inbox_switchtonewview",
	},
	{
		S3Directory: "cloud_route_session",
		TableName:   "cloud_route_session",
	},
	{
		S3Directory: "fleet_reports_advanced_idling_tab_change",
		TableName:   "fleet_reports_advanced_idling_tab_change",
	},
	{
		S3Directory: "fleet_reports_advanced_idling_filter_change",
		TableName:   "fleet_reports_advanced_idling_filter_change",
	},
	{
		S3Directory: "org_config_fuel_advanced_idling_settings_saved",
		TableName:   "org_config_fuel_advanced_idling_settings_saved",
	},
	{
		S3Directory: "fleet_reports_advanced_idling_heat_map_zoom",
		TableName:   "fleet_reports_advanced_idling_heat_map_zoom",
	},
	{
		S3Directory: "fleet_reports_safety_aggregated_inbox_toolbar_click_send_to_coaching",
		TableName:   "fleet_reports_safety_aggregated_inbox_toolbar_click_send_to_coaching",
	},
	{
		S3Directory: "supervisor_view_safety_score_navigation_click",
		TableName:   "supervisor_view_safety_score_navigation_click",
	},
	{
		S3Directory: "supervisor_view_ecodriving_score_navigation_click",
		TableName:   "supervisor_view_ecodriving_score_navigation_click",
	},
	{
		S3Directory: "positive_recognition_kudos_send_kudos",
		TableName:   "positive_recognition_kudos_send_kudos",
	},
	{
		S3Directory: "positive_recognition_recognize_button_clicked",
		TableName:   "positive_recognition_recognize_button_clicked",
	},
	{
		S3Directory: "supervisor_insights_drivers_to_recognize_click_driver_name",
		TableName:   "supervisor_insights_drivers_to_recognize_click_driver_name",
	},
	{
		S3Directory: "supervisor_insights_drivers_requiring_attention_click_send_to_coaching",
		TableName:   "supervisor_insights_drivers_requiring_attention_click_send_to_coaching",
	},
	{
		S3Directory: "supervisor_insights_drivers_requiring_attention_click_send_message",
		TableName:   "supervisor_insights_drivers_requiring_attention_click_send_message",
	},
	{
		S3Directory: "supervisor_insights_drivers_requiring_attention_click_driver_name",
		TableName:   "supervisor_insights_drivers_requiring_attention_click_driver_name",
	},
	{
		S3Directory: "supervisor_insights_coaching_timeliness_click_send_notification",
		TableName:   "supervisor_insights_coaching_timeliness_click_send_notification",
	},
	{
		S3Directory: "supervisor_insights_coaching_effectiveness_click_view_drivers",
		TableName:   "supervisor_insights_coaching_effectiveness_click_view_drivers",
	},
	{
		S3Directory: "supervisor_insights_training_timeliness_click_send_message",
		TableName:   "supervisor_insights_training_timeliness_click_send_message",
	},
	{
		S3Directory: "fleet_assetshow_linkedasset_click",
		TableName:   "fleet_assetshow_linkedasset_click",
	},
	{
		S3Directory: "fleet_assetshow_linkedasset_hover",
		TableName:   "fleet_assetshow_linkedasset_hover",
	},
	{
		S3Directory: "fleet_assetmaptooltip_linkedasset_click",
		TableName:   "fleet_assetmaptooltip_linkedasset_click",
	},
	{
		S3Directory: "fleet_assetmaptooltip_linkedasset_hover",
		TableName:   "fleet_assetmaptooltip_linkedasset_hover",
	},
	{
		S3Directory: "fleet_assetlist_linkedasset_click",
		TableName:   "fleet_assetlist_linkedasset_click",
	},
	{
		S3Directory: "fleet_assetlist_linkedasset_hover",
		TableName:   "fleet_assetlist_linkedasset_hover",
	},
	{
		S3Directory: "fleet_address_autocomplete_lsd_coordinate_search",
		TableName:   "fleet_address_autocomplete_lsd_coordinate_search",
	},
	{
		S3Directory: "fleet_address_autocomplete_location_selected",
		TableName:   "fleet_address_autocomplete_location_selected",
	},
	{
		S3Directory: "fleet_lsdoverlay_toggle",
		TableName:   "fleet_lsdoverlay_toggle",
	},
	{
		S3Directory: "live_share",
		TableName:   "live_share",
	},
	{
		S3Directory: "vehicle_show_accordion_toggle",
		TableName:   "vehicle_show_accordion_toggle",
	},
	{
		S3Directory: "fleet_vehicleshow_opendiagnostics",
		TableName:   "fleet_vehicleshow_opendiagnostics",
	},
	{
		S3Directory: "fleet_deviceedit",
		TableName:   "fleet_deviceedit",
	},
	{
		S3Directory: "view_graph_select",
		TableName:   "view_graph_select",
	},
	{
		S3Directory: "client_mfa_email_otp_submitted",
		TableName:   "client_mfa_email_otp_submitted",
	},
	{
		S3Directory: "client_mfa_sms_otp_submitted",
		TableName:   "client_mfa_sms_otp_submitted",
	},
	{
		S3Directory: "iam_user_profile_phone_verified",
		TableName:   "iam_user_profile_phone_verified",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_add_row",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_add_row",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_asset_tag_filter_change",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_asset_tag_filter_change",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_column_append",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_column_append",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_column_prepend",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_column_prepend",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_create",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_create",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_delete",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_delete",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_driver_tag_filter_change",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_driver_tag_filter_change",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_module_select",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_module_select",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_time_filter_change",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_time_filter_change",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_dashboard_update",
		TableName:   "fleet_reports_advanced_custom_dashboard_dashboard_update",
	},
	{
		S3Directory: "fleet_reports_advanced_custom_dashboard_loading_time",
		TableName:   "fleet_reports_advanced_custom_dashboard_loading_time",
	},
	{
		S3Directory: "fleet_threat_details_tooltip_click",
		TableName:   "fleet_threat_details_tooltip_click",
	},
	{
		S3Directory: "smart_maps_weather_forecast_overlay_details_clicked",
		TableName:   "smart_maps_weather_forecast_overlay_details_clicked",
	},
	{
		S3Directory: "weatherforecast_dropdown_select",
		TableName:   "weatherforecast_dropdown_select",
	},
	{
		S3Directory: "weatherforecast_playpause_toggle",
		TableName:   "weatherforecast_playpause_toggle",
	},
	{
		S3Directory: "fleet_constructionzones_toggle",
		TableName:   "fleet_constructionzones_toggle",
	},
	{
		S3Directory: "fleet_publicalerts_toggle",
		TableName:   "fleet_publicalerts_toggle",
	},
	{
		S3Directory: "fleet_public_alert_overlay_click",
		TableName:   "fleet_public_alert_overlay_click",
	},
	{
		S3Directory: "fleet_weather_toggle",
		TableName:   "fleet_weather_toggle",
	},
	{
		S3Directory: "worker_safety_threat_detail_map_card_click",
		TableName:   "worker_safety_threat_detail_map_card_click",
	},
	{
		S3Directory: "worker_safety_threat_detail_click",
		TableName:   "worker_safety_threat_detail_click",
	},
	{
		S3Directory: "fleet_safety_aggregated_inbox_verified_speed_sign_context_label_mismatch",
		TableName:   "fleet_safety_aggregated_inbox_verified_speed_sign_context_label_mismatch",
	},
	{
		S3Directory: "fleet_reports_safety_aggregated_inbox_details_page_speed_sign_verified_label_toggled",
		TableName:   "fleet_reports_safety_aggregated_inbox_details_page_speed_sign_verified_label_toggled",
	},
	{
		S3Directory: "fleet_jammingon_toggle",
		TableName:   "fleet_jammingon_toggle",
	},
	{
		S3Directory: "org_config_safety_speeding_commercial_speed_limits_toggle",
		TableName:   "org_config_safety_speeding_commercial_speed_limits_toggle",
	},
	{
		S3Directory: "fleet_reports_safety_coaching_group_coaching_page_load",
		TableName:   "fleet_reports_safety_coaching_group_coaching_page_load",
	},
	{
		S3Directory: "fleet_dispatch_route_edit_route_save",
		TableName:   "fleet_dispatch_route_edit_route_save",
	},
	{
		S3Directory: "fleet_dispatch_route_edit_job_row_location_update",
		TableName:   "fleet_dispatch_route_edit_job_row_location_update",
	},
	{
		S3Directory: "org_config_safety_adas_roadside_parking_toggle",
		TableName:   "org_config_safety_adas_roadside_parking_toggle",
	},
	{
		S3Directory: "worker_safety_threat_detail_map_card_on_the_ground_tab_click",
		TableName:   "worker_safety_threat_detail_map_card_on_the_ground_tab_click",
	},
	// Competition metrics - Driver App
	{
		S3Directory: "driver_competition_card_clicked",
		TableName:   "driver_competition_card_clicked",
	},
	{
		S3Directory: "driver_competition_details_page_opened",
		TableName:   "driver_competition_details_page_opened",
	},
	{
		S3Directory: "driver_competition_details_page_viewed",
		TableName:   "driver_competition_details_page_viewed",
	},
	{
		S3Directory: "driver_competition_details_page_exited",
		TableName:   "driver_competition_details_page_exited",
	},
	{
		S3Directory: "driver_competition_alert_shown",
		TableName:   "driver_competition_alert_shown",
	},
	{
		S3Directory: "driver_competition_points_info_viewed",
		TableName:   "driver_competition_points_info_viewed",
	},
	{
		S3Directory: "driver_competition_leaderboard_viewed",
		TableName:   "driver_competition_leaderboard_viewed",
	},
	{
		S3Directory: "driver_competition_engagement_started",
		TableName:   "driver_competition_engagement_started",
	},
	{
		S3Directory: "driver_competition_engagement_completed",
		TableName:   "driver_competition_engagement_completed",
	},
	// Competition metrics - Admin UI
	{
		S3Directory: "create_competition_button_pressed",
		TableName:   "create_competition_button_pressed",
	},
	{
		S3Directory: "create_competition_confirmed",
		TableName:   "create_competition_confirmed",
	},
	{
		S3Directory: "competitions_landing_page_visited",
		TableName:   "competitions_landing_page_visited",
	},
	{
		S3Directory: "competition_create_modal_opened",
		TableName:   "competition_create_modal_opened",
	},
	{
		S3Directory: "competition_tab_clicked",
		TableName:   "competition_tab_clicked",
	},
	{
		S3Directory: "competition_clicked",
		TableName:   "competition_clicked",
	},
	{
		S3Directory: "end_competition_clicked",
		TableName:   "end_competition_clicked",
	},
	{
		S3Directory: "end_competition_initiated",
		TableName:   "end_competition_initiated",
	},
	{
		S3Directory: "remove_participant_initiated",
		TableName:   "remove_participant_initiated",
	},
	{
		S3Directory: "remove_participant_clicked",
		TableName:   "remove_participant_clicked",
	},
	{
		S3Directory: "ecodriving_difficultyscore_tooltip_opened",
		TableName:   "ecodriving_difficultyscore_tooltip_opened",
	},
	{
		S3Directory: "ai_chat_advanced_custom_report_chat_entrypoint",
		TableName:   "ai_chat_advanced_custom_report_chat_entrypoint",
	},
	{
		S3Directory: "ai_chat_inline_icon_click",
		TableName:   "ai_chat_inline_icon_click",
	},
	{
		S3Directory: "ai_chat_fault_code_chat_entrypoint",
		TableName:   "ai_chat_fault_code_chat_entrypoint",
	},
	{
		S3Directory: "ai_chat_agent_context_selected",
		TableName:   "ai_chat_agent_context_selected",
	},
	{
		S3Directory: "fleet_reports_data_usage_page_view",
		TableName:   "fleet_reports_data_usage_page_view",
	},
	{
		S3Directory: "fleet_reports_data_usage_month_changed",
		TableName:   "fleet_reports_data_usage_month_changed",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_apps_page_mounted",
		TableName:   "cloud_ui_app_marketplace_apps_page_mounted",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_apps_page_unmounted",
		TableName:   "cloud_ui_app_marketplace_apps_page_unmounted",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_search",
		TableName:   "cloud_ui_app_marketplace_search",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_filter_category_changed",
		TableName:   "cloud_ui_app_marketplace_filter_category_changed",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_filter_region_changed",
		TableName:   "cloud_ui_app_marketplace_filter_region_changed",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_filter_pricing_changed",
		TableName:   "cloud_ui_app_marketplace_filter_pricing_changed",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_app_clicked",
		TableName:   "cloud_ui_app_marketplace_app_clicked",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_enable_clicked",
		TableName:   "cloud_ui_app_marketplace_enable_clicked",
	},
	{
		S3Directory: "cloud_ui_app_marketplace_how_to_enable_clicked",
		TableName:   "cloud_ui_app_marketplace_how_to_enable_clicked",
	},
	// Notification Center metrics
	{
		S3Directory: "notificationcenter_unreadindicatorshown",
		TableName:   "notificationcenter_unreadindicatorshown",
	},
	{
		S3Directory: "notificationcenter_panelopen",
		TableName:   "notificationcenter_panelopen",
	},
	{
		S3Directory: "notificationcenter_panelclose",
		TableName:   "notificationcenter_panelclose",
	},
	{
		S3Directory: "notificationcenter_fetchnextpage",
		TableName:   "notificationcenter_fetchnextpage",
	},
	{
		S3Directory: "notificationcenter_markallasread",
		TableName:   "notificationcenter_markallasread",
	},
	{
		S3Directory: "notificationcenter_alltabclick",
		TableName:   "notificationcenter_alltabclick",
	},
	{
		S3Directory: "notificationcenter_unreadtabclick",
		TableName:   "notificationcenter_unreadtabclick",
	},
	{
		S3Directory: "notificationcenter_notificationclick",
		TableName:   "notificationcenter_notificationclick",
	},
	{
		S3Directory: "notificationcenter_notificationaction_givefeedback",
		TableName:   "notificationcenter_notificationaction_givefeedback",
	},
	{
		S3Directory: "notificationcenter_notificationaction_managenotification",
		TableName:   "notificationcenter_notificationaction_managenotification",
	},
	{
		S3Directory: "notificationcenter_notificationaction_markasread",
		TableName:   "notificationcenter_notificationaction_markasread",
	},
	{
		S3Directory: "notificationcenter_notificationaction_markasunread",
		TableName:   "notificationcenter_notificationaction_markasunread",
	},
	{
		S3Directory: "notificationcenter_batchaction_markasread",
		TableName:   "notificationcenter_batchaction_markasread",
	},
	{
		S3Directory: "notificationcenter_batchaction_markasunread",
		TableName:   "notificationcenter_batchaction_markasunread",
	},
	{
		S3Directory: "notificationcenter_batchaction_fetchnextpage",
		TableName:   "notificationcenter_batchaction_fetchnextpage",
	},
	{
		S3Directory: "notificationcenter_batchaction_managenotification",
		TableName:   "notificationcenter_batchaction_managenotification",
	},
	{
		S3Directory: "notificationcenter_batchaction_expandbatch",
		TableName:   "notificationcenter_batchaction_expandbatch",
	},
	{
		S3Directory: "notificationcenter_batchaction_collapsebatch",
		TableName:   "notificationcenter_batchaction_collapsebatch",
	},
	{
		S3Directory: "notificationcenter_batchaction_givefeedback",
		TableName:   "notificationcenter_batchaction_givefeedback",
	},
	{
		S3Directory: "notificationcenter_batchaction_viewsourceeventpage",
		TableName:   "notificationcenter_batchaction_viewsourceeventpage",
	},
	{
		S3Directory: "fleet_dispatch_route_job_detail_view_automated_route_stop_call_submitted",
		TableName:   "fleet_dispatch_route_job_detail_view_automated_route_stop_call_submitted",
	},
	{
		S3Directory: "fleet_dispatch_route_job_detail_view_automated_route_stop_call",
		TableName:   "fleet_dispatch_route_job_detail_view_automated_route_stop_call",
	},
	{
		S3Directory: "fleet_reports_safety_coaching_group_coaching_engagement_booster_impression",
		TableName:   "fleet_reports_safety_coaching_group_coaching_engagement_booster_impression",
	},
	{
		S3Directory: "fleet_reports_safety_coaching_group_coaching_engagement_booster_engagement",
		TableName:   "fleet_reports_safety_coaching_group_coaching_engagement_booster_engagement",
	},
	{
		S3Directory: "fleet_reports_safety_coaching_group_coaching_engagement_booster_conversion",
		TableName:   "fleet_reports_safety_coaching_group_coaching_engagement_booster_conversion",
	},
	{
		S3Directory: "fleet_reports_safety_coaching_group_coaching_engagement_booster_drop_off",
		TableName:   "fleet_reports_safety_coaching_group_coaching_engagement_booster_drop_off",
	},
	{
		S3Directory: "troy_driver_global_navigate",
		TableName:   "troy_driver_global_navigate",
	},
	{
		S3Directory: "supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click",
		TableName:   "supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click",
	},
	{
		S3Directory: "supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click",
		TableName:   "supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_coach_assignment_changed",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_coach_assignment_changed",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_dismiss_menu_opened",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_dismiss_menu_opened",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_event_dismissed",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_event_dismissed",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_event_thumbnail_clicked",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_event_thumbnail_clicked",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_modal_opened",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_modal_opened",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_send_to_coaching_submitted",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_send_to_coaching_submitted",
	},
	{
		S3Directory: "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_show_all_events_clicked",
		TableName:   "fleet_reports_safety_supervisor_insights_send_to_coaching_modal_show_all_events_clicked",
	},
	{
		S3Directory: "safety_event_review_portal_review_complete_duration",
		TableName:   "safety_event_review_portal_review_complete_duration",
	},
	{
		S3Directory: "onboarding_assistant_notification_sent",
		TableName:   "onboarding_assistant_notification_sent",
	},
	{
		S3Directory: "onboarding_assistant_auto_opened",
		TableName:   "onboarding_assistant_auto_opened",
	},
	{
		S3Directory: "onboarding_assistant_card_next_card",
		TableName:   "onboarding_assistant_card_next_card",
	},
	{
		S3Directory: "onboarding_assistant_card_selected",
		TableName:   "onboarding_assistant_card_selected",
	},
	{
		S3Directory: "onboarding_assistant_closed",
		TableName:   "onboarding_assistant_closed",
	},
	{
		S3Directory: "onboarding_assistant_expanded",
		TableName:   "onboarding_assistant_expanded",
	},
	{
		S3Directory: "onboarding_assistant_group_selected",
		TableName:   "onboarding_assistant_group_selected",
	},
	{
		S3Directory: "onboarding_assistant_image_close",
		TableName:   "onboarding_assistant_image_close",
	},
	{
		S3Directory: "onboarding_assistant_image_open",
		TableName:   "onboarding_assistant_image_open",
	},
	{
		S3Directory: "onboarding_assistant_minimized",
		TableName:   "onboarding_assistant_minimized",
	},
	{
		S3Directory: "onboarding_assistant_opened",
		TableName:   "onboarding_assistant_opened",
	},
	{
		S3Directory: "onboarding_assistant_task_completed",
		TableName:   "onboarding_assistant_task_completed",
	},
	{
		S3Directory: "onboarding_assistant_task_incompleted",
		TableName:   "onboarding_assistant_task_incompleted",
	},
	{
		S3Directory: "onboarding_assistant_task_link_clicked",
		TableName:   "onboarding_assistant_task_link_clicked",
	},
	{
		S3Directory: "onboarding_assistant_video_ended",
		TableName:   "onboarding_assistant_video_ended",
	},
	{
		S3Directory: "onboarding_assistant_video_fullscreen",
		TableName:   "onboarding_assistant_video_fullscreen",
	},
	{
		S3Directory: "onboarding_assistant_video_pause",
		TableName:   "onboarding_assistant_video_pause",
	},
	{
		S3Directory: "onboarding_assistant_video_play",
		TableName:   "onboarding_assistant_video_play",
	},
	{
		S3Directory: "onboarding_assistant_video_resized",
		TableName:   "onboarding_assistant_video_resized",
	},
	// Billing page metrics
	{
		S3Directory: "org_config_billing_invoices_invoice_click",
		TableName:   "org_config_billing_invoices_invoice_click",
	},
	{
		S3Directory: "org_config_billing_invoices_status_filter",
		TableName:   "org_config_billing_invoices_status_filter",
	},
	{
		S3Directory: "org_config_billing_invoices_search",
		TableName:   "org_config_billing_invoices_search",
	},
	{
		S3Directory: "org_config_billing_invoices_export_csv",
		TableName:   "org_config_billing_invoices_export_csv",
	},
	{
		S3Directory: "org_config_billing_payment_settings_click",
		TableName:   "org_config_billing_payment_settings_click",
	},
	{
		S3Directory: "org_config_billing_invoice_download",
		TableName:   "org_config_billing_invoice_download",
	},
	{
		S3Directory: "org_config_billing_invoice_download_error",
		TableName:   "org_config_billing_invoice_download_error",
	},
}
