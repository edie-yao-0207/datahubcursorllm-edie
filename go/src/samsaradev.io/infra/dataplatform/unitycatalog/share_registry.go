package unitycatalog

import (
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	teamComponents "samsaradev.io/team/components"
)

const (
	ObjectTypeTable  = "TABLE"
	ObjectTypeView   = "VIEW" // We do not support views currently
	ObjectTypeSchema = "SCHEMA"
	ObjectTypeVolume = "VOLUME"
	ObjectTypeModel  = "MODEL"
	usProviderName   = "aws:us-west-2:a4ada860-2122-418f-ae38-e374e756bc04"
	euProviderName   = "aws:eu-west-1:fecf0cfb-bf1e-43a3-8803-b536ba03a2ca"
	caProviderName   = "aws:ca-central-1:175be941-32cd-46ef-9229-1f986aefe4b1"
)

// CommonDataToolsShareObjects contains ShareObjects that are shared between
// data_tools_delta_share_eu and data_tools_delta_share_ca.
// if the object you're adding is not common, just add it to the share in question like before
var CommonDataToolsShareObjects = map[string]ShareObject{
	"default.product_analytics.dim_devices_safety_settings": {
		Name:           "default.product_analytics.dim_devices_safety_settings",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics.dim_organizations_safety_settings": {
		Name:           "default.product_analytics.dim_organizations_safety_settings",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.stg_daily_fuel_type": {
		Name:           "default.product_analytics_staging.stg_daily_fuel_type",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.stg_organization_config_buckets": {
		Name:           "default.product_analytics_staging.stg_organization_config_buckets",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.datamodel_platform.dim_users_organizations": {
		Name:           "default.datamodel_platform.dim_users_organizations",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.datamodel_platform.fct_alert_incidents": {
		Name:           "default.datamodel_platform.fct_alert_incidents",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.datamodel_platform.dim_alert_configs": {
		Name:           "default.datamodel_platform.dim_alert_configs",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.vehicle_mpg_lookback": {
		Name:           "default.dataengineering.vehicle_mpg_lookback",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.dim_organizations_classification": {
		Name:           "default.product_analytics_staging.dim_organizations_classification",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.safety_event_review_results_details_region": {
		Name:           "default.dataengineering.safety_event_review_results_details_region",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.safety_event_reviews_details_region": {
		Name:           "default.dataengineering.safety_event_reviews_details_region",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.safety_inbox_events_status": {
		Name:           "default.dataengineering.safety_inbox_events_status",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.safety_event_surveys": {
		Name:           "default.dataengineering.safety_event_surveys",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.dataengineering.video_requests": {
		Name:           "default.dataengineering.video_requests",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.stg_organization_benchmark_metrics": {
		Name:           "default.product_analytics_staging.stg_organization_benchmark_metrics",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.dim_device_vehicle_properties": {
		Name:           "default.product_analytics_staging.dim_device_vehicle_properties",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics.dim_app_installs": {
		Name:           "default.product_analytics.dim_app_installs",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics.fct_api_usage": {
		Name:           "default.product_analytics.fct_api_usage",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.product_analytics_staging.fct_device_utilization": {
		Name:           "default.product_analytics_staging.fct_device_utilization",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
	"default.datamodel_platform.dim_users_organizations_permissions": {
		Name:           "default.datamodel_platform.dim_users_organizations_permissions",
		Owner:          team.DataPlatform,
		DataObjectType: ObjectTypeTable,
	},
}

type Share struct {
	Name   string
	Region string

	Owner teamComponents.TeamInfo

	RecipientName string

	Catalog ShareCatalog

	ShareObjects map[string]ShareObject
}
type ShareCatalog struct {
	Name   string
	Region string

	Owner teamComponents.TeamInfo

	ProviderName  string
	CanReadGroups map[string][]teamComponents.TeamInfo
}

type ShareObject struct {
	Name string

	Owner                    teamComponents.TeamInfo
	DataObjectType           string
	CdfEnabled               bool
	HistoryDataSharingStatus string
	Partitions               []ShareObjectPartition
}

type ShareObjectPartition struct {
	Values []ShareObjectPartitionValue
}

type ShareObjectPartitionValue struct {
	Name                 string
	Op                   string
	RecipientPropertyKey string
	Value                string
}

func (s Share) InSourceRegion(region string) bool {
	return s.Region == region
}

func (s Share) InCatalogRegion(region string) bool {
	return s.Catalog.Region == region
}

// mergeShareObjects merges multiple ShareObject maps into a single map.
// Later maps override earlier maps if there are duplicate keys.
func mergeShareObjects(maps ...map[string]ShareObject) map[string]ShareObject {
	result := make(map[string]ShareObject)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// addHistoryDataSharingStatus adds HistoryDataSharingStatus to all ShareObjects in the map.
func addHistoryDataSharingStatus(objects map[string]ShareObject, status string) map[string]ShareObject {
	result := make(map[string]ShareObject)
	for k, v := range objects {
		v.HistoryDataSharingStatus = status
		result[k] = v
	}
	return result
}

var ShareRegistry = []Share{
	{
		Name:          "edw_delta_share",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_eu",
		Catalog: ShareCatalog{
			Name:         "edw_delta_share",
			Region:       infraconsts.SamsaraAWSEURegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"netsuite_sterling": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"salesforce_sterling": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"workday": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"silver": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.CustomerSuccessOperations,
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
					team.Hardware,
					team.ProductManagement,
					team.Security,
					team.TPM,
					team.SupportL2,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"edw.netsuite_sterling.currency": {
				Name:                     "edw.netsuite_sterling.currency",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.item_product_group": {
				Name:                     "edw.netsuite_sterling.item_product_group",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.netsuite_customer": {
				Name:                     "edw.netsuite_sterling.netsuite_customer",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.netsuite_transaction_lines": {
				Name:                     "edw.netsuite_sterling.netsuite_transaction_lines",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.netsuite_transactions": {
				Name:                     "edw.netsuite_sterling.netsuite_transactions",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.transaction_inventory_numbers": {
				Name:                     "edw.netsuite_sterling.transaction_inventory_numbers",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.transactions": {
				Name:                     "edw.netsuite_sterling.transactions",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.netsuite_sterling.transactions_extended": {
				Name:                     "edw.netsuite_sterling.transactions_extended",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.account": {
				Name:                     "edw.salesforce_sterling.account",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.plm_product_hierarchy": {
				Name:                     "edw.salesforce_sterling.plm_product_hierarchy",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.sfdc_contract": {
				Name:                     "edw.salesforce_sterling.sfdc_contract",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.sfdc_plm_product_hierarchy": {
				Name:                     "edw.salesforce_sterling.sfdc_plm_product_hierarchy",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.user": {
				Name:                     "edw.salesforce_sterling.user",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.opportunity": {
				Name:                     "edw.salesforce_sterling.opportunity",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.opportunity_feature_assignment": {
				Name:                     "edw.salesforce_sterling.opportunity_feature_assignment",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.case": {
				Name:                     "edw.salesforce_sterling.case",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.campaign_member": {
				Name:                     "edw.salesforce_sterling.campaign_member",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.campaign": {
				Name:                     "edw.salesforce_sterling.campaign",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw_bronze.workday.company_global_holiday": {
				Name:                     "edw_bronze.workday.company_global_holiday",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.contract_totals": {
				Name:                     "edw.silver.contract_totals",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_account_enriched": {
				Name:                     "edw.silver.dim_account_enriched",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_device_transaction": {
				Name:                     "edw.silver.dim_device_transaction",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_customer": {
				Name:                     "edw.silver.dim_customer",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_customer_issues": {
				Name:                     "edw.silver.dim_customer_issues",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_products": {
				Name:                     "edw.silver.dim_products",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_orders": {
				Name:                     "edw.silver.fct_orders",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_currency": {
				Name:                     "edw.silver.dim_currency",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_license_orders": {
				Name:                     "edw.silver.fct_license_orders",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.employee_hierarchy_hst": {
				Name:                     "edw.silver.employee_hierarchy_hst",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_org_sam_sfdc_account_mapping": {
				Name:                     "edw.silver.dim_org_sam_sfdc_account_mapping",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.license_hw_sku_xref": {
				Name:                     "edw.silver.license_hw_sku_xref",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.sam_account_org_xref": {
				Name:                     "edw.silver.sam_account_org_xref",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_license_orders_daily_snapshot": {
				Name:                     "edw.silver.fct_license_orders_daily_snapshot",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
		},
	},
	{
		Name:          "mixpanel_delta_share",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_eu",
		Catalog: ShareCatalog{
			Name:         "mixpanel_delta_share",
			Region:       infraconsts.SamsaraAWSEURegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"datamodel_platform_silver": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.CustomerSuccessOperations,
					team.SalesEngineering,
					team.SalesDataAnalytics,
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
					team.DataAnalytics,
					team.DataScience,
					team.PlatformReports,
				},
				"mixpanel_samsara": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.datamodel_platform_silver.stg_cloud_routes": {
				Name:           "default.datamodel_platform_silver.stg_cloud_routes",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.mixpanel_samsara.ai_chat_send": {
				Name:           "default.mixpanel_samsara.ai_chat_send",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "data_tools_delta_share_us",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_eu",
		Catalog: ShareCatalog{
			Name:         "data_tools_delta_share",
			Region:       infraconsts.SamsaraAWSEURegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"datamodel_launchdarkly_bronze": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"dojo": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"product_analytics": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.dojo.osm_way_nodes": {
				Name:           "default.dojo.osm_way_nodes",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.product_analytics.agg_product_usage_global": {
				Name:           "default.product_analytics.agg_product_usage_global",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause": {
				Name:           "default.datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "data_tools_delta_share_ca",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_ca",
		Catalog: ShareCatalog{
			Name:         "data_tools_delta_share",
			Region:       infraconsts.SamsaraAWSCARegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"dojo": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"product_analytics": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"devecosystem_dev": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.dojo.osm_way_nodes": {
				Name:           "default.dojo.osm_way_nodes",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.product_analytics.agg_product_usage_global": {
				Name:           "default.product_analytics.agg_product_usage_global",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.devecosystem_dev.token_name_app_matches": {
				Name:           "default.devecosystem_dev.token_name_app_matches",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "data_tools_delta_share_eu",
		Region:        infraconsts.SamsaraAWSEURegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_us",
		Catalog: ShareCatalog{
			Name:         "data_tools_delta_share",
			Region:       infraconsts.SamsaraAWSDefaultRegion,
			Owner:        team.DataPlatform,
			ProviderName: euProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"product_analytics": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
					team.DataAnalytics,
				},
				"product_analytics_staging": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
					team.DataAnalytics,
				},
				"dataengineering": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"metrics_repo": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_platform": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"dojo": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"feature_store": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
					team.DataAnalytics,
				},
			},
		},
		// both shared and objects specific to this share
		ShareObjects: mergeShareObjects(
			CommonDataToolsShareObjects,
			map[string]ShareObject{
				"product_analytics.dim_device_dimensions": {
					Name:           "default.product_analytics.dim_device_dimensions",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.product_analytics.agg_device_stats_secondary_coverage": {
					Name:           "default.product_analytics.agg_device_stats_secondary_coverage",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.product_analytics.fct_safety_triage_events": {
					Name:           "default.product_analytics.fct_safety_triage_events",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.product_analytics_staging.stg_organization_categories_eu": {
					Name:           "default.product_analytics_staging.stg_organization_categories_eu",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.dataengineering.vehicle_mpg_lookback_by_org": {
					Name:           "default.dataengineering.vehicle_mpg_lookback_by_org",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.dojo.device_ai_features_daily_snapshot": {
					Name:           "default.dojo.device_ai_features_daily_snapshot",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.product_analytics.fct_eco_driving_augmented_reports": {
					Name:           "default.product_analytics.fct_eco_driving_augmented_reports",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
				"default.feature_store.fct_eco_driving_key_cycle_features": {
					Name:           "default.feature_store.fct_eco_driving_key_cycle_features",
					Owner:          team.DataPlatform,
					DataObjectType: ObjectTypeTable,
				},
			},
		),
	},
	{
		Name:          "biztech_delta_share_eu",
		Region:        infraconsts.SamsaraAWSEURegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_us",
		Catalog: ShareCatalog{
			Name:         "biztech_delta_share",
			Region:       infraconsts.SamsaraAWSDefaultRegion,
			Owner:        team.DataPlatform,
			ProviderName: euProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"biztech_edw_customer_success_gold": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
				},
				"datamodel_platform": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
					team.MarketingDataAnalytics,
				},
				"datamodel_platform_bronze": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
					team.MarketingDataAnalytics,
				},
				"datamodel_platform_silver": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
					team.MarketingDataAnalytics,
				},
				"datamodel_core_bronze": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
					team.MarketingDataAnalytics,
				},
				"datamodel_core": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
					team.MarketingDataAnalytics,
				},
				"clouddb": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
				},
				"hardware_analytics": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
				},
				"hardware": {
					team.BizTechEnterpriseDataAdmin,
					team.BizTechEnterpriseData,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.biztech_edw_customer_success_gold.agg_license_utilization_analysis": {
				Name:           "default.biztech_edw_customer_success_gold.agg_license_utilization_analysis",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.biztech_edw_customer_success_gold.agg_device_type_core_utilization_kpis": {
				Name:           "default.biztech_edw_customer_success_gold.agg_device_type_core_utilization_kpis",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform.dim_users": {
				Name:           "default.datamodel_platform.dim_users",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform_bronze.raw_clouddb_custom_roles": {
				Name:           "default.datamodel_platform_bronze.raw_clouddb_custom_roles",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform_silver.stg_user_login_events": {
				Name:           "default.datamodel_platform_silver.stg_user_login_events",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_core_bronze.raw_clouddb_organizations": {
				Name:           "default.datamodel_core_bronze.raw_clouddb_organizations",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_core.dim_organizations": {
				Name:           "default.datamodel_core.dim_organizations",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform.dim_users_organizations": {
				Name:           "default.datamodel_platform.dim_users_organizations",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.clouddb.org_sfdc_accounts": {
				Name:           "default.clouddb.org_sfdc_accounts",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.clouddb.sfdc_accounts": {
				Name:           "default.clouddb.sfdc_accounts",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.hardware_analytics.field_summary_table": {
				Name:           "default.hardware_analytics.field_summary_table",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.hardware.life_summary": {
				Name:           "default.hardware.life_summary",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "dynamodb_global_tables_share",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_eu",
		Catalog: ShareCatalog{
			Name:         "dynamodb_global_tables_share",
			Region:       infraconsts.SamsaraAWSEURegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"dynamodb": {
					team.DataPlatform,
					team.DiagnosticsTools,
					team.FirmwareAutomotiveEngineering,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.dynamodb.vin_decoding_rules": {
				Name:           "default.dynamodb.vin_decoding_rules",
				Owner:          team.DiagnosticsTools,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "edw_delta_share_ca",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_ca",
		Catalog: ShareCatalog{
			Name:         "edw_delta_share",
			Region:       infraconsts.SamsaraAWSCARegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"salesforce_sterling": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"edw.salesforce_sterling.sfdc_contract": {
				Name:                     "edw.salesforce_sterling.sfdc_contract",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.plm_product_hierarchy": {
				Name:                     "edw.salesforce_sterling.plm_product_hierarchy",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.account": {
				Name:                     "edw.salesforce_sterling.account",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.opportunity": {
				Name:                     "edw.salesforce_sterling.opportunity",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.opportunity_feature_assignment": {
				Name:                     "edw.salesforce_sterling.opportunity_feature_assignment",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.case": {
				Name:                     "edw.salesforce_sterling.case",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.user": {
				Name:                     "edw.salesforce_sterling.user",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.campaign_member": {
				Name:                     "edw.salesforce_sterling.campaign_member",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.salesforce_sterling.campaign": {
				Name:                     "edw.salesforce_sterling.campaign",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_account_enriched": {
				Name:                     "edw.silver.dim_account_enriched",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_device_transaction": {
				Name:                     "edw.silver.dim_device_transaction",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_customer": {
				Name:                     "edw.silver.dim_customer",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_currency": {
				Name:                     "edw.silver.dim_currency",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_license_orders": {
				Name:                     "edw.silver.fct_license_orders",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_orders": {
				Name:                     "edw.silver.fct_orders",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.employee_hierarchy_hst": {
				Name:                     "edw.silver.employee_hierarchy_hst",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.dim_org_sam_sfdc_account_mapping": {
				Name:                     "edw.silver.dim_org_sam_sfdc_account_mapping",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.license_hw_sku_xref": {
				Name:                     "edw.silver.license_hw_sku_xref",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.sam_account_org_xref": {
				Name:                     "edw.silver.sam_account_org_xref",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
			"edw.silver.fct_license_orders_daily_snapshot": {
				Name:                     "edw.silver.fct_license_orders_daily_snapshot",
				Owner:                    team.DataPlatform,
				DataObjectType:           ObjectTypeTable,
				HistoryDataSharingStatus: "ENABLED",
			},
		},
	},
	{
		Name:          "sustainability_delta_share_eu",
		Region:        infraconsts.SamsaraAWSEURegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_us",
		Catalog: ShareCatalog{
			Name:         "sustainability_delta_share",
			Region:       infraconsts.SamsaraAWSDefaultRegion,
			Owner:        team.DataPlatform,
			ProviderName: euProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"fuelcards_shard_1db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"fuelcards_shard_0db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"datamodel_core": {
					team.DataPlatform,
					team.Sustainability,
				},
				"fuel_shard_0db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"fuel_shard_1db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"product_analytics": {
					team.DataPlatform,
					team.Sustainability,
				},
				"engineactivity_shard_1db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"engineactivity_shard_0db": {
					team.DataPlatform,
					team.Sustainability,
				},
				"kinesisstats_history": {
					team.DataPlatform,
					team.Sustainability,
				},
				"datamodel_platform": {
					team.DataPlatform,
					team.Sustainability,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.fuelcards_shard_1db.fuel_logs": {
				Name:           "default.fuelcards_shard_1db.fuel_logs",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_1db.fuel_log_unverified_reasons": {
				Name:           "default.fuelcards_shard_1db.fuel_log_unverified_reasons",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_1db.transaction_verification_rules": {
				Name:           "default.fuelcards_shard_1db.transaction_verification_rules",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_1db.merchant_settings": {
				Name:           "default.fuelcards_shard_1db.merchant_settings",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuel_shard_1db.fuel_types": {
				Name:           "default.fuel_shard_1db.fuel_types",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_0db.fuel_logs": {
				Name:           "default.fuelcards_shard_0db.fuel_logs",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_0db.fuel_log_unverified_reasons": {
				Name:           "default.fuelcards_shard_0db.fuel_log_unverified_reasons",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_0db.transaction_verification_rules": {
				Name:           "default.fuelcards_shard_0db.transaction_verification_rules",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuelcards_shard_0db.merchant_settings": {
				Name:           "default.fuelcards_shard_0db.merchant_settings",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.fuel_shard_0db.fuel_types": {
				Name:           "default.fuel_shard_0db.fuel_types",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_core.dim_organizations": {
				Name:           "default.datamodel_core.dim_organizations",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_core.dim_devices": {
				Name:           "default.datamodel_core.dim_devices",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.product_analytics.fct_api_usage": {
				Name:           "default.product_analytics.fct_api_usage",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_1db.engine_idle_events": {
				Name:           "default.engineactivity_shard_1db.engine_idle_events",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_1db.unproductive_idling_config": {
				Name:           "default.engineactivity_shard_1db.unproductive_idling_config",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_1db.engine_engage_events": {
				Name:           "default.engineactivity_shard_1db.engine_engage_events",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_0db.engine_idle_events": {
				Name:           "default.engineactivity_shard_0db.engine_idle_events",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_0db.unproductive_idling_config": {
				Name:           "default.engineactivity_shard_0db.unproductive_idling_config",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.engineactivity_shard_0db.engine_engage_events": {
				Name:           "default.engineactivity_shard_0db.engine_engage_events",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform.dim_alert_configs": {
				Name:           "default.datamodel_platform.dim_alert_configs",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.kinesisstats_history.osdcm3xaudioalertinfo": {
				Name:           "default.kinesisstats_history.osdcm3xaudioalertinfo",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.datamodel_platform.fct_alert_incidents": {
				Name:           "default.datamodel_platform.fct_alert_incidents",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "mixpanel_delta_share_ca",
		Region:        infraconsts.SamsaraAWSDefaultRegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_ca",
		Catalog: ShareCatalog{
			Name:         "mixpanel_delta_share",
			Region:       infraconsts.SamsaraAWSCARegion,
			Owner:        team.DataPlatform,
			ProviderName: usProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"datamodel_platform_silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"mixpanel_samsara": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		ShareObjects: map[string]ShareObject{
			"default.datamodel_platform_silver.stg_cloud_routes": {
				Name:           "default.datamodel_platform_silver.stg_cloud_routes",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
			"default.mixpanel_samsara.ai_chat_send": {
				Name:           "default.mixpanel_samsara.ai_chat_send",
				Owner:          team.DataPlatform,
				DataObjectType: ObjectTypeTable,
			},
		},
	},
	{
		Name:          "data_tools_delta_share_ca_us",
		Region:        infraconsts.SamsaraAWSCARegion,
		Owner:         team.DataPlatform,
		RecipientName: "databricks_ca_us",
		Catalog: ShareCatalog{
			Name:         "data_tools_delta_share_ca",
			Region:       infraconsts.SamsaraAWSDefaultRegion,
			Owner:        team.DataPlatform,
			ProviderName: caProviderName,
			CanReadGroups: map[string][]teamComponents.TeamInfo{
				"product_analytics": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"product_analytics_staging": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_platform": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_platform_bronze": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_platform_silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_core": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_core_bronze": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_core_silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_telematics": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_telematics_silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_safety": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"datamodel_safety_silver": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
				"dataengineering": {
					team.DataEngineering,
					team.DataTools,
					team.DataPlatform,
				},
			},
		},
		// both shared and objects specific to this share
		// Note: CommonDataToolsShareObjects get HistoryDataSharingStatus for CA share only
		ShareObjects: mergeShareObjects(
			addHistoryDataSharingStatus(CommonDataToolsShareObjects, "ENABLED"),
			map[string]ShareObject{
				"default.product_analytics_staging.stg_organization_categories_ca": {
					Name:                     "default.product_analytics_staging.stg_organization_categories_ca",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core.dim_devices": {
					Name:                     "default.datamodel_core.dim_devices",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core_bronze.raw_vindb_shards_device_vin_metadata": {
					Name:                     "default.datamodel_core_bronze.raw_vindb_shards_device_vin_metadata",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core_bronze.raw_productdb_devices": {
					Name:                     "default.datamodel_core_bronze.raw_productdb_devices",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core_bronze.raw_clouddb_organizations": {
					Name:                     "default.datamodel_core_bronze.raw_clouddb_organizations",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core_silver.stg_device_activity_daily": {
					Name:                     "default.datamodel_core_silver.stg_device_activity_daily",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core.dim_organizations": {
					Name:                     "default.datamodel_core.dim_organizations",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_telematics.fct_trips": {
					Name:                     "default.datamodel_telematics.fct_trips",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_safety.fct_safety_events": {
					Name:                     "default.datamodel_safety.fct_safety_events",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform_bronze.raw_clouddb_users": {
					Name:                     "default.datamodel_platform_bronze.raw_clouddb_users",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform_bronze.raw_clouddb_users_organizations": {
					Name:                     "default.datamodel_platform_bronze.raw_clouddb_users_organizations",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform_bronze.raw_clouddb_custom_roles": {
					Name:                     "default.datamodel_platform_bronze.raw_clouddb_custom_roles",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_safety_silver.stg_activity_events": {
					Name:                     "default.datamodel_safety_silver.stg_activity_events",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core.lifetime_device_activity": {
					Name:                     "default.datamodel_core.lifetime_device_activity",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_core.lifetime_device_online": {
					Name:                     "default.datamodel_core.lifetime_device_online",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform.dim_license_assignment": {
					Name:                     "default.datamodel_platform.dim_license_assignment",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform.dim_drivers": {
					Name:                     "default.datamodel_platform.dim_drivers",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform.dim_licenses": {
					Name:                     "default.datamodel_platform.dim_licenses",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_telematics_silver.stg_hos_logs": {
					Name:                     "default.datamodel_telematics_silver.stg_hos_logs",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_telematics.fct_dispatch_routes": {
					Name:                     "default.datamodel_telematics.fct_dispatch_routes",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_telematics.fct_dvirs": {
					Name:                     "default.datamodel_telematics.fct_dvirs",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
				"default.datamodel_platform_silver.stg_driver_app_events": {
					Name:                     "default.datamodel_platform_silver.stg_driver_app_events",
					Owner:                    team.DataPlatform,
					DataObjectType:           ObjectTypeTable,
					HistoryDataSharingStatus: "ENABLED",
				},
			},
		),
	},
}
