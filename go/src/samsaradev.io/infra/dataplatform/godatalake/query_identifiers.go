package godatalake

type QueryIdentifier string

// Add your QueryIdentifiers here
const (
	QueryIdentifierDriverRiskInsightsFeatures               QueryIdentifier = "driver_risk_insights_features"
	QueryIdentifierTripLevelRiskInsightsFeatures            QueryIdentifier = "trip_level_risk_insights_features"
	QueryIdentifierTripLevelRiskInsightsFeaturesRecordCount QueryIdentifier = "trip_level_risk_insights_features_record_count"
	QueryIdentifierACRTrips                                 QueryIdentifier = "acr_trips"
	QueryIdentifierACRSpeedingIntervals                     QueryIdentifier = "acr_speeding_intervals"
	QueryIdentifierProductRecommendationsInferences         QueryIdentifier = "product_recommendations"
	QueryIdentifierKnownLocationClusters                    QueryIdentifier = "known_location_clusters"
	QueryIdentifierKnownLocationClustersRecordCount         QueryIdentifier = "known_location_clusters_record_count"
	QueryIdentifierAssetTagOverviewBenchmarks               QueryIdentifier = "asset_tag_overview_benchmarks"
)
