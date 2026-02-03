package metadataregistry

// TableBadges contains a mapping of tables and their assigned badges.
// Unsupported badges will be rendered with default badge styling.
// Supported badges:
// - 'datamodel-gold' | 'datamodel-silver' | 'datamodel-bronze'

var TableBadges = map[string][]string{
	"datamodel_core.dim_devices":                    {"datamodel-gold"},
	"datamodel_core.dim_devices_fast":               {"datamodel-gold"},
	"datamodel_core.dim_devices_sensitive":          {"datamodel-gold"},
	"datamodel_core.dim_organizations":              {"datamodel-gold"},
	"datamodel_core.dim_product_variants":           {"datamodel-gold"},
	"datamodel_core.lifetime_device_activity":       {"datamodel-gold"},
	"datamodel_safety.fct_safety_events":            {"datamodel-gold"},
	"datamodel_telematics.dim_eld_relevant_devices": {"datamodel-gold"},
	"datamodel_telematics.fct_hos_logs":             {"datamodel-gold"},
	"datamodel_telematics.fct_trips":                {"datamodel-gold"},
	"datamodel_telematics.fct_trips_daily":          {"datamodel-gold"},
}
