package metadatahelpers

const (
	OrgIdDefaultDescription                = "The Samsara cloud dashboard ID that the data belongs to"
	GroupIdDefaultDescription              = "The ID of the group that the data belongs to"
	SamNumberDefaultDescription            = "Samnumber is a unique id that ties customer accounts together in Netsuite, Salesforce, and Samsara cloud dashboard."
	DeviceIdDefaultDescription             = "The ID of the customer device that the data belongs to"
	GatewayIdDefaultDescription            = "The ID of the Samsara gateway that the data belongs to"
	DriverIdDefaultDescription             = "The ID of the driver that the data belongs to"
	SchemaVersionDefaultDescription        = "The latest version of the database schema applied."
	SchemaVersionVersionDefaultDescription = "The latest version number applied."
	DeprecatedDefaultDescription           = "[DEPRECATED] DO NOT USE"
	DateDefaultDescription                 = "The date this row/event occurred on"
)

var (
	SchemaVersionCompletedDefaultDescription = EnumDescription("Whether the latest version was applied successfully.", map[int32]string{
		int32(0): "Unsuccessful",
		int32(1): "Successful",
	})
)

// DefaultDescriptionIdentifiers is a map of identifier variables to their default descriptions
// These are utilized in JSON files where we don't have proper typing.
// Developers can use the identifier variables in their JSON and the metadata engine
// will catch those identifiers and swap in the default description.
var DefaultDescriptionIdentifiers = map[string]string{
	"$org_id_default_description":     OrgIdDefaultDescription,
	"$group_id_default_description":   GroupIdDefaultDescription,
	"$device_id_default_description":  DeviceIdDefaultDescription,
	"$gateway_id_default_description": GatewayIdDefaultDescription,
	"$driver_id_default_description":  DriverIdDefaultDescription,
	"$deprecated_default_description": DeprecatedDefaultDescription,
	"$date_default_description":       DateDefaultDescription,
}
