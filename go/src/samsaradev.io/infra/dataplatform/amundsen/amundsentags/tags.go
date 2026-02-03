package amundsentags

import "github.com/samsarahq/go/oops"

// Tag represents a tag in Amundsen.
// Please add a const for your tag (and add to 'allTags' slice below) so others can utilize it!
type Tag string

const (
	TestTag Tag = "test-tag"

	CoreFirmwareTag     Tag = "core_firmware"
	FirmwareTag         Tag = "firmware"
	TelematicsTag       Tag = "telematics"
	EcoDrivingTag       Tag = "ecodriving"
	FuelTag             Tag = "fuel"
	FuelPurchasesTag    Tag = "fuel_purchases"
	MaintenanceTag      Tag = "maintenance"
	MaterialSpreaderTag Tag = "material_spreader"
	MulticamTag         Tag = "multicam"
	PerformanceTag      Tag = "performance"
	ReeferTag           Tag = "reefer"
	TachographTag       Tag = "tachograph"
	IndustrialTag       Tag = "industrial"
	AssetsTag           Tag = "assets"
	ElectricVehicleTag  Tag = "ev"
	FleetTag            Tag = "fleet"
	DebugTag            Tag = "debug"
	SafetyTag           Tag = "safety"
	MarathonTag         Tag = "marathon"
	AG46Tag             Tag = "AG46"
	AG51Tag             Tag = "AG51"
	AG52Tag             Tag = "AG52"
	AG53Tag             Tag = "AG53"
	MotionDTag          Tag = "motiond"
	ConnectedAppsTag    Tag = "connected_apps"
	MemTag              Tag = "mem"
	OemTag              Tag = "oem"
	BifrostTag          Tag = "bifrost"
)

var allTags = map[Tag]struct{}{
	TestTag:             {},
	CoreFirmwareTag:     {},
	FirmwareTag:         {},
	TelematicsTag:       {},
	EcoDrivingTag:       {},
	FuelTag:             {},
	FuelPurchasesTag:    {},
	MaterialSpreaderTag: {},
	MulticamTag:         {},
	PerformanceTag:      {},
	ReeferTag:           {},
	TachographTag:       {},
	IndustrialTag:       {},
	AssetsTag:           {},
	SafetyTag:           {},
	ElectricVehicleTag:  {},
	MarathonTag:         {},
	AG51Tag:             {},
	AG52Tag:             {},
	AG53Tag:             {},
	MotionDTag:          {},
	ConnectedAppsTag:    {},
	MemTag:              {},
	OemTag:              {},
	BifrostTag:          {},
}

func GetTag(tag string) (Tag, error) {
	if _, ok := allTags[Tag(tag)]; !ok {
		return "", oops.Errorf("Tag does not exist. Please add it to the list of tags at infra/dataplatform/amundsen/amundsentags/tags.go")
	}

	return Tag(tag), nil
}
