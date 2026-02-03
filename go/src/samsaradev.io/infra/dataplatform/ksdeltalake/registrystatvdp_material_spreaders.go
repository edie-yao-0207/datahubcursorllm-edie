package ksdeltalake

import (
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
)

/*
   Since spreader integrations create many different stats, a separate file is used to group them together.
*/

func (r *RegistryStat) GetOsDSaltSpreaderAutomationStateGranular() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDSaltSpreaderAutomationStateGranular,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.MaterialSpreaderTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Current automation state of the spreader specific to granular material",
			Frequency:           FrequencyOnChange("spreader automation state"),
			IntValueDescription: metadatahelpers.EnumDescription("Material spreader automation state", hubproto.SaltSpreaderAutomationState_name),
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderAutomationStatePrewet() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDSaltSpreaderAutomationStatePrewet,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.MaterialSpreaderTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Current automation state of the spreader specific to prewet material",
			Frequency:           FrequencyOnChange("spreader automation state"),
			IntValueDescription: metadatahelpers.EnumDescription("Material spreader automation state", hubproto.SaltSpreaderAutomationState_name),
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderAutomationStateWet() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDSaltSpreaderAutomationStateWet,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.MaterialSpreaderTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Current automation state of the spreader specific to wet material",
			Frequency:           FrequencyOnChange("spreader automation state"),
			IntValueDescription: metadatahelpers.EnumDescription("Material spreader automation state", hubproto.SaltSpreaderAutomationState_name),
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderFirmwareVersion() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderFirmwareVersion,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Firmware version of the salt spreader",
			Frequency:   FrequencyOnChange("firmware version"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The firmware version of the salt spreader",
			},
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderModel() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderModel,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Model of the salt spreader",
			Frequency:   FrequencyOnChange("spreader model"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The model of the salt spreader",
			},
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderCalibrationCompletedAt() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderCalibrationCompletedAt,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Date and time of the salt spreader calibration",
			Frequency:   FrequencyOnChange("calibration of all material types completed at"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The date and time of the salt spreader calibration",
			},
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderGranularCalibrationCompletedAt() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderGranularCalibrationCompletedAt,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Date and time of the salt spreader granular calibration",
			Frequency:   FrequencyOnChange("granular calibration completed at"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The date and time of the salt spreader granular calibration",
			},
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderPrewetCalibrationCompletedAt() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderPrewetCalibrationCompletedAt,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Date and time of the salt spreader pre-wet calibration",
			Frequency:   FrequencyOnChange("pre-wet calibration completed at"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The date and time of the salt spreader pre-wet calibration",
			},
		},
	}
}

func (r *RegistryStat) GetOsDSaltSpreaderWetCalibrationCompletedAt() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDSaltSpreaderWetCalibrationCompletedAt,
		BinaryMessageField: "DiagnosticValue",
		Kind:               StatKindObjectStat,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description: "Date and time of the salt spreader wet calibration",
			Frequency:   FrequencyOnChange("wet calibration completed at"),
			ColumnDescriptions: map[string]string{
				"value.proto_value.diagnostic_value.string_value": "The date and time of the salt spreader wet calibration",
			},
		},
	}
}
