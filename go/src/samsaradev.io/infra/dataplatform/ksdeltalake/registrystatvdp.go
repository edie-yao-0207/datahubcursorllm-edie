package ksdeltalake

import (
	"samsaradev.io/hubproto"
	"samsaradev.io/hubproto/objectstatproto"
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
)

func (r *RegistryStat) GetOsDTellTaleStatus() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDTellTaleStatus,
		Kind:               StatKindObjectStat,
		Production:         false,
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		BinaryMessageField: "TellTaleStatusReport",
		MetadataInfo: &MetadataInfo{
			Description:         "Tell Tale Status Report",
			Frequency:           FrequencyOnChange("Reported on change every 2 minutes or per configuration"),
			IntValueDescription: "Number of TTS Indicators being reported",
			ColumnDescriptions: map[string]string{
				"value.proto_value.tell_tale_status_report.values.indicator": "The OBD value of the TTS indicator OBD_VALUE_TTS_INDICATOR_",
				"value.proto_value.tell_tale_status_report.values.condition": metadatahelpers.EnumDescription("Tell Tale Status Condition", hubproto.TellTaleStatusCondition_name),
				"value.proto_value.tell_tale_status_report.values.level":     metadatahelpers.EnumDescription("Tell Tale Status Level", hubproto.TellTaleStatusLevel_name),
			},
		},
	}
}

func (r *RegistryStat) GetOsDShortTermFuelTrimLevelMilliB1() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDShortTermFuelTrimLevelB1MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Short Term Fuel Trim Level Upstream Bank 1 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Short term fuel trim level for upstream oxygen sensor bank 1 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDShortTermFuelTrimLevelMilliB2() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDShortTermFuelTrimLevelB2MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Short Term Fuel Trim Level Upstream Bank 2 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Short term fuel trim level for upstream oxygen sensor bank 2 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDLongTermFuelTrimLevelMilliB1() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDLongTermFuelTrimLevelB1MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Long Term Fuel Trim Level Upstream Bank 1 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Long term fuel trim level for upstream oxygen sensor bank 1 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDLongTermFuelTrimLevelMilliB2() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDLongTermFuelTrimLevelB2MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Long Term Fuel Trim Level Upstream Bank 2 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Long term fuel trim level for upstream oxygen sensor bank 2 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDRearO2FuelTrimLevelDownstreamMilliB1() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDRearO2FuelTrimLevelDownstreamB1MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Rear O2 Fuel Trim Level Downstream Bank 1 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Rear oxygen sensor fuel trim level for downstream bank 1 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDRearO2FuelTrimLevelDownstreamMilliB2() *Stat {
	return &Stat{
		StatType:      objectstatproto.ObjectStatEnum_osDRearO2FuelTrimLevelDownstreamB2MilliPercent,
		Kind:          StatKindObjectStat,
		DataModelStat: false,
		Production:    false,
		Tags:          []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag, amundsentags.FuelTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Rear O2 Fuel Trim Level Downstream Bank 2 (milli-percent)",
			Frequency:           FrequencyEveryXSeconds(5 * 60),
			IntValueDescription: "Rear oxygen sensor fuel trim level for downstream bank 2 in milli-percent",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939VDC2YawRateFemtoRadPerSeconds() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939VDC2YawRateFemtoRadPerSeconds,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the vehicle yaw rate from the Vehicle Dynamics Controller",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "Yaw rate in femto-radians per second",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939ETC5TransmissionReverseDirectionSwitchState() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939ETC5TransmissionReverseDirectionSwitchState,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the transmission reverse direction switch state",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "0 Reverse gear not engaged, 1 Reverse gear engaged, 2 Error, 3 Not available",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939TCO1DirectionIndicatorState() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939TCO1DirectionIndicatorState,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the direction indicator state (turn signals)",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "0 Not active, 1 Right turn active, 2 Left turn active, 3 Don't care/take no action",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939VDC2SteeringWheelAnglePicoRad() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939VDC2SteeringWheelAnglePicoRad,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the steering wheel angle from the Vehicle Dynamics Controller",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "Steering wheel angle in pico-radians",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939LCMDLeftTurnSignalLightsCommandState() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939LCMDLeftTurnSignalLightsCommandState,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the left turn signal lights command state",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "0 Off, 1 On, 2 Error, 3 Not available",
		},
	}
}

func (r *RegistryStat) GetOsDJ1939LCMDRightTurnSignalLightsCommandState() *Stat {
	return &Stat{
		StatType: objectstatproto.ObjectStatEnum_osDJ1939LCMDRightTurnSignalLightsCommandState,
		Kind:     StatKindObjectStat,
		Tags:     []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports the right turn signal lights command state",
			Frequency:           FrequencyOnChange("No more than every Second, once every hour otherwise"),
			IntValueDescription: "0 Off, 1 On, 2 Error, 3 Not available",
		},
	}
}

func (r *RegistryStat) GetOsDFreezeFrameReport() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDFreezeFrameReport,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "FreezeFrameReport",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports freeze frame data containing diagnostic information captured at the time of fault occurrence",
			Frequency:           FrequencyOnChange("Reported when new freeze frame data is available from the vehicle"),
			IntValueDescription: "Number of freeze frame records being reported",
			ColumnDescriptions: map[string]string{
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.ecu_id":             "Electronic Control Unit (ECU) identifier that reported the freeze frame",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.spn":                "Suspect Parameter Number (SPN) indicating the specific parameter or subsystem that triggered the freeze frame",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.fmi":                "Failure Mode Identifier (0-31) indicating the type of failure that occurred",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.spn_version_legacy": "Boolean indicating if the SPN version is legacy format (true) or post 1996 V4 format (false)",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.occurrence_count":   "Number of times this specific fault has occurred (0 = unknown/unsupported)",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.parameters.spn":     "SPN of the captured parameter data at time of fault",
				"value.proto_value.freeze_frame_report.j1939_freeze_frame_data.parameters.data":    "Raw parameter data bytes captured at time of fault",
			},
		},
	}
}

func (r *RegistryStat) GetOsDDpfRegenActiveFiltered() *Stat {
	return &Stat{
		StatType:   objectstatproto.ObjectStatEnum_osDDpfRegenActiveFiltered,
		Kind:       StatKindObjectStat,
		Production: false,
		Tags:       []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Derived DPF regen stat that indicates DPF regen is truly in progress",
			Frequency:           FrequencyCustom("When any of the input stats change (RPM, accel pedal, speed, DPF status)"),
			IntValueDescription: "0 = DPF regen not in progress, 1 = DPF regen in progress. Computed from multiple diagnostics: RPM >= 800, accel pedal at or below threshold, speed < 2 km/h, and DPF status active or forced.",
		},
	}
}

func (r *RegistryStat) GetOsDEcoDrivingEventHardAcceleration() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDEcoDrivingEventHardAcceleration,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "ObdSegmentedEvent",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports hard acceleration events with rich statistics about signal values during the event duration",
			Frequency:           FrequencyOnChange("Reported when hard acceleration event conditions are met"),
			IntValueDescription: "Duration of the event in milliseconds",
			ColumnDescriptions: map[string]string{
				"value.proto_value.obd_segmented_event.duration_ms":                                    "Duration of the event in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.obd_value":                 "OBD value identifier that triggered the condition",
				"value.proto_value.obd_segmented_event.triggered_conditions.mean_value":                "Mean measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.min_value":                 "Minimum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.max_value":                 "Maximum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.threshold_value":           "Threshold value that was configured for comparison",
				"value.proto_value.obd_segmented_event.triggered_conditions.operator":                  metadatahelpers.EnumDescription("Comparison operator for threshold evaluation", hubproto.ObdSegmentedEventsConfig_OperatorType_name),
				"value.proto_value.obd_segmented_event.triggered_conditions.condition_met_duration_ms": "How long the condition was met in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.value_count":               "Total count of values sampled during the condition",
				"value.proto_value.obd_segmented_event.min_active_duration_ms":                         "Minimum duration the event must be active before triggering",
			},
		},
	}
}

func (r *RegistryStat) GetOsDEcoDrivingEventOverspeed() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDEcoDrivingEventOverspeed,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "ObdSegmentedEvent",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports overspeeding events with rich statistics about signal values during the event duration",
			Frequency:           FrequencyOnChange("Reported when overspeeding event conditions are met"),
			IntValueDescription: "Duration of the event in milliseconds",
			ColumnDescriptions: map[string]string{
				"value.proto_value.obd_segmented_event.duration_ms":                                    "Duration of the event in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.obd_value":                 "OBD value identifier that triggered the condition",
				"value.proto_value.obd_segmented_event.triggered_conditions.mean_value":                "Mean measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.min_value":                 "Minimum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.max_value":                 "Maximum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.threshold_value":           "Threshold value that was configured for comparison",
				"value.proto_value.obd_segmented_event.triggered_conditions.operator":                  metadatahelpers.EnumDescription("Comparison operator for threshold evaluation", hubproto.ObdSegmentedEventsConfig_OperatorType_name),
				"value.proto_value.obd_segmented_event.triggered_conditions.condition_met_duration_ms": "How long the condition was met in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.value_count":               "Total count of values sampled during the condition",
				"value.proto_value.obd_segmented_event.min_active_duration_ms":                         "Minimum duration the event must be active before triggering",
			},
		},
	}
}

func (r *RegistryStat) GetOsDEcoDrivingEventCruiseControl() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDEcoDrivingEventCruiseControl,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "ObdSegmentedEvent",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports cruise control events with rich statistics about signal values during the event duration",
			Frequency:           FrequencyOnChange("Reported when cruise control event conditions are met"),
			IntValueDescription: "Duration of the event in milliseconds",
			ColumnDescriptions: map[string]string{
				"value.proto_value.obd_segmented_event.duration_ms":                                    "Duration of the event in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.obd_value":                 "OBD value identifier that triggered the condition",
				"value.proto_value.obd_segmented_event.triggered_conditions.mean_value":                "Mean measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.min_value":                 "Minimum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.max_value":                 "Maximum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.threshold_value":           "Threshold value that was configured for comparison",
				"value.proto_value.obd_segmented_event.triggered_conditions.operator":                  metadatahelpers.EnumDescription("Comparison operator for threshold evaluation", hubproto.ObdSegmentedEventsConfig_OperatorType_name),
				"value.proto_value.obd_segmented_event.triggered_conditions.condition_met_duration_ms": "How long the condition was met in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.value_count":               "Total count of values sampled during the condition",
				"value.proto_value.obd_segmented_event.min_active_duration_ms":                         "Minimum duration the event must be active before triggering",
			},
		},
	}
}

func (r *RegistryStat) GetOsDEcoDrivingEventWearFreeBraking() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDEcoDrivingEventWearFreeBraking,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "ObdSegmentedEvent",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports wear-free braking events with rich statistics about signal values during the event duration",
			Frequency:           FrequencyOnChange("Reported when wear-free braking event conditions are met"),
			IntValueDescription: "Duration of the event in milliseconds",
			ColumnDescriptions: map[string]string{
				"value.proto_value.obd_segmented_event.duration_ms":                                    "Duration of the event in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.obd_value":                 "OBD value identifier that triggered the condition",
				"value.proto_value.obd_segmented_event.triggered_conditions.mean_value":                "Mean measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.min_value":                 "Minimum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.max_value":                 "Maximum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.threshold_value":           "Threshold value that was configured for comparison",
				"value.proto_value.obd_segmented_event.triggered_conditions.operator":                  metadatahelpers.EnumDescription("Comparison operator for threshold evaluation", hubproto.ObdSegmentedEventsConfig_OperatorType_name),
				"value.proto_value.obd_segmented_event.triggered_conditions.condition_met_duration_ms": "How long the condition was met in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.value_count":               "Total count of values sampled during the condition",
				"value.proto_value.obd_segmented_event.min_active_duration_ms":                         "Minimum duration the event must be active before triggering",
			},
		},
	}
}

func (r *RegistryStat) GetOsDEcoDrivingEventCoasting() *Stat {
	return &Stat{
		StatType:           objectstatproto.ObjectStatEnum_osDEcoDrivingEventCoasting,
		Kind:               StatKindObjectStat,
		DataModelStat:      false,
		Production:         false,
		BinaryMessageField: "ObdSegmentedEvent",
		Tags:               []amundsentags.Tag{amundsentags.FirmwareTag, amundsentags.TelematicsTag},
		MetadataInfo: &MetadataInfo{
			Description:         "Reports coasting events when the vehicle is moving without throttle or brake applied, with rich statistics about signal values during the event duration",
			Frequency:           FrequencyOnChange("Reported when coasting event conditions are met"),
			IntValueDescription: "Duration of the event in milliseconds",
			ColumnDescriptions: map[string]string{
				"value.proto_value.obd_segmented_event.duration_ms":                                    "Duration of the event in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.obd_value":                 "OBD value identifier that triggered the condition",
				"value.proto_value.obd_segmented_event.triggered_conditions.mean_value":                "Mean measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.min_value":                 "Minimum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.max_value":                 "Maximum measured value from the signal during the event",
				"value.proto_value.obd_segmented_event.triggered_conditions.threshold_value":           "Threshold value that was configured for comparison",
				"value.proto_value.obd_segmented_event.triggered_conditions.operator":                  metadatahelpers.EnumDescription("Comparison operator for threshold evaluation", hubproto.ObdSegmentedEventsConfig_OperatorType_name),
				"value.proto_value.obd_segmented_event.triggered_conditions.condition_met_duration_ms": "How long the condition was met in milliseconds",
				"value.proto_value.obd_segmented_event.triggered_conditions.value_count":               "Total count of values sampled during the condition",
				"value.proto_value.obd_segmented_event.min_active_duration_ms":                         "Minimum duration the event must be active before triggering",
			},
		},
	}
}
