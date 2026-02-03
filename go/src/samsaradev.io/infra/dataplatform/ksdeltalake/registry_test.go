package ksdeltalake

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
	"samsaradev.io/infra/dataplatform/registrytest"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

func TestRegistryProductionFlag(t *testing.T) {
	references, err := registrytest.FindProductionTableReferences()
	require.NoError(t, err)

	var necessary []string
	var extra []string

	for _, table := range AllTables() {
		baseStatTableName := strings.ToLower(table.QualifiedName())
		_, ok := references[baseStatTableName]
		if ok && !table.Production {
			necessary = append(necessary, table.Name)
		} else if !ok && table.Production {
			extra = append(extra, table.Name)
		}
	}

	if len(necessary) > 0 || len(extra) > 0 {
		require.Fail(t, fmt.Sprintf("Production tables should be exactly the set of tables used by data pipelines or reports.  Should be production: %d %v\n\n, Should not be production: %d %v\n\n", len(necessary), necessary, len(extra), extra))
	}
}

func TestRegistryBigStatProductionFlag(t *testing.T) {
	references, err := registrytest.FindProductionTableReferences()
	require.NoError(t, err)

	for _, table := range AllTables() {
		s3BigStatTableName := strings.ToLower(table.S3BigStatsName())
		s3BigStatCombinedTableName := strings.ToLower(table.S3BigStatCombinedViewName())
		// Both BigStatProduction and Production should be marked true if the big stat table is referenced in production code.
		t.Run(s3BigStatTableName, func(t *testing.T) {
			bigStatFiles, bigStatOk := references[s3BigStatTableName]
			bigStatCombinedFiles, bigStatCombinedOk := references[s3BigStatCombinedTableName]
			if (bigStatOk || bigStatCombinedOk) && (!table.Production || !table.BigStatProduction) {
				require.Fail(t,
					fmt.Sprintf("S3 big stat table %s is used in a datapipeline or report, and should be marked as production. Please update the table to set both Production and BigStatProduction as true in ksdeltalake/registry.go.", table.Name),
					"big stat table files = %v", bigStatFiles,
					"big stat combined table files = %v", bigStatCombinedFiles,
				)
			}
		})
	}
}

func TestRegistryAllNewStatsHaveDescriptions(t *testing.T) {
	// These are stats that exist at the time we added descriptions to the registry.
	// We want to enforce that all new stats have descriptions. No new stats should be added
	// to this allow list. The test should pass once a description is added to the registry.
	statsWithNoDescription := map[string]struct{}{
		"kinesisstats.oscvgmcufatal":                               {},
		"kinesisstats.oscvgmcumetrics":                             {},
		"kinesisstats.osdaddressentry":                             {},
		"kinesisstats.osdaddressexit":                              {},
		"kinesisstats.osdag2xuptimeinfo":                           {},
		"kinesisstats.osdbatterypotentialswitchedmilliv":           {},
		"kinesisstats.osdcablechangedetected":                      {},
		"kinesisstats.osdcableidmeasuredmv":                        {},
		"kinesisstats.osdcableupgrade":                             {},
		"kinesisstats.osdcameraevent":                              {},
		"kinesisstats.osdcanbitratedetection":                      {},
		"kinesisstats.osdcanbuserrors":                             {},
		"kinesisstats.osdcellbytes":                                {},
		"kinesisstats.osdcellularcallendreasons":                   {},
		"kinesisstats.osdcellulardebug":                            {},
		"kinesisstats.osdchargerstatus":                            {},
		"kinesisstats.osdchargeusedmicroampsh":                     {},
		"kinesisstats.osdcm3xdddstats":                             {},
		"kinesisstats.osdcm3xfcwstats":                             {},
		"kinesisstats.osdcm3xpmicresetreasons":                     {},
		"kinesisstats.osdcm3xsystemstats":                          {},
		"kinesisstats.osdcm3xthermalsensors":                       {},
		"kinesisstats.osdcmattachedgatewayid":                      {},
		"kinesisstats.osddashcamdriveralbum":                       {},
		"kinesisstats.osddashcamseatbelt":                          {},
		"kinesisstats.osddeadreckoningdebug":                       {},
		"kinesisstats.osddeltagpsdistance":                         {},
		"kinesisstats.osdderivedgpsdistance":                       {},
		"kinesisstats.osdderivedpingcount":                         {},
		"kinesisstats.osddigioinput1":                              {},
		"kinesisstats.osddigioinput10":                             {},
		"kinesisstats.osddigioinput2":                              {},
		"kinesisstats.osddigioinput3":                              {},
		"kinesisstats.osddigioinput4":                              {},
		"kinesisstats.osddigioinput5":                              {},
		"kinesisstats.osddigioinput6":                              {},
		"kinesisstats.osddigioinput7":                              {},
		"kinesisstats.osddigioinput8":                              {},
		"kinesisstats.osddigioinput9":                              {},
		"kinesisstats.osdedgespeedlimitv2":                         {},
		"kinesisstats.osdengineimmobilizer":                        {},
		"kinesisstats.osdengineoilleveldecipercent":                {},
		"kinesisstats.osdengineoiltempmicroc":                      {},
		"kinesisstats.osdenginetotalfuelusedmilliliters":           {},
		"kinesisstats.osdenginetotalidlefuelusedmilliliters":       {},
		"kinesisstats.osdevlastchargeacwallenergyconsumedwh":       {},
		"kinesisstats.osdgatewaymicroaccelevent":                   {},
		"kinesisstats.osdgatewaymicrobackendconnectivity":          {},
		"kinesisstats.osdgatewaymicroconfig":                       {},
		"kinesisstats.osdgatewaymicroconfigupdateattempt":          {},
		"kinesisstats.osdgatewaymicrodiagnosticstatus":             {},
		"kinesisstats.osdgatewaymicrogpsinstrumentation":           {},
		"kinesisstats.osdgatewaymicrologbatchstats":                {},
		"kinesisstats.osdgatewaymicronetworkscan":                  {},
		"kinesisstats.osdgatewaymicroota":                          {},
		"kinesisstats.osdgatewaymicroresetreason":                  {},
		"kinesisstats.osdgatewaymicrototalselfdischargemicroampsh": {},
		"kinesisstats.osdgatewaymicrowifiscan":                     {},
		"kinesisstats.osdgatewayupgradeattempt":                    {},
		"kinesisstats.osdgpsdataextended":                          {},
		"kinesisstats.osdgpsdistance":                              {},
		"kinesisstats.osdgpstimetofirstfixms":                      {},
		"kinesisstats.osdgrpchubserverdeviceheartbeat":             {},
		"kinesisstats.osdimuyawangle":                              {},
		"kinesisstats.osdindustrialprotocolpollingduration":        {},
		"kinesisstats.osdindustrialprotocolsuccessrate":            {},
		"kinesisstats.osdj1939propanaloginputcurrent1microamps":    {},
		"kinesisstats.osdj1939propanaloginputcurrent2microamps":    {},
		"kinesisstats.osdj1939propanaloginputcurrent3microamps":    {},
		"kinesisstats.osdj1939propanaloginputvoltage1millivolts":   {},
		"kinesisstats.osdj1939propanaloginputvoltage2millivolts":   {},
		"kinesisstats.osdj1939propanaloginputvoltage3millivolts":   {},
		"kinesisstats.osdjetsonpowerstats":                         {},
		"kinesisstats.osdjetsonthermalstats":                       {},
		"kinesisstats.osdlivestreamwebrtctelemetry":                {},
		"kinesisstats.osdmaxvoltagejumpcountreached":               {},
		"kinesisstats.osdmcuappevent":                              {},
		"kinesisstats.osdmcufaultinfo":                             {},
		"kinesisstats.osdmodbusv2castingerrorcount":                {},
		"kinesisstats.osdmodiusbthermalshutoff":                    {},
		"kinesisstats.osdnvr10camerainfo":                          {},
		"kinesisstats.osdorientedaccelerometer":                    {},
		"kinesisstats.osdpingcount":                                {},
		"kinesisstats.osdplchealth":                                {},
		"kinesisstats.osdplcstatus":                                {},
		"kinesisstats.osdpowerfactorratio":                         {},
		"kinesisstats.osdpowersources":                             {},
		"kinesisstats.osdreboot":                                   {},
		"kinesisstats.osdreeferalarms":                             {},
		"kinesisstats.osdreeferbattvoltagemilliv":                  {},
		"kinesisstats.osdsolardebug":                               {},
		"kinesisstats.osdspeedlimitsigndetected":                   {},
		"kinesisstats.osdstopsigndetection":                        {},
		"kinesisstats.osdsystemstats":                              {},
		"kinesisstats.osdthermallimiterstate":                      {},
		"kinesisstats.osdthunderbirdexpectedsleepdurationms":       {},
		"kinesisstats.osdthunderbirdwakereason":                    {},
		"kinesisstats.osdtimetofirsthubconnection":                 {},
		"kinesisstats.osdtorquepercent":                            {},
		"kinesisstats.osdtotalapparentpowervoltampere":             {},
		"kinesisstats.osdtotalassetenginems":                       {},
		"kinesisstats.osdtotalchargeusedmicroampsh":                {},
		"kinesisstats.osdtotalkwhexported":                         {},
		"kinesisstats.osdtotalreactivepowervoltamperereactive":     {},
		"kinesisstats.osdtotalrealpowerwatt":                       {},
		"kinesisstats.osduploadedfilesetstart":                     {},
		"kinesisstats.osduploadstatus":                             {},
		"kinesisstats.osdvanishingpointstate":                      {},
		"kinesisstats.osdvgcableinfo":                              {},
		"kinesisstats.osdvgmcufatal":                               {},
		"kinesisstats.osdvgmcumetrics":                             {},
		"kinesisstats.osdvgmcuspitiming":                           {},
		"kinesisstats.osdvisionresult":                             {},
		"kinesisstats.osdvrpcclientconnected":                      {},
		"kinesisstats.osdvrpcclientmetrics":                        {},
		"kinesisstats.osdvulcanbootinfo":                           {},
		"kinesisstats.osdvulcanperipheralreset":                    {},
		"kinesisstats.osdwidgetlogbatchprocessingstat":             {},
		"kinesisstats.osdwifiapbytes":                              {},
		"kinesisstats.osdwifiapwhitelistbytes":                     {},
		"kinesisstats.osodeepstreamaggregates":                     {},
		"kinesisstats.osomotionaggregate":                          {},
		"kinesisstats.ospeipread":                                  {},
		"kinesisstats.ospmodbusread":                               {},
		"kinesisstats.oswadvertisementstatistics":                  {},
		"kinesisstats.oswanomalyevent":                             {},
		"kinesisstats.oswconndevicerssi":                           {},
		"kinesisstats.oswconnectinfo":                              {},
		"kinesisstats.oswconnectionlifecycle":                      {},
		"kinesisstats.oswlogstatistics":                            {},
	}

	// Some stats were added before Frequency and Int/Double/Proto checks were created
	// TODO: Ask folks who added these stats to add this metadata
	frequencyIntDoubleProtoAllowList := map[string]struct{}{
		"kinesisstats.osdbendixstats":                                 {},
		"kinesisstats.osdreeferattachedcontroller":                    {},
		"kinesisstats.osdreefercommandmetadata":                       {},
		"kinesisstats.osdcm3xpowerstats":                              {},
		"kinesisstats.osddashcamdriverobstruction":                    {},
		"kinesisstats.osdedgespeedlimitosdinternaltestodometermeters": {},
		"kinesisstats.osdpassengersupportedcommands":                  {},
		"kinesisstats.osdpgnsseen":                                    {},
		"kinesisstats.osdreboottoreadonlyevent":                       {},
		"kinesisstats.osdreporteddeviceconfig":                        {},
		"kinesisstats.osdservicestopinfo":                             {},
		"kinesisstats.osdvehiclecurrentgear":                          {},
		"kinesisstats.osdrivercardbatteryinfo":                        {},
	}

	for _, table := range AllTables() {
		name := strings.ToLower(table.QualifiedName())
		t.Run(fmt.Sprintf("%s ks delta lake metadata test", name), func(t *testing.T) {
			if _, ok := statsWithNoDescription[name]; !ok { // If the stat is not in the allow list, do all the checks below
				if table.MetadataInfo != nil {
					if table.MetadataInfo.Description ==
						"" {
						require.Fail(t,
							"The new kinesis stat in delta lake registry does not have a description field populated. Please populate description field so downstream consumers know how to use the stat.",
							"name = %v", name)
					}

					// Stats with deprecated descriptions don't need any frequency or column descriptions populated
					if strings.Contains(table.MetadataInfo.Description, metadatahelpers.DeprecatedDefaultDescription) {
						return
					}

					if _, ok := frequencyIntDoubleProtoAllowList[name]; !ok {
						if table.MetadataInfo.Frequency == nil {
							require.Fail(t,
								"The new kinesis stat in delta lake registry does not have a frequency field populated. Please populate description field so downstream consumers know how often the stat is written to the backend.",
								"name = %v", name)
						}

						// Check that int, double, or proto_value descriptions are populated
						intDoubleOrProtoValueDescriptionPopulated := false

						if table.MetadataInfo.IntValueDescription !=
							"" || table.MetadataInfo.DoubleValueDescription !=
							"" {
							intDoubleOrProtoValueDescriptionPopulated = true
						}
						for colName := range table.MetadataInfo.ColumnDescriptions {
							if strings.Contains(colName,
								"value.proto_value.") || strings.Contains(colName,
								"value.") {
								intDoubleOrProtoValueDescriptionPopulated = true
								break
							}
						}
						if !intDoubleOrProtoValueDescriptionPopulated {
							require.Fail(t,
								"The new kinesis stat in delta lake registry does not have a int, double, or proto value description field populated. Please populate a description for IntValueDescription, DoubleValueDescription, or proto_value field in columnDescriptions so downstream consumers know what the value describes.",
								"name = %v", name)
						}
					}
				} else {
					require.Fail(t,
						"The new kinesis stat in delta lake registry does not have metadata info. Please populate the fieldds in metadata info field so downstream consumers know how to use the stat.",
						"name = %v", name)
				}
			} else {
				if table.MetadataInfo != nil {
					require.Fail(t, fmt.Sprintf("%s has metadata populated. Please remove it from the allow list in infra/dataplatform/ksdeltalake/registry_test.go --> TestRegistryAllNewStatsHaveDescriptions", name))
				}
			}
		})
	}

}

func TestRegistryColumnDescriptions(t *testing.T) {
	for _, table := range AllTables() {
		if table.MetadataInfo != nil && len(table.MetadataInfo.ColumnDescriptions) > 0 {
			names := ParseFieldNamesOnType(table.Schema)
			fieldNames := make(map[string]struct{}, len(names))
			for _, name := range names {
				fieldNames[name] = struct{}{}
			}

			bigStatFieldNames := make(map[string]struct{}, len(names))
			if table.S3BigStatSchema != nil {
				bigStatNames := ParseFieldNamesOnType(table.S3BigStatSchema)
				for _, name := range bigStatNames {
					bigStatFieldNames[name] = struct{}{}
				}
			}

			for colName := range table.MetadataInfo.ColumnDescriptions {
				_, isColName := fieldNames[colName]
				_, isBigStatColName := bigStatFieldNames[colName]
				if !isColName && !isBigStatColName {
					require.Fail(t, fmt.Sprintf("KS Table description for stat: %s does not contain the column: %s (columnNames that exist on the stat): %v)", table.Name, colName, names))
				}
			}
		}
	}
}

func TestParseFieldNamesOnType(t *testing.T) {
	snapper := snapshotter.New(t)
	defer snapper.Verify()
	names := ParseFieldNamesOnType(&sparktypes.Type{
		Type: sparktypes.TypeEnumStruct,
		Fields: []*sparktypes.Field{
			{
				Name: "map_field",
				Type: &sparktypes.Type{
					Type: sparktypes.TypeEnumMap,
					KeyType: &sparktypes.Type{
						Type: sparktypes.TypeEnumString,
					},
					ElementType: &sparktypes.Type{
						Type: sparktypes.TypeEnumString,
					},
				},
			},
			{
				Name: "array_field",
				Type: &sparktypes.Type{
					Type: sparktypes.TypeEnumArray,
					ElementType: &sparktypes.Type{
						Type: sparktypes.TypeEnumString,
					},
				},
			},
			{
				Name: "struct_field",
				Type: &sparktypes.Type{
					Type: sparktypes.TypeEnumStruct,
					Fields: []*sparktypes.Field{
						{
							Name: "nested_int_field",
							Type: &sparktypes.Type{
								Type: sparktypes.TypeEnumInteger,
							},
						},
						{
							Name: "nested_string_field",
							Type: &sparktypes.Type{
								Type: sparktypes.TypeEnumString,
							},
						},
					},
				},
			},
			{
				Name: "int_field",
				Type: &sparktypes.Type{
					Type: sparktypes.TypeEnumInteger,
				},
			},
			{
				Name: "string_field",
				Type: &sparktypes.Type{
					Type: sparktypes.TypeEnumString,
				},
			},
		},
	})
	snapper.Snapshot("TestParseFieldNamesOnType", names)
}

func TestTieredScheduleJobs(t *testing.T) {
	// It is not advisable in most cases to attempt to set TieredSchedule on an existing stat.
	// This is because the new daily job, once created, will have an empty checkpoint
	// and will attempt to reprocess the entirety of the raw table.
	// For new stats or stats that have only very recently begun being ingested, it's probably more safe.
	// This test is a warning more than anything, to prevent us from accidentally turning on this setting! If you
	// know that it's okay, please add your stat to the exception. (Note that i've not made this a snapshot test
	// to avoid people using sheepy to patch the snapshot and avoid reading this).
	allowedTieredSchedules := map[string]struct{}{
		"location":                     struct{}{},
		"osDBattery":                   struct{}{},
		"osDCableVoltage":              struct{}{},
		"osDEngineState":               struct{}{},
		"osDLocationWithRoadContext":   struct{}{},
		"osDBackendComputedSpeedLimit": struct{}{},
	}

	actualTieredSchedules := make(map[string]struct{})
	for _, table := range AllTables() {
		if table.InternalOverrides != nil && table.InternalOverrides.UseTieredSchedule {
			actualTieredSchedules[table.Name] = struct{}{}
		}
	}

	// Figure out which stats are not allowed based on the test, and also which are extra in this test.
	var extraAllowedTables []string
	var disallowedTables []string

	for table := range allowedTieredSchedules {
		if _, ok := actualTieredSchedules[table]; !ok {
			extraAllowedTables = append(extraAllowedTables, table)
		}
	}

	for table := range actualTieredSchedules {
		if _, ok := allowedTieredSchedules[table]; !ok {
			disallowedTables = append(disallowedTables, table)
		}
	}

	if len(extraAllowedTables) > 0 || len(disallowedTables) > 0 {
		t.Errorf("Please update this test in ksdeltalake/registry_test.go to correct the registry. Tables with tiered schedules that should be registered in the test: [%s], Tables registered in the test where the stat is not using a tiered schedule (or, no longer exists) [%s]", strings.Join(disallowedTables, ","), strings.Join(extraAllowedTables, ","))
	}

}

func TestLegacyUint64Tables(t *testing.T) {
	// This test checks the tables that have legacy conversion parameters set. These exist only
	// for backwards compatibility before this setting was deprecated.
	// Please do not add any new tables to this list.

	var tablesWithLegacy []string
	expected := []string{
		"osCVgMcuFatal",
		"osCVgMcuMetrics",
		"osDAccelerometer",
		"osDAccelerometerShadow",
		"osDAnomalyEvent",
		"osDCM3xDDDStats",
		"osDCM3xFCWStats",
		"osDCanBitrateDetection",
		"osDCanBusErrors",
		"osDDashcamSeatbelt",
		"osDDashcamStreamStatus",
		"osDEdgeSpeedLimit",
		"osDEdgeSpeedLimitV2",
		"osDEngineGauge",
		"osDGPSDataExtended",
		"osDGatewayMicroBackendConnectivity",
		"osDGatewayMicroConfig",
		"osDGatewayMicroDiagnosticStatus",
		"osDGatewayMicroLogBatchStats",
		"osDGpsTimeToFirstFixMs",
		"osDIncrementalCellularUsage",
		"osDIndustrialProtocolPollingDuration",
		"osDIndustrialProtocolSuccessRate",
		"osDLivestreamWebRTCTelemetry",
		"osDMcuAppEvent",
		"osDMcuFaultInfo",
		"osDModiDeviceInfo",
		"osDPlcHealth",
		"osDStorageMetrics",
		"osDSystemStats",
		"osDTileDiskUsage",
		"osDVgCableInfo",
		"osDVgMcuFatal",
		"osDVgMcuMetrics",
		"osDVrpcClientMetrics",
		"osGServicesMetrics",
		"osInvalid",
		"osODeepStreamMetadata",
		"osOMotionAggregate",
		"osWAnomalyEvent",
		"osWConnectInfo",
		"osWConnectionLifeCycle",
	}

	emptyParams := sparktypes.ConversionParams{}
	for _, stat := range rawStats() {
		if stat.DoNotSetConversionParams != emptyParams {
			tablesWithLegacy = append(tablesWithLegacy, stat.StatType.String())
		}
	}
	sort.Strings(tablesWithLegacy)
	assert.Equal(t, expected, tablesWithLegacy)
}
