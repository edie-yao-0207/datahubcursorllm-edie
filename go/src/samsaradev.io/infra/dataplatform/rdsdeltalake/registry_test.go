package rdsdeltalake

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/api/api2models"
	"samsaradev.io/connectedworker/workforce/workforcevideomodels"
	"samsaradev.io/firmware/firmwaremodels"
	"samsaradev.io/fleet/activations/activationsmodels"
	"samsaradev.io/fleet/assets/associations/associationsmodels"
	"samsaradev.io/fleet/assets/jobschedules/jobschedulesmodels"
	"samsaradev.io/fleet/assets/pingschedules/pingschedulesmodels"
	"samsaradev.io/fleet/assets/sensorconfiguration/sensorconfigmodels"
	"samsaradev.io/fleet/compliance/compliancemodels"
	"samsaradev.io/fleet/compliance/eldevents/eldeventsmodels"
	"samsaradev.io/fleet/compliance/eldhos/eldhosmodels"
	"samsaradev.io/fleet/compliance/eu/eucomplianceserver/eucompliancemodels"
	"samsaradev.io/fleet/compliance/eu/eurofleetmodels"
	"samsaradev.io/fleet/compliance/eu/tachographmodels"
	"samsaradev.io/fleet/detention/detentionmodels"
	"samsaradev.io/fleet/deviceassociations/deviceassociationsmodels"
	"samsaradev.io/fleet/devicemetadata/devicemetadatamodels"
	"samsaradev.io/fleet/diagnostics/signalpromotion/signalpromotionmodels"
	"samsaradev.io/fleet/dispatch/dispatchmodels"
	"samsaradev.io/fleet/driverdocuments/driverdocumentsmodels"
	"samsaradev.io/fleet/ecodriving/ecodrivingmodels"
	"samsaradev.io/fleet/emissionsreporting/emissionsreportingmodels"
	"samsaradev.io/fleet/engineactivity/engineactivitymodels"
	"samsaradev.io/fleet/fuel/fuelcards/fuelcardintegrationsmodels"
	"samsaradev.io/fleet/fuel/fuelcards/fuelcardsmodels"
	"samsaradev.io/fleet/fuel/fuelmodels"
	"samsaradev.io/fleet/fuel/locale/localemodels"
	"samsaradev.io/fleet/maintenance/maintenancemodels"
	"samsaradev.io/fleet/messages/messagesmodels"
	"samsaradev.io/fleet/oem/oemmodels"
	"samsaradev.io/fleet/routingplatform/routeplanning/routeplanningmodels"
	"samsaradev.io/fleet/sled/timecards/timecardsmodels"
	"samsaradev.io/fleet/vin/vinmodels"
	"samsaradev.io/forms/formsmodels"
	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/hubproto"
	"samsaradev.io/inbox/inboxmodels"
	"samsaradev.io/industrial/industrialcore/industrialcoremodels"
	"samsaradev.io/infra/api/developers/developersmodels"
	"samsaradev.io/infra/dataplatform/dataplatformtestinternalmodels"
	"samsaradev.io/infra/dataplatform/registrytest"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/deploymachinery/deploymodels"
	"samsaradev.io/infra/internal/internalmodels"
	"samsaradev.io/infra/releasemanagement/releasemanagementmodels"
	"samsaradev.io/infra/retention/retentionmodels"
	"samsaradev.io/infra/security/encrypt/encryptionkeyservice/encryptionkeymodels"
	"samsaradev.io/infra/speedlimit/speedlimitsmodels"
	"samsaradev.io/infra/sredummy/sredummymodels"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/mediacatalog/mediacatalogmodels"
	"samsaradev.io/ml/agent/agentmodels"
	"samsaradev.io/mobile/mdm/mdmmodels"
	"samsaradev.io/mobile/mobilemodels"
	"samsaradev.io/mobile/remotesupport/remotesupportmodels"
	"samsaradev.io/models"
	"samsaradev.io/models/productsmodels"
	"samsaradev.io/platform/alerts/alertsmodels"
	"samsaradev.io/platform/attributes/attributescore/attributemodels"
	"samsaradev.io/platform/audits/auditsmodels"
	"samsaradev.io/platform/csvuploads/csvuploadsmodels"
	"samsaradev.io/platform/finops/finopsmodels"
	"samsaradev.io/platform/licenseentity/licenseentitymodels"
	"samsaradev.io/platform/locationservices/driverassignment/driverassignmentsmodels"
	"samsaradev.io/platform/locationservices/geoevents/geoeventsmodels"
	"samsaradev.io/platform/mapping/places/placemodels"
	"samsaradev.io/platform/oauth2/oauth2models"
	"samsaradev.io/platform/sessions/sessionsmodels"
	"samsaradev.io/platform/userorgpreferences/userorgpreferencesmodels"
	"samsaradev.io/platform/workflows/workflowsmodels"
	"samsaradev.io/reports/reportconfigmodels"
	"samsaradev.io/safety/infra/cmassetsmodels"
	"samsaradev.io/safety/infra/coachingmodels"
	"samsaradev.io/safety/infra/safetyeventingestionmodels"
	"samsaradev.io/safety/infra/safetyeventreviewmodels"
	"samsaradev.io/safety/infra/safetyeventtriagemodels"
	"samsaradev.io/safety/infra/safetymodels"
	"samsaradev.io/safety/infra/safetyreportingmodels"
	"samsaradev.io/safety/infra/safetyriskmodels"
	"samsaradev.io/safety/infra/scoringmodels"
	"samsaradev.io/safety/rewards/rewardsmodels"
	"samsaradev.io/safety/safetyvision/facial_recognition/recognitionmodels"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/service/registry"
	"samsaradev.io/stats/trips/trips2models"
	"samsaradev.io/stats/trips/tripsmodels"
	"samsaradev.io/stats_models"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/training/trainingmodels"
	"samsaradev.io/userpreferences/userpreferencesmodels"
)

func TestRegistrableTables(t *testing.T) {
	snapper := snapshotter.New(t)
	defer snapper.Verify()

	testCases := []struct {
		testName     string
		dbDefinition databaseDefinition
		errExpected  bool
	}{
		{
			testName: "Non-Sharded DB",
			dbDefinition: databaseDefinition{
				name:    dbregistry.AlertsDB,
				Sharded: false,
				Tables: map[string]tableDefinition{
					"alert_events": {
						primaryKeys:       []string{"alert_id", "started_at", "object_ids", "output_key"},
						PartitionStrategy: SinglePartition,
						Description:       "I describe alert events",
						ColumnDescriptions: map[string]string{
							"alert_id": "I describe an alert id",
						},
					},
				},
			},
			errExpected: false,
		},
		{
			testName: "Sharded DB",
			dbDefinition: databaseDefinition{
				name:    dbregistry.SafetyDB,
				Sharded: true,
				Tables: map[string]tableDefinition{
					"safety_events": {
						primaryKeys:         []string{"org_id", "device_id", "event_ms"},
						PartitionStrategy:   DateColumnFromMilliseconds("event_ms"),
						customCombineShards: true,
						ColumnDescriptions: map[string]string{
							"device_id": "Unique id associated with an device",
							"event_id":  "Unique id associated with an event",
						},
					},
				},
			},
			errExpected: false,
		},
		{
			testName: "EU only table",
			dbDefinition: databaseDefinition{
				name:    dbregistry.Api2DB,
				Sharded: false,
				Tables: map[string]tableDefinition{
					"api_events": {
						primaryKeys:       []string{"org_id", "event_id"},
						PartitionStrategy: SinglePartition,
						allowedRegions:    []string{infraconsts.SamsaraAWSEURegion},
						ColumnDescriptions: map[string]string{
							"event_id": "Unique id associated with an event",
						},
					},
				},
			},
			errExpected: false,
		},
	}

	for _, tc := range testCases {
		registryDefinition, err := tc.dbDefinition.registryDatabase()
		if tc.errExpected {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}

		// Because this is an absolute path, it will differ when run in local vs buildkite, so we set the value
		// to something constant here so that the snapshot is consistent.
		registryDefinition.SchemaPath = "test_schema_path"
		snapper.Snapshot(tc.testName, registryDefinition)
		for _, table := range registryDefinition.Tables {
			colDescriptions := registryDefinition.GetAllColumnDescriptions(table)
			listofColDescriptions := make([]string, 0, len(colDescriptions))
			for colName, colDescription := range colDescriptions {
				listofColDescriptions = append(listofColDescriptions, fmt.Sprintf("%s %s", colName, colDescription))
			}
			sort.Strings(listofColDescriptions)
			snapper.Snapshot(fmt.Sprintf("%s-get-all-descriptions", tc.testName), listofColDescriptions)
		}
	}
}

func TestGetSparkFriendlyDBName(t *testing.T) {
	testCases := []struct {
		table                *Table
		expectedDbNameOutput string
	}{
		{
			table: &Table{
				RdsDb: "prod-safetydb",
			},
			expectedDbNameOutput: "safetydb",
		},
		{
			table: &Table{
				RdsDb: "prod-safety-shard-1db",
			},
			expectedDbNameOutput: "safety_shard_1db",
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expectedDbNameOutput, GetSparkFriendlyRdsDBName(tc.table.RdsDb, tc.table.DBSharded, false))
	}
}

// test if substr exists in filename
func fileContainsStruct(filename string, substr string) (bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if strings.Contains(scanner.Text(), substr) {
			return true, nil
		}
	}
	return false, nil
}

func TestValidProtoDetails(t *testing.T) {
	for _, db := range databaseDefinitions {
		for tableName, tableDef := range db.Tables {
			if tableDef.ProtoSchema != nil {
				for protoColumnName, protoDetail := range tableDef.ProtoSchema {
					testCaseName := fmt.Sprintf("%s/%s/%s", db.Name(), tableName, protoColumnName)
					t.Run(testCaseName, func(t *testing.T) {
						// Assert that for each ProtoDetail, protoPath field is defined.
						relProtoFileName := protoDetail.ProtoPath
						assert.True(t,
							relProtoFileName != "",
							"ProtoDetail for %s/%s/%s does not have protoPath defined",
							db.Name(), tableName, protoColumnName,
						)

						// Validate that every ProtoDetail.protoPath points to the
						// correct file that ends with .proto
						extension := ".proto"
						assert.True(t,
							strings.HasSuffix(protoDetail.ProtoPath, extension),
							"Expected %s to end with %s", protoDetail.ProtoPath, extension,
						)

						// Validate that the given proto file contains the proto schema
						structInstance := protoDetail.Message
						structName := reflect.TypeOf(structInstance).Elem().Name()
						protoFilename := filepath.Join(filepathhelpers.BackendRoot, relProtoFileName)

						pbGoFilename := protoFilename[:len(protoFilename)-len(extension)] + ".pb.go"

						structExists, err := fileContainsStruct(pbGoFilename, structName)

						assert.True(t, err == nil && structExists,
							"%s is not defined in the specified proto file %s",
							structName, relProtoFileName,
						)
					})
				}
			}
		}
	}
}

func TestValidVersionsParquet(t *testing.T) {
	for _, db := range databaseDefinitions {
		db := db // capture loop variable
		t.Run(db.Name(), func(t *testing.T) {
			for tableName, tableDef := range db.Tables {
				tableName, tableDef := tableName, tableDef // capture loop variables
				t.Run(tableName, func(t *testing.T) {
					allowedVersions := make(map[TableVersion]struct{})
					for _, version := range tableDef.versionInfo.VersionsParquet() {
						allowedVersions[version] = struct{}{}
					}
					assert.Contains(t,
						tableDef.versionInfo.VersionsParquet(),
						tableDef.versionInfo.DMSOutputVersionParquet(),
						"dmsOutput parquet version must be in allVersions")
					assert.Contains(t,
						tableDef.versionInfo.VersionsParquet(),
						tableDef.versionInfo.SparkVersionParquet(),
						"spark parquet version must be in allVersions")
				})
			}
		})
	}
}

// We specify DBVersions here (instead of in registry.go) to prevent the models
// being compiled in to everything that uses the registry.
var databaseNameToDbVersions = map[string]dbhelpers.DBVersions{
	dbregistry.ActivationsDB:              activationsmodels.Versions,
	dbregistry.AgentDB:                    agentmodels.Versions,
	dbregistry.AlertsDB:                   alertsmodels.Versions,
	dbregistry.Api2DB:                     api2models.Versions,
	dbregistry.AssociationsDB:             associationsmodels.Versions,
	dbregistry.AttributeDB:                attributemodels.Versions,
	dbregistry.AuditsDB:                   auditsmodels.Versions,
	dbregistry.CloudDB:                    models.Versions,
	dbregistry.CmAssetsDB:                 cmassetsmodels.Versions,
	dbregistry.CoachingDB:                 coachingmodels.Versions,
	dbregistry.ComplianceDB:               compliancemodels.Versions,
	dbregistry.CsvUploadsDB:               csvuploadsmodels.Versions,
	dbregistry.DataPlatformTestInternalDB: dataplatformtestinternalmodels.Versions,
	dbregistry.DeployDB:                   deploymodels.Versions,
	dbregistry.DetentionDB:                detentionmodels.Versions,
	dbregistry.DevelopersDB:               developersmodels.Versions,
	dbregistry.DeviceAssociationsDB:       deviceassociationsmodels.Versions,
	dbregistry.DeviceMetadataDB:           devicemetadatamodels.Versions,
	dbregistry.DispatchDB:                 dispatchmodels.Versions,
	dbregistry.DriverAssignmentsDB:        driverassignmentsmodels.Versions,
	dbregistry.DriverDocumentsDB:          driverdocumentsmodels.Versions,
	dbregistry.EldEventsDB:                eldeventsmodels.Versions,
	dbregistry.EldHosDB:                   eldhosmodels.Versions,
	dbregistry.EmissionsReportingDB:       emissionsreportingmodels.Versions,
	dbregistry.EncryptionKeyDB:            encryptionkeymodels.Versions,
	dbregistry.EngineActivityDB:           engineactivitymodels.Versions,
	dbregistry.EcoDrivingDB:               ecodrivingmodels.Versions,
	dbregistry.EuComplianceDB:             eucompliancemodels.Versions,
	dbregistry.EurofleetDB:                eurofleetmodels.Versions,
	dbregistry.FinopsDB:                   finopsmodels.Versions,
	dbregistry.FirmwareDB:                 firmwaremodels.Versions,
	dbregistry.FormsDB:                    formsmodels.Versions,
	dbregistry.FuelCardIntegrationsDB:     fuelcardintegrationsmodels.Versions,
	dbregistry.FuelCardsDB:                fuelcardsmodels.Versions,
	dbregistry.FuelDB:                     fuelmodels.Versions,
	dbregistry.GeoEventsDB:                geoeventsmodels.Versions,
	dbregistry.InboxDB:                    inboxmodels.Versions,
	dbregistry.IndustrialCoreDB:           industrialcoremodels.Versions,
	dbregistry.InternalDB:                 internalmodels.Versions,
	dbregistry.JobSchedulesDB:             jobschedulesmodels.Versions,
	dbregistry.LocaleDB:                   localemodels.Versions,
	dbregistry.LicenseEntityDB:            licenseentitymodels.Versions,
	dbregistry.MaintenanceDB:              maintenancemodels.Versions,
	dbregistry.MdmDB:                      mdmmodels.Versions,
	dbregistry.MediaCatalogDB:             mediacatalogmodels.Versions,
	dbregistry.MessagesDB:                 messagesmodels.Versions,
	dbregistry.MobileDB:                   mobilemodels.Versions,
	dbregistry.Oauth2DB:                   oauth2models.Versions,
	dbregistry.OemDB:                      oemmodels.Versions,
	dbregistry.PingSchedulesDB:            pingschedulesmodels.Versions,
	dbregistry.PlaceDB:                    placemodels.Versions,
	dbregistry.ProductsDB:                 productsmodels.Versions,
	dbregistry.RecognitionDB:              recognitionmodels.Versions,
	dbregistry.ReleaseManagementDB:        releasemanagementmodels.Versions,
	dbregistry.RemoteSupportDB:            remotesupportmodels.Versions,
	dbregistry.ReportConfigDB:             reportconfigmodels.Versions,
	dbregistry.RetentionDB:                retentionmodels.Versions,
	dbregistry.RoutePlanningDB:            routeplanningmodels.Versions,
	dbregistry.SessionsDB:                 sessionsmodels.Versions,
	dbregistry.SafetyDB:                   safetymodels.Versions,
	dbregistry.SafetyEventIngestionDB:     safetyeventingestionmodels.Versions,
	dbregistry.SafetyEventReviewDB:        safetyeventreviewmodels.Versions,
	dbregistry.SafetyEventTriageDB:        safetyeventtriagemodels.Versions,
	dbregistry.SafetyReportingDB:          safetyreportingmodels.Versions,
	dbregistry.SafetyRiskDB:               safetyriskmodels.Versions,
	dbregistry.ScoringDB:                  scoringmodels.Versions,
	dbregistry.SensorConfigDB:             sensorconfigmodels.Versions,
	dbregistry.SignalPromotionDB:          signalpromotionmodels.Versions,
	dbregistry.SpeedlimitsDB:              speedlimitsmodels.Versions,
	dbregistry.SREDummyDB:                 sredummymodels.Versions,
	dbregistry.StatsDB:                    stats_models.Versions,
	dbregistry.TachographDB:               tachographmodels.Versions,
	dbregistry.TimecardsDB:                timecardsmodels.Versions,
	dbregistry.TrainingDB:                 trainingmodels.Versions,
	dbregistry.Trips2DB:                   trips2models.Versions,
	dbregistry.TripsDB:                    tripsmodels.Versions,
	dbregistry.UserOrgPreferencesDB:       userorgpreferencesmodels.Versions,
	dbregistry.UserPreferencesDB:          userpreferencesmodels.Versions,
	dbregistry.VinDB:                      vinmodels.Versions,
	dbregistry.WorkflowsDB:                workflowsmodels.Versions,
	dbregistry.WorkforceVideoDB:           workforcevideomodels.Versions,
	dbregistry.RewardsDB:                  rewardsmodels.Versions,
}

func TestBinlogRetentionSet(t *testing.T) {
	for _, db := range databaseDefinitions {
		db := db // capture loop variable
		t.Run(db.Name(), func(t *testing.T) {
			dbVersions, ok := databaseNameToDbVersions[db.name]
			require.True(t, ok, "DBVersions not specified")

			// Skip PostgreSQL databases for this test
			if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				return
			}

			binlogMigration := false
			currentVersion := dbVersions.Applied

			for currentVersion >= 0 {
				migration, ok := dbVersions.Versions[currentVersion]
				if !ok {
					currentVersion--
					continue
				}

				for _, stmt := range migration.GetStatements() {
					if strings.Contains(strings.ToLower(stmt), "call mysql.rds_set_configuration('binlog retention hours',") {
						re := regexp.MustCompile("[0-9]+")
						hours := re.FindString(stmt)
						hoursInt, err := strconv.Atoi(hours)
						require.NoError(t, err, "binlog retention period does not parse to an int")
						require.GreaterOrEqual(t, hoursInt, 24, "binlog retention period must be greater than or equal to 24 hours")
						binlogMigration = true
						break
					}
				}

				if binlogMigration {
					break
				}
				currentVersion--
			}

			require.True(t, binlogMigration, "binlog retention not set in any applied migration.\nPlease add a migration to set binlog retention to at least 24 hours.\nExample statement: 'CALL mysql.rds_set_configuration('binlog retention hours', 48);'.")
		})
	}
}

var passwordFile = filepath.Join(filepathhelpers.BackendRoot, "docker", "backend_build", "web", ".secrets.json")

func TestDatabasePasswordKeys(t *testing.T) {
	passwordData, err := os.ReadFile(passwordFile)
	require.NoError(t, err)

	var passwords map[string]string
	err = json.Unmarshal(passwordData, &passwords)
	require.NoError(t, err)

	for _, db := range databaseDefinitions {
		db := db // capture loop variable
		t.Run(db.Name(), func(t *testing.T) {
			assert.Contains(t, passwords, db.PasswordKey(), "cannot find password in .secrets.json consider specifying an override")
		})
	}
}

// parsePostgreSQLTableNamesFromSQLStatements parses SQL schema statements for PostgreSQL
// and returns a map of created table names (as keys) with schema prefix using "__" separator, mapping to the CREATE statement.
// Example: "public.place" becomes "public__place", and "place" becomes "public__place"
// This function should only be called when database is PostgreSQL.
func parsePostgreSQLTableNamesFromSQLStatements(sqlStatements []string) map[string]string {
	// PostgreSQL table name regex that handles schema prefixes and double quotes
	// Matches: CREATE TABLE public.place ( or CREATE TABLE "public"."place" ( or CREATE TABLE place (
	// Pattern: optional schema (with optional quotes) followed by table name (with optional quotes)
	// Group 1: schema name (may be empty if no schema prefix)
	// Group 2: table name
	tableNameRegexp := regexp.MustCompile(`CREATE TABLE\s+(?:"?([a-zA-Z_][a-zA-Z0-9_]*)"?\.)?(?:"?([a-zA-Z_][a-zA-Z0-9_]*)"?)\s*\(`)

	tableMap := make(map[string]string)
	for _, sqlStatement := range sqlStatements {
		sqlStatementTrimmed := strings.TrimSpace(sqlStatement)
		if !strings.Contains(strings.ToUpper(sqlStatementTrimmed), "CREATE TABLE") {
			continue
		}

		tableNameMatches := tableNameRegexp.FindStringSubmatch(sqlStatementTrimmed)
		if len(tableNameMatches) < 3 {
			continue
		}

		// Extract table name, handling both schema.table and just table formats
		// FindStringSubmatch returns matches in order of capturing groups:
		// [0] = full match
		// [1] = schema name (or empty if no schema prefix)
		// [2] = table name
		// If no schema is specified, default to "public"
		var tableName string
		if len(tableNameMatches) > 2 && tableNameMatches[1] != "" {
			// Has schema prefix: schema.table
			tableName = tableNameMatches[1] + "__" + tableNameMatches[2] // __ is used to separate the schema name from the table name
		} else if len(tableNameMatches) > 2 && tableNameMatches[2] != "" {
			// No schema prefix: default to public.table
			tableName = "public__" + tableNameMatches[2] // __ is used to separate the schema name from the table name
		} else {
			continue
		}

		tableMap[tableName] = sqlStatementTrimmed
	}

	return tableMap
}

// parsePostgreSQLPrimaryKeysFromCreateStatement extracts primary key column names from a PostgreSQL CREATE TABLE statement.
// Returns a slice of primary key column names (with quotes removed).
// Handles both named constraints (CONSTRAINT name PRIMARY KEY) and unnamed PRIMARY KEY constraints.
// Example inputs:
//   - "CONSTRAINT place_pkey PRIMARY KEY (id)" -> []string{"id"}
//   - "CONSTRAINT place_contacts_pkey PRIMARY KEY (place_id, contact_id)" -> []string{"place_id", "contact_id"}
//   - "PRIMARY KEY (column1)" -> []string{"column1"}
func parsePostgreSQLPrimaryKeysFromCreateStatement(createStatement string) []string {
	// Match PRIMARY KEY constraint - handles both named and unnamed constraints
	// Pattern breakdown:
	//   - Optional: CONSTRAINT constraint_name
	//   - PRIMARY KEY
	//   - Column list in parentheses: (column1, column2, ...)
	// Columns may be quoted (double quotes) or unquoted
	pkeyRegex := regexp.MustCompile(`(?i)(?:CONSTRAINT\s+[a-zA-Z_][a-zA-Z0-9_]*\s+)?PRIMARY\s+KEY\s*\(([^)]+)\)`)

	matches := pkeyRegex.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return nil
	}

	// Extract the column list (matches[1])
	columnList := strings.TrimSpace(matches[1])
	if columnList == "" {
		return nil
	}

	// Split by comma and clean up each column name
	var pkeys []string
	for _, col := range strings.Split(columnList, ",") {
		col = strings.TrimSpace(col)
		// Remove double quotes if present (PostgreSQL uses double quotes for identifiers)
		col = strings.Trim(col, "\"")
		if col != "" {
			pkeys = append(pkeys, col)
		}
	}

	return pkeys
}

func TestPrimaryKeys(t *testing.T) {
	nameRe := regexp.MustCompile("CREATE TABLE `([a-zA-Z0-9_]+)`")
	pkeyRe := regexp.MustCompile(`PRIMARY KEY \(([^\)]+)\)`)
	columnRe := regexp.MustCompile("^\\s+`([a-zA-z_]+)`")
	for _, db := range databaseDefinitions {
		db := db // capture loop variable
		t.Run(db.Name(), func(t *testing.T) {
			schemaFile, err := os.ReadFile(db.SchemaPath())
			require.NoError(t, err, "read schema file")
			sqlStatements := strings.Split(string(schemaFile), ";")

			tableCreateStatements := make(map[string]string)
			if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				// For PostgreSQL, we need to parse the table names from the SQL statements
				tableCreateStatements = parsePostgreSQLTableNamesFromSQLStatements(sqlStatements)
			} else {
				// For MySQL, we can use the table name directly as the key
				for _, sqlStatement := range sqlStatements {
					sqlStatement = strings.TrimSpace(sqlStatement)
					matches := nameRe.FindStringSubmatch(sqlStatement)
					if len(matches) > 1 {
						tableCreateStatements[matches[1]] = sqlStatement
					}
				}
			}

			for tableName, tableDef := range db.Tables {
				tableName, tableDef := tableName, tableDef // capture loop variables
				t.Run(tableName, func(t *testing.T) {

					var pkeys []string

					if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
						if tableDef.InternalOverrides.DatabaseSchemaOverride == "" {
							tableDef.InternalOverrides.DatabaseSchemaOverride = "public"
						}
						tableNameWithSchema := tableDef.InternalOverrides.DatabaseSchemaOverride + "__" + tableName
						createStatement, ok := tableCreateStatements[tableNameWithSchema]
						require.True(t, ok, "table create statement")
						pkeys = parsePostgreSQLPrimaryKeysFromCreateStatement(createStatement)
					} else {
						createStatement, ok := tableCreateStatements[tableName]
						require.True(t, ok, "table create statement")
						// determine primary keys
						matches := pkeyRe.FindStringSubmatch(createStatement)
						if len(matches) > 1 {
							// primary key defined in create statement
							pkeys = strings.Split(matches[1], ",")
							for i, pkey := range pkeys {
								pkeys[i] = strings.Trim(strings.TrimSpace(pkey), "`")
							}
							// TODO: this is a short term fix to allow us to workaround empty string
							// columns in the primary key. Remove this once such primary key columns
							// are supported. See https://samsara-net.slack.com/archives/CUA5PTXGS/p1597778502113600.
							if db.Name() == "alertsdb" && tableName == "latency_metric_timestamps" {
								pkeys = []string{"org_id", "alert_id", "object_ids", "started_at"}
							} else if tableName == "alert_contacts" {
								// TODO: Hack to get clouddb.alert_conditions working for R&A
								// They are owning fixing the upstream table so we can properly define primary key
								pkeys = []string{"alert_id", "contact_id"}
							}
						} else {
							if db.Name() == "clouddb" && tableName == "alert_contacts" {
								// TODO: Hack to get clouddb.alert_conditions working for R&A
								// They are owning fixing the upstream table so we can properly define primary key
								pkeys = []string{"alert_id", "contact_id"}
							} else {
								// no defined primary key, treat all columns as primary key
								// this is the expected behavior for an association table
								for _, line := range strings.Split(createStatement, "\n") {
									colMatches := columnRe.FindStringSubmatch(line)
									if len(colMatches) > 1 {
										pkeys = append(pkeys, colMatches[1])
									}
								}
							}
						}
					}
					assert.Equal(t, pkeys, tableDef.primaryKeys)
				})
			}
		})
	}
}

func TestRegistryProductionFlag(t *testing.T) {
	references, err := registrytest.FindProductionTableReferences()
	require.NoError(t, err)

	tables := make(map[string]bool)
	for _, table := range Registry {
		database := GetSparkFriendlyRdsDBName(table.RdsDb, table.DBSharded, false)
		if table.DBSharded {
			firstShard := !strings.Contains(database, "_shard_")
			if firstShard {
				tables[strings.ToLower(fmt.Sprintf("%s_shards.%s", database, table.TableName))] = table.Production
			}
		} else {
			tables[strings.ToLower(fmt.Sprintf("%s.%s", database, table.TableName))] = table.Production
		}
	}

	var necessary []string
	var extra []string
	for table, production := range tables {
		_, ok := references[table]
		if ok && !production {
			necessary = append(necessary, table)
		} else if !ok && production {
			extra = append(extra, table)
		}
	}
	if len(necessary) > 0 || len(extra) > 0 {
		require.Fail(t, fmt.Sprintf("Production tables should be exactly the set of tables used by data pipelines or reports.  Should be production: %d %v\n\n, Should not be production: %d %v\n\n", len(necessary), necessary, len(extra), extra))
	}
}

func TestConversionParams(t *testing.T) {
	// The legacy uint64 conversion parameters should not be set for any new tables, and only exists
	// for backwards compatibility of existing tables.
	// This test lists all the existing tables using it; please don't add any new
	// tables to this list.

	expected := []string{
		"associations.device_associations",
		"cloud.alert_conditions",
		"cloud.dashboards",
		"cloud.organizations",
		"cloud.slave_devices",
		"cloud.slave_pins",
		"industrialcore.asset_reports",
		"industrialcore.dynamic_dashboards",
		"products.devices",
	}

	var tables []string
	for _, db := range databaseDefinitions {
		for name, tabledef := range db.Tables {
			if tabledef.InternalOverrides.LegacyUint64AsBigint {
				tables = append(tables, db.name+"."+name)
			}
		}
	}
	sort.Strings(tables)
	assert.Equal(t, expected, tables)
}

func TestRegistryAllNewRDSTabesHaveDescriptions(t *testing.T) {
	tablesWithoutDescriptions := map[string]struct{}{
		"alerts.alert_admin_recipients":                   {},
		"alerts.latency_metric_timestamps":                {},
		"api2.webhook_event_subscriptions":                {},
		"api2.webhook_workflow_subscriptions":             {},
		"attribute.attribute_cloud_entities":              {},
		"attribute.attribute_values":                      {},
		"attribute.attributes":                            {},
		"cloud.alert_devices":                             {},
		"cloud.slave_devices":                             {},
		"cloud.alert_widgets":                             {},
		"cloud.features_orgs":                             {},
		"cloud.alerts":                                    {},
		"cloud.alert_machine_inputs":                      {},
		"cloud.external_keys":                             {},
		"cloud.machines":                                  {},
		"cloud.alert_machine_input_types":                 {},
		"cloud.stat_mappings":                             {},
		"cloud.driver_rating_actions":                     {},
		"cloud.machine_input_types":                       {},
		"cloud.tag_widgets":                               {},
		"cloud.alert_industrial_assets":                   {},
		"cloud.external_values":                           {},
		"cloud.api_tokens":                                {},
		"cloud.firmware_build_infos":                      {},
		"cloud.drivers":                                   {},
		"cloud.dashboards":                                {},
		"cloud.industrial_local_variables":                {},
		"cloud.alert_webhooks":                            {},
		"cloud.alert_tags":                                {},
		"cloud.alert_subconditions":                       {},
		"cloud.slave_pins":                                {},
		"cloud.alert_contacts":                            {},
		"cloud.tag_devices":                               {},
		"cloud.alert_asset_labels":                        {},
		"cloud.id_cards":                                  {},
		"cloud.webhooks":                                  {},
		"cloud.tags":                                      {},
		"cloud.machine_machine_inputs":                    {},
		"cloud.alert_drivers":                             {},
		"cloud.contacts":                                  {},
		"cmassets.inference_results":                      {},
		"coaching.coach_assignments":                      {},
		"coaching.coachable_item":                         {},
		"coaching.coaching_sessions":                      {},
		"coaching.coaching_sessions_behavior":             {},
		"coaching.organization_settings":                  {},
		"compliance.driver_hos_vehicle_assignments":       {},
		"compliance.eld_audit_actions":                    {},
		"compliance.org_carriers":                         {},
		"compliance.auto_duty_events":                     {},
		"compliance.data_transfer_audit_logs":             {},
		"compliance.driver_carrier_entries":               {},
		"compliance.driver_hos_charts":                    {},
		"compliance.driver_hos_logs":                      {},
		"developers.developer_notification_subscribers":   {},
		"developers.developer_notification_subscriptions": {},
		"dispatch.dispatch_jobs":                          {},
		"dispatch.dispatch_routes":                        {},
		"dispatch.routing_etas":                           {},
		"driverassignments.driver_assignments":            {},
		"driverassignments.driver_assignment_metadata":    {},
		"driverdocuments.driver_document_templates":       {},
		"driverdocuments.driver_documents":                {},
		"driverdocuments.driver_document_audit_logs":      {},
		"driverdocuments.driver_document_pdfs":            {},
		"finops.rmas":                                     {},
		"finops.pending_license_assignments":              {},
		"firmware.firmware_signoff_history":               {},
		"firmware.gateway_config_versions":                {},
		"firmware.gateway_rollout_stages":                 {},
		"firmware.item_firmwares":                         {},
		"firmware.organization_firmwares":                 {},
		"firmware.product_firmwares":                      {},
		"firmware.product_group_rollout_stage_by_org":     {},
		"firmware.rollout_stage_firmwares":                {},
		"fuelcards.fleetcor_integration_configs":          {},
		"fuel.driver_efficiency_configs":                  {},
		"fuel.driver_efficiency_score_profiles":           {},
		"fuel.custom_fuel_costs":                          {},
		"fuel.ifta_detail_job_outputs":                    {},
		"fuel.ifta_detail_jobs":                           {},
		"fuel.ifta_integrations":                          {},
		"fuel.profile_speed_thresholds":                   {},
		"fuel.driver_efficiency_profiles_devices":         {},
		"geoevents.geofence_events":                       {},
		"internal.all_types":                              {},
		"locale.fleetcor_files":                           {},
		"locale.locale_fuel_costs":                        {},
		"mdm.android_device":                              {},
		"mdm.android_device_location":                     {},
		"mdm.enterprise":                                  {},
		"mdm.organization_launcher_config":                {},
		"mdm.policy":                                      {},
		"mdm.signup_url":                                  {},
		"oem.oem_integration_auth":                        {},
		"oem.oem_source_enrollment_status":                {},
		"oem.oem_sources":                                 {},
		"products.id_cards":                               {},
		"products.tag_devices":                            {},
		"products.tag_widgets":                            {},
		"recognition.dark_launch_faces":                   {},
		"recognition.faces":                               {},
		"recognition.identities":                          {},
		"reportconfig.custom_report_configs":              {},
		"reportconfig.scheduled_report_plans":             {},
		"retention.retention_config":                      {},
		"safety.safety_event_geofences":                   {},
		"safety.harsh_event_surveys":                      {},
		"safety.safety_score_settings":                    {},
		"safety.safetybackfill_watermark":                 {},
		"safety.activity_events":                          {},
		"trips2.org_trip_settings":                        {},
		"trips2.device_trip_settings":                     {},
		"trips.trip_purpose_assignments":                  {},
		"userorgpreferences.user_org_preferences":         {},
		"userpreferences.fleet_list_customizations":       {},
		"userpreferences.onboarding_progresses":           {},
		"vin.device_vin_metadata_overrides":               {},
		"vin.device_vin_metadata":                         {},
		"workforcevideo.areas_of_interest":                {},
		"workforcevideo.camera_devices":                   {},
		"workforcevideo.camera_streams":                   {},
		"workforcevideo.library_video_info":               {},
		"workforcevideo.object_detection_feedback":        {},
		"workforcevideo.scene_metadata":                   {},
		"workforcevideo.sites":                            {},
		"workforcevideo.video_segments":                   {},
		"workforcevideo.views_streams":                    {},
		"workforcevideo.sites_camera_devices":             {},
		"workforcevideo.sites_floor_plans":                {},
		"workforcevideo.views":                            {},
	}

	for _, db := range databaseDefinitions {
		for name, tableDefinition := range db.Tables {
			fullName := fmt.Sprintf("%s.%s", db.name, name)
			t.Run(fmt.Sprintf("%s rds delta lake metadata test", fullName), func(t *testing.T) {
				fullName := fmt.Sprintf("%s.%s", db.name, name)
				if _, ok := tablesWithoutDescriptions[fullName]; !ok { // If the table is not in the allow list.
					if tableDefinition.Description == "" {
						require.Fail(t, "new rds tables in delta lake registry does not have a description field populated. Please populate description field so downstream consumers know how to use the table.", "db.table_name = %v", fullName)
					}
					if tableDefinition.ColumnDescriptions == nil && len(tableDefinition.ColumnDescriptions) > 0 {
						require.Fail(t, "new rds tables in delta lake registry does not have a column description field populated. Please populate column description so downstream consumers know what each of the columns mean", "db.table_name = %v", fullName)
					}
				} else {
					if tableDefinition.Description != "" {
						require.Fail(t, fmt.Sprintf("%s rds table has metadata populated. Please remove it from the allow list in infra/dataplatform/rdsdeltalake/registry_test.go --> TestRegistryAllNewRDSTabesHaveDescriptions", fullName))
					}
				}
			})
		}
	}
}

func TestSetEmptyStringsCastToNullFlag(t *testing.T) {
	// Snapshots the existing set of tables that have `EmptyStringsCastToNull_DO_NOT_SET` set.
	// No new tables should have this set and we should never add new things to the snapshot.

	snap := snapshotter.New(t)
	defer snap.Verify()

	var tables []string
	for _, db := range databaseDefinitions {
		for tableName, tableDef := range db.Tables {
			if tableDef.EmptyStringsCastToNull_DO_NOT_SET {
				tables = append(tables, db.name+"."+tableName)
			}
		}
	}
	sort.Strings(tables)
	snap.Snapshot("tables with EmptyStringsCastToNull_DO_NOT_SET set", tables)
}

func TestSetCdcVersion_DO_NOT_SET(t *testing.T) {
	// Snapshot all tables that have a non-zero version stored in the field "cdcVersion_DO_NOT_CHANGE"
	// and ensure these versions do not change, and that no new tables introduce a non-zero CDC version.
	// Changes to this snapshot are not safe.
	snap := snapshotter.New(t)
	defer snap.Verify()

	var tableVersions []string
	for _, db := range databaseDefinitions {
		for tableName, tableDef := range db.Tables {
			if tableDef.cdcVersion_DO_NOT_SET != TableVersion(0) {
				tableVersions = append(tableVersions, fmt.Sprintf("%s.%s_%s", db.name, tableName, tableDef.cdcVersion_DO_NOT_SET.Name()))
			}
		}
	}
	sort.Strings(tableVersions)
	snap.Snapshot("Non-Zero CDC Table Versions", tableVersions)
}

func TestNestedProtoNames(t *testing.T) {
	// These special proto types may contain many irrelevant fields to the table
	// we are replicating to the data lake.
	specialProtos := []interface{}{
		&hubproto.ObjectStatBinaryMessage{},
	}

	hasSpecialProtoType := func(protoType reflect.Type) bool {
		for _, proto := range specialProtos {
			specialProtoType := reflect.TypeOf(proto)
			if protoType == specialProtoType {
				return true
			}
		}
		return false
	}

	for _, dbDefinition := range databaseDefinitions {
		for name, table := range dbDefinition.Tables {
			for protoCol, protoMetadata := range table.ProtoSchema {
				protoType := reflect.TypeOf(protoMetadata.Message)
				if hasSpecialProtoType(protoType) {
					assert.NotNil(t, protoMetadata.NestedProtoNames,
						fmt.Sprintf("%v.%v is of %v, which requires at least one field in ProtobufSchema.NestedProtoNames.",
							name,
							protoCol,
							protoType,
						))
				} else {
					assert.Nil(t, protoMetadata.NestedProtoNames,
						fmt.Sprintf("%v.%v is of %v, which should have a nil ProtobufSchema.NestedProtoNames.",
							name,
							protoCol,
							protoType,
						))
				}
			}
		}
	}
}

func TestDbRegion(t *testing.T) {

	// Figure out which dbs are region-locked.
	// We assume that dbs are either fully in 1 region or not, and so check the region override on the first shard of the db.
	// The types suggest a db could be present for some shards and not for others, but we ignore this case for now as it is
	// not true in practice.
	dbsWithRegionLock := make(map[string][]string)
	for _, db := range dbregistry.AllShardedDbs {
		for _, shards := range db.CloudAwareShards {
			for _, spec := range shards {
				// TODO: just pass in DeploymentClouds as is, when the RDS registry is updated to use clouds instead of regions.
				var deploymentRegions []string
				for _, cloud := range spec.DeploymentClouds {
					deploymentRegions = append(deploymentRegions, cloud.AWSRegion)
				}
				dbsWithRegionLock[db.Type] = deploymentRegions
				break
			}
		}
	}

	for _, db := range AllDatabases() {
		name := db.RegistryName
		if regionLock, ok := dbsWithRegionLock[name]; ok {
			if len(regionLock) != 0 {
				for _, region := range []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSDefaultRegion} {
					if db.IsInRegion(region) && !registry.ShouldDeployToRegion(regionLock, region) {
						assert.Fail(t, fmt.Sprintf("%s db should not be in region %s, due to being locked to region %s\n", name, region, regionLock))
					}
				}
			}
		} else {
			// Ignore clouddb as its named differently in the registry.
			if db.RegistryName != "cloud" {
				assert.Fail(t, fmt.Sprintf("%s is in the rds registry but doesn't exist in the db registry", name))
			}
		}
	}
}

func TestShardMonitors(t *testing.T) {
	var errors []string

	for _, db := range AllDatabases() {
		if db.HasProductionTable() {
			// Make sure we have the right number of entries
			// TODO: Add CA monitor when we have production tables in CA.
			if len(db.CdcThroughputMonitors) != 2 {
				errors = append(errors, fmt.Sprintf("%s should have exactly 2 monitors, for US and EU.", db.Name))
			} else {
				// If we do have the right number of entries,
				// make sure they're valid and that the timeframes are set correctly.
				for region, monitors := range db.CdcThroughputMonitors {
					if region != infraconsts.SamsaraAWSDefaultRegion && region != infraconsts.SamsaraAWSEURegion && region != infraconsts.SamsaraAWSCARegion {
						errors = append(errors, fmt.Sprintf("%s has monitor for region %s, which is not valid. Please choose us-west-2 or eu-west-1 or ca-central-1.", db.Name, region))
					}
					for shard, monitor := range monitors {
						if monitor.TimeframeMinutes < 120 || monitor.TimeframeMinutes > 7*24*60 {
							errors = append(errors, fmt.Sprintf("%s has monitor in region %s for shard %d that has timeframe of %d. Please ensure it is between 120 and 10080 minutes.", db.Name, region, shard, monitor.TimeframeMinutes))
						}
					}
				}
			}
		}
	}

	if len(errors) > 0 {
		t.Errorf("%v\n", errors)
	}
}

func TestSortShards(t *testing.T) {
	shards := []string{
		"prod-compliance-shard-1db",
		"prod-compliancedb",
		"prod-compliance-shard-3db",
		"prod-compliance-shard-2db",
		"prod-compliance-shard-5db",
		"prod-compliance-shard-4db",
	}
	SortShards(shards)
	assert.Equal(t, []string{
		"prod-compliancedb",
		"prod-compliance-shard-1db",
		"prod-compliance-shard-2db",
		"prod-compliance-shard-3db",
		"prod-compliance-shard-4db",
		"prod-compliance-shard-5db",
	}, shards)
}

func TestShardedSparkDbNameToProdDbName(t *testing.T) {
	prodMainShardDbName, err := ShardedSparkDbNameToProdDbName("associations_shard_0db")
	assert.NoError(t, err)
	assert.Equal(t, "prod-associationsdb", prodMainShardDbName)

	prodShard2DbName, err := ShardedSparkDbNameToProdDbName("associations_shard_2db")
	assert.NoError(t, err)
	assert.Equal(t, "prod-associations-shard-2db", prodShard2DbName)
}

func TestSparkDbNameToRdsDbRegistryName(t *testing.T) {
	// Individual shards.
	assert.Equal(t, dbregistry.AssociationsDB, SparkDbNameToRdsDbRegistryName("associations_shard_0db"))

	// Combine shards view.
	assert.Equal(t, dbregistry.AssociationsDB, SparkDbNameToRdsDbRegistryName("associationsdb_shards"))

	// Unsharded.
	assert.Equal(t, dbregistry.AssociationsDB, SparkDbNameToRdsDbRegistryName("associationsdb"))
}

func TestMessagesDBCanReadGroups(t *testing.T) {
	// Find the MessagesDB definition in databaseDefinitions
	var messagesDB *databaseDefinition
	for i := range databaseDefinitions {
		if databaseDefinitions[i].name == dbregistry.MessagesDB {
			messagesDB = &databaseDefinitions[i]
			break
		}
	}

	assert.NotNil(t, messagesDB, "MessagesDB not found in databaseDefinitions")

	// Check that CanReadGroups contains the expected teams
	expectedTeams := []components.TeamInfo{
		team.DataPlatform,
		team.MLScience,
	}

	assert.ElementsMatch(t, expectedTeams, messagesDB.CanReadGroups,
		"MessagesDB CanReadGroups does not contain the expected teams")
}

func TestNoRedundantTableServerlessConfig(t *testing.T) {
	// This test ensures that tables don't set ServerlessConfig when it's identical
	// to the database-level ServerlessConfig. The database-level config automatically
	// applies to all tables, so table-level configs should only be used to override
	// with different values.

	var redundantTables []string

	for _, db := range databaseDefinitions {
		// Skip databases without a ServerlessConfig
		if db.InternalOverrides.ServerlessConfig == nil {
			continue
		}

		dbConfig := db.InternalOverrides.ServerlessConfig

		for tableName, tableDef := range db.Tables {
			// Skip tables without ServerlessConfig
			if tableDef.ServerlessConfig == nil {
				continue
			}

			tableConfig := tableDef.ServerlessConfig

			// Check if the table config is identical to the database config
			// Use DeepEqual to compare all fields (PerformanceTarget, Environments, EnvironmentKey)
			if reflect.DeepEqual(dbConfig, tableConfig) {
				fullTableName := fmt.Sprintf("%s.%s", db.name, tableName)
				redundantTables = append(redundantTables, fullTableName)
			}
		}
	}

	if len(redundantTables) > 0 {
		sort.Strings(redundantTables)
		errorMsg := fmt.Sprintf(
			"\n\nFound %d table(s) with redundant ServerlessConfig that matches their database-level config.\n"+
				"These tables inherit the database ServerlessConfig automatically, so the table-level config is unnecessary.\n"+
				"Table-level ServerlessConfig should only be used to OVERRIDE the database default with a DIFFERENT value.\n\n"+
				"Please remove the ServerlessConfig field from these tables:\n\n%s\n\n"+
				"The fallback logic is in registry.go lines 8775-8784:\n"+
				"  // If the table doesn't have a serverless config, use the database level config.\n"+
				"  if t.ServerlessConfig == nil {\n"+
				"      t.ServerlessConfig = db.InternalOverrides.ServerlessConfig\n"+
				"  }\n",
			len(redundantTables),
			strings.Join(redundantTables, "\n"),
		)
		require.Fail(t, errorMsg)
	}
}
