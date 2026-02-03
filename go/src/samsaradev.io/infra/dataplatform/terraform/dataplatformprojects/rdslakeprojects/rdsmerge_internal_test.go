package rdslakeprojects

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
)

func TestProtoColMapJsonString(t *testing.T) {
	makeExpectedProtoColumnJson := func(protoColName string, protoDescKey string, protoMsgName string, nestedProtoNames string, enablePermissiveMode string) string {
		// Expect the following map format: { "columnName": { "descKey": "descKey.desc", "enablePermissiveMode": "True/False", "message": "ProtoMessage", "nestedProtoNames": "NestedProtoMessage"}}
		return fmt.Sprintf("\\\"%s\\\":{\\\"descKey\\\":\\\"%s\\\",\\\"enablePermissiveMode\\\":\\\"%s\\\",\\\"message\\\":\\\"%s\\\",\\\"nestedProtoNames\\\":\\\"%s\\\"}",
			protoColName, protoDescKey, enablePermissiveMode, protoMsgName, nestedProtoNames)
	}

	testCases := []struct {
		description string
		rdsDbName   string
		tableName   string
		region      string
		// Escape the backslashes and quotes to produce the expected the string.
		expectedJsonString string
	}{
		{
			description:        "table with single proto column",
			rdsDbName:          dbregistry.WorkflowsDB,
			tableName:          "workflow_incidents",
			region:             infraconsts.SamsaraAWSEURegion,
			expectedJsonString: fmt.Sprintf("{%s}", makeExpectedProtoColumnJson("proto", "databricks-dev-eu/proto_descriptors/platform/workflows/workflowsproto/workflows_server.proto.desc", "IncidentProto", "", "false")),
		},
		{
			description:        "table with five proto columns with same proto file",
			rdsDbName:          dbregistry.FuelCardsDB,
			tableName:          "fuel_logs",
			region:             infraconsts.SamsaraAWSDefaultRegion,
			expectedJsonString: fmt.Sprintf("{%s,%s,%s,%s,%s}", makeExpectedProtoColumnJson("error_blob", "databricks-dev-us/proto_descriptors/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto.desc", "ErrorContent", "", "false"), makeExpectedProtoColumnJson("nearby_fueling_stations", "databricks-dev-us/proto_descriptors/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto.desc", "NearbyFuelingStations", "", "false"), makeExpectedProtoColumnJson("original_data", "databricks-dev-us/proto_descriptors/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto.desc", "RawLog", "", "true"), makeExpectedProtoColumnJson("unverified_details", "databricks-dev-us/proto_descriptors/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto.desc", "UnverifiedDetails", "", "false"), makeExpectedProtoColumnJson("verification_context", "databricks-dev-us/proto_descriptors/fleet/fuel/fuelcards/fuelcardsproto/fuelcards.proto.desc", "VerificationContext", "", "false")),
		},
		{
			description:        "table with two proto columns with different proto file",
			rdsDbName:          dbregistry.IndustrialCoreDB,
			tableName:          "dynamic_dashboards",
			region:             infraconsts.SamsaraAWSEURegion,
			expectedJsonString: fmt.Sprintf("{%s,%s}", makeExpectedProtoColumnJson("config_proto", "databricks-dev-eu/proto_descriptors/hubproto/industrialhubproto/dashboard.proto.desc", "Dashboard", "", "false"), makeExpectedProtoColumnJson("graphical_config_proto", "databricks-dev-eu/proto_descriptors/hubproto/industrialhubproto/graphical_dashboard.proto.desc", "GraphicalDashboard", "", "false")),
		},
		{
			description:        "table with permissive_mode enabled for a proto column",
			rdsDbName:          dbregistry.ComplianceDB,
			tableName:          "eld_audit_actions",
			region:             infraconsts.SamsaraAWSDefaultRegion,
			expectedJsonString: fmt.Sprintf("{%s}", makeExpectedProtoColumnJson("proto", "databricks-dev-us/proto_descriptors/fleet/compliance/complianceproto/compliance_service.proto.desc", "EldAuditActionProto", "", "true")),
		},
		{
			description:        "table with no proto columns",
			rdsDbName:          dbregistry.FuelCardsDB,
			tableName:          "fuel_log_vehicles",
			region:             infraconsts.SamsaraAWSDefaultRegion,
			expectedJsonString: "{}",
		},
		{
			description:        "table with nested field in single proto column",
			rdsDbName:          dbregistry.CmAssetsDB,
			tableName:          "camera_upload_requests",
			region:             infraconsts.SamsaraAWSDefaultRegion,
			expectedJsonString: fmt.Sprintf("{%s}", makeExpectedProtoColumnJson("request_proto", "databricks-dev-us/proto_descriptors/hubproto/object_stat.proto.desc", "ObjectStatBinaryMessage", "dashcam_report", "false")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			db, err := rdsdeltalake.GetDatabaseByName(tc.rdsDbName)
			require.NoError(t, err)

			var table rdsdeltalake.RegistryTable
			for _, t := range db.TablesInRegion(tc.region) {
				if t.TableName == tc.tableName {
					table = t
					break
				}
			}
			jsonString, err := protoColMapJsonString(tc.region, table)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedJsonString, jsonString)
		})
	}
}
