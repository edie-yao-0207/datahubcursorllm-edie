package rdsdeltalake

import (
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/samsarahq/go/oops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/sqlschemaparser"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// This translates strings from camel to lower case
// eg deviceId --> device_id
func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// getColumnNamesInProtobuf gets the column names in a protobuf, recursively
// looking through structs.
//
// For instance the structs below would output ["id", "location.longitude", "location.latitude"]
//
//	type Device struct {
//		id       int64
//		location *Location
//	}
//
//	type Location struct {
//		longitude int64
//		latitude   int64
//	}
func getColumnNamesInProtobuf(t reflect.Type, depth int) []string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	names := make([]string, 0)
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i).Type
			if field.Kind() == reflect.Slice || field.Kind() == reflect.Array {
				field = field.Elem()
			}
			if field.Kind() == reflect.Ptr {
				field = field.Elem()
			}
			if field.Kind() == reflect.Struct && depth < 10 {
				// This doesnt show up in amundsen either, so its safe to ignore. Special casing it also prevents
				// infinit recursiion since this object references itself.
				if t.Name() == "ManualEntryEnumOption" {
					continue
				}
				names = append(names, fmt.Sprintf("%s", toSnakeCase(t.Field(i).Name)))

				for _, name := range getColumnNamesInProtobuf(field, depth+1) {
					names = append(names, fmt.Sprintf("%s.%s", toSnakeCase(t.Field(i).Name), name))
				}
			} else {
				names = append(names, toSnakeCase(t.Field(i).Name))
			}
		}
	}
	return names
}

func TestColumnNamesInProtobuf(t *testing.T) {
	type NestedStruct struct {
		NestedId int64
	}
	type VideoRequestCameraInfo_CameraRequest struct {
		CameraId            int64
		TrackId             int64
		NestedStructPointer *NestedStruct
		NestedStruct        NestedStruct
	}
	type VideoRequestCameraInfo struct {
		Id                string
		RequestStruct     *VideoRequestCameraInfo_CameraRequest
		Requests          []*VideoRequestCameraInfo_CameraRequest
		NoPointerRequests []VideoRequestCameraInfo_CameraRequest
	}

	results := getColumnNamesInProtobuf(reflect.TypeOf(&VideoRequestCameraInfo{}), 0)
	sort.Strings(results)
	assert.Equal(t, []string{"id", "no_pointer_requests", "no_pointer_requests.camera_id", "no_pointer_requests.nested_struct", "no_pointer_requests.nested_struct.nested_id", "no_pointer_requests.nested_struct_pointer", "no_pointer_requests.nested_struct_pointer.nested_id", "no_pointer_requests.track_id", "request_struct", "request_struct.camera_id", "request_struct.nested_struct", "request_struct.nested_struct.nested_id", "request_struct.nested_struct_pointer", "request_struct.nested_struct_pointer.nested_id", "request_struct.track_id", "requests", "requests.camera_id", "requests.nested_struct", "requests.nested_struct.nested_id", "requests.nested_struct_pointer", "requests.nested_struct_pointer.nested_id", "requests.track_id"}, results)
}

// TestColumnDescriptions tests the column descriptions exist for real columns on the rds table
func TestColumnDescriptions(t *testing.T) {
	dbSchemaMap := make(map[string]*sqlschemaparser.Db)
	for _, db := range databaseDefinitions {
		for tableName, tableDefinition := range db.Tables {
			dbSchema, ok := dbSchemaMap[db.Name()]
			if !ok {
				var err error
				if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
					dbSchema, err = sqlschemaparser.ParsePostgreSQLSchemaFile(db.SchemaPath())
				} else {
					dbSchema, err = sqlschemaparser.ParseDBSchemaFile(db.SchemaPath())
				}
				if err != nil {
					log.Panicln(oops.Wrapf(err, "Unable to parse db_schema file for %sdb", db.Name()))
				}
				dbSchemaMap[db.Name()] = dbSchema
			}

			// Check if database is PostgreSQL, if so, we need to add the schema name to the table name
			if db.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				if tableDefinition.InternalOverrides.DatabaseSchemaOverride == "" {
					tableDefinition.InternalOverrides.DatabaseSchemaOverride = "public"
				}
				tableName = tableDefinition.InternalOverrides.DatabaseSchemaOverride + "__" + tableName
			}

			table, ok := dbSchema.Tables[tableName]
			if !ok {
				log.Panicln(oops.Errorf("Invalid tableName '%s'. Does not exist in database '%s'", tableName, db.Name()))
			}

			// Get all the column names in a list
			allColumnNames := make([]string, 0, 0)
			for _, col := range table.Columns {
				if _, ok := tableDefinition.ProtoSchema[col.ColName]; ok {
					protoObject := tableDefinition.ProtoSchema[col.ColName].Message
					names := getColumnNamesInProtobuf(reflect.TypeOf(protoObject), 0)
					allColumnNames = append(allColumnNames, fmt.Sprintf("%s", col.ColName))
					for _, name := range names {
						allColumnNames = append(allColumnNames, fmt.Sprintf("%s.%s", col.ColName, name))
					}

				} else {
					allColumnNames = append(allColumnNames, col.ColName)
				}
			}

			// Check for every column description that the column exists
			columnsNotExpected := make([]string, 0, len(tableDefinition.ColumnDescriptions))
			for colNameWithDescription := range tableDefinition.ColumnDescriptions {
				containsColumn := false
				for _, columnName := range allColumnNames {
					if columnName == colNameWithDescription {
						containsColumn = true
					}
				}
				if containsColumn == false {
					columnsNotExpected = append(columnsNotExpected, colNameWithDescription)
				}
			}
			if len(columnsNotExpected) > 0 {
				require.Fail(t, fmt.Sprintf("RDS Table description for db: %s table: %s does not contain the column: %v (columnNames that exist for reference: %v)", db.Name(), tableName, columnsNotExpected, allColumnNames))

			}
		}
	}
}
