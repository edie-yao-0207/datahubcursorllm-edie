package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/sparktypes"
	"samsaradev.io/infra/dataplatform/sqlschemaparser"
)

var (
	rdsDeltaLakePathPrefix = filepath.Join(
		filepathhelpers.BackendRoot,
		"go/src/samsaradev.io/infra/dataplatform/rdsdeltalake",
	)
	v2SchemasDirPath = filepath.Join(rdsDeltaLakePathPrefix, "v2schemas")
)

func main() {
	databaseFlag := flag.String("database", "", "Database to generate schemas for")
	flag.Parse()
	databaseToGenerateSchemasFor := *databaseFlag
	if databaseToGenerateSchemasFor != "" {
		log.Printf("Generating schemas for database %s\n", databaseToGenerateSchemasFor)
	} else {
		log.Printf("Generating schemas for all databases\n")
	}

	dbSchemaMap := make(map[string]*sqlschemaparser.Db)
	for _, dbDefinition := range rdsdeltalake.AllDatabases() {
		for tableName, tableDefinition := range dbDefinition.Tables {
			dbName := dbDefinition.Name

			log.Printf("Processing %s.%s\n", dbName, tableName)

			dbSchema, ok := dbSchemaMap[dbName]
			if !ok {
				var err error
				if dbDefinition.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
					dbSchema, err = sqlschemaparser.ParseDBSchemaFile(dbDefinition.SchemaPath, sqlschemaparser.SchemaTypePostgres)
				} else {
					dbSchema, err = sqlschemaparser.ParseDBSchemaFile(dbDefinition.SchemaPath)
				}
				if err != nil {
					log.Panicln(oops.Wrapf(err, "Unable to parse db_schema file for %sdb", dbName))
				}
				dbSchemaMap[dbName] = dbSchema
			}

			table, ok := dbSchema.Tables[tableName]
			if !ok {
				log.Panicln(oops.Errorf("Invalid tableName '%s'. Does not exist in database '%s'", tableName, dbName))
			}
			table.ConversionParams = sparktypes.ConversionParams{
				PreserveProtobufOriginalNames: true,
				LegacyUint64AsBigint:          tableDefinition.InternalOverrides.LegacyUint64AsBigint,
				JsonpbInt64AsDecimalString:    tableDefinition.InternalOverrides.JsonpbInt64AsDecimalString,
			}
			table.ExcludeColumns = tableDefinition.ExcludeColumns

			// Validate user defined partition strategy will lead to successful merge
			if tableDefinition.PartitionStrategy != rdsdeltalake.SinglePartition {
				if err := validatePartitionStrategy(tableName, table, tableDefinition.PartitionStrategy); err != nil {
					log.Panicln(oops.Wrapf(err, "Invalid partition strategy for table %s", tableName))
				}
			}

			if databaseToGenerateSchemasFor == "" || strings.HasPrefix(dbName, databaseToGenerateSchemasFor) {
				log.Printf("Generating schemas for %s.%s\n", dbName, tableName)
				if err := generateAndWriteSchemas(dbName, tableName, tableDefinition.ProtoSchema, table); err != nil {
					log.Fatalln(err)
				}
			} else {
				log.Printf("Skipping schema generation for %s.%s\n", dbName, tableName)
			}
		}
	}

	// Build a list of all the tables we have, and then remove any SQL files that don't match any of our existing tables.
	allTableFileNames := make(map[string]struct{})
	for dbName, database := range dbSchemaMap {
		for tableName := range database.Tables {
			allTableFileNames[fmt.Sprintf("%s.%s.sql", dbName, tableName)] = struct{}{}
		}
	}

	// Include appconfigs.org_shards because it is a manually created view
	allTableFileNames["appconfigs.org_shards.sql"] = struct{}{}

	for _, directory := range []string{v2SchemasDirPath} {
		err := deleteExtraSqlFiles(directory, allTableFileNames)
		if err != nil {
			log.Fatalf("Unable to delete extra sql files, got error %v\n", err)
		}
	}

}

func deleteExtraSqlFiles(directory string, tables map[string]struct{}) error {
	files, err := filepath.Glob(filepath.Join(directory, "*"))
	if err != nil {
		log.Fatalf("couldn't list files in schemadir %s, got err %v\n", directory, err)
	}
	for _, file := range files {
		base := filepath.Base(file)
		if _, ok := tables[base]; !ok && strings.HasSuffix(base, "sql") {
			fmt.Printf("Removing %s\n", file)
			err = os.Remove(file)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func isColumnExcludedFromReplication(columnName string, columnsToExclude []string) bool {
	for _, colName := range columnsToExclude {
		if columnName == colName {
			return true
		}
	}
	return false
}

func generateAndWriteSchemas(dbName, tableName string, protoBufColumns rdsdeltalake.ProtobufSchema, table *sqlschemaparser.Table) error {
	dbTableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	var v2Fields []*sparktypes.Field

	// Get the table from the registry.
	var foundTable *rdsdeltalake.RegistryTable
	var foundDatabase *rdsdeltalake.RegistryDatabase
	for _, db := range rdsdeltalake.AllDatabases() {
		if db.Name != dbName {
			continue
		}
		foundDatabase = &db
		for _, searchTable := range db.Tables {
			searchTableCopy := searchTable
			if searchTableCopy.TableName == tableName {
				foundTable = &searchTableCopy
				break
			}
		}
	}

	if foundTable == nil {
		return oops.Errorf("couldn't find table %s in the registry", dbTableKey)
	}

	// Validate the existence of proto columns in the table schema.
	for protoColumn := range protoBufColumns {
		exists := false
		for _, rdsColumn := range table.Columns {
			if rdsColumn.ColName == protoColumn {
				exists = true
				break
			}
		}
		if !exists {
			return oops.Errorf("Protobuf column %s defined in registry does not exist in %s.%s SQL schema. Please verify the proto columns are defined correctly", protoColumn, dbName, tableName)
		}
	}

	// Convert each column type.
	for _, col := range table.Columns {
		if isColumnExcludedFromReplication(col.ColName, table.ExcludeColumns) {
			continue
		}
		if protoDtl, ok := protoBufColumns[col.ColName]; ok {
			proto := protoDtl.Message

			// In our v2 replication path, we parse protobufs using spark-protobuf which has different
			// resulting types. In addition, we maintain a raw version of the proto as well.
			v2Type, err := sparktypes.ProtobufTypeToSparkType(reflect.TypeOf(proto), table.ConversionParams, protoDtl.ExcludeProtoJsonTag)
			if err != nil {
				return oops.Wrapf(err, "Error converting %s.%s protobuf type to Spark type", dbTableKey, col.ColName)
			}

			// Remove any fields we should. This is when a proto field has too many fields
			// and we need to select some of them.
			fieldsToKeep := make(map[string]struct{})
			for _, field := range protoDtl.NestedProtoNames {
				fieldsToKeep[field] = struct{}{}
			}

			if len(fieldsToKeep) > 0 {
				// We have to cache all fields up-front because removing fields will modify the `Fields` array
				// on the type, so we can't use that as something to iterate over.
				var allFields []string
				for _, field := range v2Type.Fields {
					allFields = append(allFields, field.Name)
				}

				for _, field := range allFields {
					if _, ok := fieldsToKeep[field]; !ok {
						err := v2Type.RemoveFieldAtPath(field)
						if err != nil {
							return oops.Wrapf(err, "failed")
						}
					}
				}
			}

			v2Fields = append(v2Fields, sparktypes.StructField(col.ColName, v2Type))
			v2Fields = append(v2Fields, sparktypes.StructField("_raw_"+col.ColName, sparktypes.StringType()))

		} else {
			var v2Typ *sparktypes.Type
			var err error
			if foundDatabase.InternalOverrides.AuroraDatabaseEngine.IsPostgres() {
				v2Typ, err = sparktypes.GetSparkTypeFromPostgreSQLTypeV2(col.ColType)
				if err != nil {
					return oops.Wrapf(err, "Error getting spark type from postgres type for %s", col.ColType)
				}
			} else {
				v2Typ, err = sparktypes.GetSparkTypeFromMySQLTypeV2(col.ColType)
				if err != nil {
					return oops.Wrapf(err, "Error getting spark type from mysql type for %s", col.ColType)
				}
			}
			v2Fields = append(v2Fields, sparktypes.StructField(col.ColName, v2Typ))
		}
	}

	v2Fields = append([]*sparktypes.Field{
		sparktypes.StructField("_timestamp", sparktypes.TimestampType()),
		sparktypes.StructField("_filename", sparktypes.StringType()),
		sparktypes.StructField("_rowid", sparktypes.StringType()),
		sparktypes.StructField("_op", sparktypes.StringType()),
	},
		v2Fields...,
	)

	if foundTable.PartitionStrategy.Key == "date" {
		v2Fields = append(v2Fields, sparktypes.StructField("date", sparktypes.StringType()))
	} else {
		v2Fields = append(v2Fields, sparktypes.StructField("partition", sparktypes.StringType()))
	}

	v2Type := sparktypes.StructType(v2Fields...)
	if err := v2Type.SaveToFile(filepath.Join(v2SchemasDirPath, fmt.Sprintf("%s.sql", dbTableKey))); err != nil {
		return oops.Wrapf(err, "")
	}
	return nil
}

// validatePartitionStrategy validates the developer defined partition strategy is valid
// i.e DateColumnFromTimestamp references a MySQL datetime or timestamp column
func validatePartitionStrategy(tableName string, t *sqlschemaparser.Table, partitionStrategy rdsdeltalake.PartitionStrategy) error {
	colType := ""
	for _, col := range t.Columns {
		if col.ColName == partitionStrategy.SourceColumnName {
			colType = col.ColType
			break
		}
	}

	if colType == "" {
		return oops.Errorf("Partition column defined for table %s (%s) does not exist in table schema", tableName, partitionStrategy.SourceColumnName)
	}

	switch partitionStrategy.SourceColumnType {
	case rdsdeltalake.MillisecondsPartitionColumnType:
		if !strings.Contains(colType, "bigint") {
			return oops.Errorf("Partition column for table %s is not the correct type in MySQL and will break rds merging. \nUsing DateColumnFromMilliseconds() function on a column that is %s. \nThis should be a bigint MySQL column.", tableName, colType)
		}
	case rdsdeltalake.TimestampPartitionColumnType:
		if !strings.Contains(colType, "datetime") && !strings.Contains(colType, "timestamp") {
			return oops.Errorf("Partition column for table %s is not the correct type in MySQL and will break rds merging. \nUsing DateColumnFromTimestamp() function on a column that is %s. \nThis should be a datetime or timestamp MySQL column.", tableName, colType)
		}
	case rdsdeltalake.DatePartitionColumnType:
		if colType != "date" {
			return oops.Errorf("Partition column for table %s is not the correct type in MySQL and will break rds merging. \nUsing a column that is %s. \nThis should be a date MySQL column.", tableName, colType)
		}
	default:
		return oops.Errorf("Invalid partition strategy source column type")
	}

	return nil
}
