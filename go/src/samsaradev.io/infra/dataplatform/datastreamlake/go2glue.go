package datastreamlake

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
	"samsaradev.io/infra/dataplatform/datastreamlake/datastreamtime"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

// GoStructToGlueSchema converts a Go Struct to a Glue Catalog schema
func GoStructToGlueSchema(goStruct interface{}) ([]awsresource.GlueCatalogTableStorageDescriptorColumn, error) {
	structVal := reflect.ValueOf(goStruct)
	fields := make([]awsresource.GlueCatalogTableStorageDescriptorColumn, structVal.NumField())
	colNames := map[string]struct{}{}
	for i := 0; i < structVal.NumField(); i++ {
		if structVal.Field(i).Type() == reflect.TypeOf(time.Time{}) {
			return nil, oops.Errorf("time.Time type found in Record struct. Please use datastreamtime.Time when declaring timestamp types. This is needed for proper timestamp serialization by Firehose.")
		}

		columnName := structVal.Type().Field(i).Tag.Get("json")
		if columnName == "" {
			return nil, oops.Errorf("Missing JSON tag for %s. All fields MUST have JSON tag set", structVal.Type().Field(i).Name)
		}

		// Structs have the option to be annotated with a `datalakemetadata` tag to describe the field.
		// This description populates the `Comment` field of a Glue Catalog Table definition which in turn shows up in the Databricks schema.
		comment := structVal.Type().Field(i).Tag.Get("datalakemetadata")

		// Replace default description identifiers with the corresponding description
		if description, ok := metadatahelpers.DefaultDescriptionIdentifiers[comment]; ok {
			comment = description
		}

		if strings.ToLower(columnName) == "date" {
			return nil, oops.Errorf("Invalid JSON tag date. Date can not be a JSON tag because it will map to a data lake column name and Firehose partitions the data by date so you will get date for free.")
		}

		if strings.Contains(columnName, ",") {
			return nil, oops.Errorf("Invalid JSON tag for %s. All fields MUST have non-empty JSON tags", structVal.Type().Field(i).Name)
		}

		if _, ok := colNames[columnName]; ok {
			return nil, oops.Errorf("JSON tag %s appears multiple times which will cause collisions in the data lake table column names. Please use unique JSON tags on all fields.", columnName)
		}
		colNames[columnName] = struct{}{}

		// Byte Arrays need to be specifically handled differant than other slice types.
		// JsonTypeToSparkType will convert []byte (alias of []uint8) to a BINARY type which is correct in a Parquet/Spark context.
		// However, BINARY is not a valid type for JSON so during Firehose deserialization an error is thrown:
		// 'DataFormatConversion.InvalidSchema: The schema is invalid. One or more columns have types that are not supported by org.openx.data.JsonSerDe'
		// In addition, Golangs encoding/json package states:
		// 'Array and slice values encode as JSON arrays, except that []byte encodes as a base64-encoded string, and a nil slice encodes as the null JSON value.'
		// Since it ends up getting encoded as a string we force the Glue Catalog Column Type for []byte field to be string and then inside of
		// the datalake we can utilize SparkSQL unbase64 to unpack the string.
		if structVal.Field(i).Kind() == reflect.Slice && structVal.Field(i).Type().Elem().Kind() == reflect.Uint8 {
			fields[i] = awsresource.GlueCatalogTableStorageDescriptorColumn{
				Name:    columnName,
				Type:    awsresource.GlueTableColumnTypeString,
				Comment: fmt.Sprintf("%s %s is a base64 encoded string of bytes. Unpack using SparkSQL function unbase64(), i.e. STRING(unbase64(%s))", comment, columnName, columnName),
			}
			continue
		}

		if structVal.Field(i).Kind() == reflect.Slice || structVal.Field(i).Kind() == reflect.Array || structVal.Field(i).Kind() == reflect.Map {
			complexType, err := sparktypes.JsonTypeToSparkType(structVal.Field(i).Type(), sparktypes.ConversionParams{})
			if err != nil {
				return nil, oops.Wrapf(err, "Error converting complex type to parquet type")
			}

			// Firehose does not support spaces, newlines, and must be lowercase in Glue Schema during record format conversion
			complextTypeString := strings.Replace(complexType.String(), " ", "", -1)
			complextTypeString = strings.Replace(complextTypeString, "\n", "", -1)
			complextTypeString = strings.ToLower(complextTypeString)
			fields[i] = awsresource.GlueCatalogTableStorageDescriptorColumn{
				Name:    columnName,
				Type:    awsresource.GlueTableColumnType(complextTypeString),
				Comment: comment,
			}
			continue
		}

		// Pointers are allowed to any of the standard glue types. Unwrap the pointer if found.
		goType := structVal.Field(i).Type()
		if goType.Kind() == reflect.Ptr {
			goType = goType.Elem()
		}

		glueType, ok := goPrimitiveTypeToGlueMapping[goType.Kind()]
		if !ok {
			glueType, ok = goTypeToGlueMapping[goType]
			if !ok {
				return nil, oops.Errorf("No Glue Type mapping for Go Type: %s", structVal.Field(i).Type().Name())
			}
		}

		fields[i] = awsresource.GlueCatalogTableStorageDescriptorColumn{
			Name:    columnName,
			Type:    glueType,
			Comment: comment,
		}
	}

	return fields, nil
}

var goPrimitiveTypeToGlueMapping = map[reflect.Kind]awsresource.GlueTableColumnType{
	reflect.String:  awsresource.GlueTableColumnTypeString,
	reflect.Bool:    awsresource.GlueTableColumnTypeBoolean,
	reflect.Int:     awsresource.GlueTableColumnTypeInteger,
	reflect.Int32:   awsresource.GlueTableColumnTypeBigInt,
	reflect.Int64:   awsresource.GlueTableColumnTypeBigInt,
	reflect.Float32: awsresource.GlueTableColumnTypeFloat,
	reflect.Float64: awsresource.GlueTableColumnTypeFloat,
}

var goTypeToGlueMapping = map[reflect.Type]awsresource.GlueTableColumnType{
	reflect.TypeOf(datastreamtime.Time{}): awsresource.GlueTableColumnTypeTimestamp,
}
