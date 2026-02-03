package datastreamlake

import (
	"fmt"
	"path"
	"reflect"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/sparktypes"
)

func GenerateDataStreamLakeSchemaFiles() error {
	dataStreamLakeSchemaDirPath := path.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io/infra/dataplatform/datastreamlake/schemas")
	for _, entry := range Registry {
		sparkType, err := sparktypes.JsonTypeToSparkType(reflect.TypeOf(entry.Record), sparktypes.ConversionParams{})
		if err != nil {
			return oops.Wrapf(err, "Unable to get Spark Type from Go Type for registry entry %s", entry.StreamName)
		}

		// Add 'date' field into the schema for all data streams. The date field is added implictly by Glue during partitioning so the
		// Go Struct does not explicitly containa 'date' field.
		sparkType.Fields = append(sparkType.Fields, &sparktypes.Field{
			Name: "date",
			Type: sparktypes.DateType(),
		})

		err = sparkType.SaveToFile(path.Join(dataStreamLakeSchemaDirPath, fmt.Sprintf("datastreams.%s.sql", entry.StreamName)))
		if err != nil {
			return oops.Wrapf(err, "Unable to get Spark Schema to file for registry entry %s", entry.StreamName)
		}
	}

	return nil
}
