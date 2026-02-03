package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/amundsen/metadatahelpers"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

type ColumnDescriptionGenerator struct {
	s3Client s3iface.S3API
}

func newColumnDescriptionGenerator(s3Client s3iface.S3API) (*ColumnDescriptionGenerator, error) {
	return &ColumnDescriptionGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newColumnDescriptionGenerator)
}

func (a *ColumnDescriptionGenerator) Name() string {
	return "amundsen-column-descriptions-generator"
}

type S3File struct {
	key  string
	etag string
}

func (a *ColumnDescriptionGenerator) getFilesUploaded(ctx context.Context, bucket, key string) ([]S3File, error) {

	filesUploaded := []S3File{}

	var continuationToken *string
	for {
		resp, err := a.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(key),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "Could not list S3 bucket from databricks AWS account; verify credentials")
		}

		for _, item := range resp.Contents {
			var entry S3File
			if item.Key != nil && item.ETag != nil {
				entry = S3File{key: *item.Key, etag: *item.ETag}
				filesUploaded = append(filesUploaded, entry)
			} else {
				slog.Infow(ctx, "Found invalid S3 file")
			}
		}

		if !aws.BoolValue(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	return filesUploaded, nil

}

func SortBigStatsColumnNamesToCorrectTable(stat ksdeltalake.Table, bigStatColNames map[string]map[string]struct{}, ksColNames map[string]map[string]struct{}) (map[string]string, map[string]string, map[string]string) {
	// This is the bigstat_table
	bigStatColumnDescriptions := make(map[string]string)
	// This is for the ks_table
	ksColumnDescriptions := make(map[string]string)
	// This is for the objectstat_with_bigstat
	kinesisWithBigStatColumnDescriptions := make(map[string]string)

	for colName, colDescription := range stat.MetadataInfo.GetAllColumnDescriptions() {
		// If the column name is a field in the bigstat schema, add it to both bigstats tables
		if _, ok := bigStatColNames[stat.Name][colName]; ok {
			bigStatColumnDescriptions[colName] = colDescription
			kinesisWithBigStatColumnDescriptions[colName] = colDescription
		}

		// If the column namee is a fielld in the ks stat schema, add it to the ks table
		if _, ok := ksColNames[stat.Name][colName]; ok {
			ksColumnDescriptions[colName] = colDescription
		}

		// These include default columns that exist on every ks stat (eg object_id, object_type, stat_type, ...)
		if _, ok := ksdeltalake.DefaultColumns[colName]; ok {
			ksColumnDescriptions[colName] = colDescription
			bigStatColumnDescriptions[colName] = colDescription
			kinesisWithBigStatColumnDescriptions[colName] = colDescription
		}
		// These include default columns that exist on every ks stat and start with `value.`
		// This doesnt ezist in the bigstat_table.<stat_name> tablle
		if _, ok := ksdeltalake.ValueDefaultColumnNames[colName]; ok {
			ksColumnDescriptions[colName] = colDescription
			kinesisWithBigStatColumnDescriptions[colName] = colDescription
		}
	}

	return bigStatColumnDescriptions, ksColumnDescriptions, kinesisWithBigStatColumnDescriptions
}

// getAllBigStatFieldNames gets every bigstat table, parses the field names, and makes a makes a map of
// unique fied names
func GetAllBigStatFieldNames() map[string]map[string]struct{} {
	bigStatColNames := make(map[string]map[string]struct{})
	for _, stat := range ksdeltalake.AllS3BigStatTables() {
		names := ksdeltalake.ParseFieldNamesOnType(stat.S3BigStatSchema)
		fieldNames := make(map[string]struct{}, len(names))
		for _, name := range names {
			fieldNames[name] = struct{}{}
		}
		bigStatColNames[stat.Name] = fieldNames
	}
	return bigStatColNames
}

// getAllKSFieldNamesForBigStats parses the name of every column on a kinesis stat that is also a bigstat.
func GetAllKSFieldNamesForBigStats() map[string]map[string]struct{} {
	ksColNames := make(map[string]map[string]struct{})
	for _, stat := range ksdeltalake.AllS3BigStatTables() {
		names := ksdeltalake.ParseFieldNamesOnType(stat.Schema)
		fieldNames := make(map[string]struct{}, len(names))
		for _, name := range names {
			fieldNames[name] = struct{}{}
		}
		ksColNames[stat.Name] = fieldNames
	}
	return ksColNames
}

func PipelineErrorsTableColumnDescriptions(columnDescriptions map[string]map[string]string, pipelineNames map[string]struct{}) map[string]map[string]string {
	for pipelineName := range pipelineNames {
		columnDescriptions[fmt.Sprintf("%s.pipeline_errors", pipelineName)] = map[string]string{
			"date":           "The date the error occurred. (yyyy-mm-dd).",
			"execution_id":   "​​A unique ID that represents the data pipeline run. Each invocation of a data pipeline run has a unique execution_id.",
			"execution_time": "The time the error occurred (utc) example: 2021-06-17T08:42:19.165+0000.",
			"job_type":       "The type of job that produced failing rows (eg BATCH).",
			"row":            "The row that didn’t match the expected output for that row.",
			"transform":      "The name of the pipeline and node that failed formatted like “<pipeline_name>.<node_name>”.",
		}
	}
	return columnDescriptions
}

func DataStreamDateDescription(partitionStrategy *datastreamlake.PartitionStrategy) string {
	for _, partitionCol := range partitionStrategy.PartitionColumns {
		if partitionCol == "date" {
			if partitionStrategy.DateColumnOverride != "" {
				return fmt.Sprintf("Date derived from %s", partitionStrategy.DateColumnOverride)
			} else if partitionStrategy.DateColumnOverrideExpression != "" {
				return fmt.Sprintf("Date derived from expression %s", partitionStrategy.DateColumnOverrideExpression)
			} else {
				return "Date derived from firehose ingestion date"
			}
		}
	}
	return ""
}

type Column struct {
	Name     string
	Metadata *TableMetadata
}

type TableMetadata struct {
	Comment string
}

func (a *ColumnDescriptionGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()
	columnDescriptions := make(map[string]map[string]string)

	// Dagster
	dagsterSchemas, err := a.getFilesUploaded(ctx, "samsara-amundsen-metadata", "staging/schemas/")

	if err != nil {
		slog.Infow(ctx, "error pulling Dagster schemas")
	}

	for _, fileName := range dagsterSchemas {

		output, err := a.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String("samsara-amundsen-metadata"),
			Key:    aws.String(fileName.key),
		})
		if err != nil {
			return oops.Wrapf(err, "failed to read %s", fileName.key)
		}
		defer output.Body.Close()

		fileBytes, err := ioutil.ReadAll(output.Body)
		if err != nil {
			slog.Infow(ctx, "Error reading file bytes", "file", fileName.key, "error", err)
			continue
		}

		// Skip empty files or files that contain only an empty object "{}"
		// Some Dagster assets have no schema metadata, which gets serialized as "{}"
		trimmed := bytes.TrimSpace(fileBytes)
		if len(trimmed) == 0 || string(trimmed) == "{}" || string(trimmed) == "[]" {
			continue
		}

		var tableOutput *[]Column
		err = json.Unmarshal(fileBytes, &tableOutput)
		if err != nil {
			// Log only at debug level since many schemas may have non-array formats
			slog.Debugw(ctx, "Skipping schema file with non-array format", "file", fileName.key)
			continue
		}

		columnNames := make(map[string]string)
		if tableOutput != nil {
			for _, col := range *tableOutput {
				if col.Metadata != nil && col.Metadata.Comment != "" {
					if description, ok := metadatahelpers.DefaultDescriptionIdentifiers[col.Metadata.Comment]; ok {
						columnNames[col.Name] = description
					} else {
						columnNames[col.Name] = col.Metadata.Comment
					}
				}
			}
		}

		if len(columnNames) > 0 {
			components := strings.Split(fileName.key, "/")
			database := components[2]
			filenameWithExt := filepath.Base(fileName.key)
			filename := strings.TrimSuffix(filenameWithExt, filepath.Ext(filenameWithExt))
			tableName := fmt.Sprintf("%s.%s", database, filename)
			columnDescriptions[tableName] = columnNames
		}
	}

	// Pipelines
	pipelineNames := make(map[string]struct{})
	for _, node := range metadata.DataPipelineNodes {
		tableOutput, ok := node.Output.(*outputtypes.TableOutput)
		if ok {
			columnNames := make(map[string]string)
			for _, col := range *tableOutput.Schema {
				if col.Metadata != nil && col.Metadata.Comment != "" {
					if description, ok := metadatahelpers.DefaultDescriptionIdentifiers[col.Metadata.Comment]; ok {
						columnNames[col.Name] = description
					} else {
						columnNames[col.Name] = col.Metadata.Comment
					}
				}
			}
			if len(columnNames) > 0 {
				columnDescriptions[node.Name] = columnNames
			}
			pipelineNames[tableOutput.DBName] = struct{}{}
		}

	}

	// Pipeline errors table default column descriptions
	columnDescriptions = PipelineErrorsTableColumnDescriptions(columnDescriptions, pipelineNames)

	// RDS tables
	// Reads the registry and adds to columnDescriptions where the key is "<db_name>.<table_name>" and
	// the value is the map of column names to descriptions
	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			descriptions := db.GetAllColumnDescriptions(table)
			if len(descriptions) > 0 {
				for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
					columnDescriptions[tableName] = descriptions
				}
			}
		}
	}

	// Get all the bigstat field names from the schema
	bigStatColNames := GetAllBigStatFieldNames()

	// Get all the ks field names for ks tables that are also bigstats using the schema
	ksColumnNamesForBigStat := GetAllKSFieldNamesForBigStats()

	// KS tables and BigStats
	// Reads the registry and adds to columnDescriptions where the key is "<stat_field>" (with periods to separate nested fields) and
	// the value is the desccription for the column
	// For bigstat tables parse parse all the ColumnDescriptions had decide which ones go on the
	// ks_table, bigstat_table, and objectstat_with_bigstat
	// Example:
	// objectstat_with_bigstat:  https://amundsen.internal.samsara.com/table_detail/cluster/delta/kinesisstats/osdaccelerometer_with_s3_big_stat
	// ks_table: https://amundsen.internal.samsara.com/table_detail/cluster/delta/kinesisstats/osdaccelerometer
	// bigstat_table: https://amundsen.internal.samsara.com/table_detail/cluster/delta/s3bigstats/osdaccelerometer
	for _, stat := range ksdeltalake.AllTables() {
		if stat.MetadataInfo != nil {
			if _, ok := bigStatColNames[stat.Name]; ok {
				bigStatColumnDescriptions, ksColumnDescriptions, kinesisWithBigStatColumnDescriptions := SortBigStatsColumnNamesToCorrectTable(stat, bigStatColNames, ksColumnNamesForBigStat)
				if len(ksColumnDescriptions) > 0 {
					columnDescriptions[strings.ToLower(stat.QualifiedName())] = ksColumnDescriptions
				}
				if len(bigStatColumnDescriptions) > 0 {
					columnDescriptions[strings.ToLower(stat.S3BigStatsName())] = bigStatColumnDescriptions
				}
				if len(kinesisWithBigStatColumnDescriptions) > 0 {
					columnDescriptions[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(stat.Name))] = kinesisWithBigStatColumnDescriptions
				}

			} else {
				if len(stat.MetadataInfo.GetAllColumnDescriptions()) > 0 {
					columnDescriptions[strings.ToLower(stat.QualifiedName())] = stat.MetadataInfo.GetAllColumnDescriptions()
				}

			}
		}
	}

	// S3 Tables
	for tableName, tableMetadata := range metadata.S3Tables {
		colDescriptions := make(map[string]string)
		for _, col := range tableMetadata.Schema {
			if col.Description != "" {
				if description, ok := metadatahelpers.DefaultDescriptionIdentifiers[col.Description]; ok {
					col.Description = description
				}
				colDescriptions[col.Name] = col.Description
			}
		}
		columnDescriptions[tableName] = colDescriptions
	}

	// SQL Views
	for viewName, viewMetadata := range metadata.SQLViews {
		colDescriptions := make(map[string]string)
		for _, col := range viewMetadata.Schema {
			if col.Description != "" {
				if description, ok := metadatahelpers.DefaultDescriptionIdentifiers[col.Description]; ok {
					col.Description = description
				}
				colDescriptions[col.Name] = col.Description
			}
		}
		columnDescriptions[viewName] = colDescriptions
	}

	for _, stream := range datastreamlake.Registry {
		glueSchema, err := datastreamlake.GoStructToGlueSchema(stream.Record)
		if err != nil {
			return oops.Wrapf(err, "error creating glue schema from go struct")
		}

		streamDescriptions := make(map[string]string)
		for _, col := range glueSchema {
			streamDescriptions[col.Name] = col.Comment
		}
		deltaStreamDescriptions := streamDescriptions
		if stream.PartitionStrategy != nil {
			deltaStreamDescriptions["date"] = DataStreamDateDescription(stream.PartitionStrategy)
		}
		columnDescriptions[fmt.Sprintf("datastreams.%s", stream.StreamName)] = deltaStreamDescriptions
		columnDescriptions[fmt.Sprintf("datastreams_schema.%s", stream.StreamName)] = streamDescriptions

	}

	rawDescriptions, err := json.Marshal(columnDescriptions)
	if err != nil {
		return oops.Wrapf(err, "error marshalling owners map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_column_descriptions.json"),
		Body:   bytes.NewReader(rawDescriptions),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading column description file to S3")
	}

	return nil
}

var _ MetadataGenerator = &ColumnDescriptionGenerator{}
