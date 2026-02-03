package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

type DescriptionsGenerator struct {
	s3Client s3iface.S3API
}

func newDescriptionsGenerator(s3Client s3iface.S3API) (*DescriptionsGenerator, error) {
	return &DescriptionsGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newDescriptionsGenerator)
}

func (a *DescriptionsGenerator) Name() string {
	return "amundsen-descriptions-generator"
}

func (a *DescriptionsGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	descriptions := make(map[string]string)

	// RDS tables
	// This loops through the rds tables in our registry and adds the table names and descriptions to an s3 file,
	// then used to populate the descriptions in amundsen. We can't update the descriptions on views, so we
	// need to write and read from s3 to propagate this data
	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
				descriptions[tableName] = table.BuildDescription()
			}
		}
	}

	datastreamsConfluenceLink := "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5671162/Data+Streams+Delta+Tables#How-do-we-use-it%3F"

	// Data Streams
	// This loops through the data streams in our registry and adds the stream names and descriptions to an s3 file,
	// then used to populate amundsen.
	for _, stream := range datastreamlake.Registry {
		if stream.Description != "" {
			descriptions[fmt.Sprintf("datastreams.%s", stream.StreamName)] = fmt.Sprintf("%s \n This is a view of the last 1 year of datastreams data. Data may be up to 40 minutes stale, but queries are expected to be performant. For historical data over 1 year, see datastreams_history.%s \n \n %s", stream.Description, stream.StreamName, datastreamsConfluenceLink)
			descriptions[fmt.Sprintf("datastreams_history.%s", stream.StreamName)] = fmt.Sprintf("%s This is the table of the entire history of the datastream data. Most users should prefer datastreams.%s which filters to the last year. However, in cases you need the entire history please reach out to #ask-data-platform so we can enable access to this table for you. %s", stream.Description, stream.StreamName, datastreamsConfluenceLink)
			descriptions[fmt.Sprintf("datastreams_errors.%s", stream.StreamName)] = fmt.Sprintf("This table has all of the errors for the data stream %s. %s", stream.StreamName, datastreamsConfluenceLink)
			descriptions[fmt.Sprintf("datastreams_schema.%s", stream.StreamName)] = fmt.Sprintf("%s \n This table is a view of all raw firehose parquet data for %s. This is for use only by dataplatform. %s", stream.Description, stream.StreamName, datastreamsConfluenceLink)
		}
	}

	// S3 Tables
	for tableName, tableMetadata := range metadata.S3Tables {
		if tableMetadata.Description != "" {
			descriptions[tableName] = tableMetadata.Description
		}
	}

	// SQL Views
	for viewName, viewMetadata := range metadata.SQLViews {
		if viewMetadata.Description != "" {
			descriptions[viewName] = viewMetadata.Description
		}
	}

	// Pipeline tables & pipeline error tables default descriptions
	for _, node := range metadata.DataPipelineNodes {
		descriptions[node.Name] = node.Description

		tableOutput, ok := node.Output.(*outputtypes.TableOutput)
		if ok {
			descriptions[fmt.Sprintf("%s.pipeline_errors", tableOutput.DBName)] = fmt.Sprintf("This table has all of the errors for the data pipeline %s.", tableOutput.DBName)
		}
	}

	dateFilteredMessage := "NOTE: This is a view of the last 1 year of data. For the entire historical data, query the same table but adding the suffix '_history' to the DB name. \n Reach out to #ask-data-platform for any questions"

	// Bigstats
	// Amundsen can only update descriptions based on glue entry for tables and not views
	// so this sets the description for the views to update amundsen via the cron job/s3 file
	for _, stat := range ksdeltalake.AllS3BigStatTables() {
		descriptionString := stat.BigStatsDescription()
		if descriptionString != "" {
			descriptionString += "\n\n"
		}
		descriptionString += dateFilteredMessage
		descriptions[strings.ToLower(stat.S3BigStatsName())] = descriptionString
		descriptions[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(stat.Name))] = descriptionString
	}

	// Kinesis Stats tables
	// Amundsen can only update descriptions based on glue entry for tables and not views
	// so this sets the description for the views to update amundsen via the cron job/s3 file
	for _, stat := range ksdeltalake.AllTables() {
		descriptionString := stat.KsDescription()
		if descriptionString != "" {
			descriptionString += "\n\n"
		}
		descriptionString += dateFilteredMessage
		descriptions[strings.ToLower(stat.QualifiedName())] = descriptionString
	}

	rawDescriptions, err := json.Marshal(descriptions)
	if err != nil {
		return oops.Wrapf(err, "error marshalling owners map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_descriptions.json"),
		Body:   bytes.NewReader(rawDescriptions),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading owners file to S3")
	}

	return nil
}

var _ MetadataGenerator = &DescriptionsGenerator{}
