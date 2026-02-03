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

	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

const (
	kinesisStatsAmundsenTag  amundsentags.Tag = "kinesisstats"
	rdsAmundsenTag           amundsentags.Tag = "rds"
	dataPipelinesAmundsenTag amundsentags.Tag = "datapipelines"
	// raw datastreams is no longer exposed to end users
	// dataStreamAmundsenTag      amundsentags.Tag = "datastream"
	deltaDataStreamAmundsenTag amundsentags.Tag = "deltadatastreams"
	s3BigStatAmundsenTag       amundsentags.Tag = "s3bigstat"
	dataPrepAmundsenTag        amundsentags.Tag = "dataprep"
	customer360AmundsenTag     amundsentags.Tag = "customer360"
)

type TagGenerator struct {
	s3Client s3iface.S3API
}

func newTagGenerator(s3Client s3iface.S3API) (*TagGenerator, error) {
	return &TagGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newTagGenerator)
}

func (a *TagGenerator) Name() string {
	return "amundsen-tag-generator"
}

func (a *TagGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()
	tags := make(map[string][]amundsentags.Tag)

	// Kinesis & Big Stats tables
	for _, stat := range ksdeltalake.AllTables() {
		tags[strings.ToLower(stat.QualifiedName())] = append(stat.Tags, kinesisStatsAmundsenTag)

		if stat.S3BigStatSchema != nil {
			tags[strings.ToLower(stat.S3BigStatsName())] = append(stat.Tags, s3BigStatAmundsenTag)
		}
	}

	// RDS tables
	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
				tags[tableName] = append(table.Tags, rdsAmundsenTag)
			}
		}
	}

	// Data Pipelines
	for _, pipelineNode := range metadata.DataPipelineNodes {
		tags[pipelineNode.Name] = []amundsentags.Tag{dataPipelinesAmundsenTag}

		if strings.Contains(pipelineNode.Name, "dataprep") {
			tags[pipelineNode.Name] = append(tags[pipelineNode.Name], dataPrepAmundsenTag)
		}

		if strings.Contains(pipelineNode.Name, "customer360") {
			tags[pipelineNode.Name] = append(tags[pipelineNode.Name], customer360AmundsenTag)
		}
	}

	// Data Streams
	for _, stream := range datastreamlake.Registry {
		tags[fmt.Sprintf("datastreams.%s", stream.StreamName)] = append(stream.Tags, deltaDataStreamAmundsenTag)
	}

	rawTags, err := json.Marshal(tags)
	if err != nil {
		return oops.Wrapf(err, "error marhsaling tags map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_tags.json"),
		Body:   bytes.NewReader(rawTags),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading owners file to S3")
	}

	return nil
}

var _ MetadataGenerator = &TagGenerator{}
