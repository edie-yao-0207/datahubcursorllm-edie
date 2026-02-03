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

const dateParitionName = "date"

type PartitionGenerator struct {
	s3Client s3iface.S3API
}

func newPartitionGenerator(s3Client s3iface.S3API) (*PartitionGenerator, error) {
	return &PartitionGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newPartitionGenerator)
}

func (a *PartitionGenerator) Name() string {
	return "amundsen-partition-generator"
}

func (a *PartitionGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	partitions := make(map[string][]string)

	// RDS tables
	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
				if table.PartitionStrategy.Key != "partition" {
					partitions[tableName] = []string{table.PartitionStrategy.Key}
				}
			}
		}
	}

	// Data Streams
	for _, stream := range datastreamlake.Registry {
		partitions[fmt.Sprintf("datastreams.%s", stream.StreamName)] = []string{dateParitionName}
		partitions[fmt.Sprintf("datastreams_errors.%s", stream.StreamName)] = []string{dateParitionName}
		partitions[fmt.Sprintf("datastreams_schema.%s", stream.StreamName)] = []string{dateParitionName}
		if stream.PartitionStrategy != nil {
			partitions[fmt.Sprintf("datastreams_history.%s", stream.StreamName)] = stream.PartitionStrategy.PartitionColumns
		}
	}

	// S3 Big Stats
	for _, bigStat := range ksdeltalake.AllS3BigStatTables() {
		partitions[strings.ToLower(bigStat.S3BigStatsName())] = []string{dateParitionName}
		partitions[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(bigStat.Name))] = []string{dateParitionName}
	}

	// KinesisStats
	for _, stat := range ksdeltalake.AllTables() {
		partitions[strings.ToLower(stat.QualifiedName())] = []string{dateParitionName}
	}

	// Pipelines
	for _, node := range metadata.DataPipelineNodes {
		tableOutput, ok := node.Output.(*outputtypes.TableOutput)
		if ok {
			if tableOutput.Partition != nil && len(tableOutput.Partition) > 0 {
				partitions[node.Name] = tableOutput.Partition
			}
		}
	}

	rawDescriptions, err := json.Marshal(partitions)
	if err != nil {
		return oops.Wrapf(err, "error marshalling owners map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_partitions.json"),
		Body:   bytes.NewReader(rawDescriptions),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading partitions file to S3")
	}

	return nil
}

var _ MetadataGenerator = &PartitionGenerator{}
