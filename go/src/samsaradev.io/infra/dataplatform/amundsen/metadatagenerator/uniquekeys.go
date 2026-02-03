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
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
)

type UniqueKeysGenerator struct {
	s3Client s3iface.S3API
}

func newUniqueKeysGenerator(s3Client s3iface.S3API) (*UniqueKeysGenerator, error) {
	return &UniqueKeysGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newUniqueKeysGenerator)
}

func (a *UniqueKeysGenerator) Name() string {
	return "amundsen-github-source-generator"
}

func (a *UniqueKeysGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()
	uniqueKeys := getUniqueKeys(metadata)

	rawUniqueKeys, err := json.Marshal(uniqueKeys)
	if err != nil {
		return oops.Wrapf(err, "error marshaling unique keys map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_unique_keys.json"),
		Body:   bytes.NewReader(rawUniqueKeys),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading unique keys file to S3")
	}

	return nil
}

var _ MetadataGenerator = &UniqueKeysGenerator{}

func getUniqueKeys(metadata *Metadata) map[string][]string {
	uniqueKeys := make(map[string][]string)

	// Pipeline Nodes
	for _, node := range metadata.DataPipelineNodes {
		tableOutput, ok := node.Output.(*outputtypes.TableOutput)
		if ok {
			if len(tableOutput.PrimaryKey) > 0 {
				uniqueKeys[fmt.Sprintf("%s.%s", tableOutput.DBName, tableOutput.TableName)] = tableOutput.PrimaryKey
			}
		}
	}

	// RDS tables
	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			if len(table.PrimaryKeys) > 0 {
				for _, tableName := range getAllTableNames(db, table, infraconsts.SamsaraAWSDefaultRegion) {
					uniqueKeys[tableName] = table.PrimaryKeys
				}
			}
		}
	}

	// KinesisStats & BigStats
	// ksPrimaryKey is pulled directly from ksdeltalake merge jobs
	// https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/ksdeltalake.go#L324
	ksPrimaryKey := []string{"org_id", "object_type", "object_id", "time"}

	// PrimaryKey for kinesistats.location is slightly different
	// https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/ksdeltalake.go#L328-L329
	locationPrimaryKey := []string{"org_id", "device_id", "time"}

	for _, table := range ksdeltalake.AllTables() {
		tableName := strings.ToLower(table.QualifiedName())
		uniqueKeys[tableName] = ksPrimaryKey
		if tableName == "kinesisstats.location" {
			uniqueKeys[tableName] = locationPrimaryKey
		}

		if table.S3BigStatSchema != nil {
			uniqueKeys[strings.ToLower(table.S3BigStatsName())] = ksPrimaryKey
			uniqueKeys[fmt.Sprintf("kinesisstats.%s_with_s3_big_stat", strings.ToLower(table.Name))] = ksPrimaryKey
		}
	}

	return uniqueKeys
}
