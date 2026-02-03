package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/amundsen/metadataregistry"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
)

type TableBadgesGenerator struct {
	s3Client s3iface.S3API
}

func newTableBadgesGenerator(s3Client s3iface.S3API) (*TableBadgesGenerator, error) {
	return &TableBadgesGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newTableBadgesGenerator)
}

func (a *TableBadgesGenerator) Name() string {
	return "amundsen-table-badges-generator"
}

func (a *TableBadgesGenerator) GenerateMetadata(metadata *Metadata) error {
	ctx := context.Background()

	rawTableBadges, err := json.Marshal(metadataregistry.TableBadges)
	if err != nil {
		return oops.Wrapf(err, "error marshalling table badges map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/table_badges.json"),
		Body:   bytes.NewReader(rawTableBadges),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading table badges file to S3")
	}

	return nil
}

var _ MetadataGenerator = &TableBadgesGenerator{}
