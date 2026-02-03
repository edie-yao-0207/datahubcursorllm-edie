package metadatagenerator

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/team"
)

type TeamsGenerator struct {
	s3Client s3iface.S3API
}

func newTeamsGenerator(s3Client s3iface.S3API) (*TeamsGenerator, error) {
	return &TeamsGenerator{
		s3Client: s3Client,
	}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(newTeamsGenerator)
}

func (a *TeamsGenerator) Name() string {
	return "amundsen-teams-generator"
}

func (a *TeamsGenerator) GenerateMetadata(metadata *Metadata) error {

	ctx := context.Background()

	var AllTeams = team.GetTeamsFromTree(team.Hierarchy)

	rawTeams, err := json.Marshal(AllTeams)
	if err != nil {
		return oops.Wrapf(err, "error marshalling teams map to json")
	}

	_, err = a.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String("samsara-amundsen-metadata"),
		Key:    aws.String("delta/teams.json"),
		Body:   bytes.NewReader(rawTeams),
		ACL:    aws.String(s3.ObjectCannedACLBucketOwnerFullControl),
	})
	if err != nil {
		return oops.Wrapf(err, "error uploading teams file to S3")
	}

	return nil
}

var _ MetadataGenerator = &TeamsGenerator{}
