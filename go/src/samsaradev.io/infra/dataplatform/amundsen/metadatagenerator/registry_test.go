package metadatagenerator_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/vendormocks/mock_s3iface"
)

func TestDataHubMetadataGenerators(t *testing.T) {
	var env struct {
		DataHubMetadataGenerator *metadatagenerator.DataHubMetadataGenerator
		MockS3Client             *mock_s3iface.MockS3API
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()

	// Each generator pushes a file to s3. If it doesn't push a file to s3,
	// there isn't a file for the datahub delta extractor to pull, which causes downstream issues.
	env.MockS3Client.EXPECT().PutObjectWithContext(ctx, gomock.Any()).Times(len(env.DataHubMetadataGenerator.Generators))

	env.MockS3Client.EXPECT().ListObjectsV2(gomock.Any()).Return(&s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{
				Key:  aws.String("staging/schemas/file1"),
				Size: aws.Int64(123),
			},
			{
				Key:  aws.String("staging/schemas/file2"),
				Size: aws.Int64(456),
			},
		},
		IsTruncated: aws.Bool(false),
	}, nil)

	env.MockS3Client.EXPECT().GetObjectWithContext(gomock.Any(), gomock.Any()).Return(
		&s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(""))},
		nil,
	).Times(1)

	// For each generator, generate the metadata and check that there are no errors.
	for _, generator := range env.DataHubMetadataGenerator.Generators {
		err := generator.GenerateMetadata(env.DataHubMetadataGenerator.Metadata)
		assert.NoError(t, err)
	}
}

func TestDataHubMetadataGeneratorSetup(t *testing.T) {
	a := metadatagenerator.DataHubMetadataGenerator{}
	err := a.Setup()
	require.NoError(t, err)
	assert.NotEmpty(t, a.Metadata.S3Tables)
	assert.NotEmpty(t, a.Metadata.SQLViews)
	assert.NotEmpty(t, a.Metadata.DataPipelineNodes)
}
