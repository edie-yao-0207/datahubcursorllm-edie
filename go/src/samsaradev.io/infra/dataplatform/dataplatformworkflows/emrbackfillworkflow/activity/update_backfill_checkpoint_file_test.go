package activity

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/models"
)

// mockS3Client is a mock implementation of s3iface.S3API
type mockS3Client struct {
	mock.Mock
	s3iface.S3API
	putObjectWithContextFunc func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error)
}

func (m *mockS3Client) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if m.putObjectWithContextFunc != nil {
		return m.putObjectWithContextFunc(ctx, input, opts...)
	}
	args := m.Called(ctx, input, opts)
	return args.Get(0).(*s3.PutObjectOutput), args.Error(1)
}

func TestUpdateBackfillCheckpointFile_Execute(t *testing.T) {
	const (
		testBackfillRequestId = "test-request-id"
		testEntityName        = "test_entity"
	)

	testCases := []struct {
		description string
		setupMocks  func(*mockS3Client)
		shouldError bool
		errContains string
	}{
		{
			description: "should successfully write metadata to S3",
			setupMocks: func(client *mockS3Client) {
				client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					// Verify the input parameters
					require.Contains(t, *input.Bucket, "samsara-emr-replication-export-")
					require.Equal(t, "backfill/data/test_entity/test-request-id/metadata.json", *input.Key)
					require.Equal(t, s3.ObjectCannedACLBucketOwnerFullControl, *input.ACL)

					// Verify the metadata content
					require.NotNil(t, input.Body)
					bodyBytes, err := io.ReadAll(input.Body)
					require.NoError(t, err)
					require.Contains(t, string(bodyBytes), `"completed":true`)

					return &s3.PutObjectOutput{}, nil
				}
			},
			shouldError: false,
		},
		{
			description: "should return error when S3 PutObject fails",
			setupMocks: func(client *mockS3Client) {
				client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					return nil, assert.AnError
				}
			},
			shouldError: true,
			errContains: "failed to write metadata to S3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Setenv("AWS_REGION", infraconsts.SamsaraAWSDefaultRegion)
			// Create mock
			s3Client := &mockS3Client{}

			// Setup mock
			if tc.setupMocks != nil {
				tc.setupMocks(s3Client)
			}

			// Create activity with mock
			activity := &UpdateBackfillCheckpointFile{
				S3Client:         s3Client,
				ReplicaAppModels: &models.AppModels{}, // Initialize with empty AppModels since we don't use it in this test
			}

			// Execute activity
			args := &UpdateBackfillCheckpointFileArgs{
				BackfillRequestId: testBackfillRequestId,
				EntityName:        testEntityName,
			}
			result, err := activity.Execute(context.Background(), args)

			// Assert results
			if tc.shouldError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, UpdateBackfillCheckpointFileResult{}, result)
			}
		})
	}
}
