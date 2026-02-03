package activity

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/samsaraaws/s3iface"
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

func TestWriteBackfillMetadataActivity_Execute(t *testing.T) {
	const (
		testBackfillRequestId = "test-request-id"
		testEntityName        = "test_entity"
	)

	testFailedOrgIds := []int64{123, 456, 789}

	testCases := []struct {
		description   string
		failedOrgIds  []int64
		setupMocks    func(*mockS3Client)
		shouldError   bool
		errContains   string
		setupEnvVars  func(*testing.T)
	}{
		{
			description:  "should successfully write metadata to S3 with failed org IDs",
			failedOrgIds: testFailedOrgIds,
			setupMocks: func(client *mockS3Client) {
				client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					// Verify the input parameters
					require.Contains(t, *input.Bucket, "samsara-emr-replication-export-")
					require.Equal(t, "backfill/data/test_entity/test-request-id/metrics.json", *input.Key)
					require.Equal(t, s3.ObjectCannedACLBucketOwnerFullControl, *input.ACL)

					// Verify the metadata content
					require.NotNil(t, input.Body)
					bodyBytes, err := io.ReadAll(input.Body)
					require.NoError(t, err)

					var metadata map[string]interface{}
					err = json.Unmarshal(bodyBytes, &metadata)
					require.NoError(t, err)

					failedOrgIds, ok := metadata["failed_org_ids"].([]interface{})
					require.True(t, ok, "failed_org_ids should be an array")
					require.Len(t, failedOrgIds, 3)

					// Convert to int64 slice for comparison
					var actualOrgIds []int64
					for _, id := range failedOrgIds {
						floatId, ok := id.(float64)
						require.True(t, ok)
						actualOrgIds = append(actualOrgIds, int64(floatId))
					}
					require.ElementsMatch(t, testFailedOrgIds, actualOrgIds)

					return &s3.PutObjectOutput{}, nil
				}
			},
			setupEnvVars: func(t *testing.T) {
				t.Setenv("IS_RUNNING_IN_ECS", "true")
				t.Setenv("ECS_CLUSTERNAME_OVERRIDE", "us2")
			},
			shouldError: false,
		},
		{
			description:  "should successfully write metadata to S3 with empty failed org IDs",
			failedOrgIds: []int64{},
			setupMocks: func(client *mockS3Client) {
				client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					// Verify the input parameters
					require.Contains(t, *input.Bucket, "samsara-emr-replication-export-")
					require.Equal(t, "backfill/data/test_entity/test-request-id/metrics.json", *input.Key)

					// Verify the metadata content
					require.NotNil(t, input.Body)
					bodyBytes, err := io.ReadAll(input.Body)
					require.NoError(t, err)

					var metadata map[string]interface{}
					err = json.Unmarshal(bodyBytes, &metadata)
					require.NoError(t, err)

					failedOrgIds, ok := metadata["failed_org_ids"].([]interface{})
					require.True(t, ok, "failed_org_ids should be an array")
					require.Len(t, failedOrgIds, 0)

					return &s3.PutObjectOutput{}, nil
				}
			},
			setupEnvVars: func(t *testing.T) {
				t.Setenv("IS_RUNNING_IN_ECS", "true")
				t.Setenv("ECS_CLUSTERNAME_OVERRIDE", "us3")
			},
			shouldError: false,
		},
		{
			description:  "should return error when S3 PutObject fails",
			failedOrgIds: testFailedOrgIds,
			setupMocks: func(client *mockS3Client) {
				client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					return nil, assert.AnError
				}
			},
			setupEnvVars: func(t *testing.T) {
				t.Setenv("IS_RUNNING_IN_ECS", "true")
				t.Setenv("ECS_CLUSTERNAME_OVERRIDE", "us2")
			},
			shouldError: true,
			errContains: "failed to write metrics to S3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Setup environment variables
			if tc.setupEnvVars != nil {
				tc.setupEnvVars(t)
			}

			// Create mock
			s3Client := &mockS3Client{}

			// Setup mock
			if tc.setupMocks != nil {
				tc.setupMocks(s3Client)
			}

			// Create activity with mock
			activity := &WriteBackfillMetadataActivity{
				S3Client:         s3Client,
				ReplicaAppModels: &models.AppModels{}, // Initialize with empty AppModels since we don't use it in this test
			}

			// Execute activity
			args := &WriteBackfillMetadataActivityArgs{
				BackfillRequestId: testBackfillRequestId,
				EntityName:        testEntityName,
				FailedOrgIds:      tc.failedOrgIds,
			}
			result, err := activity.Execute(context.Background(), args)

			// Assert results
			if tc.shouldError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, WriteBackfillMetadataActivityResult{}, result)
			}
		})
	}
} 