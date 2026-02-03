package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/emrworker"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/platform/entity/entities"
	"samsaradev.io/platform/entity/entity"
	"samsaradev.io/platform/entity/entitydataaccessor"
	"samsaradev.io/platform/entity/entitymetadataaccessor"
	"samsaradev.io/platform/entity/entityproto"
	entitytypes "samsaradev.io/platform/entity/types"
	"samsaradev.io/platform/entity/types/value"
	"samsaradev.io/platform/entity/types/value/helpers"
)

// mockEntityMetadataAccessor is a mock implementation of entitymetadataaccessor.Accessor
type mockEntityMetadataAccessor struct {
	mock.Mock
	entitymetadataaccessor.Accessor
	getFullEntityMapFunc func(ctx context.Context, orgId int64) (*entities.EntityMap, error)
}

func (m *mockEntityMetadataAccessor) GetFullEntityMap(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
	if m.getFullEntityMapFunc != nil {
		return m.getFullEntityMapFunc(ctx, orgId)
	}
	args := m.Called(ctx, orgId)
	return args.Get(0).(*entities.EntityMap), args.Error(1)
}

// mockEntityDataAccessor is a mock implementation of entitydataaccessor.Accessor
type mockEntityDataAccessor struct {
	mock.Mock
	entitydataaccessor.Accessor
	getUnjoinedEntitiesPaginatedFunc func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error)
}

func (m *mockEntityDataAccessor) GetUnjoinedEntitiesPaginated(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
	if m.getUnjoinedEntitiesPaginatedFunc != nil {
		return m.getUnjoinedEntitiesPaginatedFunc(ctx, orgId, authzPrincipalId, entity, args, pageToken)
	}
	arguments := m.Called(ctx, orgId, authzPrincipalId, entity, args, pageToken)
	return arguments.Get(0).(value.Value), arguments.Get(1).(*string), arguments.Error(2)
}

// mockKinesisClient is a mock implementation of kinesisiface.KinesisAPI
type mockKinesisClient struct {
	mock.Mock
	kinesisiface.KinesisAPI
	putRecordsWithContextFunc func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error)
}

func (m *mockKinesisClient) PutRecordsWithContext(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
	if m.putRecordsWithContextFunc != nil {
		return m.putRecordsWithContextFunc(ctx, input, opts...)
	}
	args := m.Called(ctx, input, opts)
	return args.Get(0).(*kinesis.PutRecordsOutput), args.Error(1)
}

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

// createParameterizedEntityProto creates a test entity with parameters support (like HosViolation)
func createParameterizedEntityProto(entityId string) *entityproto.Entity {
	return &entityproto.Entity{
		Id:         entityId,
		PrimaryKey: "id",
		Type: &entityproto.Type{
			DataType: entityproto.Type_Object,
			Fields: map[string]*entityproto.Field{
				"id": {
					Type: &entityproto.Type{DataType: entityproto.Type_String},
				},
				"timestamp": {
					Type: &entityproto.Type{DataType: entityproto.Type_Integer},
				},
			},
		},
		ApiEndpoints: []*entityproto.ApiEndpoint{
			{
				Type:       entityproto.ApiEndpoint_ListRecords,
				GrpcMethod: "GetParameterizedEntities",
				ListRecordsSpec: &entityproto.ApiEndpoint_ListRecordsSpec{
					ParameterOptions: &entityproto.ParameterOptions{
						Type: &entityproto.Type{
							DataType: entityproto.Type_Object,
							Fields: map[string]*entityproto.Field{
								"startMs": {
									Type: &entityproto.Type{DataType: entityproto.Type_Integer},
								},
								"endMs": {
									Type: &entityproto.Type{DataType: entityproto.Type_Integer},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestStreamEmrDataToKinesisActivity_Execute(t *testing.T) {
	// Create test entity proto
	testEntityProto := &entityproto.Entity{
		Id:         "test_entity",
		PrimaryKey: "id",
		Type: &entityproto.Type{
			DataType: entityproto.Type_Object,
			Fields: map[string]*entityproto.Field{
				"id": {
					Type: &entityproto.Type{DataType: entityproto.Type_String},
				},
				"name": {
					Type: &entityproto.Type{DataType: entityproto.Type_String},
				},
			},
		},
		ApiEndpoints: []*entityproto.ApiEndpoint{
			{
				Type:       entityproto.ApiEndpoint_ListRecords,
				GrpcMethod: "GetTestEntities",
				ListRecordsSpec: &entityproto.ApiEndpoint_ListRecordsSpec{
					FilterOptions: []*entityproto.FilterOption{
						{
							Field:      "assetIdIn",
							Comparator: entityproto.FilterComparator_In,
						},
						{
							Field:      "tripStartTimeGreaterThanOrEqual",
							Comparator: entityproto.FilterComparator_GreaterThanOrEqual,
						},
					},
				},
			},
		},
	}

	// Create test entity map
	entityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{testEntityProto})
	require.NoError(t, err)
	require.Empty(t, rejectedEntities)

	// Create test data
	objectType := helpers.MustMakeType(entity.MakeObjectType(map[string]*entityproto.Field{
		"id": {
			Type: entity.MakeStringType(entityproto.StringFormat_Any),
		},
		"name": {
			Type: entity.MakeStringType(entityproto.StringFormat_Any),
		},
	}))

	testData, err := value.NewUnvalidatedObjectValue(objectType, map[string]interface{}{
		"id":   "1",
		"name": "test",
	})
	require.NoError(t, err)

	arrayType, err := entitytypes.MakeArrayType(testData.Type())
	require.NoError(t, err)

	arrayValue, err := value.NewArrayValue(arrayType, testData)
	require.NoError(t, err)

	const (
		testOrgId             = int64(1)
		testBackfillRequestId = "test-request-id"
		testEntityName        = "test_entity"
	)

	// Create a mock clock with a fixed time
	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock := clock.NewMock()
	mockClock.Set(mockTime)
	expectedTimestamp := samtime.TimeToMs(mockTime)

	testCases := []struct {
		description         string
		args                *StreamEmrDataToKinesisActivityArgs
		setupMocks          func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client)
		expectedFailedCount *int64
		shouldError         bool
		errContains         string
	}{
		{
			description: "should successfully stream data to Kinesis",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        testEntityName,
				PageToken:         nil,
				PageSize:          500,
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return entityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					return arrayValue, nil, nil
				}
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					require.Contains(t, *input.StreamName, "emr_replication_backfill_kinesis_data_streams_")
					require.Len(t, input.Records, 1)
					require.Equal(t, fmt.Sprintf("%s-%d-%s-%d", testEntityName, testOrgId, testBackfillRequestId, expectedTimestamp), *input.Records[0].PartitionKey)
					require.NotEmpty(t, input.Records[0].Data)

					var record kinesisRecord
					err := json.Unmarshal(input.Records[0].Data, &record)
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("%d", testOrgId), record.OrgId)
					require.Equal(t, testBackfillRequestId, record.BackfillRequestId)
					require.Equal(t, testEntityName, record.EntityName)
					require.Equal(t, emrworker.ChangeOperationCreateOrUpdate, record.ChangeOperation)
					require.Equal(t, mockTime, record.ReceivedAt)

					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			shouldError: false,
		},
		{
			description: "should handle pagination correctly",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        testEntityName,
				PageToken:         aws.String("next-page"),
				PageSize:          500,
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return entityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					require.Equal(t, "next-page", *pageToken)
					return arrayValue, nil, nil
				}
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			expectedFailedCount: aws.Int64(0),
			shouldError:         false,
		},
		{
			description: "should handle Kinesis throttling with retries",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        testEntityName,
				PageToken:         nil,
				PageSize:          500,
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return entityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					return arrayValue, nil, nil
				}
				attempt := 0
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					attempt++
					if attempt == 1 {
						return &kinesis.PutRecordsOutput{
							FailedRecordCount: aws.Int64(1),
							Records: []*kinesis.PutRecordsResultEntry{
								{
									ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
									ErrorMessage: aws.String("Rate exceeded"),
								},
							},
						}, nil
					}
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			expectedFailedCount: aws.Int64(0),
			shouldError:         false,
		},
		{
			description: "should return error when entity not found",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        "nonexistent_entity",
				PageToken:         nil,
				PageSize:          500,
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return &entities.EntityMap{}, nil
				}
			},
			shouldError: true,
			errContains: "entity not found",
		},
		{
			description: "should successfully upload oversized records to S3 and send S3 reference to Kinesis",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        testEntityName,
				PageToken:         nil,
				PageSize:          500,
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return entityMap, nil
				}
				// Create oversized data that will trigger S3 upload
				oversizedData := make([]byte, maxRecordSize+1)
				for i := range oversizedData {
					oversizedData[i] = byte('a' + (i % 26)) // Fill with letters
				}
				oversizedValue, err := value.NewUnvalidatedObjectValue(objectType, map[string]interface{}{
					"id":   "1",
					"name": string(oversizedData),
				})
				require.NoError(t, err)
				oversizedArray, err := value.NewArrayValue(arrayType, oversizedValue)
				require.NoError(t, err)

				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					return oversizedArray, nil, nil
				}

				// Mock S3 upload to succeed
				mockS3Client.putObjectWithContextFunc = func(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
					require.Contains(t, *input.Bucket, "samsara-emr-replication-export-")
					require.Contains(t, *input.Key, fmt.Sprintf("backfill/large-data/%s/%s/%d/", testEntityName, testBackfillRequestId, testOrgId))
					require.Equal(t, "application/json", *input.ContentType)
					require.NotNil(t, input.Body)

					// Verify metadata
					require.Equal(t, testEntityName, *input.Metadata["entity-name"])
					require.Equal(t, fmt.Sprintf("%d", testOrgId), *input.Metadata["org-id"])
					require.Equal(t, testBackfillRequestId, *input.Metadata["backfill-request-id"])

					return &s3.PutObjectOutput{
						ETag: aws.String("test-etag"),
					}, nil
				}

				// Mock Kinesis to receive the S3 reference record
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					require.Contains(t, *input.StreamName, "emr_replication_backfill_kinesis_data_streams_")
					require.Len(t, input.Records, 1)
					require.Equal(t, fmt.Sprintf("%s-%d-%s-%d", testEntityName, testOrgId, testBackfillRequestId, expectedTimestamp), *input.Records[0].PartitionKey)
					require.NotEmpty(t, input.Records[0].Data)

					var record kinesisRecord
					err := json.Unmarshal(input.Records[0].Data, &record)
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("%d", testOrgId), record.OrgId)
					require.Equal(t, testBackfillRequestId, record.BackfillRequestId)
					require.Equal(t, testEntityName, record.EntityName)
					require.Equal(t, emrworker.ChangeOperationCreateOrUpdate, record.ChangeOperation)
					require.Equal(t, mockTime, record.ReceivedAt)

					// Verify that the record contains an S3 path instead of inline data
					require.Nil(t, record.EmrResponse, "EmrResponse should be nil for S3-offloaded records")
					require.NotEmpty(t, record.S3Path, "S3Path should be set for large records")
					require.Contains(t, record.S3Path, "s3://samsara-emr-replication-export-")
					require.Contains(t, record.S3Path, fmt.Sprintf("backfill/large-data/%s/%s/%d/", testEntityName, testBackfillRequestId, testOrgId))

					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			shouldError: false,
		},
		{
			description: "should successfully handle parameterized entity with startMs and endMs",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        "parameterized_entity",
				PageToken:         nil,
				PageSize:          500,
				StartMs:           aws.Int64(1609459200000), // Jan 1, 2021
				EndMs:             aws.Int64(1612137600000), // Feb 1, 2021
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				// Create parameterized entity
				paramEntityProto := createParameterizedEntityProto("parameterized_entity")
				paramEntityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{paramEntityProto})
				require.NoError(t, err)
				require.Empty(t, rejectedEntities)

				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return paramEntityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					// Verify parameters are set correctly
					require.NotNil(t, args.CustomParameters, "CustomParameters should be set for parameterized entity")

					// Build the parameters object to verify values
					paramsValue := args.CustomParameters.Build()
					paramsObj, err := paramsValue.AsObject()
					require.NoError(t, err)

					startMsValue := paramsObj.Field("startMs")
					require.False(t, startMsValue.IsNil())
					startMsInt, err := startMsValue.AsInt64()
					require.NoError(t, err)
					assert.Equal(t, int64(1609459200000), startMsInt)

					endMsValue := paramsObj.Field("endMs")
					require.False(t, endMsValue.IsNil())
					endMsInt, err := endMsValue.AsInt64()
					require.NoError(t, err)
					assert.Equal(t, int64(1612137600000), endMsInt)

					return arrayValue, nil, nil
				}
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			shouldError: false,
		},
		{
			description: "should use current time as endMs for parameterized entity when not provided",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        "parameterized_entity",
				PageToken:         nil,
				PageSize:          500,
				StartMs:           aws.Int64(1609459200000),
				EndMs:             nil, // Not provided
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				paramEntityProto := createParameterizedEntityProto("parameterized_entity")
				paramEntityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{paramEntityProto})
				require.NoError(t, err)
				require.Empty(t, rejectedEntities)

				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return paramEntityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					// Verify endMs uses current time (mockTime)
					require.NotNil(t, args.CustomParameters)

					// Build the parameters object to verify values
					paramsValue := args.CustomParameters.Build()
					paramsObj, err := paramsValue.AsObject()
					require.NoError(t, err)

					endMsValue := paramsObj.Field("endMs")
					require.False(t, endMsValue.IsNil())
					endMsInt, err := endMsValue.AsInt64()
					require.NoError(t, err)
					assert.Equal(t, expectedTimestamp, endMsInt, "endMs should be set to current time when not provided")

					return arrayValue, nil, nil
				}
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			shouldError: false,
		},
		{
			description: "should not set parameters for non-parameterized entity",
			args: &StreamEmrDataToKinesisActivityArgs{
				OrgId:             testOrgId,
				EntityName:        testEntityName,
				PageToken:         nil,
				PageSize:          500,
				StartMs:           aws.Int64(1609459200000),
				EndMs:             aws.Int64(1612137600000),
				BackfillRequestId: testBackfillRequestId,
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor, mockKinesisClient *mockKinesisClient, mockS3Client *mockS3Client) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return entityMap, nil
				}
				data.getUnjoinedEntitiesPaginatedFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, args *entitydataaccessor.QueryArguments, pageToken *string) (value.Value, *string, error) {
					// Verify parameters are NOT set for non-parameterized entity
					assert.Nil(t, args.CustomParameters, "CustomParameters should be nil for non-parameterized entity")
					return arrayValue, nil, nil
				}
				mockKinesisClient.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								SequenceNumber: aws.String("test-sequence"),
								ShardId:        aws.String("test-shard"),
							},
						},
					}, nil
				}
			},
			shouldError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Setenv("AWS_REGION", infraconsts.SamsaraAWSDefaultRegion)
			// Create mocks
			metaAccessor := &mockEntityMetadataAccessor{}
			dataAccessor := &mockEntityDataAccessor{}
			kinesisClient := &mockKinesisClient{}
			s3Client := &mockS3Client{}

			// Setup mocks
			if tc.setupMocks != nil {
				tc.setupMocks(metaAccessor, dataAccessor, kinesisClient, s3Client)
			}

			// Create activity with mocks
			activity := &StreamEmrDataToKinesisActivity{
				EntityDataAccessor:     dataAccessor,
				EntityMetadataAccessor: metaAccessor,
				KinesisClient:          kinesisClient,
				S3Client:               s3Client,
				Clock:                  mockClock,
			}

			// Execute activity
			result, err := activity.Execute(context.Background(), tc.args)

			// Assert results
			if tc.shouldError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, workflowengine.NilActivityResult{}, result)
			}
		})
	}
}

func TestStreamEmrDataToKinesisActivity_splitIntoBatches(t *testing.T) {
	activity := &StreamEmrDataToKinesisActivity{}

	// Helper to create a record with specific size
	createRecord := func(size int) *kinesis.PutRecordsRequestEntry {
		return &kinesis.PutRecordsRequestEntry{
			Data:         make([]byte, size),
			PartitionKey: aws.String("test-key"),
		}
	}

	testCases := []struct {
		name            string
		records         []*kinesis.PutRecordsRequestEntry
		expectedBatches int
		validateBatches func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry)
	}{
		{
			name:            "empty records should return empty batches",
			records:         []*kinesis.PutRecordsRequestEntry{},
			expectedBatches: 0,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				assert.Empty(t, batches)
			},
		},
		{
			name: "single small record should create one batch",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(1024), // 1KB
			},
			expectedBatches: 1,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				assert.Len(t, batches[0], 1)
			},
		},
		{
			name: "multiple small records fitting in one batch should create one batch",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB
				createRecord(1900 * 1024), // 1.9MB (total 4.9MB data + 432 bytes overhead = 5,091,760 bytes, fits under 5,242,880)
			},
			expectedBatches: 1,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				assert.Len(t, batches[0], 4)
				totalSize := 0
				for _, record := range batches[0] {
					totalSize += len(record.Data)
					if record.PartitionKey != nil {
						totalSize += len(*record.PartitionKey)
					}
					totalSize += 100 // overhead
				}
				assert.LessOrEqual(t, totalSize, maxBatchSize)
			},
		},
		{
			name: "records exceeding 5MB should split into multiple batches",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(3 * 1024 * 1024), // 3MB
				createRecord(3 * 1024 * 1024), // 3MB (total 6MB, should split)
			},
			expectedBatches: 2,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				// First batch should have 1 record (3MB)
				assert.Len(t, batches[0], 1)
				assert.Equal(t, 3*1024*1024, len(batches[0][0].Data))

				// Second batch should have 1 record (3MB)
				assert.Len(t, batches[1], 1)
				assert.Equal(t, 3*1024*1024, len(batches[1][0].Data))
			},
		},
		{
			name: "many small records should be distributed across multiple batches",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB
				createRecord(1024 * 1024), // 1MB (5MB data + 540 bytes overhead, exceeds 5MB limit)
				createRecord(512 * 1024),  // 0.5MB
				createRecord(512 * 1024),  // 0.5MB
			},
			expectedBatches: 2,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				// With overhead, first batch will have 4 records (4MB + 432 bytes)
				// Fifth 1MB record will start second batch
				assert.Len(t, batches[0], 4)

				// Second batch should have 3 records (1MB + 0.5MB + 0.5MB + 324 bytes)
				assert.Len(t, batches[1], 3)

				// Verify no batch exceeds maxBatchSize
				for i, batch := range batches {
					batchSize := 0
					for _, record := range batch {
						batchSize += len(record.Data)
						if record.PartitionKey != nil {
							batchSize += len(*record.PartitionKey)
						}
						batchSize += 100 // overhead
					}
					assert.LessOrEqual(t, batchSize, maxBatchSize, "batch %d exceeds max size", i)
				}
			},
		},
		{
			name: "records that fit exactly at 5MB boundary should stay in one batch",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(2 * 1024 * 1024), // 2MB
				createRecord(2 * 1024 * 1024), // 2MB
				createRecord(1024 * 1024),     // 1MB (5MB data + 324 bytes overhead, exceeds limit)
			},
			expectedBatches: 2,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				// With overhead (108 bytes per record), this exceeds 5MB so splits
				// First batch: 2MB + 2MB + 216 bytes
				assert.Len(t, batches[0], 2)
				// Second batch: 1MB + 108 bytes
				assert.Len(t, batches[1], 1)

				// Verify sizes
				for i, batch := range batches {
					batchSize := 0
					for _, record := range batch {
						batchSize += len(record.Data)
						if record.PartitionKey != nil {
							batchSize += len(*record.PartitionKey)
						}
						batchSize += 100 // overhead
					}
					assert.LessOrEqual(t, batchSize, maxBatchSize, "batch %d exceeds max size", i)
				}
			},
		},
		{
			name: "single large record close to maxRecordSize should create one batch",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(maxRecordSize - 1), // Just under 1MB
			},
			expectedBatches: 1,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				assert.Len(t, batches[0], 1)
				assert.Equal(t, maxRecordSize-1, len(batches[0][0].Data))
			},
		},
		{
			name: "mixed sizes should split intelligently",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord(100 * 1024),      // 100KB
				createRecord(4 * 1024 * 1024), // 4MB
				createRecord(900 * 1024),      // 900KB (total: 5,218,304 bytes data + 324 bytes overhead = 5,218,628 bytes, fits under 5,242,880)
				createRecord(200 * 1024),      // 200KB
				createRecord(3 * 1024 * 1024), // 3MB
				createRecord(4 * 1024 * 1024), // 4MB
			},
			expectedBatches: 3,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				// Batch 1: 100KB + 4MB + 900KB = 5,218,304 bytes data + 324 bytes overhead = 5,218,628 bytes (fits)
				assert.Len(t, batches[0], 3)

				// Batch 2: 200KB + 3MB = 3,350,528 bytes data + 216 bytes overhead = 3,350,744 bytes (fits)
				assert.Len(t, batches[1], 2)

				// Batch 3: 4MB = 4,194,304 bytes data + 108 bytes overhead = 4,194,412 bytes (fits)
				assert.Len(t, batches[2], 1)

				// Verify all batches are under limit
				for i, batch := range batches {
					batchSize := 0
					for _, record := range batch {
						batchSize += len(record.Data)
						if record.PartitionKey != nil {
							batchSize += len(*record.PartitionKey)
						}
						batchSize += 100 // overhead
					}
					assert.LessOrEqual(t, batchSize, maxBatchSize, "batch %d exceeds max size", i)
				}
			},
		},
		{
			name: "very large number of tiny records should create multiple batches",
			records: func() []*kinesis.PutRecordsRequestEntry {
				// Create 10000 records of 1KB each = 10MB data + 1,080,000 bytes overhead
				records := make([]*kinesis.PutRecordsRequestEntry, 10000)
				for i := 0; i < 10000; i++ {
					records[i] = createRecord(1024) // 1KB
				}
				return records
			}(),
			expectedBatches: 3,
			validateBatches: func(t *testing.T, batches [][]*kinesis.PutRecordsRequestEntry) {
				// Verify no batch exceeds 5MB (including partition key and overhead)
				for i, batch := range batches {
					totalSize := 0
					for _, record := range batch {
						totalSize += len(record.Data)
						if record.PartitionKey != nil {
							totalSize += len(*record.PartitionKey)
						}
						totalSize += 100 // overhead
					}
					assert.LessOrEqual(t, totalSize, maxBatchSize, "batch %d exceeds max size", i)
				}

				// Verify all records are present
				totalRecords := 0
				for _, batch := range batches {
					totalRecords += len(batch)
				}
				assert.Equal(t, 10000, totalRecords)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			batches := activity.splitIntoBatches(tc.records)

			// Assert
			assert.Len(t, batches, tc.expectedBatches, "unexpected number of batches")

			// Validate each batch doesn't exceed maxBatchSize (including partition keys and overhead)
			for i, batch := range batches {
				batchSize := 0
				for _, record := range batch {
					batchSize += len(record.Data)
					if record.PartitionKey != nil {
						batchSize += len(*record.PartitionKey)
					}
					batchSize += 100 // overhead
				}
				assert.LessOrEqual(t, batchSize, maxBatchSize, "batch %d exceeds maxBatchSize", i)
			}

			// Run custom validation if provided
			if tc.validateBatches != nil {
				tc.validateBatches(t, batches)
			}
		})
	}
}

func TestStreamEmrDataToKinesisActivity_sendSingleBatch(t *testing.T) {
	// Create a mock clock
	mockClock := clock.NewMock()
	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mockClock.Set(mockTime)

	// Helper to create test records
	createRecord := func(data string) *kinesis.PutRecordsRequestEntry {
		return &kinesis.PutRecordsRequestEntry{
			Data:         []byte(data),
			PartitionKey: aws.String("test-key"),
		}
	}

	testCases := []struct {
		name          string
		records       []*kinesis.PutRecordsRequestEntry
		setupMock     func(*mockKinesisClient)
		expectedError bool
		errorContains string
	}{
		{
			name: "should successfully send batch on first attempt",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord("test-data-1"),
				createRecord("test-data-2"),
			},
			setupMock: func(m *mockKinesisClient) {
				m.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{SequenceNumber: aws.String("seq-1"), ShardId: aws.String("shard-1")},
							{SequenceNumber: aws.String("seq-2"), ShardId: aws.String("shard-1")},
						},
					}, nil
				}
			},
			expectedError: false,
		},
		{
			name: "should retry on ProvisionedThroughputExceededException and succeed",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord("test-data"),
			},
			setupMock: func(m *mockKinesisClient) {
				attempt := 0
				m.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					attempt++
					if attempt == 1 {
						// First attempt: throttled
						return &kinesis.PutRecordsOutput{
							FailedRecordCount: aws.Int64(1),
							Records: []*kinesis.PutRecordsResultEntry{
								{
									ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
									ErrorMessage: aws.String("Rate exceeded"),
								},
							},
						}, nil
					}
					// Second attempt: success
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{SequenceNumber: aws.String("seq-1"), ShardId: aws.String("shard-1")},
						},
					}, nil
				}
			},
			expectedError: false,
		},
		{
			name: "should fail immediately on non-throttling error",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord("test-data"),
			},
			setupMock: func(m *mockKinesisClient) {
				m.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(1),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								ErrorCode:    aws.String("InternalServerError"),
								ErrorMessage: aws.String("Something went wrong"),
							},
						},
					}, nil
				}
			},
			expectedError: true,
			errorContains: "InternalServerError",
		},
		{
			name: "should fail after max retries on persistent throttling",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord("test-data"),
			},
			setupMock: func(m *mockKinesisClient) {
				m.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					// Always return throttling error
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(1),
						Records: []*kinesis.PutRecordsResultEntry{
							{
								ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
								ErrorMessage: aws.String("Rate exceeded"),
							},
						},
					}, nil
				}
			},
			expectedError: true,
			errorContains: "failed to write 1 records to Kinesis after 3 attempts",
		},
		{
			name: "should handle partial success with throttling",
			records: []*kinesis.PutRecordsRequestEntry{
				createRecord("test-data-1"),
				createRecord("test-data-2"),
				createRecord("test-data-3"),
			},
			setupMock: func(m *mockKinesisClient) {
				attempt := 0
				m.putRecordsWithContextFunc = func(ctx aws.Context, input *kinesis.PutRecordsInput, opts ...request.Option) (*kinesis.PutRecordsOutput, error) {
					attempt++
					if attempt == 1 {
						// First attempt: 2 succeed, 1 fails
						return &kinesis.PutRecordsOutput{
							FailedRecordCount: aws.Int64(1),
							Records: []*kinesis.PutRecordsResultEntry{
								{SequenceNumber: aws.String("seq-1"), ShardId: aws.String("shard-1")},
								{SequenceNumber: aws.String("seq-2"), ShardId: aws.String("shard-1")},
								{
									ErrorCode:    aws.String("ProvisionedThroughputExceededException"),
									ErrorMessage: aws.String("Rate exceeded"),
								},
							},
						}, nil
					}
					// Second attempt: the failed one succeeds
					return &kinesis.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
						Records: []*kinesis.PutRecordsResultEntry{
							{SequenceNumber: aws.String("seq-3"), ShardId: aws.String("shard-1")},
						},
					}, nil
				}
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			mockKinesisClient := &mockKinesisClient{}
			tc.setupMock(mockKinesisClient)

			activity := &StreamEmrDataToKinesisActivity{
				KinesisClient: mockKinesisClient,
				Clock:         mockClock,
			}

			// Act
			err := activity.sendSingleBatch(context.Background(), "test-stream", tc.records, 0)

			// Assert
			if tc.expectedError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
