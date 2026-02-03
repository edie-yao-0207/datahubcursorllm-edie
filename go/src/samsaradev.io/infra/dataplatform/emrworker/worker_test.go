package emrworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	featureconfigs "samsaradev.io/infra/dataplatform/emrworker/featureconfigs"
	"samsaradev.io/infra/releasemanagement/featureconfig"
	"samsaradev.io/infra/samsaraaws/sqsiface"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/platform/entity/entities"
	"samsaradev.io/platform/entity/entitydataaccessor"
	"samsaradev.io/platform/entity/entitymetadataaccessor"
	"samsaradev.io/platform/entity/entityproto"
	entitytypes "samsaradev.io/platform/entity/types"
	"samsaradev.io/platform/entity/types/value"
)

// Mock implementations
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

type mockEntityDataAccessor struct {
	mock.Mock
	entitydataaccessor.Accessor
	getUnjoinedEntityByIdFunc func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error)
}

func (m *mockEntityDataAccessor) GetUnjoinedEntityById(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
	if m.getUnjoinedEntityByIdFunc != nil {
		return m.getUnjoinedEntityByIdFunc(ctx, orgId, authzPrincipalId, entity, recordId)
	}
	args := m.Called(ctx, orgId, authzPrincipalId, entity, recordId)
	return args.Get(0).(value.Value), args.Error(1)
}

type mockKinesisClient struct {
	mock.Mock
	kinesisiface.KinesisAPI
	putRecordFunc func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

func (m *mockKinesisClient) PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
	if m.putRecordFunc != nil {
		return m.putRecordFunc(input)
	}
	args := m.Called(input)
	return args.Get(0).(*kinesis.PutRecordOutput), args.Error(1)
}

type mockS3Client struct {
	mock.Mock
	s3iface.S3API
	putObjectFunc func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

func (m *mockS3Client) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if m.putObjectFunc != nil {
		return m.putObjectFunc(input)
	}
	args := m.Called(ctx, input)
	return args.Get(0).(*s3.PutObjectOutput), args.Error(1)
}

type mockCalls struct {
	lastDataAccessEntity *entities.Entity
	lastDataAccessIds    []string
	lastKinesisInput     *kinesis.PutRecordInput
}

func TestUnmarshalMessage(t *testing.T) {
	var env struct {
		SQSClient sqsiface.SQSAPI
	}
	testloader.MustStart(t, &env)

	ctx := context.Background()

	tests := []struct {
		name            string
		message         *sqs.Message
		entityName      string
		expectError     bool
		errContains     string
		expectedMessage *emrChangelogMsg
	}{
		{
			name:        "nil message body",
			message:     &sqs.Message{},
			expectError: true,
			errContains: "message body is nil",
		},
		{
			name: "non-json message body",
			message: &sqs.Message{
				Body: aws.String("invalid json"),
			},
			expectError: true,
			entityName:  "test-entity",
			errContains: "failed to unmarshal EMR message",
		},
		{
			name: "invalid json",
			message: &sqs.Message{
				Body: aws.String(`{"test": "test"`),
			},
			expectError: true,
			entityName:  "test-entity",
			errContains: "failed to unmarshal EMR message",
		},
		{
			name: "entity name mismatch",
			message: &sqs.Message{
				Body: aws.String(`{
					"eventId": "315bce90-c03b-4afc-a92a-ba37595a68fa",
					"entityName": "WrongEntityName",
					"eventType": "CHANGE_EVENT_TYPE_CREATE",
					"timestamp": "2025-05-04T04:04:45Z",
					"orgId": "31415",
					"recordId": "MjgxNDc0OTc5ODExNzI1LTE3NDYzMjM5OTgxMzA="
				}`),
			},
			entityName:  "TestEntityName",
			expectError: true,
			errContains: "entity name mismatch, wrong entity name received: WrongEntityName",
		},
		{
			name: "invalid record id",
			message: &sqs.Message{
				Body: aws.String(`{
					"eventId": "315bce90-c03b-4afc-a92a-ba37595a68fa",
					"entityName": "TestEntityName",
					"eventType": "CHANGE_EVENT_TYPE_CREATE",
					"timestamp": "2025-05-04T04:04:45Z",
					"orgId": "31415",
					"recordId": "invalid-record-id"
				}`),
			},
			entityName:  "TestEntityName",
			expectError: true,
			errContains: "failed to decode record id",
		},
		{
			name: "valid message",
			message: &sqs.Message{
				Body: aws.String(`{
					"eventId": "315bce90-c03b-4afc-a92a-ba37595a68fa",
					"entityName": "TestEntityName",
					"eventType": "CHANGE_EVENT_TYPE_CREATE",
					"timestamp": "2025-05-04T04:04:45Z",
					"orgId": "31415",
					"recordId": "MjgxNDc0OTc5ODExNzI1LTE3NDYzMjM5OTgxMzA="
				}`),
			},
			entityName: "TestEntityName",
			expectedMessage: &emrChangelogMsg{
				EventId:    "315bce90-c03b-4afc-a92a-ba37595a68fa",
				EntityName: "TestEntityName",
				EventType:  "CHANGE_EVENT_TYPE_CREATE",
				Timestamp:  "2025-05-04T04:04:45Z",
				OrgId:      "31415",
				RecordId:   "281474979811725-1746323998130",
			},
		},
		{
			name: "extra fields",
			message: &sqs.Message{
				Body: aws.String(`{
					"eventId": "315bce90-c03b-4afc-a92a-ba37595a68fa",
					"entityName": "TestEntityName",
					"eventType": "CHANGE_EVENT_TYPE_CREATE",
					"timestamp": "2025-05-04T04:04:45Z",
					"orgId": "31415",
					"recordId": "MjgxNDc0OTc5ODExNzI1LTE3NDYzMjM5OTgxMzA=",
					"extraField": "extraValue"
				}`),
			},
			entityName: "TestEntityName",
			// A test case to remind the developer that extra fields do not cause an error
			// but that the extra fields are simply ignored.
			expectError: false,
			expectedMessage: &emrChangelogMsg{
				EventId:    "315bce90-c03b-4afc-a92a-ba37595a68fa",
				EntityName: "TestEntityName",
				EventType:  "CHANGE_EVENT_TYPE_CREATE",
				Timestamp:  "2025-05-04T04:04:45Z",
				OrgId:      "31415",
				RecordId:   "281474979811725-1746323998130",
			},
		},
		{
			name: "missing fields",
			message: &sqs.Message{
				Body: aws.String(`{
					"entityName": "TestEntityName",
					"timestamp": "2025-05-04T04:04:45Z",
					"orgId": "31415",
				}`),
			},
			entityName:  "TestEntityName",
			expectError: true,
			errContains: "failed to unmarshal EMR message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := &EmrSqsWorker{
				sqsClient: env.SQSClient,
				config: EmrSqsWorkerConfig{
					EntityName: tt.entityName,
				},
			}
			msg, err := processor.unmarshalMessage(ctx, tt.message)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMessage, msg)
			}
		})
	}
}

func TestQueryEMRData(t *testing.T) {
	// Create a fixed eventID for testing
	fixedEventID := uuid.New().String()

	// Create test entity proto
	testEntityProto := &entityproto.Entity{
		Id:         "testEntity",
		PrimaryKey: "id",
		Type: &entityproto.Type{
			DataType: entityproto.Type_Object,
			Fields: map[string]*entityproto.Field{
				"id": {
					Type: &entityproto.Type{DataType: entityproto.Type_String},
				},
			},
		},
	}

	// Create test data
	testData := value.NewUnvalidatedValue(entitytypes.KnownPrimitives.String, map[string]interface{}{
		"id":   "234",
		"name": "test",
	})

	tests := []struct {
		name        string
		changeEvent *emrChangelogMsg
		setupMocks  func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor)
		expectError bool
		errContains string
	}{
		{
			name: "successful query",
			changeEvent: &emrChangelogMsg{
				EventId:    fixedEventID,
				EntityName: "testEntity",
				EventType:  "CHANGE_EVENT_TYPE_CREATE",
				Timestamp:  "2025-05-04T04:04:45Z",
				OrgId:      "31415",
				RecordId:   "281474979811725-1746323998130",
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					entityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{testEntityProto})
					require.NoError(t, err)
					require.Empty(t, rejectedEntities)
					return entityMap, nil
				}
				data.getUnjoinedEntityByIdFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
					return testData, nil
				}
			},
		},
		{
			name: "entity map error",
			changeEvent: &emrChangelogMsg{
				EventId:    fixedEventID,
				EntityName: "testEntity",
				EventType:  "CHANGE_EVENT_TYPE_CREATE",
				Timestamp:  "2025-05-04T04:04:45Z",
				OrgId:      "31415",
				RecordId:   "281474979811725-1746323998130",
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					return nil, assert.AnError
				}
			},
			expectError: true,
			errContains: "failed to get entity map",
		},
		{
			name: "get unjoined entities error",
			changeEvent: &emrChangelogMsg{
				EventId:    fixedEventID,
				EntityName: "testEntity",
				EventType:  "CHANGE_EVENT_TYPE_CREATE",
				Timestamp:  "2025-05-04T04:04:45Z",
				OrgId:      "31415",
				RecordId:   "281474979811725-1746323998130",
			},
			setupMocks: func(meta *mockEntityMetadataAccessor, data *mockEntityDataAccessor) {
				meta.getFullEntityMapFunc = func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
					entityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{testEntityProto})
					require.NoError(t, err)
					require.Empty(t, rejectedEntities)
					return entityMap, nil
				}
				data.getUnjoinedEntityByIdFunc = func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
					return value.NilValue, assert.AnError
				}
			},
			expectError: true,
			errContains: "failed to get unjoined entity by id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			metaAccessor := &mockEntityMetadataAccessor{}
			dataAccessor := &mockEntityDataAccessor{}

			// Setup mocks
			if tt.setupMocks != nil {
				tt.setupMocks(metaAccessor, dataAccessor)
			}

			// Create processor
			processor := &EmrSqsWorker{
				entityMetadataAccessor: metaAccessor,
				entityDataAccessor:     dataAccessor,
				config: EmrSqsWorkerConfig{
					EntityName: "testEntity",
				},
			}

			// Execute test
			result, err := processor.queryEMRData(context.Background(), tt.changeEvent)

			// Verify results
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}
}

func TestConstructKinesisRecord(t *testing.T) {
	// Create test entity type
	testType := &entityproto.Type{
		Name:     "TestEntity",
		DataType: entityproto.Type_Object,
		Fields: map[string]*entityproto.Field{
			"id": {
				Type: &entityproto.Type{DataType: entityproto.Type_String},
			},
			"orgId": {
				Type: &entityproto.Type{DataType: entityproto.Type_Integer},
			},
		},
	}

	// Create test type map
	typeMap, rejectedTypes, err := entitytypes.TypeMapFromProtos([]*entityproto.Type{testType})
	require.NoError(t, err)
	require.Empty(t, rejectedTypes)

	objType := typeMap.Get("TestEntity")
	require.NotNil(t, objType)

	// Create mock feature evaluator
	mockFeatureEvaluator := featureconfig.NewMockEvaluator()
	featureconfig.MockValues[bool](mockFeatureEvaluator).
		WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

	tests := []struct {
		name        string
		orgId       int64
		event       *emrChangelogMsg
		setupData   func() *value.Value
		expectError bool
		validate    func(t *testing.T, record *EmrKinesisRecord)
	}{
		{
			name:  "single item - create/update operation",
			orgId: 31415,
			event: &emrChangelogMsg{
				RecordId:   "test-record-1",
				EntityName: "TestEntity",
			},
			setupData: func() *value.Value {
				val := value.NewUnvalidatedValue(objType, map[string]interface{}{
					"id":    "234",
					"orgId": int64(31415),
				})
				return &val
			},
			validate: func(t *testing.T, record *EmrKinesisRecord) {
				require.Equal(t, "31415", record.OrgId)
				require.Equal(t, "test-record-1", record.RecordId)
				require.Equal(t, "TestEntity", record.EntityName)
				require.Equal(t, ChangeOperationCreateOrUpdate, record.ChangeOperation)
				require.NotNil(t, record.EmrResponse)
				require.False(t, record.EmrResponse.IsNil())
			},
		},
		{
			name:  "nil value - delete operation",
			orgId: 31415,
			event: &emrChangelogMsg{
				RecordId:   "test-record-2",
				EntityName: "TestEntity",
			},
			setupData: func() *value.Value {
				return &value.NilValue
			},
			validate: func(t *testing.T, record *EmrKinesisRecord) {
				require.Equal(t, "31415", record.OrgId)
				require.Equal(t, "test-record-2", record.RecordId)
				require.Equal(t, "TestEntity", record.EntityName)
				require.Equal(t, ChangeOperationDelete, record.ChangeOperation)
				require.True(t, record.EmrResponse.IsNil())
			},
		},
		{
			name:  "large record - S3 offloading",
			orgId: 31415,
			event: &emrChangelogMsg{
				RecordId:   "test-record-large",
				EntityName: "TestEntity",
				Timestamp:  "2025-06-19T12:00:00Z",
			},
			setupData: func() *value.Value {
				// Create a large data structure that will exceed 1MB when marshaled
				largeData := make(map[string]interface{})
				largeData["id"] = "large-record"
				largeData["orgId"] = int64(31415)

				// Add enough data to exceed 1MB (1048576 bytes)
				// Each string is about 1000 characters, so we need about 1100 of them
				for i := 0; i < 1200; i++ {
					key := fmt.Sprintf("field_%d", i)
					// Create a string of about 1000 characters
					value := strings.Repeat(fmt.Sprintf("data_%d_", i), 100)
					largeData[key] = value
				}

				val := value.NewUnvalidatedValue(objType, largeData)
				return &val
			},
			validate: func(t *testing.T, record *EmrKinesisRecord) {
				require.Equal(t, "31415", record.OrgId)
				require.Equal(t, "test-record-large", record.RecordId)
				require.Equal(t, "TestEntity", record.EntityName)
				require.Equal(t, ChangeOperationCreateOrUpdate, record.ChangeOperation)

				// Verify S3 offloading occurred
				require.Nil(t, record.EmrResponse, "EmrResponse should be nil when data is offloaded to S3")
				require.NotEmpty(t, record.S3Path, "S3Path should be set when data is offloaded")
				require.Contains(t, record.S3Path, "s3://", "S3Path should be a valid S3 URL")
				require.Contains(t, record.S3Path, "cdc/large-data/TestEntity/31415/test-record-large/", "S3Path should contain the expected path structure")
				require.Contains(t, record.S3Path, "20250619120000.json", "S3Path should contain sanitized timestamp")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := &EmrSqsWorker{
				config: EmrSqsWorkerConfig{
					EntityName: "TestEntity",
					S3Bucket:   "test-bucket",
				},
				featureConfigEvaluator: mockFeatureEvaluator,
			}

			// Set up mock S3 client for S3 offloading test
			if tt.name == "large record - S3 offloading" {
				mockS3 := &mockS3Client{
					putObjectFunc: func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
						// Verify S3 upload parameters
						require.Equal(t, "test-bucket", *input.Bucket)
						require.Contains(t, *input.Key, "cdc/large-data/TestEntity/31415/test-record-large/")
						require.Contains(t, *input.Key, "20250619120000.json")
						require.Equal(t, "application/json", *input.ContentType)

						// Verify metadata
						require.Equal(t, "TestEntity", *input.Metadata["entity-name"])
						require.Equal(t, "31415", *input.Metadata["org-id"])
						require.Equal(t, "test-record-large", *input.Metadata["record-id"])

						return &s3.PutObjectOutput{}, nil
					},
				}
				worker.s3Client = mockS3
			}

			record, err := worker.constructKinesisRecord(context.Background(), tt.orgId, tt.event, tt.setupData())

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, record)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, record)
			tt.validate(t, record)
		})
	}
}

func TestWriteToKinesisDataStream(t *testing.T) {
	// Create a test entity type that matches our data structure
	testType := &entityproto.Type{
		Name:     "Object",
		DataType: entityproto.Type_Object,
		Fields: map[string]*entityproto.Field{
			"id": {
				Type: &entityproto.Type{DataType: entityproto.Type_String},
			},
			"orgId": {
				Type: &entityproto.Type{DataType: entityproto.Type_Integer},
			},
			"name": {
				Type: &entityproto.Type{DataType: entityproto.Type_String},
			},
		},
	}

	tests := []struct {
		name        string
		setupData   func(t *testing.T) *EmrKinesisRecord
		mockKinesis func() *mockKinesisClient
		expectError bool
		errContains string
	}{
		{
			name: "successful write",
			setupData: func(t *testing.T) *EmrKinesisRecord {
				// Create test data with orgId
				typeMap, rejectedTypes, err := entitytypes.TypeMapFromProtos([]*entityproto.Type{testType})
				require.NoError(t, err)
				require.Empty(t, rejectedTypes)

				objType := typeMap.Get("Object")
				require.NotNil(t, objType)

				val := value.NewUnvalidatedValue(objType, map[string]interface{}{
					"id":    "234",
					"orgId": int64(31415),
					"name":  "test",
				})

				return &EmrKinesisRecord{
					OrgId:           "31415",
					RecordId:        "test-record-1",
					EmrResponse:     &val,
					ChangeOperation: ChangeOperationCreateOrUpdate,
					Timestamp:       "2025-05-04T04:04:45Z",
				}
			},
			mockKinesis: func() *mockKinesisClient {
				return &mockKinesisClient{
					putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
						// Verify the partition key includes entityName, orgId, timestampBucket, and receiveCount
						// timestampBucket for "2025-05-04T04:04:45Z" with 1-min interval is "2025-05-04T04:04-04:05"
						assert.Equal(t, "TestEntity-31415-2025-05-04T04:04-04:05-1", *input.PartitionKey)
						return &kinesis.PutRecordOutput{}, nil
					},
				}
			},
		},
		{
			name: "delete operation",
			setupData: func(t *testing.T) *EmrKinesisRecord {
				return &EmrKinesisRecord{
					OrgId:           "31415",
					RecordId:        "test-record-2",
					EmrResponse:     &value.NilValue,
					ChangeOperation: ChangeOperationDelete,
					Timestamp:       "2025-05-04T04:05:30Z",
				}
			},
			mockKinesis: func() *mockKinesisClient {
				return &mockKinesisClient{
					putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
						// Verify the partition key includes entityName, orgId, timestampBucket, and receiveCount
						// timestampBucket for "2025-05-04T04:05:30Z" with 1-min interval is "2025-05-04T04:05-04:06"
						assert.Equal(t, "TestEntity-31415-2025-05-04T04:05-04:06-1", *input.PartitionKey)
						return &kinesis.PutRecordOutput{}, nil
					},
				}
			},
		},
		{
			name: "record with S3 path",
			setupData: func(t *testing.T) *EmrKinesisRecord {
				return &EmrKinesisRecord{
					OrgId:           "31415",
					RecordId:        "test-record-s3",
					EmrResponse:     nil, // Data is in S3
					S3Path:          "s3://test-bucket/cdc/large-data/TestEntity/31415/test-record-s3/20250619120000.json",
					ChangeOperation: ChangeOperationCreateOrUpdate,
					EntityName:      "TestEntity",
					Timestamp:       "2025-06-19T12:00:00Z",
				}
			},
			mockKinesis: func() *mockKinesisClient {
				return &mockKinesisClient{
					putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
						// Verify the partition key includes entityName, orgId, timestampBucket, and receiveCount
						// timestampBucket for "2025-06-19T12:00:00Z" with 1-min interval is "2025-06-19T12:00-12:01"
						assert.Equal(t, "TestEntity-31415-2025-06-19T12:00-12:01-1", *input.PartitionKey)

						// Verify the record contains S3 path and no EmrResponse
						var record map[string]interface{}
						err := json.Unmarshal(input.Data, &record)
						require.NoError(t, err)

						assert.Equal(t, "s3://test-bucket/cdc/large-data/TestEntity/31415/test-record-s3/20250619120000.json", record["s3Path"])
						assert.Nil(t, record["emrResponse"])
						assert.Equal(t, "createOrUpdate", record["changeOperation"])

						return &kinesis.PutRecordOutput{}, nil
					},
				}
			},
		},
		{
			name: "kinesis put record error",
			setupData: func(t *testing.T) *EmrKinesisRecord {
				// Create valid test data
				typeMap, rejectedTypes, err := entitytypes.TypeMapFromProtos([]*entityproto.Type{testType})
				require.NoError(t, err)
				require.Empty(t, rejectedTypes)

				objType := typeMap.Get("Object")
				require.NotNil(t, objType)

				val := value.NewUnvalidatedValue(objType, map[string]interface{}{
					"id":    "234",
					"orgId": int64(31415),
					"name":  "test",
				})

				return &EmrKinesisRecord{
					OrgId:           "31415",
					RecordId:        "test-record-4",
					EmrResponse:     &val,
					ChangeOperation: ChangeOperationCreateOrUpdate,
					Timestamp:       "2025-05-04T04:07:15Z",
				}
			},
			mockKinesis: func() *mockKinesisClient {
				return &mockKinesisClient{
					putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
						return nil, assert.AnError
					},
				}
			},
			expectError: true,
			errContains: "failed to put record to Kinesis stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create processor with mock Kinesis client
			processor := &EmrSqsWorker{
				kinesisClient: tt.mockKinesis(),
				config: EmrSqsWorkerConfig{
					StreamName: "test-stream",
					EntityName: "TestEntity",
				},
			}

			// Execute test
			record := tt.setupData(t)
			// Calculate timestampBucket from the record's timestamp using 1-minute interval
			timestampBucket := ""
			if record.Timestamp != "" {
				timestampBucket = bucketTimestampWithinXMinuteInterval(record.Timestamp, 1)
			} else {
				// Use a default timestamp for tests without timestamps
				timestampBucket = bucketTimestampWithinXMinuteInterval("2025-05-04T04:04:45Z", 1)
			}
			err := processor.writeToKinesisDataStream(record, timestampBucket, 1)

			// Verify results
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessMessage_Success(t *testing.T) {
	// Create test entity type
	entityTypeName := "TestEntity"
	entityType := &entityproto.Type{
		Name:     entityTypeName,
		DataType: entityproto.Type_Object,
		Fields: map[string]*entityproto.Field{
			"id": {
				Type: &entityproto.Type{DataType: entityproto.Type_String},
			},
			"orgId": {
				Type: &entityproto.Type{DataType: entityproto.Type_Integer},
			},
		},
	}

	// Create test entity
	testEntity := &entityproto.Entity{
		Id:         entityTypeName,
		PrimaryKey: "id",
		Type:       entityType,
	}

	// Create test entity map
	entityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{testEntity})
	require.NoError(t, err)
	require.Empty(t, rejectedEntities)

	// Track mock calls
	var lastDataAccessEntity *entities.Entity
	var lastDataAccessIds []string
	var lastKinesisInput *kinesis.PutRecordInput

	// Create mock entity metadata accessor
	mockEntityMetadataAccessor := &mockEntityMetadataAccessor{
		getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
			require.Equal(t, int64(31415), orgId, "Expected orgId to be 31415")
			return entityMap, nil
		},
	}

	// Create mock entity data accessor
	mockEntityDataAccessor := &mockEntityDataAccessor{
		getUnjoinedEntityByIdFunc: func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
			require.Equal(t, int64(31415), orgId, "Expected orgId to be 31415")
			require.Equal(t, entityTypeName, entity.Name(), "Expected entity name to be TestEntity")
			require.Equal(t, "234", recordId, "Expected recordId to be '234'")

			lastDataAccessEntity = entity
			lastDataAccessIds = []string{recordId}

			// Create test data
			arrayType, err := entitytypes.MakeArrayType(entity.Type().AsType())
			require.NoError(t, err)
			val := value.NewUnvalidatedValue(entity.Type().AsType(), map[string]interface{}{
				"id":    "234",
				"orgId": int64(31415),
			})
			return value.NewArrayValue(arrayType, val)
		},
	}

	// Create mock Kinesis client
	mockKinesisClient := &mockKinesisClient{
		putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
			lastKinesisInput = input
			return &kinesis.PutRecordOutput{}, nil
		},
	}

	// Create mock feature evaluator
	mockFeatureEvaluator := featureconfig.NewMockEvaluator()
	featureconfig.MockValues[bool](mockFeatureEvaluator).
		WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

	// Create worker
	worker := &EmrSqsWorker{
		entityMetadataAccessor: mockEntityMetadataAccessor,
		entityDataAccessor:     mockEntityDataAccessor,
		kinesisClient:          mockKinesisClient,
		featureConfigEvaluator: mockFeatureEvaluator,
		config: EmrSqsWorkerConfig{
			EntityName: entityTypeName,
			StreamName: "test-stream",
		},
	}

	// Create test message with timestamp
	message := &sqs.Message{
		Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"TestEntity","timestamp":"2025-05-04T04:04:45Z"}`),
	}

	// Process message
	err = worker.ProcessMessage(context.Background(), message)
	require.NoError(t, err)

	// Verify mock calls
	require.NotNil(t, lastDataAccessEntity, "Entity data accessor should have been called")
	require.Equal(t, entityTypeName, lastDataAccessEntity.Name(), "Expected entity name to be TestEntity")
	require.Equal(t, []string{"234"}, lastDataAccessIds, "Expected recordIds to contain '234'")

	// Verify Kinesis input
	require.NotNil(t, lastKinesisInput, "Kinesis PutRecord should have been called")
	require.Equal(t, "test-stream", *lastKinesisInput.StreamName, "Expected stream name to be test-stream")
	require.NotEmpty(t, lastKinesisInput.Data, "Expected data to be non-empty")
	// Partition key format: {entityName}-{orgId}-{timestampBucket}-{receiveCount}
	// timestampBucket for "2025-05-04T04:04:45Z" with 1-min interval is "2025-05-04T04:04-04:05"
	require.Equal(t, "TestEntity-31415-2025-05-04T04:04-04:05-0", *lastKinesisInput.PartitionKey, "Expected partition key to include timestampBucket")
}

func TestProcessMessage_Success_S3Offloading(t *testing.T) {
	// Create test entity type with additional fields to make it large
	entityTypeName := "TestEntity"
	entityType := &entityproto.Type{
		Name:     entityTypeName,
		DataType: entityproto.Type_Object,
		Fields: map[string]*entityproto.Field{
			"id": {
				Type: &entityproto.Type{DataType: entityproto.Type_String},
			},
			"orgId": {
				Type: &entityproto.Type{DataType: entityproto.Type_Integer},
			},
		},
	}

	// Create test entity
	testEntity := &entityproto.Entity{
		Id:         entityTypeName,
		PrimaryKey: "id",
		Type:       entityType,
	}

	// Create test entity map
	entityMap, rejectedEntities, err := entities.EntityMapFromProtos(entitytypes.EmptyTypeMap, []*entityproto.Entity{testEntity})
	require.NoError(t, err)
	require.Empty(t, rejectedEntities)

	// Track mock calls
	var lastDataAccessEntity *entities.Entity
	var lastDataAccessIds []string
	var lastKinesisInput *kinesis.PutRecordInput
	var s3UploadCalled bool
	var s3UploadInput *s3.PutObjectInput

	// Create mock entity metadata accessor
	mockEntityMetadataAccessor := &mockEntityMetadataAccessor{
		getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
			require.Equal(t, int64(31415), orgId, "Expected orgId to be 31415")
			return entityMap, nil
		},
	}

	// Create mock entity data accessor that returns large data
	mockEntityDataAccessor := &mockEntityDataAccessor{
		getUnjoinedEntityByIdFunc: func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
			require.Equal(t, int64(31415), orgId, "Expected orgId to be 31415")
			require.Equal(t, entityTypeName, entity.Name(), "Expected entity name to be TestEntity")
			require.Equal(t, "234", recordId, "Expected recordId to be '234'")

			lastDataAccessEntity = entity
			lastDataAccessIds = []string{recordId}

			// Create large test data that will exceed 1MB when marshaled
			largeData := make(map[string]interface{})
			largeData["id"] = "234"
			largeData["orgId"] = int64(31415)

			// Add enough data to exceed 1MB (1048576 bytes)
			for i := 0; i < 1200; i++ {
				key := fmt.Sprintf("field_%d", i)
				value := strings.Repeat(fmt.Sprintf("data_%d_", i), 100)
				largeData[key] = value
			}

			arrayType, err := entitytypes.MakeArrayType(entity.Type().AsType())
			require.NoError(t, err)
			val := value.NewUnvalidatedValue(entity.Type().AsType(), largeData)
			return value.NewArrayValue(arrayType, val)
		},
	}

	// Create mock Kinesis client
	mockKinesisClient := &mockKinesisClient{
		putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
			lastKinesisInput = input

			// Verify the record contains S3 path and no EmrResponse
			var record map[string]interface{}
			err := json.Unmarshal(input.Data, &record)
			require.NoError(t, err)

			// Should have S3 path and no inline EMR response
			require.NotEmpty(t, record["s3Path"], "Expected S3 path to be present")
			require.Nil(t, record["emrResponse"], "Expected emrResponse to be nil when data is offloaded")
			require.Contains(t, record["s3Path"], "s3://test-bucket/cdc/large-data/TestEntity/31415/234/", "Expected correct S3 path structure")

			return &kinesis.PutRecordOutput{}, nil
		},
	}

	// Create mock S3 client
	mockS3Client := &mockS3Client{
		putObjectFunc: func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			s3UploadCalled = true
			s3UploadInput = input

			// Verify S3 upload parameters
			require.Equal(t, "test-bucket", *input.Bucket)
			require.Contains(t, *input.Key, "cdc/large-data/TestEntity/31415/234/")
			require.Equal(t, "application/json", *input.ContentType)

			// Verify metadata
			require.Equal(t, "TestEntity", *input.Metadata["entity-name"])
			require.Equal(t, "31415", *input.Metadata["org-id"])
			require.Equal(t, "234", *input.Metadata["record-id"])

			return &s3.PutObjectOutput{}, nil
		},
	}

	// Create mock feature evaluator
	mockFeatureEvaluator := featureconfig.NewMockEvaluator()
	featureconfig.MockValues[bool](mockFeatureEvaluator).
		WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

	// Create worker
	worker := &EmrSqsWorker{
		entityMetadataAccessor: mockEntityMetadataAccessor,
		entityDataAccessor:     mockEntityDataAccessor,
		kinesisClient:          mockKinesisClient,
		s3Client:               mockS3Client,
		featureConfigEvaluator: mockFeatureEvaluator,
		config: EmrSqsWorkerConfig{
			EntityName: entityTypeName,
			StreamName: "test-stream",
			S3Bucket:   "test-bucket",
		},
	}

	// Create test message with timestamp
	message := &sqs.Message{
		Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"TestEntity","timestamp":"2025-06-19T12:00:00Z"}`),
	}

	// Process message
	err = worker.ProcessMessage(context.Background(), message)
	require.NoError(t, err)

	// Verify mock calls
	require.NotNil(t, lastDataAccessEntity, "Entity data accessor should have been called")
	require.Equal(t, entityTypeName, lastDataAccessEntity.Name(), "Expected entity name to be TestEntity")
	require.Equal(t, []string{"234"}, lastDataAccessIds, "Expected recordIds to contain '234'")

	// Verify S3 upload occurred
	require.True(t, s3UploadCalled, "S3 upload should have been called for large record")
	require.NotNil(t, s3UploadInput, "S3 upload input should be captured")

	// Verify Kinesis input
	require.NotNil(t, lastKinesisInput, "Kinesis PutRecord should have been called")
	require.Equal(t, "test-stream", *lastKinesisInput.StreamName, "Expected stream name to be test-stream")
	require.NotEmpty(t, lastKinesisInput.Data, "Expected data to be non-empty")
	// Partition key format: {entityName}-{orgId}-{timestampBucket}-{receiveCount}
	// Message has timestamp "2025-06-19T12:00:00Z", with 1-min interval bucket is "2025-06-19T12:00-12:01"
	require.Equal(t, "TestEntity-31415-2025-06-19T12:00-12:01-0", *lastKinesisInput.PartitionKey, "Expected partition key to include timestampBucket")
}

func TestProcessMessage_Error(t *testing.T) {
	const entityTypeName = "testEntity"

	// Create test entity
	testEntity := &entityproto.Entity{
		Id:         entityTypeName,
		PrimaryKey: "id",
		Type: &entityproto.Type{
			Name:     entityTypeName,
			DataType: entityproto.Type_Object,
			Fields: map[string]*entityproto.Field{
				"id": {
					Type: &entityproto.Type{DataType: entityproto.Type_String},
				},
				"orgId": {
					Type: &entityproto.Type{DataType: entityproto.Type_Integer},
				},
			},
		},
	}

	// Create test data
	typeMap, rejectedTypes, err := entitytypes.TypeMapFromProtos([]*entityproto.Type{testEntity.Type})
	require.NoError(t, err)
	require.Empty(t, rejectedTypes)

	entityMap, rejectedEntities, err := entities.EntityMapFromProtos(typeMap, []*entityproto.Entity{testEntity})
	require.NoError(t, err)
	require.Empty(t, rejectedEntities)

	// Create test array value
	objType := typeMap.Get(entityTypeName)
	require.NotNil(t, objType)

	arrayType, err := entitytypes.MakeArrayType(objType)
	require.NoError(t, err)

	val := value.NewUnvalidatedValue(objType, map[string]interface{}{
		"id":    "234",
		"orgId": int64(31415),
	})
	testData, err := value.NewArrayValue(arrayType, val)
	require.NoError(t, err)

	tests := []struct {
		name          string
		message       *sqs.Message
		setupWorker   func(t *testing.T) (*EmrSqsWorker, *mockCalls)
		expectedError string
		verifyMocks   func(t *testing.T, mc *mockCalls)
	}{
		{
			name: "invalid JSON message",
			message: &sqs.Message{
				Body: aws.String(`invalid json`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

				return &EmrSqsWorker{
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
					},
				}, mc
			},
			expectedError: "failed to unmarshal EMR message",
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				// No mock calls expected for invalid JSON
				require.Nil(t, mc.lastDataAccessEntity)
				require.Nil(t, mc.lastKinesisInput)
			},
		},
		{
			name: "feature flag disabled",
			message: &sqs.Message{
				Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"testEntity"}`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, false)

				return &EmrSqsWorker{
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
					},
				}, mc
			},
			expectedError: "", // No error when feature flag is disabled
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				// No mock calls expected when feature is disabled
				require.Nil(t, mc.lastDataAccessEntity)
				require.Nil(t, mc.lastKinesisInput)
			},
		},
		{
			name: "entity metadata access failure",
			message: &sqs.Message{
				Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"testEntity"}`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

				return &EmrSqsWorker{
					entityMetadataAccessor: &mockEntityMetadataAccessor{
						getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
							require.Equal(t, int64(31415), orgId)
							return nil, errors.New("metadata access error")
						},
					},
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
					},
				}, mc
			},
			expectedError: "failed to get entity map",
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				require.Nil(t, mc.lastDataAccessEntity)
				require.Nil(t, mc.lastKinesisInput)
			},
		},
		{
			name: "entity data access failure",
			message: &sqs.Message{
				Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"testEntity"}`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

				return &EmrSqsWorker{
					entityMetadataAccessor: &mockEntityMetadataAccessor{
						getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
							require.Equal(t, int64(31415), orgId)
							return entityMap, nil
						},
					},
					entityDataAccessor: &mockEntityDataAccessor{
						getUnjoinedEntityByIdFunc: func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
							require.Equal(t, int64(31415), orgId)
							require.Equal(t, "234", recordId)
							mc.lastDataAccessEntity = entity
							mc.lastDataAccessIds = []string{recordId}
							return value.NilValue, errors.New("failed to get unjoined entity by id")
						},
					},
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
					},
				}, mc
			},
			expectedError: "failed to get unjoined entity by id",
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				require.NotNil(t, mc.lastDataAccessEntity)
				require.Equal(t, entityTypeName, mc.lastDataAccessEntity.Name())
				require.Equal(t, []string{"234"}, mc.lastDataAccessIds)
				require.Nil(t, mc.lastKinesisInput)
			},
		},
		{
			name: "kinesis write failure",
			message: &sqs.Message{
				Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"testEntity","timestamp":"2025-05-04T04:08:20Z"}`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

				return &EmrSqsWorker{
					entityMetadataAccessor: &mockEntityMetadataAccessor{
						getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
							require.Equal(t, int64(31415), orgId)
							return entityMap, nil
						},
					},
					entityDataAccessor: &mockEntityDataAccessor{
						getUnjoinedEntityByIdFunc: func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
							require.Equal(t, int64(31415), orgId)
							require.Equal(t, "234", recordId)
							mc.lastDataAccessEntity = entity
							mc.lastDataAccessIds = []string{recordId}
							return testData, nil
						},
					},
					kinesisClient: &mockKinesisClient{
						putRecordFunc: func(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
							mc.lastKinesisInput = input
							return nil, errors.New("kinesis write error")
						},
					},
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
					},
				}, mc
			},
			expectedError: "failed to put record to Kinesis stream",
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				require.NotNil(t, mc.lastDataAccessEntity)
				require.Equal(t, entityTypeName, mc.lastDataAccessEntity.Name())
				require.Equal(t, []string{"234"}, mc.lastDataAccessIds)
				require.NotNil(t, mc.lastKinesisInput)
				require.Equal(t, "test-stream", *mc.lastKinesisInput.StreamName)
				// Partition key format: {entityName}-{orgId}-{timestampBucket}-{receiveCount}
				// timestampBucket for "2025-05-04T04:08:20Z" with 1-min interval is "2025-05-04T04:08-04:09"
				require.Equal(t, "testEntity-31415-2025-05-04T04:08-04:09-0", *mc.lastKinesisInput.PartitionKey)
			},
		},
		{
			name: "S3 upload failure for large record",
			message: &sqs.Message{
				Body: aws.String(`{"recordId":"MjM0","orgId":"31415","entityName":"testEntity","timestamp":"2025-06-19T12:00:00Z"}`),
			},
			setupWorker: func(t *testing.T) (*EmrSqsWorker, *mockCalls) {
				mc := &mockCalls{}
				mockFeatureEvaluator := featureconfig.NewMockEvaluator()
				featureconfig.MockValues[bool](mockFeatureEvaluator).
					WithValue(featureconfigs.Definitions.EnableEmrSqsWorkerQueryEmrData, true)

				// Create large test data that will trigger S3 offloading
				largeDataMap := make(map[string]interface{})
				largeDataMap["id"] = "234"
				largeDataMap["orgId"] = int64(31415)
				// Add enough data to exceed 1MB
				for i := 0; i < 1200; i++ {
					key := fmt.Sprintf("field_%d", i)
					value := strings.Repeat(fmt.Sprintf("data_%d_", i), 100)
					largeDataMap[key] = value
				}

				largeTestData := value.NewUnvalidatedValue(objType, largeDataMap)
				largeArrayData, err := value.NewArrayValue(arrayType, largeTestData)
				require.NoError(t, err)

				return &EmrSqsWorker{
					entityMetadataAccessor: &mockEntityMetadataAccessor{
						getFullEntityMapFunc: func(ctx context.Context, orgId int64) (*entities.EntityMap, error) {
							require.Equal(t, int64(31415), orgId)
							return entityMap, nil
						},
					},
					entityDataAccessor: &mockEntityDataAccessor{
						getUnjoinedEntityByIdFunc: func(ctx context.Context, orgId int64, authzPrincipalId *entityproto.AuthzPrincipalId, entity *entities.Entity, recordId string) (value.Value, error) {
							require.Equal(t, int64(31415), orgId)
							require.Equal(t, "234", recordId)
							mc.lastDataAccessEntity = entity
							mc.lastDataAccessIds = []string{recordId}
							return largeArrayData, nil
						},
					},
					s3Client: &mockS3Client{
						putObjectFunc: func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
							return nil, errors.New("S3 upload failed")
						},
					},
					featureConfigEvaluator: mockFeatureEvaluator,
					config: EmrSqsWorkerConfig{
						EntityName: entityTypeName,
						StreamName: "test-stream",
						S3Bucket:   "test-bucket",
					},
				}, mc
			},
			expectedError: "failed to upload large record to S3",
			verifyMocks: func(t *testing.T, mc *mockCalls) {
				require.NotNil(t, mc.lastDataAccessEntity)
				require.Equal(t, entityTypeName, mc.lastDataAccessEntity.Name())
				require.Equal(t, []string{"234"}, mc.lastDataAccessIds)
				// Kinesis should not be called when S3 upload fails
				require.Nil(t, mc.lastKinesisInput)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker, mc := tt.setupWorker(t)
			err := worker.ProcessMessage(context.Background(), tt.message)

			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			}

			tt.verifyMocks(t, mc)
		})
	}
}

func TestBucketTimestampWithinXMinuteInterval(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		minuteInterval int
		expected       string
	}{
		{
			name:           "12:26:33Z should be 12:25-12:30",
			input:          "2025-10-23T12:26:33Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:25-12:30",
		},
		{
			name:           "12:20:00Z should be 12:20-12:25",
			input:          "2025-10-23T12:20:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:20-12:25",
		},
		{
			name:           "12:25:00Z should be 12:25-12:30",
			input:          "2025-10-23T12:25:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:25-12:30",
		},
		{
			name:           "23:57:00Z should be 23:55-00:00",
			input:          "2025-10-23T23:57:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T23:55-00:00",
		},
		{
			name:           "00:00:00Z should be 00:00-00:05",
			input:          "2025-10-23T00:00:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T00:00-00:05",
		},
		{
			name:           "12:00:00Z should be 12:00-12:05",
			input:          "2025-10-23T12:00:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:00-12:05",
		},
		{
			name:           "12:04:59Z should be 12:00-12:05",
			input:          "2025-10-23T12:04:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:00-12:05",
		},
		{
			name:           "12:05:00Z should be 12:05-12:10",
			input:          "2025-10-23T12:05:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:05-12:10",
		},
		{
			name:           "12:09:59Z should be 12:05-12:10",
			input:          "2025-10-23T12:09:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:05-12:10",
		},
		{
			name:           "12:10:00Z should be 12:10-12:15",
			input:          "2025-10-23T12:10:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:10-12:15",
		},
		{
			name:           "12:14:59Z should be 12:10-12:15",
			input:          "2025-10-23T12:14:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:10-12:15",
		},
		{
			name:           "12:15:00Z should be 12:15-12:20",
			input:          "2025-10-23T12:15:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:15-12:20",
		},
		{
			name:           "12:19:59Z should be 12:15-12:20",
			input:          "2025-10-23T12:19:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:15-12:20",
		},
		{
			name:           "12:20:00Z should be 12:20-12:25",
			input:          "2025-10-23T12:20:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:20-12:25",
		},
		{
			name:           "12:24:59Z should be 12:20-12:25",
			input:          "2025-10-23T12:24:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:20-12:25",
		},
		{
			name:           "12:30:00Z should be 12:30-12:35",
			input:          "2025-10-23T12:30:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:30-12:35",
		},
		{
			name:           "12:34:59Z should be 12:30-12:35",
			input:          "2025-10-23T12:34:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:30-12:35",
		},
		{
			name:           "12:35:00Z should be 12:35-12:40",
			input:          "2025-10-23T12:35:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:35-12:40",
		},
		{
			name:           "12:39:59Z should be 12:35-12:40",
			input:          "2025-10-23T12:39:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:35-12:40",
		},
		{
			name:           "12:40:00Z should be 12:40-12:45",
			input:          "2025-10-23T12:40:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:40-12:45",
		},
		{
			name:           "12:44:59Z should be 12:40-12:45",
			input:          "2025-10-23T12:44:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:40-12:45",
		},
		{
			name:           "12:45:00Z should be 12:45-12:50",
			input:          "2025-10-23T12:45:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:45-12:50",
		},
		{
			name:           "12:49:59Z should be 12:45-12:50",
			input:          "2025-10-23T12:49:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:45-12:50",
		},
		{
			name:           "12:50:00Z should be 12:50-12:55",
			input:          "2025-10-23T12:50:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:50-12:55",
		},
		{
			name:           "12:54:59Z should be 12:50-12:55",
			input:          "2025-10-23T12:54:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:50-12:55",
		},
		{
			name:           "12:55:00Z should be 12:55-13:00",
			input:          "2025-10-23T12:55:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:55-13:00",
		},
		{
			name:           "12:59:59Z should be 12:55-13:00",
			input:          "2025-10-23T12:59:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T12:55-13:00",
		},
		{
			name:           "23:55:00Z should be 23:55-00:00",
			input:          "2025-10-23T23:55:00Z",
			minuteInterval: 5,
			expected:       "2025-10-23T23:55-00:00",
		},
		{
			name:           "23:59:59Z should be 23:55-00:00",
			input:          "2025-10-23T23:59:59Z",
			minuteInterval: 5,
			expected:       "2025-10-23T23:55-00:00",
		},
		{
			name:           "invalid timestamp format should fallback to colon truncation",
			input:          "2025-10-23T12:26:33",
			minuteInterval: 5,
			expected:       "2025-10-23T12",
		},
		{
			name:           "completely invalid timestamp should return as-is",
			input:          "invalid-timestamp",
			minuteInterval: 5,
			expected:       "invalid-timestamp",
		},
		{
			name:           "timestamp without colon should return as-is",
			input:          "2025-10-23T12",
			minuteInterval: 5,
			expected:       "2025-10-23T12",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval := tt.minuteInterval
			if interval == 0 {
				interval = 5 // default to 5 minutes for backward compatibility
			}
			result := bucketTimestampWithinXMinuteInterval(tt.input, interval)
			assert.Equal(t, tt.expected, result, "bucketTimestampWithinXMinuteInterval(%q, %d) = %q, want %q", tt.input, interval, result, tt.expected)
		})
	}
}
