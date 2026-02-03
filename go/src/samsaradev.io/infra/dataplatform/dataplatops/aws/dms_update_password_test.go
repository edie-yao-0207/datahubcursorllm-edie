package aws

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockdms "samsaradev.io/vendormocks/mock_databasemigrationserviceiface"
)

type fakeSecretReader struct {
	val   string
	err   error
	calls int
	keys  []string
}

func (f *fakeSecretReader) Read(ctx context.Context, key string) (string, error) {
	f.calls++
	f.keys = append(f.keys, key)
	return f.val, f.err
}

type fakeShardResolver struct {
	shards []string
	err    error
}

func (f *fakeShardResolver) ShardsForDB(ctx context.Context, registryName, region string) ([]string, error) {
	_ = ctx
	_ = registryName
	_ = region
	return f.shards, f.err
}

func TestDMSUpdatePasswordOp_Validate_DerivesRegistryName_And_SecretKey(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "all", "all", false)
	op.dmsClient = mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	op.secretReader = &fakeSecretReader{val: "dontcare"}
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleetdb"}}

	err := op.Validate(ctx)
	require.NoError(t, err)
	assert.Equal(t, "eurofleet", op.registryName)
	assert.Equal(t, "eurofleet_database_password", op.secretKey)
	assert.Equal(t, "all", op.normalizedTaskType)
}

func TestDMSUpdatePasswordOp_Validate_Clouddb_SecretKey(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	op := NewDMSUpdatePasswordOp("us-west-2", "clouddb", "all", "all", false)
	op.dmsClient = mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	op.secretReader = &fakeSecretReader{val: "dontcare"}
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-clouddb"}}

	err := op.Validate(ctx)
	require.NoError(t, err)
	assert.Equal(t, "clouddb", op.registryName)
	assert.Equal(t, "database_password", op.secretKey)
}

func TestDMSUpdatePasswordOp_Plan_ResolvesShards_And_Tasks_ByTaskType(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "pw"}

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "prod,2", "kinesis", false)
	op.dmsClient = dms
	op.secretReader = sec
	op.shardResolver = &fakeShardResolver{shards: []string{
		"prod-eurofleetdb",
		"prod-eurofleet-shard-1db",
		"prod-eurofleet-shard-2db",
	}}

	require.NoError(t, op.Validate(ctx))

	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.DescribeReplicationTasksInput, _ ...request.Option) (*databasemigrationservice.DescribeReplicationTasksOutput, error) {
			require.Len(t, in.Filters, 1)
			taskID := awsV1StringValue(in.Filters[0].Values[0])
			// Should be called for the selected shards: prod + shard-2
			require.True(t, taskID == "kinesis-task-cdc-prod-eurofleetdb" || taskID == "kinesis-task-cdc-prod-eurofleet-shard-2db")
			arnSuffix := "prod"
			endpointArn := "arn:endpoint-prod"
			if strings.Contains(taskID, "shard-2db") {
				arnSuffix = "shard2"
				endpointArn = "arn:endpoint-shard2"
			}
			return &databasemigrationservice.DescribeReplicationTasksOutput{
				ReplicationTasks: []*databasemigrationservice.ReplicationTask{
					{
						ReplicationTaskArn:     awsV1StringPtr("arn:task:" + arnSuffix),
						Status:                 awsV1StringPtr("stopped"),
						ReplicationInstanceArn: awsV1StringPtr("arn:ri:" + arnSuffix),
						SourceEndpointArn:      awsV1StringPtr(endpointArn),
					},
				},
			}, nil
		}).
		Times(2)

	dms.EXPECT().
		DescribeEndpointsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.DescribeEndpointsInput, _ ...request.Option) (*databasemigrationservice.DescribeEndpointsOutput, error) {
			require.Len(t, in.Filters, 1)
			endpointArn := awsV1StringValue(in.Filters[0].Values[0])
			return &databasemigrationservice.DescribeEndpointsOutput{
				Endpoints: []*databasemigrationservice.Endpoint{
					{
						EndpointArn:        awsV1StringPtr(endpointArn),
						EndpointIdentifier: awsV1StringPtr("id-" + endpointArn),
						EndpointType:       awsV1StringPtr("source"),
					},
				},
			}, nil
		}).
		AnyTimes()

	err := op.Plan(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"prod-eurofleetdb", "prod-eurofleet-shard-2db"}, op.selectedShards)
	require.Len(t, op.taskPlans, 2)
	require.Len(t, op.endpointPlansByArn, 2)
}

func TestDMSUpdatePasswordOp_Plan_InvalidShardErrors(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "4", "kinesis", false)
	op.dmsClient = mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	op.secretReader = &fakeSecretReader{val: "pw"}
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleet-shard-1db", "prod-eurofleet-shard-2db"}}

	require.NoError(t, op.Validate(ctx))
	err := op.Plan(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such shard 4")
}

func TestDMSUpdatePasswordOp_Plan_WithoutValidate_DoesNotPanic(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "all", "all", false)
	op.dmsClient = mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	op.secretReader = &fakeSecretReader{val: "pw"}
	// Intentionally do not set shardResolver and do not call Validate().

	require.NotPanics(t, func() {
		err := op.Plan(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Validate() must be called")
	})
}

func TestDMSUpdatePasswordOp_Execute_ShardTargeted_RunningTask_SkipsUpdate(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "new-password"}

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "1", "kinesis", false)
	op.dmsClient = dms
	op.secretReader = sec
	// Include an additional shard so that selector "1" is a strict subset (and triggers skip logic).
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleetdb", "prod-eurofleet-shard-1db"}}

	require.NoError(t, op.Validate(ctx))

	// Plan resolves a running task and marks it willSkipUpdate, resulting in no endpoints to update.
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeReplicationTasksOutput{
			ReplicationTasks: []*databasemigrationservice.ReplicationTask{
				{
					ReplicationTaskArn:     awsV1StringPtr("arn:task"),
					Status:                 awsV1StringPtr("running"),
					ReplicationInstanceArn: awsV1StringPtr("arn:ri"),
					SourceEndpointArn:      awsV1StringPtr("arn:endpoint"),
				},
			},
		}, nil).
		Times(1)

	require.NoError(t, op.Plan(ctx))
	require.Len(t, op.endpointPlansByArn, 0)

	res, err := op.Execute(ctx)
	require.NoError(t, err)
	// Execute should return without reading the secret if there's nothing to update.
	assert.Equal(t, 0, sec.calls)
	out := res.(*DMSUpdatePasswordResult)
	assert.Equal(t, 0, out.UpdatedEndpoints)
}

func TestDMSUpdatePasswordOp_Execute_ConnectionAlreadyBeingTested_WaitsForResult(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "new-password"}

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "1", "kinesis", false)
	op.PollInterval = 1 * time.Millisecond
	op.ConnTimeout = 50 * time.Millisecond
	op.dmsClient = dms
	op.secretReader = sec
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleet-shard-1db"}}

	require.NoError(t, op.Validate(ctx))

	// Plan expectations
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeReplicationTasksOutput{
			ReplicationTasks: []*databasemigrationservice.ReplicationTask{
				{
					ReplicationTaskArn:     awsV1StringPtr("arn:task"),
					Status:                 awsV1StringPtr("ready"),
					ReplicationInstanceArn: awsV1StringPtr("arn:ri"),
					SourceEndpointArn:      awsV1StringPtr("arn:endpoint"),
				},
			},
		}, nil).
		Times(1)

	dms.EXPECT().
		DescribeEndpointsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeEndpointsOutput{
			Endpoints: []*databasemigrationservice.Endpoint{
				{
					EndpointArn:        awsV1StringPtr("arn:endpoint"),
					EndpointIdentifier: awsV1StringPtr("rds-kinesis-prod-eurofleet-shard-1db"),
					EndpointType:       awsV1StringPtr("source"),
				},
			},
		}, nil).
		Times(1)

	require.NoError(t, op.Plan(ctx))

	// Execute expectations
	dms.EXPECT().
		ModifyEndpointWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.ModifyEndpointOutput{}, nil).
		Times(1)

	// Simulate "already being tested" from DMS.
	dms.EXPECT().
		TestConnectionWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, awserr.New(databasemigrationservice.ErrCodeInvalidResourceStateFault, "Connection is already being tested", nil)).
		Times(1)

	// Poll shows eventual success.
	dms.EXPECT().
		DescribeConnectionsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeConnectionsOutput{
			Connections: []*databasemigrationservice.Connection{
				{Status: awsV1StringPtr("successful")},
			},
		}, nil).
		AnyTimes()

	_, err := op.Execute(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, sec.calls)
	assert.Equal(t, []string{"eurofleet_database_password"}, sec.keys)
}

func TestDMSUpdatePasswordOp_Plan_ShardsExplicitAll_SkipsRunningTasks(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "pw"}

	op := NewDMSUpdatePasswordOp("us-west-2", "eurofleetdb", "prod,1", "kinesis", false)
	op.dmsClient = dms
	op.secretReader = sec
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleetdb", "prod-eurofleet-shard-1db"}}

	require.NoError(t, op.Validate(ctx))

	// Both tasks are running; we should always skip password updates for running/starting tasks.
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.DescribeReplicationTasksInput, _ ...request.Option) (*databasemigrationservice.DescribeReplicationTasksOutput, error) {
			taskID := awsV1StringValue(in.Filters[0].Values[0])
			endpointArn := "arn:endpoint-prod"
			arnSuffix := "prod"
			if strings.Contains(taskID, "shard-1db") {
				endpointArn = "arn:endpoint-shard1"
				arnSuffix = "shard1"
			}
			return &databasemigrationservice.DescribeReplicationTasksOutput{
				ReplicationTasks: []*databasemigrationservice.ReplicationTask{
					{
						ReplicationTaskArn:     awsV1StringPtr("arn:task:" + arnSuffix),
						Status:                 awsV1StringPtr("running"),
						ReplicationInstanceArn: awsV1StringPtr("arn:ri:" + arnSuffix),
						SourceEndpointArn:      awsV1StringPtr(endpointArn),
					},
				},
			}, nil
		}).
		Times(2)

	require.NoError(t, op.Plan(ctx))
	require.Len(t, op.taskPlans, 2)
	for _, tp := range op.taskPlans {
		assert.True(t, tp.willSkipUpdate)
	}
	require.Len(t, op.endpointPlansByArn, 0)
}

func TestDMSUpdatePasswordOp_Execute_ContinuesOnModifyEndpointFailure(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "new-password"}

	op := NewDMSUpdatePasswordOp("us-west-2", "eurofleetdb", "prod,1", "kinesis", false)
	op.PollInterval = 1 * time.Millisecond
	op.ConnTimeout = 50 * time.Millisecond
	op.dmsClient = dms
	op.secretReader = sec
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleetdb", "prod-eurofleet-shard-1db"}}

	require.NoError(t, op.Validate(ctx))

	// Plan: two tasks, two endpoints.
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.DescribeReplicationTasksInput, _ ...request.Option) (*databasemigrationservice.DescribeReplicationTasksOutput, error) {
			taskID := awsV1StringValue(in.Filters[0].Values[0])
			endpointArn := "arn:endpoint-prod"
			arnSuffix := "prod"
			if strings.Contains(taskID, "shard-1db") {
				endpointArn = "arn:endpoint-shard1"
				arnSuffix = "shard1"
			}
			return &databasemigrationservice.DescribeReplicationTasksOutput{
				ReplicationTasks: []*databasemigrationservice.ReplicationTask{
					{
						ReplicationTaskArn:     awsV1StringPtr("arn:task:" + arnSuffix),
						Status:                 awsV1StringPtr("stopped"),
						ReplicationInstanceArn: awsV1StringPtr("arn:ri:" + arnSuffix),
						SourceEndpointArn:      awsV1StringPtr(endpointArn),
					},
				},
			}, nil
		}).
		Times(2)

	dms.EXPECT().
		DescribeEndpointsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.DescribeEndpointsInput, _ ...request.Option) (*databasemigrationservice.DescribeEndpointsOutput, error) {
			endpointArn := awsV1StringValue(in.Filters[0].Values[0])
			return &databasemigrationservice.DescribeEndpointsOutput{
				Endpoints: []*databasemigrationservice.Endpoint{
					{
						EndpointArn:        awsV1StringPtr(endpointArn),
						EndpointIdentifier: awsV1StringPtr("id-" + endpointArn),
						EndpointType:       awsV1StringPtr("source"),
					},
				},
			}, nil
		}).
		AnyTimes()

	require.NoError(t, op.Plan(ctx))

	// Execute: first endpoint fails, second succeeds (order is map iteration; accept either).
	dms.EXPECT().
		ModifyEndpointWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.ModifyEndpointInput, _ ...request.Option) (*databasemigrationservice.ModifyEndpointOutput, error) {
			arn := aws.StringValue(in.EndpointArn)
			if arn == "arn:endpoint-prod" {
				return nil, awserr.New("AccessDeniedException", "denied", nil)
			}
			return &databasemigrationservice.ModifyEndpointOutput{}, nil
		}).
		AnyTimes()

	// Only successful endpoint should be tested.
	dms.EXPECT().
		TestConnectionWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.TestConnectionOutput{}, nil).
		AnyTimes()

	dms.EXPECT().
		DescribeConnectionsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeConnectionsOutput{
			Connections: []*databasemigrationservice.Connection{
				{Status: aws.String("successful")},
			},
		}, nil).
		AnyTimes()

	_, err := op.Execute(ctx)
	require.Error(t, err) // should return failure at the end
	require.Equal(t, 1, sec.calls)
}

func TestDMSUpdatePasswordOp_Execute_ResumeFailure_CountsAsTaskFailure(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	sec := &fakeSecretReader{val: "new-password"}

	op := NewDMSUpdatePasswordOp("eu-west-1", "eurofleetdb", "1", "kinesis", true)
	op.PollInterval = 1 * time.Millisecond
	op.ConnTimeout = 50 * time.Millisecond
	op.dmsClient = dms
	op.secretReader = sec
	op.shardResolver = &fakeShardResolver{shards: []string{"prod-eurofleet-shard-1db"}}

	require.NoError(t, op.Validate(ctx))

	// Plan: one task, one endpoint.
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeReplicationTasksOutput{
			ReplicationTasks: []*databasemigrationservice.ReplicationTask{
				{
					ReplicationTaskArn:     awsV1StringPtr("arn:task"),
					Status:                 awsV1StringPtr("stopped"),
					ReplicationInstanceArn: awsV1StringPtr("arn:ri"),
					SourceEndpointArn:      awsV1StringPtr("arn:endpoint"),
				},
			},
		}, nil).
		Times(1)

	dms.EXPECT().
		DescribeEndpointsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeEndpointsOutput{
			Endpoints: []*databasemigrationservice.Endpoint{
				{
					EndpointArn:        awsV1StringPtr("arn:endpoint"),
					EndpointIdentifier: awsV1StringPtr("rds-kinesis-prod-eurofleet-shard-1db"),
					EndpointType:       awsV1StringPtr("source"),
				},
			},
		}, nil).
		Times(1)

	require.NoError(t, op.Plan(ctx))

	// Execute: update endpoint + connection succeeds.
	dms.EXPECT().
		ModifyEndpointWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.ModifyEndpointOutput{}, nil).
		Times(1)

	dms.EXPECT().
		TestConnectionWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.TestConnectionOutput{}, nil).
		Times(1)

	dms.EXPECT().
		DescribeConnectionsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeConnectionsOutput{
			Connections: []*databasemigrationservice.Connection{
				{Status: aws.String("successful")},
			},
		}, nil).
		AnyTimes()

	// Resume: status indicates not running, start fails.
	dms.EXPECT().
		DescribeReplicationTasksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.DescribeReplicationTasksOutput{
			ReplicationTasks: []*databasemigrationservice.ReplicationTask{
				{Status: awsV1StringPtr("stopped")},
			},
		}, nil).
		Times(1)

	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, awserr.New("AccessDeniedException", "denied", nil)).
		Times(1)

	res, err := op.Execute(ctx)
	require.Error(t, err)
	require.Equal(t, 1, sec.calls)
	out := res.(*DMSUpdatePasswordResult)
	assert.Equal(t, 0, out.FailedEndpoints)
	assert.Equal(t, 1, out.FailedTasks)
}

// Tiny helpers to avoid importing aws-sdk v1 helper packages in tests.
func awsV1StringPtr(s string) *string { return &s }
func awsV1Int64Ptr(i int64) *int64    { return &i }
func awsV1StringValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func awsV1BoolValue(p *bool) bool {
	if p == nil {
		return false
	}
	return *p
}
