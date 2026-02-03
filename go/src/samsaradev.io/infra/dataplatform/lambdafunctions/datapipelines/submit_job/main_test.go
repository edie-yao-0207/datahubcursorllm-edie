package main

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/service/ssm"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/libs/ni/pointer"
)

func TestParseParameter(t *testing.T) {
	paramString := "ourvalue"
	testCases := map[string]struct {
		Param    *ssm.GetParameterOutput
		Expected string
		IsError  bool
	}{
		"nil object": {
			Param:   nil,
			IsError: true,
		},
		"empty param field": {
			Param:   &ssm.GetParameterOutput{},
			IsError: true,
		},
		"non-nil param but nil value field": {
			Param: &ssm.GetParameterOutput{
				Parameter: &ssm.Parameter{},
			},
			IsError: true,
		},
		"valid param": {
			Param: &ssm.GetParameterOutput{
				Parameter: &ssm.Parameter{
					Value: &paramString,
				},
			},
			Expected: paramString,
		},
	}

	for description, tc := range testCases {
		t.Run(description, func(t *testing.T) {
			output, err := parseParameter(tc.Param)
			if tc.IsError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.Expected, output)
			}
		})
	}
}

func TestHandleSubmit(t *testing.T) {
	controller := gomock.NewController(t)

	mockApiClient := mock_databricks.NewMockAPI(controller)
	ctx := context.TODO()

	// NOTE: this is a realistic example that you can use to actually attempt a run.
	expectedApiInput := databricks.SubmitRunInput{
		NewCluster: &databricks.NewCluster{
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			SparkVersion: sparkversion.SparkVersion122xScala212,
			SparkConf:    nil,
			SparkEnvVars: nil,
			AwsAttributes: &databricks.ClusterAwsAttributes{
				InstanceProfileArn: "arn:aws:iam::492164655156:instance-profile/dataplatform-cluster",
			},
			InitScripts:      nil,
			NodeTypeId:       "i3.4xlarge",
			DriverNodeTypeId: "i3.xlarge",
			ClusterLogConf:   nil,
			InstancePoolId:   "",
			CustomTags: map[string]string{
				"samsara:service":            "databricks-data-pipeline-node-parth_testing_local_lambda",
				"samsara:team":               "dataplatform",
				"samsara:product-group":      "Platform",
				"samsara:rnd-allocation":     "1",
				"samsara:pipeline-node-name": "parth-testing-local-things",
			},
		},
		SparkPythonTask: &databricks.SparkPythonTask{
			PythonFile: "s3://samsara-databricks-workspace/dataplatform/parth/parth_test.py",
		},
	}

	lambdaInput := SubmitJobInput{
		RunInput: databricks.SubmitRunInput{
			NewCluster: &databricks.NewCluster{
				AutoScale: &databricks.ClusterAutoScale{
					MinWorkers: 1,
					MaxWorkers: 16,
				},
				SparkVersion: sparkversion.SparkVersion122xScala212,
				SparkConf:    nil,
				SparkEnvVars: nil,
				AwsAttributes: &databricks.ClusterAwsAttributes{
					InstanceProfileArn: "arn:aws:iam::492164655156:instance-profile/dataplatform-cluster",
				},
				InitScripts:      nil,
				NodeTypeId:       "i3.4xlarge",
				DriverNodeTypeId: "i3.xlarge",
				ClusterLogConf:   nil,
				InstancePoolId:   "",
				CustomTags: map[string]string{
					"samsara:service":            "databricks-data-pipeline-node-parth_testing_local_lambda",
					"samsara:team":               "dataplatform",
					"samsara:product-group":      "Platform",
					"samsara:rnd-allocation":     "1",
					"samsara:pipeline-node-name": "parth-testing-local-things",
				},
			},
			SparkPythonTask: &databricks.SparkPythonTask{
				PythonFile: "s3://samsara-databricks-workspace/dataplatform/parth/parth_test.py",
			},
		},
	}

	// Test the happy path, and assert that we return the job id correctly.
	mockApiClient.EXPECT().SubmitRun(ctx, &expectedApiInput).Return(&databricks.SubmitRunOutput{RunId: 1234}, nil).Times(1)
	output, err := HandleSubmit(ctx, mockApiClient, lambdaInput)
	require.NoError(t, err)
	assert.Equal(t, output.RunId, int64(1234))

	// Test that failures are correctly returned.
	mockApiClient.EXPECT().SubmitRun(ctx, &expectedApiInput).Return(nil, errors.New("hello")).Times(1)
	output, err = HandleSubmit(ctx, mockApiClient, lambdaInput)
	assert.Error(t, err)

	// Test that we correctly fill in the instance pool id.
	// Modify the input to pass pool name, and expect the properly returned ID when submitting the job.
	lambdaInput.InstancePoolName = pointer.StringPtr("mypool")
	mockApiClient.EXPECT().ListInstancePools(ctx).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "wrong pool",
				},
				InstancePoolId: "not_this_id",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "mypool",
				},
				InstancePoolId: "this_id",
			},
		},
	}, nil).Times(1)
	expectedApiInput.NewCluster.InstancePoolId = "this_id"
	mockApiClient.EXPECT().SubmitRun(ctx, &expectedApiInput).Return(&databricks.SubmitRunOutput{RunId: 1234}, nil).Times(1)
	output, err = HandleSubmit(ctx, mockApiClient, lambdaInput)
	require.NoError(t, err)
	assert.Equal(t, output.RunId, int64(1234))

	// Test that we correctly fill in the instance pool id with just a worker.
	// Modify the input to pass pool name, and expect the properly returned ID when submitting the job.
	lambdaInput.InstancePoolName = pointer.StringPtr("wont_be_used_since_deprecated")
	lambdaInput.Pools = &databricks.SubmitJobPools{
		InstancePoolName: "mypool",
	}
	mockApiClient.EXPECT().ListInstancePools(ctx).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "wrong pool",
				},
				InstancePoolId: "not_this_id",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "mypool",
				},
				InstancePoolId: "this_id",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "mypool-driver",
				},
				InstancePoolId: "this_id_driver",
			},
		},
	}, nil).Times(1)
	expectedApiInput.NewCluster.InstancePoolId = "this_id"
	mockApiClient.EXPECT().SubmitRun(ctx, &expectedApiInput).Return(&databricks.SubmitRunOutput{RunId: 1234}, nil).Times(1)
	output, err = HandleSubmit(ctx, mockApiClient, lambdaInput)
	require.NoError(t, err)
	assert.Equal(t, output.RunId, int64(1234))

	// Test that we correctly fill in the instance pool id with a worker/driver.
	// Modify the input to pass pool name, and expect the properly returned ID when submitting the job.
	lambdaInput.InstancePoolName = pointer.StringPtr("wont_be_used_since_deprecated")
	lambdaInput.Pools = &databricks.SubmitJobPools{
		InstancePoolName: "mypool",
		DriverPoolName:   pointer.StringPtr("mypool-driver"),
	}
	mockApiClient.EXPECT().ListInstancePools(ctx).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "wrong pool",
				},
				InstancePoolId: "not_this_id",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "mypool",
				},
				InstancePoolId: "this_id",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "mypool-driver",
				},
				InstancePoolId: "this_id_driver",
			},
		},
	}, nil).Times(2)
	expectedApiInput.NewCluster.InstancePoolId = "this_id"
	expectedApiInput.NewCluster.DriverInstancePoolId = "this_id_driver"
	mockApiClient.EXPECT().SubmitRun(ctx, &expectedApiInput).Return(&databricks.SubmitRunOutput{RunId: 1234}, nil).Times(1)
	output, err = HandleSubmit(ctx, mockApiClient, lambdaInput)
	require.NoError(t, err)
	assert.Equal(t, output.RunId, int64(1234))
}

func TestSetPermissions(t *testing.T) {
	controller := gomock.NewController(t)

	mockApiClient := mock_databricks.NewMockAPI(controller)
	ctx := context.TODO()

	expectedRunInput := &databricks.GetRunInput{RunId: 10}

	expectedPermissionsInput := &databricks.PatchPermissionsInput{
		ObjectType: databricks.ObjectTypeJob,
		ObjectId:   "20",
		AccessControlList: []*databricks.AccessControlRequest{
			{
				AccessControlPrincipal: databricks.AccessControlPrincipal{GroupName: "samsara-users-group"},
				PermissionLevel:        databricks.PermissionLevelCanView,
			},
			{
				AccessControlPrincipal: databricks.AccessControlPrincipal{GroupName: "data-platform-group"},
				PermissionLevel:        databricks.PermissionLevelCanManage,
			},
		},
	}

	// Test the happy path, and assert that we return the job id correctly.
	mockApiClient.EXPECT().GetRun(ctx, expectedRunInput).Return(&databricks.GetRunOutput{Run: databricks.Run{
		JobId: 20,
		RunId: 10,
	}}, nil).Times(1)

	mockApiClient.EXPECT().PatchPermissions(ctx, expectedPermissionsInput).Return(&databricks.PatchPermissionsOutput{}, nil).Times(1)
	err := SetPermissions(ctx, mockApiClient, 10)
	assert.NoError(t, err)

	// Test that invalid job response is errored correctly.
	mockApiClient.EXPECT().GetRun(ctx, expectedRunInput).Return(&databricks.GetRunOutput{Run: databricks.Run{
		JobId: 0,
		RunId: 10,
	}}, nil).Times(1)
	err = SetPermissions(ctx, mockApiClient, 10)
	assert.Error(t, err)

	// Test that errored job response is an error.
	mockApiClient.EXPECT().GetRun(ctx, expectedRunInput).Return(&databricks.GetRunOutput{}, errors.New("error")).Times(1)
	err = SetPermissions(ctx, mockApiClient, 10)
	assert.Error(t, err)

	// Test that errored permissions is an error.
	mockApiClient.EXPECT().GetRun(ctx, expectedRunInput).Return(&databricks.GetRunOutput{Run: databricks.Run{
		JobId: 20,
		RunId: 10,
	}}, nil).Times(1)
	mockApiClient.EXPECT().PatchPermissions(ctx, expectedPermissionsInput).Return(&databricks.PatchPermissionsOutput{}, errors.New("error")).Times(1)
	err = SetPermissions(ctx, mockApiClient, 10)
	assert.Error(t, err)
}
