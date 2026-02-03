/*
submit_job is a lambda function that submits a job to databricks.

	input: {
		runInput: <> // JSON object based on the RunInput struct, please see that for more details.
	}

	output: {
		run_id: 1234 // run ID of the databricks run
	}

Important Notes:
  - When submitting a job that creates a new cluster, please ensure you have all the required custom tags.
    See: samsaradev.io/infra/dataplatform/terraform/dataplatformresource/workspace.go#L326
*/
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

var ssmClient = ssm.New(session.New(util.RetryerConfig))

type SubmitJobInput struct {
	RunInput databricks.SubmitRunInput

	// This field is deprecated, please use Pools
	InstancePoolName *string

	// If Pools is specified, the submitted job will run on the instance pool with this worker/driver name.
	// Users should still set everything else except the InstancePoolId/DriverInstancePoolId on RunInput.NewCluster.
	// This field exists because it is hard to know the instance pool id without querying the databricks
	// API, and specifically pipeline nodes do not have access to the instance pool ID when they are created.
	Pools *databricks.SubmitJobPools
}

type SubmitJobOutput struct {
	RunId int64 `json:"run_id"`
}

const (
	TOKEN                       = "E2_DATABRICKS_DATAPIPELINES_TOKEN"
	SERVICE_PRINCIPAL_PAT_TOKEN = "DATABRICKS_DATAPIPELINES_SERVICE_PRINCIPAL_PAT_TOKEN"
	HOST                        = "E2_DATABRICKS_HOST"
)

func parseParameter(output *ssm.GetParameterOutput) (string, error) {
	if output == nil {
		return "", errors.New("output object for was nil")
	}

	param := output.Parameter
	if param == nil {
		return "", errors.New("parameter field for was nil")
	}

	value := param.Value
	if value == nil {
		return "", errors.New("value field for was nil")
	}

	return *value, nil
}

func GetHostAndToken(ctx context.Context, ssmClient *ssm.SSM) (string, string, error) {
	// Get token and host from AWS SSM
	hostParamName := HOST
	hostOutput, err := ssmClient.GetParameter(&ssm.GetParameterInput{
		Name:           &hostParamName,
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "", "", fmt.Errorf("could not find host token, %v", err)
	}

	host, err := parseParameter(hostOutput)
	if err != nil {
		return "", "", fmt.Errorf("failure while trying to parse host parameter from ssm client, %v", err)
	}

	tokenParamName := TOKEN
	tokenOutput, err := ssmClient.GetParameter(&ssm.GetParameterInput{
		Name:           &tokenParamName,
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "", "", fmt.Errorf("could not fetch token from ssmclient, %v", err)
	}

	token, err := parseParameter(tokenOutput)
	if err != nil {
		return "", "", fmt.Errorf("failure while trying to parse token parameter from ssm client, %v", err)
	}

	return host, token, nil
}

func GetInstancePoolId(ctx context.Context, client databricks.API, instancePoolName string) (string, error) {
	output, err := client.ListInstancePools(ctx)
	if err != nil || output.InstancePools == nil {
		return "", fmt.Errorf("Could not fetch instance pools from databricks API. %v\n", err)
	}

	for _, pool := range output.InstancePools {
		if pool.InstancePoolName == instancePoolName {
			return pool.InstancePoolId, nil
		}
	}

	return "", fmt.Errorf("Could not find instance pool with name %s\n", instancePoolName)
}

func HandleSubmit(ctx context.Context, client databricks.API, input SubmitJobInput) (SubmitJobOutput, error) {
	// Fill in instance pool id if necessary.
	if input.Pools != nil {
		workerId, err := GetInstancePoolId(ctx, client, input.Pools.InstancePoolName)
		if err != nil {
			return SubmitJobOutput{}, fmt.Errorf("Couldn't get an instance pool id for %s, %v\n", input.Pools.InstancePoolName, err)
		}
		input.RunInput.NewCluster.InstancePoolId = workerId

		if input.Pools.DriverPoolName != nil {
			driverId, err := GetInstancePoolId(ctx, client, *input.Pools.DriverPoolName)
			if err != nil {
				return SubmitJobOutput{}, fmt.Errorf("Couldn't get an instance pool id for %s, %v\n", *input.Pools.DriverPoolName, err)
			}
			input.RunInput.NewCluster.DriverInstancePoolId = driverId
		}
	} else if input.InstancePoolName != nil {
		id, err := GetInstancePoolId(ctx, client, *input.InstancePoolName)
		if err != nil {
			return SubmitJobOutput{}, fmt.Errorf("Couldn't get an instance pool id for %s, %v\n", *input.InstancePoolName, err)
		}
		input.RunInput.NewCluster.InstancePoolId = id
	}

	output, err := client.SubmitRun(ctx, &input.RunInput)
	if err != nil {
		return SubmitJobOutput{}, fmt.Errorf("failure submitting to databricks, %v", err)
	}

	return SubmitJobOutput{RunId: output.RunId}, nil
}

func SetPermissions(ctx context.Context, client databricks.API, runId int64) error {
	// Get job id from run id
	run, err := client.GetRun(ctx, &databricks.GetRunInput{RunId: runId})
	if err != nil {
		return fmt.Errorf("failed to get run %d, err %v\n", runId, err)
	}
	if run.JobId == 0 {
		return fmt.Errorf("empty job id for run %d, %v\n", runId, run)
	}

	_, err = client.PatchPermissions(ctx, &databricks.PatchPermissionsInput{
		ObjectType: databricks.ObjectTypeJob,
		ObjectId:   fmt.Sprintf("%d", run.JobId),
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
	})
	return err
}

func GetE2ServicePrincipalPatToken(ctx context.Context, ssmClient *ssm.SSM) (string, error) {
	tokenParamName := SERVICE_PRINCIPAL_PAT_TOKEN
	tokenOutput, err := ssmClient.GetParameter(&ssm.GetParameterInput{
		Name:           &tokenParamName,
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "", fmt.Errorf("could not fetch token from ssmclient, %v", err)
	}

	token, err := parseParameter(tokenOutput)
	if err != nil {
		return "", fmt.Errorf("failure while trying to parse sp token parameter from ssm client, %v", err)
	}

	return token, nil
}

// TODO:
// 1. Implement a retry policy.
// 2. Add test for SSM fetch after we get the ssmiface working
func LambdaFunction(ctx context.Context, input SubmitJobInput) (SubmitJobOutput, error) {
	host, token, err := GetHostAndToken(ctx, ssmClient)
	if err != nil {
		return SubmitJobOutput{}, fmt.Errorf("could not fetch host and token from ssm %v\n", err)
	}

	// Get the service principal token from SSM.
	token, err = GetE2ServicePrincipalPatToken(ctx, ssmClient)
	if err != nil {
		return SubmitJobOutput{}, fmt.Errorf("could not fetch service principal token from ssmclient, %v", err)
	}

	// Build databricks client
	client, err := databricks.New(host, token)
	if err != nil {
		return SubmitJobOutput{}, fmt.Errorf("could not construct databricks client, %v", err)
	}

	// Kick off job, returning whatever error if it exists
	submitJobOutput, err := HandleSubmit(ctx, client, input)
	if err != nil {
		return submitJobOutput, fmt.Errorf("failed to submit job to databricks %v\n", err)
	}

	// After kicking off the job, we need to modify the permissions on the logs.
	err = SetPermissions(ctx, client, submitJobOutput.RunId)
	if err != nil {
		fmt.Printf("Failed to set permissions on job %d, received error %v\n. Not failing task, and just continuing...", submitJobOutput.RunId, err)
	}

	return submitJobOutput, nil
}

func main() {
	middleware.StartWrapped(LambdaFunction, middleware.LogInputOutput)
}
