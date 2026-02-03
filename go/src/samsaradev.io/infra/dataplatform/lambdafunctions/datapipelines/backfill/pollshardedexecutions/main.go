/*
This lambda calls the SFN api in a loop to poll for the status of each sharded execution
The logic is as follows:

1. Call sfn describe api endpoint on each sharded execution that was started
2. If any shard is in a RUNNING state, return RUNNING from the lambda
3. If there is a singular FAILED, ABORTED, or TIMED_OUT state, return FAILED
4. Otherwise, return SUCCESS
*/

package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/aws/aws-sdk-go/service/sfn/sfniface"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

var sfnClient = sfn.New(session.New(util.RetryerConfig))

var dbxFailureStatuses = map[string]struct{}{
	"FAILED":   {},
	"TIMEDOUT": {},
	"CANCELED": {},
}

var sfnFailureStatuses = map[string]struct{}{
	"FAILED":    {},
	"TIMED_OUT": {},
	"ABORTED":   {},
}

type DescribeShardedExecutionsInput struct {
	StartExecutionsResult StartExecutionsOutput `json:"start_executions_result"`
}

type StartExecutionsOutput struct {
	ExecutionArns []string `json:"execution_arns"`
}

type DescribeShardedExecutionsOutput struct {
	ResultState       string   `json:"result_state"`
	RunPageUrls       []string `json:"run_page_urls"`
	FailedRunPageUrls []string `json:"failed_run_page_urls"`
}

func describeShardedExecutions(ctx context.Context, sfnApi sfniface.SFNAPI, input DescribeShardedExecutionsInput) (DescribeShardedExecutionsOutput, error) {
	var describeOutputDetails struct {
		RunPageUrl  string `json:"run_page_url"`
		ResultState string `json:"result_state"`
	}

	failed := false
	runPageUrls := []string{}
	failedRunPageUrls := []string{}
	for _, executionArn := range input.StartExecutionsResult.ExecutionArns {
		describeOutput, err := sfnApi.DescribeExecutionWithContext(ctx, &sfn.DescribeExecutionInput{
			ExecutionArn: aws.String(executionArn),
		})
		if err != nil {
			return DescribeShardedExecutionsOutput{}, err
		}

		// First look at sql-transformation-backfill-node SFN status
		execStatus := aws.StringValue(describeOutput.Status)
		// If SFN is still running, return running result state
		if execStatus == "RUNNING" {
			return DescribeShardedExecutionsOutput{ResultState: execStatus}, nil
		}

		// If SFN has failed, it means there was some kind of infra error and thus there is no output
		// Since result state won't be readable, just return failed here
		if _, ok := sfnFailureStatuses[execStatus]; ok {
			return DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"Step function failed"}, FailedRunPageUrls: []string{"Step function failed"}}, nil
		}

		// Otherwise, the SFN has succeeded, so look at the job result_state
		if err := json.Unmarshal([]byte(aws.StringValue(describeOutput.Output)), &describeOutputDetails); err != nil {
			return DescribeShardedExecutionsOutput{}, err
		}

		dbxJobExecStatus := describeOutputDetails.ResultState
		// If dbx job is still running, return running result state
		if dbxJobExecStatus == "RUNNING" {
			return DescribeShardedExecutionsOutput{ResultState: dbxJobExecStatus}, nil
		}

		runPageUrls = append(runPageUrls, describeOutputDetails.RunPageUrl)

		if _, ok := dbxFailureStatuses[dbxJobExecStatus]; ok {
			failedRunPageUrls = append(failedRunPageUrls, describeOutputDetails.RunPageUrl)
			failed = true
		}
	}

	finalOutput := DescribeShardedExecutionsOutput{ResultState: "SUCCESS", RunPageUrls: runPageUrls}
	if failed {
		finalOutput.ResultState = "FAILED"
	}

	if len(failedRunPageUrls) > 0 {
		finalOutput.FailedRunPageUrls = failedRunPageUrls
	} else {
		finalOutput.FailedRunPageUrls = []string{"No failed jobs!"}
	}

	return finalOutput, nil
}

func HandleRequest(ctx context.Context, input DescribeShardedExecutionsInput) (DescribeShardedExecutionsOutput, error) {
	return describeShardedExecutions(ctx, sfnClient, input)
}

func main() {
	middleware.StartWrapped(HandleRequest, middleware.LogInputOutput)
}
