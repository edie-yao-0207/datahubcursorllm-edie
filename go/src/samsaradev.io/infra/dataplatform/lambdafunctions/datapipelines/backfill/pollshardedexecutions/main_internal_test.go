package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/vendormocks/mock_sfniface"
)

func TestDescribeShardedExecutionsSingleShard(t *testing.T) {
	testCases := map[string]struct {
		apiResult      sfn.DescribeExecutionOutput
		pollerInput    DescribeShardedExecutionsInput
		expectedResult DescribeShardedExecutionsOutput
	}{
		"testSingleShardSuccess": {
			apiResult:      sfn.DescribeExecutionOutput{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "SUCCESS"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "SUCCESS", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"No failed jobs!"}},
		},
		"testSingleShardFailed": {
			apiResult:      sfn.DescribeExecutionOutput{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "FAILED"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"https://dbx.com/1"}},
		},
		"testSingleShardCanceled": {
			apiResult:      sfn.DescribeExecutionOutput{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "CANCELED"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"https://dbx.com/1"}},
		},
		"testSingleShardTimedOut": {
			apiResult:      sfn.DescribeExecutionOutput{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "TIMEDOUT"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"https://dbx.com/1"}},
		},
		"testSingleShardRunning": {
			apiResult:      sfn.DescribeExecutionOutput{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "RUNNING"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "RUNNING"},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSFN := mock_sfniface.NewMockSFNAPI(gomock.NewController(t))
			for _, execArn := range testCase.pollerInput.StartExecutionsResult.ExecutionArns {
				mockSFN.EXPECT().DescribeExecutionWithContext(gomock.Any(), &sfn.DescribeExecutionInput{
					ExecutionArn: aws.String(execArn),
				}).Return(&testCase.apiResult, nil).Times(1)
			}
			mockSFN.EXPECT().DescribeExecutionWithContext(gomock.Any(), &testCase.pollerInput).Return(&testCase.apiResult, nil).MaxTimes(1)
			pollResult, err := describeShardedExecutions(nil, mockSFN, testCase.pollerInput)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedResult, pollResult)
		})
	}
}

func TestDescribeShardedExecutionsMultipleShards(t *testing.T) {
	testCases := map[string]struct {
		apiResult      []sfn.DescribeExecutionOutput
		pollerInput    DescribeShardedExecutionsInput
		expectedResult DescribeShardedExecutionsOutput
	}{
		"testMultipleShardsSuccess": {
			apiResult: []sfn.DescribeExecutionOutput{
				{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "SUCCESS"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/2", "result_state": "SUCCESS"}`)},
			},
			pollerInput: DescribeShardedExecutionsInput{
				StartExecutionsOutput{
					[]string{
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:2",
					},
				},
			},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "SUCCESS", RunPageUrls: []string{"https://dbx.com/1", "https://dbx.com/2"}, FailedRunPageUrls: []string{"No failed jobs!"}},
		},
		"testMultipleShardsFailed": {
			apiResult: []sfn.DescribeExecutionOutput{
				{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "FAILED"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/2", "result_state": "FAILED"}`)},
			},
			pollerInput: DescribeShardedExecutionsInput{
				StartExecutionsOutput{
					[]string{
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:2",
					},
				},
			},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1", "https://dbx.com/2"}, FailedRunPageUrls: []string{"https://dbx.com/1", "https://dbx.com/2"}},
		},
		"testMultipleShardsGrabBagFailed": {
			apiResult: []sfn.DescribeExecutionOutput{
				{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "FAILED"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/2", "result_state": "SUCCESS"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/3", "result_state": "TIMEDOUT"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/4", "result_state": "CANCELED"}`)},
			},
			pollerInput: DescribeShardedExecutionsInput{
				StartExecutionsOutput{
					[]string{
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:2",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:3",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:4",
					},
				},
			},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1", "https://dbx.com/2", "https://dbx.com/3", "https://dbx.com/4"}, FailedRunPageUrls: []string{"https://dbx.com/1", "https://dbx.com/3", "https://dbx.com/4"}},
		},
		"testMultipleShardsWithOneRunning": {
			apiResult: []sfn.DescribeExecutionOutput{
				{Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "FAILED"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/2", "result_state": "SUCCESS"}`)},
				{Output: aws.String(`{"run_page_url": "https://dbx.com/3", "result_state": "RUNNING"}`)},
			},
			pollerInput: DescribeShardedExecutionsInput{
				StartExecutionsOutput{
					[]string{
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:2",
						"arn:aws:states:us-west-2:492164655156:execution:test_report-report:3",
					},
				},
			},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "RUNNING"},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSFN := mock_sfniface.NewMockSFNAPI(gomock.NewController(t))
			for i, execArn := range testCase.pollerInput.StartExecutionsResult.ExecutionArns {
				mockSFN.EXPECT().DescribeExecutionWithContext(gomock.Any(), &sfn.DescribeExecutionInput{
					ExecutionArn: aws.String(execArn),
				}).Return(&testCase.apiResult[i], nil).Times(1)
			}

			pollResult, err := describeShardedExecutions(nil, mockSFN, testCase.pollerInput)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedResult, pollResult)
		})
	}
}

func TestDescribeShardedExecutionsSFNStatus(t *testing.T) {
	testCases := map[string]struct {
		apiResult      sfn.DescribeExecutionOutput
		pollerInput    DescribeShardedExecutionsInput
		expectedResult DescribeShardedExecutionsOutput
	}{
		"testSFNRunning": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("RUNNING"), Output: aws.String("")},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "RUNNING"},
		},
		"testSFNFailed": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("FAILED"), Output: aws.String("")},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"Step function failed"}, FailedRunPageUrls: []string{"Step function failed"}},
		},
		"testSFNTimedOut": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("TIMED_OUT"), Output: aws.String("")},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"Step function failed"}, FailedRunPageUrls: []string{"Step function failed"}},
		},
		"testSFNAborted": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("ABORTED"), Output: aws.String("")},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"Step function failed"}, FailedRunPageUrls: []string{"Step function failed"}},
		},
		"testSFNSuccessJobSuccess": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("SUCCESS"), Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "SUCCESS"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "SUCCESS", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"No failed jobs!"}},
		},
		"testSFNSuccessJobFailed": {
			apiResult:      sfn.DescribeExecutionOutput{Status: aws.String("SUCCESS"), Output: aws.String(`{"run_page_url": "https://dbx.com/1", "result_state": "FAILED"}`)},
			pollerInput:    DescribeShardedExecutionsInput{StartExecutionsOutput{[]string{"arn:aws:states:us-west-2:492164655156:execution:test_report-report:1"}}},
			expectedResult: DescribeShardedExecutionsOutput{ResultState: "FAILED", RunPageUrls: []string{"https://dbx.com/1"}, FailedRunPageUrls: []string{"https://dbx.com/1"}},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSFN := mock_sfniface.NewMockSFNAPI(gomock.NewController(t))
			for _, execArn := range testCase.pollerInput.StartExecutionsResult.ExecutionArns {
				mockSFN.EXPECT().DescribeExecutionWithContext(gomock.Any(), &sfn.DescribeExecutionInput{
					ExecutionArn: aws.String(execArn),
				}).Return(&testCase.apiResult, nil).Times(1)
			}
			mockSFN.EXPECT().DescribeExecutionWithContext(gomock.Any(), &testCase.pollerInput).Return(&testCase.apiResult, nil).MaxTimes(1)
			pollResult, err := describeShardedExecutions(nil, mockSFN, testCase.pollerInput)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedResult, pollResult)
		})
	}
}
