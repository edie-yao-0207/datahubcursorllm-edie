package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/vendormocks/mock_sfniface"
)

func TestCreateEndDatesList(t *testing.T) {
	testCases := map[string]struct {
		startDate      string
		endDate        string
		expectedResult []string
		errorExpected  bool
	}{
		"singleShardSUCCESS": {
			startDate:      "2020-01-20",
			endDate:        "2020-01-22",
			expectedResult: []string{"'2020-01-22'"},
			errorExpected:  false,
		},
		"multipleShardSUCCESS": {
			startDate:      "2020-01-20",
			endDate:        "2020-04-10",
			expectedResult: []string{"'2020-02-19'", "'2020-03-20'", "'2020-04-10'"},
			errorExpected:  false,
		},
		"startDateAfterEndDateFAILURE": {
			startDate:      "2020-01-20",
			endDate:        "2020-01-10",
			expectedResult: nil,
			errorExpected:  true,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			endDates, err := createEndDatesList(testCase.startDate, testCase.endDate)
			assert.Equal(t, testCase.expectedResult, endDates, "Based on a shard duration of 30, this function should create the correct number of end date partitions")
			if testCase.errorExpected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetExecutionInputs(t *testing.T) {
	testCases := map[string]struct {
		input          StartShardedBackfillsInput
		expectedResult []StartShardedBackfillsInput
	}{
		"singleShardSUCCESS": {
			input: StartShardedBackfillsInput{
				BackfillNESFInput{
					StartDate:   "'2020-01-20'",
					EndDate:     "'2020-01-22'",
					StartedById: "arn:aws:states:us-west-2:492164655156:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
				},
			},
			expectedResult: []StartShardedBackfillsInput{
				{
					BackfillNESFInput{
						StartDate:   "'2020-01-20'",
						EndDate:     "'2020-01-22'",
						StartedById: "arn:aws:states:us-west-2:492164655156:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
					},
				},
			},
		},
		"multipleShardSUCCESS": {
			input: StartShardedBackfillsInput{
				BackfillNESFInput{
					StartDate:   "'2020-01-20'",
					EndDate:     "'2020-02-22'",
					StartedById: "arn:aws:states:eu-west-1:353964698255:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
				},
			},
			expectedResult: []StartShardedBackfillsInput{
				{
					BackfillNESFInput{
						StartDate:   "'2020-01-20'",
						EndDate:     "'2020-02-19'",
						StartedById: "arn:aws:states:eu-west-1:353964698255:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
					},
				},
				{
					BackfillNESFInput{
						StartDate:   "'2020-02-19'",
						EndDate:     "'2020-02-22'",
						StartedById: "arn:aws:states:eu-west-1:353964698255:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
					},
				},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			inputs, err := getExecutionInputs(testCase.input)
			require.NoError(t, err)

			// Must individually compare each entry in array as name field in step function input is defined with a random UUID, which will cause entire arrays to be different
			for i, input := range inputs {
				assert.Equal(t, testCase.expectedResult[i].StartDate, input.StartDate, "start dates should be equal")
				assert.Equal(t, testCase.expectedResult[i].EndDate, input.EndDate, "end dates should be equal")
				assert.Equal(t, testCase.expectedResult[i].StartedById, input.StartedById, "started by ids should be equal")
			}
		})
	}
}

func TestStartShardedExecutions(t *testing.T) {
	mockSFN := mock_sfniface.NewMockSFNAPI(gomock.NewController(t))

	testCases := map[string]struct {
		input                 StartShardedBackfillsInput
		expectedNumExecutions int
	}{
		"singleShardSUCCESS": {
			input: StartShardedBackfillsInput{
				BackfillNESFInput{
					BackfillStartDate: "2020-01-20",
					EndDate:           "2020-01-22",
					StartedById:       "arn:aws:states:us-west-2:492164655156:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
				},
			},
			expectedNumExecutions: 1,
		},
		"multipleShardSUCCESS": {
			input: StartShardedBackfillsInput{
				BackfillNESFInput{
					BackfillStartDate: "2020-01-20",
					EndDate:           "2020-04-10",
					StartedById:       "arn:aws:states:us-west-2:492164655156:execution:test_report-report:8f695f79-a23d-4dc1-8c5b-33229e5f16d7",
				},
			},
			expectedNumExecutions: 3,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			sfnOutput := sfn.StartExecutionOutput{ExecutionArn: aws.String("placeholder_exec_arn")}
			mockSFN.EXPECT().StartExecutionWithContext(gomock.Any(), gomock.Any()).Return(&sfnOutput, nil).AnyTimes()
			output, err := startShardedExecutions(nil, mockSFN, testCase.input)
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedNumExecutions, len(output.ExecutionArns), "Based on a shard duration of 30, this function should create the correct number of execution Arns")
		})
	}
}
