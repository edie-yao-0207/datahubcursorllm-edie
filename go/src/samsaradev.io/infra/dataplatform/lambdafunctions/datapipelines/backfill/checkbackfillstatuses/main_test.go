package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var backfillEntryEmpty = dynamodb.GetItemOutput{
	Item: map[string]*dynamodb.AttributeValue{},
}

var backfillEntryRunning = dynamodb.GetItemOutput{
	Item: map[string]*dynamodb.AttributeValue{
		"backfill_status": {
			S: aws.String("RUNNING"),
		},
	},
}

var backfillEntrySuccess = dynamodb.GetItemOutput{
	Item: map[string]*dynamodb.AttributeValue{
		"backfill_status": {
			S: aws.String("SUCCESS"),
		},
	},
}

func TestItemBackfillEntryExists(t *testing.T) {
	testCases := map[string]struct {
		dynamoItem     dynamodb.GetItemOutput
		expectedResult bool
	}{
		"itemEntryExistsRUNNING": {
			dynamoItem:     backfillEntryRunning,
			expectedResult: true,
		},
		"itemEntryExistsSUCCESS": {
			dynamoItem:     backfillEntrySuccess,
			expectedResult: true,
		},
		"itemEntryDoesNotExists": {
			dynamoItem:     backfillEntryEmpty,
			expectedResult: false,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, itemBackfillEntryExists(testCase.dynamoItem))
		})
	}
}

func TestItemBackfillEntrySUCCESS(t *testing.T) {
	testCases := map[string]struct {
		dynamoItem     dynamodb.GetItemOutput
		expectedResult bool
	}{
		"itemEntrySUCCESS": {
			dynamoItem:     backfillEntrySuccess,
			expectedResult: true,
		},
		"itemEntryRUNNING": {
			dynamoItem:     backfillEntryRunning,
			expectedResult: false,
		},
		"itemEntryDoesNotExists": {
			dynamoItem:     backfillEntryEmpty,
			expectedResult: false,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testCase.expectedResult, itemBackfillEntrySuccessful(testCase.dynamoItem))
		})
	}
}

func TestCheckBackfillStatusForNode(t *testing.T) {
	parents := []string{"parent_1", "parent_2"}

	testCases := map[string]struct {
		parents                  []string
		parentsBackfillStatueses []dynamodb.GetItemOutput
		nodeBackfillStatus       dynamodb.GetItemOutput
		executeBackfill          bool
	}{
		"nodeBackfillRunning": {
			parents: parents,
			parentsBackfillStatueses: []dynamodb.GetItemOutput{
				backfillEntryEmpty,
				backfillEntryEmpty,
			},
			nodeBackfillStatus: backfillEntryRunning,
			executeBackfill:    false,
		},
		"nodeBackfillCompleted": {
			parents: parents,
			parentsBackfillStatueses: []dynamodb.GetItemOutput{
				backfillEntryEmpty,
				backfillEntryEmpty,
			},
			nodeBackfillStatus: backfillEntrySuccess,
			executeBackfill:    false,
		},
		"missingParentBackfillEntries": {
			parents: parents,
			parentsBackfillStatueses: []dynamodb.GetItemOutput{
				backfillEntrySuccess,
			},
			nodeBackfillStatus: backfillEntryEmpty,
			executeBackfill:    false,
		},
		"parentBackfillsRunning": {
			parents: parents,
			parentsBackfillStatueses: []dynamodb.GetItemOutput{
				backfillEntrySuccess,
				backfillEntryRunning,
			},
			nodeBackfillStatus: backfillEntryEmpty,
			executeBackfill:    false,
		},
		"successExecuteBacfkill": {
			parents: parents,
			parentsBackfillStatueses: []dynamodb.GetItemOutput{
				backfillEntrySuccess,
				backfillEntrySuccess,
			},
			nodeBackfillStatus: backfillEntryEmpty,
			executeBackfill:    true,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			res, err := checkBackfillStatusForNode(nil, CheckBackfillStatusesInput{
				Parents:                 testCase.parents,
				ParentsBackfillStatuses: testCase.parentsBackfillStatueses,
				NodeBackfillStatus:      testCase.nodeBackfillStatus,
			})
			require.NoError(t, err)
			assert.Equal(t, testCase.executeBackfill, res.ExecuteBackfill)
		})
	}
}
