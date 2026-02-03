package datapipelineapi

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/samsarahq/go/oops"
)

const MetastoreTableName = "datapipeline-execution-state"
const BackfillTableName = "datapipeline-backfill-state"

type NESFExecutionStatus struct {
	NodeId           string     `dynamodbav:"node_id"`
	ExecutionId      string     `dynamodbav:"execution_id"`
	ExecutionStart   time.Time  `dynamodbav:"execution_start"`
	ExecutionEnd     *time.Time `dynamodbav:"execution_end"`
	ResultState      *string    `dynamodbav:"result_state"`
	NESFExecutionArn *string    `dynamodbav:"nesf_execution_arn"`
}

func (c *Client) describeNESFExecutionStatus(ctx context.Context, nodeId, executionId string) (*NESFExecutionStatus, error) {
	out, err := c.DynamoDB.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(MetastoreTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"node_id":      {S: aws.String(nodeId)},
			"execution_id": {S: aws.String(executionId)},
		},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "describe NESF execution status: %s, %s", nodeId, executionId)
	}
	if len(out.Item) == 0 {
		return nil, nil
	}

	var status NESFExecutionStatus
	if err := dynamodbattribute.UnmarshalMap(out.Item, &status); err != nil {
		return nil, oops.Wrapf(err, "unmarshal map: %v", out.Item)
	}

	return &status, nil
}

func (c *Client) DeleteBackfillEntry(ctx context.Context, nodeId string) error {
	_, err := c.DynamoDB.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(BackfillTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"node_id": {S: aws.String(nodeId)},
		},
	})
	if err != nil {
		return oops.Wrapf(err, "error deleting entry from pipeline backfill table. node: %s table: %s", nodeId, BackfillTableName)
	}

	return nil
}
