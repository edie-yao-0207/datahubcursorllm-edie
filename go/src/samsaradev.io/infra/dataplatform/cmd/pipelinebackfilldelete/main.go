/*
This main.go is a script to delete an entry in the Data Pipeline Backfill State dynamodb table
It should be used when a node's SQL is updated and a backfill needs to be run on the node to change data
To use the tool, you must specify which node you wish to delete an entry and which region to delete the entry in as
US and EU are treated separately.

Example usage:

	go run go/src/samsaradev.io/infra/dataplatform/cmd/pipelinebackfilldelete/main.go -node_name=[node_name] -region=[region]
*/
package main

import (
	"context"
	"flag"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
)

const (
	datapipelineBackfillStateDynamoTable = "datapipeline-backfill-state"
)

var nodeName string
var region string

func init() {
	flag.StringVar(&nodeName, "node", "", "The name of the node to destroy entry for in the DyanmoDB backfill table")
	flag.StringVar(&region, "region", "", "The region [us-west-2, eu-west-1] to destroy entry for in the DyanmoDB backfill table")
	flag.Parse()
}

func main() {
	ctx := context.Background()
	if nodeName == "" {
		log.Fatal("Please specify the name of the node destroy DynamoDB entry for")
	}

	if region == "" {
		log.Fatal("Please specify the region")
	}

	dags, err := graphs.BuildDAGs()
	if err != nil {
		log.Fatal(oops.Wrapf(err, "Error building Pipeline graphs"))
	}

	nodeExists := false
	for _, dag := range dags {
		if nodeRef := dag.GetNodeFromName(nodeName); nodeRef != nil {
			nodeExists = true
		}
	}

	if !nodeExists {
		log.Fatalf("Node name '%s' argument invalid. Node does not exist.", nodeName)
	}

	databricksAWSSession := dataplatformhelpers.GetDatabricksAWSSession(region)
	dynamoClient := dynamodb.New(databricksAWSSession)
	output, err := dynamoClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"node_id": {
				S: aws.String(nodeName),
			},
		},
		TableName: aws.String(datapipelineBackfillStateDynamoTable),
	})
	if err != nil {
		log.Fatal(oops.Wrapf(err, "Could not find entry for node_id %s in dynamo table %s", nodeName, datapipelineBackfillStateDynamoTable))
	}
	if len(output.Item) == 0 {
		log.Fatalf("Dynamo backfill entry with node_id '%s' does not exist.", nodeName)
	}

	_, err = dynamoClient.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"node_id": {
				S: aws.String(nodeName),
			},
		},
		TableName: aws.String(datapipelineBackfillStateDynamoTable),
	})
	if err != nil {
		log.Fatal(oops.Wrapf(err, ""))
	}

	log.Printf("Successfully deleted %s backfill entry in Dynamo table", nodeName)
}
