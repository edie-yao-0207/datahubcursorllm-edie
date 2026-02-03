package datapipelineapi

import (
	"context"
	"time"

	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineapi/databricksdynamodb"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineapi/databrickss3"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineapi/databrickssfn"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws"
)

type StartPipelineArgs struct {
	Name            string
	StateMachineArn string
	StartDate       string
	EndDate         string
	ShardDuration   int
}

type DeployedPipeline struct {
	Name            string
	StateMachineArn string
	CreatedAt       time.Time
}

type PipelineExecution struct {
	ExecutionArn string
	Status       ExecutionStatus
	StartTime    time.Time
	EndTime      *time.Time
}

type PipelineExecutionInput struct {
	StartDate             string
	EndDate               string
	PipelineExecutionTime string
}

type PipelineExecutionDetails struct {
	ExecutionArn        string
	PipelineName        string
	StateMachineArn     string
	ExecutionId         string
	DefinitionUpdatedAt time.Time
	StartTime           time.Time
	EndTime             *time.Time
	Status              ExecutionStatus
	Nodes               []*NodeExecution
	Input               *PipelineExecutionInput
}

type NodeExecution struct {
	Name string

	// Start and End prefer to timestamps recorded in Metastore.
	StartTime *time.Time
	EndTime   *time.Time

	// Status is one of "not-ready", "running", "succeeded", "failed".
	Status ExecutionStatus

	// ExecutionArn is the ExecutionArn of the active NESF recorded in Metastore.
	ExecutionArn *string

	// Edges.
	NextNodes []string
	PrevNodes []string
}

type NodeExecutionDetails struct {
	JobUrl          string
	Owner           string
	ErrorMessage    string
	DatabricksRunId int
	Error           string
}

type API interface {
	ListDeployedPipelines(ctx context.Context, namePattern string) ([]*DeployedPipeline, error)
	ListPipelineExecutions(ctx context.Context, stateMachineArn string, limit int) ([]*PipelineExecution, error)
	DescribePipeline(ctx context.Context, stateMachineArn string) (*DeployedPipeline, error)
	DescribePipelineExecution(ctx context.Context, executionArn string) (*PipelineExecutionDetails, error)
	DescribeNodeExecution(ctx context.Context, executionArn string) (*NodeExecutionDetails, error)
	ListNodeExecutions(ctx context.Context, sfnArn string, limit int) ([]*NodeExecution, error)
	StartPipelineExecution(ctx context.Context, startDate string, endDate string, sfnArn string, shardDuration int) (string, error)
	DeleteNodeData(ctx context.Context, nodeName string) error
	DeleteBackfillEntry(ctx context.Context, nodeId string) error
}

type Client struct {
	SFN      databrickssfn.SFNAPI
	DynamoDB databricksdynamodb.DynamoDBAPI
	S3       databrickss3.S3API
	Region   string
}

var _ API = (*Client)(nil)

func NewClient(sfnAPI databrickssfn.SFNAPI, dynamoDBAPI databricksdynamodb.DynamoDBAPI, s3API databrickss3.S3API) *Client {
	return &Client{
		SFN:      sfnAPI,
		DynamoDB: dynamoDBAPI,
		S3:       s3API,
		Region:   samsaraaws.GetAWSRegion(),
	}
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewClient)
	fxregistry.MustRegisterDefaultConstructor(func(c *Client) API { return c })
}
