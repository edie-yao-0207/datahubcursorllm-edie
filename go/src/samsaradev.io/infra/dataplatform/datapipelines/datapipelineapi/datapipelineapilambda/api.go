package datapipelineapilambda

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sfn/sfniface"
)

/*
	This package is copied from the datapipelineapi package (parent directory) with some minor tweaks
  to remove unneeded imports. This code is meant to be a slimmer datapipelineapi package for the purpose
  of using in lambda functions. Lambda functions need to be slim to make sure:
     1. Their size stays under the AWS Limit of 50MBs
     2. We manage lambda layers in terraform so each change of an imported package requires a terraform approval

	Please use this package (and add to it if need be) if you need the datapipelineapi functionality in your lambda function
*/

type NodeExecutionDetails struct {
	JobUrl          string
	Owner           string
	ErrorMessage    string
	DatabricksRunId int
	Error           string
}

type API interface {
	DescribeNodeExecution(ctx context.Context, executionArn string) (*NodeExecutionDetails, error)
}

type Client struct {
	SFN sfniface.SFNAPI
}

var _ API = (*Client)(nil)

func NewClient(sfnAPI sfniface.SFNAPI) *Client {
	return &Client{
		SFN: sfnAPI,
	}
}
