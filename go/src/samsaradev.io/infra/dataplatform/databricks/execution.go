package databricks

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/samsarahq/go/oops"
)

type CreateExecutionContextInput struct {
	Language  string `json:"language"`
	ClusterId string `json:"clusterId"`
}

type CreateExecutionContextOutput struct {
	ContextId string `json:"id"`
}

type DestroyExecutionContextInput struct {
	ContextId string `json:"contextId"`
	ClusterId string `json:"clusterId"`
}

type DestroyExecutionContextOutput struct {
	ContextId string `json:"id"`
}

type ExecuteCommandInput struct {
	ContextId string `json:"contextId"`
	ClusterId string `json:"clusterId"`
	Language  string `json:"language"`
	Command   string `json:"command"`
}

type ExecuteCommandOutput struct {
	CommandId string `json:"id"`
}

type CancelCommandInput struct {
	ContextId string `json:"contextId"`
	ClusterId string `json:"clusterId"`
	CommandId string `json:"commandId"`
}

type CancelCommandOutput struct {
	CommandId string `json:"id"`
}

type CommandStatus string

const (
	CommandStatusQueued     = CommandStatus("Queued")
	CommandStatusRunning    = CommandStatus("Running")
	CommandStatusCancelling = CommandStatus("Cancelling")
	CommandStatusFinished   = CommandStatus("Finished")
	CommandStatusCancelled  = CommandStatus("Cancelled")
	CommandStatusError      = CommandStatus("Error")
)

type GetCommandStatusInput struct {
	ContextId string `json:"contextId"`
	ClusterId string `json:"clusterId"`
	CommandId string `json:"commandId"`
}

type JsonSchemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type GetCommandStatusOutputResults struct {
	Data         json.RawMessage `json:"data"`
	Schema       json.RawMessage `json:"schema"`
	Truncated    bool            `json:"truncated"`
	IsJsonSchema bool            `json:"isJsonSchema"`
	ResultType   string          `json:"resultType"`
}

type GetCommandStatusOutput struct {
	CommandId string                        `json:"id"`
	Status    CommandStatus                 `json:"status"`
	Results   GetCommandStatusOutputResults `json:"results"`
}

type ExecutionContextAPI interface {
	CreateExecutionContext(context.Context, *CreateExecutionContextInput) (*CreateExecutionContextOutput, error)
	DestroyExecutionContext(context.Context, *DestroyExecutionContextInput) (*DestroyExecutionContextOutput, error)
	ExecuteCommand(context.Context, *ExecuteCommandInput) (*ExecuteCommandOutput, error)
	CancelCommand(context.Context, *CancelCommandInput) (*CancelCommandOutput, error)
	GetCommandStatus(context.Context, *GetCommandStatusInput) (*GetCommandStatusOutput, error)
}

func (c *Client) CreateExecutionContext(ctx context.Context, input *CreateExecutionContextInput) (*CreateExecutionContextOutput, error) {
	var output CreateExecutionContextOutput
	if err := c.do(ctx, http.MethodPost, "/api/1.2/contexts/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) DestroyExecutionContext(ctx context.Context, input *DestroyExecutionContextInput) (*DestroyExecutionContextOutput, error) {
	var output DestroyExecutionContextOutput
	if err := c.do(ctx, http.MethodPost, "/api/1.2/contexts/destroy", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) ExecuteCommand(ctx context.Context, input *ExecuteCommandInput) (*ExecuteCommandOutput, error) {
	var output ExecuteCommandOutput
	if err := c.do(ctx, http.MethodPost, "/api/1.2/commands/execute", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CancelCommand(ctx context.Context, input *CancelCommandInput) (*CancelCommandOutput, error) {
	var output CancelCommandOutput
	if err := c.do(ctx, http.MethodPost, "/api/1.2/commands/cancel", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetCommandStatus(ctx context.Context, input *GetCommandStatusInput) (*GetCommandStatusOutput, error) {
	var output GetCommandStatusOutput
	if err := c.do(ctx, http.MethodGet, "/api/1.2/commands/status", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func ParseJsonSchemaColumns(msg json.RawMessage) ([]*JsonSchemaColumn, error) {
	var columns []*JsonSchemaColumn
	if err := json.Unmarshal(msg, &columns); err != nil {
		return nil, oops.Wrapf(err, "%s", string(msg))
	}
	for _, col := range columns {
		col.Type = strings.Trim(col.Type, `"`)
		switch col.Type {
		case "date", "long", "double", "string", "boolean", "integer":
			continue
		case "array", "struct":
			return nil, oops.Errorf("unsupported type: %s", col.Type)
		default:
			return nil, oops.Errorf("unknown type: %s", col.Type)
		}
	}
	return columns, nil
}
