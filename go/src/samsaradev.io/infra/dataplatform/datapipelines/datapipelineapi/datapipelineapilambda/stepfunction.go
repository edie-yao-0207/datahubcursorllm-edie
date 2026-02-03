package datapipelineapilambda

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
)

type ExecutionStatus string

const (
	ExecutionStatusNotReady  = ExecutionStatus("notready")
	ExecutionStatusSucceeded = ExecutionStatus("succeeded")
	ExecutionStatusRunning   = ExecutionStatus("running")
	ExecutionStatusFailed    = ExecutionStatus("failed")
	ExecutionStatusTimedOut  = ExecutionStatus("timedout")
	ExecutionStatusAborted   = ExecutionStatus("aborted")
)

func (s ExecutionStatus) Failed() bool {
	switch s {
	case ExecutionStatusAborted, ExecutionStatusFailed, ExecutionStatusTimedOut:
		return true
	default:
		return false
	}
}

type SFNStateExecution struct {
	Status                            ExecutionStatus
	StartTime                         time.Time
	EndTime                           time.Time
	Input                             string
	Output                            string
	Error                             string
	TaskStateStepFunctionExecutionArn string
}

type SFNExecution struct {
	StateMachineName    string
	StateMachineArn     string
	Definition          *statemachine.StepFunctionDefinition
	DefinitionUpdatedAt time.Time
	StartTime           time.Time
	EndTime             *time.Time
	Status              ExecutionStatus
	States              map[string]*SFNStateExecution
	Input               map[string]interface{}
}

func mapSFNStatus(status string) ExecutionStatus {
	return map[string]ExecutionStatus{
		sfn.ExecutionStatusAborted:   ExecutionStatusAborted,
		sfn.ExecutionStatusRunning:   ExecutionStatusRunning,
		sfn.ExecutionStatusSucceeded: ExecutionStatusSucceeded,
		sfn.ExecutionStatusTimedOut:  ExecutionStatusTimedOut,
		sfn.ExecutionStatusFailed:    ExecutionStatusFailed,
	}[status]
}

func (c *Client) DescribeNodeExecution(ctx context.Context, executionArn string) (*NodeExecutionDetails, error) {
	execution, err := c.describeStepFunctionExecution(ctx, executionArn)
	if err != nil {
		return nil, err
	}

	pollerState, ok := execution.States["databricks_poller"]
	if !ok {
		return nil, fmt.Errorf("databricks_poller not found: %s", executionArn)
	}

	var output struct {
		RunPageUrl string `json:"run_page_url"`
		Owner      string `json:"team"`
		RunId      int    `json:"run_id"`
	}

	errorMessage := ""

	if pollerState.Output == "" {
		sqlState, ok := execution.States["databricks_sql_transformation"]
		if !ok {
			// https://samsara.atlassian-us-gov-mod.net/browse/DATAPLAT-107
			log.Printf("run_page_url missing from poller and sql transformation is not found. executionArn: %s", executionArn)
			return nil, nil
		}

		if sqlState.Error != "" {
			urlRegex := regexp.MustCompile(`https[^\s"]*`)
			output.RunPageUrl = urlRegex.FindString(sqlState.Error)

			// HACK: The sql_transformation node can fail when trying to list instances pool.
			// If that happens, the error we are parsing here has a url to the instance-pool/list endpoint.
			// We don't want to populate the UI with that endpoint since devs can't access it so we set it to empty.
			if strings.Contains(output.RunPageUrl, "instance-pools/list") {
				output.RunPageUrl = ""
			}

			output.Owner = "DataPlatform"
			errorMessage = sqlState.Error
		} else {
			log.Printf("run_page_url missing from errored sql transformation state. executionArn: %s", executionArn)
			return nil, nil
		}
	} else {
		if err := json.Unmarshal([]byte(pollerState.Output), &output); err != nil {
			return nil, err
		}

		if output.RunPageUrl == "" {
			return nil, fmt.Errorf("run_page_url is empty: %s", executionArn)
		}

		if output.Owner == "" {
			output.Owner = "UNKNOWN"
		}
	}

	return &NodeExecutionDetails{
		JobUrl:          output.RunPageUrl,
		Owner:           output.Owner,
		ErrorMessage:    errorMessage,
		DatabricksRunId: output.RunId,
		Error:           datapipelineerrors.GetErrorClassification(errorMessage).ErrorType,
	}, nil
}

func (c *Client) describeStepFunctionExecution(ctx context.Context, executionArn string) (*SFNExecution, error) {
	describeExecutionOutput, err := c.SFN.DescribeExecutionWithContext(ctx, &sfn.DescribeExecutionInput{
		ExecutionArn: aws.String(executionArn),
	})
	if err != nil {
		return nil, err
	}

	var executionInput map[string]interface{}
	if err := json.Unmarshal([]byte(aws.StringValue(describeExecutionOutput.Input)), &executionInput); err != nil {
		return nil, err
	}

	describeStateMachineOutput, err := c.SFN.DescribeStateMachineForExecutionWithContext(ctx,
		&sfn.DescribeStateMachineForExecutionInput{
			ExecutionArn: aws.String(executionArn),
		},
	)
	if err != nil {
		return nil, err
	}

	var definition statemachine.StepFunctionDefinition
	if err := definition.UnmarshalJSON([]byte(aws.StringValue(describeStateMachineOutput.Definition))); err != nil {
		return nil, err
	}

	var events []*sfn.HistoryEvent
	if err := c.SFN.GetExecutionHistoryPagesWithContext(ctx, &sfn.GetExecutionHistoryInput{
		ExecutionArn: aws.String(executionArn),
	}, func(output *sfn.GetExecutionHistoryOutput, _ bool) bool {
		events = append(events, output.Events...)
		return true
	}); err != nil {
		return nil, err
	}

	stateExecutions := make(map[string]*SFNStateExecution)
	stack := []statemachine.StepFunctionDefinition{definition}
	for len(stack) != 0 {
		last := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for stateName, state := range last.StateMap {
			stateExecutions[stateName] = &SFNStateExecution{
				Status: ExecutionStatusNotReady,
			}
			if parallelState, ok := state.(*statemachine.ParallelState); ok {
				stack = append(stack, parallelState.Branches...)
			}
		}
	}

	for _, event := range events {
		eventId := int(aws.Int64Value(event.Id))

		// Track task statuses.
		switch aws.StringValue(event.Type) {
		case sfn.HistoryEventTypeTaskStarted,
			sfn.HistoryEventTypeTaskFailed,
			sfn.HistoryEventTypeTaskTimedOut,
			sfn.HistoryEventTypeTaskSucceeded:
			source, ok := findSourceEvent(events, eventId, sfn.HistoryEventTypeTaskStateEntered)
			if !ok {
				return nil, fmt.Errorf("source for TaskSubmitted event %d not found", eventId)
			}
			stateName := aws.StringValue(source.StateEnteredEventDetails.Name)

			execution := stateExecutions[stateName]
			switch aws.StringValue(event.Type) {
			case sfn.HistoryEventTypeTaskStarted:
				execution.StartTime = aws.TimeValue(event.Timestamp)
				execution.Status = ExecutionStatusRunning
			case sfn.HistoryEventTypeTaskFailed:
				execution.EndTime = aws.TimeValue(event.Timestamp)
				execution.Status = ExecutionStatusFailed
			case sfn.HistoryEventTypeTaskTimedOut:
				execution.EndTime = aws.TimeValue(event.Timestamp)
				execution.Status = ExecutionStatusTimedOut
			case sfn.HistoryEventTypeTaskSucceeded:
				execution.EndTime = aws.TimeValue(event.Timestamp)
				execution.Status = ExecutionStatusSucceeded
			}
		}

		// Extract state machine task execution results.
		switch aws.StringValue(event.Type) {
		case sfn.HistoryEventTypeTaskSubmitted:
			source, ok := findSourceEvent(events, eventId, sfn.HistoryEventTypeTaskStateEntered)
			if !ok {
				return nil, fmt.Errorf("source for TaskSubmitted event %d not found", eventId)
			}
			stateName := aws.StringValue(source.StateEnteredEventDetails.Name)

			type stepFunctionExecutionOutput struct {
				ExecutionArn string `json:"ExecutionArn"`
			}
			var executionOutput stepFunctionExecutionOutput
			jsonOutput := aws.StringValue(event.TaskSubmittedEventDetails.Output)
			if err := json.Unmarshal([]byte(jsonOutput), &executionOutput); err != nil {
				return nil, err
			}

			// Keep track of latest execution per task. We only retry on SDK errors,
			// so we should only have one actual execution per state.
			stateExecutions[stateName].TaskStateStepFunctionExecutionArn = executionOutput.ExecutionArn
		}

		// Extract inputs and outputs.
		switch aws.StringValue(event.Type) {
		case sfn.HistoryEventTypeChoiceStateEntered,
			sfn.HistoryEventTypeFailStateEntered,
			sfn.HistoryEventTypeMapStateEntered,
			sfn.HistoryEventTypeParallelStateEntered,
			sfn.HistoryEventTypePassStateEntered,
			sfn.HistoryEventTypeSucceedStateEntered,
			sfn.HistoryEventTypeWaitStateEntered,
			sfn.HistoryEventTypeTaskStateEntered:
			stateExecutions[aws.StringValue(event.StateEnteredEventDetails.Name)].Input = aws.StringValue(event.StateEnteredEventDetails.Input)
		case sfn.HistoryEventTypeChoiceStateExited,
			sfn.HistoryEventTypeMapStateExited,
			sfn.HistoryEventTypeParallelStateExited,
			sfn.HistoryEventTypePassStateExited,
			sfn.HistoryEventTypeSucceedStateExited,
			sfn.HistoryEventTypeWaitStateExited,
			sfn.HistoryEventTypeTaskStateExited:
			stateExecutions[aws.StringValue(event.StateExitedEventDetails.Name)].Output = aws.StringValue(event.StateExitedEventDetails.Output)
		case sfn.HistoryEventTypeLambdaFunctionFailed:
			// This catches the case in which the sql transformation node itself fails
			// In this case, we want to get the run page url from the Exception cause message
			// and artificially inject it into the poller states output so the toolshed page can display it
			source, ok := findSourceEvent(events, eventId, sfn.HistoryEventTypeTaskStateEntered)
			if !ok {
				return nil, fmt.Errorf("source for TaskSubmitted event %d not found", eventId)
			}

			stateName := aws.StringValue(source.StateEnteredEventDetails.Name)
			stateExecutions[stateName].Error = aws.StringValue(event.LambdaFunctionFailedEventDetails.Cause)
		}
	}
	return &SFNExecution{
		StateMachineName:    aws.StringValue(describeStateMachineOutput.Name),
		StateMachineArn:     aws.StringValue(describeStateMachineOutput.StateMachineArn),
		Definition:          &definition,
		DefinitionUpdatedAt: aws.TimeValue(describeStateMachineOutput.UpdateDate),
		StartTime:           aws.TimeValue(describeExecutionOutput.StartDate),
		EndTime:             describeExecutionOutput.StopDate,
		Status:              mapSFNStatus(aws.StringValue(describeExecutionOutput.Status)),
		States:              stateExecutions,
		Input:               executionInput,
	}, nil
}

func findSourceEvent(events []*sfn.HistoryEvent, id int, typ string) (*sfn.HistoryEvent, bool) {
	for {
		event := events[id-1]
		if aws.StringValue(event.Type) == typ {
			return event, true
		}
		if event.PreviousEventId == nil {
			return nil, false
		}
		id = int(aws.Int64Value(event.PreviousEventId))
	}
}
