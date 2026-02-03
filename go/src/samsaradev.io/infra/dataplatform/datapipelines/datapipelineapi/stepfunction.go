package datapipelineapi

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
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

func (c *Client) describeStepFunctionExecution(ctx context.Context, executionArn string) (*SFNExecution, error) {
	describeExecutionOutput, err := c.SFN.DescribeExecutionWithContext(ctx, &sfn.DescribeExecutionInput{
		ExecutionArn: aws.String(executionArn),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "describe execution: %s", executionArn)
	}

	var executionInput map[string]interface{}
	if err := json.Unmarshal([]byte(aws.StringValue(describeExecutionOutput.Input)), &executionInput); err != nil {
		return nil, oops.Wrapf(err, "parse execution input: %s", aws.StringValue(describeExecutionOutput.Input))
	}

	describeStateMachineOutput, err := c.SFN.DescribeStateMachineForExecutionWithContext(ctx,
		&sfn.DescribeStateMachineForExecutionInput{
			ExecutionArn: aws.String(executionArn),
		},
	)
	if err != nil {
		return nil, oops.Wrapf(err, "describe state machine for execution: %s", executionArn)
	}

	var definition statemachine.StepFunctionDefinition
	if err := definition.UnmarshalJSON([]byte(aws.StringValue(describeStateMachineOutput.Definition))); err != nil {
		return nil, oops.Wrapf(err, "parse state machine definition for execution: %s", executionArn)
	}

	var events []*sfn.HistoryEvent
	if err := c.SFN.GetExecutionHistoryPagesWithContext(ctx, &sfn.GetExecutionHistoryInput{
		ExecutionArn: aws.String(executionArn),
	}, func(output *sfn.GetExecutionHistoryOutput, _ bool) bool {
		events = append(events, output.Events...)
		return true
	}); err != nil {
		return nil, oops.Wrapf(err, "get execution history: %s", executionArn)
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
				return nil, oops.Errorf("source for TaskSubmitted event %d not found", eventId)
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
				return nil, oops.Errorf("source for TaskSubmitted event %d not found", eventId)
			}
			stateName := aws.StringValue(source.StateEnteredEventDetails.Name)

			type stepFunctionExecutionOutput struct {
				ExecutionArn string `json:"ExecutionArn"`
			}
			var executionOutput stepFunctionExecutionOutput
			jsonOutput := aws.StringValue(event.TaskSubmittedEventDetails.Output)
			if err := json.Unmarshal([]byte(jsonOutput), &executionOutput); err != nil {
				return nil, oops.Wrapf(err, "parse %s", jsonOutput)
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
				return nil, oops.Errorf("source for TaskSubmitted event %d not found", eventId)
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
