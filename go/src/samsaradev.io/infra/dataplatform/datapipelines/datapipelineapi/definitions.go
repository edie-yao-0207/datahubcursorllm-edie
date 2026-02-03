package datapipelineapi

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/dataplatform/datapipelines/stepfunctions"
)

// PipelineDefinitionVisitor walks pipeline state machine definition and
// constructs a graph of NESF task nodes.
type PipelineDefinitionVisitor struct{}

type NESFTask struct {
	NodeName        string
	StateName       string
	StateMachineArn string
	Parents         []*NESFTask
}

func (v PipelineDefinitionVisitor) Visit(definition *statemachine.StepFunctionDefinition) (*NESFTask, error) {
	tasks, err := v.visitDefinition(definition)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if len(tasks) != 1 {
		return nil, oops.Errorf("found %d final nodes", len(tasks))
	}
	return tasks[0], nil
}

func (v PipelineDefinitionVisitor) visitDefinition(definition *statemachine.StepFunctionDefinition) ([]*NESFTask, error) {
	stateName := definition.StartAtName
	var parents []*NESFTask
	for {
		state := definition.StateMap[stateName]
		tasks, err := v.visitState(stateName, state)
		if err != nil {
			return nil, oops.Wrapf(err, "visit state: %s", stateName)
		}

		if len(tasks) != 0 {
			for _, task := range tasks {
				for _, parent := range parents {
					task.Parents = append(task.Parents, parent)
				}
			}
			parents = tasks
		}

		next := state.NextState()
		if next == nil {
			break
		}
		stateName = string(*next)
	}
	return parents, nil
}

func (v PipelineDefinitionVisitor) visitState(stateName string, state statemachine.State) ([]*NESFTask, error) {
	switch state := state.(type) {
	case *statemachine.ParallelState:
		return v.visitParallelState(stateName, state)
	case *statemachine.TaskState:
		return v.visitTaskState(stateName, state)
	default:
		return nil, nil
	}
}

func (v PipelineDefinitionVisitor) visitParallelState(stateName string, parallelState *statemachine.ParallelState) ([]*NESFTask, error) {
	var nodes []*NESFTask
	for idx, branch := range parallelState.Branches {
		newNodes, err := v.visitDefinition(&branch)
		if err != nil {
			return nil, oops.Wrapf(err, "visit branch %d", idx)
		}
		nodes = append(nodes, newNodes...)
	}
	return nodes, nil
}

func (v PipelineDefinitionVisitor) visitTaskState(stateName string, taskState *statemachine.TaskState) ([]*NESFTask, error) {
	// The metrics_logger lambda task state that logs metrics to datadog should be abstracted away from the pipeline
	// definition as it is internal to the framework.
	// The parameters looks like map[FunctionName:metrics_logger InvocationType:Event Payload:map[Output.$:$.Output]],
	// so we can check for this function name and ignore any tasks with it.
	functionName, ok := taskState.Parameters["FunctionName"].(string)
	if ok && functionName == stepfunctions.MetricsLoggerLambdaName {
		return nil, nil
	}

	stateMachineArn, ok := taskState.Parameters["StateMachineArn"].(string)
	if !ok {
		return nil, oops.Errorf("task state %q unexpected input %v", stateName, taskState.Parameters["Input"])
	}
	input, ok := taskState.Parameters["Input"].(map[string]interface{})
	if !ok {
		return nil, oops.Errorf("task state %q unexpected input %v", stateName, taskState.Parameters["Input"])
	}
	nodeName, ok := input["node_id"].(string)
	if !ok {
		return nil, oops.Errorf("task state %q has no node_id", stateName)
	}

	return []*NESFTask{{
		NodeName:        nodeName,
		StateName:       stateName,
		StateMachineArn: stateMachineArn,
	}}, nil
}
