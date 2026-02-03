package datapipelineapi

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/samuuid"
)

func (c *Client) DescribePipeline(ctx context.Context, stateMachineArn string) (*DeployedPipeline, error) {
	out, err := c.SFN.DescribeStateMachineWithContext(ctx, &sfn.DescribeStateMachineInput{
		StateMachineArn: aws.String(stateMachineArn),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "describe %s", stateMachineArn)
	}
	return &DeployedPipeline{
		Name:            aws.StringValue(out.Name),
		StateMachineArn: aws.StringValue(out.StateMachineArn),
		CreatedAt:       aws.TimeValue(out.CreationDate),
	}, nil
}

func (c *Client) ListDeployedPipelines(ctx context.Context, namePattern string) ([]*DeployedPipeline, error) {
	var pipelines []*DeployedPipeline

	if err := c.SFN.ListStateMachinesPagesWithContext(ctx, &sfn.ListStateMachinesInput{}, func(output *sfn.ListStateMachinesOutput, _ bool) bool {
		for _, stateMachine := range output.StateMachines {
			stateMachineName := aws.StringValue(stateMachine.Name)
			if strings.HasPrefix(stateMachineName, "nesf-") {
				continue
			}

			switch stateMachineName {
			case "data_pipeline_backfill_sfn",
				"data_pipeline_backfill_sfn_sharded",
				"data_pipeline_orchestration_sfn",
				"sql-transformation-node",
				"sql-transformation-backfill-node":
				continue
			}

			if namePattern != "" {
				if !strings.Contains(stateMachineName, namePattern) {
					continue
				}
			}

			pipelines = append(pipelines, &DeployedPipeline{
				Name:            stateMachineName,
				StateMachineArn: aws.StringValue(stateMachine.StateMachineArn),
				CreatedAt:       aws.TimeValue(stateMachine.CreationDate),
			})
		}
		return true
	}); err != nil {
		return nil, oops.Wrapf(err, "list state machines")
	}
	return pipelines, nil
}

func (c *Client) ListPipelineExecutions(ctx context.Context, stateMachineArn string, limit int) ([]*PipelineExecution, error) {
	var executions []*PipelineExecution
	if err := c.SFN.ListExecutionsPagesWithContext(ctx, &sfn.ListExecutionsInput{
		StateMachineArn: aws.String(stateMachineArn),
		MaxResults:      aws.Int64(int64(limit)),
	}, func(output *sfn.ListExecutionsOutput, _ bool) bool {
		for _, entry := range output.Executions {
			executions = append(executions, &PipelineExecution{
				ExecutionArn: aws.StringValue(entry.ExecutionArn),
				StartTime:    aws.TimeValue(entry.StartDate),
				EndTime:      entry.StopDate,
				Status:       mapSFNStatus(aws.StringValue(entry.Status)),
			})
		}
		if len(executions) >= limit {
			return false
		}
		return true
	}); err != nil {
		return nil, oops.Wrapf(err, "list executions: %s", stateMachineArn)
	}
	return executions, nil
}

func (c *Client) DescribePipelineExecution(ctx context.Context, executionArn string) (*PipelineExecutionDetails, error) {
	pipelineExecution, err := c.describeStepFunctionExecution(ctx, executionArn)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	executionIdIface, ok := pipelineExecution.Input["execution_id"]
	if !ok {
		return nil, oops.Errorf("execution_id not found for %s", executionArn)
	}
	executionId, ok := executionIdIface.(string)
	if !ok {
		return nil, oops.Errorf("execution_id is not a string: %v", executionIdIface)
	}

	inputStartDateIface, ok := pipelineExecution.Input["start_date"]
	if !ok {
		return nil, oops.Errorf("start_date not found for %s", executionArn)
	}
	inputStartDate, ok := inputStartDateIface.(string)
	if !ok {
		return nil, oops.Errorf("start_date is not a string: %v", inputStartDateIface)
	}

	inputEndDateIface, ok := pipelineExecution.Input["end_date"]
	if !ok {
		return nil, oops.Errorf("end_date not found for %s", executionArn)
	}
	inputEndDate, ok := inputEndDateIface.(string)
	if !ok {
		return nil, oops.Errorf("end_date is not a string: %v", inputEndDateIface)
	}

	inputPipelineExecutionTimeIface, ok := pipelineExecution.Input["pipeline_execution_time"]
	if !ok {
		return nil, oops.Errorf("pipeline_execution_time not found for %s", executionArn)
	}
	inputPipelineExecutionTime, ok := inputPipelineExecutionTimeIface.(string)
	if !ok {
		return nil, oops.Errorf("pipeline_execution_time is not a string: %v", inputPipelineExecutionTimeIface)
	}

	definitionVisitor := PipelineDefinitionVisitor{}
	finalNESFTask, err := definitionVisitor.Visit(pipelineExecution.Definition)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	nesfTaskByState := make(map[string]*NESFTask)
	nesfTaskStatus := make(map[string]ExecutionStatus)
	nodeSet := make(map[string]struct{})
	{
		// Walk NESF task graph and collect all NESF tasks and pipeline node names.
		stack := []*NESFTask{finalNESFTask}
		for len(stack) != 0 {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			nesfTaskByState[last.StateName] = last
			nodeSet[last.NodeName] = struct{}{}

			// Mark node as failed if the NESF task has failed. This correctly marks
			// nodes as failed rather than pending when the failed node is unable to
			// write to the metastore.
			if state, ok := pipelineExecution.States[last.StateName]; ok {
				if state.Status.Failed() {
					nesfTaskStatus[last.NodeName] = state.Status
				}
			}

			stack = append(stack, last.Parents...)
		}
	}

	// Describe NESF execution status in Metastore.
	nesfStatuses := make(map[string]*NESFExecutionStatus)
	for nodeName := range nodeSet {
		status, err := c.describeNESFExecutionStatus(ctx, nodeName, executionId)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		nesfStatuses[nodeName] = status
	}

	nodeExecutionMap := make(map[string]*NodeExecution)
	for nodeName, status := range nesfStatuses {
		nodeExecution := NodeExecution{
			Name: nodeName,
		}
		if status == nil {
			nodeExecution.Status = ExecutionStatusNotReady
		} else {
			nodeExecution.ExecutionArn = status.NESFExecutionArn
			nodeExecution.StartTime = &status.ExecutionStart
			if status.ExecutionEnd != nil {
				nodeExecution.EndTime = status.ExecutionEnd
			}
			if status.ResultState == nil {
				nodeExecution.Status = ExecutionStatusRunning
			} else {
				switch *status.ResultState {
				case "SUCCESS":
					nodeExecution.Status = ExecutionStatusSucceeded
				case "FAILED":
					nodeExecution.Status = ExecutionStatusFailed
				}
			}
		}

		if !nodeExecution.Status.Failed() {
			// Override status if the NESF or wrapper call has failed, which may not
			// have been recorded in the metastore.
			if status, ok := nesfTaskStatus[nodeName]; ok {
				nodeExecution.Status = status
			}
		}

		nodeExecutionMap[nodeName] = &nodeExecution
	}

	for _, task := range nesfTaskByState {
		nodeExecution := nodeExecutionMap[task.NodeName]
		for _, parent := range task.Parents {
			nodeExecution.PrevNodes = append(nodeExecution.PrevNodes, parent.NodeName)
			parentNodeExecution := nodeExecutionMap[parent.NodeName]
			parentNodeExecution.NextNodes = append(parentNodeExecution.NextNodes, task.NodeName)
		}
	}

	nodeExecutions := make([]*NodeExecution, 0, len(nodeExecutionMap))
	for _, nodeExecution := range nodeExecutionMap {
		nodeExecutions = append(nodeExecutions, nodeExecution)
	}
	sort.Slice(nodeExecutions, func(i, j int) bool {
		return nodeExecutions[i].Name < nodeExecutions[j].Name
	})

	return &PipelineExecutionDetails{
		ExecutionArn:        executionArn,
		PipelineName:        pipelineExecution.StateMachineName,
		StateMachineArn:     pipelineExecution.StateMachineArn,
		ExecutionId:         executionId,
		DefinitionUpdatedAt: pipelineExecution.DefinitionUpdatedAt,
		StartTime:           pipelineExecution.StartTime,
		EndTime:             pipelineExecution.EndTime,
		Status:              pipelineExecution.Status,
		Nodes:               nodeExecutions,
		Input: &PipelineExecutionInput{
			StartDate:             inputStartDate,
			EndDate:               inputEndDate,
			PipelineExecutionTime: inputPipelineExecutionTime,
		},
	}, nil
}

func (c *Client) DescribeNodeExecution(ctx context.Context, executionArn string) (*NodeExecutionDetails, error) {
	execution, err := c.describeStepFunctionExecution(ctx, executionArn)
	if err != nil {
		return nil, oops.Wrapf(err, "describe execution: %s", executionArn)
	}

	pollerState, ok := execution.States["databricks_poller"]
	if !ok {
		return nil, oops.Errorf("databricks_poller not found: %s", executionArn)
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
			slog.Infow(ctx, "run_page_url missing from poller and sql transformation is not found", "executionArn", executionArn)
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
			slog.Infow(ctx, "run_page_url missing from errored sql transformation state", "executionArn", executionArn)
			return nil, nil
		}
	} else {
		if err := json.Unmarshal([]byte(pollerState.Output), &output); err != nil {
			return nil, oops.Wrapf(err, "parse poller output")
		}

		if output.RunPageUrl == "" {
			return nil, oops.Errorf("run_page_url is empty: %s", executionArn)
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

func (c *Client) ListNodeExecutions(ctx context.Context, sfnArn string, limit int) ([]*NodeExecution, error) {
	var executions []*NodeExecution
	if err := c.SFN.ListExecutionsPagesWithContext(ctx, &sfn.ListExecutionsInput{
		StateMachineArn: aws.String(sfnArn),
		MaxResults:      aws.Int64(int64(limit)),
	}, func(output *sfn.ListExecutionsOutput, _ bool) bool {
		for _, entry := range output.Executions {
			executions = append(executions, &NodeExecution{
				ExecutionArn: entry.ExecutionArn,
				StartTime:    entry.StartDate,
				EndTime:      entry.StopDate,
				Status:       mapSFNStatus(aws.StringValue(entry.Status)),
			})
		}
		if len(executions) >= limit {
			return false
		}
		return true
	}); err != nil {
		return nil, oops.Wrapf(err, "list node executions: arn=%s", sfnArn)
	}
	return executions, nil
}

func (c *Client) StartPipelineExecution(ctx context.Context, startDate string, endDate string, sfnArn string, shardDuration int) (string, error) {
	endDateLimit := time.Now().UTC().AddDate(0, 0, -2)
	endDateTime, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return "", oops.Errorf("Cannot parse end date. Please enter a valid end date in the format YYYY-MM-DD")
	}

	startDateTime, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return "", oops.Errorf("Cannot parse start date. Please enter a valid end date in the format YYYY-MM-DD")
	}

	if endDateTime.After(endDateLimit) {
		return "", oops.Errorf("Cannot specify end_date greater than two days ago. This will conflict with ongoing merge jobs, please see runbook for more details")
	}

	if startDate > endDate {
		return "", oops.Errorf("Please specify a start date less than the end date")
	}

	currTime := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	var output *sfn.StartExecutionOutput
	if shardDuration > 0 {
		if strings.Contains(sfnArn, "albert") || strings.Contains(sfnArn, "parth") {
			canShard, err := canBackfillSharded(sfnArn)
			if err != nil {
				return "", oops.Wrapf(err, "Error identifying if backfill can be done in a sharded manner")
			}

			if !canShard {
				return "", oops.Errorf("Cannot do a sharded backfill. One or more nodes in this pipeline are not partitioned by date. All nodes in a pipeline must be partitioned by date for a sharded backfill to successfully execute on that pipeline.")
			}
		}

		// Shard backfills by dividing time range into shardDuration increments
		// Start a different execution for each shardDuration increment
		currEndDateTime := startDateTime.AddDate(0, 0, shardDuration)
		var executionId string
		for {
			quotedStartDate := "'" + startDate + "'"
			executionId = samuuid.NewUuid().String()
			if currEndDateTime.After(endDateTime) {
				currEndDateTime = endDateTime
			}
			quotedEndDate := "'" + currEndDateTime.Format("2006-01-02") + "'"
			argsMap, err := json.Marshal(map[string]string{
				"end_date":                quotedEndDate,
				"execution_id":            executionId,
				"pipeline_execution_time": currTime,
				"start_date":              quotedStartDate,
			})
			if err != nil {
				return "", oops.Wrapf(err, "Could not marshal step function inputs into json")
			}
			args := string(argsMap)
			input := sfn.StartExecutionInput{Input: aws.String(args), Name: aws.String(executionId), StateMachineArn: aws.String(sfnArn)}
			output, err = c.SFN.StartExecutionWithContext(ctx, &input)
			if err != nil {
				return "", oops.Wrapf(err, "Start pipeline execution failed: sfnarn=%s, start_date=%s, end_date=%s", sfnArn, startDate, endDate)
			}
			if currEndDateTime.Equal(endDateTime) {
				break
			}
			startDate = currEndDateTime.Format("2006-01-02")
			currEndDateTime = currEndDateTime.AddDate(0, 0, 30)
		}
	} else {
		quotedStartDate := "'" + startDate + "'"
		// Create an execution id, get the current time, and call sfn api to start the execution
		executionId := samuuid.NewUuid().String()
		quotedEndDate := "'" + endDate + "'"
		argsMap, err := json.Marshal(map[string]string{
			"end_date":                quotedEndDate,
			"execution_id":            executionId,
			"pipeline_execution_time": currTime,
			"start_date":              quotedStartDate,
		})
		if err != nil {
			return "", oops.Wrapf(err, "Could not marshal step function inputs into json")
		}
		args := string(argsMap)
		input := sfn.StartExecutionInput{Input: aws.String(args), Name: aws.String(executionId), StateMachineArn: aws.String(sfnArn)}
		output, err = c.SFN.StartExecutionWithContext(ctx, &input)
		if err != nil {
			return "", oops.Wrapf(err, "Start pipeline execution failed: sfnarn=%s, start_date=%s, end_date=%s", sfnArn, startDate, endDate)
		}
	}

	// Parse the returned execution arn to create the URl
	execArn := aws.StringValue(output.ExecutionArn)
	parsed, err := arn.Parse(execArn)
	if err != nil {
		return "", oops.Wrapf(err, "parse: %s", execArn)
	}
	url := fmt.Sprintf("https://%s.console.aws.amazon.com/states/home?region=%s#/executions/details/%s", parsed.Region, parsed.Region, execArn)
	return url, nil
}

// Returns boolean representing whether this pipeline can be backfilled using a sharded mechanism or not
func canBackfillSharded(sfnArn string) (bool, error) {
	// Find name of pipeline from sfnArn
	parsedArn, err := arn.Parse(sfnArn)
	if err != nil {
		return false, oops.Wrapf(err, "Cannot parse sfnArn: %s", sfnArn)
	}

	pipelineName := strings.Split(parsedArn.Resource, ":")[1]

	// Get list of nodes associated with pipeline
	var nodes []nodetypes.Node

	allDags, err := graphs.BuildDAGs()
	if err != nil {
		return false, err
	}
	for _, dag := range allDags {
		sfnName := strings.ReplaceAll(util.HashTruncate(dag.Name(), 64, 10, "_"), ".", "-")
		if sfnName == pipelineName {
			nodes = dag.GetNodes()
			break
		}
	}

	// Figure out if all nodes are sharded by date
	for _, node := range nodes {
		if !node.Output().IsDateSharded() {
			return false, nil
		}
	}

	// Every node in pipeline is sharded by date, we can backfill in a sharded manner
	return true, nil
}
