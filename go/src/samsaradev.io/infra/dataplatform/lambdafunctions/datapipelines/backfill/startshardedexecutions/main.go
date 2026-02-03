// This lambda calls the SFN api in a loop to kick off multiple `shardDuration` length executions in parallel for Data Pipeline backfills

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/aws/aws-sdk-go/service/sfn/sfniface"
	"github.com/google/uuid"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

// This is how many days each shard of the backfill will run for
const shardDuration = 30

var sfnClient = sfn.New(session.New(util.RetryerConfig))

type BackfillNESFInput struct {
	BackfillStartDate     string `json:"backfill_start_date"`
	StartDate             string `json:"start_date"`
	EndDate               string `json:"end_date"`
	Team                  string `json:"team"`
	ProductGroup          string `json:"product_group"`
	StartedById           string `json:"AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID"`
	NodeId                string `json:"node_id"`
	TransformationName    string `json:"transformation_name"`
	PipelineExecutionTime string `json:"pipeline_execution_time"`
	SqlPath               string `json:"s3_sql_path"`
	JsonPath              string `json:"s3_json_metadata_path"`
	ExpectationPath       string `json:"s3_expectation_path"`
	DatabricksOwnerGroup  string `json:"databricks_owner_group"`
	NodeExecutionEnv      string `json:"node_execution_environment"`
	Geospark              bool   `json:"geospark"`
	ExecutionId           string `json:"execution_id"`
}

type StartShardedBackfillsInput struct {
	BackfillNESFInput
}

type StartShardedBackfillsOutput struct {
	ExecutionArns []string `json:"execution_arns"`
}

// This helper function splits a start and end date range into an array of end dates separated by `shardDuration` increments
func createEndDatesList(startDate string, endDate string) ([]string, error) {
	log.Printf("Creating end dates list for start_date: %s, end_date: %s ...", startDate, endDate)
	// Convert start date and end date strings into Time.time objects so they can be easily compared
	startDateObj, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, err
	}

	endDateObj, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, err
	}

	if startDateObj.After(endDateObj) {
		return nil, errors.New("Cannot have start date greater than end date")
	}

	allEndDates := []string{}
	for {
		currEndDateObj := startDateObj.AddDate(0, 0, shardDuration)

		// If last shard doesn't fit into 30 days, last shard = however many days left
		if currEndDateObj.After(endDateObj) {
			currEndDateObj = endDateObj
		}

		allEndDates = append(allEndDates, "'"+currEndDateObj.Format("2006-01-02")+"'")
		if currEndDateObj.Equal(endDateObj) {
			break
		}

		// Date range should be [`start_date`, `end_date`) as advised by the data pipelines how to guide
		// https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17757182/How+To+Use+the+Data+Pipelines+Framework#JSON-Metadata
		startDateObj = currEndDateObj
	}

	log.Printf("End date list created: %v", allEndDates)
	return allEndDates, nil
}

func getExecutionInputs(input StartShardedBackfillsInput) ([]StartShardedBackfillsInput, error) {
	log.Printf("Getting execution input for %s", input.NodeId)

	// Loop and for each iteration start a backfill execution for `start_date` ~ `start_date` + `shard_duration` date range
	endDates, err := createEndDatesList(strings.Trim(input.StartDate, "'"), strings.Trim(input.EndDate, "'"))
	if err != nil {
		return nil, err
	}

	executionInputs := []StartShardedBackfillsInput{}
	for _, endDate := range endDates {
		execId := uuid.New().String()

		input.EndDate = endDate
		input.ExecutionId = execId
		executionInputs = append(executionInputs, input)
		input.StartDate = endDate
	}

	log.Printf("Execution inputs created with length: %d", len(executionInputs))
	return executionInputs, nil
}

func startShardedExecutions(ctx context.Context, sfnApi sfniface.SFNAPI, input StartShardedBackfillsInput) (StartShardedBackfillsOutput, error) {
	// End date is inputted as DATE_SUB(CURRENT_DATE(), 2) which cannot be parsed and subtracted from
	// For now, manually set the end date since end date is always 2 days before today
	// For daily jobs, this will be DATE_ADD(CURRENT_DATE, -90).
	// TODO: Once custom end date is implemented, remove these
	// TODO: check for specific node names after fully testing
	if input.EndDate == "DATE_SUB(CURRENT_DATE(), 2)" {
		input.EndDate = time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02")
	} else if input.EndDate == "DATE_ADD(CURRENT_DATE(), -90)" && (strings.Contains(input.NodeId, "dataplatform") || strings.Contains(input.NodeId, "safety_report")) {
		input.EndDate = time.Now().UTC().AddDate(0, 0, -90).Format("2006-01-02")
	}

	// When unmarshalling input, the start date we want is called `backfill_start_date`
	// However, when marshalling input into nesf, we want `start_date`
	// Thus, use StartDate variable for rest of lambda
	input.StartDate = input.BackfillStartDate

	startExecutionInputs, err := getExecutionInputs(input)
	if err != nil {
		return StartShardedBackfillsOutput{}, err
	}

	// Parse execution started by ID ARN to get get the region, then put together backfill nesf ARN
	parsedArn, err := arn.Parse(input.StartedById)
	if err != nil {
		return StartShardedBackfillsOutput{}, err
	}

	sfnArn := fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:sql-transformation-backfill-node", parsedArn.Region, parsedArn.AccountID)

	executionArns := []string{}
	for _, apiInput := range startExecutionInputs {
		argsMap, err := json.Marshal(apiInput)
		if err != nil {
			return StartShardedBackfillsOutput{}, err
		}

		args := string(argsMap)
		output, err := sfnApi.StartExecutionWithContext(ctx, &sfn.StartExecutionInput{Input: aws.String(args), Name: aws.String(apiInput.ExecutionId), StateMachineArn: aws.String(sfnArn)})
		if err != nil {
			return StartShardedBackfillsOutput{}, err
		}
		log.Printf("backfill nesf successfully started for %s", aws.StringValue(output.ExecutionArn))

		executionArns = append(executionArns, aws.StringValue(output.ExecutionArn))
	}

	return StartShardedBackfillsOutput{ExecutionArns: executionArns}, nil
}

func HandleRequest(ctx context.Context, input StartShardedBackfillsInput) (StartShardedBackfillsOutput, error) {
	return startShardedExecutions(ctx, sfnClient, input)
}

func main() {
	middleware.StartWrapped(HandleRequest, middleware.LogInputOutput)
}
