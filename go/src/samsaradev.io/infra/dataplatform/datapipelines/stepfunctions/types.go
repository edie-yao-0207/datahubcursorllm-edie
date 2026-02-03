package stepfunctions

// GetBackfillInput takes in a map of backfill sfn input and adds on common fields and returns the map
func GetBackfillInput(input map[string]interface{}) map[string]interface{} {
	input["AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$"] = "$$.Execution.Id"
	input["node_id.$"] = "$.node_id"
	input["transformation_name.$"] = "$.transformation_name"
	input["pipeline_execution_time.$"] = "$.pipeline_execution_time"
	input["s3_sql_path.$"] = "$.s3_sql_path"
	input["s3_json_metadata_path.$"] = "$.s3_json_metadata_path"
	input["s3_expectation_path.$"] = "$.s3_expectation_path"
	input["team.$"] = "$.team"
	input["product_group.$"] = "$.product_group"
	input["backfill_start_date.$"] = "$.backfill_start_date"
	input["databricks_owner_group.$"] = "$.databricks_owner_group"
	input["node_execution_environment.$"] = "$.node_execution_environment"
	input["geospark.$"] = "$.geospark"
	return input
}

func GetSubmitJobBackfillInput(input map[string]interface{}) map[string]interface{} {
	input["AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$"] = "$$.Execution.Id"
	input["node_id.$"] = "$.node_id"
	input["pipeline_execution_time.$"] = "$.pipeline_execution_time"
	input["backfill_start_date.$"] = "$.backfill_start_date"
	return input
}
