package datapipelineerrors

import (
	"strings"
)

type ErrorCategory string

const (
	INFRA_FAILURE   ErrorCategory = "infra_failure"
	DEVELOPER_ERROR ErrorCategory = "developer_error"
)

type ErrorDefinition struct {
	ErrorMatchString string        `json:"error_match_string"`
	ErrorType        string        `json:"error_type"`
	ErrorCategory    ErrorCategory `json:"error_category"`
	SpotError        bool          `json:"spot_error"`
	AWSCapacityError bool          `json:"capacity_error"`
}

// errors must be correctly ordered. The first match wins.
var errors = []ErrorDefinition{
	{
		ErrorMatchString: "aws_error_message:There is no Spot capacity available that matches your request.",
		ErrorType:        "insufficient_spot_capacity",
		ErrorCategory:    INFRA_FAILURE,
		SpotError:        true,
		AWSCapacityError: true,
	},
	{
		ErrorMatchString: "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
		ErrorType:        "insufficient_instance_capacity",
		ErrorCategory:    INFRA_FAILURE,
		AWSCapacityError: true,
	},
	{
		ErrorMatchString: "aws_api_error_code:InsufficientInstanceCapacity,aws_error_message:We currently do not have sufficient",
		ErrorType:        "insufficient_instance_capacity",
		ErrorCategory:    INFRA_FAILURE,
		AWSCapacityError: true,
	},
	{
		ErrorMatchString: "Cannot up cast",
		ErrorType:        "casting_issue",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "The metadata of the Delta table has been changed by a concurrent update",
		ErrorType:        "concurrent_metadata_update",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "com.databricks.sql.io.FileReadException: Error while reading file",
		ErrorType:        "file_read_error",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "AWS_MAX_SPOT_INSTANCE_COUNT_EXCEEDED_FAILURE(CLIENT_ERROR)",
		ErrorType:        "max_spot_instance_count",
		ErrorCategory:    INFRA_FAILURE,
		SpotError:        true,
		AWSCapacityError: true,
	},
	{
		ErrorMatchString: "SPARK_STARTUP_FAILURE(SERVICE_FAULT)",
		ErrorType:        "spark_start_up_failure",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Database being modified concurrently",
		ErrorType:        "concurrent_database_modification",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "AWS_REQUEST_LIMIT_EXCEEDED(CLOUD_FAILURE)",
		ErrorType:        "aws_request_limit_exceeded",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "com.databricks.sql.transaction.tahoe.ConcurrentAppendException",
		ErrorType:        "concurrent_partition_update",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "NoSuchKey: An error occurred (NoSuchKey) when calling the GetObject operation:",
		ErrorType:        "s3_no_such_key_error",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "duplicate primary key matches found.",
		ErrorType:        "duplicate_primary_keys",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "NULL primary keys found",
		ErrorType:        "null_primary_keys",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "AnalysisException: Data written out does not match replaceWhere",
		ErrorType:        "replace_where_partition_issue",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "org.apache.spark.SparkException: Job aborted",
		ErrorType:        "spark_error",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Cannot read the python file",
		ErrorType:        "python_file_read_error",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Transformation output failed user-specified expectation rule",
		ErrorType:        "transformation_expectation_issue",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden",
		ErrorType:        "table_permission_error",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "AnalysisException: Table or view not found",
		ErrorType:        "table_or_view_not_found",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "overwrite mode only supported for date partitioned and non-partitioned tables",
		ErrorType:        "invalid_overwrite_mode",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "databricks_error_message:Self-bootstrap failure during launch",
		ErrorType:        "bootstrap_failure",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "SELF_BOOTSTRAP_FAILURE",
		ErrorType:        "bootstrap_failure",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "BOOTSTRAP_TIMEOUT",
		ErrorType:        "bootstrap_timeout",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		// This is a catch-all error message for all other cluster startup errors
		ErrorMatchString: "Unexpected failure while waiting for the cluster",
		ErrorType:        "cluster_startup_error_catch_all",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Failed to merge incompatible data types",
		ErrorType:        "incompatible_data_types",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "AnalysisException: A schema mismatch detected when writing to the Delta table",
		ErrorType:        "schema_mismatch",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "Max retries exceeded with url",
		ErrorType:        "rate_limited_exceeded",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "java.net.URISyntaxException: Relative path in absolute URI",
		ErrorType:        "uri_syntax_error",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Datadog returned a bad HTTP response code: 503 - Service Unavailable",
		ErrorType:        "datadog_unavailable",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "DELTA_TABLE_NOT_FOUND",
		ErrorType:        "delta_table_not_found",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "[DELTA_FAILED_RECOGNIZE_PREDICATE] Cannot recognize the predicate 'manifest IN ()'",
		ErrorType:        "no_manifest_data_files",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "Job aborted due to stage failure: A shuffle map stage with indeterminate output was failed and retried",
		ErrorType:        "spark_shuffle_map_stage_failure",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "DELTA_CONCURRENT_APPEND",
		ErrorType:        "delta_concurrent_append",
		ErrorCategory:    INFRA_FAILURE,
	},
	{
		ErrorMatchString: "DELTA_REPLACE_WHERE_MISMATCH",
		ErrorType:        "delta_replace_where_mismatch",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "TABLE_OR_VIEW_NOT_FOUND",
		ErrorType:        "table_or_view_not_found",
		ErrorCategory:    DEVELOPER_ERROR,
	},
	{
		ErrorMatchString: "SPOT_INSTANCE_TERMINATION",
		ErrorType:        "spot_instance_termination",
		ErrorCategory:    INFRA_FAILURE,
		SpotError:        true,
	},
}

func GetErrorClassification(errorMsg string) ErrorDefinition {
	for _, e := range errors {
		if strings.Contains(strings.ToLower(errorMsg), strings.ToLower(e.ErrorMatchString)) {
			return e
		}
	}

	return ErrorDefinition{
		ErrorType:     "unknown",
		ErrorCategory: "unknown",
	}
}
