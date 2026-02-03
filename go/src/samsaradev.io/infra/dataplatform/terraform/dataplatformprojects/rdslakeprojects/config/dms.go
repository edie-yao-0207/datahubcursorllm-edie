// Shared configuration for rdsdeltalake.

package config

// WARNING! Anything imported here will be included in lambdas
import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
)

var DesiredDMSReplicationTaskSettings = map[string]interface{}{
	"TargetMetadata": map[string]interface{}{
		"SupportLobs":        aws.Bool(true),
		"FullLobMode":        aws.Bool(false),
		"LimitedSizeLobMode": aws.Bool(true),
		"LobMaxSize":         aws.Int(10240),
		"BatchApplyEnabled":  aws.Bool(false),
	},
	"FullLoadSettings": map[string]interface{}{
		"TargetTablePrepMode":           aws.String("DO_NOTHING"),
		"MaxFullLoadSubTasks":           aws.Int(8),
		"TransactionConsistencyTimeout": aws.Int(600),
		"CommitRate":                    aws.Int(50000),
	},
	"Logging": map[string]interface{}{
		"EnableLogging": aws.Bool(true),
		"LogComponents": []map[string]string{
			{
				"Id":       "SOURCE_UNLOAD",
				"Severity": "LOGGER_SEVERITY_DEFAULT",
			},
			{
				"Id":       "SOURCE_CAPTURE",
				"Severity": "LOGGER_SEVERITY_DEFAULT",
			},
			{
				"Id":       "TARGET_LOAD",
				"Severity": "LOGGER_SEVERITY_DEFAULT",
			},
			{
				"Id":       "TARGET_APPLY",
				"Severity": "LOGGER_SEVERITY_DEFAULT",
			},
			{
				"Id":       "TASK_MANAGER",
				"Severity": "LOGGER_SEVERITY_DEFAULT",
			},
		},
	},
}

// PostgresExtraConnAttributes are extra connection attributes for Postgres.
// This is used to exclude DDLs from the replication.
func PostgresExtraConnAttributes() string {
	attributes := map[string]string{
		"CaptureDdls": "false",
	}
	attributeParts := make([]string, 0, len(attributes))
	for key, value := range attributes {
		attributeParts = append(attributeParts, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(attributeParts)
	return strings.Join(attributeParts, ";") + ";"
}

// GenerateS3SettingsForDMSEndpoint generates the S3 settings for a DMS endpoint.
func GenerateS3SettingsForDMSEndpoint(accountId int, bucket string, prefix string, dbname string, taskType string) *awsresource.S3EndpointSettings {
	s3Settings := &awsresource.S3EndpointSettings{
		ServiceAccessRoleArn:          fmt.Sprintf("arn:aws:iam::%d:role/dms-replication-write-rds-export", accountId),
		BucketFolder:                  fmt.Sprintf("%s/%s", prefix, taskType),
		BucketName:                    bucket,
		DataFormat:                    "parquet",
		ParquetVersion:                "parquet-2-0",
		ParquetTimestampInMillisecond: false,
		CompressionType:               "NONE",
		CannedAclForObjects:           "bucket-owner-full-control",
		IncludeOpForFullLoad:          true,
		TimestampColumnName:           "_timestamp",
		DatePartitionDelimiter:        "DASH",
	}

	if taskType == "cdc" {
		s3Settings.DatePartitionEnabled = true
	}

	return s3Settings
}
