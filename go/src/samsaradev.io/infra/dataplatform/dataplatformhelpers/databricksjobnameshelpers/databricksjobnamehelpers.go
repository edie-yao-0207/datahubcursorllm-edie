package databricksjobnameshelpers

import (
	"fmt"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

type GetRDSMergeJobNameInput struct {
	Shard        string
	MySqlDb      string
	TableName    string
	TableVersion int
	Region       string // If left blank, the region suffix will not be returned (b/c dataplatformresource.JobSpec adds it automatically)
}

func GetRDSMergeJobName(input GetRDSMergeJobNameInput) string {
	jobName := fmt.Sprintf("rds-merge-%s-%s-%s-v%d", input.Shard, input.MySqlDb, input.TableName, input.TableVersion)
	if input.Region != "" {
		jobName = fmt.Sprintf("%s-%s", jobName, dataplatformresource.RegionLabels[input.Region])
	}
	return jobName
}

func GetRDSParquetMergeJobName(input GetRDSMergeJobNameInput) string {
	jobName := fmt.Sprintf("rds-parquet-merge-%s-%s-%s-v%d", input.Shard, input.MySqlDb, input.TableName, input.TableVersion)
	if input.Region != "" {
		jobName = fmt.Sprintf("%s-%s", jobName, dataplatformresource.RegionLabels[input.Region])
	}
	return jobName
}

type GetKSMergeJobNameInput struct {
	TableName   string
	JobSchedule dataplatformconsts.KsJobType
	Region      string
}

func GetKSMergeJobName(input GetKSMergeJobNameInput) string {
	jobName := fmt.Sprintf("kinesisstats-merge-%s-%s", input.TableName, input.JobSchedule)
	if input.Region != "" {
		jobName = fmt.Sprintf("%s-%s", jobName, dataplatformresource.RegionLabels[input.Region])
	}
	return jobName
}

type GetDynamoDBMergeJobNameInput struct {
	TableName string
	Region    string
}

func GetDynamoDBMergeJobName(input GetDynamoDBMergeJobNameInput) string {
	return fmt.Sprintf("dynamodb-merge-%s-%s", input.TableName, dataplatformresource.RegionLabels[input.Region])
}

type GetEmrReplicationMergeJobNameInput struct {
	Cell   string
	Region string
	Entity string
}

func GetEmrReplicationMergeJobName(input GetEmrReplicationMergeJobNameInput) string {
	return fmt.Sprintf("emr-replication-merge-%s-%s-%s", input.Entity, input.Cell, dataplatformresource.RegionLabels[input.Region])
}

type GetEmrValidationJobNameInput struct {
	Cell   string
	Region string
	Entity string
}

func GetEmrValidationJobName(input GetEmrValidationJobNameInput) string {
	return fmt.Sprintf("emr-validation-%s-%s-%s", input.Entity, input.Cell, dataplatformresource.RegionLabels[input.Region])
}
