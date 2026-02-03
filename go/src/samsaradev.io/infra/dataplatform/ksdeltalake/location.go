package ksdeltalake

import (
	"fmt"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

type TableType string

const (
	TableTypeInvalid      = TableType("")
	TableTypeRaw          = TableType("raw")
	TableTypeDeduplicated = TableType("deduplicated")
	TableTypeByOrgDate    = TableType("byorgdate")
	TableTypeByDate       = TableType("bydate")
)

func TableLocation(region string, typ TableType, name string) string {
	return fmt.Sprintf("s3://%skinesisstats-delta-lake/table/%s/%s/", awsregionconsts.RegionPrefix[region], string(typ), name)
}

func S3BigStatTableLocation(region string, name string) string {
	return fmt.Sprintf("s3://%ss3bigstats-delta-lake/table/%s/%s/", awsregionconsts.RegionPrefix[region], string(TableTypeRaw), name)
}

func S3BigStatS3FilesLocation(region string, name string) string {
	return fmt.Sprintf("s3://%ss3bigstats-delta-lake/s3files/%s/", awsregionconsts.RegionPrefix[region], name)
}

func S3BigStatCheckpointLocation(region string, name string) string {
	return fmt.Sprintf("s3://%ss3bigstats-delta-lake/checkpoint/%s/", awsregionconsts.RegionPrefix[region], name)
}

func S3BigStatStreamOutputLocation(region string, name string) string {
	return fmt.Sprintf("s3://%ss3bigstats-delta-lake/write_stream_dummy_folder/%s/", awsregionconsts.RegionPrefix[region], name)
}

func S3FilesTableLocation(region string, tableName string, schedName dataplatformconsts.KsJobType) string {
	// schedName can be "all", "every-3hr", or "daily", depending on the job's
	// schedule.
	return fmt.Sprintf("s3://%skinesisstats-delta-lake/table/s3files/%s/%s/", awsregionconsts.RegionPrefix[region], tableName, schedName)
}

func S3RetentionConfigLocation(region string, tableName string) string {
	return fmt.Sprintf("s3://%skinesisstats-delta-lake/retention/%s/", awsregionconsts.RegionPrefix[region], tableName)
}
