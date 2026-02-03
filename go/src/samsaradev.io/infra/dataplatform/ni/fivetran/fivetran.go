package fivetran

var FivetranCustomSparkConfigurations = map[string]string{
	"spark.hadoop.fs.s3.impl":                               "shaded.databricks.org.apache.hadoop.fs.s3a.S3AFileSystem",
	"spark.hadoop.fs.s3a.impl":                              "shaded.databricks.org.apache.hadoop.fs.s3a.S3AFileSystem",
	"spark.hadoop.fs.s3n.impl":                              "shaded.databricks.org.apache.hadoop.fs.s3a.S3AFileSystem",
	"spark.hadoop.fs.s3a.acl.default":                       "BucketOwnerFullControl",
	"spark.databricks.delta.autoCompact.enabled":            "true",
	"spark.hadoop.fs.s3.impl.disable.cache":                 "true",
	"spark.hadoop.fs.s3a.impl.disable.cache":                "true",
	"spark.hadoop.fs.s3n.impl.disable.cache":                "true",
	"spark.databricks.delta.alterTable.rename.enabledOnAWS": "true",
	"spark.databricks.delta.optimizeWrite.enabled":          "true",
	"spark.databricks.hive.metastore.glueCatalog.enabled":   "true",
	"spark.driver.maxResultSize":                            "16g",
}

var FivetranCustomSparkEnvVars = map[string]string{
	"AWS_STS_REGIONAL_ENDPOINTS": "us-east-1",
}
