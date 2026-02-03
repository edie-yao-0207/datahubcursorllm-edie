package dataplatformresource

import (
	"fmt"
	"strconv"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func concatenateStringMap(maps ...map[string]string) map[string]string {
	ret := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			// If spark.sql.extensions is already set sparkconfs map, *append* new extensions to the entry instead of overwriting the entry.
			// If the new value is empty, overwrite the entire entry with the empty value. We set "spark.sql.extensions" to empty for
			// the admin cluster because it should have full access control and not be limited by Spark Extensions.
			existingValue, entryExists := ret[k]
			if k == "spark.sql.extensions" && entryExists && existingValue != "" && v != "" {
				ret[k] = existingValue + "," + v
			} else {
				ret[k] = v
			}
		}
	}
	return ret
}

type SparkConf struct {
	Region string

	// Enable delta.io metadata caching to reduce latency in repeatedly queries.
	EnableCaching bool

	// Override managed table location to playground bucket.
	EnablePlaygroundWarehouse bool

	// Enable sparkrules extension. See scala/sparkrules/README.md.
	EnableExtension bool

	// Enable BigQuery connector. Note the credentials must be installed via an
	// init script and some environment variables may be required.
	EnableBigQuery bool

	// EnableAggressiveSkewJoinOptimization lowers skew threshold in skew join
	// optimization.
	EnableAggressiveSkewJoinOptimization bool

	// Enable high concurrency settings.
	EnableHighConcurrency bool

	// Enable multiple results in a cell.
	EnableMultipleResults bool

	Overrides map[string]string

	// Disable standard Spark Configurations
	DisableStockSparkConf bool

	// DisableQueryWatchdog disables Databricks' built-in large query protection.
	// Often large queries are intentional in automated jobs, so we provide a knob
	// to disable it.
	DisableQueryWatchdog bool

	// Spark configuration property specifies in seconds how often a cluster makes
	// down-scaling decisions. Increasing the value causes a cluster to scale down
	// more slowly. The default varies by cluster type.
	AggressiveWindowDownSeconds int

	NetworkTimeoutSeconds int

	// Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes
	MaxResultSize string

	// We need to know the data security mode being used on the cluster to decide which spark confs to use.
	DataSecurityMode string

	// Disables file modification checks introduced in DBR 13.3 and above.
	DisableFileModificationCheck bool
}

func (c SparkConf) ToMap() map[string]string {
	m := make(map[string]string)

	if !c.DisableStockSparkConf {
		// Start with some required defaults.
		m = concatenateStringMap(m, map[string]string{
			// See: https://docs.databricks.com/administration-guide/cloud-configurations/aws/iam-roles.html#step-7-update-cross-account-s3-object-acls
			"spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
			"spark.hadoop.fs.s3a.canned.acl":  "BucketOwnerFullControl",

			// Optimize S3 writes. This reduces number of S3 files written which saves S3 cost.
			"spark.databricks.delta.optimizeWrite.enabled": "true",
			"spark.databricks.delta.autoCompact.enabled":   "true",

			// Enable merge metrics (available in 6.4). It tells us rows deleted, updated, and inserted in each MERGE.
			"spark.databricks.delta.history.metricsEnabled": "true",

			// Keep transaction logs in last 30 days, including checkpoint files.
			// This configuration only applies to new tables.
			// https://github.com/delta-io/delta/blob/f573ce7e272f2ca0f1085fa4e7a3f35f305a9d2c/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala#L264
			"spark.databricks.delta.properties.defaults.checkpointRetentionDuration": "INTERVAL 30 DAYS", // Default without override is 2 days.
			"spark.databricks.delta.properties.defaults.logRetentionDuration":        "INTERVAL 30 DAYS", // Default without override is 30 days.

			// Disable delta table format check. Otherwise Databricks Runtime
			// recursively walks parent parents in S3 to look for "delta_log" when we
			// read non-delta tables. If we have restricted S3::ListObject to the
			// table's prefix, these checks will fail with "Forbidden" rather than
			// "NoSuchKey" and break the query. Plus, we do a pretty good job of
			// requiring every table to be a delta table, so which is unnecessary.
			"spark.databricks.delta.formatCheck.enabled": "false",

			// Some Spark operations like JOINs and MERGEs include a shuffle stage
			// where the entire RDD is hash into a fixed number of partitions. A
			// partition is a unit of work that is processed by a single thread. The
			// default partition count is 200, which is often less than the total
			// number of cores available in a cluster, especially in a large backfill
			// with 100+ nodes. By increasing this number significantly, we can ensure
			// that we don't waste resources in large clusters. This adds a small
			// overhead for small clusters, but it's better than wasting resources.
			//
			// Databricks also recommends us to set a high initial partition count, and let
			// Adaptive Query Execution (AQE) coalesce small partitions if needed.
			// https://docs.databricks.com/spark/latest/spark-sql/aqe.html#why-didnt-aqe-change-the-shuffle-partition-number-despite-the-partition-coalescing-already-being-enabled
			"spark.sql.shuffle.partitions": "4096",

			// Spark decommission reduces the impact of losing spot instances. The cloud provider sends a
			// notification (~30sec-2min) before a spot instance is decommissioned. Spark decommission attempts
			// to migrate shuffle data to healthy executors. Additionally, task failures caused by spot instance
			// preemption are not added to the total number of failed attempts when decommissioning is enabled.
			// Note that this is only available from DBR 8.0+, but has been tested below that and has no effect, so is
			// safe to enable across the board.
			// https://docs.databricks.com/clusters/clusters-manage.html#decommission-spot-instances
			"spark.decommission.enabled":                       "true",
			"spark.storage.decommission.enabled":               "true",
			"spark.storage.decommission.shuffleBlocks.enabled": "true",
			"spark.storage.decommission.rddBlocks.enabled":     "true",
		})

		if c.EnableCaching {
			m = concatenateStringMap(m, map[string]string{
				// Set TTL on delta table metadata cache.
				"spark.databricks.delta.stalenessLimit": "15m",
			})
		}

		if c.DisableFileModificationCheck {
			// After DBR 13.3, databricks introduced a check that will return an error if a file is
			// updated between query planning and invocation. This causes jobs to fail.
			// By setting this configs, we are enforcing previously accepted behavhiour and letting jobs succeed.
			// This is because of widely used  s3://samsara-prod-app-configs/org-shards-csv/all.csv. This file gets
			// updated very frequently and used in lot of views and queries.
			m = concatenateStringMap(m, map[string]string{
				"databricks.loki.fileStatusCache.enabled":              "false",
				"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
				"spark.databricks.scan.modTimeCheck.enabled":           "false",
			})
		}

		if c.EnablePlaygroundWarehouse {
			m = concatenateStringMap(m, map[string]string{
				// Override default warehouse location.
				"spark.sql.warehouse.dir": fmt.Sprintf("s3://%sdatabricks-playground/warehouse/", awsregionconsts.RegionPrefix[c.Region]),
			})
		}

		if c.EnableExtension {
			m = concatenateStringMap(m, map[string]string{
				// Samsara sparkrules extension.
				"spark.sql.extensions": "com.samsara.dataplatform.sparkrules.Extension",
				// Added based on https://samsara-rd.slack.com/archives/G010UNRQGFN/p1726606143611219. We don't want to
				// have any partition filtering rules permeated by other processes
				"spark.samsara.partitionFilterRule.enabled": "false",
			})
		}

		if c.EnableBigQuery {
			m = concatenateStringMap(m, map[string]string{
				// spark-bigquery-connector configurations.
				// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/#properties
				"viewMaterializationDataset": "spark_view_materialization",
				"temporaryGcsBucket":         "samsara-bigquery-spark-connector",
				"viewsEnabled":               "true",
				"credentialsFile":            databricks.BigQueryCredentialsLocation(),

				// GCP Hadoop connector configurations.
				"spark.hadoop.google.cloud.auth.service.account.enable":       "true",
				"spark.hadoop.fs.gs.project.id":                               "samsara-data",
				"spark.hadoop.google.cloud.auth.service.account.json.keyfile": databricks.BigQueryCredentialsLocation(),
			})
		}

		if c.EnableAggressiveSkewJoinOptimization {
			m = concatenateStringMap(m, map[string]string{
				"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64MB",
				"spark.sql.adaptive.skewJoin.skewedPartitionFactor":           "2",
			})
		}

		if c.EnableHighConcurrency {
			// These configurations are useful regardless of unity catalog.
			m = concatenateStringMap(m, map[string]string{
				"spark.databricks.service.server.enabled": "true",

				// Read delta format by default.
				"spark.sql.sources.default": "delta",
			})

			// These settings are only meaningful when unity catalog is not enabled.
			if c.DataSecurityMode == "" {
				m = concatenateStringMap(m, map[string]string{
					// "High Concurrency" mode.
					// This is a legacy setting that we'll stop setting once all clusters explicitly
					// set the data security mode to NONE.
					"spark.databricks.cluster.profile": "serverless",
					// SQL or Python only.
					"spark.databricks.repl.allowedLanguages": "sql,python",
				})
			} else if c.DataSecurityMode == databricksresource.DataSecurityModeNone {
				m = concatenateStringMap(m, map[string]string{
					// SQL or Python only.
					"spark.databricks.repl.allowedLanguages": "sql,python",
				})
			}
		}

		if c.EnableMultipleResults {
			m = concatenateStringMap(m, map[string]string{
				"spark.databricks.workspace.multipleResults.enabled": "true",
			})
		}

		if c.DisableQueryWatchdog {
			m = concatenateStringMap(m, map[string]string{
				"spark.databricks.queryWatchdog.enabled": "false",
			})
		}

		if c.AggressiveWindowDownSeconds != 0 {
			m = concatenateStringMap(m, map[string]string{
				"spark.databricks.aggressiveWindowDownS": strconv.Itoa(c.AggressiveWindowDownSeconds),
			})
		}

		if c.NetworkTimeoutSeconds != 0 {
			m = concatenateStringMap(m, map[string]string{
				"spark.network.timeout": strconv.Itoa(c.NetworkTimeoutSeconds),
			})
		}

		if c.MaxResultSize != "" {
			m = concatenateStringMap(m, map[string]string{
				"spark.driver.maxResultSize": c.MaxResultSize,
			})
		}

		// In unity catalog clusters, set the catalog `default` as the default catalog.
		// (workspace setting at this time is hive_metastore). And in non-UC clusters
		// make sure to keep glue enabled.
		if c.DataSecurityMode == databricksresource.DataSecurityModeUserIsolation || c.DataSecurityMode == databricksresource.DataSecurityModeSingleUser {
			m = concatenateStringMap(m, map[string]string{
				"spark.databricks.sql.initial.catalog.name": "default",
			})
		} else {
			m = concatenateStringMap(m, map[string]string{
				// Use Glue Catalog to enforce table access via IAM.
				"spark.databricks.hive.metastore.glueCatalog.enabled": "true",
				"spark.hadoop.aws.glue.max-error-retries":             "10", // Default without override is 5
				// Max concurrent API calls per cluster = num.segments * pool.size
				"spark.hadoop.aws.glue.partition.num.segments":     "1", // Default without override is 5
				"spark.databricks.hive.metastore.client.pool.size": "3", // Default without override is 5
				// For non-UC clusters, we need to set the initial catalog name to hive_metastore.
				"spark.databricks.sql.initial.catalog.name": "hive_metastore",
			})
		}
	}

	m = concatenateStringMap(m, c.Overrides)

	return m
}
