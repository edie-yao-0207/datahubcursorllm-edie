package dataplatformprojects

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// Test against a known working configuration just to make sure we did the string interpolation correctly
func TestDefaultJobClusterPolicy(t *testing.T) {
	usDefinition, err := defaultJobPolicyDefinition("databricks-dev-us", team.EngineeringOperations.TeamName, team.EngineeringOperations)
	require.NoError(t, err)
	euDefinition, err := defaultJobPolicyDefinition("databricks-dev-eu", team.EngineeringOperations.TeamName, team.EngineeringOperations)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(engopsDefinitionUS), usDefinition)
	assert.Equal(t, strings.TrimSpace(engopsDefinitionEU), euDefinition)
}

var engopsDefinitionUS = `
{
  "cluster_type": {
    "type": "fixed",
    "value": "job"
  },
  "spark_version": {
    "type": "fixed",
    "value": "15.4.x-scala2.12"
  },
  "node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "md-fleet.xlarge"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "rd-fleet.xlarge"
  },
  "custom_tags.samsara:team": {
    "type": "fixed",
    "value": "engineeringoperations"
  },
  "custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricksjobcluster-engineeringoperations"
  },
  "custom_tags.samsara:product-group": {
    "type": "fixed",
    "value": "engineeringoperations"
  },
  "custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "forbidden"
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.repl.allowedLanguages": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://samsara-databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  "spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::492164655156:instance-profile/unity-catalog-cluster"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://samsara-databricks-cluster-logs/engineeringoperations"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "us-west-2"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  "spark_env_vars.DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": {
    "type": "unlimited",
    "defaultValue": "team-engineeringoperations"
  },
  "spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "us-west-2"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "us-west-2"
  },
  "spark_conf.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.hadoop.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.databricks.scan.modTimeCheck.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "SINGLE_USER"
  },
  "spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  "init_scripts.0.volumes.destination": {
    "type": "fixed",
    "value": "/Volumes/s3/dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "default"
  }
}
`

var engopsDefinitionEU = `
{
  "cluster_type": {
    "type": "fixed",
    "value": "job"
  },
  "spark_version": {
    "type": "fixed",
    "value": "15.4.x-scala2.12"
  },
  "node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "md-fleet.xlarge"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "rd-fleet.xlarge"
  },
  "custom_tags.samsara:team": {
    "type": "fixed",
    "value": "engineeringoperations"
  },
  "custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricksjobcluster-engineeringoperations"
  },
  "custom_tags.samsara:product-group": {
    "type": "fixed",
    "value": "engineeringoperations"
  },
  "custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "forbidden"
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.repl.allowedLanguages": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  "spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::353964698255:instance-profile/unity-catalog-cluster"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-cluster-logs/engineeringoperations"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "eu-west-1"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  "spark_env_vars.DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": {
    "type": "unlimited",
    "defaultValue": "team-engineeringoperations"
  },
  "spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_conf.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.hadoop.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.databricks.scan.modTimeCheck.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "SINGLE_USER"
  },
  "spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  "init_scripts.0.volumes.destination": {
    "type": "fixed",
    "value": "/Volumes/s3/dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "default"
  }
}
`

func TestDefaultDLTClusterPolicy(t *testing.T) {
	usDefinition, err := deltaLiveTablesPolicyDefinition("databricks-dev-us", team.BizTechEnterpriseData.TeamName, team.BizTechEnterpriseData)
	require.NoError(t, err)
	euDefinition, err := deltaLiveTablesPolicyDefinition("databricks-dev-eu", team.BizTechEnterpriseData.TeamName, team.BizTechEnterpriseData)
	require.NoError(t, err)

	assert.Equal(t, strings.TrimSpace(biztechEnterpriseDataDefinitionUS), usDefinition)
	assert.Equal(t, strings.TrimSpace(biztechEnterpriseDataDefinitionEU), euDefinition)
}

var biztechEnterpriseDataDefinitionUS = `
{
  "cluster_type": {
    "type": "fixed",
    "value": "dlt"
  },
  "node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "md-fleet.xlarge"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "rd-fleet.xlarge"
  },
  "custom_tags.samsara:team": {
    "type": "fixed",
    "value": "biztechenterprisedata"
  },
  "custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricksdltcluster-biztechenterprisedata"
  },
  "custom_tags.samsara:product-group": {
    "type": "fixed",
    "value": "biztechenterprisedata"
  },
  "custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "fixed",
    "value": "10"
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://samsara-databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  "spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "regex",
    "defaultValue": "1",
    "pattern": "[1-5]"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "regex",
    "defaultValue": "3",
    "pattern": "([1-9]|1[0])"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::492164655156:instance-profile/biztechenterprisedata-cluster"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://samsara-databricks-cluster-logs/deltalivetables/biztechenterprisedata"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "us-west-2"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  "spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "us-west-2"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "us-west-2"
  },
  "spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  "spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  "init_scripts.0.s3.destination": {
    "type": "fixed",
    "value": "s3://samsara-dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "init_scripts.0.s3.region": {
    "type": "fixed",
    "value": "us-west-2"
  },
  "spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "hive_metastore"
  }
}
`

var biztechEnterpriseDataDefinitionEU = `
{
  "cluster_type": {
    "type": "fixed",
    "value": "dlt"
  },
  "node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "md-fleet.xlarge"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "rd-fleet.xlarge"
  },
  "custom_tags.samsara:team": {
    "type": "fixed",
    "value": "biztechenterprisedata"
  },
  "custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricksdltcluster-biztechenterprisedata"
  },
  "custom_tags.samsara:product-group": {
    "type": "fixed",
    "value": "biztechenterprisedata"
  },
  "custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "fixed",
    "value": "10"
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  "spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "regex",
    "defaultValue": "1",
    "pattern": "[1-5]"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "regex",
    "defaultValue": "3",
    "pattern": "([1-9]|1[0])"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::353964698255:instance-profile/biztechenterprisedata-cluster"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-cluster-logs/deltalivetables/biztechenterprisedata"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "eu-west-1"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  "spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  "spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  "init_scripts.0.s3.destination": {
    "type": "fixed",
    "value": "s3://samsara-eu-dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "init_scripts.0.s3.region": {
    "type": "fixed",
    "value": "eu-west-1"
  },
  "spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "hive_metastore"
  }
}
`

func TestUCJobClusterPolicy(t *testing.T) {
	testArn := "arn:aws:iam::2222:instance-profile/unity-catalog-cluster"
	euDefinition := defaultPolicyDefinition(true, "job", "unity-catalog-automated-job-cluster", testArn, infraconsts.SamsaraAWSEURegion, team.DataPlatform)
	assert.Equal(t, strings.TrimSpace(ucJobPolicyDefinitionEU), strings.TrimSpace(euDefinition))
}

var ucJobPolicyDefinitionEU = `
{
  "cluster_type": {
    "type": "fixed",
    "value": "job"
  },
  "spark_version": {
    "type": "fixed",
    "value": "15.4.x-scala2.12"
  },
  "node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "md-fleet.xlarge"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
    "defaultValue": "rd-fleet.xlarge"
  },
  "custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricksjobcluster-unity-catalog-automated-job-cluster"
  },
  "custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "forbidden"
  },
  "spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.repl.allowedLanguages": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  "spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "forbidden"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "forbidden"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "arn:aws:iam::2222:instance-profile/unity-catalog-cluster"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://samsara-eu-databricks-cluster-logs/unity-catalog-automated-job-cluster"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "eu-west-1"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  "spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "eu-west-1"
  },
  "spark_conf.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.hadoop.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_conf.spark.databricks.scan.modTimeCheck.enabled": {
    "type": "fixed",
    "value": "false"
  },
  "spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "SINGLE_USER"
  },
  "spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  "init_scripts.0.volumes.destination": {
    "type": "fixed",
    "value": "/Volumes/s3/dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "default"
  }
}
`

func TestGetNodetypePattern(t *testing.T) {
	type args struct {
		t components.TeamInfo
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "DefaultSupportedNodeTypes",
			args: args{
				t: team.DataPlatform,
			},
			want: "(m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getNodeTypePattern(); got != tt.want {
				t.Errorf("getNodetypePattern() = %v, want %v", got, tt.want)
			}
		})
	}
}
