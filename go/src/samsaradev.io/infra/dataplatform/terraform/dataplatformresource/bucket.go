package dataplatformresource

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go/aws"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/gluedefinitions"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

type Bucket struct {
	Name                             string
	Region                           string
	CurrentExpirationDays            int
	NonCurrentExpirationDaysOverride int
	LoggingBucket                    string
	ACLOverride                      string
	ExtraLifecycleRules              []awsresource.S3LifecycleRule
	EnableS3IntelligentTiering       bool
	S3GlacierIRDays                  int
	InventoryFrequency               string
	Metrics                          bool
	RnDCostAllocation                float64
	ReplicationConfiguration         awsresource.S3ReplicationConfiguration
	PreventDestroy                   bool // https://developer.hashicorp.com/terraform/language/meta-arguments/lifecycle#prevent_destroy
	SkipPublicAccessBlock            bool // SCP forbids setting the public access block, so we should not try
}

func (b Bucket) Bucket() *awsresource.S3Bucket {
	name := strings.Replace(b.Name, "_", "-", -1)
	prefix := awsregionconsts.RegionPrefix[b.Region]
	bucketName := prefix + name
	resourceName := "samsara_" + strings.Replace(name, "-", "_", -1)

	// Do not set the acl field unless the value is *not* "private".
	// In the past we used to set it as "private" instead of "", but this is deprecated
	// and we should leave it empty.
	acl := b.ACLOverride
	if acl == "private" {
		acl = ""
	}

	lifecycleRules := make([]awsresource.S3LifecycleRule, 0, 2+len(b.ExtraLifecycleRules))

	if b.NonCurrentExpirationDaysOverride != -1 {
		expireNonCurrentVersionsLifecycleRule := awsresource.ExpireNonCurrentVersionsLifecycleRule(b.NonCurrentExpirationDaysOverride)
		if b.NonCurrentExpirationDaysOverride == 0 {
			expireNonCurrentVersionsLifecycleRule = awsresource.ExpireNonCurrentVersionsLifecycleRule(90)
		}
		expireNonCurrentVersionsLifecycleRule.Expiration.ExpiredObjectDeleteMarker = aws.Bool(true)
		lifecycleRules = append(lifecycleRules, expireNonCurrentVersionsLifecycleRule)
	}

	if b.CurrentExpirationDays != 0 {
		lifecycleRules = append(lifecycleRules, awsresource.ExpireCurrentVersionLifecycleRule(b.CurrentExpirationDays))
	}
	lifecycleRules = append(lifecycleRules, b.ExtraLifecycleRules...)
	lifecycleRules = append(lifecycleRules, awsresource.ExpiredDeleteMarkerAndAbortMultipartRule(3))

	if b.EnableS3IntelligentTiering {
		lifecycleRules = append(lifecycleRules, awsresource.TransitionToS3IntelligentTiering(0))
	}

	if b.S3GlacierIRDays > 0 {
		lifecycleRules = append(lifecycleRules, awsresource.TransitionToS3GlacierIR(b.S3GlacierIRDays))
	}

	var logging awsresource.S3Logging
	if b.LoggingBucket != "" {
		logging.TargetBucket = b.LoggingBucket
		logging.TargetPrefix = fmt.Sprintf("logs/%s/", bucketName)
	}

	bucket := &awsresource.S3Bucket{
		Name:                          resourceName,
		Bucket:                        bucketName,
		ACL:                           acl,
		LifecycleRules:                lifecycleRules,
		DefineLoggingInBucketResource: true,
		Logging:                       logging,
		Tags: map[string]string{
			"samsara:product-group":  strings.ToLower(team.TeamProductGroup[team.DataPlatform.Name()]),
			"samsara:service":        "s3-" + strings.Replace(name, "-", "_", -1),
			"samsara:team":           strings.ToLower(team.DataPlatform.Name()),
			"samsara:rnd-allocation": strconv.FormatFloat(b.RnDCostAllocation, 'f', -1, 64),
		},
		ReplicationConfiguration:         b.ReplicationConfiguration,
		DefineVersioningInBucketResource: true,
	}

	if b.PreventDestroy {
		bucket.BaseResource.Lifecycle.PreventDestroy = true
	}

	return bucket

}

func (b Bucket) BucketOwnershipAndAccess() []tf.Resource {
	bucket := b.Bucket()

	var resources []tf.Resource

	// If the bucket's ACL is empty, then we should also make sure to emit
	// bucket owner enforced and public access block resources.
	// These are the defaults for newer buckets, but for some older buckets
	// they may not have been set correctly.
	if bucket.ACL == "" {
		resources = append(resources, &awsresource.S3BucketOwnershipControls{
			Bucket: bucket.ResourceId(),
			Rule:   awsresource.BucketOwnerEnforced(),
		})

		// Only create public access block if not explicitly skipped and if not in
		// CA region (e.g., due to SCP restrictions)
		if !b.SkipPublicAccessBlock && b.Region != infraconsts.SamsaraAWSCARegion {
			resources = append(resources, &awsresource.S3BucketPublicAccessBlock{
				Bucket:                bucket.ResourceId(),
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     true,
				RestrictPublicBuckets: true,
			})
		}
	}

	return resources
}

func (b Bucket) BucketInventory() *awsresource.S3BucketInventory {
	frequency := awsresource.S3InventoryFrequencyDaily
	if b.InventoryFrequency != "" {
		frequency = b.InventoryFrequency
	}
	return &awsresource.S3BucketInventory{
		Bucket:                 b.Bucket().ResourceId(),
		Name:                   "entire-bucket-daily-parquet",
		IncludedObjectVersions: "All",
		Schedule: awsresource.S3BucketInventorySchedule{
			Frequency: frequency,
		},
		Destination: awsresource.S3BucketInventoryDestination{
			Bucket: awsresource.S3BucketInventoryDestinationBucket{
				BucketArn: fmt.Sprintf("arn:aws:s3:::%s", awsregionconsts.RegionPrefix[b.Region]+"databricks-s3-inventory"),
				Format:    "Parquet",
			},
		},
		// Ask for all optional fields.
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETInventoryConfig.html
		OptionalFields: []string{
			"Size", "LastModifiedDate", "StorageClass", "ETag",
			"IsMultipartUploaded", "ReplicationStatus", "EncryptionStatus",
			"ObjectLockRetainUntilDate", "ObjectLockMode", "ObjectLockLegalHoldStatus",
			"IntelligentTieringAccessTier",
		},
	}
}

func (b Bucket) BucketMetrics() *awsresource.S3BucketMetric {
	return &awsresource.S3BucketMetric{
		Bucket: b.Bucket().ResourceId(),
		Name:   "EntireBucket",
	}
}

func (b Bucket) BucketInventoryTable() *awsresource.GlueCatalogTable {
	schemaStruct := gluedefinitions.TableStruct{Type: "struct", Fields: []gluedefinitions.ColumnStruct{}}
	columns := []awsresource.GlueCatalogTableStorageDescriptorColumn{
		{Name: "bucket", Type: awsresource.GlueTableColumnTypeString},
		{Name: "key", Type: awsresource.GlueTableColumnTypeString},
		{Name: "version_id", Type: awsresource.GlueTableColumnTypeString},
		{Name: "is_latest", Type: awsresource.GlueTableColumnTypeBoolean},
		{Name: "is_delete_marker", Type: awsresource.GlueTableColumnTypeBoolean},
		{Name: "size", Type: awsresource.GlueTableColumnTypeBigInt},
		{Name: "last_modified_date", Type: awsresource.GlueTableColumnTypeTimestamp},
		{Name: "e_tag", Type: awsresource.GlueTableColumnTypeString},
		{Name: "storage_class", Type: awsresource.GlueTableColumnTypeString},
		{Name: "is_multipart_uploaded", Type: awsresource.GlueTableColumnTypeBoolean},
		{Name: "replication_status", Type: awsresource.GlueTableColumnTypeString},
		{Name: "encryption_status", Type: awsresource.GlueTableColumnTypeString},
		{Name: "object_lock_retain_until_date", Type: awsresource.GlueTableColumnTypeTimestamp},
		{Name: "object_lock_mode", Type: awsresource.GlueTableColumnTypeString},
		{Name: "object_lock_legal_hold_status", Type: awsresource.GlueTableColumnTypeString},
		{Name: "dt", Type: awsresource.GlueTableColumnTypeString},
	}

	for _, c := range columns {
		stringType := string(c.Type)
		if stringType == "bigint" {
			stringType = "long"
		}
		column := gluedefinitions.ColumnStruct{
			Name:     c.Name,
			Nullable: true,
			Metadata: struct{}{},
			Type:     stringType,
		}
		schemaStruct.Fields = append(schemaStruct.Fields, column)
	}

	// Create a json schema from the parsed columns.
	jsonSchema, err := json.Marshal(schemaStruct)
	if err != nil {
		panic(err)
	}

	parameters := map[string]string{
		"EXTERNAL":                          "TRUE",
		"spark.sql.sources.schema.numParts": "1",
		"spark.sql.sources.schema.part.0":   strings.ReplaceAll(string(jsonSchema), `"`, `\"`),
	}

	return &awsresource.GlueCatalogTable{
		Name:         strings.ReplaceAll(b.Bucket().Bucket, "-", "_"),
		DatabaseName: "s3inventory",
		TableType:    "EXTERNAL_TABLE",
		StorageDescriptor: awsresource.GlueCatalogTableStorageDescriptor{
			InputFormat:  "org.apache.hadoop.mapred.TextInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
			Location: fmt.Sprintf(
				"s3://%s/%s/entire-bucket-daily-parquet/data",
				awsregionconsts.RegionPrefix[b.Region]+"databricks-s3-inventory",
				b.Bucket().Bucket,
			),
			SerDeInfo: awsresource.GlueCatalogTableStorageDescriptorSerDeInfo{
				Name:                 "Parquet",
				SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			},
			Columns: columns,
		},
		Parameters: parameters,
	}
}

func (b Bucket) BucketLogsTable() *awsresource.GlueCatalogTable {
	// https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/
	regex := `([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\\"[^\\"]*\\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\\"[^\\"]*\\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$`
	definition := template.Must(template.New("").Parse(`
select
  regexp_extract(value, '{{.RegExp}}', 1) as bucket_owner,
  regexp_extract(value, '{{.RegExp}}', 2) as bucket,
  to_timestamp(regexp_extract(value, '{{.RegExp}}', 3), 'dd/MMM/yyyy:HH:mm:ss +0000') as request_datetime,
  regexp_extract(value, '{{.RegExp}}', 4) as remote_ip,
  regexp_extract(value, '{{.RegExp}}', 5) as requester,
  regexp_extract(value, '{{.RegExp}}', 6) as request_id,
  regexp_extract(value, '{{.RegExp}}', 7) as operation,
  regexp_extract(value, '{{.RegExp}}', 8) as key,
  regexp_extract(value, '{{.RegExp}}', 9) as request_uri,
  regexp_extract(value, '{{.RegExp}}', 10) as http_status,
  regexp_extract(value, '{{.RegExp}}', 11) as error_code,
  cast(regexp_extract(value, '{{.RegExp}}', 12) as bigint) as bytes_sent,
  cast(regexp_extract(value, '{{.RegExp}}', 13) as bigint) as object_size,
  regexp_extract(value, '{{.RegExp}}', 14) as total_time,
  regexp_extract(value, '{{.RegExp}}', 15) as turn_around_time,
  regexp_extract(value, '{{.RegExp}}', 16) as referrer,
  regexp_extract(value, '{{.RegExp}}', 17) as user_agent,
  regexp_extract(value, '{{.RegExp}}', 18) as version_id,
  regexp_extract(value, '{{.RegExp}}', 19) as host_id,
  regexp_extract(value, '{{.RegExp}}', 20) as sig_v,
  regexp_extract(value, '{{.RegExp}}', 21) as cipher_suite,
  regexp_extract(value, '{{.RegExp}}', 22) as auth_type,
  regexp_extract(value, '{{.RegExp}}', 23) as end_point,
  regexp_extract(value, '{{.RegExp}}', 24) as tls_version
from {{.Location}}`,
	))
	var buf bytes.Buffer
	_ = definition.Execute(&buf, map[string]string{
		"RegExp":   strings.ReplaceAll(regex, `"`, `\"`),
		"Location": fmt.Sprintf("text.`s3://%s/%s`", awsregionconsts.RegionPrefix[b.Region]+"databricks-s3-logging", b.Bucket().Logging.TargetPrefix),
	})
	text := strings.TrimSpace(buf.String())

	return &awsresource.GlueCatalogTable{
		Name:             strings.ReplaceAll(b.Bucket().Bucket, "-", "_"),
		DatabaseName:     "s3logs",
		TableType:        "VIRTUAL_VIEW",
		Description:      fmt.Sprintf("S3 Server Log for s3://%s in %s", b.Bucket().Bucket, b.Region),
		ViewOriginalText: text,
		ViewExpandedText: text,
		StorageDescriptor: awsresource.GlueCatalogTableStorageDescriptor{
			InputFormat:  "org.apache.hadoop.mapred.SequenceFileInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
			// Databricks requires a location value to be set, but ignores the value for views.
			Location: "placeholder",
			SerDeInfo: awsresource.GlueCatalogTableStorageDescriptorSerDeInfo{
				Name:                 "VIEW",
				SerializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
				Parameters: map[string]string{
					"serialization.format": "1",
				},
			},
			Columns: []awsresource.GlueCatalogTableStorageDescriptorColumn{
				{Name: "bucket_owner", Type: awsresource.GlueTableColumnTypeString},
				{Name: "bucket", Type: awsresource.GlueTableColumnTypeString},
				{Name: "request_datetime", Type: awsresource.GlueTableColumnTypeTimestamp},
				{Name: "remote_ip", Type: awsresource.GlueTableColumnTypeString},
				{Name: "requester", Type: awsresource.GlueTableColumnTypeString},
				{Name: "request_id", Type: awsresource.GlueTableColumnTypeString},
				{Name: "operation", Type: awsresource.GlueTableColumnTypeString},
				{Name: "key", Type: awsresource.GlueTableColumnTypeString},
				{Name: "request_uri", Type: awsresource.GlueTableColumnTypeString},
				{Name: "http_status", Type: awsresource.GlueTableColumnTypeString},
				{Name: "error_code", Type: awsresource.GlueTableColumnTypeString},
				{Name: "bytes_sent", Type: awsresource.GlueTableColumnTypeBigInt},
				{Name: "object_size", Type: awsresource.GlueTableColumnTypeBigInt},
				{Name: "total_time", Type: awsresource.GlueTableColumnTypeString},
				{Name: "turn_around_time", Type: awsresource.GlueTableColumnTypeString},
				{Name: "referrer", Type: awsresource.GlueTableColumnTypeString},
				{Name: "user_agent", Type: awsresource.GlueTableColumnTypeString},
				{Name: "version_id", Type: awsresource.GlueTableColumnTypeString},
				{Name: "host_id", Type: awsresource.GlueTableColumnTypeString},
				{Name: "sig_v", Type: awsresource.GlueTableColumnTypeString},
				{Name: "cipher_suite", Type: awsresource.GlueTableColumnTypeString},
				{Name: "auth_type", Type: awsresource.GlueTableColumnTypeString},
				{Name: "end_point", Type: awsresource.GlueTableColumnTypeString},
				{Name: "tls_version", Type: awsresource.GlueTableColumnTypeString},
			},
		},
	}
}

func (b Bucket) Resources() []tf.Resource {
	resources := []tf.Resource{
		b.Bucket(),
		b.BucketInventory(),
		b.BucketInventoryTable(),
	}

	resources = append(resources, b.BucketOwnershipAndAccess()...)

	if b.LoggingBucket != "" {
		resources = append(resources, b.BucketLogsTable())
	}
	if b.Metrics {
		resources = append(resources, b.BucketMetrics())
	}
	return resources
}
