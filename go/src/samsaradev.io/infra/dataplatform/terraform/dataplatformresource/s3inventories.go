package dataplatformresource

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/s3/s3buckets"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/gluedefinitions"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

func S3InventoryProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	s3inventories, err := s3inventoryResources(config)
	if err != nil {
		return nil, err
	}
	p := &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: config.DatabricksProviderGroup,
		Class:    "s3inventory",
		ResourceGroups: map[string][]tf.Resource{
			"s3inventories": s3inventories,
		},
		GenerateOutputs: true,
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return []*project.Project{p}, nil
}

func S3InventoryBuckets(cloud infraconsts.SamsaraCloud) ([]string, error) {
	buckets, err := s3buckets.GetAllS3BucketsForCloud(cloud)
	if err != nil {
		return nil, oops.Wrapf(err, "Error parsing s3buckets terraform resources")
	}

	s3BucketNames := make([]string, 0, len(buckets))
	for _, v := range buckets {
		bucket, ok := v.(*awsresource.S3Bucket)
		if ok {
			if bucket.Name != "" && strings.HasPrefix(bucket.Name, "samsara") {
				s3BucketNames = append(s3BucketNames, bucket.Name)
			}
		}
	}

	return s3BucketNames, nil
}

func s3inventoryResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	s3InventoryDatabase := &awsresource.GlueCatalogDatabase{
		Name: "s3inventory",
	}
	resources = append(resources, s3InventoryDatabase)

	// s3inventory glue catalog database is part of this resource,
	// it is required for any bucket creation,
	// so, for bootstrapping a region, we will only create s3inventory.
	// rest of the database will be created in later milestones.
	if config.Region == infraconsts.SamsaraAWSCARegion {
		return resources, nil
	}

	// TODO: the cloud should ideally be stored in project.ProviderConfigs, but
	// we'll need to investigate more on how the region is used across the codebase.
	cloud := infraconsts.GetProdCloudByRegion(config.Region)
	if cloud == infraconsts.SamsaraClouds.NoSuchCloud {
		return nil, oops.Errorf("failed to get prod cloud by region %s", config.Region)
	}

	s3Buckets, err := S3InventoryBuckets(cloud)
	if err != nil {
		return nil, err
	}

	for _, table := range s3Buckets {
		resources = append(resources, NewS3Inventory(config.Region, table))
	}
	return resources, nil
}

func NewS3Inventory(region string, table string) *awsresource.GlueCatalogTable {
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
		Name:         strings.ReplaceAll(table, "-", "_"),
		DatabaseName: "s3inventory",
		TableType:    "EXTERNAL_TABLE",
		StorageDescriptor: awsresource.GlueCatalogTableStorageDescriptor{
			InputFormat:  "org.apache.hadoop.mapred.TextInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
			Location: fmt.Sprintf(
				"s3://%s/%s/entire-bucket-daily-parquet/data",
				awsregionconsts.RegionPrefix[region]+"s3-inventory",
				table,
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
