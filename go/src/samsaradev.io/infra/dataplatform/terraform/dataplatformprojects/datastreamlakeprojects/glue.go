package datastreamlakeprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/team"
)

func glueProjects(config dataplatformconfig.DatabricksConfig) ([]*project.Project, error) {
	var projects []*project.Project

	// Create the datastreams database. Currently, the tables in this point to the raw
	// parquet data from firehose but will eventually point to views of the optimized delta tables.
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: config.DatabricksProviderGroup,
		Class:    "datastreamlake",
		Name:     "database",
		Group:    "datastreams",
		ResourceGroups: map[string][]tf.Resource{
			"glue": {&awsresource.GlueCatalogDatabase{
				Name:        "datastreams",
				LocationUri: fmt.Sprintf("s3://%sdata-stream-lake/datastreams.db/", awsregionconsts.RegionPrefix[config.Region]),
			}},
		},
	})

	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: config.DatabricksProviderGroup,
		Class:    "datastreamslakeraw",
		Name:     "database",
		Group:    "datastreams_history",
		ResourceGroups: map[string][]tf.Resource{
			"glue": {&awsresource.GlueCatalogDatabase{
				Name:        "datastreams_history",
				LocationUri: fmt.Sprintf("s3://%sdata-streams-delta-lake/datastreams_history.db/", awsregionconsts.RegionPrefix[config.Region]),
			}},
		},
	})

	// Create the datastreams_schema database, which holds glue entries used by firehose for format conversion.
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: config.DatabricksProviderGroup,
		Class:    "datastreamsschema",
		Name:     "database",
		Group:    "datastreams_schema",
		ResourceGroups: map[string][]tf.Resource{
			"glue": {&awsresource.GlueCatalogDatabase{
				Name:        "datastreams_schema",
				LocationUri: fmt.Sprintf("s3://%sdata-stream-lake/datastreams_schema.db/", awsregionconsts.RegionPrefix[config.Region]),
			}},
		},
	})

	for _, entry := range datastreamlake.Registry {
		datastreamsSchemaTable, err := getDataStreamGlueTableResource("datastreams_schema", entry, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "Error creating Glue Table for data stream %s", entry.StreamName)
		}

		projects = append(projects, &project.Project{
			RootTeam: entry.Owner.TeamName,
			Provider: config.DatabricksProviderGroup,
			Class:    "datastreamsschema",
			Group:    fmt.Sprintf("datastreams_%s", entry.StreamName),
			Name:     "table",
			ResourceGroups: map[string][]tf.Resource{
				"glue": {datastreamsSchemaTable},
			},
		})
	}

	return projects, nil
}

func getDataStreamGlueTableResource(databaseName string, entry datastreamlake.DataStream, region string) (*awsresource.GlueCatalogTable, error) {
	glueSchema, err := datastreamlake.GoStructToGlueSchema(entry.Record)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to convert Go Struct to Glue Schema for Data Stream Registry entry %s", entry.StreamName)
	}

	// The column names must be lowercase for Athena.
	for i := range glueSchema {
		glueSchema[i].Name = strings.ToLower(glueSchema[i].Name)
	}

	return &awsresource.GlueCatalogTable{
		Name:         entry.StreamName,
		DatabaseName: databaseName,
		TableType:    "EXTERNAL_TABLE",
		Description:  entry.Description,
		StorageDescriptor: awsresource.GlueCatalogTableStorageDescriptor{
			Location:     fmt.Sprintf("s3://%sdata-stream-lake/%s/data/", awsregionconsts.RegionPrefix[region], entry.StreamName),
			InputFormat:  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			SerDeInfo: awsresource.GlueCatalogTableStorageDescriptorSerDeInfo{
				Name:                 "Parquet",
				SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			},
			Columns: glueSchema,
		},
		Parameters: map[string]string{
			"spark.sql.partitionProvider": "catalog",
			"classification":              "parquet",
		},
		PartitionKeys: []awsresource.GlueCatalogTableStorageDescriptorColumn{
			{
				Name:    "date",
				Type:    awsresource.GlueTableColumnTypeDate,
				Comment: fmt.Sprintf("Partition %s data stream table on date for efficient timeseries querying", entry.StreamName),
			},
		},
	}, nil
}
