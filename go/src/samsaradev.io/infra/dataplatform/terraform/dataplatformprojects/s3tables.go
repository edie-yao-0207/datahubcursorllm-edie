package dataplatformprojects

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/s3tables"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

var configBackendPath string = "dataplatform/tables/s3data"
var configFullPath string = filepath.Join(filepathhelpers.BackendRoot, configBackendPath)

type s3TableResources struct {
	ucTable *databricksresource_official.SqlTable
	s3File  *awsresource.S3BucketObject
}

// Eventually remove this and have all databases folders not use hash. We are slowly rolling this out
// to ensure it is safe
var dbFoldersToNotUseHash = map[string]struct{}{
	"perf_infra/": struct{}{},
}

var awsProviderVersionOverridesByTable map[string]string = map[string]string{
	"dandmv2_rollout_targeting_exclusions_2023_11_23": "4.30.0",
}

func S3UCTablesProject(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	teamToTableConfigResources, err := ReadTableConfigurations(config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var projects []*project.Project
	monolithPath := getProjectMonolithStateFilePath(config.Region, providerGroup, "s3tables")
	for _, tables := range teamToTableConfigResources {
		for _, tableResources := range tables {
			p := &project.Project{
				RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
				Provider: providerGroup,
				Class:    "s3uctables",
				Name:     tableResources.ucTable.SchemaName,
				Group:    tableResources.ucTable.Name,
				ResourceGroups: map[string][]tf.Resource{
					"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
					"resources":           {tableResources.ucTable},
				},
			}
			awsProviderResource := resource.ProjectAWSProvider(p)
			if awsProviderVersionOverride, ok := awsProviderVersionOverridesByTable[tableResources.ucTable.Name]; ok {
				awsProviderResource = resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersionOverride))
			}

			p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
				"aws_provider": awsProviderResource,
				"tf_backend":   resource.ProjectTerraformBackend(p),
			})
			warehouse := &genericresource.Data{
				Type: "databricks_sql_warehouse",
				Name: "aianddata",
				Args: SqlWarehouseArgs{
					Name: "aianddata",
				},
			}
			p.ResourceGroups["resources"] = append(p.ResourceGroups["resources"], warehouse)
			if tableResources.s3File != nil {
				p.ResourceGroups["resources"] = append(p.ResourceGroups["resources"], tableResources.s3File)
			}
			p.MonolithStatePathOverride = monolithPath
			projects = append(projects, p)
		}
	}

	return projects, nil
}

func ReadTableConfigurations(config dataplatformconfig.DatabricksConfig) (map[string][]s3TableResources, error) {
	// Create look up table of managed DBs
	managedDbs := make(map[string]struct{})

	playgroundAndProdDbs := append(databaseregistries.GetDatabasesByGroup(config, databaseregistries.PlaygroundDatabaseGroup_LEGACY, databaseregistries.LegacyOnlyDatabases),
		databaseregistries.GetDatabasesByGroup(config, databaseregistries.ProductionDatabaseGroup, databaseregistries.LegacyOnlyDatabases)...)
	for _, db := range playgroundAndProdDbs {
		managedDbs[db.Name] = struct{}{}
	}

	for _, db := range databaseregistries.GetCustomBucketDatabases(config, databaseregistries.LegacyOnlyDatabases) {
		if db.GetRegionsMap()[config.Region] {
			managedDbs[db.Name] = struct{}{}
		}
	}

	teamsWithTFPipelines := team.AllTeamsWithTerraformPipelines
	teamToS3TableResources := make(map[string][]s3TableResources)
	walkErr := filepath.Walk(configFullPath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		// *_data.json files are considered static data files, not s3 table schema files.
		if filepath.Ext(path) == ".json" && !strings.HasSuffix(path, "_data.json") {
			err := validateSchema(path, "dataplatform/_json_schemas/s3table_schema.json")
			if err != nil {
				return oops.Wrapf(err, "validation failed for s3 table json metadata file: %s", path)
			}

			fileBytes, err := os.ReadFile(path)
			if err != nil {
				return oops.Wrapf(err, "error reading config file %s", path)
			}

			// Unmarshall JSON, get table config values, and add entry in map if dne
			var tableConfig s3tables.S3TableConfig
			jsonErr := json.Unmarshal(fileBytes, &tableConfig)
			if jsonErr != nil {
				return oops.Wrapf(jsonErr, "error unmarshaling JSON")
			}

			terraformTeam, ok := team.TeamByName[tableConfig.Owner]
			if !ok {
				return oops.Errorf("Invalid team %s in table config at %s", tableConfig.Owner, path)
			}
			if _, ok := teamToS3TableResources[terraformTeam.TeamName]; !ok {
				if _, ok := teamsWithTFPipelines[terraformTeam.TeamName]; ok {
					teamToS3TableResources[terraformTeam.TeamName] = []s3TableResources{}
				} else {
					// If s3table owner does not have a terraform pipeline, keep in Data Platform
					terraformTeam = team.DataPlatform
				}
			}

			if _, ok := tableConfig.RegionToLocation[config.Region]; ok || len(tableConfig.RegionToLocation) == 0 {
				if _, ok := managedDbs[tableConfig.Database]; !ok {
					return oops.Errorf("Please register your database in dataplatformprojects/databases.go")
				}
			}

			// create S3BucketObject first if data type is static
			var location string
			s3TableResources := s3TableResources{}
			if len(tableConfig.RegionToLocation) == 0 {
				dataFilename := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
				switch tableConfig.DataType {
				case "csv":
					dataFilename = dataFilename + "." + tableConfig.DataType
				case "json":
					// The data file for static json table types is called *_data.json.
					dataFilename = dataFilename + "_data." + tableConfig.DataType
				default:
					return oops.Errorf("unsupported table type: %s", tableConfig.DataType)
				}

				bo, err := getS3BucketObjectStatic(config.Region, dataFilename, tableConfig.Database+"/")
				if err != nil {
					return oops.Wrapf(err, "error creating s3 bucket object")
				}
				s3TableResources.s3File = bo
				location = bo.URL()
			} else {
				var ok bool
				location, ok = tableConfig.RegionToLocation[config.Region]
				if !ok {
					return nil
				}
			}

			ucTable, err := createUnityCatalogSparkTable(tableConfig, location, config.Region)
			if err != nil {
				return oops.Wrapf(err, "error creating unity catalog table")
			}
			s3TableResources.ucTable = ucTable

			teamToS3TableResources[terraformTeam.TeamName] = append(teamToS3TableResources[terraformTeam.TeamName], s3TableResources)
		}

		return nil
	})
	if walkErr != nil {
		return nil, oops.Wrapf(walkErr, "Error walking s3tables directory")
	}
	return teamToS3TableResources, nil
}

func getS3BucketObjectStatic(region string, filename string, databasefolder string) (*awsresource.S3BucketObject, error) {
	basePath := filepath.Join(configBackendPath, databasefolder, filename)
	path, err := filepath.EvalSymlinks(basePath)
	if err != nil {
		return &awsresource.S3BucketObject{}, oops.Wrapf(err, "resolving sym link for %s", basePath)
	}

	var obj *awsresource.S3BucketObject
	if _, ok := dbFoldersToNotUseHash[databasefolder]; ok {
		obj, err = dataplatformresource.DeployedArtifactObjectNoHash(region, path)
	} else {
		obj, err = dataplatformresource.DeployedArtifactObject(region, path)
	}
	if err != nil {
		return &awsresource.S3BucketObject{}, oops.Wrapf(err, "")
	}
	return obj, nil
}

// SqlWarehouseArgs is the configuration for a Databricks SQL Warehouse.
type SqlWarehouseArgs struct {
	Name string `hcl:"name"`
}

func createUnityCatalogSparkTable(tableConfig s3tables.S3TableConfig, location string, region string) (*databricksresource_official.SqlTable, error) {
	// Set the format based on the data type
	dataSourceFormat := tableConfig.DataType

	// Build the columns for the SQL table based on tableConfig.Schema
	var columns []*databricksresource_official.SqlTableColumn
	for _, c := range tableConfig.Schema {
		// Convert bigints to longs to align with Databricks' supported types
		if c.Type == "bigint" {
			c.Type = "long"
		}

		// By default schema column description is defined in "description" but sometimes it is defined in "metadata" field.
		comment := c.Description
		if c.Metadata.Comment != "" {
			comment = c.Metadata.Comment
		}

		column := &databricksresource_official.SqlTableColumn{
			Name:     strings.ToLower(c.Name),
			Type:     c.Type,
			Comment:  comment,
			Nullable: true,
		}
		columns = append(columns, column)
	}

	// Configure partitions if there are partition keys
	var partitions []string
	for _, partition := range tableConfig.PartitionKeys {
		partitions = append(partitions, strings.ToLower(partition.Name))
		comment := partition.Description
		if partition.Metadata.Comment != "" {
			comment = partition.Metadata.Comment
		}
		partitionColumn := &databricksresource_official.SqlTableColumn{
			Name:     strings.ToLower(partition.Name),
			Type:     partition.Type,
			Comment:  comment,
			Nullable: true,
		}
		columns = append(columns, partitionColumn)
	}

	// Based on table type, add specific options needed by Spark readers.
	options := make(map[string]string)
	switch tableConfig.DataType {
	case "csv":
		// See list of valid parameters defined in
		// https://spark.apache.org/docs/3.0.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv.
		if tableConfig.CSV.Separator != "" {
			options["field.delim"] = tableConfig.CSV.Separator
		} else {
			options["field.delim"] = ","
		}

		if tableConfig.CSV.Escape != "" {
			options["escape"] = tableConfig.CSV.Escape
		}

		options["header"] = strconv.FormatBool(tableConfig.CSV.Header)
		options["multiline"] = strconv.FormatBool(tableConfig.CSV.Multiline)
	case "json":
	default:
		return nil, oops.Errorf("unsupported table type: %s", tableConfig.DataType)
	}

	// Escape double quotes in options
	for k, v := range options {
		options[k] = strings.ReplaceAll(v, `"`, `\"`)
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get ci service principal app id for region %s", region)
	}

	// Initialize the SqlTable resource with necessary fields
	sqlTable := &databricksresource_official.SqlTable{
		Name:             strings.ToLower(tableConfig.Table),
		CatalogName:      "default",
		SchemaName:       strings.ToLower(tableConfig.Database),
		TableType:        "EXTERNAL",
		StorageLocation:  location,
		DataSourceFormat: dataSourceFormat,
		Comment:          tableConfig.Description,
		Options:          options,
		Partitions:       partitions,
		Columns:          columns,
		WarehouseId:      genericresource.DataResourceId("databricks_sql_warehouse", "aianddata").ReferenceAttr("id"),
		Owner:            ciServicePrincipalAppId,
	}

	// 445_calendar table is failing to terraform plan because tf resource names cannot begin with a number.
	// Hence, we put in a special case to set the resource name for the tf resource to a string that works i.e. calendar_445
	if tableConfig.Table == "445_calendar" {
		sqlTable.ResourceName = "calendar_445"
	}

	return sqlTable, nil
}
