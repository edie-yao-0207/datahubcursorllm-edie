package dataplatformprojects

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/didip/tollbooth/libstring"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/s3views"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

var configBackendPathViews = "dataplatform/tables/sqlview"
var configFullPathViews = filepath.Join(filepathhelpers.BackendRoot, configBackendPathViews)

func S3UCViewsProject(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	viewConfigResources, err := readUCViewConfigurations(config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var projects []*project.Project
	monolithPath := getProjectMonolithStateFilePath(config.Region, providerGroup, "s3ucviews")
	for _, s3ViewResources := range viewConfigResources {
		for _, ucView := range s3ViewResources {
			p := &project.Project{
				RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
				Provider: providerGroup,
				Class:    "s3ucviews",
				Name:     ucView.SchemaName,
				Group:    ucView.Name,
				ResourceGroups: map[string][]tf.Resource{
					"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
					"resources":           {ucView},
				},
			}
			warehouse := &genericresource.Data{
				Type: "databricks_sql_warehouse",
				Name: "aianddata",
				Args: SqlWarehouseArgs{
					Name: "aianddata",
				},
			}
			p.ResourceGroups["resources"] = append(p.ResourceGroups["resources"], warehouse)

			p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
				"tf_backend": resource.ProjectTerraformBackend(p),
			})
			p.MonolithStatePathOverride = monolithPath
			projects = append(projects, p)
		}
	}
	return projects, nil
}

func readUCViewConfigurations(config dataplatformconfig.DatabricksConfig) (map[string][]*databricksresource_official.SqlTable, error) {
	teamsWithTFPipelines := team.AllTeamsWithTerraformPipelines
	teamToS3ViewResources := make(map[string][]*databricksresource_official.SqlTable)
	// Create set of databases
	managedDbs := make(map[string]struct{})
	for _, dbName := range databaseregistries.GetAllDBNamesSorted(config, databaseregistries.LegacyOnlyDatabases) {
		managedDbs[dbName] = struct{}{}
	}

	validRegions := make(map[string]struct{})
	for _, region := range dataplatformconfig.AllRegions {
		validRegions[region] = struct{}{}
	}

	walkErr := filepath.Walk(configFullPathViews, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		validationErr := validateSchema(path, "dataplatform/_json_schemas/s3view_schema.json")
		if validationErr != nil {
			return oops.Wrapf(validationErr, "validation failed for sql view json metadata file: %s", path)
		}

		fileBytes, err := ioutil.ReadFile(path)
		if err != nil {
			return oops.Wrapf(err, "error reading config file %s", path)
		}

		// Unmarshall JSON and get view config values
		var viewConfig s3views.S3ViewConfig
		jsonErr := json.Unmarshal(fileBytes, &viewConfig)
		if jsonErr != nil {
			return oops.Wrapf(jsonErr, "error unmarshaling JSON")
		}

		if len(viewConfig.Regions) != 0 {
			for _, region := range viewConfig.Regions {
				// Checks if the region specified is in the list of valid regions
				if _, ok := validRegions[region]; !ok {
					return oops.Errorf("%s specifies invalid region %s", path, region)
				}
			}

			// Checks if the region specified is in the list of regions for the view
			if !libstring.StringInSlice(viewConfig.Regions, config.Region) {
				// This view isn't supposed to be created in this region
				return nil
			}
		}

		terraformTeam, ok := team.TeamByName[viewConfig.Owner]
		if !ok {
			return oops.Errorf("Invalid team %s in table config at %s", viewConfig.Owner, path)
		}
		if _, ok := teamToS3ViewResources[terraformTeam.TeamName]; !ok {
			if _, ok := teamsWithTFPipelines[terraformTeam.TeamName]; ok {
				teamToS3ViewResources[terraformTeam.TeamName] = []*databricksresource_official.SqlTable{}
			} else {
				// If s3table owner does not have a terraform pipeline, keep in Data Platform
				terraformTeam = team.DataPlatform
			}
		}

		// Check Database is registered
		if _, ok := managedDbs[viewConfig.Database]; !ok {
			return oops.Errorf(`Cannot create views for database "%s"; not defined in dataplatformprojects/databases.go`, viewConfig.Database)
		}

		// Get corresponding view sql as a string
		sqlString, err := getSQLString(getSQLStringArgs{
			path:              path,
			database:          viewConfig.Database,
			useRegionalValues: viewConfig.UseRegionalValues,
			region:            config.Region,
			view:              viewConfig.View,
		})
		if err != nil {
			return oops.Wrapf(err, "getting sql string from path: %s, database: %s, use regional values: %v", path, viewConfig.Database, viewConfig.UseRegionalValues)
		}

		parenthesizedSqlString := fmt.Sprintf("(\n%s\n)", sqlString)

		ucView, err := createUnityCatalogView(viewConfig, parenthesizedSqlString, config.Region)
		if err != nil {
			return oops.Wrapf(err, "error creating unity catalog view")
		}
		teamToS3ViewResources[terraformTeam.TeamName] = append(teamToS3ViewResources[terraformTeam.TeamName], ucView)
		return nil
	})
	if walkErr != nil {
		return nil, oops.Wrapf(walkErr, "Error walking s3views directory")
	}
	return teamToS3ViewResources, nil
}

type getSQLStringArgs struct {
	path              string
	database          string
	useRegionalValues bool
	region            string
	view              string
}

func getSQLString(args getSQLStringArgs) (string, error) {
	dir := filepath.Join(configBackendPathViews, args.database)
	filename := strings.TrimSuffix(filepath.Base(args.path), filepath.Ext(args.path)) + ".sql"
	sqlPath := filepath.Join(dir, filename)
	sourcePath := filepath.Join(
		tf.LocalId("backend_root").Reference(),
		sqlPath,
	)

	if !args.useRegionalValues {
		return fmt.Sprintf(`${file("%s")}`, sourcePath), nil
	}

	// Insert region specific values if the view specifies
	fileBytes, err := ioutil.ReadFile(sqlPath)
	if err != nil {
		return "", oops.Wrapf(err, "reading view file %s", sourcePath)
	}

	tmpl, err := template.New("sqlView").Parse(string(fileBytes))
	if err != nil {
		return "", oops.Wrapf(err, "failed to parse view %s for template variables", args.view)
	}
	buf := new(bytes.Buffer)

	regionPrefix, ok := awsregionconsts.RegionPrefix[args.region]
	if !ok {
		return "", oops.Errorf("failed to determine region prefix for s3 view %s using region %s", args.view, args.region)
	}

	// Determine the product usage share catalog based on region
	productUsageShareCatalog := "default"
	if args.region == infraconsts.SamsaraAWSEURegion || args.region == infraconsts.SamsaraAWSCARegion {
		productUsageShareCatalog = "data_tools_delta_share"
	}

	err = tmpl.Execute(buf, s3views.RegionalViewTemplate{
		BucketPrefix:             regionPrefix,
		ProductUsageShareCatalog: productUsageShareCatalog,
	})
	if err != nil {
		return "", oops.Wrapf(err, "failed to insert template variables into sql view %s", args.view)
	}

	return buf.String(), nil

}

func createUnityCatalogView(viewConfig s3views.S3ViewConfig, sqlString string, region string) (*databricksresource_official.SqlTable, error) {

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get ci service principal app id for region %s", region)
	}

	// Initialize the SqlTable resource with necessary fields
	sqlTable := &databricksresource_official.SqlTable{
		Name:           strings.ToLower(viewConfig.View),
		CatalogName:    "default",
		SchemaName:     strings.ToLower(viewConfig.Database),
		TableType:      "VIEW",
		ViewDefinition: sqlString,
		Comment:        viewConfig.Description,
		WarehouseId:    genericresource.DataResourceId("databricks_sql_warehouse", "aianddata").ReferenceAttr("id"),
		Owner:          ciServicePrincipalAppId,
	}

	return sqlTable, nil
}
