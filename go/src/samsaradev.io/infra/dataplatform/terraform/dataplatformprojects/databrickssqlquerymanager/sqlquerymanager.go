package databrickssqlquerymanager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

const DatabricksSQLDirectory = "/ManagedQueries"
const BackendRepoQueryRootDir = "dataplatform/queries"

// QueryMetadata is the configuration for a Databricks SQL Query.
type QueryMetadata struct {
	Owner          string          `json:"owner"`
	Name           string          `json:"name"`
	SqlWarehouse   string          `json:"sql_warehouse"`
	Description    string          `json:"description"`
	Region         []string        `json:"region"`
	RunAsRole      string          `json:"run_as_role"`
	Permissions    []Permission    `json:"permissions"`
	Visualizations []VisConfig     `json:"visualizations"`
	Parameters     []ParameterJson `json:"parameters"`
}

type Permission struct {
	Level     databricks.PermissionLevel `json:"level"`
	GroupName string                     `json:"group_name"`
	UserName  string                     `json:"user_name"`
}

// VisConfig is the configuration for a Databricks SQL Query visualization.
type VisConfig struct {
	Type                  string `json:"type"`
	Name                  string `json:"name"`
	Description           string `json:"description"`
	VisualizationFilename string `json:"visualization_filename"`
}

// ParameterJson is the configuration for a Databricks SQL Query parameter.
// Below ParameterJson are the different types of parameters that can be used in a Databricks SQL Query.
type ParameterJson struct {
	Title            string          `json:"title"`
	Name             string          `json:"name"`
	Text             *TextParam      `json:"text,omitempty"`
	Number           *NumberParam    `json:"number,omitempty"`
	Enum             *EnumParam      `json:"enum,omitempty"`
	Query            *QueryParam     `json:"query,omitempty"`
	Date             *DateParam      `json:"date,omitempty"`
	DateTime         *DateParam      `json:"datetime,omitempty"`
	DateTimeSec      *DateParam      `json:"datetimesec,omitempty"`
	DateRange        *DateRangeParam `json:"date_range,omitempty"`
	DateTimeRange    *DateRangeParam `json:"datetime_range,omitempty"`
	DateTimeSecRange *DateRangeParam `json:"datetimesec_range,omitempty"`
}

type TextParam struct {
	Value string `json:"value"`
}

type NumberParam struct {
	Value float64 `json:"value"`
}

type Multiple struct {
	Prefix    string `json:"prefix"`
	Suffix    string `json:"suffix"`
	Separator string `json:"separator"`
}

type EnumParam struct {
	Value    string    `json:"value"`
	Values   []string  `json:"values"`
	Options  []string  `json:"options"`
	Multiple *Multiple `json:"multiple,omitempty"`
}

type QueryParam struct {
	Type       string            `json:"type"` // uuid or name
	Value      string            `json:"value"`
	Values     []string          `json:"values"`
	QueryName  string            `json:"query_name"`
	QueryIdMap map[string]string `json:"query_id_map"`
	Multiple   *Multiple         `json:"multiple,omitempty"`
	Region     string            `json:"region"`
}

type DateParam struct {
	Value string `json:"value"`
}

type DateRangeParam struct {
	Range *Range `json:"range,omitempty"`
}

type Range struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// DatabricksSQLQueryManager returns a list of Databricks SQL Query projects.
func DatabricksSQLQueryManager(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Find all folders within the queries directory that contain SQL files
	queriesAbsPath := filepath.Join(filepathhelpers.BackendRoot, BackendRepoQueryRootDir)
	queryFolders, err := findQueryFolders(queriesAbsPath)
	if err != nil {
		return nil, err
	}

	var projects []*project.Project
	for _, folderPath := range queryFolders {
		// Sanitize the folder name to be a valid Terraform project name
		sanitizedFolderName := strings.ReplaceAll(folderPath, "/", "_")

		// Create a new project for each folder containing SQL files
		p := &project.Project{
			RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
			Provider: providerGroup,
			Class:    "sqlqueries",
			Name:     sanitizedFolderName,
			ResourceGroups: map[string][]tf.Resource{
				"databricks_provider": databricksofficialprovider.DatabricksWorkspaceProvider(config.Hostname),
			},
		}
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"tf_backend": resource.ProjectTerraformBackend(p),
		})

		// List all SQL files within the folder
		sqlFiles, err := listSQLFiles(filepath.Join(queriesAbsPath, folderPath))
		if err != nil {
			return nil, err
		}

		numQueries := 0
		warehouseSet := make(map[string]struct{})

		for _, sqlFile := range sqlFiles {
			// Extract metadata from the SQL file configuration
			metadataFilePath := filepath.Join(queriesAbsPath, folderPath, sqlFile+".metadata.json")
			metadata, err := extractMetadata(metadataFilePath)
			if err != nil {
				return nil, oops.Wrapf(err, "extracting metadata from %s", metadataFilePath)
			} else if metadata.Owner == "" {
				return nil, oops.Errorf("Owner not defined for query: %s", metadata.Name)
			} else if !strings.HasSuffix(metadata.Owner, "-group") {
				return nil, oops.Errorf("Owner %s must have the -group suffix", metadata.Owner)
			}

			for _, region := range metadata.Region {
				// Only generate a Terraform resource for the SQL query if it is in the correct region
				if region == config.Region {
					warehouseSet[metadata.SqlWarehouse] = struct{}{}

					// Generate a Terraform resource for each SQL queries and its visualizations
					singleQuery, err := SqlQuery(config, folderPath, sqlFile, *metadata, os.ReadFile)
					if err != nil {
						return nil, oops.Wrapf(err, "generating SQL query for %s", sqlFile)
					}

					p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, singleQuery)
					numQueries += 1
				}
			}

		}

		// Generate a Terraform resource for the directory and its warehouses
		directory := &databricksresource_official.Directory{
			Path: filepath.Join(DatabricksSQLDirectory, folderPath),
		}
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"directory": {directory},
		})

		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, Warehouses(warehouseSet))

		// Only add the project if it contains SQL queries which may be 0 if the current region is not supported
		if numQueries > 0 {
			projects = append(projects, p)
		}
	}

	return projects, nil
}

// findQueryFolders finds all folders within a given directory that contain SQL files.
func findQueryFolders(rootDir string) ([]string, error) {
	folderSet := make(map[string]struct{})
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".sql" {
			relativePath, _ := filepath.Rel(rootDir, filepath.Dir(path))
			folderSet[relativePath] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "walking directory: %s", rootDir)
	}

	// Convert the set back to a slice
	queryFolders := make([]string, 0, len(folderSet))
	for folder := range folderSet {
		queryFolders = append(queryFolders, folder)
	}
	return queryFolders, nil
}

// listSQLFiles lists all SQL files within a given directory.
func listSQLFiles(dir string) ([]string, error) {
	var files []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, oops.Wrapf(err, "reading directory: %s", dir)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, strings.TrimSuffix(entry.Name(), ".sql"))
		}
	}
	return files, nil
}

// extractMetadata extracts the metadata from a given metadata file.
func extractMetadata(metadataFilePath string) (*QueryMetadata, error) {
	metadataBytes, err := os.ReadFile(metadataFilePath)
	if err != nil {
		return nil, oops.Errorf("metadata file %s not found", metadataFilePath)
	}
	var metadata QueryMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, oops.Wrapf(err, "error unmarshalling metadata file %s", metadataFilePath)
	}

	return &metadata, nil
}

// SqlWarehouseArgs is the configuration for a Databricks SQL Warehouse.
type SqlWarehouseArgs struct {
	Name string `hcl:"name"`
}

// Warehouses generates a Terraform data resource block for each unique SQL warehouse.
func Warehouses(warehouseSet map[string]struct{}) map[string][]tf.Resource {
	warehouses := []tf.Resource{}

	for sql_warehouse := range warehouseSet {
		warehouse := &genericresource.Data{
			Type: "databricks_sql_warehouse",
			Name: sql_warehouse,
			Args: SqlWarehouseArgs{
				Name: sql_warehouse,
			},
		}

		warehouses = append(warehouses, warehouse)
	}

	return map[string][]tf.Resource{
		"warehouse": warehouses,
	}
}

// ReadFileFunc is a function that reads a file from the filesystem.
type ReadFileFunc func(filename string) ([]byte, error)

// escapeQuotes escapes unescaped double quotes in the input string
func escapeQuotes(input string) string {
	var result strings.Builder
	n := len(input)
	for i := 0; i < n; i++ {
		if input[i] == '"' {
			// Check if this quote is not escaped
			if i == 0 || input[i-1] != '\\' {
				result.WriteString(`\"`)
			} else {
				result.WriteByte('"')
			}
		} else {
			result.WriteByte(input[i])
		}
	}
	return result.String()
}

// SqlQuery generates a Terraform resource for a Databricks SQL Query and its visualizations.
func SqlQuery(c dataplatformconfig.DatabricksConfig, folderPath string, queryName string, metadata QueryMetadata, readFile ReadFileFunc) (map[string][]tf.Resource, error) {
	queriesAbsPath := filepath.Join(filepathhelpers.BackendRoot, BackendRepoQueryRootDir)

	directory := &databricksresource_official.Directory{
		Path: filepath.Join(DatabricksSQLDirectory, folderPath),
	}

	sqlFileByteArray, err := readFile(filepath.Join(queriesAbsPath, folderPath, queryName+".sql"))
	if err != nil {
		return nil, err
	}
	sqlFileContent := escapeQuotes(string(sqlFileByteArray))

	var queryParams []*databricksresource_official.SqlQueryParameter

	for _, param := range metadata.Parameters {
		if param.Query != nil {
			param.Query.Region = c.Region
		}
		queryParam, err := convertParameterToHCL(param)
		if err != nil {
			return nil, oops.Wrapf(err, "converting parameter %s", param.Name)
		}
		queryParams = append(queryParams, queryParam)
	}

	query := &databricksresource_official.SqlQuery{

		DataSourceId: genericresource.DataResourceId("databricks_sql_warehouse", metadata.SqlWarehouse).ReferenceAttr("data_source_id"),
		Query:        sqlFileContent,
		Name:         queryName,
		Parent:       fmt.Sprintf("folders/%s", directory.ResourceId().ReferenceAttr("object_id")),
		Description:  metadata.Description,
		Parameters:   queryParams,
	}

	runAsRole := databricksresource_official.RunAsRole(metadata.RunAsRole)

	switch runAsRole {
	case databricksresource_official.RunAsRoleOwner:
		query.RunAsRole = databricksresource_official.RunAsRoleOwner
	case databricksresource_official.RunAsRoleViewer:
		query.RunAsRole = databricksresource_official.RunAsRoleViewer
	default:
		return nil, oops.Errorf("RunAsRole %s is not supported", runAsRole)
	}

	var tfResources []tf.Resource
	tfResources = append(tfResources, query)

	// Generate a Terraform resource for each visualization
	for _, visualization := range metadata.Visualizations {
		contents, err := readFile(filepath.Join(queriesAbsPath, "visualizations", visualization.VisualizationFilename+".json"))
		if err != nil {
			return nil, err
		}

		visConfig := strings.ReplaceAll(string(contents), `"`, `\"`)

		visName := fmt.Sprintf("%s_%s", queryName, visualization.Name)
		// If the visualization is owned by the firmware group, use the visualization name as the name of the visualization
		if metadata.Owner == "firmware-group" {
			visName = visualization.Name
		}

		vis := &databricksresource_official.SqlVisualization{
			QueryID:     query.ResourceId().ReferenceAttr("id"),
			Type:        visualization.Type,
			Name:        visName,
			Description: visualization.Description,
			Options:     visConfig,
		}
		tfResources = append(tfResources, vis)
	}

	// Add custom permissions to the SQL query as defined in the metadata.json file.
	var acls []*databricksresource_official.AccessControl
	groupsAndUsers := make(map[string]struct{})
	for _, permission := range metadata.Permissions {
		acl, err := generateACL(permission, runAsRole)
		if err != nil {
			return nil, oops.Wrapf(err, "Error generating ACL")
		}

		groupOrUserName, err := getGroupOrUser(permission)
		if err != nil {
			return nil, oops.Wrapf(err, "Error getting group or user")
		}

		if _, exists := groupsAndUsers[groupOrUserName]; exists {
			return nil, oops.Errorf("Duplicate permission detected for group/user: %s for query: %s", groupOrUserName, metadata.Name)
		} else {
			acls = append(acls, acl)
			groupsAndUsers[permission.GroupName] = struct{}{}
		}
	}

	if _, exists := groupsAndUsers[metadata.Owner]; exists {
		return nil, oops.Errorf("Duplicate permission detected for owner group: %s for query: %s", metadata.Owner, metadata.Name)
	} else {
		// By default, add a CAN_MANAGE permission to the owning team.
		acls = append(acls, &databricksresource_official.AccessControl{
			PermissionLevel: databricks.PermissionLevelCanManage,
			GroupName:       metadata.Owner,
		})
	}

	// By default, the folder that these are put in give `CAN_VIEW` to the `users` group. We have to add this
	// manually, as this is a system managed group.
	acls = append(acls, &databricksresource_official.AccessControl{
		PermissionLevel: databricks.PermissionLevelCanView,
		GroupName:       "users",
	})

	// Create permissions resource
	tfResources = append(tfResources, &databricksresource_official.Permissions{
		ResourceName:   queryName + "_permission",
		SqlQueryId:     query.ResourceId().ReferenceAttr("id"),
		AccessControls: acls,
	})

	return map[string][]tf.Resource{
		queryName: tfResources,
	}, nil
}

// getGroupOrUser returns either GroupName or UserName from a permission.
func getGroupOrUser(permission Permission) (string, error) {
	if permission.GroupName != "" {
		// All group names should end with "-group" suffix
		if !strings.HasSuffix(permission.GroupName, "-group") {
			return "", oops.Errorf("Permission for group %s must have the -group suffix", permission.GroupName)
		}
		return permission.GroupName, nil
	} else if permission.UserName != "" {
		return permission.UserName, nil
	}
	return "", oops.Errorf("Permission %s must have either a group_name or user_name attribute", permission.Level)
}

// generateACL converts a Permission struct into a Databricks AccessControl struct.
func generateACL(permission Permission, runAsRole databricksresource_official.RunAsRole) (*databricksresource_official.AccessControl, error) {
	if !databricks.ObjectTypePermissionAllowed(
		databricks.ObjectTypeSqlQuery,
		databricks.PermissionLevel(permission.Level),
	) {
		return nil, oops.Errorf("Permission level %s is not allowed for SQL queries", permission.Level)
	} else if runAsRole == databricksresource_official.RunAsRoleOwner &&
		(permission.Level == databricks.PermissionLevelCanManage || permission.Level == databricks.PermissionLevelCanEdit) {
		return nil, oops.Errorf("Permission level %s is not allowed for SQL queries with run_as_role: %s", permission.Level, runAsRole)
	}

	accessControl := &databricksresource_official.AccessControl{
		PermissionLevel: databricks.PermissionLevel(permission.Level),
	}

	switch {
	case permission.GroupName != "" && permission.UserName != "":
		return nil, oops.Errorf("Permission %s cannot have both a group_name and user_name attribute", permission.Level)
	case permission.GroupName != "":
		accessControl.GroupName = permission.GroupName
	case permission.UserName != "":
		accessControl.UserName = permission.UserName
	default:
		return nil, oops.Errorf("Permission %s must have either a group_name or user_name attribute", permission.Level)
	}

	return accessControl, nil
}

// convertParameterToHCL converts a ParameterJson to a SqlQueryParameter.
func convertParameterToHCL(param ParameterJson) (*databricksresource_official.SqlQueryParameter, error) {
	// Define the generic SqlQueryParameter with required default attributes
	hclParameter := &databricksresource_official.SqlQueryParameter{
		Name:  param.Name,
		Title: param.Title,
	}

	switch {
	case param.Text != nil:
		hclParameter.Text = databricksresource_official.QueryParameterText{
			Value: param.Text.Value,
		}
	case param.Number != nil:
		hclParameter.Number = databricksresource_official.QueryParameterNumber{
			Value: param.Number.Value,
		}
	case param.Date != nil:
		hclParameter.Date = databricksresource_official.QueryParameterDateLike{
			Value: param.Date.Value,
		}
	case param.DateTime != nil:
		hclParameter.DateTime = databricksresource_official.QueryParameterDateLike{
			Value: param.DateTime.Value,
		}
	case param.DateTimeSec != nil:
		hclParameter.DateTimeSec = databricksresource_official.QueryParameterDateLike{
			Value: param.DateTimeSec.Value,
		}
	case param.DateRange != nil:
		if param.DateRange.Range != nil {
			hclParameter.DateRange = databricksresource_official.QueryParameterDateRangeLike{
				Range: databricksresource_official.QueryParameterDateRangeValue{
					Start: param.DateRange.Range.Start,
					End:   param.DateRange.Range.End,
				},
			}
		} else {
			hclParameter.DateRange = databricksresource_official.QueryParameterDateRangeLike{}
		}
	case param.DateTimeRange != nil:
		if param.DateTimeRange.Range != nil {
			hclParameter.DateTimeRange = databricksresource_official.QueryParameterDateRangeLike{
				Range: databricksresource_official.QueryParameterDateRangeValue{
					Start: param.DateTimeRange.Range.Start,
					End:   param.DateTimeRange.Range.End,
				},
			}
		} else {
			hclParameter.DateTimeRange = databricksresource_official.QueryParameterDateRangeLike{}
		}
	case param.DateTimeSecRange != nil:
		if param.DateTimeSecRange.Range != nil {
			hclParameter.DateTimeSecRange = databricksresource_official.QueryParameterDateRangeLike{
				Range: databricksresource_official.QueryParameterDateRangeValue{
					Start: param.DateTimeSecRange.Range.Start,
					End:   param.DateTimeSecRange.Range.End,
				},
			}
		} else {
			hclParameter.DateTimeSecRange = databricksresource_official.QueryParameterDateRangeLike{}
		}
	case param.Enum != nil:
		hclParameter.Enum = databricksresource_official.QueryParameterEnum{
			Options: param.Enum.Options,
		}

		switch {
		case param.Enum.Value != "" && len(param.Enum.Values) != 0:
			return nil, oops.Errorf("Enum parameter %s cannot have both a value and values attribute", param.Name)
		case param.Enum.Value != "":
			// although "Value" is supported, if the query metadata provides a single value, we wrap it in
			// an array to be consistent with the "Values" attribute because the default values for
			// the parameter gets populated on the SQL Query only when the "Values" attribute is provided
			hclParameter.Enum.Values = []string{param.Enum.Value}
		case len(param.Enum.Values) != 0:
			hclParameter.Enum.Values = param.Enum.Values
		default:
			return nil, oops.Errorf("Enum parameter %s must have either a value or values attribute", param.Name)
		}

		if param.Enum.Multiple != nil {
			hclParameter.Enum.Multiple = databricksresource_official.QueryParameterAllowMultiple{
				// The `\"`` is doubly escaped for the string generated
				// in the HCL code to be the intended `\"`
				Prefix:    strings.ReplaceAll(string(param.Enum.Multiple.Prefix), `"`, `\"`),
				Suffix:    strings.ReplaceAll(string(param.Enum.Multiple.Suffix), `"`, `\"`),
				Separator: param.Enum.Multiple.Separator,
			}
		} else if len(param.Enum.Values) > 1 {
			return nil, oops.Errorf("Values cannot be defined for %s without the Multiple attribute", param.Name)
		}
	case param.Query != nil:
		// For parameters of type Query, the referenced query needs to have been executed at least once
		// to be able to retrieve the session ID that is used to define the options for the query parameter
		// This is a limitation of the Databricks Terraform Provider
		var values []string
		switch {
		case param.Query.Value != "" && len(param.Query.Values) != 0:
			return nil, oops.Errorf("Query parameter %s cannot have both a value and values attribute", param.Name)
		case param.Query.Value != "":
			// although "Value" is supported, if the query metadata provides a single value, we wrap it in
			// an array to be consistent with the "Values" attribute because the default values for
			// the parameter gets populated on the SQL Query only when the "Values" attribute is provided
			values = []string{param.Query.Value}
		case len(param.Query.Values) != 0:
			values = param.Query.Values
		default:
			return nil, oops.Errorf("Query parameter %s must have either a value or values attribute", param.Name)
		}

		if param.Query.Type == "uuid" {
			hclParameter.Query = databricksresource_official.QueryParameterQuery{
				Values:  values,
				QueryId: param.Query.QueryIdMap[param.Query.Region],
			}
		} else if param.Query.Type == "name" {
			query := &databricksresource_official.SqlQuery{
				Name: param.Query.QueryName,
			}
			hclParameter.Query = databricksresource_official.QueryParameterQuery{
				Values:  values,
				QueryId: query.ResourceId().ReferenceAttr("id"),
			}
		} else {
			return nil, oops.Errorf("Query parameter %s must have a valid type attribute", param.Name)
		}
		if param.Query.Multiple != nil {
			hclParameter.Query.Multiple = databricksresource_official.QueryParameterAllowMultiple{
				// The `\"`` is doubly escaped for the string generated
				// in the HCL code to be the intended `\"`
				Prefix:    strings.ReplaceAll(string(param.Query.Multiple.Prefix), `"`, `\"`),
				Suffix:    strings.ReplaceAll(string(param.Query.Multiple.Suffix), `"`, `\"`),
				Separator: param.Query.Multiple.Separator,
			}
		} else if len(param.Query.Values) > 1 {
			return nil, oops.Errorf("Values cannot be defined for %s without the Multiple attribute", param.Name)
		}
	}

	return hclParameter, nil
}
