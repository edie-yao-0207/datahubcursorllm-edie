package dataplatformresource

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/team/components"
	"samsaradev.io/team/databricksadmins"
)

const QueryKey string = "QUERY"

type SQLQueryConfig struct {
	Name              string
	Owner             components.TeamInfo
	AdminTeams        []components.TeamInfo
	Region            string
	Description       string
	Query             string
	SQLWarehouseId    string // data_source_id of SQLWarehouse
	RunAsRole         string
	Tags              []string
	OwnerUser         string // Databricks SQLQuery must be owned by an individual user
	FilePath          string // Source file path in backend repo
	WorkspaceFolderId string // Databricks Workspace Folder Id
}

func SQLQueryResource(c SQLQueryConfig) ([]tf.Resource, error) {
	resourceName := tf.SanitizeResourceName(strings.ToLower(strings.TrimSuffix(c.FilePath, filepath.Ext(c.FilePath))))

	runAsRole := databricksresource.RunAsRoleViewer
	if c.RunAsRole == string(databricksresource.RunAsRoleOwner) {
		runAsRole = databricksresource.RunAsRoleOwner
	}

	query := &databricksresource.SqlQuery{
		ResourceName: resourceName,
		Name:         c.Name,
		Description:  c.Description,
		Query:        c.Query,
		DataSourceId: c.SQLWarehouseId,
		RunAsRole:    runAsRole,
		Tags:         c.Tags,
		Parent:       fmt.Sprintf("folders/%s", c.WorkspaceFolderId),
	}

	var admins []DatabricksPrincipal
	for _, team := range append(c.AdminTeams, c.Owner) {
		admins = append(admins, TeamPrincipal{Team: team})
	}
	queryPermissions, err := Permissions(PermissionsConfig{
		ResourceName: resourceName,
		ObjectType:   databricks.ObjectTypeSqlQuery,
		ObjectId:     databricksresource.SqlQueryResourceId(resourceName).Reference(),
		Owner:        UserPrincipal{Email: c.OwnerUser}, // SQLQuery owner must be a user
		Admins:       admins,
		Readonly:     []DatabricksPrincipal{GroupPrincipal{GroupName: dataplatformterraformconsts.AllSamsaraUsersGroup}}, // all samsara users can view sqlqueries
		Region:       c.Region,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	// The 'admins' Databricks group implicitly inherits 'CAN_MANAGE' permissions. Since the permissions
	// model for Databricks SQLQueries does not include a field denoting whether an access policy is
	// inherited we need to explicitly include it in our terraform plan in order to match the resource state
	// that is returned to the provider.
	queryPermissions.AccessControls = append(queryPermissions.AccessControls, &databricksresource.AccessControl{
		GroupName:       databricksadmins.DatabricksAdminsGroup,
		PermissionLevel: databricks.PermissionLevelCanManage,
	})

	return []tf.Resource{query, queryPermissions}, nil
}
