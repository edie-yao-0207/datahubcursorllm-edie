package dataplatformresource

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type DatabricksPrincipal interface {
	// Returns the list of AccessControlPrincipals associated with the principal string.
	// This returns 2 principals for groups, because there is a workspace-local group name, and
	// an account group name associated with each team.
	// We will clean this up to delete workspace local group permissions when we complete the group migration.
	databricksPrincipalIds(region string) []databricksresource.AccessControlPrincipal
}

type ProductGroupPrincipal struct {
	ProductGroupName string
}

func (p ProductGroupPrincipal) databricksPrincipalIds(region string) []databricksresource.AccessControlPrincipal {
	return []databricksresource.AccessControlPrincipal{
		{GroupName: strings.ToLower(p.ProductGroupName)},
	}
}

var _ DatabricksPrincipal = ProductGroupPrincipal{}

type TeamPrincipal struct {
	Team components.TeamInfo
}

func (p TeamPrincipal) databricksPrincipalIds(region string) (m []databricksresource.AccessControlPrincipal) {
	return []databricksresource.AccessControlPrincipal{
		{GroupName: p.Team.DatabricksAccountGroupName()},
	}
}

var _ DatabricksPrincipal = TeamPrincipal{}

type UserPrincipal struct {
	Email string
}

func (p UserPrincipal) databricksPrincipalIds(region string) []databricksresource.AccessControlPrincipal {
	return []databricksresource.AccessControlPrincipal{
		{UserName: p.Email},
	}
}

var _ DatabricksPrincipal = UserPrincipal{}

type GroupPrincipal struct {
	GroupName string
}

func (p GroupPrincipal) databricksPrincipalIds(region string) (m []databricksresource.AccessControlPrincipal) {
	return []databricksresource.AccessControlPrincipal{
		{GroupName: p.GroupName},
	}
}

var _ DatabricksPrincipal = GroupPrincipal{}

type ServicePrincipal struct {
	ServicePrincipalAppId string
}

func (p ServicePrincipal) databricksPrincipalIds(region string) (m []databricksresource.AccessControlPrincipal) {
	return []databricksresource.AccessControlPrincipal{
		{ServicePrincipalName: p.ServicePrincipalAppId},
	}
}

var _ DatabricksPrincipal = ServicePrincipal{}

type PermissionsConfig struct {
	ResourceName string

	ObjectType databricks.ObjectType
	ObjectId   string

	// Jobs must be owned by a Databricks user.
	// Usually this is the CI machine user.
	JobOwnerUser string

	// ServicePrincipalName is the name of the service principal to run the job as.
	// This is used for CI jobs.
	JobOwnerServicePrincipalName string

	Owner    DatabricksPrincipal
	Admins   []DatabricksPrincipal
	Users    []DatabricksPrincipal
	Readonly []DatabricksPrincipal

	// IgnoreAccessControlChanges specifies whether we will ignore changes to the `access_control` configuration.
	// This is motivated by the fact that some teams suffer from persistent diffs in the output in Terraform to
	// `access_control` for permissions despite there being no discernible difference.
	IgnoreAccessControlChanges bool

	// Temporary field specifying which region the permission is for.
	// This is so we can rollout permission changes for the workspace to account group migration separately.
	Region string
}

// Permissions builds a databricks_permissions terraform resource that specifies
// access control list for a given object, e.g. cluster, notebook, job, etc.
func Permissions(c PermissionsConfig) (*databricksresource.Permissions, error) {
	// Map permission level based on object type.
	// We will only use these pre-defined permission levels.
	var ownerPermLevel databricks.PermissionLevel
	var adminPermLevel databricks.PermissionLevel
	var userPermLevel databricks.PermissionLevel
	var readonlyPermLevel databricks.PermissionLevel
	switch c.ObjectType {
	case databricks.ObjectTypeSqlEndpoint:
		ownerPermLevel = databricks.PermissionLevelIsOwner
		adminPermLevel = databricks.PermissionLevelCanManage
		userPermLevel = databricks.PermissionsLevelCanMonitor
	case databricks.ObjectTypeCluster:
		ownerPermLevel = databricks.PermissionLevelCanRestart
		adminPermLevel = databricks.PermissionLevelCanManage
		userPermLevel = databricks.PermissionLevelCanRestart
		readonlyPermLevel = databricks.PermissionLevelCanAttachTo
	case databricks.ObjectTypeInstancePool:
		ownerPermLevel = databricks.PermissionLevelCanManage
		adminPermLevel = databricks.PermissionLevelCanManage
		userPermLevel = databricks.PermissionLevelCanRestart
		readonlyPermLevel = databricks.PermissionLevelCanAttachTo
	case databricks.ObjectTypeJob:
		ownerPermLevel = databricks.PermissionLevelCanManageRun
		adminPermLevel = databricks.PermissionLevelCanManageRun
		readonlyPermLevel = databricks.PermissionLevelCanView
	case databricks.ObjectTypeNotebook, databricks.ObjectTypeDirectory:
		ownerPermLevel = databricks.PermissionLevelCanEdit
		adminPermLevel = databricks.PermissionLevelCanEdit
		userPermLevel = databricks.PermissionLevelCanRun
		readonlyPermLevel = databricks.PermissionLevelCanRead
	case databricks.ObjectTypeSqlDashboard:
		ownerPermLevel = databricks.PermissionLevelCanManage
		adminPermLevel = databricks.PermissionLevelCanRun
		userPermLevel = databricks.PermissionLevelCanRun
		readonlyPermLevel = databricks.PermissionLevelCanView
	case databricks.ObjectTypeSqlQuery:
		ownerPermLevel = databricks.PermissionLevelCanManage
		adminPermLevel = databricks.PermissionLevelCanRun
		userPermLevel = databricks.PermissionLevelCanRun
		readonlyPermLevel = databricks.PermissionLevelCanView
	case databricks.ObjectTypeClusterPolicy:
		ownerPermLevel = databricks.PermissionLevelCanUse
		adminPermLevel = databricks.PermissionLevelCanUse
		userPermLevel = databricks.PermissionLevelCanUse
		readonlyPermLevel = databricks.PermissionLevelCanUse
	default:
		return nil, oops.Errorf("unknown object type: %q", c.ObjectType)
	}

	// Permission level by team.
	m := make(map[databricksresource.AccessControlPrincipal]databricks.PermissionLevel)

	// Start with defaults.
	// All teams have readonly access to jobs.
	switch c.ObjectType {
	case databricks.ObjectTypeJob:
		m[databricksresource.AccessControlPrincipal{GroupName: dataplatformterraformconsts.AllSamsaraUsersGroup}] = readonlyPermLevel
	}

	// Assign readonly teams.
	for _, principal := range c.Readonly {
		if readonlyPermLevel == databricks.PermissionLevelUndefined {
			return nil, oops.Errorf("object type %q does not support Readonly", c.ObjectType)
		}
		m = addPrincipalPermissions(principal, readonlyPermLevel, m, c.Region)
	}

	// Assign user teams.
	for _, principal := range c.Users {
		if userPermLevel == databricks.PermissionLevelUndefined {
			return nil, oops.Errorf("object type %q does not support Users", c.ObjectType)
		}
		m = addPrincipalPermissions(principal, userPermLevel, m, c.Region)
	}

	// Assign admin teams.
	for _, principal := range c.Admins {
		if adminPermLevel == databricks.PermissionLevelUndefined {
			return nil, oops.Errorf("object type %q does not support Admins", c.ObjectType)
		}
		m = addPrincipalPermissions(principal, adminPermLevel, m, c.Region)
	}

	// Assign owner team.
	m = addPrincipalPermissions(c.Owner, ownerPermLevel, m, c.Region)

	// Jobs are special because they must be owned by a user.
	if c.ObjectType == databricks.ObjectTypeJob {
		if c.JobOwnerUser == "" && c.JobOwnerServicePrincipalName == "" {
			return nil, oops.Errorf("job requires an owner user or service principal name")
		}
		m[databricksresource.AccessControlPrincipal{UserName: c.JobOwnerUser, ServicePrincipalName: c.JobOwnerServicePrincipalName}] = databricks.PermissionLevelIsOwner
	}

	// Catch-all: always add DataPlatform as an admin.
	m = addPrincipalPermissions(TeamPrincipal{Team: team.DataPlatform}, adminPermLevel, m, c.Region)

	// Sort for stable output.
	acl := make([]*databricksresource.AccessControl, 0, len(m))
	for principal, perm := range m {
		acl = append(acl, &databricksresource.AccessControl{
			UserName:             principal.UserName,
			ServicePrincipalName: principal.ServicePrincipalName,
			GroupName:            principal.GroupName,
			PermissionLevel:      perm,
		})
	}
	sort.Slice(acl, func(i, j int) bool {
		if acl[i].GroupName != acl[j].GroupName {
			return acl[i].GroupName < acl[j].GroupName
		}
		return acl[i].UserName < acl[j].UserName
	})

	// Catch bad permission mapping early.
	for _, ac := range acl {
		if !databricks.ObjectTypePermissionAllowed(c.ObjectType, ac.PermissionLevel) {
			return nil, oops.Errorf("object type %s does not support permission level %s", c.ObjectType, ac.PermissionLevel)
		}
	}

	var ignoreChanges []string
	if c.IgnoreAccessControlChanges {
		ignoreChanges = append(ignoreChanges, "access_control")
	}
	perms := &databricksresource.Permissions{
		ResourceName:   fmt.Sprintf("%s_%s", c.ObjectType, c.ResourceName),
		ObjectType:     c.ObjectType,
		ObjectId:       c.ObjectId,
		AccessControls: acl,
	}
	perms.Lifecycle.IgnoreChanges = ignoreChanges
	return perms, nil
}

func addPrincipalPermissions(principal DatabricksPrincipal, permLevel databricks.PermissionLevel,
	permissionsMap map[databricksresource.AccessControlPrincipal]databricks.PermissionLevel,
	region string) map[databricksresource.AccessControlPrincipal]databricks.PermissionLevel {
	for _, accessControlPrincipal := range principal.databricksPrincipalIds(region) {
		permissionsMap[accessControlPrincipal] = permLevel
	}
	return permissionsMap
}
