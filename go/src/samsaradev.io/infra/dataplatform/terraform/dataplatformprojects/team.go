package dataplatformprojects

import (
	"fmt"
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/databricksadmins"
)

// Add a suffix to terraform managed users. Since Okta can automatically
// provision users, often with mixed-case emails, we want to make users not
// managed by Terraform stand out from the rest.
const managedByTerraform = " (Managed by Terraform)"

type DatabricksUser struct {
	Email              string
	Name               string
	SQLAnalyticsAccess bool
}

func (u *DatabricksUser) Resource(legacy bool) *databricksresource.User {
	user := &databricksresource.User{
		UserName:    u.Email,
		DisplayName: u.Name + managedByTerraform,

		// display_name doesnt get updated via workspace level Patch API
		// update is usually no op and cause terraform diff.
		// We exclude any updates to display_name to prevent unnecessary update.
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"display_name"},
				},
			},
		},
	}

	entitlements := []string{}
	// SQL Analytics is not available in legacy workspaces
	if u.SQLAnalyticsAccess && !legacy {
		entitlements = append(entitlements, "databricks-sql-access")
	}

	if len(entitlements) != 0 {
		user.Entitlements = entitlements
	}

	return user
}

// To use unity catalog, we need to create groups at the account level. All account groups will have "-group"
// appended to the workspace-local group name, so that both workspace and account level groups can exist together before the
// workspace-local groups are deleted.
func (group DatabricksGroup) DatabricksAccountGroupName() string {
	return fmt.Sprintf("%s-group", group.Name)
}

type DatabricksGroup struct {
	Name string

	// TODO: this field does not seem used. Do we need it?
	Team        components.TeamTreeNode
	ChildGroups []*DatabricksGroup
	Users       []*DatabricksUser

	// IgnoreMembers determines whether Terraform should ignore changes to members
	// (users and child groups) made outside of Terraform, e.g. by BizTech's Okta
	// integration.
	IgnoreMembers bool
}

func (g *DatabricksGroup) Resource(legacy bool) *databricksresource.Group {
	resource := &databricksresource.Group{
		DisplayName: g.Name,
	}
	for _, user := range g.Users {
		resource.Members = append(resource.Members, user.Resource(legacy).ResourceId().ReferenceAttr("id"))
	}
	for _, group := range g.ChildGroups {
		resource.Members = append(resource.Members, group.Resource(legacy).ResourceId().ReferenceAttr("id"))
	}
	sort.Strings(resource.Members)

	if g.IgnoreMembers {
		resource.BaseResource = tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"members"},
				},
			},
		}
	}

	return resource
}

func makeAdminsGroupResource(memberIds []string, providerGroup string) *databricksresource.Group {

	baseResource := tf.BaseResource{}

	// Admin group is manually imported, as its a system group provided by Databricks.
	// When we import it, the terraform state doesnt have display_name and it causes
	// terraform to recreate the admin group, which is not allowed.
	// All the new workspaces will ignore the display_name for the admin group.
	if providerGroup != "databricks-dev-us" && providerGroup != "databricks-dev-eu" {
		baseResource = tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				Lifecycle: tf.Lifecycle{
					IgnoreChanges: []string{"display_name"},
				},
			},
		}
	}

	adminsGroup := &databricksresource.Group{
		BaseResource: baseResource,
		DisplayName:  "admins",
		Members:      memberIds,
		Entitlements: databricksadmins.DatabricksAdminsEntitlements,
	}
	adminsGroup.MetaParameters.Lifecycle.PreventDestroy = true
	return adminsGroup
}

func TeamProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Create workspace local resources for each team (users, machine users, and groups).
	// We will migrate workspace local groups to account groups, however we can continue generating users
	// at the workspace level because they will automatically get created at the account level as well.
	p := &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "team",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
	}

	teams, err := Teams(config)
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, teams)
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}

func Teams(c dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	hierarchy, err := MakeGroupHierarchy()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	allUsers := hierarchy.AllUsers

	// Remove any users explicitly disallowed for this region by the registry.
	allUsers = filterUsersByDisallowedRegion(allUsers, c.Region)

	var teamUsers []tf.Resource
	var adminUsersIds []string
	var adminsMap map[string]struct{}
	if c.Region == infraconsts.SamsaraAWSDefaultRegion {
		adminsMap = databricksadmins.MakeDatabricksAdminsMap(databricksadmins.DatabricksAdminsUS)
	} else if c.Region == infraconsts.SamsaraAWSEURegion {
		adminsMap = databricksadmins.MakeDatabricksAdminsMap(databricksadmins.DatabricksAdminsEU)
	} else if c.Region == infraconsts.SamsaraAWSCARegion {
		adminsMap = databricksadmins.MakeDatabricksAdminsMap(databricksadmins.DatabricksAdminsCA)
	} else {
		return nil, oops.Wrapf(err, "invalid region [%s]", c.Region)
	}
	for _, user := range allUsers {
		u := user.Resource(c.LegacyWorkspace)
		teamUsers = append(teamUsers, u)
		if _, ok := adminsMap[user.Email]; ok {
			adminUsersIds = append(adminUsersIds, u.ResourceId().ReferenceAttr("id"))
		}
	}

	machineUsers := []*DatabricksUser{
		{
			Email: c.MachineUsers.CI.Email,
			Name:  c.MachineUsers.CI.Name,
			// CI needs to be able to manage SQL endpoints.
			SQLAnalyticsAccess: true,
		},
		{
			Email: c.MachineUsers.DataPipelineLambda.Email,
			Name:  c.MachineUsers.DataPipelineLambda.Name,
		},
		{
			Email: c.MachineUsers.Metrics.Email,
			Name:  c.MachineUsers.Metrics.Name,
			// databricksqueryhistorylogger uses this user to access SQL query history.
			SQLAnalyticsAccess: true,
		},
	}

	// Only add FiveTran user when a config has one populated
	if c.MachineUsers.Fivetran.Email != "" {
		machineUsers = append(machineUsers, &DatabricksUser{
			Email: c.MachineUsers.Fivetran.Email,
			Name:  c.MachineUsers.Fivetran.Name,
		})
	}

	if c.MachineUsers.ProdGQL.Email != "" {
		machineUsers = append(machineUsers, &DatabricksUser{
			Email:              c.MachineUsers.ProdGQL.Email,
			Name:               c.MachineUsers.ProdGQL.Name,
			SQLAnalyticsAccess: true,
		})
	}

	if c.MachineUsers.DataStreams.Email != "" {
		machineUsers = append(machineUsers, &DatabricksUser{
			Email:              c.MachineUsers.DataStreams.Email,
			Name:               c.MachineUsers.DataStreams.Name,
			SQLAnalyticsAccess: true,
		})
	}

	if c.MachineUsers.Matik.Email != "" {
		machineUsers = append(machineUsers, &DatabricksUser{
			Email:              c.MachineUsers.Matik.Email,
			Name:               c.MachineUsers.Matik.Name,
			SQLAnalyticsAccess: true,
		})
	}

	if c.MachineUsers.BizTech.Email != "" {
		machineUsers = append(machineUsers, &DatabricksUser{
			Email:              c.MachineUsers.BizTech.Email,
			Name:               c.MachineUsers.BizTech.Name,
			SQLAnalyticsAccess: true,
		})
	}

	if c.MachineUsers.DbtCi.Email != "" {
		machineUser := &DatabricksUser{
			Email:              c.MachineUsers.DbtCi.Email,
			Name:               c.MachineUsers.DbtCi.Name,
			SQLAnalyticsAccess: true,
		}
		machineUsers = append(machineUsers, machineUser)
	}

	if c.MachineUsers.DbtProd.Email != "" {
		machineUser := &DatabricksUser{
			Email:              c.MachineUsers.DbtProd.Email,
			Name:               c.MachineUsers.DbtProd.Name,
			SQLAnalyticsAccess: true,
		}
		machineUsers = append(machineUsers, machineUser)
	}

	if c.MachineUsers.Dagster.Email != "" {
		machineUser := &DatabricksUser{
			Email:              c.MachineUsers.Dagster.Email,
			Name:               c.MachineUsers.Dagster.Name,
			SQLAnalyticsAccess: true,
		}
		machineUsers = append(machineUsers, machineUser)
	}

	if c.MachineUsers.Siem.Email != "" {
		machineUser := &DatabricksUser{
			Email:              c.MachineUsers.Siem.Email,
			Name:               c.MachineUsers.Siem.Name,
			SQLAnalyticsAccess: true,
		}
		machineUsers = append(machineUsers, machineUser)
	}

	var machineUserResources []tf.Resource
	for _, user := range machineUsers {
		u := user.Resource(c.LegacyWorkspace)
		machineUserResources = append(machineUserResources, u)
		if _, ok := adminsMap[user.Email]; ok {
			adminUsersIds = append(adminUsersIds, u.ResourceId().ReferenceAttr("id"))
		}
	}

	// Add service principals for admins.
	if c.Region == infraconsts.SamsaraAWSDefaultRegion {
		adminUsersIds = append(adminUsersIds, databricksadmins.DatabricksAdminsServicePrincipalsUS...)
	} else if c.Region == infraconsts.SamsaraAWSCARegion {
		adminUsersIds = append(adminUsersIds, databricksadmins.DatabricksAdminsServicePrincipalsCA...)
	} else if c.Region == infraconsts.SamsaraAWSEURegion {
		adminUsersIds = append(adminUsersIds, databricksadmins.DatabricksAdminsServicePrincipalsEU...)
	} else {
		return nil, oops.Wrapf(err, "invalid region [%s]", c.Region)
	}

	// Admin bit on user is maintained via "admins" group membership. We
	// manage admin privileges in terraform but to de-risk accidentally
	// cutting off our admin access we ignore specific super admin principals
	// not managed in terraform.
	adminGroup := makeAdminsGroupResource(adminUsersIds, c.DatabricksProviderGroup)

	return map[string][]tf.Resource{
		"team_users": teamUsers,
		// Only manage admin group at the workspace level, because it is a Systems group that cannot be deleted.
		// All other groups are managed at the account level.
		"team_groups":   {adminGroup},
		"machine_users": machineUserResources,
	}, nil
}
