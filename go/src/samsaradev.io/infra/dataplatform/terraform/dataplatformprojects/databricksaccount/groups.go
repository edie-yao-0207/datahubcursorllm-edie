package databricksaccount

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/databricksserviceprincipals"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

// https://registry.terraform.io/providers/databricks/databricks/latest/docs/data-sources/user
type UserDataSourceArgs struct {
	UserName string `hcl:"user_name"`
}

var groupToMachineUserTypes = map[string][]dataplatformconfig.DatabricksMachineUserType{
	team.BizTechEnterpriseDataApplication.DatabricksAccountGroupName(): {
		dataplatformconfig.DatabricksMachineUserDbtCi,
		dataplatformconfig.DatabricksMachineUserDbtProd,
	},
	// Data Platform group is givin admin permissions/all privileges on the default catalog.
	// We adding machine users to this team to grant them admin permissions in UC.
	// This list was taken from the MakeDatabricksAdminsMap.
	team.DataPlatform.DatabricksAccountGroupName(): {
		dataplatformconfig.DatabricksMachineUserDagster,
		dataplatformconfig.DatabricksMachineUserCi,
		dataplatformconfig.DatabricksMachineUserDatapipelinesLambda,
		dataplatformconfig.DatabricksMachineUserMetrics,
		dataplatformconfig.DatabricksMachineUserProdGql,
		dataplatformconfig.DatabricksMachineUserMatik,
		dataplatformconfig.DatabricksMachineUserBiztech,
		dataplatformconfig.DatabricksMachineUserSafetyAirflow,
	},
	team.Security.DatabricksAccountGroupName(): {dataplatformconfig.DatabricksMachineUserSiem},
}

// AccountGroups only creates account level groups for each group in the Samsara team registry hierarchy.
// Note: The "admins" Databricks group is a Systems workspace-local group that cannot be deleted, and will
// to be maintained at the workspace level in a separate TF pipeline.
func AccountGroups() (map[string][]tf.Resource, error) {
	hierarchy, err := dataplatformprojects.MakeGroupHierarchy()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var accountGroups []tf.Resource
	var accountGroupMembers []tf.Resource
	var userDataSources []tf.Resource
	var workspaceAssignments []tf.Resource
	// Track what user data sources exist by user email.
	userDataSourceByEmail := make(map[string]struct{})
	// Keep references to the resource id for each group that is created.
	groupNameToResourceId := make(map[string]tf.ResourceId)
	for _, group := range hierarchy.AllGroups {
		// Create group.
		accountGroup := &databricksresource_official.Group{
			DisplayName: group.DatabricksAccountGroupName(),
		}
		accountGroups = append(accountGroups, accountGroup)
		groupResourceId := accountGroup.ResourceId()
		groupNameToResourceId[group.Name] = groupResourceId

		// Add user members to group.
		if !group.IgnoreMembers {
			userDataSourcesForGroup, userMemberships := userMembershipResources(groupResourceId, group.Users, userDataSourceByEmail)
			userDataSources = append(userDataSources, userDataSourcesForGroup...)
			accountGroupMembers = append(accountGroupMembers, userMemberships...)

			// Add machine users to group if there are any machine users specified. This is adding all region specific machine users created in
			// each workspace to the account group. Long term, we want to create a single account level machine user of each type, and assign it
			// admin permissions to each workspace (instead of having separate admin machine users for each workspace).
			if machineUserTypes, ok := groupToMachineUserTypes[group.DatabricksAccountGroupName()]; ok {
				machineUserDataSources, machineUserMemberships := machineUserMemberResources(machineUserTypes, groupResourceId, userDataSourceByEmail)
				userDataSources = append(userDataSources, machineUserDataSources...)
				accountGroupMembers = append(accountGroupMembers, machineUserMemberships...)
			}
		}

		// Assign account groups to workspaces.
		workspaceAssignments = append(workspaceAssignments, groupWorkspaceAssignments(groupResourceId.Reference(), group.Name)...)
	}

	// Add sub-group membership for groups *after* all group resources have been defined so there is
	// a reference to the ResourceId for each child group.
	subGroupMemberships, err := subGroupMembershipResources(hierarchy.AllGroups, groupNameToResourceId)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating child group membership resources")
	}
	accountGroupMembers = append(accountGroupMembers, subGroupMemberships...)

	// Add service principal memberships for groups.
	servicePrincipalMemberships, err := servicePrincipalMembershipResources()
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating service principal membership resources")
	}

	return map[string][]tf.Resource{
		"team_groups":                   accountGroups,
		"group_members":                 accountGroupMembers,
		"user_data_sources":             userDataSources,
		"workspace_assignments":         workspaceAssignments,
		"service_principal_memberships": servicePrincipalMemberships,
	}, nil
}

type ServicePrincipalDataResourceArgs struct {
	DisplayName string `hcl:"display_name"`
}

func servicePrincipalDataResource(displayName string) *genericresource.Data {
	return &genericresource.Data{
		Name: fmt.Sprintf("%s", displayName),
		Type: "databricks_service_principal",
		Args: ServicePrincipalDataResourceArgs{
			DisplayName: displayName,
		},
	}
}

// Creates group membership resources for all service principals across all regions.
func servicePrincipalMembershipResources() ([]tf.Resource, error) {
	var resources []tf.Resource
	for _, sp := range databricksserviceprincipals.ServicePrincipals {

		// If the service principal is not used in the dev workspace, skip it.
		if sp.ProdWorkspace {
			continue
		}

		for _, region := range []string{
			infraconsts.SamsaraAWSDefaultRegion,
			infraconsts.SamsaraAWSEURegion,
			infraconsts.SamsaraAWSCARegion,
		} {
			regionalName, err := sp.RegionalServicePrincipalName(region)
			if err != nil {
				return nil, oops.Wrapf(err, "getting regional service principal name")
			}
			// If the service principal is not a member of any Databricks groups, skip it.
			if len(sp.DatabricksGroups) == 0 {
				continue
			}
			// Create a data source for the service principal.
			resources = append(resources, servicePrincipalDataResource(regionalName))

			for _, group := range sp.DatabricksGroups {
				accountGroup := &databricksresource_official.Group{
					DisplayName: group,
				}
				// Create a group membership resource for the service principal.
				resources = append(resources, &databricksresource_official.GroupMember{
					ResourceName: fmt.Sprintf("%s_%s", group, regionalName),
					GroupId:      accountGroup.ResourceId().Reference(),
					MemberId:     genericresource.DataResourceId("databricks_service_principal", regionalName).ReferenceAttr("id"),
				})
			}
		}

	}
	return resources, nil
}

// userMembershipResources creates group membership resources for all users within the group. It also creates
// data sources for any users that do not already have a data source created, and updates the map userDataSourceByEmail
// to reflect the new data sources. It returns the new user data sources that were created and the group membership resources.
func userMembershipResources(groupResourceId tf.ResourceId, groupUsers []*dataplatformprojects.DatabricksUser, userDataSourceByEmail map[string]struct{},
) (groupUserDataSources []tf.Resource, groupMemberships []tf.Resource) {
	var userDataSources []tf.Resource
	var accountGroupMembers []tf.Resource
	for _, user := range groupUsers {
		// It doesn't matter what we pass in as the LegacyResource argument to Resource here.
		// LegacyResource is only used to determine entitlements on the user, which are not going to be used here.
		userId := user.Resource(false).ResourceId()

		// Users are created at the workspace level in a different project within the team TF pipeline.
		// Create a data source, if one does not already exist, so we can reference the users to add them to account groups.
		if _, ok := userDataSourceByEmail[user.Email]; !ok {
			userDataSources = append(userDataSources, userDataSource(userId.Name, user.Email))
			userDataSourceByEmail[user.Email] = struct{}{}
		}

		accountGroupMembers = append(accountGroupMembers, &databricksresource_official.GroupMember{
			ResourceName: fmt.Sprintf("%s_%s", groupResourceId.Name, userId.Name),
			GroupId:      groupResourceId.Reference(),
			MemberId:     genericresource.DataResourceId("databricks_user", userId.Name).ReferenceAttr("id"),
		})
	}
	return userDataSources, accountGroupMembers
}

func machineUserMemberResources(machineUserTypes []dataplatformconfig.DatabricksMachineUserType, groupResourceId tf.ResourceId, userDataSourceByEmail map[string]struct{},
) (machineUserDataSources []tf.Resource, machineUserMemberships []tf.Resource) {
	for _, userType := range machineUserTypes {
		// Handle the safety airflow machine user separately. it is not defined in the databricks provider; instead it's created in the databricksusers file.
		if userType == dataplatformconfig.DatabricksMachineUserSafetyAirflow {
			user := &dataplatformprojects.DatabricksUser{
				Email: "dev-safety-airflow-databricks@samsara.com",
				Name:  "SafetyAirflow",
			}
			userDataSources, memberships := userMembershipResources(groupResourceId, []*dataplatformprojects.DatabricksUser{user}, userDataSourceByEmail)
			machineUserDataSources = append(machineUserDataSources, userDataSources...)
			machineUserMemberships = append(machineUserMemberships, memberships...)
		}
		for _, machineUser := range dataplatformconfig.DatabricksE2WorkspacesMachineUsersOfType(userType) {
			user := &dataplatformprojects.DatabricksUser{
				// If we decided to generate users at the account level instead of at the workspace level, remember to set SQLAnalyticsAccess to true.
				// We do not set this field right now because this is just a temp user struct to allow us to create a user data source.
				Email: machineUser.Email,
				Name:  machineUser.Name,
			}
			userDataSources, memberships := userMembershipResources(groupResourceId, []*dataplatformprojects.DatabricksUser{user}, userDataSourceByEmail)
			machineUserDataSources = append(machineUserDataSources, userDataSources...)
			machineUserMemberships = append(machineUserMemberships, memberships...)
		}
	}
	return machineUserDataSources, machineUserMemberships
}

func subGroupMembershipResources(allGroups []*dataplatformprojects.DatabricksGroup, groupNameToResourceId map[string]tf.ResourceId,
) (subGroupMembers []tf.Resource, err error) {
	for _, group := range allGroups {
		parentGroupResourceId, ok := groupNameToResourceId[group.Name]
		if !ok {
			return nil, fmt.Errorf("Parent group '%s' not found", group.Name)
		}
		// Create a GroupMember resource for each child group.
		for _, childGroup := range group.ChildGroups {
			childGroupResourceId, ok := groupNameToResourceId[childGroup.Name]
			if !ok {
				return nil, fmt.Errorf("Child group '%s' not found", childGroup.Name)
			}
			subGroupMembers = append(subGroupMembers, &databricksresource_official.GroupMember{
				ResourceName: fmt.Sprintf("%s_%s", parentGroupResourceId.Name, childGroupResourceId.Name),
				GroupId:      parentGroupResourceId.Reference(),
				MemberId:     childGroupResourceId.Reference(),
			})
		}
	}
	return subGroupMembers, nil
}

func userDataSource(name string, email string) tf.Resource {
	return &genericresource.Data{
		Type: "databricks_user",
		Name: name,
		Args: UserDataSourceArgs{
			UserName: email,
		},
	}
}

func groupWorkspaceAssignments(groupId string, groupName string) []tf.Resource {
	var workspaceAssignments []tf.Resource
	for _, workspaceId := range dataplatformconsts.GetDevDatabricksWorkspaceIds() {
		assignment := &databricksresource_official.DatabricksMwsPermissionAssignment{
			ResourceName: fmt.Sprintf("%s_%d_workspace_assignment", groupName, workspaceId),
			PrincipalId:  groupId,
			WorkspaceId:  fmt.Sprintf("%d", workspaceId),
			Permissions: []databricksresource_official.WorkspacePermissionType{
				databricksresource_official.WorkspacePermissionTypeUser,
			},
		}
		workspaceAssignments = append(workspaceAssignments, assignment)
	}
	return workspaceAssignments
}
