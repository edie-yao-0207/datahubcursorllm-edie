package dataplatformprojects

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/databricksusers"
)

type GroupHierarchy struct {
	AllUsers  []*DatabricksUser
	AllGroups []*DatabricksGroup
	GroupTree *DatabricksGroup
}

// MakeGroupHierarchy maps the entire team hierarchy into Databricks groups by
// creating a group for every team as well as every intermediate grouping of
// teams.
func MakeGroupHierarchy() (*GroupHierarchy, error) {
	allUserMap := make(map[string]*DatabricksUser)
	allGroupMap := make(map[string]*DatabricksGroup)

	var walkNode func(nodeName string, node components.TeamTreeNode) (*DatabricksGroup, error)
	walkNode = func(nodeName string, node components.TeamTreeNode) (*DatabricksGroup, error) {
		var childGroups []*DatabricksGroup

		// TODO: this variable is never set so it's not actually doing anything right now.
		ignoreMembers := false

		// A node must be either one team or a slice of child nodes representing an
		// grouping of teams, but not both.

		// We will create a group for every child and attach them as members of this
		// current node.
		for childName, childNode := range node.Children() {
			childGroup, err := walkNode(childName, childNode)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			childGroups = append(childGroups, childGroup)
		}

		groupName := nodeName
		var users []*DatabricksUser
		if team := node.Team(); team != nil {
			groupName = team.LegacyDatabricksWorkspaceGroupName()

			// This node is a team node, which has a set of users. Users are only
			// defined in leaf nodes (team nodes).
			// Add all the users for this team.
			for _, user := range databricksusers.UsersOnTeam(team.Name()) {
				if user.ExcludeFromDatabricks {
					continue
				}
				user := &DatabricksUser{
					Email:              user.Email,
					Name:               user.Name,
					SQLAnalyticsAccess: true,
				}
				allUserMap[user.Email] = user
				users = append(users, user)
			}

			// A team can have a Slack email delivery endpoint. In this case, create a databricks machine
			// user whose email address is the Slack email delivery endpoint. This user can then be set as
			// the target of SQL Dashboard deliveries so that deliveries are automatically forwarded to a
			// Slack channel. There is no corresponding Okta user, so no one can log in as this user.
			if team.SlackAlertsChannelEmail != nil {
				dbxMachineUser := &DatabricksUser{
					Email:              team.SlackAlertsChannelEmail.Email,
					Name:               fmt.Sprintf("Slack Email #%s (%s)", team.SlackAlertsChannelEmail.Channel, team.Name()),
					SQLAnalyticsAccess: true,
				}
				allUserMap[dbxMachineUser.Email] = dbxMachineUser
				users = append(users, dbxMachineUser)
			}

			// A team can also have multiple sub-teams. This is not expressed as
			// children of the node, but rather as a field in the Team struct. We
			// handle them the same way as we handle node childrens above.
			//
			// Non-development teams have members managed by BizTech. That means we
			// cannot manage their child groups here in Terraform. As a workaround, we
			// simply do not generate these child groups and we will ask BizTech to
			// assign users to their parent team.
			if !ignoreMembers {
				for _, subTeam := range team.SubTeams {
					childGroup, err := walkNode(subTeam.Name(), subTeam)
					if err != nil {
						return nil, oops.Wrapf(err, "")
					}
					childGroups = append(childGroups, childGroup)
				}
			}
		}

		sort.Slice(childGroups, func(i, j int) bool {
			return childGroups[i].Name < childGroups[j].Name
		})

		if ignoreMembers {
			if len(users) != 0 || len(childGroups) != 0 {
				return nil, oops.Errorf("group %s has members managed outside of Terraform, but the code is attempting to assign it %d users and %d groups", groupName, len(users), len(childGroups))
			}
		}

		group := &DatabricksGroup{
			Name:          strings.ToLower(groupName),
			Team:          node,
			ChildGroups:   childGroups,
			Users:         users,
			IgnoreMembers: ignoreMembers,
		}
		if _, ok := allGroupMap[group.Name]; ok {
			return nil, oops.Errorf("duplicate group: %s", group.Name)
		}
		allGroupMap[group.Name] = group
		return group, nil
	}

	// Make a super-group of all Samsara users. If we add limited-access user for
	// Databricks support team, we can use this super-group to restrict data
	// access.
	root, err := walkNode("samsara-users", team.Hierarchy)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Ensure registry users with no team mappings still exist in workspaces, but
	// are not assigned to any Databricks groups.
	for _, regUser := range databricksusers.DatabricksUserRegistry {
		if len(regUser.Teams) != 0 {
			continue
		}
		if _, exists := allUserMap[regUser.Email]; exists {
			continue
		}
		user := &DatabricksUser{
			Email:              regUser.Email,
			Name:               regUser.Name,
			SQLAnalyticsAccess: true,
		}
		allUserMap[user.Email] = user
	}

	allUsers := make([]*DatabricksUser, 0, len(allUserMap))
	for _, user := range allUserMap {
		allUsers = append(allUsers, user)
	}
	sort.Slice(allUsers, func(i, j int) bool {
		return allUsers[i].Email < allUsers[j].Email
	})

	var rndUsers []*DatabricksUser
	var nonRndUsers []*DatabricksUser
	nonRndUsersSet := nonRndUserList()

	for _, user := range allUsers {
		if _, ok := nonRndUsersSet[user.Email]; ok {
			nonRndUsers = append(nonRndUsers, user)
		} else {
			rndUsers = append(rndUsers, user)
		}
	}

	rndGroup := DatabricksGroup{
		Name:  dataplatformconsts.RndUserGroup,
		Users: rndUsers,
	}

	nonRndGroup := DatabricksGroup{
		Name:  dataplatformconsts.NonRndUserGroup,
		Users: nonRndUsers,
	}

	allGroups := make([]*DatabricksGroup, 0, len(allGroupMap)+2)
	for _, group := range allGroupMap {
		allGroups = append(allGroups, group)
	}
	allGroups = append(allGroups, &rndGroup, &nonRndGroup)

	sort.Slice(allGroups, func(i, j int) bool {
		return allGroups[i].Name < allGroups[j].Name
	})

	return &GroupHierarchy{
		AllUsers:  allUsers,
		AllGroups: allGroups,
		GroupTree: root,
	}, nil
}

// IncludeDevelopmentTeamUsersOnly filters a GroupHierarchy to only include
// users from development teams. This is used in Canada region to restrict
// Databricks access to development teams only.
func IncludeDevelopmentTeamUsersOnly(hierarchy *GroupHierarchy) []*DatabricksUser {
	var filteredUsers []*DatabricksUser

	// Filter users - only include those from development teams.
	for _, user := range hierarchy.AllUsers {
		// Check if this user belongs to any development team
		belongsToDevelopmentTeam := false
		for _, group := range hierarchy.AllGroups {
			if group.Team != nil && group.Team.Team() != nil && group.Team.Team().IsDevelopmentTeam() {
				// This is a development team group - check if user is in it.
				for _, groupUser := range group.Users {
					if groupUser.Email == user.Email {
						belongsToDevelopmentTeam = true
						break
					}
				}
			}
			if belongsToDevelopmentTeam {
				break
			}
		}

		if belongsToDevelopmentTeam {
			filteredUsers = append(filteredUsers, user)
		}
	}

	return filteredUsers
}

func nonRndUserList() map[string]struct{} {
	users := make(map[string]struct{}, len(databricksusers.DatabricksUserRegistry))
	for _, user := range databricksusers.DatabricksUserRegistry {
		users[user.Email] = struct{}{}
	}
	return users
}

// filterUsersByDisallowedRegion filters out users present in the Databricks user registry
// whose DisallowedRegions contains the specified region.
func filterUsersByDisallowedRegion(allUsers []*DatabricksUser, region string) []*DatabricksUser {
	// Build a set of emails disallowed for this region from the registry
	disallowed := make(map[string]struct{})
	for _, regUser := range databricksusers.DatabricksUserRegistry {
		for _, r := range regUser.DisallowedRegions {
			if r == region {
				disallowed[regUser.Email] = struct{}{}
				break
			}
		}
	}

	// Filter the provided users
	filtered := make([]*DatabricksUser, 0, len(allUsers))
	for _, u := range allUsers {
		if _, blocked := disallowed[u.Email]; blocked {
			continue
		}
		filtered = append(filtered, u)
	}
	return filtered
}
