package dataplatformprojects

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/team"
)

func TestIncludeDevelopmentTeamUsersOnly(t *testing.T) {
	t.Run("uses real team hierarchy and verifies team classifications", func(t *testing.T) {
		originalHierarchy, err := MakeGroupHierarchy()
		assert.NoError(t, err)

		var dataPlatformGroup *DatabricksGroup
		var biztechGroup *DatabricksGroup

		for _, group := range originalHierarchy.AllGroups {
			if group.Team != nil && group.Team.Team() != nil {
				teamInfo := group.Team.Team()
				switch teamInfo.TeamName {
				case team.DataPlatform.TeamName:
					dataPlatformGroup = group
					assert.True(t, teamInfo.IsDevelopmentTeam(), "DataPlatform should be a development team")
				case team.BizTechEnterpriseData.TeamName:
					biztechGroup = group
					assert.False(t, teamInfo.IsDevelopmentTeam(), "BizTechEnterpriseData should be a non-development team")
				}
			}
		}

		assert.NotNil(t, dataPlatformGroup, "DataPlatform group should exist in hierarchy")
		assert.NotNil(t, biztechGroup, "BizTechEnterpriseData group should exist in hierarchy")

		filteredUsers := IncludeDevelopmentTeamUsersOnly(originalHierarchy)

		expectedDataPlatformUserEmails := make([]string, 0)
		for _, user := range dataPlatformGroup.Users {
			expectedDataPlatformUserEmails = append(expectedDataPlatformUserEmails, user.Email)
		}

		biztechUserEmails := make([]string, 0)
		for _, user := range biztechGroup.Users {
			biztechUserEmails = append(biztechUserEmails, user.Email)
		}

		actualDataPlatformUserEmails := 0
		actualBiztechUserEmails := 0
		for _, user := range filteredUsers {
			for _, dpEmail := range expectedDataPlatformUserEmails {
				if user.Email == dpEmail {
					actualDataPlatformUserEmails++
					break
				}
			}
			for _, btEmail := range biztechUserEmails {
				if user.Email == btEmail {
					actualBiztechUserEmails++
					break
				}
			}
		}

		// DataPlatform users should remain (development team).
		assert.Equal(t, len(expectedDataPlatformUserEmails), actualDataPlatformUserEmails,
			"All DataPlatform users should remain after filtering")

		// BizTech users should be filtered out (non-development team).
		assert.Equal(t, 0, actualBiztechUserEmails,
			"No BizTechEnterpriseData users should remain after filtering")
	})
}
