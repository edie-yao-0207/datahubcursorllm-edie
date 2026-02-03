package govramp

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/team"
)

// Test to curate the list of nonDevelopment teams in R&D with access to the default catalog in Databricks.
func TestNonDevTeamsInRnDWithDefaultCatalogAccess(t *testing.T) {
	// List of teams in R&D with NonDevelopment set to true
	nonDevRnDTeams := []string{
		team.EngineeringOperations.TeamName,
		team.FirmwareGithubAdmins.TeamName,
		team.Hardware.TeamName,
		team.PSEDataAnalytics.TeamName,
		team.ProductDesign.TeamName,
		team.ProductManagement.TeamName,
		team.ProductOperations.TeamName,
		team.RDOperations.TeamName,
		team.SafetyFirmwareApps.TeamName,
		team.SafetyFirmwareOSPlatform.TeamName,
		team.SafetyFirmwarePerception.TeamName,
		team.SecOps.TeamName,
		team.Support.TeamName,
		team.TPM.TeamName,
		team.VulnManagement.TeamName,
	}

	// Ensure that all the teams listed above have NonDevelopment and NonDevelopmentTeamHasAccessToDefaultCatalog set to true
	for _, rdTeamName := range nonDevRnDTeams {
		foundTeam := team.TeamFromTeamName(rdTeamName)
		assert.NotNil(t, foundTeam, "Team %s should exist in team registry", rdTeamName)
		if foundTeam != nil {
			assert.True(t, foundTeam.NonDevelopment, "The team %s should have NonDevelopment set to true", rdTeamName)
			assert.True(t, foundTeam.NonDevelopmentTeamHasAccessToDefaultCatalog, "The team %s should have NonDevelopmentTeamHasAccessToDefaultCatalog set to true", rdTeamName)
		}
	}

	// Ensure that there is no other teams that have NonDevelopment and NonDevelopmentTeamHasAccessToDefaultCatalog set to true
	// outside of the above list. If there are, the team should be added to the above list.
	for _, tm := range team.AllTeams {
		if tm.NonDevelopment && tm.NonDevelopmentTeamHasAccessToDefaultCatalog {
			if !slices.Contains(nonDevRnDTeams, tm.TeamName) {
				t.Errorf("Team %s has NonDevelopment and NonDevelopmentTeamHasAccessToDefaultCatalog set to true. If the team is a valid nonDevelopment team in R&D, please add it to the above list.", tm.TeamName)
			}
		}
	}
}
