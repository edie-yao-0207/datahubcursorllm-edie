package dataplatformprojects

import (
	"testing"

	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
)

// TestCursorQueryAgentTeamsMatch ensures that the teams with access to the cursor-query-agent SQL endpoint
// match the teams with read access to the DataHubMetadataBucket volume. These two lists must always match.
func TestCursorQueryAgentTeamsMatch(t *testing.T) {
	// Get teams from the shared constant
	expectedTeams := dataplatformterraformconsts.CursorQueryAgentTeams()

	// Get teams from the volume registry
	volumeSchemas := unitycatalog.S3BucketVolumeSchemas
	var dataHubMetadataVolumeTeams []string
	for _, schema := range volumeSchemas {
		if schema.UnprefixedBucketLocation == databaseregistries.DataHubMetadataBucket {
			for _, volume := range schema.VolumePrefixes {
				if volume.Name == "root" {
					for _, team := range volume.ReadTeams {
						dataHubMetadataVolumeTeams = append(dataHubMetadataVolumeTeams, team.TeamName)
					}
					break
				}
			}
			break
		}
	}

	// Verify the volume registry uses the shared constant
	require.Equal(t, len(expectedTeams), len(dataHubMetadataVolumeTeams),
		"DataHubMetadataBucket volume should have the same number of teams as CursorQueryAgentTeams")

	// Create a map of expected team names for easy lookup
	expectedTeamNames := make(map[string]bool)
	for _, team := range expectedTeams {
		expectedTeamNames[team.TeamName] = true
	}

	// Verify all volume teams are in the expected list
	for _, teamName := range dataHubMetadataVolumeTeams {
		require.True(t, expectedTeamNames[teamName],
			"Team %s in DataHubMetadataBucket volume should be in CursorQueryAgentTeams", teamName)
	}

	// Verify all expected teams are in the volume list
	volumeTeamNames := make(map[string]bool)
	for _, teamName := range dataHubMetadataVolumeTeams {
		volumeTeamNames[teamName] = true
	}

	for _, team := range expectedTeams {
		require.True(t, volumeTeamNames[team.TeamName],
			"Team %s in CursorQueryAgentTeams should be in DataHubMetadataBucket volume", team.TeamName)
	}
}

