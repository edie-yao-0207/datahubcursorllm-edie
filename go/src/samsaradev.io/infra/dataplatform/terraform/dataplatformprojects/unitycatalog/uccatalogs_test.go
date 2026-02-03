package unitycatalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team/components"
)

func TestNoDuplicateSchemaGrants(t *testing.T) {
	testCases := []struct {
		name   string
		schema databaseregistries.SamsaraDB
	}{
		{
			name: "owner team in read and readwrite",
			schema: databaseregistries.SamsaraDB{
				Name:      "test_db",
				OwnerTeam: components.TeamInfo{TeamName: "owner_team"},
				CanReadGroups: []components.TeamInfo{
					{TeamName: "owner_team"},
					{TeamName: "read_team"},
				},
				CanReadWriteGroups: []components.TeamInfo{
					{TeamName: "owner_team"},
					{TeamName: "write_team"},
				},
			},
		},
		{
			name: "readwrite team in read",
			schema: databaseregistries.SamsaraDB{
				Name:          "test_db2",
				OwnerTeamName: "owner_team",
				CanReadGroups: []components.TeamInfo{
					{TeamName: "read_team"},
					{TeamName: "write_team"},
				},
				CanReadWriteGroups: []components.TeamInfo{
					{TeamName: "write_team"},
				},
			},
		},
		{
			name: "owner team in read",
			schema: databaseregistries.SamsaraDB{
				Name:      "test_db2",
				OwnerTeam: components.TeamInfo{TeamName: "owner_team"},
				CanReadGroups: []components.TeamInfo{
					{TeamName: "read_team"},
					{TeamName: "owner_team"},
				},
				CanReadWriteGroups: []components.TeamInfo{
					{TeamName: "write_team"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resources, err := schemaAndGrantsResources(tc.schema, "test_catalog_ref", "test_catalog", infraconsts.SamsaraAWSDefaultRegion)
			require.NoError(t, err)

			var grantsResource *databricksresource_official.DatabricksGrants
			for _, resource := range resources {
				if gr, ok := resource.(*databricksresource_official.DatabricksGrants); ok {
					grantsResource = gr
					break
				}
			}

			require.NotNil(t, grantsResource, "Grants resource should exist")

			// Track number of grants per group
			grantCounts := make(map[string]int)
			for _, grant := range grantsResource.Grants {
				grantCounts[grant.Principal]++
				assert.Equal(t, 1, grantCounts[grant.Principal],
					"Group %s has %d grants, expected exactly 1. "+
						"We cannot have multiple grant statements for the same team because terraform will only apply one of them. "+
						"When granting read access to all teams and write access to specific teams, we should not generate "+
						"the read grant for teams receiving write access and the write grants for the owner team.",
					grant.Principal,
					grantCounts[grant.Principal])
			}
		})
	}
}
