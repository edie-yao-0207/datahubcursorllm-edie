package clusterhelpers

import (
	"strings"

	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/databricksusers"
)

// databricksClusterTeams is the memoized result of a DatabricksClusterTeams
// invocation.
var databricksClusterTeams []components.TeamInfo

// DatabricksClusterTeams returns list of teams that have Databricks clusters.
func DatabricksClusterTeams() []components.TeamInfo {
	if len(databricksClusterTeams) > 0 {
		return databricksClusterTeams
	}

	for _, t := range team.AllTeams {
		if t.NonDevelopment && !t.KeepEmptyGroup && len(databricksusers.UsersOnTeam(t.Name())) == 0 {
			continue
		}
		databricksClusterTeams = append(databricksClusterTeams, t)
	}
	return databricksClusterTeams
}

// Helper checks if the given instance type is in the family of supported
// Nitro instance type by Databricks.
func ValidDatabricksClusterNitroInstanceType(instanceType string) bool {
	instanceType = strings.ToLower(instanceType)
	for _, prefix := range dataplatformconsts.DatabricksNitroInstanceTypePrefixes {
		if strings.HasPrefix(instanceType, prefix) {
			return true
		}
	}
	return false
}
