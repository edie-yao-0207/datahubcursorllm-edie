package dataplatformprojects

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/customdatabricksimages"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/team"
)

func TestInteractiveClustersOnlySupportedInstanceTypes(t *testing.T) {
	supportedInstanceTypes := dataplatformconsts.DatabricksNitroInstanceTypePrefixes
	teams := clusterhelpers.DatabricksClusterTeams()

	for _, teamInfo := range teams {
		if teamInfo.DatabricksInteractiveClusterOverrides.DriverNodeType != "" {
			driverNode := teamInfo.DatabricksInteractiveClusterOverrides.DriverNodeType
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(driverNode),
				fmt.Sprintf("Unsupported driver instance type: %s, for team %s, only support types are: %v", driverNode, teamInfo.TeamName, supportedInstanceTypes),
			)
		}

		if teamInfo.DatabricksInteractiveClusterOverrides.WorkerNodeType != "" {
			workerNode := teamInfo.DatabricksInteractiveClusterOverrides.WorkerNodeType
			assert.Equal(
				t,
				true,
				clusterhelpers.ValidDatabricksClusterNitroInstanceType(workerNode),
				fmt.Sprintf("Unsupported worker instance type: %s, for team %s, only support types are: %v", workerNode, teamInfo.TeamName, supportedInstanceTypes),
			)
		}

		// Teams can define more clusters, check for those instance types.
		clusters := teamInfo.DatabricksInteractiveClusters

		for _, cluster := range clusters {

			if cluster.DriverNodeType != "" {
				driverNode := cluster.DriverNodeType
				assert.Equal(
					t,
					true,
					clusterhelpers.ValidDatabricksClusterNitroInstanceType(driverNode),
					fmt.Sprintf("Unsupported driver instance type: %s, for team cluster %s, only support types are: %v", driverNode, cluster.Name, supportedInstanceTypes),
				)
			}

			if cluster.WorkerNodeType != "" {
				workerNode := cluster.WorkerNodeType
				assert.Equal(
					t,
					true,
					clusterhelpers.ValidDatabricksClusterNitroInstanceType(workerNode),
					fmt.Sprintf("Unsupported worker instance type: %s, for team cluster %s, only support types are: %v", workerNode, cluster.Name, supportedInstanceTypes),
				)
			}
		}
	}
}

func TestAllowedCustomDockerClusters(t *testing.T) {
	// We don't want to unintentionally add clusters that don't need custom docker images
	// as they are hard to maintain and work with.
	// This test serves as a second line of defense to make sure when new clusters are added
	// that reference custom docker images, it's done so intentionally.
	// If you want to add to this allowlist, please ask in #ask-data-platform
	// so we can confirm that custom docker images are necessary (and the issue can't be solved
	// with init scripts, cluster libraries, or other means).

	allowList := map[customdatabricksimages.ECRRepoName]map[string]struct{}{}

	var errors []string
	for _, t := range team.AllTeams {
		if t.DatabricksInteractiveClusterOverrides.CustomRepoName != "" {
			repo := t.DatabricksInteractiveClusterOverrides.CustomRepoName
			clusterName := strings.ToLower(t.TeamName)
			if _, ok := allowList[repo]; ok {
				if _, ok := allowList[repo][clusterName]; !ok {
					errors = append(errors, fmt.Sprintf("Cluster %s is not part of the allowlist for image %s", clusterName, repo))
				} else {
					delete(allowList[repo], clusterName)
				}
			} else {
				errors = append(errors, fmt.Sprintf("Cluster %s references a custom docker image %s, which is not a valid image in the allowlist", clusterName, repo))
			}
		}
		for _, cluster := range t.DatabricksInteractiveClusters {
			if cluster.CustomRepoName != "" {
				repo := cluster.CustomRepoName
				clusterName := strings.ToLower(t.TeamName) + "-" + cluster.Name
				if _, ok := allowList[repo]; ok {
					if _, ok := allowList[repo][clusterName]; !ok {
						errors = append(errors, fmt.Sprintf("Cluster %s is not part of the allowlist for image %s", clusterName, repo))
					} else {
						delete(allowList[repo], clusterName)
					}
				} else {
					errors = append(errors, fmt.Sprintf("Cluster %s references a custom docker image %s, which is not a valid image in the allowlist", clusterName, repo))
				}
			}
		}
	}

	// Ensure there's no un-allowlisted elements as well as that the allowlist doesn't contain
	// extras.
	if len(errors) > 0 {
		t.Errorf("Errors found: %s", strings.Join(errors, "\n"))
	}
	for image, elems := range allowList {
		if len(elems) > 0 {
			t.Errorf("Clusters %v are allowlisted for image %s but not using that image. Please cleanup allowlist", elems, image)
		}
	}
}

func TestSanitizeClusterName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ASCII only",
			input:    "John Doe",
			expected: "John-Doe",
		},
		{
			name:     "With spaces",
			input:    "John Smith Doe",
			expected: "John-Smith-Doe",
		},
		{
			name:     "With accented characters",
			input:    "Łukasz Smith",
			expected: "ukasz-Smith",
		},
		{
			name:     "With multiple non-Latin1 characters",
			input:    "José Ñandú",
			expected: "Jos-and",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := SanitizeClusterName(tc.input)
			if result != tc.expected {
				t.Errorf("sanitizeClusterName(%q) = %q, expected %q", tc.input, result, tc.expected)
			}
		})
	}
}
