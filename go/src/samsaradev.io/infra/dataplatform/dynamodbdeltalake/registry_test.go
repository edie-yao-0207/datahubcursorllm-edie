package dynamodbdeltalake

import (
	"sort"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
)

func TestIsUsedForDataLakeDeletion(t *testing.T) {
	// Snapshots the existing set of tables that have `IsUsedForDataLakeDeletion` set.
	// No new tables should have this set and we should never add new things to the snapshot.
	// This field should only be set by the data platform team for tables used in data deletion purposes.
	// If you need to add a table to this list, please consult with the data platform team.

	snap := snapshotter.New(t)
	defer snap.Verify()

	// Explicit allowlist of tables that can have IsUsedForDataLakeDeletion set to true.
	allowedTables := map[string]struct{}{
		// Add tables here that are allowed to have IsUsedForDataLakeDeletion set to true.
		// Example: "table-name": {},
		"ks0-kinesisstats-deleter-journal": {},
		"ks1-kinesisstats-deleter-journal": {},
		"ks2-kinesisstats-deleter-journal": {},
		"ks3-kinesisstats-deleter-journal": {},
		"ks4-kinesisstats-deleter-journal": {},
		"ks5-kinesisstats-deleter-journal": {},
	}

	var tables []string
	for _, table := range AllTables() {
		if table.InternalOverrides.IsUsedForDataLakeDeletion {
			tables = append(tables, table.TableName)

			_, allowed := allowedTables[table.TableName]
			assert.True(t, allowed,
				"Table %q should not have IsUsedForDataLakeDeletion flag set. "+
					"This field should only be set by the data platform team for tables used in data deletion purposes.",
				table.TableName)
		}
	}
	sort.Strings(tables)
	snap.Snapshot("tables with IsUsedForDataLakeDeletion set", tables)
}

