package unitycatalog

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"

	"samsaradev.io/infra/testloader"
)

type testEnv struct {
	Snap *snapshotter.Snapshotter
}

func TestShareRegistryAlwaysUsesThreeLevelNamespace(t *testing.T) {
	for _, share := range ShareRegistry {
		for _, obj := range share.ShareObjects {
			parts := strings.Split(obj.Name, ".")
			if len(parts) != 3 {
				t.Errorf("Share object %s does not have a three-level namespace. Must be in format catalog.db.table", obj.Name)
			}
		}
	}
}

func TestShareRegistry_CatalogSnapshots(t *testing.T) {
	catalogTablesMap := make(map[string]map[string]struct{})

	for _, share := range ShareRegistry {
		for _, obj := range share.ShareObjects {
			parts := strings.Split(obj.Name, ".")
			catalog := parts[0]
			if _, exists := catalogTablesMap[catalog]; !exists {
				catalogTablesMap[catalog] = make(map[string]struct{})
			}
			catalogTablesMap[catalog][obj.Name] = struct{}{}
		}
	}

	// Convert the map of maps to a map of slices
	catalogTables := make(map[string][]string)
	for catalog, tables := range catalogTablesMap {
		for table := range tables {
			catalogTables[catalog] = append(catalogTables[catalog], table)
		}
	}

	for catalog, tables := range catalogTables {
		t.Run(catalog, func(t *testing.T) {
			var env testEnv
			testloader.MustStart(t, &env)

			sort.Strings(tables)

			snapshotName := fmt.Sprintf("%s tables shared in a delta share", catalog)
			env.Snap.Snapshot(snapshotName, tables)
		})
	}
}

// Views and Delta Shares don't work well together right now. This may be removed in the future.
func TestShareRegistryNoViews(t *testing.T) {
	for _, share := range ShareRegistry {
		for key := range share.ShareObjects {
			object := share.ShareObjects[key]
			if object.DataObjectType == ObjectTypeView {
				t.Errorf("Share object %s should not be a view. Must be a table or another allowable type", object.Name)
			}
		}
	}
}
