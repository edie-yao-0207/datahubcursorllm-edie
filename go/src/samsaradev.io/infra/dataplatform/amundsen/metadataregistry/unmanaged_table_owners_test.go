package metadataregistry

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/s3tables"
	"samsaradev.io/infra/dataplatform/s3views"
	"samsaradev.io/libs/ni/infraconsts"
)

func TestUnmanagedTableOwners(t *testing.T) {
	managedTables := make(map[string]struct{})

	for _, table := range ksdeltalake.AllTables() {
		managedTables[table.QualifiedName()] = struct{}{}
		managedTables[table.S3BigStatsName()] = struct{}{}
	}

	for _, stream := range datastreamlake.Registry {
		managedTables[fmt.Sprintf("datastreams.%s", stream.StreamName)] = struct{}{}
	}

	nodes, err := configvalidator.ReadNodeConfigurations()
	require.NoError(t, err)

	for _, node := range nodes {
		managedTables[node.Name] = struct{}{}
	}

	for _, db := range rdsdeltalake.AllDatabases() {
		for _, table := range db.TablesInRegion(infraconsts.SamsaraAWSDefaultRegion) {
			managedTables[fmt.Sprintf("%s.%s", db.Name, table.TableName)] = struct{}{}
		}
	}

	s3Tables, err := s3tables.ReadS3TableFiles()
	require.NoError(t, err)
	for s3TableName := range s3Tables {
		managedTables[s3TableName] = struct{}{}
	}

	sqlViews, err := s3views.ReadSQLViewFiles()
	require.NoError(t, err)
	for sqlViewName := range sqlViews {
		managedTables[sqlViewName] = struct{}{}
	}

	for tableName := range Owners {
		t.Run("test table %s is not managed", func(t *testing.T) {
			if _, ok := managedTables[tableName]; ok {
				t.Fatalf("table %s is a managed table. owners is populated in the tables respective registry", tableName)
			}
		})
	}
}
