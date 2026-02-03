package dataplatformtestinternalmodels_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformtestinternalmodels"
	"samsaradev.io/infra/dbtools/dbconfighelpers"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/testloader"
)

func TestDataplatformtestinternalMigrate(t *testing.T) {
	var env struct {
		AppConfig *config.AppConfig
	}
	testloader.MustStart(t, &env)

	baseDB, err := dbhelpers.OpenMySQL(env.AppConfig.GetTestBaseDatabaseConfig())
	require.NoError(t, err)

	// Create a new dataplatformtestinternal database and attempt to migrate it.
	dbConfig := dbconfighelpers.GetDataplatformtestinternalDatabaseConfigForCurrentClusterAndApp(env.AppConfig)
	dbConfig.DatabaseName = dbhelpers.TestDBName(dbConfig)
	dbConfig.IsTest = true
	dbhelpers.CreateNewTestDB(baseDB, dbConfig)

	dbhelpers.MigratePending(dataplatformtestinternalmodels.Versions, dbConfig, false, true)

	handle, err := dbhelpers.OpenMySQL(dbConfig)
	require.NoError(t, err)

	defer handle.Close()
	dbhelpers.DropTestDatabase(handle)
}
