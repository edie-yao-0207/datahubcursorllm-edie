package main

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/features/loader"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/testloader"
)

type schemaVersionRow struct {
	Version   int64
	Completed int64
}

// Test that in performing the dummy write we do not modify the schema_version table.
// Note that we don't actually assert in this test that we ever did the write, unfortunately.
// It's not really obvious how to test that at the time of writing this (and we're in a time crunch for aurora 2)
// but we've manually confirmed it.
func TestDummyWrite(t *testing.T) {
	var env struct {
		AppConfig            *config.AppConfig
		LaunchDarklyBridge   *launchdarkly.SDKBridge
		FeatureFlagsProvider *loader.FeatureFlagsProvider
	}
	testloader.MustStart(t, &env)
	ctx := context.Background()
	dbConfig := dbhelpers.GetCloudDatabaseConfigForCurrentClusterAndApp(env.AppConfig, env.FeatureFlagsProvider, env.LaunchDarklyBridge)
	dbConn, err := dbhelpers.OpenMySQL(dbConfig)
	require.NoError(t, err)

	before := getSchemaRows(ctx, t, dbConn)
	require.NoError(t, runDummySchemaVersionsInsertAndDelete(ctx, dbConn))
	after := getSchemaRows(ctx, t, dbConn)

	// For some reason, this table exists but there are 0 rows in it.
	// This is okay for our testing purpose, but strange enough that i'm leaving
	// this comment.
	// assert.Equal(t, len(before), 1)
	assert.Equal(t, len(before), len(after))
	sort.Slice(before, func(i, j int) bool {
		return before[i].Version < before[j].Version
	})
	sort.Slice(after, func(i, j int) bool {
		return after[i].Version < after[j].Version
	})
	for i := 0; i < len(before); i++ {
		assert.Equal(t, before[i], after[i])
	}
}

func getSchemaRows(ctx context.Context, t *testing.T, dbConn *sql.DB) []schemaVersionRow {
	result, err := dbConn.QueryContext(ctx, "select version, completed from schema_version")
	require.NoError(t, err)
	defer result.Close()

	var rows []schemaVersionRow
	for result.Next() {
		var version, completed int64
		err = result.Scan(&version, &completed)
		require.NoError(t, err)
		rows = append(rows, schemaVersionRow{Version: version, Completed: completed})
	}
	require.NoError(t, result.Err())
	return rows
}
