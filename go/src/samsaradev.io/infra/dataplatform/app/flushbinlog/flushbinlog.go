package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/config/databaseconfig"
	"samsaradev.io/infra/dbtools/dbhelpers"
)

func FlushBinlogForDb(appconfig *config.AppConfig, db string, dryRun bool, withDummyWrite bool) error {
	// Add the db name to the slogging context.
	// I am not 100% sure how context cancellations will work when we do this (and below),
	// but given this is a one-off script I'm not sure thats as big of an issue as having improved logging.
	ctx := slog.With(context.Background(), "db", db)
	slog.Infow(ctx, "Opening mysql connection")

	shardedMap := getDbToShardedConfigMap(appconfig)
	unshardedMap := getDbToConfigMap(appconfig)

	if _, ok := shardedMap[db]; ok {
		for shard, dbConfig := range shardedMap[db]().Shards {
			// For the case of sharded dbs, add in the shard name as well here.
			ctx := slog.With(ctx, "shard", shard)
			slog.Infow(ctx, fmt.Sprintf("=> Running for Shard %s", shard))
			// skiplint: +loopedexpensivecall
			err := flushAndWriteToSchemaVersion(ctx, dbConfig.Writer, dryRun, withDummyWrite)
			if err != nil {
				return oops.Wrapf(err, "Failed to run for shard %s", shard)
			}
			slog.Infow(ctx, "=> Done.")
		}
	} else if configFunc, ok := unshardedMap[db]; ok {
		dbConfig := configFunc()
		slog.Infow(ctx, fmt.Sprintf("=> Running for the only shard of %s", dbConfig.DatabaseName))
		// skiplint: +loopedexpensivecall
		err := flushAndWriteToSchemaVersion(ctx, dbConfig, dryRun, withDummyWrite)
		if err != nil {
			return oops.Wrapf(err, "Failed to run for %s", db)
		}
		slog.Infow(ctx, "=> Done.")
	} else {
		return oops.Errorf("Invalid db passed to FlushBinlogForDb; this shouldn't have happened.")
	}

	return nil
}

func flushAndWriteToSchemaVersion(ctx context.Context, config databaseconfig.DatabaseConfig, dryRun bool, withDummyWrite bool) error {
	// Connect and set up a deferred close.
	dbconn, err := dbhelpers.OpenMySQL(config)
	if err != nil {
		return oops.Wrapf(err, "failed to connect to db")
	}
	defer dbconn.Close()

	// Run `show binary logs` before starting.
	before, err := runShowBinaryLogs(ctx, dbconn)
	if err != nil {
		return oops.Wrapf(err, "failed to list binary logs before flushing")
	}
	if len(before) > 0 {
		slog.Infow(ctx, fmt.Sprintf("Before Flush: %d files between %s and %s", len(before), before[0], before[len(before)-1]))
	} else {
		return oops.Errorf("No rows returned from show binary logs; this is an error.")
	}

	// If it's a dry run, lets end here.
	if dryRun {
		slog.Infow(ctx, "Dry run is true; ending after listing binary log files.")
		return nil
	}

	// Run the flush command.
	err = runFlushBinaryLogs(ctx, dbconn)
	if err != nil {
		return oops.Wrapf(err, "failed to run flush binary logs")
	}

	// After running flush, lets list the binary logs again. We should see an extra file (and maybe an old file evicted).
	after, err := runShowBinaryLogs(ctx, dbconn)
	if err != nil {
		return oops.Wrapf(err, "failed to list binary logs after flushing")
	}

	if len(after) > 0 {
		slog.Infow(ctx, fmt.Sprintf("After Flush: %d files between %s and %s", len(after), after[0], after[len(after)-1]))
	} else {
		return oops.Errorf("No rows returned from show binary logs after flush; this is a REALLY big error go check the db.")
	}

	if withDummyWrite {
		slog.Infow(ctx, "withDummyWrite set to true, inserting a row into schema_versions and deleting it")
		err = runDummySchemaVersionsInsertAndDelete(ctx, dbconn)
		if err != nil {
			return oops.Wrapf(err, "error running schema version insert and delete")
		}
	}

	return nil
}

type showBinlogList struct {
	filename string
	filesize string
}

func runShowBinaryLogs(ctx context.Context, dbconn *sql.DB) ([]showBinlogList, error) {
	var list []showBinlogList
	rows, err := dbconn.QueryContext(ctx, "show binary logs")
	if err != nil {
		return nil, oops.Wrapf(err, "failed to run show binary logs")
	}
	defer rows.Close()
	for rows.Next() {
		var filename, filesize string
		err = rows.Scan(&filename, &filesize)
		if err != nil {
			return nil, oops.Wrapf(err, "failed to scan rows returned from db")
		}
		list = append(list, showBinlogList{filename: filename, filesize: filesize})
	}
	if rows.Err() != nil {
		return nil, oops.Wrapf(rows.Err(), "")
	}

	// The results have always been ordered whenever i've seen
	// them, but let's sort just to be 100% safe.
	sort.Slice(list, func(i, j int) bool {
		return list[i].filename < list[j].filename
	})

	return list, nil
}

func runFlushBinaryLogs(ctx context.Context, dbconn *sql.DB) error {
	_, err := dbconn.ExecContext(ctx, "flush binary logs")
	if err != nil {
		return oops.Wrapf(err, "failed to run flush binary logs")
	}
	return nil
}

func runDummySchemaVersionsInsertAndDelete(ctx context.Context, dbconn *sql.DB) error {
	printSchemaVersionTable(ctx, dbconn, "Schema Rows before any insertions")
	_, err := dbconn.ExecContext(ctx, "insert into schema_version values (-1, -1)")
	if err != nil {
		return oops.Wrapf(err, "failed to run insert into schema versions")
	}

	printSchemaVersionTable(ctx, dbconn, "Schema Rows after inserting dummy value")

	_, err = dbconn.ExecContext(ctx, "delete from schema_version where version = -1 and completed = -1")
	if err != nil {
		return oops.Wrapf(err, "failed to run delete from schema versions")
	}

	printSchemaVersionTable(ctx, dbconn, "Schema Rows after deleting dummy value")

	return nil
}

func printSchemaVersionTable(ctx context.Context, dbconn *sql.DB, actionMessage string) {
	rows, err := dbconn.QueryContext(ctx, "select version, completed from schema_version")
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "failed to select from schema version. NOT stopping the script, but just logging"), nil)
	} else {
		defer rows.Close()
		var schemaRows []string
		for rows.Next() {
			var version, completed int64
			err = rows.Scan(&version, &completed)
			if err != nil {
				slog.Errorw(ctx, oops.Wrapf(err, "failed to scan rows from schema version. not erroring the script, but just printing"), nil)
				break
			}
			schemaRows = append(schemaRows, fmt.Sprintf("[%d %d]", version, completed))
		}
		if rows.Err() != nil {
			slog.Errorw(ctx, oops.Wrapf(err, ""), nil)
		}
		slog.Infow(ctx, fmt.Sprintf("%s %s", actionMessage, strings.Join(schemaRows, ",")))
	}
}
