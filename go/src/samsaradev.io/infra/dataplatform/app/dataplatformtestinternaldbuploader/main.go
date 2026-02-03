package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformtestinternalmodels"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/system"
)

const MetricName = "dataplatformtestinternaldbuploader.run.count"
const NOTE = "random note"
const JsonNotesTableName = "json_notes"

// This is Eileen's personal test org.
const TestOrgId = 33026

type DataPlatformTestInternalDBUploader struct {
	Clock                     clock.Clock
	ShardedDataPlatformModels dataplatformtestinternalmodels.PrimaryShardedDataplatformtestinternalModelsInput
}

func NewDataPlatformTestInternalDBUploader(
	clock clock.Clock,
	shardedDataPlatformModels dataplatformtestinternalmodels.PrimaryShardedDataplatformtestinternalModelsInput,
) *DataPlatformTestInternalDBUploader {
	return &DataPlatformTestInternalDBUploader{
		Clock:                     clock,
		ShardedDataPlatformModels: shardedDataPlatformModels,
	}
}

func main() {
	ctx := context.Background()
	var num int
	flag.IntVar(&num, "num", 0, "number of rows to insert")
	var region string
	flag.StringVar(&region, "region", "us-west-2", "region")
	var tableName string
	flag.StringVar(&tableName, "tableName", "notes", "name of table")
	flag.Parse()

	var dataPlatformWorker *DataPlatformTestInternalDBUploader
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Provide(NewDataPlatformTestInternalDBUploader),
		fx.Populate(&dataPlatformWorker),
	)
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}

	if err := run(ctx, dataPlatformWorker, region, num, tableName); err != nil {
		slog.Fatalw(ctx, "failed to run application", "error", err)
	}
	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}

func run(ctx context.Context, uploader *DataPlatformTestInternalDBUploader, region string, num int, tableName string) error {
	var shard *dataplatformtestinternalmodels.DataplatformtestinternalModels
	var err error
	if region == infraconsts.SamsaraAWSDefaultRegion {
		shard, err = uploader.ShardedDataPlatformModels.ShardedDataplatformtestinternalModels.ForOrg(ctx, TestOrgId)
		if err != nil {
			return oops.Wrapf(err, "%s", fmt.Sprintf("failed to find shard for org_id=%d in %s region", TestOrgId, region))
		}
	} else {
		// EU and CA have "dataplatformtestinternal_shard_0" or
		// "dataplatformtestinternal_shard_1". We want to hardcode this for non-US
		// regions since the test org doesn't exist outside of it.
		shardName := "dataplatformtestinternal_shard_0"
		shard, err = uploader.ShardedDataPlatformModels.ShardedDataplatformtestinternalModels.ForShard(ctx, shardName)
		if err != nil {
			return oops.Wrapf(err, "%s", fmt.Sprintf("failed to find shard for org_id=%d in %s region", TestOrgId, region))
		}
	}
	// skiplint: +loopedexpensivecall
	id, err := getLatestRowId(ctx, shard.DB(), tableName)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(MetricName, "result:error", "reason:get_latest_notes_id")
		return oops.Wrapf(err, "failed to get latest id from notes table")
	}
	// skiplint: +loopedexpensivecall
	err = insertCustomNumOfRowsIntoTableStartingWithId(ctx, shard.DB(), num, id+1, tableName, uploader.Clock.Now())
	if err != nil {
		monitoring.AggregatedDatadog.Incr(MetricName, "result:error", "reason:insert_rows")
		return oops.Wrapf(err, "%s", fmt.Sprintf("failed to insert rows into %s table", tableName))
	}
	monitoring.AggregatedDatadog.Incr(MetricName, "result:success")
	return nil
}

// Gets the id from the most recently created row.
func getLatestRowId(ctx context.Context, sqlDB *sql.DB, tableName string) (int, error) {
	// skiplint: +loopedexpensivecall
	rows, queryErr := sqlDB.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s WHERE org_id = %d ORDER BY created_at DESC, id DESC LIMIT 1;", tableName, TestOrgId))
	if queryErr != nil && queryErr != sql.ErrNoRows {
		return 0, oops.Wrapf(queryErr, "%s", fmt.Sprintf("failed to query row from %s table", tableName))
	}
	defer rows.Close()
	var id int
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, oops.Wrapf(err, "%s", fmt.Sprintf("failed to get id from %s table", tableName))
		}
	}
	if rows.Err() != nil {
		return 0, oops.Wrapf(rows.Err(), "%s", fmt.Sprintf("failed to scan rows from %s table", tableName))
	}
	return id, nil
}

func insertCustomNumOfRowsIntoTableStartingWithId(ctx context.Context, sqlDB *sql.DB, num, id int, tableName string, currentTime time.Time) error {
	newRows := generateRowsForTable(id, num, tableName)
	for id, note := range newRows {
		var execErr error
		if tableName == JsonNotesTableName {
			// skiplint: +loopedexpensivecall
			_, execErr = sqlDB.ExecContext(ctx, "INSERT INTO json_notes (id, org_id, note, created_at) VALUES (?, ?, ?, ?)", id, TestOrgId, note, currentTime)
		} else {
			// skiplint: +loopedexpensivecall
			_, execErr = sqlDB.ExecContext(ctx, "INSERT INTO notes (id, org_id, note, created_at) VALUES (?, ?, ?, ?)", id, TestOrgId, note, currentTime)
		}
		if execErr != nil {
			return oops.Wrapf(execErr, "%s", fmt.Sprintf("failed to insert row into %s table", tableName))
		}
	}
	return nil
}

func generateRowsForTable(startingId, num int, tableName string) map[int]string {
	rows := make(map[int]string)
	for i := 0; i < num; i++ {
		id := startingId + i
		note := fmt.Sprintf("%s %d", NOTE, id)
		if tableName == JsonNotesTableName {
			noteStruct := map[string]string{"note": note}
			noteJSONBytes, _ := json.Marshal(noteStruct)
			note = string(noteJSONBytes)
		}
		rows[id] = note
	}
	return rows
}
