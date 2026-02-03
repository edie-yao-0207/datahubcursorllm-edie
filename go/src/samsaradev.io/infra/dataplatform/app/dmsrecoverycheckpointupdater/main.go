package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/config/databaseconfig"
	"samsaradev.io/infra/dataplatform/app/dmsrecoverycheckpointupdater/internal/features"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/system"
)

const metricName = "dms_recovery_checkpoint_updater.databases.processed.count"

const createTbl = "CREATE TABLE IF NOT EXISTS dataplat_internal (version BIGINT(20) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
const insertInto = "INSERT INTO dataplat_internal VALUES (1);"
const dropTable = "DROP TABLE IF EXISTS dataplat_internal;"

// dmsrecoverycheckpointupdater is a daily cron job that indirectly updates the
// DMS recovery checkpoint for low-write traffic databases. There's an aurora 2
// DMS bug that can cause DMS recovery checkpoints to point to a binlog file
// that has been retentioned away without any new writes. As a result, the DMS
// task will fail because it cannot resume from its old binary log position
// since that file no longer exists. dmsrecoverycheckpointupdater is a temporary
// workaround that fixes this issue by inserting a dummy write to these
// low-write databases.
// https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5397541/How+To+Get+The+Recovery+Checkpoint+To+Move
func main() {
	ctx := context.Background()
	var appConfig *config.AppConfig
	var launchDarklyBridge *launchdarkly.SDKBridge
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(&appConfig),
		fx.Populate(&launchDarklyBridge),
	)
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}
	if err := run(ctx, appConfig, launchDarklyBridge); err != nil {
		slog.Fatalw(ctx, "failed to run application", "error", err)
	}
	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}

func run(ctx context.Context, appConfig *config.AppConfig, launchDarklyBridge *launchdarkly.SDKBridge) error {
	databasesToWriterConfig := databasesToForceUpdateDmsRecoveryCheckpoint(ctx, appConfig, launchDarklyBridge)
	var failedDbs []string
	for dbName, writer := range databasesToWriterConfig {
		slog.Infow(ctx, fmt.Sprintf("Running for %s", dbName))
		// skiplint: +loopedexpensivecall
		err := insertAndDeleteDummyWriteForDatabase(ctx, dbName, writer)
		if err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "failed to insert and delete dataplat_internal dummy write for db %s", dbName), nil)
			failedDbs = append(failedDbs, dbName)
		}
		slog.Infow(ctx, fmt.Sprintf("Completed for %s", dbName))
	}

	if len(failedDbs) > 0 {
		slog.Errorw(ctx, oops.Errorf("Failed to process some dbs, see logs for errors %s\n", failedDbs), nil)
	}

	return nil
}

func databasesToForceUpdateDmsRecoveryCheckpoint(
	ctx context.Context,
	appConfig *config.AppConfig,
	launchDarklyBridge *launchdarkly.SDKBridge) map[string]databaseconfig.DatabaseConfig {
	shardedMap := getDbToShardedConfigMap(appConfig)
	unshardedMap := getDbToConfigMap(appConfig)
	databasesToWriterConfig := make(map[string]databaseconfig.DatabaseConfig)

	var databasesToUpdateDmsRecoveryCheckpoint []string
	var databasesToNotUpdateDmsRecoveryCheckpoint []string

	// Find all sharded dbs to force update the dms recovery checkpoint.
	for dbName, shardedConfigs := range shardedMap {
		dbConfig := shardedConfigs()
		enabledForAllShards := features.UpdateDmsRecoveryCheckpointCronJobWithBridge(&features.BackendUser{
			ShardedDatabaseName: dbName,
		}, launchDarklyBridge)
		// Disabled for all shards of db.
		if !enabledForAllShards {
			for shard := range dbConfig.Shards {
				databasesToNotUpdateDmsRecoveryCheckpoint = append(databasesToNotUpdateDmsRecoveryCheckpoint, shard)
			}
			continue
		}

		// Otherwise, check if enabled for specific shards.
		for shard, dbConfig := range dbConfig.Shards {
			enabledForShard := features.UpdateDmsRecoveryCheckpointCronJobWithBridge(&features.BackendUser{
				ShardSpecificDatabaseName: shard,
			}, launchDarklyBridge)
			// Disabled for this shard of db.
			if !enabledForShard {
				databasesToNotUpdateDmsRecoveryCheckpoint = append(databasesToNotUpdateDmsRecoveryCheckpoint, shard)
			} else {
				databasesToWriterConfig[shard] = dbConfig.Writer
				databasesToUpdateDmsRecoveryCheckpoint = append(databasesToUpdateDmsRecoveryCheckpoint, shard)
			}
		}
	}

	// Find all unsharded dbs to force update the dms recovery checkpoint.
	for dbName, dbConfig := range unshardedMap {
		writer := dbConfig()
		enabledForDb := features.UpdateDmsRecoveryCheckpointCronJobWithBridge(&features.BackendUser{
			UnshardedDatabaseName: dbName,
		}, launchDarklyBridge)
		// Enabled for this unsharded db.
		if enabledForDb {
			databasesToWriterConfig[dbName] = writer
			databasesToUpdateDmsRecoveryCheckpoint = append(databasesToUpdateDmsRecoveryCheckpoint, dbName)
		} else {
			databasesToNotUpdateDmsRecoveryCheckpoint = append(databasesToNotUpdateDmsRecoveryCheckpoint, dbName)
		}
	}

	slog.Infow(ctx, "updating dms recovery checkpoint for these databases", "databases", strings.Join(databasesToUpdateDmsRecoveryCheckpoint, ", "))
	slog.Infow(ctx, "not updating dms recovery checkpoint for these databases", "databases", strings.Join(databasesToNotUpdateDmsRecoveryCheckpoint, ", "))
	return databasesToWriterConfig
}

func insertAndDeleteDummyWriteForDatabase(ctx context.Context, dbName string, writer databaseconfig.DatabaseConfig) error {
	dbconn, err := dbhelpers.OpenMySQL(writer)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(metricName, fmt.Sprintf("database:%s", dbName), "result:error", "reason:conn")
		return oops.Wrapf(err, "failed to connect to db")
	}
	defer dbconn.Close()

	_, err = dbconn.ExecContext(ctx, createTbl)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(metricName, fmt.Sprintf("database:%s", dbName), "result:error", "reason:create_table_fail")
		return oops.Wrapf(err, "failed to execute dataplat_internal create table")
	}

	_, err = dbconn.ExecContext(ctx, insertInto)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(metricName, fmt.Sprintf("database:%s", dbName), "result:error", "reason:insert_into_fail")
		return oops.Wrapf(err, "failed to execute dataplat_internal insert into")
	}

	_, err = dbconn.ExecContext(ctx, dropTable)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(metricName, fmt.Sprintf("database:%s", dbName), "result:error", "reason:drop_table_fail")
		return oops.Wrapf(err, "failed to execute dataplat_internal drop table")
	}

	monitoring.AggregatedDatadog.Incr(metricName, fmt.Sprintf("database:%s", dbName), "result:success")
	return nil
}
