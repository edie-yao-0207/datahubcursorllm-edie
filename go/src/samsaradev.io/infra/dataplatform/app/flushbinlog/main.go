package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()

	// We accept command line arguments to this app which lets us manually kick off ecs tasks with those arguments.
	var dbs string
	var dryRun, withDummyWrite bool

	flag.StringVar(&dbs, "dbs", "", "comma separated list of dbs to flush; we'll do all shards")
	flag.BoolVar(&dryRun, "dryrun", true, "set to false to actually run the command; otherwise we just list")
	flag.BoolVar(&withDummyWrite, "withDummyWrite", false, "set to true to also write and subsequently delete a row to the schema_version table")
	flag.Parse()

	if dbs == "" {
		slog.Infow(ctx, "No databases provided. This could be in CI or could be in error. Using internaldb only, with dryrun = true.")
		dbs = "internal"
	}

	if dryRun && withDummyWrite {
		slog.Fatalw(ctx, "cannot specify both dryrun and withDummyWrite")
	}

	var appConfig *config.AppConfig

	app := system.NewFx(&config.ConfigParams{}, fx.Populate(&appConfig))
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}

	// Before running the script, let's just check that the dbs provided are all correct.
	var invalidDbs []string
	dbSet := make(map[string]struct{}, len(getDbToConfigMap(appConfig))+len(getDbToShardedConfigMap(appConfig)))
	for dbname := range getDbToConfigMap(appConfig) {
		dbSet[dbname] = struct{}{}
	}
	for dbname := range getDbToShardedConfigMap(appConfig) {
		dbSet[dbname] = struct{}{}
	}

	dbList := strings.Split(strings.TrimSpace(dbs), ",")
	for _, elem := range dbList {
		if _, ok := dbSet[elem]; !ok {
			invalidDbs = append(invalidDbs, elem)
		}
	}

	if len(invalidDbs) > 0 {
		log.Fatalf("DB names must match those in the registry (e.g. 'workflows', not 'workflowsdb'. Heres a list of invalid dbs provided: %s\n", invalidDbs)
	}

	var failedDbs []string
	for _, db := range dbList {
		slog.Infow(ctx, fmt.Sprintf("===== Running for all shards of db %s =====", db))
		// skiplint: +loopedexpensivecall
		err := FlushBinlogForDb(appConfig, db, dryRun, withDummyWrite)
		if err != nil {
			slog.Errorw(ctx, oops.Errorf("Failed to complete %s with error: %v. Skipping...", db, err), nil)
		}
		slog.Infow(ctx, "===== Done =====")
	}

	if len(failedDbs) > 0 {
		slog.Errorw(ctx, oops.Errorf("Failed to process some dbs, see logs for errors %s\n", failedDbs), nil)
	}

	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}

}
