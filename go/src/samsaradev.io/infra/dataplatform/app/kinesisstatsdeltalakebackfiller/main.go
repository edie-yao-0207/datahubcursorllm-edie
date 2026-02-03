package main

import (
	"context"
	"fmt"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/app/kinesisstatsdeltalakebackfiller/internal/features"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ksdeltalake/backfiller"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()
	var ksb *backfiller.KSBackfiller
	var launchDarklyBridge *launchdarkly.SDKBridge
	app := system.NewFx(&config.ConfigParams{}, fx.Provide(datadoghttp.NewDatadogHTTPClient), fx.Populate(&ksb), fx.Populate(&launchDarklyBridge))
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}
	defer app.Stop(ctx)
	allObjectStats := ksdeltalake.AllTables()
	for _, table := range allObjectStats {
		if features.KinesisstatsDeltaLakeBackfillWithBridge(&features.BackendUser{Objectstat: table.Name}, launchDarklyBridge) {
			if ksb.TriggeredBackfillCount >= 2 {
				slog.Infow(ctx, "Already backfilled two object stats, no more backfills will be completed this run to avoid overloading kinesisstatsjsonexportworker")
				return
			}
			slog.Infow(ctx, "now backfilling", "table", table.Name)
			if err := ksb.Run(table.Name, fmt.Sprintf("%s.db", table.Name)); err != nil {
				slog.Errorw(ctx, oops.Wrapf(err, "%s", fmt.Sprintf("failed to run backfill for table: %s", table.Name)), nil)
			}
		}
	}

	// Do the same thing for partial backfills
	// We want to prioritize getting full backfills in, especially so new stats get propogated quickly into our data lake
	// We have an alloted 2 backfills per 3 hour run, so if any of those do not get used up above, we use them for partial backfills
	ksb.PartialBackfill = true
	for _, table := range allObjectStats {
		if ksb.TriggeredBackfillCount >= 2 {
			slog.Infow(ctx, "Already backfilled two object stats, no more backfills will be completed this run to avoid overloading kinesisstatsjsonexportworker")
			return
		}
		slog.Infow(ctx, "now backfilling", "table", table.Name)
		if err := ksb.Run(table.Name, fmt.Sprintf("%s.db", table.Name)); err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "%s", fmt.Sprintf("failed to run backfill for table: %s", table.Name)), nil)
		}
	}
}
