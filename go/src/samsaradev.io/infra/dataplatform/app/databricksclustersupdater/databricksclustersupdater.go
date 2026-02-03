package main

import (
	"context"

	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/customdatabricksimages/updateclusters"
	"samsaradev.io/infra/dataplatform/databricksawsiface/databricksecr"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()
	var cu *updateclusters.ClusterUpdater
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Provide(databricksecr.NewECR),
		fx.Populate(&cu),
	)
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}

	if err := cu.Run(); err != nil {
		slog.Fatalw(ctx, "failed to run application", "error", err)
	}

	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}
