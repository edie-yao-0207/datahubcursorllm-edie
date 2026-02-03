package main

import (
	"context"

	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricksawsiface/databricksglue"
	"samsaradev.io/infra/dataplatform/glueversiondeleter"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()
	var deleter *glueversiondeleter.GlueVersionDeleter
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Provide(databricksglue.NewGlue),
		fx.Populate(&deleter),
	)

	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}

	if err := deleter.Run(); err != nil {
		slog.Fatalw(ctx, "failed to run application", "error", err)
	}

	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}
