package main

import (
	"context"
	"log"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/errgroup"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/amundsen/metadatagenerator"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()
	var dmg *metadatagenerator.DataHubMetadataGenerator
	dataHubMetadataGeneratorApp := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(&dmg),
	)

	if err := dataHubMetadataGeneratorApp.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start dataHubMetadataGenerator cron", "error", err)
	}

	defer func(ctx context.Context) {
		if err := dataHubMetadataGeneratorApp.Stop(ctx); err != nil {
			slog.Fatalw(ctx, "Failed to stop dataHubMetadataGenerator cron", "error", err)
		}
	}(ctx)

	errGroup, ctx := errgroup.WithContext(ctx)
	for _, generator := range dmg.Generators {
		errGroup.Go(func() error {
			log.Printf("Running metadata generation for: %s \n", generator.Name())
			return generator.GenerateMetadata(dmg.Metadata)
		})
	}
	if err := errGroup.Wait(); err != nil {
		slog.Fatalw(ctx, "error generating metadata", "error", oops.Wrapf(err, "metadata generation failed"))
	}
}
