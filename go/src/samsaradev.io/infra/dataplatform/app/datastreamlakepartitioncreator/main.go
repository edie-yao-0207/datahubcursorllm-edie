package main

import (
	"context"
	"flag"
	"time"

	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/system"
)

func main() {
	var startDateString, endDateString string
	flag.StringVar(&startDateString, "start", "", "inclusive start of the date range to create partitions for")
	flag.StringVar(&endDateString, "end", "", "inclusive end of the date range to create partitions for")
	flag.Parse()

	ctx := context.Background()

	oneDay := 24 * time.Hour
	startDate := time.Now().Truncate(oneDay).Add(-2 * oneDay)
	if startDateString != "" {
		d, err := time.Parse("2006-01-02", startDateString)
		if err != nil {
			slog.Fatalw(ctx, "cannot parse start date", "start", startDateString)
		}
		startDate = d
	}
	endDate := startDate.Add(7 * oneDay)
	if endDateString != "" {
		d, err := time.Parse("2006-01-02", endDateString)
		if err != nil {
			slog.Fatalw(ctx, "cannot parse end date", "end", endDateString)
		}
		endDate = d
	}
	if endDate.Before(startDate) {
		slog.Fatalw(ctx, "start date is after end date", "start", startDate, "end", endDate)
	}

	var waitForLeaderHelper leaderelection.WaitForLeaderLifecycleHookHelper
	var partitionCreator *PartitionCreator

	partitionCreatorApp := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(&partitionCreator, &waitForLeaderHelper),
	)

	if err := partitionCreatorApp.Start(ctx); err != nil {
		slog.Fatalw(ctx, "Failed to start DataStreamLakePartitionCreator cron", "error", err)
	}

	if err := waitForLeaderHelper.BlockUntilLeader(ctx); err != nil {
		slog.Fatalw(ctx, "WaitForLeaderLifecycleHookHelper failed while BlockUntilLeader", "error", err)
	}

	defer func() {
		if err := partitionCreatorApp.Stop(ctx); err != nil {
			slog.Fatalw(ctx, "Failed to stop DataStreamLakePartitionCreator cron", "error", err)
		}
	}()

	date := startDate
	for !date.After(endDate) {
		if err := partitionCreator.CreatePartitions(ctx, "datastreams_schema", date.Format("2006-01-02")); err != nil {
			slog.Fatalw(ctx, "DataStreamLakePartitionCreator failed to create partitions", "error", err)
		}
		date = date.Add(24 * time.Hour)
	}
}
