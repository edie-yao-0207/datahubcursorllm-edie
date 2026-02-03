package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/juju/ratelimit"

	"go.uber.org/fx"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/stats/kinesisstats/kinesiswrite"
	"samsaradev.io/system"
)

type InternalTestObjectStatUploader struct {
	KSWriter             kinesiswrite.Writer
	Clock                clock.Clock
	WriteRateLimitBucket *ratelimit.Bucket
}

func main() {
	ctx := context.Background()

	var num int
	var dryRun bool
	flag.IntVar(&num, "num", 0, "number of stats to upload")
	flag.BoolVar(&dryRun, "dryrun", true, "set to false to actually run the command; otherwise we just list the stats we'd create")
	flag.Parse()

	var uploader InternalTestObjectStatUploader
	app := system.NewFx(
		&config.ConfigParams{},
		fx.Populate(
			&uploader.KSWriter,
			&uploader.Clock,
		),
	)
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}

	const bytesPerSecondLimit = 100 * 1024 // 100KiB
	uploader.WriteRateLimitBucket = ratelimit.NewBucketWithRate(bytesPerSecondLimit, bytesPerSecondLimit)
	currentTimeMs := samtime.TimeToMs(uploader.Clock.Now())
	objectstats := generateObjectStats(ctx, currentTimeMs, num)

	if dryRun {
		slog.Infow(ctx, "printing objectstats", "objectstats", objectstats)
	} else {
		slog.Infow(ctx, "uploading internal test objectstats", "objectstats", objectstats)
		for _, stat := range objectstats {
			uploader.WriteRateLimitBucket.Wait(int64(stat.Size()))
			_, err := uploader.KSWriter.Write(ctx, stat)
			if err != nil {
				slog.Fatalw(ctx, "failed to upload stat", "error", err)
			}
		}
		slog.Infow(ctx, fmt.Sprintf("successfully uploaded %d internal test objectstats", num))
	}

	if err := app.Stop(ctx); err != nil {
		slog.Fatalw(ctx, "failed to stop application", "error", err)
	}
}
