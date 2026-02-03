package main

import (
	"context"
	"fmt"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/app/s3bigstatsbackfiller/internal/features"
	"samsaradev.io/infra/dataplatform/ksdeltalake"
	"samsaradev.io/infra/dataplatform/ksdeltalake/s3bigstatbackfiller"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/infra/releasemanagement/launchdarkly"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/system"
)

func main() {
	ctx := context.Background()
	var backfiller *s3bigstatbackfiller.S3BigStatBackfiller
	var helper leaderelection.WaitForLeaderLifecycleHookHelper
	var launchDarklyBridge *launchdarkly.SDKBridge
	app := system.NewFx(&config.ConfigParams{}, fx.Provide(datadoghttp.NewDatadogHTTPClient), fx.Populate(&helper), fx.Populate(&backfiller), fx.Populate(&launchDarklyBridge))
	if err := app.Start(ctx); err != nil {
		slog.Fatalw(ctx, "failed to start application", "error", err)
	}
	helper.BlockUntilLeader(ctx)
	defer app.Stop(ctx)

	awsSession := awssessions.NewInstrumentedAWSSession()
	region := *awsSession.Config.Region

	allS3BigStats := ksdeltalake.AllS3BigStatTables()
	finishedBackfills := make(map[string]bool, len(allS3BigStats))

	// Check if there is a backfill in progress
	backfillInProgress := false
	for _, table := range allS3BigStats {
		if features.S3bigstatsBackfillEnabledWithBridge(&features.BackendUser{S3bigstat: table.Name}, launchDarklyBridge) {
			backfillStatus, err := backfiller.GetBackfillStatus(ctx, table.Name, region)
			if err != nil {
				slog.Errorw(ctx, oops.Wrapf(err, "%s", fmt.Sprintf("Failed to run backfill for S3BigStat: %s", table.Name)), nil)
				return
			}
			if backfillStatus == s3bigstatbackfiller.InProgressStatus {
				backfillInProgress = true
				break
			}
			backfillFinished := backfillStatus == s3bigstatbackfiller.FinishedStatus
			finishedBackfills[table.Name] = backfillFinished
		}
	}

	// If there are no backfills in progress
	if !backfillInProgress {
		for _, table := range allS3BigStats {
			if features.S3bigstatsBackfillEnabledWithBridge(&features.BackendUser{S3bigstat: table.Name}, launchDarklyBridge) && !finishedBackfills[table.Name] {
				slog.Infow(ctx, "s3Bigstats Backfill started", "table", table.Name)
				err := backfiller.Run(ctx, table.Name, fmt.Sprintf("%s.db", table.Name), region)
				if err != nil {
					slog.Errorw(ctx, oops.Wrapf(err, "%s", fmt.Sprintf("s3Bigstats Backfill encountered an error: %s", table.Name)), nil)
					dynamoError := backfiller.SetBackfillStatusToError(ctx, table.Name, region)
					if dynamoError != nil {
						slog.Errorw(ctx, oops.Wrapf(dynamoError, "%s", fmt.Sprintf("Failed to update dynamo status to started for S3BigStat: %s", table.Name)), nil)
						return
					}
				}
				slog.Infow(ctx, "s3Bigstats Backfill finished", "table", table.Name)
				return
			}
		}
	}

}
