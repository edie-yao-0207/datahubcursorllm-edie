package main

import (
	"time"

	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/stats/kinesisstats/jsonexporter"
	"samsaradev.io/system"
)

func main() {
	system.NewFx(
		&config.ConfigParams{},
		fx.Provide(func(appConfig *config.AppConfig) jsonexporter.Config {
			return jsonexporter.Config{
				QueueUrl: appConfig.KinesisStatsJsonBackfillExporterSqsQueueUrl,

				// The max p99 for message process time between Sep-Nov was about
				// 8 min. To be safe, we'll keep the timeout at 15 minutes,
				// since we'd prefer the backfill worker to "just work" and don't
				// care too much about latency on retries.
				ProcessMessageTimeout:    time.Minute * 15,
				VisibilityTimeoutSeconds: int64(time.Minute.Seconds() * 16),

				// Allows ignoring some orgs when ingesting data.
				// See: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5539346/Runbook+Large+Test+Org+Causing+Problems+a.k.a+Big+Red+Button
				// IgnoredOrgs: map[int64]struct{}{},
			}
		}),
		fx.Invoke(func(*jsonexporter.Processor) {}),
	).Run()
}
