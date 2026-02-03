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
				QueueUrl: appConfig.KinesisStatsJsonExporterSqsQueueUrl,

				// The max p99 over Sep-Nov was 5 min, so we'll set
				// the process timeout at 7min. We would like messages to
				// get retried quickly so that they don't age too long
				// if there's a problem.
				ProcessMessageTimeout:    time.Minute * 7,
				VisibilityTimeoutSeconds: int64(time.Minute.Seconds() * 8),

				// Allows ignoring some orgs when ingesting data.
				// See: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5539346/Runbook+Large+Test+Org+Causing+Problems+a.k.a+Big+Red+Button
				// IgnoredOrgs: map[int64]struct{}{},
			}
		}),
		fx.Invoke(func(*jsonexporter.Processor) {}),
	).Run()
}
