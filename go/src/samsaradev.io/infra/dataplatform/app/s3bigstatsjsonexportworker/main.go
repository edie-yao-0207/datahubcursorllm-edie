package main

import (
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/stats/kinesisstats/bigstatsjsonexporter"
	"samsaradev.io/system"
)

func main() {
	system.NewFx(
		&config.ConfigParams{},
		fx.Provide(func(appConfig *config.AppConfig) bigstatsjsonexporter.ExporterConfig {
			return bigstatsjsonexporter.ExporterConfig{
				QueueURL: appConfig.S3BigStatsJsonExporterSqsQueueUrl,
				// Allows ignoring some orgs when ingesting data.
				// See: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5539346/Runbook+Large+Test+Org+Causing+Problems+a.k.a+Big+Red+Button
				// IgnoredOrgs: map[int64]struct{}{},
			}
		}),
		fx.Invoke(func(*bigstatsjsonexporter.Exporter) {}),
	).Run()
}
