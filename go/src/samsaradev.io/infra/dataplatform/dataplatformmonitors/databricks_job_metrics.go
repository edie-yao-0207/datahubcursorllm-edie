package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var databricksJobMetricsMonitorHighSeverity = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {

	// Create a monitor that checks whether there are any metrics in the last 30 minutes.
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] The Databricks jobs metric worker has not reported data in last 30 minutes", region),
			Message:                  "There are no databricks job metrics for the last 30 minutes, meaning we have no visibility into databricks run status. Please see the runbook.",
			Query:                    fmt.Sprintf("sum(last_30m):default_zero(sum:databricks.jobs.run.finish{region:%s}.as_count()) < 1", region),
			Severity:                 getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink:              "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5742325/Databricks+Job+Metrics",
			RequireFullWindow:        true,
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 30,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "failed to create databricksjobmetric")
		}
		monitors = append(monitors, monitor)
	}

	// Additionally, create a monitor that checks whether there are any successful polls in the last hour.
	// It's possible that the service logs some metrics but that the overall state was failure.
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] The Databricks jobs metric worker has not succeeded in the last 60 minutes", region),
			Message:                  "There have been no successful polls in the last hour. This may not mean metrics loss but should be addressed. Please see the runbook.",
			Query:                    fmt.Sprintf("sum(last_60m):default_zero(sum:databricksjobmetrics.run{success:true,region:%s} by {region}.as_count().rollup(sum)) <= 0", region),
			Severity:                 getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink:              "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5742325/Databricks+Job+Metrics",
			RequireFullWindow:        true,
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 60,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "failed to create databricksjobmetric")
		}
		monitors = append(monitors, monitor)
	}

	return monitors, nil
})
