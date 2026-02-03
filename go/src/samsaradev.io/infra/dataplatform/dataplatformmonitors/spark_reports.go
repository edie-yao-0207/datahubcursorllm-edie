package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/reports/sparkreportregistry"
	"samsaradev.io/team"
)

var consecutiveSparkReportFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		for _, report := range sparkreportregistry.AllReportsInRegion(region) {
			// We don't want to generate monitors for reports that have no jobs or are disabled.
			if len(report.Jobs) == 0 || report.Disable {
				continue
			}

			team, ok := team.TeamByName[report.TeamName]
			if !ok {
				return nil, oops.Errorf("could not find team with name: %s", report.TeamName)
			}

			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:                   fmt.Sprintf("[%s] [%s] No successful report job runs for in the last 6 hours", region, report.Name),
				Message:                fmt.Sprintf("This monitor notifies when a Spark Report job has not succeeded once in the last 6 hours. \n If the report is in development and actively being worked on feel free to mute the monitor. %s %s", team.DatadogPagerdutyLowUrgencyIntegrationTag(), team.DatadogSlackLowUrgencyIntegrationTag()),
				Query:                  fmt.Sprintf("sum(last_7h):default_zero(sum:databricks.jobs.run.finish{region:%s,database:reports,table:%s,status:success} by {table}.as_count()) < 1", region, report.Name),
				RunbookLink:            "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5446507",
				NewGroupDelayInMinutes: 1440, // 1 day
				TeamToPage:             team,
				Severity:               lowSeverity,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}

			monitors = append(monitors, monitor)
		}
	}
	return monitors, nil
})
