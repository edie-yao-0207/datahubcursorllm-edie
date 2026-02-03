package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var ksDiffToolMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	// Enable this for EU this once we fix the difftool to work there https://samsara.atlassian-us-gov-mod.net/browse/DATAPLAT-45
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion} {
		// We expect this to run once every day, and the jobs could take +/- 4 hours, so we set the range to be 28 hours.
		hours := 24 + 4

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] KS Diff Tool has not run successfully in the last day.", region),
			Message:                  "The kinesisstats difftool hasn't run successfully in the last day. Go to cloudwatch logs in the main aws account, (i.e. using the dataplatformadmin role), look under 'kinesisstatsdeltalakedifftoolcron', and diagnose what the failure was.",
			Query:                    fmt.Sprintf("sum(last_%dh):sum:ksdifftool.executions{region:%s,status:success,app:kinesisstatsdeltalakedifftoolcron}.as_count() <= 0", hours, region),
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: hours * 60,
			RunbookLink:              "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6083418",
			Severity:                 lowSeverity,
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor")
		}

		monitors = append(monitors, monitor)

		sloTarget := 7
		errorMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("[%s] KS diff tool job run has not succeeded in %d days", region, sloTarget),
			Message:                 fmt.Sprintf("KS diff tool job has not succeeded in %d days. Check the runbook to see how to address this.", sloTarget),
			Query:                   fmt.Sprintf("max(last_%dh):default_zero(avg:databricks.ksdifftool.executions.success{region:%s,production:true} by {production}) <= 0", sloTarget*24, region),
			Severity:                lowSeverity,
			RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6083418/Runbook+KS+Diff+Tool+TODO",
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{ksDiffToolTag},
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating ks diff tool job monitors in region %s", region)
		}

		monitors = append(monitors, errorMonitor)
	}

	return monitors, nil
})
