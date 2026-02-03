package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var vacuumJobRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4774808/Runbook+No+successful+vacuum+job+in+the+last+8+days"

var vacuumJobFailures = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	var jobs = []string{"datamodel", "biztech", "other"}

	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {

		for _, job := range jobs {

			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:              fmt.Sprintf("[%s] [%s] Vacuum Job has no successful run in the last 8 days", region, job),
				Message:           "The Vacuum job has not succeeded in the last 8 days.",
				Query:             fmt.Sprintf("sum(last_8d):default_zero(sum:databricks.jobs.run.finish{job_name:vacuum-%s-dbs*,region:%s,status:success}.as_count()) < 1", job, region),
				RunbookLink:       vacuumJobRunbook,
				RequireFullWindow: false,
				NotifyNoData:      true,
				Severity:          lowSeverity,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}

			monitors = append(monitors, monitor)
		}
	}

	return monitors, nil
})
