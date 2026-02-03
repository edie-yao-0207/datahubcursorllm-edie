package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
)

var unityCatalogSyncJobMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	sloTarget := 24 + 1 // 24 hours + 1 for jitter
	jobName := "unity_catalog_reverse_sync_job"
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		errorMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Unity Catalog Sync Job %s has not succeeded in %d hours", region, jobName, sloTarget),
			Message:     fmt.Sprintf("The Unity Catalog Sync Job job has not succeeded in %d hours. Check the runbook to see how to address this.", sloTarget),
			Query:       fmt.Sprintf("sum(last_%dh):default_zero(sum:databricks.jobs.run.finish{job_name:%s*,status:success,region:%s} by {job_name}) <= 0", sloTarget, jobName, region),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: "https://samsaradev.atlassian.net/wiki/x/S4Ausw",
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region:    region,
				searchKey: jobName,
			},
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{ucSyncJobTag},
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating unity catalog sync job monitors in region %s", region)
		}

		monitors = append(monitors, errorMonitor)
	}
	return monitors, nil
})
