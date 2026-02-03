package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/datadogresource"
	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datastreamlake"
	"samsaradev.io/libs/ni/infraconsts"
)

var datastreamRecordFormatConversionFailureMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		for _, stream := range datastreamlake.Registry {
			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:        fmt.Sprintf("Firehose for %s Data Stream in %s failing to convert records to S3", stream.StreamName, region),
				Message:     fmt.Sprintf("The Firehose for %s Data Stream in %s is failing to convert records to S3. The impact of this data delay. If it persists for 24hrs there will be data loss. Please go into Databricks and look at datastreams_errors.%s table to diagnose the errors.", stream.StreamName, region, stream.StreamName),
				Query:       fmt.Sprintf("max(last_5m):sum:aws.firehose.failed_conversion_records{deliverystreamname:%s,region:%s} >= 1", stream.StreamName, region),
				Severity:    lowSeverity,
				RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6070450/Data+streams+Firehose+failing+to+convert+records+to+S3",

				DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
					dashboardLink: datastreamsDashboard,
					variables: map[string]string{
						"region":         region,
						"stream":         stream.StreamName,
						"deliverystream": stream.StreamName,
					},
				},

				DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataStreamsTag},
				EvaluationDelay:         datadogresource.AWSResourceEvaluationDelay,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor for %s in region %s", stream.StreamName, region)
			}

			monitors = append(monitors, monitor)
		}
	}

	return monitors, nil
})

var datastreamLiveIngestionMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	jobNamePrefix := "backend_dataplatform_data_streams_live_ingestion_data_streams_live_ingestion"
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		warningMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Data Streams Live Ingestion Job Taking more than 30 minutes", region),
			Message:     "The live ingestion job is taking longer than our 30 minute SLA. Check the runbook to see how to address this.",
			Query:       fmt.Sprintf("avg(last_1h):avg:databricks.jobs.run.duration{job_name:%s-*,!status:skipped,region:%s} by {job_name} > 1827600", jobNamePrefix, region),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6065614/Runbook+Data+Streams+Live+Ingestion+Job+Delayed",
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region:    region,
				searchKey: jobNamePrefix,
			},
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataStreamsTag},
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating live ingestion monitors in region %s", region)
		}

		monitors = append(monitors, warningMonitor)
	}
	return monitors, nil
})

var datastreamsVacuumOptimizeMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	jobnames := map[string]string{
		infraconsts.SamsaraAWSDefaultRegion: "backend_dataplatform_datastreams_vacuum_and_optimize_vacuum_and_optimize-us",
		infraconsts.SamsaraAWSEURegion:      "backend_dataplatform_datastreams_vacuum_and_optimize_vacuum_and_optimize-eu",
		infraconsts.SamsaraAWSCARegion:      "backend_dataplatform_datastreams_vacuum_and_optimize_vacuum_and_optimize-ca",
	}
	for region, job := range jobnames {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Datastreams vacuum/optimize job has not succeeded in last 2 days.", region),
			Message:     "The vacuum/optimize job has not succeeded in the last 2 days.",
			Query:       fmt.Sprintf("sum(last_2d):default_zero(sum:databricks.jobs.run.finish{job_name:%s,status:success} by {job_name}) <= 0", job),
			Severity:    lowSeverity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4828993/Runbook+Datastreams+Vacuum+Optimize+Job+is+failing",
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region:    region,
				searchKey: job,
			},
			DataPlatformDatadogTags:  []dataPlatformDatadogTagType{dataStreamsTag},
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 3 * 24 * 60 * 60, // 3 days
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating optimize / vacuum monitors in region %s", region)
		}
		monitors = append(monitors, monitor)
	}
	return monitors, nil
})
