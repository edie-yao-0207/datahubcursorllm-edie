package dataplatformmonitors

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/libs/ni/infraconsts"
)

func buildThreshold(threshold string, region string, types []string) string {
	typesString := strings.Join(types, ",")
	return fmt.Sprintf("avg:databricks.jobs.%s{region:%s AND dataplatform-job-type IN (%s)} by {job_id,database,table}", threshold, region, typesString)
}

func buildTimeSinceSuccess(region string, types []string) string {
	typesString := strings.Join(types, ",")
	return fmt.Sprintf("(avg:databricks.jobs.time_since_last_success{region:%s AND dataplatform-job-type IN (%s)} by {job_id,database,table} / 60 / 60 / 1000)", region, typesString)
}

var mergeJobs = []string{
	string(dataplatformconsts.KinesisStatsDeltaLakeIngestionMerge),
	string(dataplatformconsts.RdsDeltaLakeIngestionMerge),
	string(dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionMerge),
	string(dataplatformconsts.DynamoDbDeltaLakeIngestionMerge),
	string(dataplatformconsts.EmrDeltaLakeIngestionMerge),
	string(dataplatformconsts.OrgShardsTableV2), // This is not a merge job, but we want to monitor it for SLOs.
}

var vacuumJobs = []string{
	string(dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionVacuum),
	string(dataplatformconsts.RdsDeltaLakeIngestionVacuum),
	string(dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionVacuum),
	string(dataplatformconsts.DataPipelinesVacuum),
}

// Creates consolidated merge monitors based on time since last success for merge jobs and the thresholds that are
// specified for them.
var consolidatedMergeMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {

	var monitors []tf.Resource

	// All the monitors look very similar, just with slightly different queries.
	// Notably, we don't alert on no data; that would mean that the databricksjobmetricsworker
	// is not functioning and we page on that separately, rather than here.
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		timeSinceSuccessHours := buildTimeSinceSuccess(region, mergeJobs)
		lowUrgencyMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Low Urgency Merge Job Monitor", region),
			Message:     "A merge job has reached its low urgency threshold for time without success. Please follow the runbook to investigate.",
			Query:       fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThreshold(dataplatformconsts.LowUrgencyThreshold, region, mergeJobs), timeSinceSuccessHours),
			Severity:    lowSeverity,
			RunbookLink: mergeJobRunbook,

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: mergeJobRunsDashboad,
				variables: map[string]string{
					"region": region,
					// this may be kinesisstats or an RDS db, so just use `*` prefix which works for RDS dbs which are `prod-<dbname>`
					dataplatformconsts.DATABASE_TAG: "*{{database.name}}",
					dataplatformconsts.TABLE_TAG:    "{{table.name}}",
				},
			},
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region: region,
				jobId:  "{{job_id.name}}",
			},
			NotifyNoData: false,

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		businessHoursMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Business Hours Merge Job Monitor", region),
			Message:     "A merge job has reached its business hours threshold for time without success. Please follow the runbook to investigate, and treat as high urgency during business hours.",
			Query:       fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThreshold(dataplatformconsts.BusinessHoursThreshold, region, mergeJobs), timeSinceSuccessHours),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: mergeJobRunbook,

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: mergeJobRunsDashboad,
				variables: map[string]string{
					"region": region,
					// this may be kinesisstats or an RDS db, so just use `*` prefix which works for RDS dbs which are `prod-<dbname>`
					dataplatformconsts.DATABASE_TAG: "*{{database.name}}",
					dataplatformconsts.TABLE_TAG:    "{{table.name}}",
				},
			},
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region: region,
				jobId:  "{{job_id.name}}",
			},
			NotifyNoData: false,

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		highUrgencyMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] High Urgency Merge Job Monitor", region),
			Message:     "A merge job has reached its high urgency threshold for time without success. Please treat as urgent at all hours, and follow the runbook to investigate.",
			Query:       fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThreshold(dataplatformconsts.HighUrgencyThreshold, region, mergeJobs), timeSinceSuccessHours),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: mergeJobRunbook,

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: mergeJobRunsDashboad,
				variables: map[string]string{
					"region": region,
					// this may be kinesisstats or an RDS db, so just use `*` prefix which works for RDS dbs which are `prod-<dbname>`
					dataplatformconsts.DATABASE_TAG: "*{{database.name}}",
					dataplatformconsts.TABLE_TAG:    "{{table.name}}",
				},
			},
			DatabricksJobSearchLink: &databricksJobSearchLink{
				region: region,
				jobId:  "{{job_id.name}}",
			},
			NotifyNoData: false,

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, lowUrgencyMonitor, businessHoursMonitor, highUrgencyMonitor)
	}

	return monitors, nil
})

var consolidatedVacuumMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		timeSinceSuccessHours := buildTimeSinceSuccess(region, vacuumJobs)

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Vacuum Job Monitor", region),
			Message:     "A vacuum job has reached its low urgency threshold for time without success. Please follow the runbook to investigate.",
			Query:       fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThreshold(dataplatformconsts.LowUrgencyThreshold, region, vacuumJobs), timeSinceSuccessHours),
			Severity:    lowSeverity,
			RunbookLink: "https://samsaradev.atlassian.net/wiki/x/pIHJmQ",

			DatabricksJobSearchLink: &databricksJobSearchLink{
				region: region,
				jobId:  "{{job_id.name}}",
			},
			NotifyNoData: false,

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{vacuumTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

// This monitor forms the basis of our merge job SLOs.
var mergeJobSloMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		timeSinceSuccessHours := buildTimeSinceSuccess(region, mergeJobs)
		sloMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("[%s] Merge Job SLO Monitor", region),
			Message:                 "This monitor is not intended to be alerted on, but used only for informational purposes to power our SLOs.",
			Query:                   fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThreshold(dataplatformconsts.SloTarget, region, mergeJobs), timeSinceSuccessHours),
			Severity:                noAlert,
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		monitors = append(monitors, sloMonitor)
	}

	return monitors, nil
})
