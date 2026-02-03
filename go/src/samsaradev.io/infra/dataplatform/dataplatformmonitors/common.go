package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

func buildThresholdForJob(threshold string, region string, jobName string) string {
	return fmt.Sprintf("avg:databricks.jobs.%s{region:%s AND job_name:%s} by {job_name}", threshold, region, jobName)
}

func buildTimeSinceSuccessForJob(region string, jobName string) string {
	return fmt.Sprintf("(avg:databricks.jobs.time_since_last_success{region:%s AND job_name:%s} by {job_name} / 60 / 60 / 1000)", region, jobName)
}

var bulkSparkJobFailures = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {

	largePortionReplicationJobsFailMonitorName := "More than 25 percent of KS or RDS Stats replication jobs failed in the last 3 hours"
	largePortionReplicationJobsFail, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        largePortionReplicationJobsFailMonitorName,
		Message:     "If we see a lot of errors, it's possible we are seeing infrastructure issues, e.g. spot availability issues.",
		Query:       "sum(last_3h):sum:databricks.jobs.run.finish{NOT status:success AND dataplatform-job-type IN (rds_deltalake_ingestion_merge,kinesisstats_deltalake_ingestion_merge)} by {region,dataplatform-job-type}.as_count() / sum:databricks.jobs.run.finish{dataplatform-job-type IN (rds_deltalake_ingestion_merge,kinesisstats_deltalake_ingestion_merge)} by {region,dataplatform-job-type}.as_count() > 0.25",
		Severity:    highSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5715298/Runbook+25+of+jobs+are+failing.",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: sparkInfraDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag, dayLightHoursTag},

		NotifyNoData:      false,
		RequireFullWindow: true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	largePortionDagsterJobsFailMonitorName := "More than 25 percent of Dagster jobs failed in the last 6 hours"
	largePortionDagsterJobsFail, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        largePortionDagsterJobsFailMonitorName,
		Message:     "If we see a lot of errors, it's possible we are seeing infrastructure (Dagster) issues.",
		Query:       "sum(last_6h):sum:dagster.jobs.run{status:failure AND job_type IN (asset_materialization)} by {region}.as_count() / sum:dagster.job.run{job_type IN (asset_materialization)} by {region}.as_count() > 0.25",
		Severity:    lowSeverity,
		TeamToPage:  team.DataEngineering,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5271186/Large+Number+of+Dagster+Failures",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: "https://dagster.internal.samsara.com/overview/activity/timeline",
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dagsterTag},

		NotifyNoData:      false,
		RequireFullWindow: true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	noSuccessfulSparkJobs, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "No successful Spark jobs in last 4 hours",
		Message:     fmt.Sprintf("This monitor ensures we are getting metrics. If this has fired and our '%s' monitor hasn't, check databricksjobmetrics logs to see why metrics are not being emitted.", largePortionReplicationJobsFailMonitorName),
		Query:       "sum(last_4h):default_zero(sum:databricks.jobs.run.finish{status:success} by {region}) < 1",
		Severity:    highSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6123115/No+successful+Spark+job+in+last+4+hours",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: sparkInfraDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags:  []dataPlatformDatadogTagType{dayLightHoursTag},
		NotifyNoData:             true,
		RequireFullWindow:        true,
		NoDataTimeframeInMinutes: 480, // 4 hours
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{
		largePortionReplicationJobsFail,
		largePortionDagsterJobsFail,
		// Since we don't notify on no data, make sure we have at least 1 successful run every 4 hours
		noSuccessfulSparkJobs,
	}, nil
})

var approachingDatabricksJobLimitMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	actualHardLimit := 10000

	notifyApproachingAt := actualHardLimit - 100
	notifyVeryCloseAt := actualHardLimit - 25

	approachingJobLimit, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Approaching Databricks Job Limit",
		Message:     fmt.Sprintf("We're approaching the databricks job limit of %d UI Jobs, and once we hit this we won't be able to create new jobs. Find jobs to delete/consolidate, and prioritize sharding jobs between workspaces.", actualHardLimit),
		Query:       fmt.Sprintf("max(last_1h):sum:databricks.jobs.count{*} by {region}.rollup(max, 3600) >= %d", notifyApproachingAt),
		Severity:    lowSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4682274/Runbook+Approaching+Databricks+Job+Limit",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: sparkInfraDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	veryCloseJobLimit, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Very close to Databricks Job Limit",
		Message:     fmt.Sprintf("We're extremely close to the databricks job limit of %d UI Jobs, and once we hit this we won't be able to create new jobs. Urgently find jobs to delete/consolidate, and prioritize sharding jobs between workspaces.", actualHardLimit),
		Query:       fmt.Sprintf("max(last_1h):sum:databricks.jobs.count{*} by {region}.rollup(max, 3600) >= %d", notifyVeryCloseAt),
		Severity:    highSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4682274/Runbook+Approaching+Databricks+Job+Limit",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: sparkInfraDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dayLightHoursTag},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{
		approachingJobLimit,
		veryCloseJobLimit,
	}, nil
})

var endToEndLocationMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	sixHoursMs := 6 * 60 * 60 * 1000
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "kinesisstats.location has not had a new row in more than 6 hours",
		Message:     "kinesisstats.location has not had a new row in more than 6 hours, which could indicate a problem with the table or with general ingestion.",
		Severity:    highSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5143719/Runbook+kinesisstats.location+has+not+had+new+rows+in+6+hours",
		Query:       fmt.Sprintf("max(last_2h):max:ksdeltalake.ms_since_last_update{table:location} by {region}.rollup(max, 7200) > %d", sixHoursMs),

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag, ksMergeJobTag, dayLightHoursTag},

		NotifyNoData:             true,
		NoDataTimeframeInMinutes: 60 * 6, // 6 hours
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{
		monitor,
	}, nil
})

var billingWorkflowLowUrgency = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Workflow that processes billing data has not succeeded in 24 hours.",
		Message:     "The workflow that populates our cost data (aws + databricks) has not succeeded in 24 hours. Investigate, or the cost data may be missing or stale, which others rely on.",
		Severity:    lowSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4555099/Runbook+Job+to+create+billing.dataplatform+costs+table+has+not+succeeded+in+24+hours",
		Query:       "sum(last_25h):default_zero(sum:databricks.jobs.run.finish{job_name:backend_dataplatform_aws_databricks_costs_daily-us,status:success}.as_count()) < 1",

		DatabricksJobSearchLink: &databricksJobSearchLink{
			region:    infraconsts.SamsaraAWSDefaultRegion,
			searchKey: "backend_dataplatform_aws_databricks_costs",
		},

		NotifyNoData:             true,
		NoDataTimeframeInMinutes: 60 * 48, // 48 hours

	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{
		monitor,
	}, nil
})

var billingWorkflowHighUrgency = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Workflow that processes billing data has not succeeded in 96 hours.",
		Message:     "The workflow that populates our cost data (aws + databricks) has not succeeded in 96 hours. Investigate, or the cost data may be missing or stale, which others rely on. This is now an urgent alert, please debug asap during business hours.",
		Severity:    businessHoursSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4555099/Runbook+Job+to+create+billing.dataplatform+costs+table+has+not+succeeded+in+24+hours",
		Query:       "sum(last_96h):default_zero(sum:databricks.jobs.run.finish{job_name:backend_dataplatform_aws_databricks_costs_daily-us,status:success}.as_count()) < 1",

		DatabricksJobSearchLink: &databricksJobSearchLink{
			region:    infraconsts.SamsaraAWSDefaultRegion,
			searchKey: "backend_dataplatform_aws_databricks_costs",
		},

		NotifyNoData:             true,
		NoDataTimeframeInMinutes: 60 * 120, // 120 hours

	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{
		monitor,
	}, nil
})

var combineShardsUCJobFailures = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		jobSuffix := "us"
		if region == infraconsts.SamsaraAWSEURegion {
			jobSuffix = "eu"
		} else if region == infraconsts.SamsaraAWSCARegion {
			jobSuffix = "ca"
		}
		jobName := fmt.Sprintf("rds_combine_shards_uc_all-%s", jobSuffix)
		timeSinceSuccessHours := buildTimeSinceSuccessForJob(region, jobName)
		severity := getSeverityForRegion(region, highSeverity)
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("[%s] Combine shards job (UC) has not succeeded in last 7 hours.", region),
			Message:     "Combine shards job (UC) runs every 3 hours from 0:00 UTC to create the combined shards view for each table in the RDS data lake in UC. We expect at least 1 success over the past 2 job runs.",
			Query:       fmt.Sprintf("avg(last_30m):%s - %s <= 0", buildThresholdForJob(dataplatformconsts.HighUrgencyThreshold, region, jobName), timeSinceSuccessHours),
			Severity:    severity,
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4565072/Runbook+Combine+shards+job+has+not+succeeded",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: sparkInfraDashboard,
				variables: map[string]string{
					"region": "{{region.name}}",
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dayLightHoursTag},

			RequireFullWindow: false,
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		monitors = append(monitors, monitor)
	}

	return monitors, nil
})
