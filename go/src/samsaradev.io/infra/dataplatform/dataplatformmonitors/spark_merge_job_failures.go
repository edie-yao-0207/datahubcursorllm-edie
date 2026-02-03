package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
)

const MergeDlqRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5249405/Runbook+Merge+Queue+Dead+Letter+Queues"
const MergeDlqMessage = "Items on merge dead letter queues need to be redriven to be processed correctly. This can be addressed during business hours. See the runbook for more details: " + MergeDlqRunbook

var mergeJobRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5875715/Runbook+No+successful+merge+job+in+the+last+6+hours"

var sparkMergeJobOverruns = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Production Merge Job has frequently overrun its schedule.",
		Message:     "Production merge jobs start every 3 hours from 0:00 UTC, and should finish before the next hour starts, to finish before datapipelines.",
		Query:       "sum(last_1d):sum:databricks.jobs.run.merge_job_overrun{*} by {region,database,table} > 1",
		Severity:    lowSeverity,
		RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4381291/Runbook+Merge+Job+Overrun",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: mergeJobRunsDashboad,
			variables: map[string]string{
				"region": "{{region.name}}",
				// this may be kinesisstats or an RDS db, so just use `*` prefix which works for RDS dbs which are `prod-<dbname>`
				dataplatformconsts.DATABASE_TAG: "*{{database.name}}",
				dataplatformconsts.TABLE_TAG:    "{{table.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{
		monitor,
	}, nil
})

// For all merge queues, we combine the monitors below. We create 3 monitors:
//  1. Production Jobs
//  2. Nonproduction Jobs
//  3. New Jobs (those without the production:*) tag.
//     It takes datadog many days after an SQS queue is created to propagate the custom
//     tags we have, like `deadletterqueue`, `production`, etc. In that interim,
//     we have this third monitor to catch any of those jobs and at least low urgency
//     let us know that there are dlq messages. In practice we have seen that the tags
//     should propagate in max 1 week.
var consolidatedMergeQueueDlq = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	prodDlqMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Production Merge Dead Letter Queue has messages on it",
		Message:     MergeDlqMessage,
		Query:       "max(last_15m):default_zero(max:aws.sqs.approximate_number_of_messages_visible{deadletterqueue:true AND queuename:samsara_delta_lake_* AND production:true AND pipeline IN (rdsdeltalake,ksdeltalake,s3bigstats)} by {region,queuename}) > 0",
		Severity:    businessHoursSeverity,
		RunbookLink: MergeDlqRunbook,

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: dataplatIngestionDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	nonProdDlqMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "Nonproduction Merge Dead Letter Queue has messages on it",
		Message:     MergeDlqMessage,
		Query:       "max(last_60m):default_zero(max:aws.sqs.approximate_number_of_messages_visible{deadletterqueue:true AND queuename:samsara_delta_lake_* AND production:false AND pipeline IN (rdsdeltalake,ksdeltalake,s3bigstats)} by {region,queuename}) > 0",
		Severity:    businessHoursSeverity,
		RunbookLink: MergeDlqRunbook,

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: dataplatIngestionDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	newTableDlqMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:        "New Merge Dead Letter Queue has messages on it",
		Message:     fmt.Sprintf("New merge queues won't propagate our custom tags for a few days, so this monitor exists to catch any dlq messages on them. We can take the same actions as normal. %s", MergeDlqMessage),
		Query:       "min(last_60m):default_zero(max:aws.sqs.approximate_number_of_messages_visible{(queuename:samsara_delta_lake_merge* AND (queuename:*dead_letter_queue OR queuename:*dlq)) AND NOT production:*} by {region,queuename}) > 0",
		Severity:    businessHoursSeverity,
		RunbookLink: MergeDlqRunbook,

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: dataplatIngestionDashboard,
			variables: map[string]string{
				"region": "{{region.name}}",
			},
		},

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{mergeJobTag},
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{
		prodDlqMonitor, nonProdDlqMonitor, newTableDlqMonitor,
	}, nil

})
