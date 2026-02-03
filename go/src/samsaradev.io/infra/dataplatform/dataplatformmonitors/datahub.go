package dataplatformmonitors

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/team"
)

var datahubSiteReliabilityMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:    "DataHub 5XX error rate exceeds 0.1% in the last 15 minutes",
		Message: "We are observing an elevated level of 5XX errors in DataHub.",
		Query:   "sum(last_15m):sum:aws.applicationelb.httpcode_elb_5xx{aws_account:492164655156, name:k8s-datahubp-datahubd-*}.as_count() / sum:aws.applicationelb.request_count{aws_account:492164655156, name:k8s-datahubp-datahubd-*}.as_count() > 0.001",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17731167",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubFrontendLoadBalancerLatencyMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "avg(last_6h):max:aws.applicationelb.target_response_time.p99{aws_account:492164655156,name:k8s-datahubp-datahubd-7fa327af5c} > 5",
		Name:    "DataHub avg of P99 frontend load balancer latency exceeds 5s in the last 6 hours",
		Message: "We are observing an elevated level of latency in the DataHub frontend load balancer.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17739415",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubIngestedBytesMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_1h):sum:aws.networkelb.processed_bytes{aws_account:492164655156}.as_count() < 8000000",
		Name:    "DataHub ingested bytes is less than 8MB in the last hour",
		Message: "We are observing an unusually low amount of data being ingested into DataHub.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17753049",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubExpiringTokensMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "max(last_5m):max:datahub.gms_token.ttl_hours{*} < 96",
		Name:    "DataHub GMS token expires in less than 4 days",
		Message: "The DataHub GMS token will expire within 4 days. Please update the token to prvent breakage in metadata ingestion.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4727084",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubGMSEventsMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_1h):sum:datahub.gms.sdk_emit_event{status:success}.as_count() + sum:datahub.gms.graphql_query{status:success}.as_count() < 1",
		Name:    "No DataHub GMS events have been emitted in the last hour",
		Message: "We are observing that no GMS events are being emitted in the last hour.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17780992",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubGMSEventFailureRateMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_1h):(sum:datahub.gms.graphql_query{status:failure}.as_count() + sum:datahub.gms.sdk_emit_event{status:failure}.as_count()) / (sum:datahub.gms.graphql_query{*}.as_count() + sum:datahub.gms.sdk_emit_event{*}.as_count()) > 0.01",
		Name:    "DataHub GMS event failure rate exceeds 1% in the last hour",
		Message: "We are observing an elevated level of GMS event failures in DataHub.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17753059",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubDagsterNoSuccessfulHourlyRunsMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_120m):default_zero(sum:dagster.job.run{status:success, name:extract_metadata_to_datahub_hourly_*} by {name}.as_count()) + default_zero(sum:dagster.job.run{status:success, name:extract_dq_checks_to_datahub} by {name}.as_count()) < 1",
		Name:    "No successful Dagster runs for DataHub hourly metadata extraction in the last 120 minutes",
		Message: "We are observing that no successful Dagster runs have occurred for DataHub hourly metadata extraction in the last 120 minutes.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17939756",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
		NotifyNoData:            true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubDagsterNoSuccessfulIntraDayRunsMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_8h):default_zero(sum:dagster.job.run{status:success,name:*_datahub,!name:*hourly*,!name:*dq_checks*,!name:extract_query_stats*} by {name}.as_count()) < 1",
		Name:    "No successful Dagster runs for DataHub intra-day (6h) metadata extraction in the last 8 hours",
		Message: "We are observing that no successful Dagster runs have occurred for DataHub intra-day metadata extraction in the last 8 hours.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17939756",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
		NotifyNoData:            true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubDagsterNoSuccessfulDailyRunsMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "sum(last_28h):default_zero(sum:dagster.job.run{status:success,name:extract_query_stats*} by {name}.as_count()) < 1",
		Name:    "No successful Dagster runs for DataHub daily metadata extraction in the last 28 hours",
		Message: "We are observing that no successful Dagster runs have occurred for DataHub intra-day metadata extraction in the last 12 hours.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17939756",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
		NotifyNoData:            true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var datahubLongRunningJobMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "avg(last_4h):max:dagster.datahub.job.run.duration{op:extract_hourly_op} > 7200",
		Name:    "Average hourly DataHub Dagster job has been running for more than 120 minutes",
		Message: "We are observing that the average hourly DataHub Dagster job has been running for more than 120 minutes.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/17780997",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})

var dataPlatformLoadBalancerMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "max(last_5m):sum:aws.elb.healthy_host_count{aws_account:492164655156} + sum:aws.networkelb.healthy_host_count{aws_account:492164655156} > 3",
		Name:    "More than 3 load balancers created in Databricks AWS account",
		Message: "This means a potentially new load balancer was added. Please check if this was intentional.",

		DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
			dashboardLink: "https://app.datadoghq.com/dashboard/muh-n24-9mv/data-hub?fromUser=false&refresh_mode=sliding&view=spans&from_ts=1718056153993&to_ts=1718142553993&live=true",
		},
		DataPlatformDatadogTags: []dataPlatformDatadogTagType{datahubTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5259375/Extra+Load+Balancer+Created",
		TeamToPage:              team.DataTools,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor}, nil
})
