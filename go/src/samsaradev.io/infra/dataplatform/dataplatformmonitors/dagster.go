package dataplatformmonitors

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"
	"golang.org/x/exp/slices"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const dagsterJobSearchLink = "https://dagster.internal.samsara.com/runs"
const dagsterAssetsLink = "https://dagster.internal.samsara.com/assets"

var teamInfo = []struct {
	team               components.TeamInfo
	monitorTypesToEmit []string
}{
	{team.DataPlatform, []string{"table_freshness"}},
	{team.DataTools, []string{"asset_materialization", "table_freshness"}},
	{team.DataEngineering, []string{"asset_materialization", "table_freshness", "partition_freshness"}},
	{team.FirmwareVdp, []string{"asset_materialization"}},
}

var dagsterNoDataMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                     fmt.Sprintf("[%s] No Dagster metrics recorded for asset materialization runs", region),
			Message:                  fmt.Sprintf("[%s] No Dagster metrics have been recorded for asset materialization runs in the past 24hrs. This means there are issues with Dagster emitting metrics to Datadog. Please consult the Data Engineering Oncall Guide below for debugging techniques and/or notify the Data Engineering (#ask-data-engineering) team if persists or need help on debugging. %s %s", region, team.DataEngineering.DatadogSlackLowUrgencyIntegrationTag(), team.DataEngineering.DatadogPagerdutyLowUrgencyIntegrationTag()),
			DagsterDashboardLink:     fmt.Sprintf("%s", dagsterJobSearchLink),
			Query:                    fmt.Sprintf("sum(last_6h):default_zero(sum:dagster.job.run{region:%s AND job_type:asset_materialization AND run_type IN (schedule,automaterialization,backfill)}.as_count()) < 1", region),
			RequireFullWindow:        true,
			NotifyNoData:             true,
			NoDataTimeframeInMinutes: 720, // 12hrs.
			DataPlatformDatadogTags:  []dataPlatformDatadogTagType{dagsterTag},
			RunbookLink:              "https://https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5841416/Data+Engineering+-+Oncall+Guide",
			TeamToPage:               team.DataEngineering,
			Severity:                 lowSeverity,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor")
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

var dagsterAssetMaterializationFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	for _, info := range teamInfo {
		if !slices.Contains(info.monitorTypesToEmit, "asset_materialization") {
			continue
		}

		teamName := strings.ToLower(info.team.TeamName)

		for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:                    fmt.Sprintf("[%s] [%s] [{{database.name}}__{{table.name}}] Failing to materialize partition {{partition.name}}", region, info.team.TeamName),
				Message:                 fmt.Sprintf("[%s] [{{name.name}}] Failing to materialize partition {{partition.name}}. Please consult the Data Engineering Oncall Guide below for debugging techniques and/or notify the Data Engineering (#ask-data-engineering) team if persists or need help on debugging. \n\n Dagster Asset Details Link : %s/%s/{{database.name}}/{{table.name}}", region, dagsterAssetsLink, region),
				Query:                   fmt.Sprintf("sum(last_2d):ewma_3(sum:dagster.job.run{region:%s AND (job_type:asset_materialization OR job_type:dq) AND status:success AND (owner:%s)} by {database,table,partition}.as_count() / sum:dagster.job.run{region:%s AND (job_type:asset_materialization OR job_type:dq) AND status:* AND (owner:%s)} by {database,table,partition}.as_count()) < 0.5", region, teamName, region, teamName),
				DagsterDashboardLink:    fmt.Sprintf("%s", dagsterJobSearchLink),
				RequireFullWindow:       false,
				DataPlatformDatadogTags: []dataPlatformDatadogTagType{dagsterTag},
				RunbookLink:             "https://https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5841416/Data+Engineering+-+Oncall+Guide",
				TeamToPage:              info.team,
				NotifyBy:                []string{"database", "table"},
				Severity:                lowSeverity,
			})
			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}
			monitors = append(monitors, monitor)
		}
	}
	return monitors, nil
})

var dagsterDataQualityFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("[%s] Data quality checks failing for asset [{{name.name}}]", region),
			Message:                 fmt.Sprintf("[%s] One or more data quality checks have failed for asset [{{name.name}}]. %s %s  \n\n Dagster Dashboard Link : https://dagster.internal.samsara.com/runs", region, team.DataEngineering.DatadogSlackLowUrgencyIntegrationTag(), team.DataEngineering.DatadogPagerdutyLowUrgencyIntegrationTag()),
			Query:                   fmt.Sprintf("sum(last_72h):sum:dagster.job.run{region:%s,job_type:dq,status:success} by {name}.as_count() < 1", region),
			DagsterDashboardLink:    fmt.Sprintf("%s", dagsterJobSearchLink),
			RunbookLink:             "https://https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5841416/Data+Engineering+-+Oncall+Guide",
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dagsterTag},
			RequireFullWindow:       false,
			TeamToPage:              team.DataEngineering,
			Severity:                lowSeverity,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, monitor)

	}

	return monitors, nil
})

var tableFreshnessMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	var tableFreshnessMonitorsRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5486055/Runbook+Table+Freshness+Monitors"

	for _, info := range teamInfo {
		if !slices.Contains(info.monitorTypesToEmit, "table_freshness") {
			continue
		}

		teamName := strings.ToLower(info.team.TeamName)

		query := fmt.Sprintf("max(last_1h):max:dagster_table_monitors.delta_table.freshness{owner:%s} by {database,table,region} > 0", teamName)

		tableFreshnessMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:              fmt.Sprintf("[%s] Table freshness monitor: table is not being updated per SLO", info.team.TeamName),
			Message:           "This table is not meeting it's freshness SLO. See table in DataHub: https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,{{database.name}}.{{table.name}},PROD)",
			Query:             query,
			RunbookLink:       tableFreshnessMonitorsRunbook,
			RequireFullWindow: false,
			NotifyNoData:      false,
			Severity:          lowSeverity,
			TeamToPage:        info.team,
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating tableFreshnessMonitor")
		}

		monitors = append(monitors, tableFreshnessMonitor)
	}

	return monitors, nil
})

var partitionFreshnessMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	var tableFreshnessMonitorsRunbook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5486055/Runbook+Table+Freshness+Monitors"
	var baselineExpectedLandingTime = 41 // hours since 12:01am UTC of parition date (clock starts around 41 hours for noon PST)
	var lowSeverityThreshold = baselineExpectedLandingTime + 25
	var highSeverityThreshold = baselineExpectedLandingTime + 73
	for _, info := range teamInfo {
		if !slices.Contains(info.monitorTypesToEmit, "partition_freshness") {
			continue
		}

		teamName := strings.ToLower(info.team.TeamName)
		var name string
		var severity dataPlatformMonitorSeverity

		for _, threshold := range []int{lowSeverityThreshold, highSeverityThreshold} {

			query := fmt.Sprintf("max(last_1h):max:dagster_table_monitors.latest_partition.freshness{owner:%s} by {database,table,region} > %d", teamName, threshold)
			msg := fmt.Sprintf("The latest partition is over %d hours stale. See table in DataHub: https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,{{database.name}}.{{table.name}},PROD)", threshold)

			if threshold == highSeverityThreshold {
				severity = businessHoursSeverity
				name = fmt.Sprintf("[%s] [Business Hours High Urgency] Latest Partition freshness monitor: latest partition has not been updated in the past %d hours", info.team.TeamName, threshold)
			} else {
				severity = lowSeverity
				name = fmt.Sprintf("[%s] [Low Urgency] Latest Partition freshness monitor: latest partition has not been updated in the past %d hours", info.team.TeamName, threshold)
			}

			partitionFreshnessMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:              name,
				Message:           msg,
				Query:             query,
				RunbookLink:       tableFreshnessMonitorsRunbook,
				RequireFullWindow: false,
				NotifyNoData:      false,
				Severity:          severity,
				TeamToPage:        info.team,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating partitionFreshnessMonitor")
			}

			monitors = append(monitors, partitionFreshnessMonitor)
		}
	}

	return monitors, nil
})
