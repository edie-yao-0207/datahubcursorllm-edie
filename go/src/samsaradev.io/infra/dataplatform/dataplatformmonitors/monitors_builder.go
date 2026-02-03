package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/datadogresource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/components/datadogtags"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// dataplatformNonBizHoursTag is a scheduled downtime (made manually) in datadog that we attach
// to our "business hours" monitors to prevent being paged outside of business hours.
// During oncall handoff, the current oncall will modify this in datadog
// to account for whoever is oncall (east-coast or UK members).
// https://app.datadoghq.com/monitors/downtimes/d9284adc-3d0e-11ee-98c1-da7ad0900002/edit
const dataplatformNonBizHoursTag = "dataplatform-nonbizhours"
const dataengineeringNonBizHoursTag = "dataengineering-nonbizhours"

const infoChannel = "@slack-warn-data-platform"

// getSeverityForRegion returns the appropriate severity for a given region. All
// Canada alerts are set to low urgency for now.
func getSeverityForRegion(region string, defaultSeverity dataPlatformMonitorSeverity) dataPlatformMonitorSeverity {
	if region == infraconsts.SamsaraAWSCARegion {
		return lowSeverity
	}
	return defaultSeverity
}

type dataPlatformMonitorSeverity int

const (
	// noAlert is for monitors that need to be created for some reason but should not
	// send alerts anywhere.
	noAlert dataPlatformMonitorSeverity = iota

	// infoSeverity sends a slack message to our info channel but will not otherwise
	// alert us. It's useful for things that are passively consumable but not actionable.
	infoSeverity

	// lowSeverity goes to low urgency PD + alerts-data-platform slack channel.
	lowSeverity

	// businessHoursSeverity pages high urgency during business hours using
	// our scheduled downtime tag.
	businessHoursSeverity

	// highSeverity pages high urgency during all hours.
	highSeverity
)

type dataPlatformDatadogTagType string

const ( // lint: +sorted
	amundsenTag          dataPlatformDatadogTagType = "dataplatform:amundsen"
	datahubTag           dataPlatformDatadogTagType = "dataplatform:datahub"
	dagsterTag           dataPlatformDatadogTagType = "dataplatform:dagster"
	dataPipelinesTag     dataPlatformDatadogTagType = "dataplatform:data_pipelines"
	dataStreamsTag       dataPlatformDatadogTagType = "dataplatform:data_streams"
	dmsTag               dataPlatformDatadogTagType = "dataplatform:dms"
	ksMergeJobTag        dataPlatformDatadogTagType = "dataplatform:ks_merge_job"
	kinesisStatsTag      dataPlatformDatadogTagType = "dataplatform:kinesis_stats_job"
	mergeJobTag          dataPlatformDatadogTagType = "dataplatform:merge_job"
	rdsMergeJobTag       dataPlatformDatadogTagType = "dataplatform:rds_merge_job"
	dynamodbDeltaLakeTag dataPlatformDatadogTagType = "dataplatform:dynamodb_delta_lake"
	vacuumTag            dataPlatformDatadogTagType = "dataplatform:vacuum"
	ucSyncJobTag         dataPlatformDatadogTagType = "dataplatform:uc_sync_job"
	ksDiffToolTag        dataPlatformDatadogTagType = "dataplatform:ks_diff_tool"
	mlInfraTag           dataPlatformDatadogTagType = "dataplatform:ml_infra"
	// dayLightHoursTag is a datadog tag that we use to mute alerts after 6PM PST and before 8AM PST all days a week.
	dayLightHoursTag dataPlatformDatadogTagType = dataPlatformDatadogTagType(string(datadogtags.MuteUSWestEvenings))
)

type dataPlatformMonitor struct {
	Name    string
	Message string
	Query   string
	// Severity controls how this monitor is alerted on
	Severity    dataPlatformMonitorSeverity
	RunbookLink string

	DatadogDashboardLink    *dataPlatformDatadogDashboardLink
	DatabricksJobSearchLink *databricksJobSearchLink
	DagsterDashboardLink    string

	DataPlatformDatadogTags []dataPlatformDatadogTagType

	NotifyNoData             bool
	NotifyBy                 []string
	NoDataTimeframeInMinutes int
	RequireFullWindow        bool
	NewGroupDelayInMinutes   int
	TeamToPage               components.TeamInfo // TODO: should convert this to a slice to allow multiple subscribers
	EvaluationDelay          int
}

func createDataPlatformMonitor(input dataPlatformMonitor) (*datadogresource.Monitor, error) {
	if input.RunbookLink == "" && input.Severity != noAlert {
		return nil, oops.Errorf("Data Platform monitor %s must have a runbook; if none ready, make a dummy/skeleton one for now and link here", input.Name)
	}

	datadogTags := make([]string, 0, len(input.DataPlatformDatadogTags))
	for _, tag := range input.DataPlatformDatadogTags {
		datadogTags = append(datadogTags, string(tag))
	}

	allTags := datadogTags

	var pagerdutyTag string
	var slackTag string

	if input.TeamToPage.TeamName != "" {
		slackTag = input.TeamToPage.DatadogSlackIntegrationTag()

		if input.Severity == lowSeverity {
			pagerdutyTag = input.TeamToPage.DatadogPagerdutyLowUrgencyIntegrationTag()
		} else if input.Severity == highSeverity {
			pagerdutyTag = input.TeamToPage.DatadogPagerdutyIntegrationTag()
		} else if input.Severity == businessHoursSeverity && input.TeamToPage.Team().Name() == team.DataPlatform.Team().Name() {
			// Allow business urgency for only DataPlatform team.
			pagerdutyTag = input.TeamToPage.DatadogPagerdutyIntegrationTag()
			allTags = append(allTags, dataplatformNonBizHoursTag)
		} else if input.Severity == businessHoursSeverity && input.TeamToPage.Team().Name() == team.DataEngineering.Team().Name() {
			// Allow business urgency for only DataEngineering team.
			pagerdutyTag = input.TeamToPage.DatadogPagerdutyIntegrationTag()
			allTags = append(allTags, dataengineeringNonBizHoursTag)
		} else {
			return nil, oops.Errorf("we only support paging for low/high urgency to other teams.")
		}

	} else {
		// if a custom (non-dataplat) PagerDuty tag is set, dataplatform should not be the listed team in the tags
		allTags = append([]string{"team:dataplatform"}, allTags...)

		if input.Severity == noAlert {
			// no-op, leave pagerduty and slack tags empty
		} else if input.Severity == infoSeverity {
			slackTag = infoChannel
		} else if input.Severity == lowSeverity {
			pagerdutyTag = team.DataPlatform.DatadogPagerdutyLowUrgencyIntegrationTag()
			slackTag = team.DataPlatform.DatadogSlackIntegrationTag()
		} else if input.Severity == businessHoursSeverity && input.TeamToPage.Team().Name() == team.DataTools.Team().Name() {
			pagerdutyTag = team.DataTools.DatadogPagerdutyIntegrationTag()
			slackTag = team.DataEngineering.DatadogSlackIntegrationTag()
			allTags = append(allTags, dataengineeringNonBizHoursTag)
		} else if input.Severity == businessHoursSeverity {
			pagerdutyTag = team.DataPlatform.DatadogPagerdutyIntegrationTag()
			slackTag = team.DataPlatform.DatadogSlackIntegrationTag()
			allTags = append(allTags, dataplatformNonBizHoursTag)
		} else if input.Severity == highSeverity {
			pagerdutyTag = team.DataPlatform.DatadogPagerdutyIntegrationTag()
			slackTag = team.DataPlatform.DatadogSlackIntegrationTag()
		} else {
			return nil, oops.Errorf("Unknown severity: %d", int(input.Severity))
		}

	}

	buildMessageFunc := func(input dataPlatformMonitor) string {
		message := fmt.Sprintf("%s \n\n Runbook: %s", input.Message, input.RunbookLink)
		if input.DatadogDashboardLink != nil {
			message = fmt.Sprintf("%s \n\n Datadog dashboard: %s", message, input.DatadogDashboardLink.generateDashboardLink())
		}
		if input.DatabricksJobSearchLink != nil {
			message = fmt.Sprintf("%s \n\n Databricks Job Search Link: %s", message, input.DatabricksJobSearchLink.generateLink())
		}
		if input.DagsterDashboardLink != "" {
			message = fmt.Sprintf("%s \n\n Dagster Dashboard Link: %s", message, input.DagsterDashboardLink)
		}
		message = fmt.Sprintf("%s \n\n %s %s", message, slackTag, pagerdutyTag)

		return message
	}

	return &datadogresource.Monitor{
		Name:    input.Name,
		Type:    datadogresource.MonitorType_MetricAlert,
		Message: buildMessageFunc(input),
		Query:   input.Query,

		Tags: allTags,

		EvaluationDelay:   input.EvaluationDelay,
		NotifyNoData:      input.NotifyNoData,
		NotifyBy:          input.NotifyBy,
		NoDataTimeframe:   input.NoDataTimeframeInMinutes,
		RequireFullWindow: input.RequireFullWindow,
		NewGroupDelay:     input.NewGroupDelayInMinutes * 60,
	}, nil
}
