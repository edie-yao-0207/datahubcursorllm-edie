package dataplatformmonitors

import (
	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/team/teamnames"
)

var Monitors = func() []emitters.ResourceEmitter {
	emitterFuncs := []emitters.ResourceEmitterFunc{ // lint: +sorted
		approachingDatabricksJobLimitMonitors,
		billingWorkflowHighUrgency,
		billingWorkflowLowUrgency,
		bulkNodeFailureMonitor,
		bulkSparkJobFailures,
		cdcThroughputMonitors,
		combineShardsUCJobFailures,
		consecutiveSparkReportFailureMonitors,
		consolidatedMergeMonitors,
		consolidatedMergeQueueDlq,
		consolidatedVacuumMonitors,
		dagsterAssetMaterializationFailureMonitors,
		dagsterDataQualityFailureMonitors,
		dagsterNoDataMonitors,
		databricksGrantMetricsMonitorHighSeverity,
		databricksJobMetricsMonitorHighSeverity,
		datahubDagsterNoSuccessfulDailyRunsMonitor,
		datahubDagsterNoSuccessfulHourlyRunsMonitor,
		datahubDagsterNoSuccessfulIntraDayRunsMonitor,
		datahubExpiringTokensMonitor,
		datahubFrontendLoadBalancerLatencyMonitor,
		datahubGMSEventFailureRateMonitor,
		datahubGMSEventsMonitor,
		datahubIngestedBytesMonitor,
		datahubLongRunningJobMonitor,
		datahubSiteReliabilityMonitor,
		dataPipelineNoDataMonitors,
		dataPipelinesPipelineLevelFailureMonitors,
		dataPipelinesPytestMemoryMonitors,
		dataPipelinesUnknownCauseFailureMonitor,
		dataPlatformLoadBalancerMonitor,
		datastreamLiveIngestionMonitor,
		datastreamRecordFormatConversionFailureMonitor,
		datastreamsVacuumOptimizeMonitor,
		dmsMetricsWorkerMonitors,
		dmsSourceLatencyMonitor,
		dmsTableFailureMonitors,
		dmsTargetLatencyMonitor,
		dmsTaskFailureMonitors,
		dmsTaskNotRunningMonitor,
		dmsTaskStoppedMonitors,
		dynamodbDeltaLakeMonitors,
		endToEndLocationMonitor,
		ksDiffToolMonitor,
		langfuseK8sCronExportMonitor,
		mergeJobSloMonitor,
		notebookJobSloMonitors,
		orchestrationFailureMonitor,
		orchestrationNoDataMonitor,
		partitionFreshnessMonitors,
		pipelinedReportStalenessMonitors,
		sparkMergeJobOverruns,
		sqliteExportNodeFailureMonitors,
		tableExistenceMonitors,
		tableFreshnessMonitors,
		unityCatalogSyncJobMonitors,
		vacuumJobFailures,
	}

	list := []emitters.ResourceEmitter{}
	for _, emitterFunc := range emitterFuncs {
		builder := emitters.ResourceEmitterWithTeamOwnersBuilder(emitterFunc)
		list = append(list, builder([]string{teamnames.DataPlatform}))
	}
	return list
}()
