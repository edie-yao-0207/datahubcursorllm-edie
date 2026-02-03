package dataplatformmonitors

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
)

// All of our production replication instances start with `rds-prod`,
// which we can use to help us filter out unused dms resources.
// A better method might be to use tags, but tag propagation to datadog
// can take a long time (many days) so we would get no metrics in that
// interim; it'd be worth for someone to investigate this eventually.
var parquetDmsPrefix = "parquet-"

var cdcTaskPrefix = "parquet-task-cdc"

// TODO: it's not clear why we have these monitors alongside the task failure ones.
// It's likely not a huge deal, but could be good to investigate.
var dmsTaskNotRunningMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:                     fmt.Sprintf("[%s] DMS task not running", parquetDmsPrefix),
		Message:                  "This monitor attempts to catch DMS replication tasks that are automatically stopped by AWS in \"failed\" state and no longer producing CDC files. As a result, their downstream data lake tables no longer receive updates.\n\nThe alert threshold configured in this monitor does not matter. Instead, we are interested in knowing replication tasks that stop producing this metric for more than 30 minutes, indicating a task has run before, and has stopped running. \n\nPlease see [Runbook: DMS task failure](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5608589/Runbook+DMS+task+failure)\n\n@slack-alerts-data-platform @pagerduty-DataPlatform",
		Query:                    fmt.Sprintf("min(last_60m):max:aws.dms.run_counter{replicationinstanceidentifier:%s*} by {region,replicationinstanceidentifier} > 100000", parquetDmsPrefix),
		DataPlatformDatadogTags:  []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
		NoDataTimeframeInMinutes: 120,
		Severity:                 lowSeverity,
		RunbookLink:              "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5608589/Runbook+DMS+task+failure",
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error creating monitor")
	}
	monitors = append(monitors, monitor)

	return monitors, nil
})

var dmsTaskFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	for dmsTaskServiceName, severityLevel := range dmsTaskServiceNameToSeverityMap {

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("DMS task failure for %s", dmsTaskServiceName),
			Message:                 "If a DMS task is in a \"failed\" state, it stops replicating data from its configured database to data lake. \n\n Please see [Runbook: DMS task failure](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5608589/Runbook+DMS+task+failure)",
			Query:                   fmt.Sprintf("min(last_1h):min:dmsmetrics.task.failed{owner:dataplatform,samsara:service:%s} by {region,repilcationtaskidentifier} > 0", dmsTaskServiceName),
			RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5608589/Runbook+DMS+task+failure",
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
			TeamToPage:              team.DataPlatform,
			Severity:                severityLevel,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s", dmsTaskServiceName)
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

var dmsTaskStoppedMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	for dmsTaskServiceName, severityLevel := range dmsTaskServiceNameToSeverityMap {

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("DMS task stopped for %s", dmsTaskServiceName),
			Message:                 "If a DMS task is in a \"stopped\" state, it stops replicating data from its configured database to data lake. \n\n Please see [Runbook: DMS task stopped](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4548960/Runbook+DMS+task+stopped)",
			Query:                   fmt.Sprintf("min(last_1h):min:dmsmetrics.task.stopped{owner:dataplatform,samsara:service:%s,replicationtaskidentifier:%s*} by {region,replicationtaskidentifier} > 0", dmsTaskServiceName, cdcTaskPrefix),
			RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4548960/Runbook+DMS+task+stopped",
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
			TeamToPage:              team.DataPlatform,
			Severity:                severityLevel,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s", dmsTaskServiceName)
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

var dmsTaskStuckCreatedMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource

	for dmsTaskServiceName, _ := range dmsTaskServiceNameToSeverityMap {

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:                    fmt.Sprintf("DMS task stuck in Created for %s", dmsTaskServiceName),
			Message:                 "If a DMS task is stuck in a \"Created\" state, it means data replication has never begun for this database due to some configuration issue that's preventing Dagster to start the task. \n\n Please see [Runbook: DMS Task Stuck in Created](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5724004/Runbook+DMS+Task+Stuck+in+Created)",
			Query:                   fmt.Sprintf("min(last_3d):min:dmsmetrics.task.created{owner:dataplatform,samsara:service:%s} by {region,repilcationtaskidentifier} > 0", dmsTaskServiceName),
			RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5724004/Runbook+DMS+Task+Stuck+in+Created",
			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
			TeamToPage:              team.DataPlatform,
			Severity:                businessHoursSeverity,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor for %s", dmsTaskServiceName)
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

var dmsTableFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var resources []tf.Resource
	for _, databaseDefinition := range rdsdeltalake.AllDatabases() {
		for tableName, table := range databaseDefinition.Tables {
			severityLevel := lowSeverity
			if table.Production {
				severityLevel = highSeverity
			}
			schemaName := databaseDefinition.MySqlDb

			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:                    fmt.Sprintf("DMS table failure for %s %s.%s", databaseDefinition.Name, schemaName, tableName),
				Message:                 "If a DMS replication task reports a table in a failed state, the task will no longer replicate data from the said table, but it will continue to replicate data from other healthy tables.\n\nWe've seen this a few times, but we don't yet understand the root cause. If you get paged, please notify the DataPlatform team channel. \nReload table runbook: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5269985/Runbook+DMS+Reload",
				Query:                   fmt.Sprintf("max(last_60m):max:dmsmetrics.task.table.failed{owner:dataplatform,schema:%s,table:%s} by {region,repilcationtaskidentifier} > 0", schemaName, tableName),
				DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
				Severity:                severityLevel,
				RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5269985/Runbook+DMS+Reload",
				TeamToPage:              team.DataPlatform,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}

			resources = append(resources, monitor)
		}
	}
	return resources, nil
})

var dmsSourceLatencyMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:                    fmt.Sprintf("[%s] High DMS Source Latency", parquetDmsPrefix),
		Message:                 fmt.Sprintf("High DMS Source Latency indicates that the DMS replication task is getting behind on processing the binlog. This page indicates a serious problem, and if this value gets to 1 day, we will have to reload the database. Runbook: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5818424/Runbook+DMS+High+Source+Target+Latency+Monitor . %s %s", team.DataPlatform.DatadogSlackIntegrationTag(), team.DataPlatform.DatadogPagerdutyIntegrationTag()),
		Query:                   fmt.Sprintf("min(last_1h):min:aws.dms.cdclatency_source{replicationinstanceidentifier:%s*} by {replicationinstanceidentifier,region}.rollup(min) > 3600", parquetDmsPrefix),
		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5818424/Runbook+DMS+High+Source+Target+Latency+Monitor",
	})

	if err != nil {
		return nil, oops.Wrapf(err, "error creating monitor")
	}
	monitors = append(monitors, monitor)

	return monitors, nil

})

var dmsTargetLatencyMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Name:                    fmt.Sprintf("[%s] High DMS Target Latency", parquetDmsPrefix),
		Message:                 fmt.Sprintf("High DMS Target Latency indicates that the DMS replication task is getting behind on writing cdc updates to S3. This page indicates a serious problem, and if this value gets to 1 day, we will have to reload the database. Runbook: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5818424/Runbook+DMS+High+Source+Target+Latency+Monitor . %s %s", team.DataPlatform.DatadogSlackIntegrationTag(), team.DataPlatform.DatadogPagerdutyIntegrationTag()),
		Query:                   fmt.Sprintf("min(last_1h):min:aws.dms.cdclatency_target{replicationinstanceidentifier:%s*} by {replicationinstanceidentifier,region}.rollup(min) > 3600", parquetDmsPrefix),
		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5818424/Runbook+DMS+High+Source+Target+Latency+Monitor",
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error creating monitor")
	}
	monitors = append(monitors, monitor)

	return monitors, nil
})

var dmsTaskServiceNameToSeverityMap = computeDmsTaskServiceNameToSeverity()

// computeDmsTaskServiceNameToPagerdutyIntegrationTagMap returns a map where the key is the service name for each DMS task
// and the value is what pagerduty tag we want to add for any monitors on this tag (i.e high urgency or business urgency).
// For any DMS tasks that are for databases with at least 1 production table, we want the page to be high urgency. Otherwise, low urgency.
func computeDmsTaskServiceNameToSeverity() map[string]dataPlatformMonitorSeverity {
	dmsTaskServiceNameToPagerdutyIntegrationTagMap := make(map[string]dataPlatformMonitorSeverity)

	var largeDatabases = map[string]struct{}{
		dbregistry.CmAssetsDB: {},
	}

	for _, databaseDefinition := range rdsdeltalake.AllDatabases() {
		severityLevel := businessHoursSeverity

		if _, ok := largeDatabases[databaseDefinition.Name]; ok {
			severityLevel = highSeverity
		} else if databaseDefinition.HasProductionTable() {
			severityLevel = highSeverity
		}

		serviceName := fmt.Sprintf("dms-%s", databaseDefinition.MySqlDb)
		// Since productsdb has the same mysql db name as clouddb, use productsdb here instead of prod_db to prevent name collision.
		if databaseDefinition.Name == "productsdb" {
			serviceName = fmt.Sprintf("dms-%s", databaseDefinition.Name)
		}

		dmsTaskServiceNameToPagerdutyIntegrationTagMap[serviceName] = severityLevel
	}

	return dmsTaskServiceNameToPagerdutyIntegrationTagMap
}

var cdcThroughputMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, db := range rdsdeltalake.AllDatabases() {
		for region, shardMonitors := range db.CdcThroughputMonitors {
			for shardNum, timeframe := range shardMonitors {
				// shardNum is 1-indexed, so we subtract 1 from it.
				shardName := db.RegionToShards[region][int(shardNum-1)]
				replicationInstanceIdentifier := fmt.Sprintf("parquet-%s", shardName)
				monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
					Name:                    fmt.Sprintf("[%s,%s] Low CDC Throughput", region, replicationInstanceIdentifier),
					Message:                 "This alert indicates that the CDC throughput on the DMS task is lower than expected. This usually indicates a problem with DMS, please investigate urgently.",
					Query:                   fmt.Sprintf("sum(last_%dm):default_zero(sum:aws.dms.cdcthroughput_rows_target{region:%s,replicationinstanceidentifier:%s} by {replicationinstanceidentifier,region}) <= 0", timeframe.TimeframeMinutes, region, replicationInstanceIdentifier),
					DataPlatformDatadogTags: []dataPlatformDatadogTagType{dmsTag, dayLightHoursTag},
					RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4570965/Runbook+Zero+Row+Monitors",
					// TODO: enable high urgency once we vet that this is working well.
					Severity: lowSeverity,
				})
				if err != nil {
					return nil, oops.Wrapf(err, "failed to build low cdc throughput monitor")
				}
				monitors = append(monitors, monitor)
			}
		}
	}

	return monitors, nil
})
