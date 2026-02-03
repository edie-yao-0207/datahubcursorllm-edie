package dataplatformmonitors

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource/stepfunctions/statemachine"
	"samsaradev.io/infra/app/generate_terraform/emitters"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
	"samsaradev.io/infra/dataplatform/datapipelines/graphs"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

func executionsFailureQuery(duration string, threshold int, region, filter string) string {
	sfnFailureMetrics := []string{
		"aws.states.executions_aborted",
		"aws.states.executions_failed",
		"aws.states.executions_timed_out",
	}

	var queries []string
	for _, metric := range sfnFailureMetrics {
		queries = append(queries, fmt.Sprintf("sum:%s{region:%s,%s}.as_count()", metric, region, filter))
	}
	combined := fmt.Sprintf("sum(last_%s):%s >= %d", duration, strings.Join(queries, " + "), threshold)
	return combined
}

type pipelineMonitorMetadata struct {
	name string
	team components.TeamInfo
}

func getDataPipelineMetadata(region string) ([]pipelineMonitorMetadata, error) {
	dags, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "Error building dags")
	}

	pipelineMonitorsMetadata := make([]pipelineMonitorMetadata, 0, len(dags))
	for _, dag := range dags {
		disabled := false
		for _, disabledRegion := range dag.GetLeafNodes()[0].DisabledRegions() {
			if region == disabledRegion {
				disabled = true
				break
			}
		}
		if disabled {
			continue
		}
		pipelineMonitorsMetadata = append(pipelineMonitorsMetadata, pipelineMonitorMetadata{
			name: strings.Replace(statemachine.SanitizeName(dag.Name()), ".", "-", -1), // Sanitize DAG name using the State Machine AWS sanitization function to generate a 1:1 mapping of DAG name to Step Function name
			team: dag.GetLeafNodes()[0].Owner(),
		})
	}

	return pipelineMonitorsMetadata, nil
}

var dataPipelineNoDataMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		databricksAccountId := infraconsts.GetDatabricksAccountIdForRegion(region)
		pipelineMonitorsMetadata, err := getDataPipelineMetadata(region)
		if err != nil {
			return nil, oops.Wrapf(err, "Error generating pipeline monitor metadata")
		}

		var regionMonitors []tf.Resource
		for _, metadata := range pipelineMonitorsMetadata {
			monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name:                     fmt.Sprintf("%s %s data pipeline no data monitor", region, metadata.name),
				Message:                  fmt.Sprintf("The Data Pipeline %s has not executed for 3hrs in %s. Please consult the Data Pipeline Operators Guide (https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17832306/Data+Pipelines+Operators+Guide) for debugging techniques and/or notify the Data Platform (#ask-data-platform) team if persists or need help on debugging. %s %s", metadata.name, region, metadata.team.DatadogSlackLowUrgencyIntegrationTag(), metadata.team.DatadogPagerdutyLowUrgencyIntegrationTag()),
				NotifyNoData:             true,
				Query:                    fmt.Sprintf("sum(last_4h):sum:aws.states.executions_started{region:%s,statemachinearn:arn:aws:states:%s:%d:statemachine:%s}.as_count() < 1", region, region, databricksAccountId, metadata.name),
				RequireFullWindow:        true,
				NoDataTimeframeInMinutes: 275, // 3hrs and 5 minutes to make sure an execution has started for the 3hr merges
				DataPlatformDatadogTags:  []dataPlatformDatadogTagType{dataPipelinesTag},
				RunbookLink:              "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17832306/Data+Pipelines+Operators+Guide",
				TeamToPage:               metadata.team,
				Severity:                 lowSeverity,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}

			regionMonitors = append(regionMonitors, monitor)
		}

		monitors = append(monitors, regionMonitors...)
	}

	return monitors, nil
})

var bulkNodeFailureMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Fifty Data Pipeline node failures in %s", region),
			Message:     fmt.Sprintf("Fifty Data Pipeline nodes have failed in the past run in %s", region),
			Query:       executionsFailureQuery("3h", 50, region, "samsara:service:data-pipelines-nesf"),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4545118/Runbook+Fifty+Data+Pipeline+node+failures+in+Region",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: sparkInfraDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},

			NotifyNoData:             true,
			RequireFullWindow:        true,
			NoDataTimeframeInMinutes: 275, // 3hrs and 5 minutes to make sure an execution has started for the 3hr merges
		})
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, monitor)

	}

	return monitors, nil
})

var sqliteExportNodeFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Multiple sqlite node failues in %s", region),
			Message:     fmt.Sprintf("Multiple sqlite export nodes are failing in %s. This most likely indicates an underlying infrastructure issue with sqlite export.", region),
			Query:       executionsFailureQuery("3h", 4, region, "samsara:service:data-pipelines-nesf-sqlite"),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4458246/Multiple+SQLite+node+failures",

			DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
				dashboardLink: sparkInfraDashboard,
				variables: map[string]string{
					"region": region,
				},
			},

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},

			NotifyNoData:             true,
			RequireFullWindow:        true,
			NoDataTimeframeInMinutes: 275, // 3hrs and 5 minutes to make sure an execution has started for the 3hr merges
		})
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, monitor)

	}

	return monitors, nil
})

var orchestrationFailureMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Data pipeline orchestration state machine failed in %s", region),
			Message:     "Pipeline orchestration state machine failed",
			Query:       executionsFailureQuery("4h", 1, region, fmt.Sprintf("statemachinearn:arn:aws:states:%s:%d:statemachine:%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), "data_pipeline_orchestration_sfn")),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5791661/Runbook+Pipeline+Orchestration+Monitors",

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},

			NotifyNoData:      false,
			RequireFullWindow: false,
		})
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

var orchestrationNoDataMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		databricksAccountId := infraconsts.GetDatabricksAccountIdForRegion(region)

		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name:        fmt.Sprintf("Data pipeline orchestration state machine no data in %s", region),
			Message:     "Pipeline orchestration state machine did not run in last 4 hours.",
			Query:       fmt.Sprintf("sum(last_4h):sum:aws.states.executions_started{region:%s,statemachinearn:arn:aws:states:%s:%d:statemachine:%s}.as_count() < 1", region, region, databricksAccountId, "data_pipeline_orchestration_sfn"),
			Severity:    getSeverityForRegion(region, businessHoursSeverity),
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5791661/Runbook+Pipeline+Orchestration+Monitors",

			DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},

			NotifyNoData:      true,
			RequireFullWindow: true,
		})

		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

func pipelineToolshedLink(region string, name string) string {
	if region == infraconsts.SamsaraAWSDefaultRegion {
		return fmt.Sprintf("https://toolshed.internal.samsara.com/dataplatform/datapipelines/show/arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.SamsaraAWSDatabricksAccountID, name)
	} else if region == infraconsts.SamsaraAWSEURegion {
		return fmt.Sprintf("https://toolshed.internal.eu.samsara.com/dataplatform/datapipelines/show/arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.SamsaraAWSEUDatabricksAccountID, name)
	} else if region == infraconsts.SamsaraAWSCARegion {
		return fmt.Sprintf("https://toolshed.internal.ca.samsara.com/dataplatform/datapipelines/show/arn:aws:states:%s:%d:stateMachine:%s", region, infraconsts.SamsaraAWSCADatabricksAccountID, name)
	} else {
		return ""
	}
}

var pipelinedReportStalenessMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	nodeConfigs, err := configvalidator.ReadNodeConfigurations()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var monitors []tf.Resource
	for _, region := range []string{
		infraconsts.SamsaraAWSDefaultRegion,
		infraconsts.SamsaraAWSEURegion,
		infraconsts.SamsaraAWSCARegion,
	} {
		databricksAccountId := infraconsts.GetDatabricksAccountIdForRegion(region)
		for _, node := range nodeConfigs {
			if node.SqliteReportConfig != nil {
				for _, config := range node.SqliteReportConfig.StalenessMonitors {
					if config.Disable {
						continue
					}

					// Convert report_acceptable_staleness_hours to minutes
					stalenessHours := config.Hours
					stalenessMinutes := stalenessHours * 60

					// Send alerts to teams based on the configured urgency.
					var severityLevel dataPlatformMonitorSeverity
					if config.Urgency == "LOW" {
						severityLevel = lowSeverity
					} else {
						severityLevel = highSeverity
					}
					severityLevel = getSeverityForRegion(region, severityLevel)

					nodeName := strings.Replace(node.Name, ".", "-", -1)

					message := fmt.Sprintf(
						"[%s] Report %s is %d hours stale. The data pipeline for %s report has not succeeded in the acceptable staleness period of %d hours. This means that customers are seeing data that is %d hours stale. Please debug the pipeline on toolshed. Link: %s \n",
						region,
						nodeName,
						stalenessHours,
						nodeName,
						stalenessHours,
						stalenessHours,
						pipelineToolshedLink(region, nodeName),
					)

					if node.SqliteReportConfig.TeamRunbookUrl != "" {
						message += "\n Team Runbook: " + node.SqliteReportConfig.TeamRunbookUrl
					}

					monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
						Name:         fmt.Sprintf("[%s] Report %s is %d hours stale", region, nodeName, stalenessHours),
						Message:      message,
						NotifyNoData: true,
						Query: fmt.Sprintf(
							"sum(last_%dm):sum:aws.states.executions_succeeded{aws_account:%d,region:%s,statemachinearn:arn:aws:states:%s:%d:statemachine:%s}.as_count() < 1",
							stalenessMinutes,
							databricksAccountId,
							region,
							region,
							databricksAccountId,
							nodeName,
						),
						RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4540225/Runbook+Report+has+been+stale+X+hours",
						TeamToPage:  node.Owner,
						Severity:    severityLevel,
					})

					if err != nil {
						return nil, oops.Wrapf(err, "error creating monitor")
					}

					monitors = append(monitors, monitor)
				}
			}
		}
	}

	return monitors, nil
})

var dataPipelinesPytestMemoryMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "avg(last_1h):avg:spark.pytest.memory_usage{buildkite_pipeline_name:python-test, buildkite_label:_python_data_pipeline_node_test} > 8000000000",
		Name:    "Pytest memory usage is high",
		Message: "We are observing a large amount of memory consumed for a Datapipelines pytest in the last hour.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5977949",
		TeamToPage:              team.DataPlatform,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	monitorNewPipeline, err := createDataPlatformMonitor(dataPlatformMonitor{
		Query:   "avg(last_1h):avg:spark.pytest.memory_usage{buildkite_pipeline_name:dataplatform-python-tests, buildkite_label:_python_data_pipeline_node_test} > 8000000000",
		Name:    "Pytest memory usage is high in dataplatform-python-tests pipeline",
		Message: "[dataplatform-python-tests] We are observing a large amount of memory consumed for a Datapipelines pytest in the last hour.",

		DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},
		RunbookLink:             "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5977949",
		TeamToPage:              team.DataPlatform,
		Severity:                lowSeverity,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return []tf.Resource{monitor, monitorNewPipeline}, nil
})

var dataPipelinesPipelineLevelFailureMonitors = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	pipelines, err := graphs.BuildDAGs()
	if err != nil {
		return nil, oops.Wrapf(err, "error building pipeline for data pipeline monitor generation")
	}

	allWhollyOwnedNodes := make(map[string]nodetypes.Node)
	allSharedNotOwnedNodes := make(map[string]nodetypes.Node)

	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		for _, pipeline := range pipelines {
			allNodes := make([]string, len(pipeline.GetNodes()))
			var whollyOwnedNodes []string
			var sharedNotOwnedNodes []string
			leafNode := pipeline.GetLeafNodes()[0]
			owningTeam := leafNode.Owner()
			for idx, node := range pipeline.GetNodes() {
				// Add node to allNodes slice to keep track of all nodes
				allNodes[idx] = node.Name()

				if node.Owner().Name() == owningTeam.Name() {
					// If the node owned by the pipeline owner, they are responsible for it, add to whollyOwnedNodes slice
					whollyOwnedNodes = append(whollyOwnedNodes, node.Name())
					allWhollyOwnedNodes[node.Name()] = node
				} else {
					// If the node is not owned by the pipeline owner, it is shared, add to sharedNotOwnedNodes slice
					// The pipeline owning team will want to know when these fail and cause their pipeline to fail but are not responsible for them
					sharedNotOwnedNodes = append(sharedNotOwnedNodes, node.Name())
					allSharedNotOwnedNodes[node.Name()] = node
				}
			}

			// Use allNodes to create a pipeline level monitor for infra issues that sends to data platform
			// 01-31-2022 NOTE: This monitor is currently sending to DataPlatform-LowUrgency since we are running into
			// lots of instance capacity issues. In FY2023Q1 we are working on these issue and this monitor should page HighUrgency
			// for production pipelines when we solve the instance capacity issues.
			pipelineNameForDatadog := strings.Replace(statemachine.SanitizeName(pipeline.Name()), ".", "-", -1)
			pipelineInfraFailureMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name: fmt.Sprintf("[%s] %s data pipeline has failed due to Infrastructure Error", region, pipeline.Name()),
				Message: fmt.Sprintf("[%s] %s data pipeline has failed due to Infrastructure Error. Debug on toolshed: %s.",
					region,
					pipeline.Name(),
					pipelineToolshedLink(region, pipelineNameForDatadog),
				),
				Query: fmt.Sprintf("sum(last_4h):default_zero(sum:databricks.datapipelines.run.finish{%s AND status:failed AND cause:%s AND region:%s}.as_count()) >= 1",
					nodeSliceToInStatement(allNodes),
					datapipelineerrors.INFRA_FAILURE,
					region,
				),
				Severity:    getSeverityForRegion(region, infoSeverity),
				RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4805486/Pipeline+Fails+due+to+Infrastructure+Error",

				DatadogDashboardLink: &dataPlatformDatadogDashboardLink{
					dashboardLink: sparkInfraDashboard,
					variables: map[string]string{
						"region":          region,
						"statemachinearn": fmt.Sprintf("arn:aws:states:%s:%d:statemachine:%s", region, infraconsts.GetDatabricksAccountIdForRegion(region), pipelineNameForDatadog),
					},
				},

				DataPlatformDatadogTags: []dataPlatformDatadogTagType{dataPipelinesTag},
			})
			if err != nil {
				return nil, oops.Wrapf(err, "error creating pipelineInfraFailureMonitor")
			}

			monitors = append(monitors, pipelineInfraFailureMonitor)

			// use whollyOwnedNodes to create a pipeline level monitor for developer errors
			severityLevel := lowSeverity
			if leafNode.PipelineConfig() != nil && leafNode.PipelineConfig().DeveloperErrorSeverity == "HIGH" {
				severityLevel = highSeverity
			}

			toolshedLink := pipelineToolshedLink(region, strings.Replace(statemachine.SanitizeName(pipeline.Name()), ".", "-", -1))

			pipelineLevelWhollyOwnedNodesDeveloperErrorMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name: fmt.Sprintf("[%s] %s data pipeline has failed due to Developer Error", region, pipeline.Name()),
				Message: fmt.Sprintf("[%s] %s data pipeline has failed due to Developer Error. Debug on toolshed: %s.",
					region,
					pipeline.Name(),
					toolshedLink,
				),
				Query: fmt.Sprintf("sum(last_4h):default_zero(sum:databricks.datapipelines.run.finish{%s AND status:failed AND cause:%s AND region:%s}.as_count()) >= 1",
					nodeSliceToInStatement(whollyOwnedNodes),
					datapipelineerrors.DEVELOPER_ERROR,
					region,
				),
				RunbookLink: toolshedLink,
				TeamToPage:  owningTeam,
				Severity:    severityLevel,
			})

			monitors = append(monitors, pipelineLevelWhollyOwnedNodesDeveloperErrorMonitor)
		}

		// sharedNonTerminalNodes is a list of nodes that fall into a case where a team has developed a node
		// that is only used in other pipelines and the node itself doesn't fall into a pipeline.
		// These nodes don't end up getting a 'Developer Error' monitor since they don't exist in a wholly owned pipeline
		sharedNonTerminalNodes := findOnlySharedNodes(allWhollyOwnedNodes, allSharedNotOwnedNodes)

		// Create a node level monitor for each node in this edge case so the owner of the node can be notified properly
		for _, node := range sharedNonTerminalNodes {
			nodeName := node.Name()
			owningTeam := node.Owner()
			pipelineLevelWhollyOwnedNodesDeveloperErrorMonitor, err := createDataPlatformMonitor(dataPlatformMonitor{
				Name: fmt.Sprintf("[%s] %s node has failed due to Developer Error", region, nodeName),
				Message: fmt.Sprintf("[%s] %s node has failed due to Developer Error. This node is a dependency in multiple pipelines but is owned by your team. Please find a run on toolshed and debug. %s %s",
					region,
					nodeName,
					owningTeam.DatadogPagerdutyLowUrgencyIntegrationTag(),
					owningTeam.DatadogSlackLowUrgencyIntegrationTag(),
				),
				Query: fmt.Sprintf("sum(last_4h):default_zero(sum:databricks.datapipelines.run.finish{node:%s,status:failed,cause:%s,region:%s}.as_count()) >= 1",
					nodeName,
					datapipelineerrors.DEVELOPER_ERROR,
					region,
				),
				RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17832306/Data+Pipelines+Operators+Guide",
				TeamToPage:  owningTeam,
				Severity:    lowSeverity,
			})

			if err != nil {
				return nil, oops.Wrapf(err, "error creating monitor")
			}

			monitors = append(monitors, pipelineLevelWhollyOwnedNodesDeveloperErrorMonitor)
		}
	}

	return monitors, nil
})

var dataPipelinesUnknownCauseFailureMonitor = emitters.ResourceEmitterFunc(func() ([]tf.Resource, error) {
	var monitors []tf.Resource
	for _, region := range []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion} {
		monitor, err := createDataPlatformMonitor(dataPlatformMonitor{
			Name: fmt.Sprintf("[%s] {{node.name}} is failing with 'unknown' cause", region),
			Message: fmt.Sprintf(`[%s] {{node.name}} is failing with 'unknown' cause. Our error classification system is unable to classify a pipelines error.
1. Find that failing node ({{node.name}}) on toolshed
2. Click into the run to find the error
  - Might have to go into the databricks job
  - Might be an infrastructure issue present on the UI directly
3. Put up a new error definition in 'datapipelines/datapipelineerrors/error_definitions.go and make a PR.
%s %s'`, region, team.DataPlatform.DatadogSlackIntegrationTag(), team.DataPlatform.DatadogPagerdutyLowUrgencyIntegrationTag()),
			Query:       fmt.Sprintf("sum(last_4h):default_zero(sum:databricks.datapipelines.run.finish{status:failed,cause:unknown,region:%s} by {node}.as_count()) >= 1", region),
			RunbookLink: "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17832306/Data+Pipelines+Operators+Guide",
			Severity:    getSeverityForRegion(region, infoSeverity),
		})

		if err != nil {
			return nil, oops.Wrapf(err, "error creating monitor")
		}

		monitors = append(monitors, monitor)
	}

	return monitors, nil
})

// nodeSliceToInStatement takes in a slice of node names and outputs an IN statement
// to be used in datadog monitors
func nodeSliceToInStatement(nodes []string) string {
	sort.Strings(nodes)
	return fmt.Sprintf("node IN (%s)", strings.Join(nodes, ","))
}

func findOnlySharedNodes(whollyOwnedNodes map[string]nodetypes.Node, sharedNodes map[string]nodetypes.Node) []nodetypes.Node {
	var onlySharedNodes []nodetypes.Node
	for node, nodeData := range sharedNodes {
		if _, ok := whollyOwnedNodes[node]; !ok {
			onlySharedNodes = append(onlySharedNodes, nodeData)
		}
	}

	return onlySharedNodes
}
