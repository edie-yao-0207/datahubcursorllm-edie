package nodetypes

import (
	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/replicationtypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/sparktypes"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team/components"
)

type Node interface {
	Name() string
	Dependencies() []dependencytypes.Dependency
	PipelineDependencies() []dependencytypes.Dependency
	Description() string
	Output() outputtypes.Output
	BackfillStartDate() string
	Owner() components.TeamInfo
	SparkExecutionEnvironment() sparktypes.SparkExecutionEnvironment
	RegionsToReplicateToUs() []string
	DisabledRegions() []string
	NonProduction() bool
	SqliteReportConfig() *configvalidator.SqliteReportConfig
	SqliteNodeName() *string
	DailyRunConfig() *configvalidator.DailyRunConfig
	PipelineConfig() *configvalidator.PipelineConfig
	UcEnabled() bool
}

type BaseNode struct {
	NodeName                  string
	owner                     components.TeamInfo
	tags                      []amundsentags.Tag
	description               string
	Deps                      []dependencytypes.Dependency
	Parameters                []string
	output                    outputtypes.Output
	backfillStartDate         string
	sparkVersion              string
	sparkExecutionEnvironment sparktypes.SparkExecutionEnvironment
	replication               replicationtypes.Replication
	disabledRegions           []string
	nonproduction             bool
	sqliteReportConfig        *configvalidator.SqliteReportConfig
	dailyRunConfig            *configvalidator.DailyRunConfig
	pipelineConfig            *configvalidator.PipelineConfig
	ucEnabled                 bool
}

var _ Node = (*BaseNode)(nil)

func (bn BaseNode) Name() string {
	return bn.NodeName
}

func (bn BaseNode) Dependencies() []dependencytypes.Dependency {
	return bn.Deps
}

func (bn BaseNode) Description() string {
	return bn.description
}

func (bn BaseNode) Output() outputtypes.Output {
	return bn.output
}

func (bn BaseNode) PipelineDependencies() []dependencytypes.Dependency {
	var nonExternalDeps []dependencytypes.Dependency
	for _, dep := range bn.Dependencies() {
		if !dep.IsExternalDep() {
			nonExternalDeps = append(nonExternalDeps, dep)
		}
	}

	return nonExternalDeps
}

func (bn BaseNode) BackfillStartDate() string {
	return bn.backfillStartDate
}

func (bn BaseNode) Owner() components.TeamInfo {
	return bn.owner
}

func (bn BaseNode) SparkExecutionEnvironment() sparktypes.SparkExecutionEnvironment {
	return bn.sparkExecutionEnvironment
}

func (bn BaseNode) RegionsToReplicateToUs() []string {
	return bn.replication.ReplicateToUsFromRegions
}

func (bn BaseNode) DisabledRegions() []string {
	return bn.disabledRegions
}

func (bn BaseNode) NonProduction() bool {
	return bn.nonproduction
}

func (bn BaseNode) UcEnabled() bool {
	return bn.ucEnabled
}

func (bn BaseNode) SqliteReportConfig() *configvalidator.SqliteReportConfig {
	return bn.sqliteReportConfig
}

func (bn BaseNode) DailyRunConfig() *configvalidator.DailyRunConfig {
	return bn.dailyRunConfig
}

func (bn BaseNode) SqliteNodeName() *string {
	if bn.sqliteReportConfig != nil {
		return pointer.StringPtr(bn.NodeName + "-sqlite")
	}
	return nil
}

func (bn BaseNode) PipelineConfig() *configvalidator.PipelineConfig {
	return bn.pipelineConfig
}

// NodeConfigToDAGNode converts a configuration for a node to a DAG Node
func NodeConfigToDAGNode(nodeConfig configvalidator.NodeConfiguration) Node {
	return &BaseNode{
		NodeName:                  nodeConfig.Name,
		owner:                     nodeConfig.Owner,
		tags:                      nodeConfig.Tags,
		description:               nodeConfig.Description,
		Deps:                      nodeConfig.Dependencies,
		Parameters:                nodeConfig.Parameters,
		output:                    nodeConfig.Output,
		backfillStartDate:         nodeConfig.BackfillStartDate,
		sparkExecutionEnvironment: nodeConfig.SparkExecutionEnvironment,
		replication:               nodeConfig.Replication,
		disabledRegions:           nodeConfig.DisabledRegions,
		nonproduction:             nodeConfig.NonProduction,
		sqliteReportConfig:        nodeConfig.SqliteReportConfig,
		dailyRunConfig:            nodeConfig.DailyRunConfig,
		pipelineConfig:            nodeConfig.PipelineConfig,
	}
}
