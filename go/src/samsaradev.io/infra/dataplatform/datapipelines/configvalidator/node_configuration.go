package configvalidator

import (
	"encoding/json"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/amundsen/amundsentags"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/dependencytypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/outputtypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/replicationtypes"
	"samsaradev.io/infra/dataplatform/datapipelines/nodetypes/sparktypes"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type StalenessMonitor struct {
	Hours   int64  `json:"hours"`
	Urgency string `json:"urgency"`
	Disable bool   `json:"disable"`
}

type SqliteReportConfig struct {
	ReportName        string             `json:"report_name"`
	StalenessMonitors []StalenessMonitor `json:"staleness_monitors,omitempty"`

	// A runbook that will be linked in monitors for this report. This is optional, but owning teams
	// may want to specify a team-specific runbook that can provide additional context when dealing with
	// outages.
	TeamRunbookUrl string `json:"team_runbook_url,omitempty"`

	// These fields should not be set in general; they exist for some special reports (e.g. cm_health_report) that
	// have "snapshot" behavior, i.e. every run overwrites the existing rows at a fixed point.
	HardcodedStartDate *string `json:"hardcoded_start_date,omitempty"`
	HardcodedEndDate   *string `json:"hardcoded_end_date,omitempty"`
}

// TODO: move DailyRunConfig inside of PipelineConfig as it is a pipeline-level setting
type DailyRunConfig struct {
	StartDateOffset int64 `json:"start_date_offset"`
}

type PipelineConfig struct {
	DeveloperErrorSeverity string `json:"developer_error_severity"`
}

type NodeConfiguration struct {
	Name                      string                               `json:"name"`
	RawOwner                  string                               `json:"owner"`
	Owner                     components.TeamInfo                  `json:"-"`
	RawTags                   []string                             `json:"tags"`
	Tags                      []amundsentags.Tag                   `json:"-"`
	BackfillStartDate         string                               `json:"backfill_start_date"`
	UnshardedBackfills        bool                                 `json:"unsharded_backfills"`
	Description               string                               `json:"description"`
	RawDependencies           []json.RawMessage                    `json:"dependencies"`
	Dependencies              []dependencytypes.Dependency         `json:"-"`
	Parameters                []string                             `json:"parameters"`
	RawOutput                 json.RawMessage                      `json:"output"`
	Output                    outputtypes.Output                   `json:"-"`
	SparkExecutionEnvironment sparktypes.SparkExecutionEnvironment `json:"spark_execution_environment"`
	Replication               replicationtypes.Replication         `json:"replication"`
	DisabledRegions           []string                             `json:"disabled_regions"`
	NonProduction             bool                                 `json:"nonproduction"`
	SqliteReportConfig        *SqliteReportConfig                  `json:"sqlite_report_config,omitempty"`
	DailyRunConfig            *DailyRunConfig                      `json:"daily_run_config,omitempty"`
	PipelineConfig            *PipelineConfig                      `json:"pipeline_config,omitempty"`
}

func (nc *NodeConfiguration) UnmarshalJSON(b []byte) error {
	type nodeConfig NodeConfiguration
	err := json.Unmarshal(b, (*nodeConfig)(nc))
	if err != nil {
		return err
	}

	teamInfo, ok := team.TeamByName[nc.RawOwner]
	if !ok {
		return oops.Errorf("Invalid team %s", nc.RawOwner)
	}
	nc.Owner = teamInfo

	// Parse dependencies
	nc.Dependencies = make([]dependencytypes.Dependency, len(nc.RawDependencies))
	for i, rawDep := range nc.RawDependencies {
		dep, err := dependencytypes.ParseRawDependency(rawDep)
		if err != nil {
			return oops.Wrapf(err, "Error parsing dependency")
		}
		nc.Dependencies[i] = dep
	}

	// Parse output
	output, err := outputtypes.ParseRawOutput(nc.RawOutput)
	if err != nil {
		return oops.Wrapf(err, "Error parsing output")
	}
	nc.Output = output

	// Validate Spark Execution Environment if developer defined override values
	if _, err := sparktypes.IsValidSparkExecutionEnvironment(nc.SparkExecutionEnvironment); err != nil {
		return oops.Wrapf(err, "Error parsing spark_execution_environment")
	}

	validRegionsToReplicateToUs := make(map[string]struct{})
	for _, region := range infraconsts.DONOTUSE_AllRegions {
		if region != infraconsts.SamsaraAWSDefaultRegion {
			validRegionsToReplicateToUs[region] = struct{}{}
		}
	}
	for _, region := range nc.Replication.ReplicateToUsFromRegions {
		if _, ok := validRegionsToReplicateToUs[region]; !ok {
			return oops.Errorf("replicate_to_us_from_regions must be one of: %v", validRegionsToReplicateToUs)
		}
	}

	// Parse and validate tags
	tags := make([]amundsentags.Tag, len(nc.RawTags))
	for idx, tag := range nc.RawTags {
		typedTag, err := amundsentags.GetTag(tag)
		if err != nil {
			return oops.Wrapf(err, "invalid tag %s", tag)
		}
		tags[idx] = typedTag
	}
	nc.Tags = tags

	// Validate that the staleness monitor is correctly configured, either explicitly disabled or with hours+urgency set.
	if nc.SqliteReportConfig != nil {
		if len(nc.SqliteReportConfig.StalenessMonitors) == 0 {
			return oops.Errorf("Please configure at least one staleness monitor for every sqlite report. If you explicitly don't want one, set `disable: true` inside the monitor.")
		}

		for _, monitor := range nc.SqliteReportConfig.StalenessMonitors {
			if !monitor.Disable {
				if monitor.Hours <= 6 {
					return oops.Errorf("'report_acceptable_staleness_hours' key less than 6 hours for %s. A value of <= 6 hours will page after even 1 failure, so we don't recommend setting a monitor like this. Please increase the staleness value, or contact #ask-data-platform if you have any questions.", nc.SqliteReportConfig.ReportName)
				}

				switch monitor.Urgency {
				case "LOW", "HIGH":
				default:
					return oops.Errorf("staleness_monitor_urgency should be LOW or HIGH for low or high urgency respectively. (The default is high urgency)")
				}
			}
		}
	}

	if nc.PipelineConfig != nil {
		switch nc.PipelineConfig.DeveloperErrorSeverity {
		case "", "LOW", "HIGH":
		default:
			return oops.Errorf("developer_error_severity should be left empty, or set to LOW or HIGH. The default is LOW.")
		}
	}

	return nil
}
