package databricks

import (
	"context"
	"fmt"
	"net/http"

	"github.com/samsarahq/go/oops"
	"golang.org/x/time/rate"

	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

const (
	jobsApiVersion20 = "2.0"
	jobsApiVersion21 = "2.1"
)

const MultiTaskKey = "MULTI_TASK"
const DefaultTaskSource = "WORKSPACE"

const (
	TaskKeyMaxLength       = 100
	JobClusterKeyMaxLength = 100
	WorkspaceJobsLimit     = 10000
	ApiLimit               = 25
	ListJobsApiPageLimit   = 100
	DefaultJobClusterKey   = "default"
)

const TimezoneIdGMT = "GMT"

const PauseStatusUnpaused = "UNPAUSED"
const PauseStatusPaused = "PAUSED"

type SparkJarTask struct {
	MainClassName string   `json:"main_class_name"`
	Parameters    []string `json:"parameters"`
}

type SparkPythonTask struct {
	PythonFile string   `json:"python_file"`
	Parameters []string `json:"parameters"`
}

type NotebookTask struct {
	NotebookPath        string            `json:"notebook_path"`
	BaseParameters      map[string]string `json:"base_parameters,omitempty"`
	Source              string            `json:"source,omitempty"`
	UnityCatalogEnabled bool              `json:"unity_catalog_enabled,omitempty"`
}

type SparkSubmitTask struct {
	Parameters []string `json:"parameters,omitempty"`
}

type PythonWheelTask struct {
	EntryPoint      string            `json:"entry_point,omitempty"`
	PackageName     string            `json:"package_name,omitempty"`
	Parameters      []string          `json:"parameters,omitempty"`
	NamedParameters map[string]string `json:"named_parameters,omitempty"`
}

type PipelineTask struct {
	PipelineID string `json:"pipeline_id"`
}

type SqlTaskSubscriptions struct {
	UserName      string `json:"user_name,omitempty"`
	DestinationId string `json:"destination_id,omitempty"`
}

type SqlTaskQuery struct {
	QueryId string `json:"query_id,omitempty"`
}

type SqlTaskDashboard struct {
	DashboardId   string                  `json:"dashboard_id,omitempty"`
	Subscriptions []*SqlTaskSubscriptions `json:"subscriptions,omitempty"`
	CustomSubject string                  `json:"custom_subject,omitempty"`
}

type SqlTaskAlert struct {
	AlertId       string                  `json:"alert_id,omitempty"`
	Subscriptions []*SqlTaskSubscriptions `json:"subscriptions,omitempty"`
}

type SqlTask struct {
	Query       *SqlTaskQuery     `json:"query,omitempty"`
	Dashboard   *SqlTaskDashboard `json:"dashboard,omitempty"`
	Alert       *SqlTaskAlert     `json:"alert,omitempty"`
	Parameters  map[string]string `json:"parameters,omitempty"`
	WarehouseId string            `json:"warehouse_id,omitempty"`
}

type DbtTask struct {
	ProjectDirectory  string   `json:"project_directory,omitempty"`
	Commands          []string `json:"commands,omitempty"`
	Schema            string   `json:"schema,omitempty"`
	WarehouseId       string   `json:"warehouse_id,omitempty"`
	Catalog           string   `json:"catalog,omitempty"`
	ProfilesDirectory string   `json:"profiles_directory,omitempty"`
}

// EmailNotifications maps to https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobsjobsettingsjobemailnotifications
type EmailNotifications struct {
	OnStart               []string `json:"on_start,omitempty"`
	OnSuccess             []string `json:"on_success,omitempty"`
	OnFailure             []string `json:"on_failure,omitempty"`
	NoAlertForSkippedRuns bool     `json:"no_alert_for_skipped_runs,omitempty"`
}

func (n *EmailNotifications) IsEmpty() bool {
	return len(n.OnStart) == 0 && len(n.OnSuccess) == 0 && len(n.OnFailure) == 0
}

type CronSchedule struct {
	QuartzCronExpression string `json:"quartz_cron_expression"`
	TimezoneId           string `json:"timezone_id"`
	PauseStatus          string `json:"pause_status"`
}

type WebhookNotificationDestination struct {
	Id string `json:"id,omitmempty"`
}

type WebhookNotifications struct {
	OnStart   []*WebhookNotificationDestination `json:"on_start,omitempty"`
	OnSuccess []*WebhookNotificationDestination `json:"on_success,omitempty"`
	OnFailure []*WebhookNotificationDestination `json:"on_failure,omitempty"`
}

func (n *WebhookNotifications) IsEmpty() bool {
	return len(n.OnStart) == 0 && len(n.OnSuccess) == 0 && len(n.OnFailure) == 0
}

type GitSnapshot struct {
	UsedCommit string `json:"used_commit,omitempty"`
}

type GitSource struct {
	GitUrl      string       `json:"git_url,omitempty"`
	GitProvider string       `json:"git_provider,omitempty"`
	GitBranch   string       `json:"git_branch,omitempty"`
	GitCommit   string       `json:"git_commit,omitempty"`
	GitTag      string       `json:"git_tag,omitempty"`
	GitSnapshot *GitSnapshot `json:"git_snaptshot,omitempty"`
}

// NewCluster maps the available fields https://docs.databricks.com/dev-tools/api/latest/jobs.html#newcluster
// NOTE: this is slightly different from the general `ClusterSpec` struct defined in clusters.go,
//
//	namely you cannot specify a cluster name or auto termination minutes.
type NewCluster struct {
	NumWorkers int               `json:"num_workers,omitempty"`
	AutoScale  *ClusterAutoScale `json:"autoscale,omitempty"`

	SparkVersion         sparkversion.SparkVersion `json:"spark_version"`
	SparkConf            map[string]string         `json:"spark_conf,omitempty"`
	SparkEnvVars         map[string]string         `json:"spark_env_vars,omitempty"`
	AwsAttributes        *ClusterAwsAttributes     `json:"aws_attributes,omitempty"`
	InitScripts          []*InitScript             `json:"init_scripts,omitempty"`
	NodeTypeId           string                    `json:"node_type_id,omitempty"`
	DriverNodeTypeId     string                    `json:"driver_node_type_id,omitempty"`
	ClusterLogConf       *ClusterLogConf           `json:"cluster_log_conf,omitempty"`
	EnableElasticDisk    bool                      `json:"enable_elastic_disk,omitempty"`
	InstancePoolId       string                    `json:"instance_pool_id,omitempty"`
	DriverInstancePoolId string                    `json:"driver_instance_pool_id,omitempty"`
	CustomTags           map[string]string         `json:"custom_tags,omitempty"`
	DataSecurityMode     string                    `json:"data_security_mode,omitempty"`
}

type TaskDependency struct {
	TaskKey string `json:"task_key,omitempty"`
}
type JobTaskSettings struct {
	TaskKey                      string              `json:"task_key"`
	Description                  string              `json:"description,omitempty"`
	DependsOn                    []*TaskDependency   `json:"depends_on,omitempty"`
	ExistingClusterId            string              `json:"existing_cluster_id,omitempty"`
	NewCluster                   *ClusterSpec        `json:"new_cluster,omitempty"`
	JobClusterKey                string              `json:"job_cluster_key,omitempty"`
	NotebookTask                 *NotebookTask       `json:"notebook_task,omitempty"`
	SparkJarTask                 *SparkJarTask       `json:"spark_jar_task,omitempty"`
	SparkPythonTask              *SparkPythonTask    `json:"spark_python_task,omitempty"`
	SparkSubmitTask              *SparkSubmitTask    `json:"spark_submit_task,omitempty"`
	PipelineTask                 *PipelineTask       `json:"pipeline_task,omitempty"`
	PythonWheelTask              *PythonWheelTask    `json:"python_wheel_task,omitempty"`
	DbtTask                      *DbtTask            `json:"dbt_task,omitempty"`
	SqlTask                      *SqlTask            `json:"sql_task,omitempty"`
	Libraries                    []*Library          `json:"libraries,omitempty"`
	EmailNotifications           *EmailNotifications `json:"email_notifications,omitempty"`
	TimeoutSeconds               int                 `json:"timeout_seconds,omitempty"`
	MaxRetries                   int                 `json:"max_retries,omitempty"`
	MinRetryIntervalMilliseconds int                 `json:"min_retry_interval_millis,omitempty"`
	RetryOnTimeout               bool                `json:"retry_on_timeout,omitempty"`
	EnvironmentKey               string              `json:"environment_key,omitempty"`
}

type JobCluster struct {
	JobClusterKey string       `json:"job_cluster_key,omitempty"`
	NewCluster    *ClusterSpec `json:"new_cluster,omitempty"`
}

type JobQueue struct {
	Enabled bool `json:"enabled,omitempty"`
}

type RunAsSetting struct {
	UserName             string `json:"user_name,omitempty"`
	ServicePrincipalName string `json:"service_principal_name,omitempty"`
}

type JobSettings struct {
	Name               string              `json:"name,omitempty"`
	EmailNotifications *EmailNotifications `json:"email_notifications,omitempty"`
	TimeoutSeconds     int                 `json:"timeout_seconds,omitempty"`
	Schedule           *CronSchedule       `json:"schedule,omitempty"`
	Format             string              `json:"format,omitempty"` // Determines which JobSettings format is used (see https://docs.databricks.com/data-engineering/jobs/jobs-api-updates.html#api-changes)
	// JobsAPI 2.0
	ExistingClusterId            string           `json:"existing_cluster_id,omitempty"`
	NewCluster                   *ClusterSpec     `json:"new_cluster,omitempty"`
	SparkJarTask                 *SparkJarTask    `json:"spark_jar_task,omitempty"`
	SparkPythonTask              *SparkPythonTask `json:"spark_python_task,omitempty"`
	NotebookTask                 *NotebookTask    `json:"notebook_task,omitempty"`
	Libraries                    []*Library       `json:"libraries,omitempty"`
	MaxRetries                   int              `json:"max_retries,omitempty"`
	RetryOnTimeout               bool             `json:"retry_on_timeout,omitempty"`
	MinRetryIntervalMilliseconds int              `json:"min_retry_interval_millis,omitempty"`
	MaxConcurrentRuns            int              `json:"max_concurrent_runs,omitempty"`
	// JobsAPI 2.1
	Tasks                []*JobTaskSettings       `json:"tasks,omitempty"`
	JobClusters          []*JobCluster            `json:"job_clusters,omitempty"`
	Tags                 map[string]string        `json:"tags,omitempty"`
	GitSource            *GitSource               `json:"git_source,omitempty"`
	WebhookNotifications *WebhookNotifications    `json:"webhook_notifications,omitempty"`
	Queue                *JobQueue                `json:"queue,omitempty"`
	RunAs                *RunAsSetting            `json:"run_as,omitempty"`
	Environments         []*ServerlessEnvironment `json:"environments,omitempty"`
	PerformanceTarget    string                   `json:"performance_target,omitempty"`
	BudgetPolicyId       string                   `json:"budget_policy_id,omitempty"`
}

// CreateJobInput maps to https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
type CreateJobInput struct {
	JobSettings
}

// CreateJobOutput maps to https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
type CreateJobOutput struct {
	JobId     int64  `json:"job_id,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type RunNowInput struct {
	JobId             int64             `json:"job_id"`
	JarParams         []string          `json:"jar_params,omitempty"`
	NotebookParams    map[string]string `json:"notebook_params,omitempty"`
	PythonParams      []string          `json:"python_params,omitempty"`
	SparkSubmitParams []string          `json:"spark_submit_params,omitempty"`
	IdempotencyToken  string            `json:"idempotency_token,omitempty"`
	PythonNamedParams map[string]string `json:"python_named_parameters,omitempty"`
	SqlParams         map[string]string `json:"sql_params,omitempty"`
	DbtCommands       []string          `json:"dbt_commands,omitempty"`
}

type RunNowOutput struct {
	RunId       int64  `json:"run_id,omitempty"`
	NumberInJob int64  `json:"number_in_job,omitempty"` // DEPRECATED
	ErrorCode   string `json:"error_code,omitempty"`
	Message     string `json:"message,omitempty"`
}

type ListJobsInput struct {
	Limit       int    `json:"limit,omitempty"`
	Offset      int    `json:"offset,omitempty"`
	Name        string `json:"name,omitempty"`
	ExpandTasks bool   `json:"expand_tasks,omitempty"`
	PageToken   string `json:"page_token,omitempty"`
}

type Job struct {
	JobId           int64       `json:"job_id,omitempty"`
	JobSettings     JobSettings `json:"settings,omitempty"`
	CreatorUserName string      `json:"creator_user_name,omitempty"`
	CreatedTime     int64       `json:"created_time,omitempty"`
}

type ListJobsOutput struct {
	Jobs          []Job  `json:"jobs,omitempty"`
	HasMore       bool   `json:"has_more,omitempty"`
	NextPageToken string `json:"next_page_token,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	Message       string `json:"message,omitempty"`
}

type ResetJobInput struct {
	JobId       int64       `json:"job_id"`
	NewSettings JobSettings `json:"new_settings"`
}

type ResetJobOutput struct {
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type GetJobInput struct {
	JobId int64 `json:"job_id"`
}

type GetJobOutput struct {
	Job
	RunAsUserName string `json:"run_as_user_name,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	Message       string `json:"message,omitempty"`
}

type DeleteJobInput struct {
	JobId int64 `json:"job_id"`
}

type DeleteJobOutput struct {
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type ListRunsInput struct {
	JobId         int64  `json:"job_id,omitempty"`
	ActiveOnly    bool   `json:"active_only,omitempty"`
	CompletedOnly bool   `json:"completed_only,omitempty"`
	Offset        int    `json:"offset,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	RunType       string `json:"run_type,omitempty"`
	ExpandTasks   bool   `json:"expand_tasks,omitempty"`
	StartTimeFrom int    `json:"start_time_from,omitempty"`
	StartTimeTo   int    `json:"start_time_to,omitempty"`
	PageToken     string `json:"page_token,omitempty"`
}

type CancelAllJobRunsInput struct {
	JobId int64 `json:"job_id"`
}

type CancelAllJobRunsOutput struct {
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type NewJobSettings struct {
	Schedule *CronSchedule `json:"schedule,omitempty"`
}

type UpdateJobInput struct {
	JobId       int64          `json:"job_id"`
	NewSettings NewJobSettings `json:"new_settings,omitempty"`
}

type UpdateJobOutput struct {
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type RunLifeCycleState string

const (
	RunLifeCycleStatePending         = RunLifeCycleState("PENDING")
	RunLifeCycleStateRunning         = RunLifeCycleState("RUNNING")
	RunLifeCycleStateTerminating     = RunLifeCycleState("TERMINATING")
	RunLifeCycleStateTerminated      = RunLifeCycleState("TERMINATED")
	RunLifeCycleStateSkipped         = RunLifeCycleState("SKIPPED")
	RunLifeCycleStateInternalError   = RunLifeCycleState("INTERNAL_ERROR")
	RunLifeCycleStateBlocked         = RunLifeCycleState("BLOCKED")
	RunLifeCycleStateWaitingForRetry = RunLifeCycleState("WAITING_FOR_RETRY")
)

type RunResultState string

const (
	RunResultStateSuccess             = RunResultState("SUCCESS")
	RunResultStateFailed              = RunResultState("FAILED")
	RunResultStateTimedout            = RunResultState("TIMEDOUT")
	RunResultStateCanceled            = RunResultState("CANCELED")
	RunResultStateSuccessWithFailures = RunResultState("SUCCESS_WITH_FAILURES")
)

type TriggerType string

const (
	TriggerTypePeriodic = TriggerType("PERIODIC")
	TriggerTypeOneTime  = TriggerType("ONE_TIME")
	TriggerTypeRetry    = TriggerType("RETRY")
)

type RunType string

const (
	RunTypeJobRun      = RunType("JOB_RUN")
	RunTypeWorkflowRun = RunType("WORKFLOW_RUN")
	RunTypeSubmitRun   = RunType("SUBMIT_RUN")
)

type RepairType string

const (
	RepairTypeOriginal = RepairType("ORIGINAL")
	RepairTypeRepair   = RepairType("REPAIR")
)

type SqlDashboardWidgetStatus string

const (
	SqlDashboardWidgetStatusPending  = SqlDashboardWidgetStatus("PENDING")
	SqlDashboardWidgetStatusRunning  = SqlDashboardWidgetStatus("RUNNING")
	SqlDashboardWidgetStatusSuccess  = SqlDashboardWidgetStatus("SUCCESS")
	SqlDashboardWidgetStatusFailed   = SqlDashboardWidgetStatus("FAILED")
	SqlDashboardWidgetStatusCanelled = SqlDashboardWidgetStatus("CANCELLED")
)

type RunState struct {
	LifeCycleState        RunLifeCycleState `json:"life_cycle_state"`
	ResultState           *RunResultState   `json:"result_state,omitempty"`
	StateMessage          string            `json:"state_message"`
	UserCancelledTimedOut bool              `json:"user_cancelled_or_timedout,omitempty"`
}

type ClusterInstance struct {
	ClusterId      string `json:"cluster_id"`
	SparkContextId string `json:"spark_context_id"`
}

type RunClusterSpec struct {
	ExistingClusterId string       `json:"existing_cluster_id"`
	NewCluster        *ClusterSpec `json:"new_cluster"`
	Libraries         []*Library   `json:"libraries,omitempty"`
}

type OverridingParameters struct {
	JarParams         []string          `json:"jar_params,omitempty"`
	NotebookParams    map[string]string `json:"notebook_params,omitempty"`
	PythonParams      []string          `json:"python_params,omitempty"`
	SparkSubmitParams []string          `json:"spark_submit_params,omitempty"`
	PythonNamedParams map[string]string `json:"python_named_parameters,omitempty"`
	SqlParams         map[string]string `json:"sql_params,omitempty"`
	DbtCommands       []string          `json:"dbt_commands,omitempty"`
}

type RepairHistoryItem struct {
	Id         int64      `json:"id,omitempty"`
	Type       RepairType `json:"type,omitempty"`
	StartTime  int64      `json:"start_time,omitempty"`
	EndTime    int64      `json:"end_time,omitempty"`
	State      RunState   `json:"state,omitempty"`
	TaskRunIds []int64    `json:"task_run_ids,omitempty"`
}

type RunTask struct {
	RunId             int64             `json:"run_id,omitempty"`
	TaskKey           string            `json:"task_key,omitempty"`
	Description       string            `json:"description,omitempty"`
	State             RunState          `json:"state,omitempty"`
	DependsOn         []*TaskDependency `json:"depends_on,omitempty"`
	ExistingClusterId string            `json:"existing_cluster_id,omitempty"`
	NewCluster        *ClusterSpec      `json:"new_cluster,omitempty"`
	JobClusterKey     string            `json:"job_cluster_key,omitempty"`
	Libraries         []*Library        `json:"libraries,omitempty"`
	NotebookTask      *NotebookTask     `json:"notebook_task,omitempty"`
	SparkJarTask      *SparkJarTask     `json:"spark_jar_task,omitempty"`
	SparkPythonTask   *SparkPythonTask  `json:"spark_python_task,omitempty"`
	SparkSubmitTask   *SparkSubmitTask  `json:"spark_submit_task,omitempty"`
	PipelineTask      *PipelineTask     `json:"pipeline_task,omitempty"`
	PythonWheelTask   *PythonWheelTask  `json:"python_wheel_task,omitempty"`
	SqlTask           *SqlTask          `json:"sql_task,omitempty"`
	DbtTask           *DbtTask          `json:"dbt_task,omitempty"`
	StartTime         int64             `json:"start_time,omitempty"`
	SetupDuration     int64             `json:"setup_duration,omitempty"`
	ExecutionDuration int64             `json:"execution_duration,omitempty"`
	CleanupDuration   int64             `json:"cleanup_duration,omitempty"`
	EndTime           int64             `json:"end_time,omitempty"`
	AttemptNumber     int               `json:"attempt_number,omitempty"`
	ClusterInstance   *ClusterInstance  `json:"cluster_instance,omitempty"`
	GitSource         *GitSource        `json:"git_source,omitempty"`
	RunPageUrl        string            `json:"run_page_url,omitempty"`
}

type Run struct {
	JobId                int64                `json:"job_id,omitempty"`
	RunId                int64                `json:"run_id,omitempty"`
	RunName              string               `json:"run_name,omitempty"`
	NumberInJob          int64                `json:"number_in_job,omitempty"` // DEPRECATED
	CreatorUserName      string               `json:"creator_user_name,omitempty"`
	OriginalAttemptRunId int64                `json:"original_attempt_run_id,omitempty"`
	State                RunState             `json:"state,omitempty"`
	Schedule             CronSchedule         `json:"schedule,omitempty"`
	StartTime            int64                `json:"start_time,omitempty"`
	EndTime              int64                `json:"end_time,omitempty"`
	SetupDuration        int64                `json:"setup_duration,omitempty"`
	ExecutionDuration    int64                `json:"execution_duration,omitempty"`
	CleanupDuration      int64                `json:"cleanup_duration,omitempty"`
	RunDuration          int                  `json:"run_duration,omitempty"`
	Trigger              TriggerType          `json:"trigger,omitempty"`
	RunPageUrl           string               `json:"run_page_url,omitempty"`
	RunType              RunType              `json:"run_type,omitempty"`
	ClusterSpec          RunClusterSpec       `json:"cluster_spec,omitempty"`
	ClusterInstance      *ClusterInstance     `json:"cluster_instance"`
	OverridingParameters OverridingParameters `json:"overriding_parameters,omitempty"`
	AttemptNumber        int                  `json:"attempt_number,omitempty"`
	Tasks                []*RunTask           `json:"tasks,omitempty"`
	JobClusters          []*JobCluster        `json:"job_clusters,omitempty"`
	GitSource            *GitSource           `json:"git_source,omitempty"`
	RepairHistory        []*RepairHistoryItem `json:"repair_history,omitempty"`
}

type ListRunsOutput struct {
	Runs          []*Run `json:"runs,omitempty"`
	HasMore       bool   `json:"has_more,omitempty"`
	NextPageToken string `json:"next_page_token,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	Message       string `json:"message,omitempty"`
}

// https://docs.databricks.com/api/workspace/jobs/submit
type SubmitRunInput struct {
	// RunName is technically optional but we'll make it required.
	RunName          string  `json:"run_name"`
	TimeoutSeconds   *int    `json:"timeout_seconds,omitempty"`
	IdempotencyToken *string `json:"idempotency_token,omitempty"`
	// JobsAPI 2.0
	// Must provide exactly one of these fields.
	ExistingClusterId *string     `json:"existing_cluster_id,omitempty"`
	NewCluster        *NewCluster `json:"new_cluster,omitempty"`
	// Must provide exactly one of these fields.
	SparkJarTask         *SparkJarTask         `json:"spark_jar_task,omitempty"`
	SparkPythonTask      *SparkPythonTask      `json:"spark_python_task,omitempty"`
	NotebookTask         *NotebookTask         `json:"notebook_task,omitempty"`
	Libraries            []*Library            `json:"libraries,omitempty"`
	WebhookNotifications *WebhookNotifications `json:"webhook_notifications,omitempty"`
	// JobsAPI 2.1
	Tasks     []*JobTaskSettings `json:"tasks,omitempty"`
	GitSource *GitSource         `json:"git_source,omitempty"`
}

type SubmitRunOutput struct {
	RunId     int64  `json:"run_id,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
type GetRunInput struct {
	RunId          int64 `json:"run_id"`
	IncludeHistory bool  `json:"include_history,omitempty"`
}

type GetRunOutput struct {
	Run
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

type RunNotebookOutput struct {
	Result    string `json:"result,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type SqlStatementsOutput struct {
	LookupKey string `json:"lookup_key,omitempty"`
}

type SqlQueryOutput struct {
	QueryText     string              `json:"query_text,omitempty"`
	WarehouseId   string              `json:"warehouse_id,omitempty"`
	SqlStatements SqlStatementsOutput `json:"sql_statements,omitempty"`
	OutputLink    string              `json:"output_link,omitempty"`
}

type SqlOutputError struct {
	Message string `json:"message,omitempty"`
}

type SqlDashboardWidgetOutput struct {
	WidgetId    string                   `json:"widget_id,omitempty"`
	WidgetTitle string                   `json:"widget_title,omitempty"`
	OutputLink  string                   `json:"output_string,omitempty"`
	Status      SqlDashboardWidgetStatus `json:"status,omitempty"`
	Error       SqlOutputError           `json:"error,omitempty"`
	StartTime   int64                    `json:"start_time,omitempty"`
	EndTime     int64                    `json:"end_time,omitempty"`
}

type SqlDashboardOutput struct {
	Widgets SqlDashboardWidgetOutput `json:"widgets,omitempty"`
}

type SqlAlertOutput struct {
	SqlQueryOutput
}

type RunSqlOutput struct {
	QueryOutput     SqlQueryOutput     `json:"query_output,omitempty"`
	DashboardOutput SqlDashboardOutput `json:"dashboard_output,omitempty"`
	AlertOutput     SqlAlertOutput     `json:"alert_output,omitempty"`
}

type RunDbtOutput struct {
	ArtifactsLink   string            `json:"artifacts_link,omitempty"`
	ArtifactHeaders map[string]string `json:"artifacts_headers,omitempty"`
}

type GetRunOutputInput struct {
	RunId int64 `json:"run_id"`
}

type GetRunOutputOutput struct {
	Metadata       Run               `json:"metadata"`
	Error          string            `json:"error,omitempty"`
	NotebookOutput RunNotebookOutput `json:"notebook_output,omitempty"`
	SqlOutput      RunSqlOutput      `json:"sql_output,omitempty"`
	DbtOutput      RunDbtOutput      `json:"dbt_output,omitempty"`
	Logs           string            `json:"logs,omitempty"`
	ErrorTrace     string            `json:"error_trace,omitempty"`
	LogsTruncated  bool              `json:"logs_truncated,omitempty"`
	ErrorCode      string            `json:"error_code,omitempty"`
	Message        string            `json:"message,omitempty"`
}

type RepairRunInput struct {
	RunId               int               `json:"run_id"`
	RerunTasks          []string          `json:"rerun_tasks"`                      // List of task_keys. Conflicts with 'rerun_all_failed_tasks'
	RerunAllFailedTasks bool              `json:"rerun_all_failed_tasks,omitempty"` // Conflicts with 'rerun_tasks'
	LatestRepairId      int               `json:"last_repair_id,omitempty"`
	JarParams           []string          `json:"jar_params,omitempty"`
	NotebookParams      map[string]string `json:"notebook_params,omitempty"`
	PythonParams        []string          `json:"python_params,omitempty"`
	SparkSubmitParams   []string          `json:"spark_submit_params,omitempty"`
	PythonNamedParams   map[string]string `json:"python_named_parameters,omitempty"`
	SqlParams           map[string]string `json:"sql_params,omitempty"`
	DbtCommands         []string          `json:"dbt_commands,omitempty"`
}

type RepairRunOutput struct {
	RepairId  int    `json:"repair_id,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

func AdaptMultiTaskJobSettings(settings JobSettings) JobSettings {
	singleTaskJobSettings := JobSettings{
		Name:                         settings.Name,
		EmailNotifications:           settings.EmailNotifications,
		Schedule:                     settings.Schedule,
		MaxConcurrentRuns:            settings.MaxConcurrentRuns,
		TimeoutSeconds:               settings.Tasks[0].TimeoutSeconds,
		ExistingClusterId:            settings.Tasks[0].ExistingClusterId,
		NewCluster:                   settings.Tasks[0].NewCluster,
		SparkJarTask:                 settings.Tasks[0].SparkJarTask,
		SparkPythonTask:              settings.Tasks[0].SparkPythonTask,
		NotebookTask:                 settings.Tasks[0].NotebookTask,
		Libraries:                    settings.Tasks[0].Libraries,
		MaxRetries:                   settings.Tasks[0].MaxRetries,
		RetryOnTimeout:               settings.Tasks[0].RetryOnTimeout,
		MinRetryIntervalMilliseconds: settings.Tasks[0].MinRetryIntervalMilliseconds,
	}
	return singleTaskJobSettings
}

type JobsAPI interface {
	CreateJob(context.Context, *CreateJobInput) (*CreateJobOutput, error)
	RunNow(context.Context, *RunNowInput) (*RunNowOutput, error)
	ListJobs(context.Context, *ListJobsInput) (*ListJobsOutput, error)
	ResetJob(context.Context, *ResetJobInput) (*ResetJobOutput, error)
	GetJob(context.Context, *GetJobInput) (*GetJobOutput, error)
	DeleteJob(context.Context, *DeleteJobInput) (*DeleteJobOutput, error)
	SubmitRun(context.Context, *SubmitRunInput) (*SubmitRunOutput, error)
	ListRuns(context.Context, *ListRunsInput) (*ListRunsOutput, error)
	GetRun(context.Context, *GetRunInput) (*GetRunOutput, error)
	GetRunOutput(context.Context, *GetRunOutputInput) (*GetRunOutputOutput, error)
	RepairRun(context.Context, *RepairRunInput) (*RepairRunOutput, error)
	CancelAllJobRuns(ctx context.Context, input *CancelAllJobRunsInput) (*CancelAllJobRunsOutput, error)
	UpdateJob(ctx context.Context, input *UpdateJobInput) (*UpdateJobOutput, error)
}

func (c *Client) CreateJob(ctx context.Context, input *CreateJobInput) (*CreateJobOutput, error) {
	var output CreateJobOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/create", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) RunNow(ctx context.Context, input *RunNowInput) (*RunNowOutput, error) {
	var output RunNowOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/run-now", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) CancelAllJobRuns(ctx context.Context, input *CancelAllJobRunsInput) (*CancelAllJobRunsOutput, error) {
	var output CancelAllJobRunsOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/runs/cancel-all", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) UpdateJob(ctx context.Context, input *UpdateJobInput) (*UpdateJobOutput, error) {
	var output UpdateJobOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/update", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) ListJobs(ctx context.Context, input *ListJobsInput) (*ListJobsOutput, error) {
	var output ListJobsOutput
	var jobs []Job

	// Databricks' '/2.1/jobs/list' endpoint has a maximum of 100 for the Limit property. Pagination is implemented internally to abstract
	// this limit away from clients so more than 100 objects can be returned in a single invocation.
	requestInput := *input
	requestInput.Limit = int(ListJobsApiPageLimit)
	hasMore := true
	hasReachedLimit := false
	returnAllJobs := input.Limit == 0
	for hasMore && !hasReachedLimit {
		if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/list", requestInput, &output); err != nil {
			return nil, oops.Wrapf(err, "")
		}
		jobs = append(jobs, output.Jobs...)
		// NextPageToken is an empty string when there are no more jobs left to retrieve.
		hasMore = output.HasMore || output.NextPageToken != ""
		requestInput.PageToken = output.NextPageToken
		if !returnAllJobs { // if the caller wants to return all jobs, we don't exit the for loop early
			hasReachedLimit = len(jobs) >= input.Limit
		}
		output = ListJobsOutput{}
	}

	if returnAllJobs || !hasReachedLimit {
		output.Jobs = jobs
	} else {
		output.Jobs = jobs[:input.Limit]
	}

	return &output, nil
}

func (c *Client) ResetJob(ctx context.Context, input *ResetJobInput) (*ResetJobOutput, error) {
	var output ResetJobOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/reset", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetJob(ctx context.Context, input *GetJobInput) (*GetJobOutput, error) {
	var output GetJobOutput

	if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/get", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &output, nil
}

func (c *Client) DeleteJob(ctx context.Context, input *DeleteJobInput) (*DeleteJobOutput, error) {
	var output DeleteJobOutput

	if err := c.do(ctx, http.MethodPost, "/api/2.1/jobs/delete", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) SubmitRun(ctx context.Context, input *SubmitRunInput) (*SubmitRunOutput, error) {
	var output SubmitRunOutput
	version := jobsApiVersion20
	if input.Tasks != nil && len(input.Tasks) > 0 {
		version = jobsApiVersion21
	} else {
		clusters := 0
		if input.ExistingClusterId != nil {
			clusters += 1
		}
		if input.NewCluster != nil {
			clusters += 1
		}
		if clusters != 1 {
			return nil, oops.Errorf("Must define exactly 1 of ExistingClusterId or NewCluster.")
		}

		jobs := 0
		if input.NotebookTask != nil {
			jobs += 1
		}
		if input.SparkJarTask != nil {
			jobs += 1
		}
		if input.SparkPythonTask != nil {
			jobs += 1
		}
		if jobs != 1 {
			return nil, oops.Errorf("Must define exactly 1 of SparkJarTask, SparkPythonTask, or NotebookTask.")
		}
	}

	if err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/%s/jobs/runs/submit", version), input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return &output, nil
}

func (c *Client) ListRuns(ctx context.Context, input *ListRunsInput) (*ListRunsOutput, error) {
	var output ListRunsOutput
	var runs []*Run

	// Databricks' '/2.1/jobs/runs/list' endpoint has a maximum of 25 for the Limit property. Pagination is implemented internally to abstract
	// this limit away from clients so more than 25 objects can be returned in a single invocation.
	requestInput := *input
	requestInput.Limit = int(ApiLimit)
	hasMore := true
	hasReachedLimit := false
	returnAllJobs := input.Limit == 0
	for hasMore && !hasReachedLimit {
		if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/runs/list", requestInput, &output); err != nil {
			return nil, oops.Wrapf(err, "")
		}
		runs = append(runs, output.Runs...)
		// NextPageToken is an empty string when there are no more runs left to retrieve.
		hasMore = output.HasMore || output.NextPageToken != ""
		requestInput.PageToken = output.NextPageToken

		if !returnAllJobs { // if the caller wants to return all jobs, we don't exit the for loop early
			hasReachedLimit = len(runs) >= input.Limit
		}
		output = ListRunsOutput{}
	}
	if returnAllJobs || !hasReachedLimit {
		output.Runs = runs
	} else {
		output.Runs = runs[:input.Limit]
	}

	return &output, nil
}

func (c *Client) GetRun(ctx context.Context, input *GetRunInput) (*GetRunOutput, error) {
	var output GetRunOutput

	// Databricks JobsAPI has a rate limit of 100 reqs/s for the /jobs/runs/get endpoint (default: 30 reqs/s)
	// Setting this to just under the maximum limit of 100 to increase client's request rate
	// https://docs.databricks.com/resources/limits.html#api-rate-limits
	c.rateLimiter = rate.NewLimiter(90, 20)

	if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/runs/get", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) GetRunOutput(ctx context.Context, input *GetRunOutputInput) (*GetRunOutputOutput, error) {
	var output GetRunOutputOutput

	if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/runs/get-output", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}

func (c *Client) RepairRun(ctx context.Context, input *RepairRunInput) (*RepairRunOutput, error) {
	var output RepairRunOutput
	if err := c.do(ctx, http.MethodGet, "/api/2.1/jobs/runs/repair", input, &output); err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &output, nil
}
