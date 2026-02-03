package dataplatformresource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/customdatabricksimages"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var RegionLabels = map[string]string{
	infraconsts.SamsaraAWSDefaultRegion: "us",
	infraconsts.SamsaraAWSEURegion:      "eu",
	infraconsts.SamsaraAWSCARegion:      "ca",
}

// Compute a random number between startInclusive and endExclusive, using the
// provided string as a hash.
func RandomFromNameHash(name string, startInclusive, endExclusive uint32) uint32 {
	diff := endExclusive - startInclusive
	return (Hash(name) % diff) + startInclusive
}

// JobNameCronDay takes the name for a job and provides a random day from 1-7
func JobNameCronDay(name string) uint32 {
	return Hash(name)%7 + 1
}

// NameAZ takes the name of a cluster, job, or pool and assigns it to an AZ
func NameAZ(name string, region string) string {
	allAz := infraconsts.GetDatabricksAvailabilityZones(region)
	index := int(Hash(name)) % len(allAz)
	return allAz[index]
}

type JobLibraryConfig struct {
	LoadDefaultLibraries   bool
	DefaultLibrariesToSkip []string
	Mavens                 []MavenName
	Jars                   []JarName
	PyPIs                  []PyPIName
}

type UnityCatalogSetting struct {
	DataSecurityMode string
	SingleUserName   string
}

type JobSpec struct {
	Name               string
	Region             string
	Tags               map[string]string
	Script             *awsresource.S3BucketObject
	WorkspacePath      string
	Parameters         []string
	NotebookPath       string
	ParameterMap       map[string]string
	Cron               string
	Pool               string
	DriverPool         string
	MinWorkers         int
	MaxWorkers         int
	MaxRetries         int
	MinRetryInterval   time.Duration
	TimeoutSeconds     int
	EmailNotifications []string
	Profile            string
	SparkVersion       sparkversion.SparkVersion
	SparkConf          map[string]string
	SparkEnvVars       map[string]string
	DriverNodeType     string
	WorkerNodeType     string

	// If set, will run the job in a single node configuration. This can be useful for cost savings
	// for jobs that don't do much / don't do much parallelizable work.
	// https://docs.databricks.com/clusters/single-node.html
	SingleNodeJob bool
	Owner         components.TeamInfo

	// BudgetingTeamOverride specifies to team who owns this service's running cost.
	BudgetingTeamOverride        components.TeamInfo
	JobOwnerUser                 string
	JobOwnerServicePrincipalName string
	AdminTeams                   []components.TeamInfo
	VolumesInitScripts           []string
	Libraries                    JobLibraryConfig

	// RnDCostAllocation specifies percent (0 to 1) of the job's cost to be allocated
	// to R&D. By default jobs are allocated to COGS.
	RnDCostAllocation float64

	// IsProduction should be true when this is a production job.
	IsProduction bool

	// Set the JobType so that we tag the job with the right tag to let us break down
	// cost and usage by different job types / features we support.
	JobType dataplatformconsts.DataPlatformJobType

	// CustomRepoName refers to the name of the custom docker image the cluster will use
	// This allows us to add features on top of Databricks' base images: https://docs.databricks.com/clusters/custom-containers.html
	// The current allowed types are:
	// customdatabrickscontainers.Standard: Default image built on Databricks DBR version 7.x. Provides access to all python repo code within databricks
	CustomRepoName customdatabricksimages.ECRRepoName

	// The following settings are to configure a Job to use Databricks' multi-task job format, supported by the JobsAPI >= 2.0.
	// Format is an enum ['SINGLE_TASK' | 'MULTI_TASK'] used to control which format (and api version) will be used by terraform. Igorning Format will default to JobAPI 2.0 and SINGLE_TASK format.
	// The settings above will be used in the mutli-task format as follows:
	// - All cluster settings will be used to create a default JobCluster
	// -
	Format               string
	Tasks                []*databricks.JobTaskSettings
	JobClusters          []*databricks.JobCluster
	JobTags              map[string]string
	GitSource            *databricks.GitSource
	WebhookNotifications *databricks.WebhookNotifications

	// If this field is set, we will add various tags to the databricks job that reflect features
	// about its SL0 and monitoring.
	SloConfig *dataplatformconsts.JobSlo

	// Unity Catalog Settings
	// The default is not enabled
	UnityCatalogSetting UnityCatalogSetting

	// DisableFileModificationCheck is a flag to disable the file modification check in the spark configuration
	DisableFileModificationCheck bool

	// Queue specifies whether the job should be queued if there is already a job running.
	Queue *databricks.JobQueue

	// RunAs specifies the user to run the job as.
	RunAs *databricks.RunAsSetting

	// UseOnDemandCluster specifies whether the job should be run on an on-demand cluster.
	UseOnDemandCluster bool

	ServerlessConfig *databricks.ServerlessConfig
}

func (spec *JobSpec) getServerlessTags() []databricks.ServerlessJobTags {

	tagPrefix := "samsara:"
	tagTeamName := spec.Owner.Name()
	// If BudgetingTeamOverride we use it as tag for the name and to find the product group tag.
	if spec.BudgetingTeamOverride.TeamName != "" {
		tagTeamName = spec.BudgetingTeamOverride.Name()
	}

	tags := []databricks.ServerlessJobTags{
		{
			Key:   tagPrefix + "product-group",
			Value: strings.ToLower(team.TeamProductGroup[tagTeamName]),
		},
		{
			Key:   tagPrefix + "team",
			Value: strings.ToLower(tagTeamName),
		},
		{
			Key:   tagPrefix + "service",
			Value: fmt.Sprintf("databricksjob-%s", spec.Name),
		},
		{
			Key:   tagPrefix + "rnd-allocation",
			Value: strconv.FormatFloat(spec.RnDCostAllocation, 'f', -1, 64),
		},
		{
			Key:   tagPrefix + dataplatformconsts.PRODUCTION_TAG,
			Value: strconv.FormatBool(spec.IsProduction),
		},
		{
			Key:   tagPrefix + dataplatformconsts.DATAPLAT_JOBTYPE_TAG,
			Value: string(spec.JobType),
		},
	}

	// If a job has an SLOConfig, we'll add some tags to the job indicating various features
	// about its slo and monitoring. This gets used downstream by our metrics worker.
	if spec.SloConfig != nil {
		if spec.SloConfig.SloTargetHours != 0 {
			tags = append(tags, databricks.ServerlessJobTags{
				Key:   tagPrefix + dataplatformconsts.SLO_TARGET_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.SloTargetHours),
			})
		}
		if spec.SloConfig.LowUrgencyThresholdHours != 0 {
			tags = append(tags, databricks.ServerlessJobTags{
				Key:   tagPrefix + dataplatformconsts.LOW_URGENCY_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.LowUrgencyThresholdHours),
			})
		}
		if spec.SloConfig.BusinessHoursThresholdHours != 0 {
			tags = append(tags, databricks.ServerlessJobTags{
				Key:   tagPrefix + dataplatformconsts.BUSINESS_HOURS_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.BusinessHoursThresholdHours),
			})
		}
		if spec.SloConfig.HighUrgencyThresholdHours != 0 {
			tags = append(tags, databricks.ServerlessJobTags{
				Key:   tagPrefix + dataplatformconsts.HIGH_URGENCY_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.HighUrgencyThresholdHours),
			})
		}
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags
}

func (spec *JobSpec) ServerlessEnabled() bool {
	return spec.ServerlessConfig != nil
}

func (spec *JobSpec) TfResource() (*databricksresource.Job, error) {
	owner := spec.Owner.Name()
	if owner == "" {
		return nil, oops.Errorf("Cannot create databricks job without owner")
	}

	regionLabel, ok := RegionLabels[spec.Region]
	if !ok {
		return nil, oops.Errorf("Did not specify valid Region for job")
	}

	if spec.RnDCostAllocation < 0 || spec.RnDCostAllocation > 1 {
		return nil, oops.Errorf("invalid RnDCostAllocation: %f", spec.RnDCostAllocation)
	}

	if spec.JobType == "" {
		return nil, oops.Errorf("Must populate job type")
	}

	job := &databricksresource.Job{
		ResourceName: spec.Name,
		Name:         fmt.Sprintf("%s-%s", spec.Name, regionLabel),
	}

	if spec.EmailNotifications != nil {
		job.EmailNotifications = &databricksresource.EmailNotifications{
			OnFailure:             spec.EmailNotifications,
			NoAlertForSkippedRuns: true,
		}
	}

	if spec.Cron != "" {
		job.Schedule = &databricksresource.CronSchedule{
			TimezoneId:           databricks.TimezoneIdGMT,
			QuartzCronExpression: spec.Cron,
			PauseStatus:          databricks.PauseStatusUnpaused,
		}
	}

	job.Format = spec.Format

	cluster, err := spec.ClusterResource()
	if err != nil {
		return nil, oops.Wrapf(err, "cluster resource")
	}

	libraries, err := spec.LibrariesResource()
	if err != nil {
		return nil, oops.Wrapf(err, "libraries resource")
	}

	if spec.Queue != nil {
		job.Queue = &databricksresource.JobQueue{
			Enabled: spec.Queue.Enabled,
		}
	}

	if spec.RunAs != nil {
		job.RunAs = &databricksresource.RunAsSetting{
			UserName:             spec.RunAs.UserName,
			ServicePrincipalName: spec.RunAs.ServicePrincipalName,
		}
	}

	if spec.ServerlessEnabled() {

		job.PerformanceTarget = spec.ServerlessConfig.GetPerformanceTarget()

		job.BudgetPolicyId = spec.ServerlessConfig.BudgetPolicyId

		job.Environments = []*databricksresource.ServerlessEnvironment{}
		if spec.ServerlessConfig.GetEnvironments() != nil {
			for _, env := range spec.ServerlessConfig.GetEnvironments() {
				job.Environments = append(job.Environments, &databricksresource.ServerlessEnvironment{
					EnvironmentKey: env.EnvironmentKey,
					Spec: &databricksresource.ServerlessEnvironmentSpec{
						Client:       env.Spec.Client,
						Dependencies: env.Spec.Dependencies,
					},
				})
			}
		}

	}

	// MULTI_TASK format
	if spec.Tasks != nil && len(spec.Tasks) > 0 {

		tasks, err := spec.JobTaskSettingsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "failed to parse job tasks")
		}
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].TaskKey < tasks[j].TaskKey
		})
		job.Tasks = tasks

		if spec.ServerlessEnabled() {
			job.JobClusters = nil
		} else {
			jobClusters, err := spec.JobClustersResource()
			if err != nil {
				return nil, oops.Wrapf(err, "failed to parse job clusters")
			}
			sort.Slice(jobClusters, func(i, j int) bool {
				return jobClusters[i].JobClusterKey < jobClusters[j].JobClusterKey
			})

			job.JobClusters = jobClusters
		}

		if spec.GitSource != nil {
			job.GitSource = &databricksresource.GitSource{
				GitUrl:      spec.GitSource.GitUrl,
				GitProvider: spec.GitSource.GitProvider,
				GitBranch:   spec.GitSource.GitBranch,
				GitCommit:   spec.GitSource.GitCommit,
			}
		} else {
			job.GitSource = nil
		}

		if spec.WebhookNotifications != nil {
			webhokNotifications := spec.WebhookNotificationsResource()
			job.WebhookNotifications = &webhokNotifications
		}

		job.TimeoutSeconds = spec.TimeoutSeconds

	} else {
		// Convert Legacy SINGLE_TASK to MULTI_TASK format
		taskKey := spec.Name
		if len(taskKey) > databricks.TaskKeyMaxLength {
			taskKey = spec.Name[(len(spec.Name) - databricks.TaskKeyMaxLength):]
			if idx := strings.Index(taskKey, "_"); idx != -1 {
				taskKey = taskKey[idx+1:]
			}
		}

		jobClusterKey := taskKey
		jobCluster := databricksresource.JobCluster{
			JobClusterKey: jobClusterKey,
			NewCluster:    cluster,
		}
		job.JobClusters = append([]*databricksresource.JobCluster{}, &jobCluster)

		task := databricksresource.JobTaskSettings{
			TaskKey:       taskKey,
			JobClusterKey: jobClusterKey,
		}

		if spec.ServerlessEnabled() {
			key := task.EnvironmentKey
			if key == "" {
				key = databricks.DefaultServerlessEnvironmentKey
			}
			task.EnvironmentKey = key
			task.JobClusterKey = ""
			job.JobClusters = []*databricksresource.JobCluster{}
		} else {
			task.Libraries = libraries
		}

		// If both script and workspace path are set, we will error.
		if spec.Script != nil && spec.WorkspacePath != "" {
			return nil, oops.Errorf("Cannot specify both script and workspace path for a job: %s", spec.Name)
		}

		// If the workspace path is set, we will use it as the python file for the task.
		if spec.WorkspacePath != "" {
			task.SparkPythonTask = &databricksresource.SparkPythonTask{
				PythonFile: spec.WorkspacePath,
				Parameters: spec.Parameters,
			}
		}

		// if the script is set, we will use it as the python file for the task.
		if spec.Script != nil {
			task.SparkPythonTask = &databricksresource.SparkPythonTask{
				PythonFile: spec.Script.URL(),
				Parameters: spec.Parameters,
			}
		}

		if spec.NotebookPath != "" {
			task.NotebookTask = &databricksresource.NotebookTask{
				NotebookPath:   spec.NotebookPath,
				BaseParameters: spec.ParameterMap,
				Source:         databricks.DefaultTaskSource,
			}
		}
		task.TimeoutSeconds = spec.TimeoutSeconds
		task.MaxRetries = spec.MaxRetries
		task.MinRetryIntervalMilliseconds = int(spec.MinRetryInterval.Milliseconds())

		job.Tasks = []*databricksresource.JobTaskSettings{&task}

	}
	job.Tags = spec.JobTags

	if spec.ServerlessEnabled() {
		// In serverless mode, we need to add the cluster tags to the job tags
		for _, tag := range spec.getServerlessTags() {
			job.Tags[tag.Key] = tag.Value
		}
	}

	return job, nil
}

func (spec *JobSpec) PermissionsResource() (*databricksresource.Permissions, error) {
	var admins []DatabricksPrincipal
	for _, team := range spec.AdminTeams {
		admins = append(admins, TeamPrincipal{Team: team})
	}
	return Permissions(PermissionsConfig{
		ResourceName:                 spec.Name,
		ObjectType:                   databricks.ObjectTypeJob,
		ObjectId:                     databricksresource.JobResourceId(spec.Name).Reference(),
		Owner:                        TeamPrincipal{Team: spec.Owner},
		JobOwnerUser:                 spec.JobOwnerUser,
		JobOwnerServicePrincipalName: spec.JobOwnerServicePrincipalName,
		Admins:                       admins,
		Region:                       spec.Region,
	})
}

func (spec *JobSpec) JobSettings() (*databricks.JobSettings, error) {
	databricksJobTfResource, err := spec.TfResource()
	if err != nil {
		return nil, err
	}

	jobSettings := databricks.JobSettings{
		Name: databricksJobTfResource.Name,
	}

	if databricksJobTfResource.EmailNotifications != nil {
		jobSettings.EmailNotifications = &databricks.EmailNotifications{
			OnStart:               databricksJobTfResource.EmailNotifications.OnStart,
			OnSuccess:             databricksJobTfResource.EmailNotifications.OnSuccess,
			OnFailure:             databricksJobTfResource.EmailNotifications.OnFailure,
			NoAlertForSkippedRuns: databricksJobTfResource.EmailNotifications.NoAlertForSkippedRuns,
		}
	}

	if databricksJobTfResource.Schedule != nil {
		jobSettings.Schedule = &databricks.CronSchedule{
			QuartzCronExpression: databricksJobTfResource.Schedule.QuartzCronExpression,
			TimezoneId:           databricksJobTfResource.Schedule.TimezoneId,
			PauseStatus:          databricksJobTfResource.Schedule.PauseStatus,
		}
	}

	if databricksJobTfResource.TimeoutSeconds != 0 {
		jobSettings.TimeoutSeconds = databricksJobTfResource.TimeoutSeconds
	}

	if databricksJobTfResource.Format != "" {
		jobSettings.Format = databricksJobTfResource.Format
	}

	if databricksJobTfResource.JobClusters != nil && len(databricksJobTfResource.JobClusters) > 0 && !spec.ServerlessEnabled() {
		jobClusters := []*databricks.JobCluster{}
		for _, jc := range databricksJobTfResource.JobClusters {
			cluster := expandCluster(jc.NewCluster, spec.ServerlessEnabled())
			jobCluster := databricks.JobCluster{
				JobClusterKey: jc.JobClusterKey,
				NewCluster:    cluster,
			}
			jobClusters = append(jobClusters, &jobCluster)
		}
		jobSettings.JobClusters = jobClusters
	}

	if databricksJobTfResource.Tasks != nil && len(databricksJobTfResource.Tasks) > 0 {
		tasks := []*databricks.JobTaskSettings{}

		for _, t := range databricksJobTfResource.Tasks {
			task := databricks.JobTaskSettings{
				TaskKey: t.TaskKey,
			}

			if t.Description != "" {
				task.Description = t.Description
			}

			if t.DependsOn != nil && len(t.DependsOn) > 0 {
				var taskDependencies []*databricks.TaskDependency
				for _, d := range t.DependsOn {
					taskDependencies = append(taskDependencies, &databricks.TaskDependency{
						TaskKey: d.TaskKey,
					})
				}
				task.DependsOn = taskDependencies
			}

			if t.ExistingClusterId != "" {
				task.ExistingClusterId = t.ExistingClusterId
			}

			if cluster := expandCluster(t.NewCluster, spec.ServerlessEnabled()); cluster != nil {
				task.NewCluster = cluster
			}

			if t.JobClusterKey != "" {
				task.JobClusterKey = t.JobClusterKey
			}

			if libraries := expandLibraries(t.Libraries); libraries != nil {
				task.Libraries = libraries
			}

			if t.NotebookTask != nil {
				task.NotebookTask = &databricks.NotebookTask{
					NotebookPath:   t.NotebookTask.NotebookPath,
					BaseParameters: t.NotebookTask.BaseParameters,
				}
			}

			if t.SparkPythonTask != nil {
				task.SparkPythonTask = &databricks.SparkPythonTask{
					PythonFile: t.SparkPythonTask.PythonFile,
					Parameters: t.SparkPythonTask.Parameters,
				}
			}

			if t.EmailNotifications != nil {
				jobSettings.EmailNotifications = &databricks.EmailNotifications{
					OnStart:               t.EmailNotifications.OnStart,
					OnSuccess:             t.EmailNotifications.OnSuccess,
					OnFailure:             t.EmailNotifications.OnFailure,
					NoAlertForSkippedRuns: t.EmailNotifications.NoAlertForSkippedRuns,
				}
			}

			if t.TimeoutSeconds != 0 {
				jobSettings.TimeoutSeconds = int(t.TimeoutSeconds)
			}

			if t.MaxRetries != 0 {
				jobSettings.MaxRetries = int(t.MaxRetries)
			}

			if t.MinRetryIntervalMilliseconds != 0 {
				jobSettings.MinRetryIntervalMilliseconds = int(t.MinRetryIntervalMilliseconds)
			}

			if spec.ServerlessEnabled() {
				key := t.EnvironmentKey
				if key == "" {
					key = databricks.DefaultServerlessEnvironmentKey
				}
				task.EnvironmentKey = key
			}

			tasks = append(tasks, &task)
		}
		jobSettings.Tasks = tasks
	}
	if databricksJobTfResource.Tags != nil {
		tags := make(map[string]string)
		for k, v := range databricksJobTfResource.Tags {
			tags[k] = v
		}
		jobSettings.Tags = tags
	}
	return &jobSettings, nil
}

func (spec *JobSpec) SparkVersionResource() sparkversion.SparkVersion {
	sparkVersion := sparkversion.JobSpecDefaultDbrVersion
	if spec.SparkVersion != "" {
		sparkVersion = spec.SparkVersion
	}
	return sparkVersion
}

func (spec *JobSpec) JobClustersResource() ([]*databricksresource.JobCluster, error) {

	if spec.ServerlessEnabled() {
		return nil, nil
	}

	cluster, err := spec.ClusterResource()
	if err != nil {
		return nil, oops.Wrapf(err, "cluster resource")
	}
	var jobClusters []*databricksresource.JobCluster
	if len(spec.JobClusters) > 0 {
		for _, jc := range spec.JobClusters {
			if len(jc.JobClusterKey) > databricks.JobClusterKeyMaxLength {
				return nil, oops.Errorf("Error: must specify a 'job_cluster_key' less than 100 chars")
			}
			c := convertClusterSpec(jc.NewCluster)
			jobCluster := databricksresource.JobCluster{
				JobClusterKey: jc.JobClusterKey,
				NewCluster:    updateClusterResource(spec.Region, cluster, c),
			}
			jobClusters = append(jobClusters, &jobCluster)
		}
	} else {
		jobClusters = append(jobClusters, &databricksresource.JobCluster{
			JobClusterKey: databricks.DefaultJobClusterKey,
			NewCluster:    cluster,
		})
	}
	return jobClusters, nil
}

func (spec *JobSpec) JobTaskSettingsResource() ([]*databricksresource.JobTaskSettings, error) {
	tasks := []*databricksresource.JobTaskSettings{}

	libraries, err := spec.LibrariesResource()
	if err != nil {
		return nil, oops.Wrapf(err, "task libraries resource")
	}

	cluster, err := spec.ClusterResource()
	if err != nil {
		return nil, oops.Wrapf(err, "task cluster resource")
	}

	// Cluster precedence for a task is: 1) ExistingClusterId; 2) NewCluster; 3) JobClusterKey; 4) default
	existingClusterId := ""
	var newCluster *databricksresource.Cluster
	if !spec.ServerlessEnabled() {
		newCluster = cluster
	}
	jobClusterKey := ""
	for _, t := range spec.Tasks {
		if t.TaskKey == "" || len(t.TaskKey) > databricks.TaskKeyMaxLength {
			return nil, oops.Errorf("Error: must specify a 'task_key' less than 100 chars")
		}

		if t.JobClusterKey != "" {
			existingClusterId = ""
			newCluster = nil
			jobClusterKey = t.JobClusterKey
		}
		if t.NewCluster != nil && !spec.ServerlessEnabled() {
			nc := convertClusterSpec(t.NewCluster)
			existingClusterId = ""
			newCluster = updateClusterResource(spec.Region, newCluster, nc)
			jobClusterKey = ""
		}
		if t.ExistingClusterId != "" {
			return nil, oops.Errorf("Cannot use all-purpose clusters (%s) for managed workflows", t.ExistingClusterId)
		}

		for _, l := range t.Libraries {
			if l.Maven != nil {
				libraries = append(libraries, &databricksresource.Library{
					Maven: databricksresource.MavenLibrary{
						Coordinates: l.Maven.Coordinates,
					},
				})
			} else if l.Pypi != nil {
				libraries = append(libraries, &databricksresource.Library{
					Pypi: databricksresource.PypiLibrary{
						Package: l.Pypi.Package,
					},
				})
			} else if l.Jar != "" {
				libraries = append(libraries, &databricksresource.Library{
					Jar: l.Jar,
				})
			}
		}

		task := databricksresource.JobTaskSettings{
			TaskKey:                      t.TaskKey,
			JobClusterKey:                jobClusterKey,
			ExistingClusterId:            existingClusterId,
			NewCluster:                   newCluster,
			Libraries:                    libraries,
			TimeoutSeconds:               t.TimeoutSeconds,
			MaxRetries:                   t.MaxRetries,
			RetryOnTimeout:               t.RetryOnTimeout,
			MinRetryIntervalMilliseconds: t.MinRetryIntervalMilliseconds,
		}

		if spec.ServerlessEnabled() {
			environmentKey := t.EnvironmentKey
			if environmentKey == "" {
				environmentKey = databricks.DefaultServerlessEnvironmentKey
			}
			task.EnvironmentKey = environmentKey
			task.NewCluster = nil
		}

		var dependencies []*databricksresource.TaskDependency
		if t.DependsOn != nil && len(t.DependsOn) > 0 {
			for _, d := range t.DependsOn {
				dependencies = append(dependencies, &databricksresource.TaskDependency{
					TaskKey: d.TaskKey,
				})
			}
			sort.Slice(dependencies, func(i, j int) bool {
				return dependencies[i].TaskKey < dependencies[j].TaskKey
			})
			task.DependsOn = dependencies
		}

		if t.SparkPythonTask != nil {
			if t.SparkPythonTask.PythonFile == "" {
				return nil, oops.Errorf("Python File is required for Spark Python Tasks")
			}
			task.SparkPythonTask = &databricksresource.SparkPythonTask{
				PythonFile: t.SparkPythonTask.PythonFile,
				Parameters: t.SparkPythonTask.Parameters,
			}
		}
		if t.NotebookTask != nil {
			if t.NotebookTask.NotebookPath == "" {
				return nil, oops.Errorf("Notebook Path is required for Notebook Tasks")
			}
			source := databricks.DefaultTaskSource
			if t.NotebookTask.Source != "" {
				source = t.NotebookTask.Source
			}
			task.NotebookTask = &databricksresource.NotebookTask{
				NotebookPath:   t.NotebookTask.NotebookPath,
				BaseParameters: t.NotebookTask.BaseParameters,
				Source:         source,
			}
		}
		if t.SqlTask != nil {
			if t.SqlTask.Query != nil {
				if t.SqlTask.Query.QueryId == "" {
					return nil, oops.Errorf("Query Id is required for Sql Query Tasks")
				}
				task.SqlTask = &databricksresource.SqlTask{
					Query: &databricksresource.SqlTaskQuery{
						QueryId: t.SqlTask.Query.QueryId,
					},
				}
			}
			if t.SqlTask.Dashboard != nil {
				if t.SqlTask.Dashboard.DashboardId == "" {
					return nil, oops.Errorf("Dashboard Id is required for Sql Dashboard Tasks")
				}
				dashboard := databricksresource.SqlTaskDashboard{
					DashboardId:   t.SqlTask.Dashboard.DashboardId,
					CustomSubject: t.SqlTask.Dashboard.CustomSubject,
				}
				var subscriptions []*databricksresource.SqlTaskSubscriptions
				if t.SqlTask.Dashboard.Subscriptions != nil && len(t.SqlTask.Dashboard.Subscriptions) > 0 {
					for _, s := range t.SqlTask.Dashboard.Subscriptions {
						if s.UserName != "" && s.DestinationId != "" {
							return nil, oops.Errorf("Error: Subscriptions should contain one of user name or destination id")
						}
						if s != nil {
							subscriptions = append(subscriptions, &databricksresource.SqlTaskSubscriptions{
								UserName:      s.UserName,
								DestinationId: s.DestinationId,
							})
						}
					}
					dashboard.Subscriptions = subscriptions
				} else {
					dashboard.Subscriptions = nil
				}
				task.SqlTask = &databricksresource.SqlTask{
					Dashboard: &dashboard,
				}
			}
			if t.SqlTask.Alert != nil {
				if t.SqlTask.Alert.AlertId == "" {
					return nil, oops.Errorf("Alert Id is required for Sql Alert Tasks")
				}
				alert := databricksresource.SqlTaskAlert{
					AlertId: t.SqlTask.Alert.AlertId,
				}
				var subscriptions []*databricksresource.SqlTaskSubscriptions
				if t.SqlTask.Alert.Subscriptions != nil && len(t.SqlTask.Alert.Subscriptions) > 0 {
					for _, s := range t.SqlTask.Alert.Subscriptions {
						if s.UserName != "" && s.DestinationId != "" {
							return nil, oops.Errorf("Error: Subscriptions should contain one of user name or destination id")
						}
						subscriptions = append(subscriptions, &databricksresource.SqlTaskSubscriptions{
							UserName:      s.UserName,
							DestinationId: s.DestinationId,
						})
					}
					alert.Subscriptions = subscriptions
				}
				task.SqlTask = &databricksresource.SqlTask{
					Alert: &alert,
				}
			}
			if t.SqlTask.WarehouseId == "" {
				return nil, oops.Errorf("Warehouse Id is required for Sql Tasks")
			}
			task.SqlTask.WarehouseId = t.SqlTask.WarehouseId
			task.JobClusterKey = ""
			task.ExistingClusterId = ""
			task.NewCluster = nil
			task.Libraries = nil
		}
		if t.DbtTask != nil {
			if t.DbtTask.Commands == nil || len(t.DbtTask.Commands) == 0 {
				return nil, oops.Errorf("Dbt Commands is required for Dbt Tasks")
			}
			task.DbtTask = &databricksresource.DbtTask{
				Commands:          t.DbtTask.Commands,
				ProjectDirectory:  t.DbtTask.ProjectDirectory,
				Schema:            t.DbtTask.Schema,
				WarehouseId:       t.DbtTask.WarehouseId,
				Catalog:           t.DbtTask.Catalog,
				ProfilesDirectory: t.DbtTask.ProfilesDirectory,
			}
		}
		if t.PythonWheelTask != nil {
			task.PythonWheelTask = &databricksresource.PythonWheelTask{
				EntryPoint:      t.PythonWheelTask.EntryPoint,
				PackageName:     t.PythonWheelTask.PackageName,
				Parameters:      t.PythonWheelTask.Parameters,
				NamedParameters: t.PythonWheelTask.NamedParameters,
			}
		}
		if t.SparkSubmitTask != nil {
			task.SparkSubmitTask = &databricksresource.SparkSubmitTask{
				Parameters: t.SparkSubmitTask.Parameters,
			}
		}
		if t.SparkJarTask != nil {
			task.SparkJarTask = &databricksresource.SparkJarTask{
				MainClassName: t.SparkJarTask.MainClassName,
				Parameters:    t.SparkJarTask.Parameters,
			}
		}
		if t.PipelineTask != nil {
			if t.PipelineTask.PipelineID == "" {
				return nil, oops.Errorf("Pipeline Id is required for Pipeline Tasks")
			}
			task.PipelineTask = &databricksresource.PipelineTask{
				PipelineID: t.PipelineTask.PipelineID,
			}
		}

		tasks = append(tasks, &task)
	}

	return tasks, nil
}

func (spec *JobSpec) WebhookNotificationsResource() databricksresource.WebhookNotifications {
	var webhookNotifications databricksresource.WebhookNotifications
	if spec.WebhookNotifications.OnFailure != nil {
		for _, d := range spec.WebhookNotifications.OnFailure {
			webhookNotifications.OnFailure = append(webhookNotifications.OnFailure, &databricksresource.WebhookNotificationDestination{
				Id: d.Id,
			})
		}
	}
	if spec.WebhookNotifications.OnSuccess != nil {
		for _, d := range spec.WebhookNotifications.OnSuccess {
			webhookNotifications.OnSuccess = append(webhookNotifications.OnSuccess, &databricksresource.WebhookNotificationDestination{
				Id: d.Id,
			})
		}
	}
	if spec.WebhookNotifications.OnStart != nil {
		for _, d := range spec.WebhookNotifications.OnStart {
			webhookNotifications.OnStart = append(webhookNotifications.OnStart, &databricksresource.WebhookNotificationDestination{
				Id: d.Id,
			})
		}
	}

	return webhookNotifications
}

func (spec *JobSpec) LibrariesResource() ([]*databricksresource.Library, error) {

	if spec.ServerlessEnabled() {
		return nil, nil
	}

	defaultLibrariesToSkip := make(map[string]struct{}, len(spec.Libraries.DefaultLibrariesToSkip))
	for _, lib := range spec.Libraries.DefaultLibrariesToSkip {
		defaultLibrariesToSkip[lib] = struct{}{}
	}

	ucEnabled := true
	if spec.UnityCatalogSetting.DataSecurityMode == databricksresource.DataSecurityModeNone || spec.UnityCatalogSetting.DataSecurityMode == "" {
		ucEnabled = false
	}

	var libraries []*databricksresource.Library
	if spec.Libraries.LoadDefaultLibraries {
		for _, sparkLibrary := range DefaultLibraries(ucEnabled) {
			// Skip specified default libraries.
			if _, ok := defaultLibrariesToSkip[sparkLibrary.String()]; ok {
				continue
			}
			tfLibrary, err := sparkLibrary.TfResource(spec.Region, ucEnabled)
			if err != nil {
				return nil, oops.Wrapf(err, "library resource")
			}
			libraries = append(libraries, tfLibrary)
		}
	}
	for _, jar := range spec.Libraries.Jars {
		tfLibrary, err := JarName(jar).TfResource(spec.Region, ucEnabled)
		if err != nil {
			return nil, oops.Wrapf(err, "jar library resource")
		}
		libraries = append(libraries, tfLibrary)
	}
	for _, pkg := range spec.Libraries.PyPIs {
		tfLibrary, err := PyPIName(pkg).TfResource(spec.Region, ucEnabled)
		if err != nil {
			return nil, oops.Wrapf(err, "python library resource")
		}
		libraries = append(libraries, tfLibrary)
	}
	for _, pkg := range spec.Libraries.Mavens {
		tfLibrary, err := MavenName(pkg).TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "maven library resource")
		}
		libraries = append(libraries, tfLibrary)
	}
	return libraries, nil
}

func (spec *JobSpec) defaultDriverInstanceType() string {
	// CA spark clusters are failing to start with AWS_UNSUPPORTED_FAILURE when m5dn.large is used.
	/// TODO_CA: Remove this once m5dn.large is supported.
	if spec.Region == infraconsts.SamsaraAWSCARegion {
		return "m5d.large"
	}
	return "m5dn.large"
}

func (spec *JobSpec) defaultWorkerInstanceType() string {
	return "rd-fleet.xlarge"
}

func (spec *JobSpec) ClusterResource() (*databricksresource.Cluster, error) {

	if spec.ServerlessEnabled() {
		return nil, nil
	}

	tags := spec.ClusterTagsResource()

	cluster := &databricksresource.Cluster{
		AwsAttributes: &databricksresource.ClusterAwsAttributes{
			InstanceProfileArn: spec.Profile,
		},
		SparkVersion: string(spec.SparkVersionResource()),
		SparkConf: SparkConf{
			DisableQueryWatchdog:         true,
			Region:                       spec.Region,
			DataSecurityMode:             spec.UnityCatalogSetting.DataSecurityMode,
			Overrides:                    spec.SparkConf,
			DisableFileModificationCheck: spec.DisableFileModificationCheck,
		}.ToMap(),
		SparkEnvVars: concatenateStringMap(spec.SparkEnvVars, map[string]string{
			"AWS_DEFAULT_REGION":   spec.Region,
			"AWS_REGION":           spec.Region,
			"GOOGLE_CLOUD_PROJECT": "samsara-data",

			// Runtime 7.x BigQuery connector configurations.
			// https://docs.databricks.com/data/data-sources/google/bigquery.html
			"GOOGLE_APPLICATION_CREDENTIALS": databricks.BigQueryCredentialsLocation(),
		}),
		ClusterLogConf: &databricksresource.ClusterLogConf{
			S3: &databricksresource.S3StorageInfo{
				Destination: fmt.Sprintf("s3://%sdatabricks-cluster-logs/%s", awsregionconsts.RegionPrefix[spec.Region], spec.Name),
				Region:      spec.Region,
			},
		},
		CustomTags: tags,
	}

	if spec.Pool == "" {
		cluster.AwsAttributes.ZoneId = NameAZ(spec.Name, spec.Region)
	}

	// TODO(parth): I think the following code assumes that if an instance pool is not provided, that we should set
	// the worker nodes and driver nodes parameters. This isn't true since we could set the driver pool,
	// so we should come back and revisit this but for now it's likely ok since we're not going to use
	// driver pools without using instance pools too.
	if spec.DriverPool != "" {
		cluster.DriverInstancePoolId = spec.DriverPool
	}

	if spec.Pool != "" {
		cluster.InstancePoolId = spec.Pool
	} else {

		driverNodeType := spec.defaultDriverInstanceType()

		if spec.DriverNodeType != "" {
			driverNodeType = spec.DriverNodeType
		}

		workerNodeType := spec.defaultWorkerInstanceType()

		if spec.WorkerNodeType != "" {
			workerNodeType = spec.WorkerNodeType
		}
		cluster.DriverNodeTypeId = driverNodeType
		cluster.NodeTypeId = workerNodeType
		cluster.AwsAttributes.FirstOnDemand = 1
		cluster.EnableElasticDisk = true

	}

	// Single node job should set num workers count to 0, as well as some other configuration:
	// https://docs.databricks.com/dev-tools/api/latest/clusters.html#examples
	if spec.SingleNodeJob {
		cluster.NumWorkers = pointer.IntPtr(0)
		cluster.CustomTags = append(cluster.CustomTags, databricksresource.ClusterTag{
			Key:   "ResourceClass",
			Value: "SingleNode",
		})
		cluster.SparkConf = concatenateStringMap(cluster.SparkConf, map[string]string{
			"spark.databricks.cluster.profile": "singleNode",
			"spark.master":                     "local[*]",
			"spark.sql.shuffle.partitions":     "8", // usually have like 2-8 vcpu for a driver, so don't have too many partitions.
		})
	} else {
		minWorkers := mathutil.Max(spec.MinWorkers, 1)
		maxWorkers := mathutil.Max(spec.MaxWorkers, minWorkers)
		if minWorkers == maxWorkers {
			cluster.NumWorkers = pointer.IntPtr(minWorkers)
		} else {
			cluster.AutoScale = &databricksresource.ClusterAutoScale{
				MinWorkers: minWorkers,
				MaxWorkers: maxWorkers,
			}
		}
		// If we're using an on-demand cluster, we need to set the first on demand to the max workers.
		if spec.UseOnDemandCluster {
			// We need to add 1 to the first on demand because the first on demand is the driver node.
			cluster.AwsAttributes.FirstOnDemand = maxWorkers + 1
		}
	}

	if len(spec.VolumesInitScripts) != 0 {
		scripts := make([]*databricksresource.InitScript, 0, len(spec.VolumesInitScripts))
		for _, location := range spec.VolumesInitScripts {
			scripts = append(scripts, &databricksresource.InitScript{
				Volumes: &databricksresource.VolumesStorageInfo{
					Destination: location,
				},
			})
		}
		cluster.InitScripts = scripts
	}

	switch spec.UnityCatalogSetting.DataSecurityMode {
	// TODO: we should improve the job spec to know who the owner is
	case databricksresource.DataSecurityModeSingleUser:
		cluster.DataSecurityMode = databricksresource.DataSecurityModeSingleUser
		cluster.SingleUserName = spec.UnityCatalogSetting.SingleUserName
	case databricksresource.DataSecurityModeUserIsolation:
		cluster.DataSecurityMode = databricksresource.DataSecurityModeUserIsolation
	case "", databricksresource.DataSecurityModeNone:
		cluster.DataSecurityMode = databricksresource.DataSecurityModeNone
	default:
		return nil, oops.Errorf("Unknown UnityCatalogSetting %s", spec.UnityCatalogSetting)
	}

	return cluster, nil
}

func (spec *JobSpec) ClusterTagsResource() []databricksresource.ClusterTag {
	tagPrefix := "samsara:"
	if spec.Pool != "" {
		tagPrefix = tagPrefix + "pooled-job:"
	}
	tagTeamName := spec.Owner.Name()
	// If BudgetingTeamOverride we use it as tag for the name and to find the product group tag.
	if spec.BudgetingTeamOverride.TeamName != "" {
		tagTeamName = spec.BudgetingTeamOverride.Name()
	}

	tags := []databricksresource.ClusterTag{
		{
			Key:   tagPrefix + "product-group",
			Value: strings.ToLower(team.TeamProductGroup[tagTeamName]),
		},
		{
			Key:   tagPrefix + "team",
			Value: strings.ToLower(tagTeamName),
		},
		{
			Key:   tagPrefix + "service",
			Value: fmt.Sprintf("databricksjob-%s", spec.Name),
		},
		{
			Key:   tagPrefix + "rnd-allocation",
			Value: strconv.FormatFloat(spec.RnDCostAllocation, 'f', -1, 64),
		},
		{
			Key:   tagPrefix + dataplatformconsts.PRODUCTION_TAG,
			Value: strconv.FormatBool(spec.IsProduction),
		},
		{
			Key:   tagPrefix + dataplatformconsts.DATAPLAT_JOBTYPE_TAG,
			Value: string(spec.JobType),
		},
	}
	if spec.Pool != "" {
		tags = append(tags, databricksresource.ClusterTag{
			Key:   tagPrefix + "pool-id",
			Value: spec.Pool,
		})
	}
	if spec.DriverPool != "" {
		tags = append(tags, databricksresource.ClusterTag{
			Key:   tagPrefix + "driver-pool-id",
			Value: spec.DriverPool,
		})
	}

	// If a job has an SLOConfig, we'll add some tags to the job indicating various features
	// about its slo and monitoring. This gets used downstream by our metrics worker.
	if spec.SloConfig != nil {
		if spec.SloConfig.SloTargetHours != 0 {
			tags = append(tags, databricksresource.ClusterTag{
				Key:   tagPrefix + dataplatformconsts.SLO_TARGET_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.SloTargetHours),
			})
		}
		if spec.SloConfig.LowUrgencyThresholdHours != 0 {
			tags = append(tags, databricksresource.ClusterTag{
				Key:   tagPrefix + dataplatformconsts.LOW_URGENCY_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.LowUrgencyThresholdHours),
			})
		}
		if spec.SloConfig.BusinessHoursThresholdHours != 0 {
			tags = append(tags, databricksresource.ClusterTag{
				Key:   tagPrefix + dataplatformconsts.BUSINESS_HOURS_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.BusinessHoursThresholdHours),
			})
		}
		if spec.SloConfig.HighUrgencyThresholdHours != 0 {
			tags = append(tags, databricksresource.ClusterTag{
				Key:   tagPrefix + dataplatformconsts.HIGH_URGENCY_THRESHOLD_TAG,
				Value: fmt.Sprintf("%d", spec.SloConfig.HighUrgencyThresholdHours),
			})
		}
	}

	for k, v := range spec.Tags {
		tags = append(tags, databricksresource.ClusterTag{
			Key:   tagPrefix + k,
			Value: v,
		})
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags
}

func updateClusterResource(region string, cluster *databricksresource.Cluster, newCluster *databricksresource.Cluster) *databricksresource.Cluster {

	if newCluster == nil {
		return cluster
	}
	c := *cluster
	if newCluster.SparkVersion != "" {
		c.SparkVersion = newCluster.SparkVersion
	}

	if newCluster.AutoScale != nil {
		c.AutoScale = newCluster.AutoScale
	}

	if newCluster.AwsAttributes != nil {
		c.AwsAttributes.FirstOnDemand = newCluster.AwsAttributes.FirstOnDemand
	}

	if newCluster.SparkConf != nil {
		c.SparkConf = newCluster.SparkConf
	}

	if newCluster.SparkEnvVars != nil {
		for key, value := range newCluster.SparkEnvVars {
			switch key {
			case "AWS_REGION", "AWS_DEFAULT_REGION":
				continue
			}
			c.SparkEnvVars[key] = value
		}
	}

	if newCluster.DriverNodeTypeId != "" {
		c.DriverNodeTypeId = newCluster.DriverNodeTypeId
	}

	if newCluster.NodeTypeId != "" {
		c.NodeTypeId = newCluster.NodeTypeId
	}

	if newCluster.InstancePoolId != "" {
		c.InstancePoolId = newCluster.InstancePoolId
		c.NodeTypeId = ""
	}
	if newCluster.DriverInstancePoolId != "" {
		c.DriverInstancePoolId = newCluster.DriverInstancePoolId
		c.DriverNodeTypeId = ""
	}

	if newCluster.NumWorkers != nil {
		c.NumWorkers = newCluster.NumWorkers
	}

	if newCluster.EnableLocalDiskEncryption == true {
		c.EnableLocalDiskEncryption = true
	} else {
		c.EnableLocalDiskEncryption = false
	}

	if newCluster.InitScripts != nil && len(newCluster.InitScripts) > 0 {
		for _, initScript := range newCluster.InitScripts {
			if initScript.S3 != nil && initScript.S3.Region != region {
				continue
			}
			c.InitScripts = append(cluster.InitScripts, newCluster.InitScripts...)
		}
	}

	if newCluster.CustomTags != nil {
		tagNames := map[string]struct{}{}
		for _, t := range c.CustomTags {
			tagNames[t.Key] = struct{}{}
		}
		for _, t := range newCluster.CustomTags {
			if _, ok := tagNames[t.Key]; !ok {
				c.CustomTags = append(c.CustomTags, t)
			}
		}
	}

	return &c
}

func convertClusterSpec(cs *databricks.ClusterSpec) *databricksresource.Cluster {

	cluster := databricksresource.Cluster{
		SparkVersion:              string(cs.SparkVersion),
		SparkConf:                 cs.SparkConf,
		SparkEnvVars:              cs.SparkEnvVars,
		InstancePoolId:            cs.InstancePoolId,
		DriverInstancePoolId:      cs.DriverInstancePoolId,
		DriverNodeTypeId:          cs.DriverNodeTypeId,
		NodeTypeId:                cs.NodeTypeId,
		EnableElasticDisk:         cs.EnableElasticDisk,
		AutoTerminationMinutes:    cs.AutoTerminationMinutes,
		EnableLocalDiskEncryption: cs.EnableLocalDiskEncryption,
	}

	var customTags []databricksresource.ClusterTag
	if len(cs.CustomTags) > 0 {
		for k, v := range cs.CustomTags {
			customTags = append(customTags, databricksresource.ClusterTag{
				Key:   k,
				Value: v,
			})
		}
		cluster.CustomTags = customTags
	}

	var initScripts []*databricksresource.InitScript
	if cs.InitScripts != nil {
		for _, initScript := range cs.InitScripts {
			if initScript.S3 != nil {
				initScripts = append(initScripts, &databricksresource.InitScript{
					S3: &databricksresource.S3StorageInfo{
						Destination: initScript.S3.Destination,
						Region:      initScript.S3.Region,
					},
				})
			} else if initScript.DBFS != nil {
				initScripts = append(initScripts, &databricksresource.InitScript{
					Dbfs: &databricksresource.DBFSStorageInfo{
						Destination: initScript.DBFS.Destination,
					},
				})
			} else if initScript.Volumes != nil {
				initScripts = append(initScripts, &databricksresource.InitScript{
					Volumes: &databricksresource.VolumesStorageInfo{
						Destination: initScript.Volumes.Destination,
					},
				})
			}
		}
		cluster.InitScripts = initScripts
	}

	if cs.AwsAttributes != nil {
		cluster.AwsAttributes = &databricksresource.ClusterAwsAttributes{
			InstanceProfileArn: cs.AwsAttributes.InstanceProfileArn,
			FirstOnDemand:      cs.AwsAttributes.FirstOnDemand,
			ZoneId:             cs.AwsAttributes.ZoneId,
		}
	}

	if cs.ClusterLogConf != nil {
		if cs.ClusterLogConf.S3 != nil {
			cluster.ClusterLogConf = &databricksresource.ClusterLogConf{
				S3: &databricksresource.S3StorageInfo{
					Destination: cs.ClusterLogConf.S3.Destination,
					Region:      cs.ClusterLogConf.S3.Region,
				},
			}
		} else if cs.ClusterLogConf.DBFS != nil {
			cluster.ClusterLogConf = &databricksresource.ClusterLogConf{
				Dbfs: &databricksresource.DBFSStorageInfo{
					Destination: cs.ClusterLogConf.DBFS.Destination,
				},
			}
		}
	}

	if cs.AutoScale != nil {
		cluster.AutoScale = &databricksresource.ClusterAutoScale{
			MinWorkers: cs.AutoScale.MinWorkers,
			MaxWorkers: cs.AutoScale.MaxWorkers,
		}
	} else {
		cluster.NumWorkers = cs.NumWorkers
	}

	return &cluster
}

func expandLibraries(libs []*databricksresource.Library) []*databricks.Library {
	if libs != nil {
		libraries := make([]*databricks.Library, len(libs))
		for i, l := range libs {
			lib := &databricks.Library{}
			if l.Jar != "" {
				lib.Jar = l.Jar
			}
			if l.Maven.Coordinates != "" {
				lib.Maven = &databricks.MavenLibrary{
					Coordinates: l.Maven.Coordinates,
				}
			}
			if l.Pypi.Package != "" {
				lib.Pypi = &databricks.PypiLibrary{
					Package: l.Pypi.Package,
				}
			}
			libraries[i] = lib
		}
		return libraries
	}
	return nil
}

func expandClusterTags(tags []databricksresource.ClusterTag) map[string]string {
	if tags != nil {
		newClusterCustomTags := make(map[string]string, len(tags))
		for _, tag := range tags {
			newClusterCustomTags[tag.Key] = tag.Value
		}
		return newClusterCustomTags
	}
	return nil
}

func expandCluster(cluster *databricksresource.Cluster, isServerless bool) *databricks.ClusterSpec {

	if isServerless {
		return nil
	}

	if cluster != nil {
		clusterSpec := &databricks.ClusterSpec{
			NumWorkers:   cluster.NumWorkers,
			ClusterName:  cluster.ClusterName,
			SparkVersion: sparkversion.SparkVersion(cluster.SparkVersion),
			SparkConf:    cluster.SparkConf,

			NodeTypeId:        cluster.NodeTypeId,
			DriverNodeTypeId:  cluster.DriverNodeTypeId,
			EnableElasticDisk: cluster.EnableElasticDisk,

			InstancePoolId:            cluster.InstancePoolId,
			DriverInstancePoolId:      cluster.DriverInstancePoolId,
			AutoTerminationMinutes:    cluster.AutoTerminationMinutes,
			EnableLocalDiskEncryption: cluster.EnableLocalDiskEncryption,
		}

		if cluster.AutoScale != nil {
			clusterSpec.AutoScale = &databricks.ClusterAutoScale{
				MinWorkers: cluster.AutoScale.MinWorkers,
				MaxWorkers: cluster.AutoScale.MaxWorkers,
			}
		}

		if cluster.AwsAttributes != nil {
			clusterSpec.AwsAttributes = &databricks.ClusterAwsAttributes{
				InstanceProfileArn: cluster.AwsAttributes.InstanceProfileArn,
				FirstOnDemand:      cluster.AwsAttributes.FirstOnDemand,
				ZoneId:             cluster.AwsAttributes.ZoneId,
			}
		}

		if cluster.InitScripts != nil {
			initScripts := make([]*databricks.InitScript, len(cluster.InitScripts))
			for i, initScript := range cluster.InitScripts {
				initScripts[i] = &databricks.InitScript{}
				if initScript.Dbfs != nil {
					initScripts[i].DBFS = &databricks.DBFSStorageInfo{
						Destination: initScript.Dbfs.Destination,
					}
				}

				if initScript.S3 != nil {
					initScripts[i].S3 = &databricks.S3StorageInfo{
						Destination: initScript.S3.Destination,
						Region:      initScript.S3.Region,
					}
				}

				if initScript.Volumes != nil {
					initScripts[i].Volumes = &databricks.VolumeStorageInfo{
						Destination: initScript.Volumes.Destination,
					}
				}
			}
			clusterSpec.InitScripts = initScripts
		}

		if cluster.ClusterLogConf != nil {
			clusterSpec.ClusterLogConf = &databricks.ClusterLogConf{}
			if cluster.ClusterLogConf.Dbfs != nil {
				clusterSpec.ClusterLogConf.DBFS = &databricks.DBFSStorageInfo{
					Destination: cluster.ClusterLogConf.Dbfs.Destination,
				}
			}
			if cluster.ClusterLogConf.S3 != nil {
				clusterSpec.ClusterLogConf.S3 = &databricks.S3StorageInfo{
					Destination: cluster.ClusterLogConf.S3.Destination,
					Region:      cluster.ClusterLogConf.S3.Region,
				}
			}
		}

		tags := expandClusterTags(cluster.CustomTags)
		if tags != nil {
			clusterSpec.CustomTags = tags
		}

		if cluster.SparkEnvVars != nil {
			newSparkEnvVars := make(map[string]string, len(cluster.SparkEnvVars))
			for envVar, envValue := range cluster.SparkEnvVars {
				newSparkEnvVars[envVar] = envValue
			}
			clusterSpec.SparkEnvVars = newSparkEnvVars
		}
		return clusterSpec
	}
	return nil
}
