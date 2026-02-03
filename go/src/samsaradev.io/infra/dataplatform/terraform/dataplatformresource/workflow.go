package dataplatformresource

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/team/components"
)

const WorkflowKey string = "WORKFLOW"

type WorkflowOverrides struct {
	EmailNotifications   *databricks.EmailNotifications   `json:"email_notifications,omitempty"`
	JobClusters          []*databricks.JobCluster         `json:"job_clusters,omitempty"`
	WebhookNotifications *databricks.WebhookNotifications `json:"webhook_notifications,omitempty"`
	Tags                 map[string]string                `json:"tags,omitempty"`
	GitSource            *databricks.GitSource            `json:"git_source,omitempty"`
}

type WorkflowScheduleConfig struct {
	FilePath           string
	WorkflowPath       string
	NameSuffix         string
	Owner              components.TeamInfo
	Profile            string
	AdminTeams         []components.TeamInfo
	JobOwnerUser       string
	Region             string
	TimeoutSeconds     int
	Cron               string
	PythonLibraries    []string
	SparkConfs         map[string]string
	ParameterOverrides *databricks.OverridingParameters
	WorkflowOverrides  *WorkflowOverrides
	Workflow           *databricks.JobSettings
}

func generateJobClusters(c WorkflowScheduleConfig) ([]*databricks.JobCluster, error) {
	jobClusterMap := make(map[string]*databricks.JobCluster)

	if len(c.Workflow.JobClusters) > 0 {
		for _, jc := range c.Workflow.JobClusters {
			jobClusterMap[jc.JobClusterKey] = jc
		}
	}
	if c.WorkflowOverrides != nil && len(c.WorkflowOverrides.JobClusters) > 0 {
		for _, jc := range c.WorkflowOverrides.JobClusters {
			if _, ok := jobClusterMap[jc.JobClusterKey]; ok {
				jobClusterMap[jc.JobClusterKey] = jc
			} else {
				return nil, oops.Errorf("JobCluster override %s not in Workflow JobClusters. Cannot add new JobCluster in overrides.", jc.JobClusterKey)
			}
		}
	}
	var jobClusters []*databricks.JobCluster
	for _, jc := range jobClusterMap {
		jobClusters = append(jobClusters, jc)
	}

	return jobClusters, nil
}

func WorkflowResource(c WorkflowScheduleConfig) ([]tf.Resource, error) {
	name := tf.SanitizeResourceName(c.WorkflowPath + "_" + c.NameSuffix)
	pyPILibraries := make([]PyPIName, len(c.PythonLibraries))
	if c.PythonLibraries != nil && len(c.PythonLibraries) > 0 {
		for idx, lib := range c.PythonLibraries {
			pyPILibraries[idx] = PyPIName(lib)
		}
	}

	jobClusters, err := generateJobClusters(c)
	if err != nil {
		return nil, oops.Wrapf(err, "Error generating workklow (%s) job clusters", c.Workflow.Name)
	}

	jobTags := map[string]string{
		"format": databricks.MultiTaskKey,
	}
	if c.WorkflowOverrides != nil && c.WorkflowOverrides.Tags != nil {
		for k, v := range c.WorkflowOverrides.Tags {
			jobTags[k] = v
		}
	}

	for _, t := range c.Workflow.Tasks {
		if c.ParameterOverrides != nil {
			if t.NotebookTask != nil && len(c.ParameterOverrides.NotebookParams) > 0 {
				t.NotebookTask.BaseParameters = c.ParameterOverrides.NotebookParams
			}
			if t.SparkPythonTask != nil && len(c.ParameterOverrides.PythonParams) > 0 {
				t.SparkPythonTask.Parameters = c.ParameterOverrides.PythonParams
			}
		}
	}

	var gitSource *databricks.GitSource
	if c.WorkflowOverrides != nil && c.WorkflowOverrides.GitSource != nil {
		gitSource = &databricks.GitSource{
			GitUrl:      c.WorkflowOverrides.GitSource.GitUrl,
			GitProvider: c.WorkflowOverrides.GitSource.GitProvider,
			GitBranch:   c.WorkflowOverrides.GitSource.GitBranch,
			GitCommit:   c.WorkflowOverrides.GitSource.GitCommit,
		}
	} else if c.Workflow.GitSource != nil {
		gitSource = &databricks.GitSource{
			GitUrl:      c.Workflow.GitSource.GitUrl,
			GitProvider: c.Workflow.GitSource.GitProvider,
			GitBranch:   c.Workflow.GitSource.GitBranch,
			GitCommit:   c.Workflow.GitSource.GitCommit,
		}
	}

	var webhookNotifications *databricks.WebhookNotifications
	if c.WorkflowOverrides != nil && c.WorkflowOverrides.WebhookNotifications != nil {
		webhookNotifications = c.WorkflowOverrides.WebhookNotifications
	} else if c.Workflow.WebhookNotifications != nil {
		webhookNotifications = c.Workflow.WebhookNotifications
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(c.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal for region %s", c.Region)
	}

	ucSettings := UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	jobSpec := JobSpec{
		Name:              name,
		Owner:             c.Owner,
		Region:            c.Region,
		Profile:           c.Profile,
		Cron:              c.Cron,
		TimeoutSeconds:    c.TimeoutSeconds,
		RnDCostAllocation: 1,
		JobType:           dataplatformconsts.Workflow,
		SparkConf: SparkConf{
			Region:                    c.Region,
			EnableCaching:             true,
			EnableBigQuery:            true,
			EnablePlaygroundWarehouse: true,
			EnableExtension:           true,
			DisableQueryWatchdog:      true,
			Overrides:                 c.SparkConfs,
		}.ToMap(),
		SparkEnvVars: map[string]string{
			"GOOGLE_CLOUD_PROJECT": "samsara-data",
		},
		MinWorkers: 1,
		MaxWorkers: 8,
		AdminTeams: c.AdminTeams,
		VolumesInitScripts: []string{
			databricksinstaller.BigQueryCredentialsInitScript(),
			databricksinstaller.GetSparkRulesInitScript(),
		},
		Libraries: JobLibraryConfig{
			LoadDefaultLibraries: true,
			PyPIs:                pyPILibraries,
		},
		JobClusters:                  jobClusters,
		Tasks:                        c.Workflow.Tasks,
		Format:                       databricks.MultiTaskKey,
		JobTags:                      jobTags,
		GitSource:                    gitSource,
		WebhookNotifications:         webhookNotifications,
		UnityCatalogSetting:          ucSettings,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	job, err := jobSpec.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	if c.WorkflowOverrides != nil && c.WorkflowOverrides.EmailNotifications != nil {
		emailNotifications := databricksresource.EmailNotifications{
			OnStart:               c.WorkflowOverrides.EmailNotifications.OnStart,
			OnSuccess:             c.WorkflowOverrides.EmailNotifications.OnSuccess,
			OnFailure:             c.WorkflowOverrides.EmailNotifications.OnFailure,
			NoAlertForSkippedRuns: c.WorkflowOverrides.EmailNotifications.NoAlertForSkippedRuns,
		}
		job.EmailNotifications = &emailNotifications
	}

	jobPermissions, err := jobSpec.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{job, jobPermissions}, nil
}
