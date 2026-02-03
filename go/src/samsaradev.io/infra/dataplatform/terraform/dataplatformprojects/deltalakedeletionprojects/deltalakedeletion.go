package deltalakedeletionprojects

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/deltalakedeletion"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

const (
	DeletionInstanceProfile = "delta-lake-deletion-cluster"
	DeletionJobMaxWorkers   = 32
)

const (
	jobClusterCount = 5
	plansBatchSize  = 2
)

const DeletionScriptFileName = "delta_lake_deletion.py"

func DataDeletionProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	deletionArn, err := dataplatformresource.InstanceProfileArn(providerGroup, DeletionInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, deletionArn),
		},
	}

	deletionJobResources, err := dataDeletionJobResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating deletion job resources")
	}
	auditJobResources, err := dataDeletionAuditJobResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "failed generating deletion audit job resources")
	}
	jobResources := append(deletionJobResources, auditJobResources...)

	var projects []*project.Project
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "delta-lake-deletion",
		ResourceGroups: map[string][]tf.Resource{
			"job":                 jobResources,
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
	})

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
			"infra_remote": instanceProfileResources,
		})
	}
	return projects, nil
}

func buildJobClusters() []*databricks.JobCluster {
	jobClusters := []*databricks.JobCluster{}
	cluster := databricks.ClusterSpec{}

	for i := 0; i < jobClusterCount; i++ {
		jc := databricks.JobCluster{
			JobClusterKey: fmt.Sprintf("%s-%d", DeletionInstanceProfile, i),
			NewCluster:    &cluster,
		}
		jobClusters = append(jobClusters, &jc)
	}
	return jobClusters
}

func setUpstreamDependencies(tasks []*databricks.JobTaskSettings, dependentTasks []*databricks.JobTaskSettings) []*databricks.JobTaskSettings {
	for _, t := range tasks {
		for _, dt := range dependentTasks {
			t.DependsOn = append(t.DependsOn, &databricks.TaskDependency{TaskKey: dt.TaskKey})
		}
	}
	return tasks
}

func buildJobTaskSettings(plans []deltalakedeletion.DeletionPlan, scriptUrl string, priorityTag string) ([]*databricks.JobTaskSettings, error) {

	planBatches := make([][]deltalakedeletion.DeletionPlan, (len(plans)/plansBatchSize)+1)
	for idx, p := range plans {
		planBatches[idx%len(planBatches)] = append(planBatches[idx%len(planBatches)], p)
	}

	tasks := []*databricks.JobTaskSettings{}
	for idx, b := range planBatches {
		planJson, err := json.Marshal(b)
		if err != nil {
			return nil, oops.Wrapf(err, "error marshalling deletion plan to json")
		}

		nbt := databricks.NotebookTask{
			NotebookPath: scriptUrl,
			BaseParameters: map[string]string{
				"org_ids":        "[]",
				"retain_days":    "",
				"dryrun":         "true",
				"deletion_plans": strings.ReplaceAll(string(planJson), "\"", "\\\""), // [][]*TableDeletionSpec as json
			},
			Source: databricks.DefaultTaskSource,
		}

		t := databricks.JobTaskSettings{
			TaskKey:                      fmt.Sprintf("plan-%s-%d", priorityTag, idx),
			TimeoutSeconds:               28800, // 8 hours
			MaxRetries:                   1,     // retry once in case of concurrent write exceptions
			MinRetryIntervalMilliseconds: 30000, // 30 seconds
			RetryOnTimeout:               true,
			NotebookTask:                 &nbt,
		}
		tasks = append(tasks, &t)
	}
	return tasks, nil
}

func dataDeletionJobResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	notebookConfig := dataplatformresource.NotebookConfig{
		FilePath: filepath.Join(
			tf.LocalId("backend_root").Reference(),
			"python3/samsaradev/infra/dataplatform/deltalakedeletion",
			DeletionScriptFileName,
		),
		NotebookPath: filepath.Join("/backend/dataplatform", strings.TrimSuffix(filepath.Base(DeletionScriptFileName), filepath.Ext(DeletionScriptFileName))),
		Language:     databricks.NotebookLanguagePython,
		Owner:        team.DataPlatform,
	}

	// Note: This notebook is in a job project, not a notebooks project, so we create
	// the resource directly without using the Notebook() helper. The helper adds a
	// depends_on for the directory resource which doesn't exist in this project.
	notebookResource := &databricksresource.Notebook{
		ResourceName: tf.SanitizeResourceName(strings.TrimPrefix(notebookConfig.NotebookPath, "/")),
		Path:         notebookConfig.NotebookPath,
		Language:     databricks.NotebookLanguagePython,
		Content:      fmt.Sprintf(`${file("%s")}`, notebookConfig.FilePath),
	}

	resources = append(resources, notebookResource)

	emailAlerts := []string{team.DataPlatform.SlackAlertsChannelEmail.Email}

	deletionChain, err := deltalakedeletion.GenerateDeletionChain(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	p1Tasks, err := buildJobTaskSettings(deletionChain.PriorityLevelOne, notebookConfig.NotebookPath, "p1")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	p2Tasks, err := buildJobTaskSettings(deletionChain.PriorityLevelTwo, notebookConfig.NotebookPath, "p2")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	p3Tasks, err := buildJobTaskSettings(deletionChain.PriorityLevelThree, notebookConfig.NotebookPath, "p3")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	p2Tasks = setUpstreamDependencies(p2Tasks, p1Tasks)
	p3Tasks = setUpstreamDependencies(p3Tasks, p2Tasks)

	allTasks := append(p1Tasks, p2Tasks...)
	allTasks = append(allTasks, p3Tasks...)

	jobClusters := buildJobClusters()
	for idx, _ := range allTasks {
		jobClusterId := idx % jobClusterCount
		allTasks[idx].JobClusterKey = fmt.Sprintf("%s-%d", DeletionInstanceProfile, jobClusterId)
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal for region %s", config.Region)
	}
	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	overrideSparkConf := map[string]string{}
	overrideSparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
	overrideSparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

	job := dataplatformresource.JobSpec{
		Name:         "delta-lake-deletion",
		Region:       config.Region,
		Owner:        team.DataPlatform,
		SparkVersion: sparkversion.DeltaLakeDeletionDbrVersion,
		SparkConf: dataplatformresource.SparkConf{
			Overrides: overrideSparkConf,
		}.ToMap(),
		Profile:                      tf.LocalId("instance_profile").Reference(),
		MaxWorkers:                   DeletionJobMaxWorkers,
		DriverNodeType:               "rd-fleet.2xlarge",
		WorkerNodeType:               "rd-fleet.2xlarge",
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.DeltaLakeDeletion,
		EmailNotifications:           emailAlerts,
		Format:                       databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		JobClusters:         jobClusters,
		Tasks:               allTasks,
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := job.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := job.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, jobResource, permissionsResource)

	return resources, nil
}

func dataDeletionAuditJobResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	jobName := "delta-lake-deletion-audit"

	script, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/deltalakedeletion/delta_lake_deletion_audit.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, script)

	spt := databricks.SparkPythonTask{
		PythonFile: script.URL(),
	}
	t := databricks.JobTaskSettings{
		TaskKey:                      jobName,
		JobClusterKey:                jobName,
		TimeoutSeconds:               28800, // 8 hours
		MaxRetries:                   1,     // retry once in case of concurrent write exceptions
		MinRetryIntervalMilliseconds: 30000, // 30 seconds
		RetryOnTimeout:               true,
		SparkPythonTask:              &spt,
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal for region %s", config.Region)
	}

	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	overrideSparkConf := map[string]string{}
	overrideSparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
	overrideSparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
	overrideSparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

	emailAlerts := []string{team.DataPlatform.SlackAlertsChannelEmail.Email}
	job := dataplatformresource.JobSpec{
		Name:         jobName,
		Region:       config.Region,
		Owner:        team.DataPlatform,
		SparkVersion: sparkversion.DeltaLakeDeletionDbrVersion,
		SparkConf: dataplatformresource.SparkConf{
			Overrides: overrideSparkConf,
		}.ToMap(),
		Profile:                      tf.LocalId("instance_profile").Reference(),
		MaxWorkers:                   2,
		DriverNodeType:               "rd-fleet.2xlarge",
		WorkerNodeType:               "rd-fleet.2xlarge",
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.DeltaLakeDeletion,
		EmailNotifications:           emailAlerts,
		Format:                       databricks.MultiTaskKey,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		Tasks: []*databricks.JobTaskSettings{&t},
		JobClusters: []*databricks.JobCluster{
			{
				JobClusterKey: jobName,
				NewCluster:    &databricks.ClusterSpec{},
			},
		},
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := job.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := job.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, jobResource, permissionsResource)

	return resources, nil

}
