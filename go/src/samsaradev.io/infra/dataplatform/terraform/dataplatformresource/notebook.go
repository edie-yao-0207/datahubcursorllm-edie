package dataplatformresource

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/team/components"
)

type NotebookConfig struct {
	FilePath               string
	NotebookPath           string
	Language               databricks.NotebookLanguage
	Owner                  components.TeamInfo
	EmailNotifications     []string
	PythonLibraries        []string
	DefaultLibrariesToSkip []string
}

// Notebook creates a databricks_notebook resource with an MD5 hash local value
// to ensure terraform diffs are detected when the notebook source file changes.
func Notebook(c NotebookConfig) ([]tf.Resource, error) {
	// Extract the backend-relative path from the terraform expression.
	// FilePath is like "${local.backend_root}/dataplatform/notebooks/foo/bar.py"
	// We need just "dataplatform/notebooks/foo/bar.py" for md5Hash.
	backendPath := strings.TrimPrefix(c.FilePath, tf.LocalId("backend_root").Reference()+"/")

	resourceName := tf.SanitizeResourceName(strings.TrimPrefix(c.NotebookPath, "/"))

	// Compute the MD5 hash of the notebook source file at terraform generation time.
	// This ensures that when the notebook content changes, the generated terraform
	// also changes, triggering a terraform diff in CI even if only the source file
	// (not the metadata) was modified.
	sourceHash, err := md5Hash(backendPath)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to compute source hash for notebook %s", c.FilePath)
	}

	// Create a local value with the source hash. This local value is included in
	// the generated terraform file, ensuring that any change to the notebook source
	// file results in a change to the terraform file itself.
	sourceHashLocal := &genericresource.StringLocal{
		Name:  fmt.Sprintf("%s_source_hash", resourceName),
		Value: fmt.Sprintf("%q", sourceHash),
	}

	notebook := &databricksresource.Notebook{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					databricksresource.DirectoryResourceId(tf.SanitizeResourceName(filepath.Dir(c.NotebookPath))).String(),
				},
			},
		},
		ResourceName: resourceName,
		Path:         c.NotebookPath,
		Language:     c.Language,
		Content:      fmt.Sprintf(`${file("%s")}`, c.FilePath),
	}
	return []tf.Resource{sourceHashLocal, notebook}, nil
}

type NotebookJobConfig struct {
	Owner                  components.TeamInfo
	Notebook               NotebookConfig
	NameSuffix             string
	Region                 string
	Cron                   string
	TimeoutSeconds         int
	DriverNodeTypeId       string
	NodeTypeId             string
	MinWorkers             int
	MaxWorkers             int
	Profile                string
	JobOwnerUser           string
	EmailNotifications     []string
	SparkConf              map[string]string
	PythonLibraries        []string
	DefaultLibrariesToSkip []string
	SparkVersion           sparkversion.SparkVersion
	Parameters             map[string]string
	MaxRetries             int
	MinRetryInterval       time.Duration
	RndAllocation          float64
	IsProductionJob        bool
	InitScripts            []string
	LokiSettingsEnabled    bool
	UnityCatalogSetting    UnityCatalogSetting
	SloConfig              *dataplatformconsts.JobSlo
}

func unityCatalogSettingWithDefaults(ucSetting UnityCatalogSetting, jobOwner string) (UnityCatalogSetting, error) {
	switch ucSetting.DataSecurityMode {
	case databricksresource.DataSecurityModeSingleUser, "": // Default to single user, backwards compatible.
		return UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   jobOwner,
		}, nil
	case databricksresource.DataSecurityModeUserIsolation:
		// No need to set SingleUserName for Standard mode.
		return UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeUserIsolation,
		}, nil
	default:
		return UnityCatalogSetting{}, oops.Errorf("invalid data security mode in metadata: %s", ucSetting.DataSecurityMode)
	}
}

func NotebookJob(c NotebookJobConfig) ([]tf.Resource, error) {
	name := tf.SanitizeResourceName(c.Notebook.NotebookPath + "_" + c.NameSuffix)
	pyPILibraries := make([]PyPIName, len(c.PythonLibraries))
	for idx, lib := range c.PythonLibraries {
		pyPILibraries[idx] = PyPIName(lib)
	}

	initScripts := []string{
		databricksinstaller.BigQueryCredentialsInitScript(),
		databricksinstaller.GetSparkRulesInitScript(),
	}
	for _, script := range c.InitScripts {
		initScripts = append(initScripts, script)
	}

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(c.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal app id for region %s", c.Region)
	}

	ucSettings, err := unityCatalogSettingWithDefaults(c.UnityCatalogSetting, ciServicePrincipalAppId)
	if err != nil {
		return nil, err
	}

	overrideSparkConf := c.SparkConf

	overrideSparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
	overrideSparkConf["spark.databricks.acl.needAdminPermissionToViewLogs"] = "false" // Allows non-admins to view logs of notebook jobs

	if !c.LokiSettingsEnabled {
		overrideSparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
		overrideSparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
		overrideSparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"
	}

	jobSpec := JobSpec{
		Name:           name,
		NotebookPath:   c.Notebook.NotebookPath,
		Region:         c.Region,
		Owner:          c.Owner,
		Cron:           c.Cron,
		TimeoutSeconds: c.TimeoutSeconds,
		Profile:        c.Profile,
		DriverNodeType: c.DriverNodeTypeId,
		WorkerNodeType: c.NodeTypeId,
		SparkConf: SparkConf{
			Region:                    c.Region,
			EnableCaching:             true,
			EnableBigQuery:            true,
			EnablePlaygroundWarehouse: true,
			EnableExtension:           true,
			DisableQueryWatchdog:      true,
			Overrides:                 overrideSparkConf,
			DataSecurityMode:          ucSettings.DataSecurityMode,
		}.ToMap(),
		SparkEnvVars: map[string]string{
			"GOOGLE_CLOUD_PROJECT": "samsara-data",
		},
		EmailNotifications:           c.EmailNotifications,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		SparkVersion:                 c.SparkVersion,
		VolumesInitScripts:           initScripts,
		MinWorkers:                   c.MinWorkers,
		MaxWorkers:                   c.MaxWorkers,
		Libraries: JobLibraryConfig{
			LoadDefaultLibraries:   true,
			PyPIs:                  pyPILibraries,
			DefaultLibrariesToSkip: c.DefaultLibrariesToSkip,
		},
		RnDCostAllocation: c.RndAllocation,
		IsProduction:      c.IsProductionJob,
		ParameterMap:      c.Parameters,
		JobType:           dataplatformconsts.ScheduledNotebook,
		MaxRetries:        c.MaxRetries,
		MinRetryInterval:  c.MinRetryInterval,
		Format:            databricks.MultiTaskKey,
		SloConfig:         c.SloConfig,
		JobTags: map[string]string{
			"format": databricks.MultiTaskKey,
		},
		UnityCatalogSetting: ucSettings,
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	job, err := jobSpec.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	jobPermissions, err := jobSpec.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return []tf.Resource{job, jobPermissions}, nil
}
