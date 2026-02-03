package dataplatformprojects

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type NotebookMetadataJob struct {
	Name                    string                    `json:"name"`
	Cron                    string                    `json:"cron,omitempty"`
	Regions                 []string                  `json:"regions,omitempty"`
	TimeoutSeconds          int                       `json:"timeout_seconds,omitempty"`
	Parameters              map[string]string         `json:"parameters,omitempty"`
	DriverNodeTypeId        string                    `json:"driver_node_type_id,omitempty"`
	NodeTypeId              string                    `json:"node_type_id,omitempty"`
	MaxWorkers              int                       `json:"max_workers,omitempty"`
	MinWorkers              int                       `json:"min_workers,omitempty"`
	SparkVersion            sparkversion.SparkVersion `json:"spark_version,omitempty"`
	SparkConfOverrides      map[string]string         `json:"spark_conf_overrides,omitempty"`
	MaxRetries              int                       `json:"max_retries,omitempty"`
	MinRetryIntervalSeconds int                       `json:"min_retry_interval_seconds,omitempty"`
	RndAllocation           *float64                  `json:"rnd_allocation,omitempty"`
	// Only specify the base filename. The tf generation will handle populating the correct region-ified path.
	// Remember to upload the init script to S3 for both the US and EU if the job is enabled in both regions.
	// All init scripts should be uploaded to "samsara-[eu-]dataplatform-deployed-artifacts/init_scripts/[base_filename.sh]"
	InitScripts []string `json:"init_scripts,omitempty"`
	// LokiSettingsEnabled is a flag to enable Loki settings for the job.
	LokiSettingsEnabled bool `json:"loki_settings_enabled,omitempty"`
	// UnityCatalogSetting contains settings related to Unity Catalog for the job.
	// These settings only apply when Unity Catalog is enabled for the job.
	UnityCatalogSetting *UnityCatalogSetting `json:"unity_catalog_setting,omitempty"`
	// SloConfig defines SLO thresholds for the job to power time_since_last_success monitoring.
	SloConfig *NotebookSloConfig `json:"slo_config,omitempty"`
}

// UnityCatalogSetting contains configuration related to Unity Catalog for notebook jobs.
type UnityCatalogSetting struct {
	// DataSecurityMode specifies the data security mode to use for the job.
	// When omitted, the default SINGLE_USER (Dedicated) mode is used.
	// Valid values: "SINGLE_USER" (Dedicated, formerly Single user), "USER_ISOLATION" (Standard, formerly Shared).
	DataSecurityMode string `json:"data_security_mode,omitempty"`
}

// NotebookSloConfig defines SLO settings for notebook jobs.
type NotebookSloConfig struct {
	LowUrgencyThresholdHours    int64 `json:"low_urgency_threshold_hours,omitempty"`
	BusinessHoursThresholdHours int64 `json:"business_hours_threshold_hours,omitempty"`
	HighUrgencyThresholdHours   int64 `json:"high_urgency_threshold_hours,omitempty"`
}

func (c *NotebookSloConfig) ToJobSlo() (*dataplatformconsts.JobSlo, error) {
	if c == nil {
		return nil, nil
	}
	if c.LowUrgencyThresholdHours == 0 &&
		c.BusinessHoursThresholdHours == 0 &&
		c.HighUrgencyThresholdHours == 0 {
		return nil, nil
	}
	if c.LowUrgencyThresholdHours < 0 ||
		c.BusinessHoursThresholdHours < 0 ||
		c.HighUrgencyThresholdHours < 0 {
		return nil, oops.Errorf("slo_config hours must be non-negative")
	}
	return &dataplatformconsts.JobSlo{
		LowUrgencyThresholdHours:    c.LowUrgencyThresholdHours,
		BusinessHoursThresholdHours: c.BusinessHoursThresholdHours,
		HighUrgencyThresholdHours:   c.HighUrgencyThresholdHours,
	}, nil
}

type NotebookMetadata struct {
	Owner                  string                 `json:"owner"`
	EmailNotifications     []string               `json:"email_notifications"`
	Jobs                   []*NotebookMetadataJob `json:"jobs,omitempty"`
	PythonLibraries        []string               `json:"python_libraries,omitempty"`
	DefaultLibrariesToSkip []string               `json:"default_libraries_to_skip,omitempty"`
}

func ScheduledNotebookProjects(providerGroup string) ([]*project.Project, error) {
	// Create a project for each subdirectory in $BACKEND_ROOT/dataplatform/notebooks.
	var dirs []string
	if err := filepath.Walk(
		dataplatformterraformconsts.NotebooksRoot,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return oops.Wrapf(err, "")
			}
			if !info.IsDir() {
				return nil
			}
			if strings.HasPrefix(filepath.Base(path), ".") {
				// Ignore hidden dot files.
				return nil
			}
			relPath, err := filepath.Rel(dataplatformterraformconsts.NotebooksRoot, path)
			if err != nil {
				return oops.Wrapf(err, "filepath.Rel: %s", path)
			}
			dirs = append(dirs, relPath)
			return nil
		}); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var projects []*project.Project
	projectDir := make(map[string]string)
	for _, dir := range dirs {
		p, err := scheduledNotebooksProject(providerGroup, dir)
		if err != nil {
			return nil, oops.Wrapf(err, "scheduledNotebooksProject: %s", dir)
		}
		if p == nil {
			// The directory is empty.
			continue
		}
		if other, ok := projectDir[p.Name]; ok {
			return nil, oops.Errorf("directory %s and %s map to the same project name %s. please rename one of the directories.", dir, other, p.Name)
		}
		projects = append(projects, p)
		projectDir[p.Name] = dir
	}
	return projects, nil
}

func scheduledNotebooksProject(providerGroup string, subdir string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Find all source code and metadata files.
	sourceFiles := make(map[string]struct{})
	metadataFiles := make(map[string]struct{})

	files, err := ioutil.ReadDir(filepath.Join(
		dataplatformterraformconsts.NotebooksRoot, subdir,
	))
	if err != nil {
		return nil, oops.Wrapf(err, "list: %s", subdir)
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		switch filepath.Base(file.Name()) {
		case "CODEREVIEW",
			"notebook_metadata_schema.json",
			"README.md",
			".DS_Store":
			continue
		}

		path := filepath.Join(subdir, file.Name())
		ext := filepath.Ext(file.Name())
		switch ext {
		case ".sql", ".py":
			sourceFiles[path] = struct{}{}
		case ".json":
			if !strings.HasSuffix(path, ".metadata.json") {
				return nil, oops.Errorf("metadata files must end in .metadata.json; unsupported file: %s", path)
			}
			metadataFiles[path] = struct{}{}
		default:
			return nil, oops.Errorf("unsupported file extension: %s. supported types are: [.py, .sql, .metadata.json]", path)
		}
	}

	directoryResource := &databricksresource.Directory{
		Path: "/" + filepath.Join("backend", subdir),
	}

	// Read each notebook file and their metadata to build configs.
	var directoryOwner *components.TeamInfo
	var notebookConfigs []dataplatformresource.NotebookConfig
	var jobConfigs []dataplatformresource.NotebookJobConfig
	for source := range sourceFiles {
		// Every notebook source file must have a metadata file.
		metadataFile := strings.TrimSuffix(source, filepath.Ext(source)) + ".metadata.json"
		if _, ok := metadataFiles[metadataFile]; !ok {
			return nil, oops.Errorf("notebook file %s does not have a corresponding metadata file", source)
		}
		delete(metadataFiles, metadataFile)

		metadataBytes, err := ioutil.ReadFile(filepath.Join(
			dataplatformterraformconsts.NotebooksRoot, metadataFile,
		))
		if err != nil {
			return nil, oops.Wrapf(err, "read: %s", metadataFile)
		}
		var metadata NotebookMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			return nil, oops.Wrapf(err, "")
		}

		owner := team.TeamByName[metadata.Owner]
		if owner.Name() == "" {
			return nil, oops.Errorf("%s specifies owner as %q but it's not found in team directory", source, metadata.Owner)
		}

		if directoryOwner == nil {
			directoryOwner = &owner
		} else if directoryOwner.TeamName != owner.TeamName {
			return nil, oops.Errorf("Notebook directory %s has notebooks with different owners. %s and %s. Notebooks in a directory must all be owned by a single team.", subdir, directoryOwner.TeamName, owner.TeamName)
		}

		var lang databricks.NotebookLanguage
		switch filepath.Ext(source) {
		case ".sql":
			lang = databricks.NotebookLanguageSQL
		case ".py":
			lang = databricks.NotebookLanguagePython
		default:
			return nil, oops.Errorf("unknown ext: %s", source)
		}

		notebook := dataplatformresource.NotebookConfig{
			FilePath: filepath.Join(
				tf.LocalId("backend_root").Reference(),
				"dataplatform",
				"notebooks",
				source,
			),
			NotebookPath:           filepath.Join(directoryResource.Path, strings.TrimSuffix(filepath.Base(source), filepath.Ext(source))),
			Language:               lang,
			Owner:                  owner,
			EmailNotifications:     metadata.EmailNotifications,
			PythonLibraries:        metadata.PythonLibraries,
			DefaultLibrariesToSkip: metadata.DefaultLibrariesToSkip,
		}
		notebookConfigs = append(notebookConfigs, notebook)

		jobNames := make(map[string]struct{}, len(metadata.Jobs))
		for i, jobMetadata := range metadata.Jobs {
			if jobMetadata.Name == "" {
				return nil, oops.Errorf("notebook %s job %d must be named", source, i)
			}

			if _, ok := jobNames[jobMetadata.Name]; ok {
				return nil, oops.Errorf("notebook %s has a duplicate job name: %s. Job names must be unique", source, jobMetadata.Name)
			} else {
				jobNames[jobMetadata.Name] = struct{}{}
			}

			if len(jobMetadata.Regions) == 0 {
				return nil, oops.Errorf("Region field not populated in notebook metadata file: %s", metadataFile)
			}
			for _, region := range jobMetadata.Regions {
				switch region {
				case "us-west-2", "eu-west-1", "ca-central-1":
				default:
					return nil, oops.Errorf("%s specifies invalid region %q", source, region)
				}
			}

			found := false
			for _, region := range jobMetadata.Regions {
				if region == config.Region {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			timeout := jobMetadata.TimeoutSeconds
			if timeout == 0 {
				timeout = 3600
			}

			driverNodeTypeId := jobMetadata.DriverNodeTypeId
			if driverNodeTypeId != "" {
				if ok, err := dataplatformresource.ValidateNodeTypeId(driverNodeTypeId); !ok {
					return nil, oops.Wrapf(err, "invalid node type id for notebook %s", notebook.FilePath)
				}
			}

			nodeTypeId := jobMetadata.NodeTypeId
			if nodeTypeId != "" {
				if ok, err := dataplatformresource.ValidateNodeTypeId(nodeTypeId); !ok {
					return nil, oops.Wrapf(err, "invalid node type id for notebook %s", notebook.FilePath)
				}
			}

			maxWorkers := jobMetadata.MaxWorkers
			if maxWorkers == 0 {
				maxWorkers = 4
			}

			minWorker := jobMetadata.MinWorkers
			if minWorker == 0 {
				minWorker = 1
			}

			// Initialize sparkVersion with the default value from jobMetadata.
			var sparkVersion = jobMetadata.SparkVersion

			// Check for special cases where SparkVersion is not directly taken from jobMetadata.
			if jobMetadata.SparkVersion == "" {
				return nil, oops.Errorf("notebook metadata %s job %s must have the spark version defined", metadataFile, jobMetadata.Name)
			} else if jobMetadata.SparkVersion == "datastreams" {
				sparkVersion = sparkversion.DataStreamsNotebookJobSparkVersion
			} else if jobMetadata.SparkVersion == "default" {
				sparkVersion = sparkversion.DefaultNotebookJobSparkVersion
			} else if jobMetadata.SparkVersion == "next" {
				sparkVersion = sparkversion.NextNotebookJobSparkVersion
			}

			// All backend notebooks migrated to use the unity-catalog-cluster profile, except for 2 notebooks (ifta & sites),
			// which are blocked by a bug in DBX where credentials are not passed to executors. Keep these 2 jobs on the
			// dataprep-cluster profile until the bug is fixed.
			// See JIRA ticket https://samsara.atlassian-us-gov-mod.net/browse/DAT-417 for more details.
			profile := "unity-catalog-cluster"
			useUnityCatalog := true
			sanitizedJobName := tf.SanitizeResourceName(notebook.NotebookPath + "_" + jobMetadata.Name)
			if sanitizedJobName == "backend_ifta_ifta_integrations_file_generation_generate" || sanitizedJobName == "backend_dataprep_ml_sites_images_restate-weekly" {
				profile = "dataprep-cluster"
				useUnityCatalog = false
			}

			isE2 := dataplatformresource.IsE2ProviderGroup(providerGroup)
			profileArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, profile)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}

			rndAllocation := float64(1)
			if jobMetadata.RndAllocation != nil {
				rndAllocation = *jobMetadata.RndAllocation
			}

			// By default we set is-production-job to false for notebook jobs but for those that
			// notebooks have specified rnd-allocation as 0, we set the tag to true for
			// accurate representation.

			isProductionJob := rndAllocation == 0

			sparkConfOverrides := jobMetadata.SparkConfOverrides

			if sparkConfOverrides == nil {
				sparkConfOverrides = make(map[string]string)
			}
			sparkConfOverrides["spark.databricks.sql.initial.catalog.name"] = "hive_metastore"

			var initScripts []string
			for _, initScript := range jobMetadata.InitScripts {
				if useUnityCatalog {
					initScripts = append(initScripts, databricksinstaller.GetInitScriptVolumeDestination(initScript))
				} else {
					initScripts = append(initScripts, dataplatformprojecthelpers.InitScriptS3URI(config.Region, initScript))
				}

				// If the Apache Sedona library is being added, also add the required spark configs.
				if initScript == "sedona-shaded-1.4.1-init.sh" {
					if sparkConfOverrides == nil {
						sparkConfOverrides = make(map[string]string)
					}
					sparkConfOverrides["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
					sparkConfOverrides["spark.kryo.registrator"] = "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
					sparkConfOverrides["spark.sql.extensions"] = "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
				}
			}

			var jobSloConfig *dataplatformconsts.JobSlo
			if jobMetadata.SloConfig != nil {
				sloConfig, err := jobMetadata.SloConfig.ToJobSlo()
				if err != nil {
					return nil, oops.Wrapf(err, "invalid slo_config in metadata %s job %s", metadataFile, jobMetadata.Name)
				}
				jobSloConfig = sloConfig
			}

			jobConfig := dataplatformresource.NotebookJobConfig{
				Owner:                  owner,
				NameSuffix:             jobMetadata.Name,
				Notebook:               notebook,
				Region:                 config.Region,
				TimeoutSeconds:         timeout,
				DriverNodeTypeId:       jobMetadata.DriverNodeTypeId,
				NodeTypeId:             jobMetadata.NodeTypeId,
				MaxWorkers:             maxWorkers,
				MinWorkers:             minWorker,
				JobOwnerUser:           config.MachineUsers.CI.Email,
				EmailNotifications:     notebook.EmailNotifications,
				Profile:                profileArn,
				PythonLibraries:        notebook.PythonLibraries,
				DefaultLibrariesToSkip: notebook.DefaultLibrariesToSkip,
				SparkVersion:           sparkVersion,
				SparkConf:              sparkConfOverrides,
				Parameters:             jobMetadata.Parameters,
				MaxRetries:             jobMetadata.MaxRetries,
				MinRetryInterval:       time.Duration(jobMetadata.MinRetryIntervalSeconds) * time.Second,
				RndAllocation:          rndAllocation,
				IsProductionJob:        isProductionJob,
				InitScripts:            initScripts,
				LokiSettingsEnabled:    jobMetadata.LokiSettingsEnabled,
				SloConfig:              jobSloConfig,
			}

			// Add Unity Catalog settings if specified
			if jobMetadata.UnityCatalogSetting != nil {
				switch jobMetadata.UnityCatalogSetting.DataSecurityMode {
				case "", databricksresource.DataSecurityModeSingleUser, databricksresource.DataSecurityModeUserIsolation:
				default:
					return nil, oops.Errorf("invalid data security mode in metadata: %s", jobMetadata.UnityCatalogSetting.DataSecurityMode)
				}

				jobConfig.UnityCatalogSetting = dataplatformresource.UnityCatalogSetting{
					DataSecurityMode: jobMetadata.UnityCatalogSetting.DataSecurityMode,
				}
			}

			if isE2 {
				jobConfig.Cron = jobMetadata.Cron
			}
			jobConfigs = append(jobConfigs, jobConfig)
		}
	}

	// No dangling metadata file allowed.
	if len(metadataFiles) != 0 {
		return nil, oops.Errorf("extra metadata files: %v", metadataFiles)
	}

	notebooks := make(map[string][]tf.Resource)
	for _, config := range notebookConfigs {
		resources, err := dataplatformresource.Notebook(config)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		notebooks[filepath.Base(config.NotebookPath)] = resources
	}

	notebookJobs := make(map[string][]tf.Resource)
	for _, config := range jobConfigs {
		resources, err := dataplatformresource.NotebookJob(config)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		notebookJobs[filepath.Base(config.Notebook.NotebookPath)] = append(notebookJobs[filepath.Base(config.Notebook.NotebookPath)], resources...)
	}

	if len(notebooks) == 0 {
		return nil, nil
	}

	terraformTeamPipeline := directoryOwner.TeamName

	// Check if team has a manual Data Platform terraform override
	if override, ok := dataplatformterraformconsts.DataPlatformTerraformOverride[directoryOwner.TeamName]; ok {
		terraformTeamPipeline = override.TeamName
	}

	// Teams without a Terraform pipeline will have their notebooks go through the Data Platform Pipeline
	if _, ok := team.AllTeamsWithTerraformPipelines[terraformTeamPipeline]; !ok {
		terraformTeamPipeline = team.DataPlatform.TeamName
	}

	p := &project.Project{
		RootTeam: terraformTeamPipeline,
		Provider: providerGroup,
		Class:    "notebooks",
		Name:     strings.Replace(subdir, "/", "_", -1),
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
			"directory":           {directoryResource},
		},
	}
	// we add notebooks and notebookJobs
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, notebooks, notebookJobs)

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}
