package dataplatformprojects

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var WorkflowsRoot = filepath.Join(dataplatformterraformconsts.DataPlatformRoot, "workflows")

type WorkflowSchedule struct {
	Name                   string                                  `json:"name"`
	Regions                []string                                `json:"regions"`
	TimeoutSeconds         int                                     `json:"timeout_seconds"`
	Cron                   string                                  `json:"cron,omitempty"`
	ParameterOverrides     *databricks.OverridingParameters        `json:"parameter_overrides,omitempty"`
	WorkflowOverrides      *dataplatformresource.WorkflowOverrides `json:"workflow_overrides,omitempty"`
	PythonLibraryOverrides []string                                `json:"python_library_overrides,omitempty"`
}

type WorkflowMetadata struct {
	Owner           string              `json:"owner"`
	Schedules       []*WorkflowSchedule `json:"schedules"`
	AdminTeams      []string            `json:"admin_teams,omitempty"`
	PythonLibraries []string            `json:"python_libraries,omitempty"`
	SparkConfs      map[string]string   `json:"spark_confs,omitempty"`
}

func WorkflowProjects(providerGroup string) ([]*project.Project, error) {
	// Create a project for each subdirectory in $BACKEND_ROOT/dataplatform/workflows.
	var dirs []string
	if err := filepath.Walk(WorkflowsRoot, func(path string, info os.FileInfo, err error) error {
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
		relPath, err := filepath.Rel(WorkflowsRoot, path)
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
		p, err := workflowsProject(providerGroup, dir)
		if err != nil {
			return nil, oops.Wrapf(err, "workflowsProject: %s", dir)
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

func workflowsProject(providerGroup string, subdir string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Find all source code and metadata files.
	sourceFiles := make(map[string]struct{})
	metadataFiles := make(map[string]struct{})

	files, err := ioutil.ReadDir(filepath.Join(WorkflowsRoot, subdir))
	if err != nil {
		return nil, oops.Wrapf(err, "list: %s", subdir)
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		switch filepath.Base(file.Name()) {
		case "CODEREVIEW",
			"workflow_metadata_schema.json",
			".DS_Store":
			continue
		}

		path := filepath.Join(subdir, file.Name())
		ext := filepath.Ext(file.Name())
		if ext != ".json" {
			return nil, oops.Errorf("unsupported file extension: %s. supported types are: [.json, .metadata.json]", path)
		}
		if strings.HasSuffix(path, ".metadata.json") {
			metadataFiles[path] = struct{}{}
		} else {
			sourceFiles[path] = struct{}{}
		}
	}

	directoryResource := &databricksresource.Directory{
		Path: "/" + filepath.Join("backend", subdir),
	}

	// Read each workflow file and their metadata to build configs.
	var directoryOwner *components.TeamInfo
	// var workflowConfigs []dataplatformresource.WorkflowConfig
	var scheduleConfigs []dataplatformresource.WorkflowScheduleConfig
	for source := range sourceFiles {
		// Read an unmarshal workflow source file
		workflowBytes, err := ioutil.ReadFile(filepath.Join(WorkflowsRoot, source))
		if err != nil {
			return nil, oops.Wrapf(err, "read: %s", source)
		}
		var workflow databricks.JobSettings
		if err := json.Unmarshal(workflowBytes, &workflow); err != nil {
			return nil, oops.Wrapf(err, "%s", source)
		}
		// Every workflow source file must have a metadata file.
		metadataFile := strings.TrimSuffix(source, filepath.Ext(source)) + ".metadata.json"
		if _, ok := metadataFiles[metadataFile]; !ok {
			return nil, oops.Errorf("workflow file %s does not have a corresponding metadata file", source)
		}
		delete(metadataFiles, metadataFile)

		metadataBytes, err := ioutil.ReadFile(filepath.Join(WorkflowsRoot, metadataFile))
		if err != nil {
			return nil, oops.Wrapf(err, "read: %s", metadataFile)
		}
		var metadata WorkflowMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			return nil, oops.Wrapf(err, "cannot parse .metadata.json file for %s", source)
		}

		owner := team.TeamByName[metadata.Owner]
		if owner.Name() == "" {
			return nil, oops.Errorf("%s specifies owner as %q but it's not found in team directory", source, metadata.Owner)
		}

		if directoryOwner == nil {
			directoryOwner = &owner
		} else if directoryOwner.TeamName != owner.TeamName {
			return nil, oops.Errorf("Workflow directory %s has workflows with different owners. %s and %s. Workflows in a directory must all be owned by a single team.", subdir, directoryOwner.TeamName, owner.TeamName)
		}

		var adminTeams []components.TeamInfo
		for _, t := range metadata.AdminTeams {
			team := team.TeamByName[t]
			if team.Name() == "" {
				return nil, oops.Errorf("%s specifies an admin team as %q but it's not found in team directory", source, team.Name())
			}
			if team.Name() == owner.Name() {
				return nil, oops.Errorf("%s specifies an admin team as %q but it is already the owner", source, team.Name())
			}
			adminTeams = append(adminTeams, team)
		}

		scheduleNames := make(map[string]struct{}, len(metadata.Schedules))

		for i, schedule := range metadata.Schedules {
			if schedule.Name == "" {
				return nil, oops.Errorf("workflow %s schedule %d must be named", source, i)
			}

			if _, ok := scheduleNames[schedule.Name]; ok {
				return nil, oops.Errorf("Workflow %s has duplicate schedule name: %s. Schedule names must be unique", source, schedule.Name)
			}
			scheduleNames[schedule.Name] = struct{}{}

			if len(schedule.Regions) == 0 {
				return nil, oops.Errorf("Region field not populated in workflow metadata file: %s", metadataFile)
			}
			for _, region := range schedule.Regions {
				switch region {
				case "us-west-2", "eu-west-1", "ca-central-1":
				default:
					return nil, oops.Errorf("%s specifies invalid region %q", source, region)
				}
			}

			found := false
			for _, region := range schedule.Regions {
				if region == config.Region {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			timeout := schedule.TimeoutSeconds
			if timeout == 0 {
				return nil, oops.Errorf("workflow %s schedule %d must have a timeout set", source, i)
			}

			pypiMap := make(map[string]struct{})
			pypiLibs := []string{}
			for _, l := range schedule.PythonLibraryOverrides {
				name, _ := dataplatformresource.SplitPyPIName(l)
				pypiMap[name] = struct{}{}
				pypiLibs = append(pypiLibs, l)
			}
			for _, l := range metadata.PythonLibraries {
				name, _ := dataplatformresource.SplitPyPIName(l)
				if _, ok := pypiMap[name]; ok {
					continue
				}
				pypiLibs = append(pypiLibs, l)
			}

			isE2 := dataplatformresource.IsE2ProviderGroup(providerGroup)
			profileArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, dataplatformterraformconsts.UnityCatalogInstanceProfile)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			wsc := dataplatformresource.WorkflowScheduleConfig{
				FilePath: filepath.Join(
					tf.LocalId("backend_root").Reference(),
					"dataplatform",
					"workflows",
					source,
				),
				WorkflowPath:       filepath.Join(directoryResource.Path, strings.TrimSuffix(filepath.Base(source), filepath.Ext(source))),
				NameSuffix:         schedule.Name,
				Owner:              owner,
				AdminTeams:         adminTeams,
				Profile:            profileArn,
				Region:             config.Region,
				TimeoutSeconds:     schedule.TimeoutSeconds,
				SparkConfs:         metadata.SparkConfs,
				PythonLibraries:    pypiLibs,
				ParameterOverrides: schedule.ParameterOverrides,
				WorkflowOverrides:  schedule.WorkflowOverrides,
				Workflow:           &workflow,
			}
			if isE2 {
				wsc.Cron = schedule.Cron
			}
			scheduleConfigs = append(scheduleConfigs, wsc)

		}
	}

	// No dangling metadata file allowed.
	if len(metadataFiles) != 0 {
		return nil, oops.Errorf("extra metadata files: %v", metadataFiles)
	}

	workflows := make(map[string][]tf.Resource)
	for _, config := range scheduleConfigs {
		resources, err := dataplatformresource.WorkflowResource(config)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		workflows[filepath.Base(config.WorkflowPath)] = append(workflows[filepath.Base(config.WorkflowPath)], resources...)
	}

	if len(workflows) == 0 {
		return nil, nil
	}

	terraformTeamPipeline := directoryOwner.TeamName

	// Check if team has a manual Data Platform terraform override
	if override, ok := dataplatformterraformconsts.DataPlatformTerraformOverride[directoryOwner.TeamName]; ok {
		terraformTeamPipeline = override.TeamName
	}

	// Teams without a Terraform pipeline will have their workflows go through the Data Platform Pipeline
	if _, ok := team.AllTeamsWithTerraformPipelines[terraformTeamPipeline]; !ok {
		terraformTeamPipeline = team.DataPlatform.TeamName
	}

	p := &project.Project{
		RootTeam: terraformTeamPipeline,
		Provider: providerGroup,
		Class:    "workflows",
		Name:     strings.Replace(subdir, "/", "_", -1),
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
			"directory":           []tf.Resource{directoryResource},
		},
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, workflows)

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}
