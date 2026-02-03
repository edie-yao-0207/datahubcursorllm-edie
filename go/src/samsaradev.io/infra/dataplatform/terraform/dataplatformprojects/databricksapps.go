package dataplatformprojects

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func DatabricksAppsProjects(providerGroup string) ([]*project.Project, error) {
	// Each root-level directory represents a databricks app project.
	// Create a project for each immediate child subdirectory in $BACKEND_ROOT/dataplatform/databricksapps.
	var dirs []string
	if err := filepath.Walk(
		dataplatformterraformconsts.DatabricksAppsRoot,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return oops.Wrapf(err, "")
			}
			if !info.IsDir() {
				return nil
			}
			if strings.HasPrefix(filepath.Base(path), ".") {
				// Ignore hidden dot files.
				return filepath.SkipDir
			}
			relPath, err := filepath.Rel(dataplatformterraformconsts.DatabricksAppsRoot, path)
			if err != nil {
				return oops.Wrapf(err, "filepath.Rel: %s", path)
			}

			// Count the depth by checking path separators in the relative path
			depth := strings.Count(relPath, string(filepath.Separator))
			if relPath == "." {
				depth = 0 // Root directory
			}

			if depth == 0 && relPath != "." {
				// This is the root directory, continue traversing
				dirs = append(dirs, relPath)
				return nil
			}
			return nil
		}); err != nil {
		return nil, oops.Wrapf(err, "")
	}

	var projects []*project.Project
	projectDir := make(map[string]string)
	for _, dir := range dirs {
		p, err := databricksAppProject(providerGroup, dir)
		if err != nil {
			return nil, oops.Wrapf(err, "databricksAppProject: %s", dir)
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

func databricksAppProject(providerGroup string, subdir string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Find all project source code files.
	sourceFiles := make(map[string]struct{})

	rootDir := filepath.Join(dataplatformterraformconsts.DatabricksAppsRoot, subdir)
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return oops.Wrapf(err, "")
		}

		// Skip directories - we only want files
		if info.IsDir() {
			return nil
		}

		// Skip CODEREVIEW files
		if filepath.Base(path) == "CODEREVIEW" {
			return nil
		}

		// Convert to relative path from DatabricksAppsRoot
		relPath, err := filepath.Rel(dataplatformterraformconsts.DatabricksAppsRoot, path)
		if err != nil {
			return oops.Wrapf(err, "filepath.Rel: %s", path)
		}

		sourceFiles[relPath] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, oops.Wrapf(err, "walk: %s", subdir)
	}

	directoryResource := &databricksresource.Directory{
		Path: "/" + filepath.Join("backend", subdir),
	}

	resources := []tf.Resource{}
	for source := range sourceFiles {
		workspaceFile := databricksresource_official.DatabricksWorkspaceFile{
			Path: fmt.Sprintf("/backend/databricksapps/%s", source),
			Source: filepath.Join(
				tf.LocalId("backend_root").Reference(),
				"dataplatform",
				"databricksapps",
				source,
			),
		}
		workspaceFile.DependsOn = []string{
			fmt.Sprintf("databricks_directory.%s", directoryResource.ResourceId().Name),
		}

		permissions, err := permissionsResource(workspaceFile)
		if err != nil {
			return nil, oops.Wrapf(err, "permissionsResource: %s", source)
		}

		resources = append(resources, &workspaceFile, permissions)
	}
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "databricksapps",
		Name:     strings.Replace(subdir, "/", "_", -1),
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
			"directory":           {directoryResource},
		},
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"workspace_files": resources,
	})

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}

func permissionsResource(file databricksresource_official.DatabricksWorkspaceFile) (*databricksresource_official.Permissions, error) {

	return &databricksresource_official.Permissions{
		ResourceName:    file.ResourceId().Name,
		WorkspaceFileId: file.ResourceId().ReferenceAttr("object_id"),
		AccessControls: []*databricksresource_official.AccessControl{
			{
				GroupName:       dataplatformterraformconsts.AllSamsaraUsersGroup,
				PermissionLevel: databricks.PermissionLevelCanRead,
			},
		},
	}, nil
}
