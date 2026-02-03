package dynamodbdeltalake

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

const DynamodbDeltaLakeResourceBaseName = "dynamodbdeltalake"

func DynamodbDeltaLakeProjects(databricksProviderGroup string) ([]*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	var projects []*project.Project
	instancePool, err := poolProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "building pool projects")
	}
	projects = append(projects, instancePool.project)

	ingestionResourceGroups := map[string][]tf.Resource{}
	ingestionResourceGroups["s3_resources"] = CreateDynamodbDeltaLakeS3Resources(config)

	ingestionProject := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDynamodbDeltaLakeProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           DynamodbDeltaLakeResourceBaseName,
		Name:            DynamodbDeltaLakeResourceBaseName + "-infra",
		ResourceGroups:  project.MergeResourceGroups(ingestionResourceGroups),
	}

	projects = append(projects, ingestionProject)

	dynamoDbImportArn, err := dataplatformresource.InstanceProfileArn(databricksProviderGroup, "dynamodb-import-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	dynamoDbUcImportArn, err := dataplatformresource.InstanceProfileArn(databricksProviderGroup, "dynamodb-import-uc-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	sharedRemotes := remoteResources{
		infra: []tf.Resource{
			&genericresource.StringLocal{
				Name:  "instance_profile",
				Value: fmt.Sprintf(`"%s"`, dynamoDbImportArn),
			},
			&genericresource.StringLocal{
				Name:  "uc_instance_profile",
				Value: fmt.Sprintf(`"%s"`, dynamoDbUcImportArn),
			},
		},
		instancePool: instancePool.remoteResources,
	}

	mergeProject, err := dynamoDbMergeSparkJobProject(config, sharedRemotes)
	if err != nil {
		return nil, oops.Wrapf(err, "dynamodb merge project")
	}
	// mergeProject might be nil because we only want to create a single merge job
	// for testing.
	if mergeProject != nil {
		projects = append(projects, mergeProject)
	}

	for _, p := range projects {
		awsProviders := resource.ProjectAWSProvider(p)
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": awsProviders,
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}

	return projects, nil
}

// DynamodbDeltaLakeAwsEngCloudProjects returns the projects for the aws-eng-cloud accounts
func DynamodbDeltaLakeAwsEngCloudProjects(providerGroup string) ([]*project.Project, error) {
	var projects []*project.Project

	cdcProject, err := dynamodbCdcProject(providerGroup)

	if err != nil {
		return nil, oops.Wrapf(err, "building CDC projects")
	}

	projects = append(projects, cdcProject)

	exportProject, err := dynamodbDeltaLakeExportProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "building IAM projects")
	}
	projects = append(projects, exportProject)

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}

	return projects, nil
}
