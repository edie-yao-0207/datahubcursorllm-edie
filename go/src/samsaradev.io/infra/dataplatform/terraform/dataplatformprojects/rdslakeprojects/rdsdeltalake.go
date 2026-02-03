package rdslakeprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
)

type remoteResources struct {
	infra        []tf.Resource
	instancePool []tf.Resource
}

func AllProjects(databricksProviderGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	var projects []*project.Project

	instancePool, err := poolProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "instance pool project")
	}
	projects = append(projects, instancePool.project)

	rdsImportArn, err := dataplatformresource.InstanceProfileArn(databricksProviderGroup, "rds-import-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	rdsUcImportArn, err := dataplatformresource.InstanceProfileArn(databricksProviderGroup, "rds-import-uc-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	sharedRemotes := remoteResources{
		infra: []tf.Resource{
			&genericresource.StringLocal{
				Name:  "instance_profile",
				Value: fmt.Sprintf(`"%s"`, rdsImportArn),
			},
			&genericresource.StringLocal{
				Name:  "uc_instance_profile",
				Value: fmt.Sprintf(`"%s"`, rdsUcImportArn),
			},
		},
		instancePool: instancePool.remoteResources,
	}

	dagsterP, err := dagsterDmsConfigProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "dagster config project")
	}
	projects = append(projects, dagsterP)

	// Create IAM role project for Kinesis DMS replication (once per region)
	// This must be created outside the database loop to avoid duplicate role creation
	kinesisIAMP, err := kinesisIAMProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "kinesis iam project")
	}
	projects = append(projects, kinesisIAMP)

	// Create S3 export bucket project for Kinesis Firehose (once per region)
	// This must be created outside the database loop to avoid duplicate resources
	kinesisExportS3P, err := kinesisExportS3Project(config)
	if err != nil {
		return nil, oops.Wrapf(err, "kinesis export s3 project")
	}
	projects = append(projects, kinesisExportS3P)

	// Create Kinesis Data Firehose project for delivery streams (once per region)
	// This must be created outside the database loop to avoid duplicate resources
	kinesisFirehoseP, err := kinesisFirehoseProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "kinesis firehose project")
	}
	projects = append(projects, kinesisFirehoseP)

	for _, db := range rdsdeltalake.AllDatabases() {

		if !db.IsInRegion(config.Region) {
			continue
		}
		replicationP, err := replicationProjects(config, db)
		if err != nil {
			return nil, oops.Wrapf(err, "replication project")
		}
		projects = append(projects, replicationP...)

		parquetMergeP, err := parquetMergeProjects(config, sharedRemotes, db)
		if err != nil {
			return nil, oops.Wrapf(err, "merge project")
		}
		projects = append(projects, parquetMergeP...)

		vacuumP, err := vacuumProject(config, sharedRemotes, db)
		if err != nil {
			return nil, oops.Wrapf(err, "vacuum project")
		}
		projects = append(projects, vacuumP)

		// Create Kinesis stream and DMS replication projects (if enabled)
		// These are now combined into a single project for simpler deployment
		if db.InternalOverrides.EnableKinesisStreamDestination {
			kinesisStreamDestinationP, err := kinesisStreamDestinationProjects(config, db)
			if err != nil {
				return nil, oops.Wrapf(err, "kinesis stream destination project")
			}
			projects = append(projects, kinesisStreamDestinationP...)
		}
	}

	combineAllShards, err := combineShardsProject(config, sharedRemotes)
	if err != nil {
		return nil, oops.Wrapf(err, "combine all shards project")
	}
	projects = append(projects, combineAllShards)

	orgShardsV2TableSparkJobProject, err := orgShardsV2TableSparkJobProject(config, sharedRemotes)
	if err != nil {
		return nil, oops.Wrapf(err, "org shards v2 table spark job project")
	}
	projects = append(projects, orgShardsV2TableSparkJobProject)

	for _, p := range projects {
		// Check if either aws_provider or tf_backend already defined
		_, hasAwsProvider := p.ResourceGroups["aws_provider"]
		_, hasTfBackend := p.ResourceGroups["tf_backend"]

		// Skip the merge if both resources are already present - needed for RDS AWS provider override.
		if hasAwsProvider && hasTfBackend {
			continue
		}

		// Otherwise perform the merge
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}

	sharedScripts, err := sharedScriptsProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "shared scripts project")
	}

	projects = append(projects, sharedScripts)

	// Creates a lambda to set the password for DMS tasks in the Canada region. We
	// plan to enable this for all regions later.
	if config.Region == infraconsts.SamsaraAWSCARegion {
		lambdaProject, err := dmsLambdaProject(databricksProviderGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "dms lambda project")
		}
		projects = append(projects, lambdaProject)
	}

	return projects, nil
}

func sharedScriptsProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	// Add rds diffing script
	rdsdiffscript, err := dataplatformresource.DeployedArtifactObjectNoHash(config.Region, "python3/samsaradev/infra/dataplatform/tools/rdsdifftool.py")
	if err != nil {
		return nil, oops.Wrapf(err, "couldn't create rdsdifftool artifact")
	}

	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "shared-scripts",
		ResourceGroups: map[string][]tf.Resource{
			"rdsdifftool":         {rdsdiffscript},
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
		GenerateOutputs: true,
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider": resource.ProjectAWSProvider(p),
		"tf_backend":   resource.ProjectTerraformBackend(p),
	})

	return p, nil
}
