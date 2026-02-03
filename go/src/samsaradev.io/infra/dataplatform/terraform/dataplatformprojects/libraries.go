package dataplatformprojects

import (
	"fmt"
	"path/filepath"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func SparkLibraries(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	var jars []dataplatformresource.JarName
	jars = append(jars, dataplatformresource.AllSpark3Jars()...)

	var resources []tf.Resource
	for _, jar := range jars {
		resources = append(resources, &awsresource.S3BucketObject{
			Bucket:       awsregionconsts.RegionPrefix[c.Region] + "dataplatform-deployed-artifacts",
			ResourceName: jar.S3Key(),
			Key:          jar.S3Key(),
			Source:       jar.FilePath(),
		})
	}
	return map[string][]tf.Resource{
		"jars": resources,
	}
}

func StaticArtifacts(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	jars := dataplatformresource.AllExternalJars()
	wheels := dataplatformresource.AllExternalPythonWheels()

	var jarResources, wheelResources []tf.Resource
	for _, jar := range jars {
		jarResources = append(jarResources, &awsresource.S3BucketObject{
			Bucket:       awsregionconsts.RegionPrefix[c.Region] + "dataplatform-deployed-artifacts",
			ResourceName: jar.S3Key(),
			Key:          jar.S3Key(),
			Source:       jar.FilePath(),
		})
	}

	for _, wheel := range wheels {
		wheelResources = append(wheelResources, &awsresource.S3BucketObject{
			Bucket:       awsregionconsts.RegionPrefix[c.Region] + "dataplatform-deployed-artifacts",
			ResourceName: wheel.S3Key(),
			Key:          wheel.S3Key(),
			Source:       wheel.FilePath(),
		})
	}

	return map[string][]tf.Resource{
		"jars":   jarResources,
		"wheels": wheelResources,
	}
}

func InitScripts(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	return map[string][]tf.Resource{
		"init_scripts": {
			&awsresource.S3BucketObject{
				Bucket:       awsregionconsts.RegionPrefix[c.Region] + "dataplatform-deployed-artifacts",
				ResourceName: fmt.Sprintf("init_scripts/%s", databricksinstaller.Spark3RulesInstaller),
				Key:          fmt.Sprintf("init_scripts/%s", databricksinstaller.Spark3RulesInstaller),
				Source: filepath.Join(
					tf.LocalId("backend_root").Reference(),
					"tools/jars",
					databricksinstaller.Spark3RulesInstaller,
				),
			},
		},
	}
}
