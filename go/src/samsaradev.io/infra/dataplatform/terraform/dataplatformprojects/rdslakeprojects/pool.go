package rdslakeprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

type poolIdentifier struct {
	DriverPoolIdentifier string
	WorkerPoolIdentifier string
}

// We need stable identifiers that we can use to reference the pools via
// terraform variables, so this will construct those for production or
// nonproduction jobs.
func getPoolIdentifiers(region string, production bool) map[string]poolIdentifier {
	identMap := make(map[string]poolIdentifier)
	prefix := "rds-nonproduction-"
	if production {
		prefix = "rds-production-"
	}
	for _, az := range infraconsts.GetDatabricksAvailabilityZones(region) {
		identMap[az] = poolIdentifier{
			DriverPoolIdentifier: prefix + "driver-fleet-" + az,
			WorkerPoolIdentifier: prefix + "worker-fleet-" + az,
		}
	}

	return identMap
}

var regionDriverPoolNodeSize = map[string]string{
	infraconsts.SamsaraAWSDefaultRegion: "xlarge",
	infraconsts.SamsaraAWSEURegion:      "xlarge",
	infraconsts.SamsaraAWSCARegion:      "xlarge",
}

var regionWorkerPoolNodeSize = map[string]string{
	infraconsts.SamsaraAWSDefaultRegion: "2xlarge",
	infraconsts.SamsaraAWSEURegion:      "xlarge",
	infraconsts.SamsaraAWSCARegion:      "xlarge",
}

type poolProjectDefinition struct {
	project         *project.Project
	remoteResources []tf.Resource
}

func poolProject(config dataplatformconfig.DatabricksConfig) (poolProjectDefinition, error) {
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "rdsmerge",
		ResourceGroups: map[string][]tf.Resource{
			"pool":                {},
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
		GenerateOutputs: true,
	}
	var remoteResources []tf.Resource

	if config.LegacyWorkspace {
		return poolProjectDefinition{
			project:         p,
			remoteResources: remoteResources,
		}, nil
	}

	workerNodeSize := regionWorkerPoolNodeSize[config.Region]

	// HACK: create a remote state reference here to use later.
	// It doesn't have any of the defaults, but we don't need them, i think,
	// and all of our code before this didn't have defaults for everything.
	remoteState, err := p.RemoteStateResource(project.RemoteStateResourceOptionalWithDefaults(map[string]string{}))

	if err != nil {
		return poolProjectDefinition{}, oops.Wrapf(err, "failed to create remote state")
	}

	remoteResources = append(remoteResources, remoteState)

	// Set up driver and worker fleet pools
	for _, production := range []bool{true, false} {
		driverNodeSize := regionDriverPoolNodeSize[config.Region]
		rndCostAllocation := float64(1)
		if production {
			rndCostAllocation = 0
		}

		for az, identifiers := range getPoolIdentifiers(config.Region, production) {
			// Set up pool resources
			preloadedVersion := sparkversion.RdsInstancePoolDbrVersion
			driverConfig := dataplatformresource.InstancePoolConfig{
				Name:                               identifiers.DriverPoolIdentifier,
				MinIdleInstances:                   0,
				IdleInstanceAutoterminationMinutes: 5,
				NodeTypeId:                         "rd-fleet." + driverNodeSize,
				PreloadedSparkVersion:              preloadedVersion,
				Owner:                              team.DataPlatform,
				OnDemand:                           true,
				RnDCostAllocation:                  rndCostAllocation,
				ZoneId:                             az,
			}

			driverPool, err := dataplatformresource.InstancePool(driverConfig, config.Region)
			if err != nil {
				return poolProjectDefinition{}, oops.Wrapf(err, "instance pool")
			}

			workerConfig := dataplatformresource.InstancePoolConfig{
				Name:                               identifiers.WorkerPoolIdentifier,
				MinIdleInstances:                   0,
				IdleInstanceAutoterminationMinutes: 5,
				NodeTypeId:                         "rd-fleet." + workerNodeSize,
				PreloadedSparkVersion:              preloadedVersion,
				Owner:                              team.DataPlatform,
				OnDemand:                           false,
				RnDCostAllocation:                  rndCostAllocation,
				ZoneId:                             az,
			}

			workerPool, err := dataplatformresource.InstancePool(workerConfig, config.Region)
			if err != nil {
				return poolProjectDefinition{}, oops.Wrapf(err, "instance pool")
			}

			p.ResourceGroups["pool"] = append(p.ResourceGroups["pool"], driverPool...)
			p.ResourceGroups["pool"] = append(p.ResourceGroups["pool"], workerPool...)

			// Create remote state identifiers
			driverResourceId := databricksresource.InstancePoolResourceId(driverConfig.Name)
			driverIdOutput := genericresource.GetResourceOutputName(driverResourceId, tf.IDAttribute)
			driverNameOutput := genericresource.GetResourceOutputName(driverResourceId, databricksresource.PoolNameAttribute)

			workerResourceId := databricksresource.InstancePoolResourceId(workerConfig.Name)
			workerIdOutput := genericresource.GetResourceOutputName(workerResourceId, tf.IDAttribute)
			workerNameOutput := genericresource.GetResourceOutputName(workerResourceId, databricksresource.PoolNameAttribute)

			// Create local variables to reference these remotes
			remoteResources = append(
				remoteResources,
				&genericresource.MapLocal{
					Name: identifiers.DriverPoolIdentifier,
					Value: map[string]string{
						"id":   fmt.Sprintf(`"%s"`, remoteState.ResourceId().ReferenceOutput(driverIdOutput)),
						"name": fmt.Sprintf(`"%s"`, remoteState.ResourceId().ReferenceOutput(driverNameOutput)),
					},
				},
				&genericresource.MapLocal{
					Name: identifiers.WorkerPoolIdentifier,
					Value: map[string]string{
						"id":   fmt.Sprintf(`"%s"`, remoteState.ResourceId().ReferenceOutput(workerIdOutput)),
						"name": fmt.Sprintf(`"%s"`, remoteState.ResourceId().ReferenceOutput(workerNameOutput)),
					},
				},
			)
		}
	}

	return poolProjectDefinition{
		project:         p,
		remoteResources: remoteResources,
	}, nil
}
