package emrreplicationproject

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
	prefix := "emr-nonproduction-"
	if production {
		prefix = "emr-production-"
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
	infraconsts.SamsaraAWSDefaultRegion: "xlarge",
	infraconsts.SamsaraAWSEURegion:      "xlarge",
	infraconsts.SamsaraAWSCARegion:      "xlarge",
}

type poolProjectDefinition struct {
	project         *project.Project
	remoteResources []tf.Resource
}

func poolProject(config dataplatformconfig.DatabricksConfig) (poolProjectDefinition, error) {
	poolProject := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformEmrReplicationProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emr_replication",
		Name:     "emr_replication_pool",
		ResourceGroups: map[string][]tf.Resource{
			"pool":                {},
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
		GenerateOutputs: true,
	}
	var poolRemoteResources []tf.Resource

	remoteState, err := poolProject.RemoteStateResource(project.RemoteStateResourceOptionalWithDefaults(map[string]string{}))
	if err != nil {
		return poolProjectDefinition{}, oops.Wrapf(err, "failed to create remote state")
	}
	poolRemoteResources = append(poolRemoteResources, remoteState)

	workerNodeSize := regionWorkerPoolNodeSize[config.Region]
	driverNodeSize := regionDriverPoolNodeSize[config.Region]

	// Set up driver and worker fleet pools.
	for _, production := range []bool{true, false} {
		rndCostAllocation := float64(1)
		if production {
			rndCostAllocation = 0
		}
		for az, identifiers := range getPoolIdentifiers(config.Region, production) {
			// Set up pool resources.
			driverConfig := dataplatformresource.InstancePoolConfig{
				Name:                               identifiers.DriverPoolIdentifier,
				MinIdleInstances:                   0,
				IdleInstanceAutoterminationMinutes: 5,
				NodeTypeId:                         "rd-fleet." + driverNodeSize,
				PreloadedSparkVersion:              sparkversion.EmrReplicationInstancePoolDbrVersion,
				Owner:                              team.DataPlatform,
				OnDemand:                           true,
				RnDCostAllocation:                  rndCostAllocation,
				ZoneId:                             az,
			}

			driverPool, err := dataplatformresource.InstancePool(driverConfig, config.Region)
			if err != nil {
				return poolProjectDefinition{}, oops.Wrapf(err, "%s", "failed to create pool "+identifiers.DriverPoolIdentifier)
			}

			workerConfig := dataplatformresource.InstancePoolConfig{
				Name:                               identifiers.WorkerPoolIdentifier,
				MinIdleInstances:                   0,
				IdleInstanceAutoterminationMinutes: 5,
				NodeTypeId:                         "rd-fleet." + workerNodeSize,
				PreloadedSparkVersion:              sparkversion.EmrReplicationInstancePoolDbrVersion,
				Owner:                              team.DataPlatform,
				OnDemand:                           false,
				RnDCostAllocation:                  rndCostAllocation,
				ZoneId:                             az,
			}

			workerPool, err := dataplatformresource.InstancePool(workerConfig, config.Region)
			if err != nil {
				return poolProjectDefinition{}, oops.Wrapf(err, "%s", "failed to create pool "+identifiers.WorkerPoolIdentifier)
			}

			poolProject.ResourceGroups["pool"] = append(poolProject.ResourceGroups["pool"], driverPool...)
			poolProject.ResourceGroups["pool"] = append(poolProject.ResourceGroups["pool"], workerPool...)

			// Create remote state identifiers
			driverResourceId := databricksresource.InstancePoolResourceId(driverConfig.Name)
			driverIdOutput := genericresource.GetResourceOutputName(driverResourceId, tf.IDAttribute)
			driverNameOutput := genericresource.GetResourceOutputName(driverResourceId, databricksresource.PoolNameAttribute)

			workerResourceId := databricksresource.InstancePoolResourceId(workerConfig.Name)
			workerIdOutput := genericresource.GetResourceOutputName(workerResourceId, tf.IDAttribute)
			workerNameOutput := genericresource.GetResourceOutputName(workerResourceId, databricksresource.PoolNameAttribute)

			// Create local variables to reference these remotes
			poolRemoteResources = append(
				poolRemoteResources,
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
		project:         poolProject,
		remoteResources: poolRemoteResources,
	}, nil
}
