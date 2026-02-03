package dagsterprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

var Ec2DatabricksClusters = []dataplatformresource.ClusterConfig{
	{
		Name:                fmt.Sprintf("%s-%s", DagsterResourceBaseName, "dev-unity"),
		Owner:               team.DataPlatform,
		BillingTeamOverride: &team.DataEngineering,
		Admins: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.UserPrincipal{Email: "michael.howard@samsara.com"},
			dataplatformresource.UserPrincipal{Email: "maime.guan@samsara.com"},
			dataplatformresource.UserPrincipal{Email: "jesse.russell@samsara.com"},
			dataplatformresource.UserPrincipal{Email: "tongan.cai@samsara.com"},
			dataplatformresource.TeamPrincipal{Team: team.DataEngineering},
			dataplatformresource.TeamPrincipal{Team: team.DataTools},
			dataplatformresource.TeamPrincipal{Team: team.FirmwareTelematicsApps},
			dataplatformresource.TeamPrincipal{Team: team.FirmwareVdp},
			dataplatformresource.TeamPrincipal{Team: team.Sustainability},
		},
		CapacityOverrides: dataplatformresource.ClusterCapacityOverrides{
			DriverNodeTypeId: "md-fleet.2xlarge",
			WorkerNodeTypeId: "rd-fleet.2xlarge",
			MinWorkers:       1,
			MaxWorkers:       16,
		},
		SparkVersion: sparkversion.DagsterDevUnityDbrVersion,
		PythonLibraries: []string{
			"datadog==0.42.0",
			"slackclient==2.8.0",
			"databricks-sql-connector==2.9.3",
			"requests==2.29.0",
			"sql-formatter==0.6.2",
			"pdpyras==5.2.0",
			"optuna==4.6.0",
			// We install dagster libraries last to ensure pip resolves dependencies for them correctly
			"dagster==1.9.1",
			"dagster-aws==0.25.1",
			"dagster-databricks==0.25.1",
			"dagster-pyspark==0.25.1",
			"dagster-slack==0.25.1",
			"databricks-sdk==0.29.0",
			"sqllineage==1.3.8",
		},
		CustomSparkConfigurations: map[string]string{
			"spark.databricks.aggressiveWindowDownS":              "600",
			"spark.databricks.sql.initial.catalog.name":           "default",
			"spark.driver.maxResultSize":                          "16g",
			"spark.databricks.hive.metastore.client.pool.size":    "3",
			"spark.databricks.hive.metastore.glueCatalog.enabled": "true",
			"spark.hadoop.aws.glue.max-error-retries":             "10",
			"spark.hadoop.aws.glue.partition.num.segments":        "3",
		},
		RnDCostAllocation:  1,
		CanRunJobs:         true,
		EnableUnityCatalog: true,
		EnableGlueCatalog:  true,
	},
}

type poolIdentifier struct {
	DriverPoolIdentifier string
	WorkerPoolIdentifier string
}

// We need stable identifiers that we can use to reference the pools via
// terraform variables, so this will construct those for production or
// nonproduction jobs.
func getPoolIdentifiers(region string, production bool, name string, instanceType string, sparkVersion sparkversion.SparkVersion) map[string]poolIdentifier {
	identMap := make(map[string]poolIdentifier)
	prefix := fmt.Sprintf("%s-nonproduction-", name)
	if production {
		prefix = fmt.Sprintf("%s-production-", name)
	}
	class := strings.Split(instanceType, "-")[0]
	version := string(sparkVersion)[0:4]
	for _, az := range infraconsts.GetDatabricksAvailabilityZones(region) {
		identMap[az] = poolIdentifier{
			DriverPoolIdentifier: fmt.Sprintf("%sdriver-%s-fleet-%s-%s", prefix, class, version, az),
			WorkerPoolIdentifier: fmt.Sprintf("%sworker-%s-fleet-%s-%s", prefix, class, version, az),
		}

	}

	return identMap
}

func clusterResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	resources := []tf.Resource{}
	accountId := awsregionconsts.RegionDatabricksAccountID[config.Region]

	for _, c := range Ec2DatabricksClusters {

		c.Region = config.Region
		c.InstanceProfile = fmt.Sprintf("arn:aws:iam::%d:instance-profile/dataplatform-dagster-dev-cluster", accountId)
		c.CustomSparkEnvVars = map[string]string{
			"ENV":                "dev", // for local Dagster run context derivation
			"AWS_DEFAULT_REGION": config.Region,
			"AWS_REGION":         config.Region,
		}

		cluster, err := dataplatformresource.InteractiveCluster(c)
		if err != nil {
			return nil, oops.Wrapf(err, "failed generating dagster databricks clusters")
		}
		resources = append(resources, cluster...)
	}

	return map[string][]tf.Resource{
		"databricks_cluster": resources,
	}, nil
}

func clusterPoolResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	resources := []tf.Resource{}
	driverNodeSize := "xlarge"
	workerNodeSize := "2xlarge"

	switch config.Region {
	case infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
		workerNodeSize = "xlarge"
	}

	// Use this loop for spark version migrations:
	// 1. Merge PR w/ new spark version in list below.
	// 2. After terraform applies, merge a separate PR updating databricks step launchers w/ new pool ids.
	// 3. Merge a separate PR removing old spark version from list below.
	for _, sparkVersion := range []sparkversion.SparkVersion{sparkversion.DagsterClusterPoolDbrVersion} {

		for _, instanceClass := range []string{"md-fleet", "rd-fleet"} {
			poolIdentifiers := getPoolIdentifiers(config.Region, false, "dagster", instanceClass, sparkVersion)

			for az, identifiers := range poolIdentifiers {

				// Set up pool resources.
				driverConfig := dataplatformresource.InstancePoolConfig{
					Name:                               identifiers.DriverPoolIdentifier,
					MinIdleInstances:                   0,
					IdleInstanceAutoterminationMinutes: 5,
					NodeTypeId:                         fmt.Sprintf("%s.%s", instanceClass, driverNodeSize), // use the fleet instance types for driver pools
					PreloadedSparkVersion:              sparkVersion,
					Owner:                              team.DataEngineering,
					OnDemand:                           true,
					RnDCostAllocation:                  1.0,
					ZoneId:                             az,
				}

				driverPoolResources, err := dataplatformresource.InstancePool(driverConfig, config.Region)
				if err != nil {
					return nil, oops.Wrapf(err, "%s", "failed to create pool "+identifiers.DriverPoolIdentifier)
				}
				resources = append(resources, driverPoolResources...)

				workerConfig := dataplatformresource.InstancePoolConfig{
					Name:                               identifiers.WorkerPoolIdentifier,
					MinIdleInstances:                   0,
					IdleInstanceAutoterminationMinutes: 5,
					NodeTypeId:                         fmt.Sprintf("%s.%s", instanceClass, workerNodeSize), // use the fleet instance types for worker pools
					PreloadedSparkVersion:              sparkVersion,
					Owner:                              team.DataEngineering,
					OnDemand:                           false,
					RnDCostAllocation:                  1.0,
					ZoneId:                             az,
				}

				workerPoolResources, err := dataplatformresource.InstancePool(workerConfig, config.Region)
				if err != nil {
					return nil, oops.Wrapf(err, "%s", "failed to create pool "+identifiers.WorkerPoolIdentifier)
				}
				resources = append(resources, workerPoolResources...)
			}
		}
	}
	return map[string][]tf.Resource{
		"databricks_cluster_pools": resources,
	}, nil
}
