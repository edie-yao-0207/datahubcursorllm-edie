package unitycatalog

import (
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// Each SingleUserCluster entry represents a custom cluster configuration
// with a specific set of members and team information.
// This will generate a UC-enabled single-user interactive clusters for
// each member of the team with the name <teamname>-<clustername>-<membername>
type SingleUserCluster struct {
	ClusterConfigurations components.DatabricksClusterOptions
	Team                  components.TeamInfo
	Members               []components.MemberInfo
}

var SingleUserClusterRegistry = []SingleUserCluster{
	{
		Team: team.DataAnalytics,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "r-language",
			DriverNodeType: "rd-fleet.4xlarge",
			WorkerNodeType: "rd-fleet.4xlarge",
			MaxWorkers:     15,
			RuntimeVersion: sparkversion.SparkVersion133xCpuMlScala212,
			CustomSparkConfigurations: map[string]string{
				"spark.driver.maxResultSize":             "0",
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			InitScripts: []string{
				databricksinstaller.RlibInstaller,
			},
		},
		Members: []components.MemberInfo{
			team.ZackBarnettHowell,
			team.ErinPost,
			team.MichaelHoward,
		},
	},
	{
		Team: team.DecisionScience,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "rdd-use",
			WorkerNodeType: "rd-fleet.4xlarge",
			MaxWorkers:     16,
			RuntimeVersion: sparkversion.SparkVersion133xScala212,
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
		},
		Members: []components.MemberInfo{
			team.JesseRussell,
			team.MaimeGuan,
			team.JuliaLin,
		},
	},
	{
		Team: team.Compliance,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "rdd-use",
			DriverNodeType: "rd-fleet.4xlarge",
			WorkerNodeType: "rd-fleet.4xlarge",
			RuntimeVersion: sparkversion.SparkVersion133xScala212,
		},
		Members: []components.MemberInfo{
			team.PaoloCaminiti,
		},
	},
	{
		Team: team.Compliance,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "rdd-use",
			DriverNodeType: "rd-fleet.4xlarge",
			WorkerNodeType: "rd-fleet.4xlarge",
			RuntimeVersion: sparkversion.SparkVersion133xScala212,
		},
		Members: []components.MemberInfo{
			team.KennyMillington,
		},
	},
	{
		Team: team.Firmware,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "ml-r-language",
			DriverNodeType: "g5.4xlarge", // 64 GB RAM, 1 A10G GPU
			WorkerNodeType: "g5.2xlarge", // 32 GB RAM, 1 A10G GPU
			MaxWorkers:     12,
			RuntimeVersion: sparkversion.NextGpuMlSparkVersion,
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			PythonLibraries: []string{
				"torch",
				"torchvision",
				"torch-geometric",
				"pyg-lib",
				"dgl",
				"osmnx",
				"networkx",
				"scikit-learn",
				"matplotlib",
				"seaborn",
			},
			InitScripts: []string{
				databricksinstaller.RlibInstaller,
			},
		},
		Members: []components.MemberInfo{
			team.MeenuRajapandian,
			team.EliasDykaar,
		},
	},
	{
		Team: team.SmartMaps,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "map-tile-generation",
			DriverNodeType: "rd-fleet.4xlarge",
			WorkerNodeType: "rd-fleet.4xlarge",
			MaxWorkers:     8,
			RuntimeVersion: sparkversion.SparkVersion154xScala212,
			InitScripts: []string{
				databricksinstaller.TippecanoeInstaller,
				databricksinstaller.GdalInstaller,
			},
			PythonLibraries: []string{"h3", "geojson", "gdal"},
		},
		Members: []components.MemberInfo{
			team.SalilGupta,
			team.JoeDownard,
			team.ShubhamShaw,
		},
	},
	{
		Team: team.ConnectedEquipment,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "mosaic",
			DriverNodeType: "rd-fleet.xlarge",
			WorkerNodeType: "rd-fleet.xlarge",
			MaxWorkers:     8,
			RuntimeVersion: sparkversion.SparkVersion133xCpuMlScala212, // Use ML runtime for Mosaic.
			InitScripts: []string{
				databricksinstaller.MosaicInstaller,
			},
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.labs.mosaic.index.system": "H3",
				"spark.databricks.labs.mosaic.geometry.api": "JTS",
				"spark.sql.extensions":                      "com.databricks.labs.mosaic.sql.extensions.MosaicSQL",
			},
		},
		Members: []components.MemberInfo{
			team.MattGeddie,
		},
	},
	{
		Team: team.DataAnalytics,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "dataanalytics-graph-embedding",
			DriverNodeType: "g5.4xlarge",                       // 64 GB Memory, 1 A10G GPU, 3.52 DBU
			WorkerNodeType: "g5.2xlarge",                       // 32 GB Memory, 1 A10G GPU, 1.76 DBU
			RuntimeVersion: sparkversion.NextGpuMlSparkVersion, // 15.4.x-gpu-ml-scala2.12
			MinWorkers:     1,
			MaxWorkers:     8,
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			PythonLibraries: []string{
				"torch",
				"torchvision",
				"torch-geometric",
				"pyg-lib",
				"dgl",
				"osmnx",
				"networkx",
				"scikit-learn",
				"matplotlib",
				"seaborn",
			},
		},
		Members: []components.MemberInfo{
			team.JamesBerglund,
		},
	},
	{
		Team: team.DataPlatform,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "r-language",
			DriverNodeType: "rd-fleet.xlarge",
			WorkerNodeType: "rd-fleet.xlarge",
			MaxWorkers:     2,
			RuntimeVersion: sparkversion.SparkVersion133xScala212,
			CustomSparkConfigurations: map[string]string{
				"spark.driver.maxResultSize":             "0",
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			InitScripts: []string{
				databricksinstaller.RlibInstaller,
			},
		},
		Members: []components.MemberInfo{
			team.KarthikBorkar,
		},
	},
	{
		Team: team.Prototyping,
		Members: []components.MemberInfo{
			team.MeelapShah,
			team.JohnBicket,
		},
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "prototyping",
			RuntimeVersion: sparkversion.NextGpuMlSparkVersion,
			// TODO(meelap):
			// Smaller p-class instances would be ideal, but since we are constrained
			// to nitro instances (see
			// https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5452277/Databricks+ESC+Enablement),
			// only larger instances are available which we don't want to pay for.
			// So we'll use g-class instances. g6 instances ought to work as they are
			// nitro, but the databricks docs only list g5 (perhaps they just haven't
			// updated it yet).
			// https://docs.databricks.com/aws/en/security/privacy/security-profile#requirements
			DriverNodeType:                      "g5.4xlarge",
			WorkerNodeType:                      "rd-fleet.xlarge",
			MaxWorkers:                          4,
			AutoTerminationAfterMinutesOverride: 240, // 4 hours
		},
	},
	{
		Team: team.Maps,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:            "maps-cuprjakm",
			DriverNodeType:  "rd-fleet.xlarge",
			WorkerNodeType:  "rd-fleet.4xlarge",
			MaxWorkers:      8,
			RuntimeVersion:  sparkversion.SparkVersion154xScala212,
			PythonLibraries: []string{"h3", "pyproj", "shapely", "morecantile", "fudgeo", "osmium"},
		},
		Members: []components.MemberInfo{
			team.MarcinCuprjak,
		},
	},
	{
		Team: team.Maps,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:            "valhalla-traffic",
			DriverNodeType:  "rd-fleet.xlarge",
			WorkerNodeType:  "rd-fleet.4xlarge",
			MaxWorkers:      8,
			RuntimeVersion:  sparkversion.SparkVersion154xScala212,
			PythonLibraries: []string{"h3", "pyproj", "shapely", "morecantile", "fudgeo", "osmium"},
		},
		Members: []components.MemberInfo{
			team.AleksandraLaska,
		},
	},
	{
		Team: team.TelematicsData,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "telematics-graph-embedding",
			DriverNodeType: "g5.4xlarge",                       // 64 GB Memory, 1 A10G GPU, 3.52 DBU
			WorkerNodeType: "g5.2xlarge",                       // 32 GB Memory, 1 A10G GPU, 1.76 DBU
			RuntimeVersion: sparkversion.NextGpuMlSparkVersion, // 15.4.x-gpu-ml-scala2.12
			MinWorkers:     1,
			MaxWorkers:     4,
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			PythonLibraries: []string{
				"torch",
				"torchvision",
				"torch-geometric",
				"pyg-lib",
				"dgl",
				"osmnx",
				"networkx",
				"scikit-learn",
				"matplotlib",
				"seaborn",
			},
		},
		Members: []components.MemberInfo{
			team.TomMeyer,
			team.JamesBerglund,
		},
	},
	{
		Team: team.STCEUnpowered,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "graph-embedding-eaglelo",
			DriverNodeType: "g5.4xlarge", // 64 GB RAM, 1 A10G GPU
			WorkerNodeType: "g5.2xlarge", // 32 GB RAM, 1 A10G GPU
			RuntimeVersion: sparkversion.NextGpuMlSparkVersion,
			MinWorkers:     1,
			MaxWorkers:     8,
			CustomSparkConfigurations: map[string]string{
				"spark.databricks.repl.allowedLanguages": "sql,python,r,scala",
			},
			PythonLibraries: []string{
				"torch",
				"torchvision",
				"torch-geometric",
				"pyg-lib",
				"dgl",
				"osmnx",
				"networkx",
				"scikit-learn",
				"matplotlib",
				"seaborn",
			},
		},
		Members: []components.MemberInfo{
			team.PierreGavaret,
		},
	},
	{
		Team: team.SafetyEventIngestionAndFiltering,
		ClusterConfigurations: components.DatabricksClusterOptions{
			Name:           "safety-event-ingestion-and-filtering-rdd-use",
			DriverNodeType: "rd-fleet.4xlarge",
			WorkerNodeType: "rd-fleet.4xlarge",
			MaxWorkers:     32,
			RuntimeVersion: sparkversion.SparkVersion154xScala212,
		},
		Members: []components.MemberInfo{
			team.ShubhamShaw,
		},
	},
}
