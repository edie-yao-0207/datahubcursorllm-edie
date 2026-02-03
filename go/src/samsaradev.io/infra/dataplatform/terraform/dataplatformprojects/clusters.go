package dataplatformprojects

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

func addDatabricksResources(providerGroup string) (map[string][]tf.Resource, error) {
	c, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error getting databricks config")
	}

	var databricksResources []tf.Resource
	for _, config := range allRawClusterProfiles(c) {
		instanceProfile, err := createDatabricksInstanceProfile(providerGroup, config.clusterName, config.instanceProfileName, "")
		if err != nil {
			return nil, oops.Wrapf(err, "error creating instance profile")
		}
		databricksResources = append(databricksResources, instanceProfile)
	}

	if providerGroup == "databricks-dev-us" {
		databricksSupportInstanceProfile, err := createDatabricksInstanceProfile(providerGroup, "databricks-support", "databricks-support-cluster", "")
		if err != nil {
			return nil, oops.Wrapf(err, "error creating databricks support instance profile")
		}
		databricksResources = append(databricksResources, databricksSupportInstanceProfile)
	}

	return map[string][]tf.Resource{
		"databricks_resources": databricksResources,
	}, nil
}

func createDatabricksInstanceProfile(providerGroup string, clusterName string, instanceProfileName string, iamRoleArn string) (*databricksresource.InstanceProfile, error) {
	arn, err := dataplatformresource.InstanceProfileArn(providerGroup, instanceProfileName)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	return &databricksresource.InstanceProfile{
		ResourceName:       strings.Replace(clusterName, "-", "_", -1),
		InstanceProfileArn: arn,
		SkipValidation:     true, // We require custom tags that are not present in test instance creation
		IamRoleArn:         iamRoleArn,
	}, nil
}

// Not all Samsara Teams have a Terraform pipeline, we need to verify the team has a TF pipeline before sending this cluster
// resources through one
var teamsWithTFPipelines = team.AllTeamsWithTerraformPipelines

func InteractiveClustersByTeamProject(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	adminCluster, err := adminClusterResource(config, false)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Admin Cluster terraform resources")
	}

	adminUcCluster, err := adminClusterResource(config, true)
	if err != nil {
		return nil, oops.Wrapf(err, "Error creating Admin-UC Cluster terraform resources")
	}

	teamClusters := map[string][]tf.Resource{
		team.DataPlatform.TeamName: append(adminCluster, adminUcCluster...),
	}

	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		clusters, err := interactiveClusters(config, t)
		if err != nil {
			return nil, oops.Wrapf(err, "Error creating %s cluster project", t.TeamName)
		}
		rootTeamOwner := t.TeamName

		// Check if team has a manual Data Platform terraform override
		if override, ok := dataplatformterraformconsts.DataPlatformTerraformOverride[rootTeamOwner]; ok {
			rootTeamOwner = override.TeamName
		}

		// Teams without a Terraform pipeline will have their clusters go through the Data Platform Pipeline
		if _, ok := teamsWithTFPipelines[rootTeamOwner]; !ok {
			rootTeamOwner = team.DataPlatform.TeamName
		}

		if _, ok := teamClusters[rootTeamOwner]; !ok {
			teamClusters[rootTeamOwner] = []tf.Resource{}
		}

		teamClusters[rootTeamOwner] = append(teamClusters[rootTeamOwner], clusters...)
	}

	for _, cluster := range unitycatalog.SingleUserClusterRegistry {
		teamName := cluster.Team.TeamName

		// Check if team has a manual Data Platform terraform override
		if override, ok := dataplatformterraformconsts.DataPlatformTerraformOverride[teamName]; ok {
			teamName = override.TeamName
		}

		// Decision Science and Data Analytics teams have a longer idle timeout for their SingleUserClusters,
		// because when they use these clusters we want them to not lose time for cluster startup during middle of the day
		if (teamName == team.DecisionScience.TeamName || teamName == team.DataAnalytics.TeamName) && cluster.ClusterConfigurations.AutoTerminationAfterMinutesOverride == 0 {
			cluster.ClusterConfigurations.AutoTerminationAfterMinutesOverride = 60 * 8
		}

		singleUserClusterResources, err := singeUserInteractiveClusters(config, cluster)
		if err != nil {
			return nil, oops.Wrapf(err, "Error creating %s-%s single user cluster", teamName, cluster.ClusterConfigurations.Name)
		}

		teamClusters[teamName] = append(teamClusters[teamName], singleUserClusterResources...)
	}

	var clusterProjects []*project.Project
	dbxProvider := dataplatformresource.DatabricksOauthProvider(config.Hostname)
	for t, clusters := range teamClusters {
		clusterProj := &project.Project{
			RootTeam: t,
			Provider: providerGroup,
			Class:    "interactiveclusters",
			Name:     fmt.Sprintf("%s_clusters", strings.ToLower(t)),
			ResourceGroups: map[string][]tf.Resource{
				"databricks_provider": dbxProvider,
				"clusters":            clusters,
			},
		}

		clusterProj.ResourceGroups = project.MergeResourceGroups(clusterProj.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(clusterProj),
			"tf_backend":   resource.ProjectTerraformBackend(clusterProj),
		})

		if t == team.DataPlatform.TeamName && dataplatformresource.IsE2ProviderGroup(providerGroup) {
			dbxResources, err := addDatabricksResources(providerGroup)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			clusterProj.ResourceGroups = project.MergeResourceGroups(clusterProj.ResourceGroups, dbxResources)
		}

		clusterProjects = append(clusterProjects, clusterProj)
	}

	// Temporarily add the monolith interactive clusters project tfstate path to each project during migration
	monolithStatePath := getProjectMonolithStateFilePath(config.Region, providerGroup, "interactiveclusters")
	for _, p := range clusterProjects {
		p.MonolithStatePathOverride = monolithStatePath
	}

	return clusterProjects, nil
}

// Returns a list of tfResources relevant for a single user interactive cluster.
func singeUserInteractiveClusters(config dataplatformconfig.DatabricksConfig, cluster unitycatalog.SingleUserCluster) ([]tf.Resource, error) {
	var clusterResources []tf.Resource

	profile := "unity-catalog-cluster"
	instanceProfile, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, profile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	clusterBaseName := fmt.Sprintf("%s-%s", cluster.Team.TeamName, cluster.ClusterConfigurations.Name)

	for _, member := range cluster.Members {
		sanitizedName := SanitizeClusterName(member.Name)
		clusterName := strings.ToLower(fmt.Sprintf("%s-%s", clusterBaseName, sanitizedName))

		clusterConfig := dataplatformresource.ClusterConfig{
			Name:            clusterName,
			Owner:           cluster.Team,
			OwnerUser:       member,
			Region:          config.Region,
			InstanceProfile: instanceProfile,
			CapacityOverrides: dataplatformresource.ClusterCapacityOverrides{
				DriverNodeTypeId: cluster.ClusterConfigurations.DriverNodeType,
				WorkerNodeTypeId: cluster.ClusterConfigurations.WorkerNodeType,
				MinWorkers:       cluster.ClusterConfigurations.MinWorkers,
				MaxWorkers:       cluster.ClusterConfigurations.MaxWorkers,
			},
			PythonLibraries:                     cluster.ClusterConfigurations.PythonLibraries,
			MavenLibraries:                      cluster.ClusterConfigurations.MavenLibraries,
			SparkVersion:                        cluster.ClusterConfigurations.RuntimeVersion,
			DisableAutoTermination:              cluster.ClusterConfigurations.DisableAutoTermination,
			AutoTerminationAfterMinutesOverride: cluster.ClusterConfigurations.AutoTerminationAfterMinutesOverride,
			CustomSparkConfigurations:           cluster.ClusterConfigurations.CustomSparkConfigurations,
			DisableStockSparkConf:               cluster.ClusterConfigurations.DisableStockSparkConf,
			CustomSparkEnvVars:                  cluster.ClusterConfigurations.CustomSparkEnvVars,
			BillingTeamOverride:                 cluster.ClusterConfigurations.BillingTeamOverride,
			InitScripts:                         cluster.ClusterConfigurations.InitScripts,
			SingleNode:                          cluster.ClusterConfigurations.SingleNode,
			RnDCostAllocation:                   1,
			EnableLocalDiskEncryption:           true,
			Pin:                                 true,
			DisableBigQuery:                     cluster.ClusterConfigurations.DisableBigQuery,
			CanRunJobs:                          false,
			IncludeDefaultLibraries:             true,
			EnableUnityCatalog:                  true,
		}

		clusterRes, err := dataplatformresource.InteractiveCluster(clusterConfig)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		clusterResources = append(clusterResources, clusterRes...)
	}

	return clusterResources, nil
}

func teamNameClusterFormatted(t components.TeamInfo) string {
	return strings.ToLower(t.Name())
}

func interactiveClusterInstanceProfileName(t components.TeamInfo, clusterProfileOverride string, c dataplatformconfig.DatabricksConfig) (string, error) {
	profileName := instanceProfileName(teamNameClusterFormatted(t))
	if clusterProfileOverride != "" {
		if !clusterProfileExists(clusterProfileOverride, c) {
			return "", oops.Errorf("Cluster profile %s does not exist, please use a defined cluster", clusterProfileOverride)
		}
		profileName = instanceProfileName(clusterProfileOverride)
	}
	return profileName, nil
}

// Returns a list of tfResources relevant for a team-shared interactive cluster.
func interactiveClusters(config dataplatformconfig.DatabricksConfig, t components.TeamInfo) ([]tf.Resource, error) {
	if t.DatabricksInteractiveClusterOverrides.Disable {
		return []tf.Resource{}, nil
	}

	var clusters []tf.Resource

	name := teamNameClusterFormatted(t)
	profile, err := interactiveClusterInstanceProfileName(t, t.DatabricksInteractiveClusterOverrides.ClusterProfileOverride, config)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	instanceProfile, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, profile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	canRunJobs := dataplatformresource.IsAllowAllPurposeJobs(t.Name())

	// Set the spark version to RuntimeVersion as defined in the team's overrides.
	sparkVersion := t.DatabricksInteractiveClusterOverrides.RuntimeVersion
	if _, ok := dataplatformresource.DONOTMIGRATE_Clusters[name]; !ok {
		// UC supports spark versions as long as they are not too old
		if sparkVersion < sparkversion.SparkVersion154xScala212 {
			sparkVersion = sparkversion.SparkVersion154xScala212
		}
		if t.DatabricksInteractiveClusterOverrides.CustomSparkEnvVars == nil {
			t.DatabricksInteractiveClusterOverrides.CustomSparkEnvVars = make(map[string]string)
		}
		t.DatabricksInteractiveClusterOverrides.CustomSparkEnvVars["DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME"] = fmt.Sprintf("team-%s", name)
	}

	clusterConfig := dataplatformresource.ClusterConfig{
		Name:            name,
		Owner:           t,
		Region:          config.Region,
		InstanceProfile: instanceProfile,
		CapacityOverrides: dataplatformresource.ClusterCapacityOverrides{
			DriverNodeTypeId: t.DatabricksInteractiveClusterOverrides.DriverNodeType,
			WorkerNodeTypeId: t.DatabricksInteractiveClusterOverrides.WorkerNodeType,
			MinWorkers:       t.DatabricksInteractiveClusterOverrides.MinWorkers,
			MaxWorkers:       t.DatabricksInteractiveClusterOverrides.MaxWorkers,
		},
		PythonLibraries:                     t.DatabricksInteractiveClusterOverrides.PythonLibraries,
		MavenLibraries:                      t.DatabricksInteractiveClusterOverrides.MavenLibraries,
		SparkVersion:                        sparkVersion,
		InitScripts:                         t.DatabricksInteractiveClusterOverrides.InitScripts,
		DisableAutoTermination:              t.DatabricksInteractiveClusterOverrides.DisableAutoTermination,
		AutoTerminationAfterMinutesOverride: t.DatabricksInteractiveClusterOverrides.AutoTerminationAfterMinutesOverride,
		CustomSparkConfigurations:           t.DatabricksInteractiveClusterOverrides.CustomSparkConfigurations,
		DisableStockSparkConf:               t.DatabricksInteractiveClusterOverrides.DisableStockSparkConf,
		CustomSparkEnvVars:                  t.DatabricksInteractiveClusterOverrides.CustomSparkEnvVars,
		BillingTeamOverride:                 t.DatabricksInteractiveClusterOverrides.BillingTeamOverride,
		RnDCostAllocation:                   1,
		EnableLocalDiskEncryption:           true,
		Pin:                                 true,
		DisableBigQuery:                     t.DatabricksInteractiveClusterOverrides.DisableBigQuery,
		CanRunJobs:                          canRunJobs,
		IncludeDefaultLibraries:             true,
		EnableUnityCatalog:                  t.DatabricksInteractiveClusterOverrides.EnableUnityCatalog,
		SingleNode:                          t.DatabricksInteractiveClusterOverrides.SingleNode,
	}
	if runtimeEngine, ok := t.DatabricksInteractiveClusterOverrides.RuntimeEngineByRegion[config.Region]; ok {
		clusterConfig.RuntimeEngine = runtimeEngine
	}

	// Set the instance profile to the stripped down shared UC instance profile, which has no S3 or glue permissions.
	ucProfile := "unity-catalog-cluster"
	ucInstanceProfile, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, ucProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	// Rollout Unity Catalog to select clusters.
	if dataplatformresource.ShouldMigrateCluster(clusterConfig) {
		clusterConfig.EnableUnityCatalog = true
		clusterConfig.InstanceProfile = ucInstanceProfile
	}

	// We generate team interactive cluster only if the cluster is not UC-enabled or the runtime is not ML.
	// For UC-enabled clusters with ML runtime, please define them in single_user_cluster_registry.go file.
	if !clusterConfig.EnableUnityCatalog || !clusterConfig.SparkVersion.IsMl() {
		cluster, err := dataplatformresource.InteractiveCluster(clusterConfig)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		clusters = append(clusters, cluster...)
	}

	for _, clusterConfig := range t.DatabricksInteractiveClusters {
		if clusterConfig.Name == "" {
			return nil, oops.Errorf("All databricks clusters must have a name. One of %s's clusters is missing a name field.", name)
		}

		// We currently don't need these clusters in the EU as Databricks doesn't need to debug anything in EU
		if config.Region != "us-west-2" && clusterConfig.Name == "databricks-support" {
			continue
		}

		profile, err := interactiveClusterInstanceProfileName(t, clusterConfig.ClusterProfileOverride, config)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		instanceProfile, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, profile)
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		clusterName := fmt.Sprintf("%s-%s", name, clusterConfig.Name)

		// If RndCostAllocation is not set for an interactive cluster, then assume it is R&D
		// (since most cases are R&D)
		rndCostAllocation := pointer.Float64ValOr(clusterConfig.RnDCostAllocation, 1)

		sparkVersion := clusterConfig.RuntimeVersion
		if _, ok := dataplatformresource.DONOTMIGRATE_Clusters[clusterName]; !ok {
			// UC supports spark versions as long as they are not too old
			if sparkVersion < sparkversion.SparkVersion154xScala212 {
				sparkVersion = sparkversion.SparkVersion154xScala212
			}
			if clusterConfig.CustomSparkEnvVars == nil {
				clusterConfig.CustomSparkEnvVars = make(map[string]string)
			}
			clusterConfig.CustomSparkEnvVars["DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME"] = fmt.Sprintf("team-%s", name)
		}

		customClusterConfig := dataplatformresource.ClusterConfig{
			Name:            clusterName,
			Owner:           t,
			Region:          config.Region,
			InstanceProfile: instanceProfile,
			CapacityOverrides: dataplatformresource.ClusterCapacityOverrides{
				DriverNodeTypeId: clusterConfig.DriverNodeType,
				WorkerNodeTypeId: clusterConfig.WorkerNodeType,
				MinWorkers:       clusterConfig.MinWorkers,
				MaxWorkers:       clusterConfig.MaxWorkers,
			},
			PythonLibraries:                     clusterConfig.PythonLibraries,
			InitScripts:                         clusterConfig.InitScripts,
			SparkVersion:                        sparkVersion,
			DisableAutoTermination:              clusterConfig.DisableAutoTermination,
			AutoTerminationAfterMinutesOverride: clusterConfig.AutoTerminationAfterMinutesOverride,
			CustomSparkConfigurations:           clusterConfig.CustomSparkConfigurations,
			RnDCostAllocation:                   rndCostAllocation,
			DisableStockSparkConf:               clusterConfig.DisableStockSparkConf,
			CustomSparkEnvVars:                  clusterConfig.CustomSparkEnvVars,
			BillingTeamOverride:                 clusterConfig.BillingTeamOverride,
			Pin:                                 true,
			DisableBigQuery:                     clusterConfig.DisableBigQuery,
			CanRunJobs:                          canRunJobs,
			IncludeDefaultLibraries:             true,
			EnableUnityCatalog:                  clusterConfig.EnableUnityCatalog,
			SingleNode:                          clusterConfig.SingleNode,
		}
		if runtimeEngine, ok := clusterConfig.RuntimeEngineByRegion[config.Region]; ok {
			customClusterConfig.RuntimeEngine = runtimeEngine
		}

		// Rollout Unity Catalog to select clusters.
		if dataplatformresource.ShouldMigrateCluster(customClusterConfig) {
			customClusterConfig.EnableUnityCatalog = true

			// Set the unity-catalog-cluster instance profile for all clusters that have UC enabled, EXCEPT for dataplatform-uc-testing-with-instance-profile.
			// This is a test cluster which we need to keep on a specific instance profile.
			if customClusterConfig.Name != "dataplatform-uc-testing-with-instance-profile" {
				customClusterConfig.InstanceProfile = ucInstanceProfile
			}
		}

		// We generate team interactive cluster only if the cluster is not UC-enabled or the runtime is not ML.
		// For UC-enabled clusters with ML runtime, please define them in single_user_cluster_registry.go file.
		if !customClusterConfig.EnableUnityCatalog || !customClusterConfig.SparkVersion.IsMl() {
			cluster, err := dataplatformresource.InteractiveCluster(customClusterConfig)
			if err != nil {
				return nil, oops.Wrapf(err, "")
			}
			clusters = append(clusters, cluster...)
		}
	}

	return clusters, nil
}

func adminClusterResource(config dataplatformconfig.DatabricksConfig, enableUnityCatalog bool) ([]tf.Resource, error) {
	adminArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, instanceProfileName("admin"))
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	clusterName := "admin"
	sparkConfs := map[string]string{
		"spark.sql.extensions":                                "",     // admin cluster should have full access control and not be limited by Spark Extensions
		"spark.databricks.hive.metastore.glueCatalog.enabled": "true", // Admin cluster needs hive metastore glue perms.
	}

	if enableUnityCatalog {
		clusterName = "admin-uc"
	} else {
		// spark.databricks.repl.allowedLanguages is not allowed when Unity Catalog is enabled
		sparkConfs["spark.databricks.repl.allowedLanguages"] = "sql,python,scala" // admin cluster needs scala for some databricks tools
	}

	adminCluster, err := dataplatformresource.InteractiveCluster(dataplatformresource.ClusterConfig{
		Name:                      clusterName,
		Owner:                     team.DataPlatform,
		Region:                    config.Region,
		RnDCostAllocation:         1,
		InstanceProfile:           adminArn,
		SparkVersion:              sparkversion.SparkVersion154xScala212,
		CustomSparkConfigurations: sparkConfs,
		Pin:                       true,
		IncludeDefaultLibraries:   true,
		EnableUnityCatalog:        enableUnityCatalog,
		EnableGlueCatalog:         true,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	return adminCluster, nil
}

// SanitizeClusterName sanitizes a cluster name by replacing spaces with hyphens
// and removing non-ASCII characters. This ensures that the cluster name is
// compatible with Databricks naming conventions.
func SanitizeClusterName(name string) string {
	name = strings.ReplaceAll(name, " ", "-")
	var result strings.Builder
	for _, r := range name {
		if r <= 127 {
			result.WriteRune(r)
		}
	}
	return result.String()
}
