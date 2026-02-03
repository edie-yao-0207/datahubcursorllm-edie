package dataplatformresource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/databricksinstaller"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
	"samsaradev.io/team/teamnames"
)

// Clusters that should not be migrated to UC.
var DONOTMIGRATE_Clusters = map[string]struct{}{
	// Do not enable UC on biztech clusters. Biztech will be moving their workloads off the dev Databricks workspace by end of FY25Q3,
	// so these clusters can be deleted soon (end of Q3).
	"accounting":                                       {},
	"accountspayable":                                  {},
	"biztechenterprisedata":                            {},
	"biztechenterprisedata-dbt-dev":                    {},
	"biztechenterprisedataapplication-dbt-ci":          {},
	"biztechenterprisedataapplication-dbt-prod":        {},
	"biztechenterprisedataapplication-fivetran-bronze": {},
	"biztechenterprisedatasensitive-biztechenterprisedata-dbt-dev-sensitive":  {},
	"biztechenterprisedatasensitive-biztechenterprisedata-dbt-prod-sensitive": {},
	"biztechenterprisedataadmin":                               {},
	"biztechenterprisedataapplication":                         {},
	"biztechenterprisedataapplication-dbt-prod-2":              {},
	"biztechenterprisedataapplication-dbt-uat":                 {},
	"biztechenterprisedataapplication-dbt-uat-2":               {},
	"biztechenterprisedatacontractors":                         {},
	"biztechenterprisedatasensitive":                           {},
	"businesssystems":                                          {},
	"businesssystems-fivetran-netsuite-finance":                {},
	"businesssystems-netsuite-finance":                         {},
	"businesssystemsrecruiting":                                {},
	"businesssystemsrecruiting-fivetran-greenhouse-recruiting": {},
	// Per request from Nick Jaton, migrate to UC.
	// "customersuccessoperations": {},
	"datascience-zendeskfivetran":          {},
	"epofinance":                           {},
	"financialoperations":                  {},
	"finssensitive":                        {},
	"globalcompliance":                     {},
	"growth":                               {},
	"gtms":                                 {},
	"marketingdataanalytics":               {},
	"marketingdataanalytics-prod":          {},
	"marketingdataanalytics-production":    {},
	"marketingdataanalytics-sandbox":       {},
	"marketingdataanalytics-stage":         {},
	"marketingdataanalyticscontractors":    {},
	"platformoperations-netsuite-fivetran": {},
	"salesdataanalytics":                   {},
	"salesengineering":                     {},
	"salesengineering-sis":                 {},
	"supplychain":                          {},
	"support":                              {},
}

func clusterMigratedToUC(clusterName string, config ClusterConfig) bool {
	// Never migrate clusters on the flagged list.
	if _, ok := DONOTMIGRATE_Clusters[clusterName]; ok {
		return false
	}

	return true
}

// Returns whether Unity Catalog should be enabled on the interactive cluster.
func ShouldMigrateCluster(config ClusterConfig) bool {
	return clusterMigratedToUC(config.Name, config)
}

func coalesce(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func max(values ...int) int {
	var found int
	for _, value := range values {
		if value > found {
			found = value
		}
	}
	return found
}

func IsAllowAllPurposeJobs(teamName string) bool {
	switch teamName {
	case
		teamnames.BusinessSystemsRecruiting:
		return true
	}
	return false
}

func IsAllowListedTeamWorkspace(teamName string) bool {
	switch teamName {
	case
		teamnames.DataEngineering,
		teamnames.DataScience,
		teamnames.DataAnalytics,
		teamnames.ConnectedWorker,
		teamnames.Safety,
		teamnames.SafetyADAS:
		return true
	}
	return false
}

// SymLinkTeamWorkspaceInitScript returns the S3 URL to an init script that
// creates a symbolic link to a team's mounted databricks workspace in directory
// that is included in a databricks cluster's PYTHONPATH.
func SymLinkTeamWorkspaceInitScript(region string) string {
	prefix := fmt.Sprintf("databricks-%s", strings.ToLower(region[0:2]))
	return fmt.Sprintf("s3://%s/%s/%s%s", ArtifactBucket(region), prefix, "infrastructure/tools/jars/symlink-team-workspaces.sh/", databricksinstaller.SymlinkTeamWorkspacesInstaller)
}

type ClusterCapacityOverrides struct {
	DriverNodeTypeId string
	WorkerNodeTypeId string
	MinWorkers       int
	MaxWorkers       int
}

type ClusterConfig struct {
	Name                                string
	Owner                               components.TeamInfo
	OwnerUser                           components.MemberInfo
	Admins                              []DatabricksPrincipal
	Users                               []DatabricksPrincipal
	ReadOnlyUsers                       []DatabricksPrincipal
	Region                              string
	InstanceProfile                     string
	CapacityOverrides                   ClusterCapacityOverrides
	PythonLibraries                     []string
	MavenLibraries                      []string
	SparkVersion                        sparkversion.SparkVersion
	InitScripts                         []string
	DisableAutoTermination              bool
	AutoTerminationAfterMinutesOverride int
	CustomSparkConfigurations           map[string]string
	FirstOnDemandNodesOverride          int
	DisableStockSparkConf               bool
	CustomSparkEnvVars                  map[string]string
	BillingTeamOverride                 *components.TeamInfo
	RnDCostAllocation                   float64
	EnableLocalDiskEncryption           bool
	Pin                                 bool
	DisableBigQuery                     bool
	CanRunJobs                          bool
	IncludeDefaultLibraries             bool
	EnableUnityCatalog                  bool
	EnableGlueCatalog                   bool
	SingleNode                          bool
	RuntimeEngine                       components.DatabricksRuntimeEngine
}

func InteractiveCluster(c ClusterConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	sparkVersion := c.SparkVersion
	if sparkVersion == "" {
		return nil, oops.Errorf("missing SparkVersion for cluster %s", c.Name)
	}

	if c.RnDCostAllocation < 0 || c.RnDCostAllocation > 1 {
		return nil, oops.Errorf("invalid RnDCostAllocation: %f", c.RnDCostAllocation)
	}

	dataSecurityMode := databricksresource.DataSecurityModeNone
	sparkConfOverrides := c.CustomSparkConfigurations
	if c.EnableUnityCatalog {
		dataSecurityMode = databricksresource.DataSecurityModeUserIsolation

		// For UC clusters, remove disallowed spark confs from the overrides that teams set.
		// Note: We make a new map of overrides because deleting in place would affect other references using the overrides map (specifically the legacy clusters).
		ucSparkOverrides := make(map[string]string)
		for k, v := range sparkConfOverrides {
			// This spark conf is not allowed for UC clusters.
			if k == "spark.databricks.repl.allowedLanguages" {
				continue
			}

			if (strings.Contains(k, "hadoop.aws.glue") || strings.Contains(k, "databricks.hive.metastore")) && !c.EnableGlueCatalog {
				continue
			}

			ucSparkOverrides[k] = v
		}
		sparkConfOverrides = ucSparkOverrides
	}

	for _, initScript := range c.InitScripts {
		// If the Apache Sedona library is being added, also add the required spark configs.
		if initScript == databricksinstaller.ApacheSedonaInstaller {
			if sparkConfOverrides == nil {
				sparkConfOverrides = make(map[string]string)
			}
			sparkConfOverrides["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
			sparkConfOverrides["spark.kryo.registrator"] = "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
			sparkConfOverrides["spark.sql.extensions"] = "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
		}
	}

	conf := SparkConf{
		Region:                               c.Region,
		EnableCaching:                        true,
		EnablePlaygroundWarehouse:            true,
		EnableExtension:                      true,
		EnableBigQuery:                       !c.DisableBigQuery,
		EnableHighConcurrency:                true,
		EnableMultipleResults:                true,
		Overrides:                            sparkConfOverrides,
		DisableStockSparkConf:                c.DisableStockSparkConf,
		DataSecurityMode:                     dataSecurityMode,
		EnableAggressiveSkewJoinOptimization: true,
		DisableFileModificationCheck:         true,
	}

	autoTerminationMinutes := 60
	if c.DisableAutoTermination {
		autoTerminationMinutes = 0
	} else if c.AutoTerminationAfterMinutesOverride != 0 {
		autoTerminationMinutes = c.AutoTerminationAfterMinutesOverride
	}

	firstOnDemand := 1
	if c.FirstOnDemandNodesOverride != 0 {
		firstOnDemand = c.FirstOnDemandNodesOverride
	}

	envVars := map[string]string{
		"AWS_DEFAULT_REGION":   c.Region,
		"AWS_REGION":           c.Region,
		"GOOGLE_CLOUD_PROJECT": "samsara-data",

		// Runtime 7.x BigQuery connector configurations.
		// https://docs.databricks.com/data/data-sources/google/bigquery.html
		"GOOGLE_APPLICATION_CREDENTIALS": databricks.BigQueryCredentialsLocation(),
	}
	if len(c.CustomSparkEnvVars) != 0 {
		for k, v := range c.CustomSparkEnvVars {
			envVars[k] = v
		}
	}

	billingTeam := c.Owner
	if c.BillingTeamOverride != nil {
		billingTeam = *c.BillingTeamOverride
	}
	productGroup, ok := team.TeamProductGroup[billingTeam.Name()]
	// If we can't find a product group, use the team name.
	if ok {
		productGroup = strings.ToLower(productGroup)
	} else {
		productGroup = strings.ToLower(billingTeam.Name())
	}

	// The following disables all-purpose clusters from being able to be used for schedule Jobs/Workflows.
	// This field is not publicly exposed (see https://drive.google.com/file/d/1dtYdj0sJCl9o5LEdpISvJ_LQsz45qfhh/view?usp=sharing) and
	// was provided by Databricks support.
	clients := databricksresource.ClientTypes{}
	if c.CanRunJobs {
		clients.Notebooks = true
		clients.Jobs = true
	} else {
		clients.Notebooks = true
		clients.Jobs = false
	}
	workloadType := databricksresource.WorkloadTypes{
		Clients: clients,
	}

	// Temporary if to roll out datadog monitoring to only dataplatform for testing
	cluster := &databricksresource.Cluster{
		ClusterName:            c.Name,
		RuntimeEngine:          string(c.RuntimeEngine),
		SparkVersion:           string(sparkVersion),
		AutoTerminationMinutes: autoTerminationMinutes,
		AwsAttributes: &databricksresource.ClusterAwsAttributes{
			InstanceProfileArn: c.InstanceProfile,
			FirstOnDemand:      firstOnDemand,
			ZoneId:             NameAZ(c.Name, c.Region),
		},
		DriverNodeTypeId:  coalesce(c.CapacityOverrides.DriverNodeTypeId, "rd-fleet.xlarge"),
		NodeTypeId:        coalesce(c.CapacityOverrides.WorkerNodeTypeId, c.CapacityOverrides.DriverNodeTypeId, "rd-fleet.xlarge"),
		EnableElasticDisk: true,
		SparkConf:         conf.ToMap(),
		SparkEnvVars:      envVars,
		ClusterLogConf: &databricksresource.ClusterLogConf{
			S3: &databricksresource.S3StorageInfo{
				Destination: fmt.Sprintf("s3://%sdatabricks-cluster-logs/%s", awsregionconsts.RegionPrefix[c.Region], c.Name),
				Region:      c.Region,
			},
		},
		CustomTags: []databricksresource.ClusterTag{
			{
				Key:   "samsara:product-group",
				Value: strings.ToLower(productGroup),
			},
			{
				Key:   "samsara:service",
				Value: fmt.Sprintf("databrickscluster-%s", strings.ToLower(c.Name)),
			},
			{
				Key:   "samsara:team",
				Value: strings.ToLower(billingTeam.Name()),
			},
			{
				Key:   "samsara:rnd-allocation",
				Value: strconv.FormatFloat(c.RnDCostAllocation, 'f', -1, 64),
			},
			{
				Key:   "samsara:dataplatform-job-type",
				Value: string(dataplatformconsts.InteractiveCluster),
			},
			{
				Key:   "samsara:uc-enabled",
				Value: strconv.FormatBool(c.EnableUnityCatalog),
			},
		},
		EnableLocalDiskEncryption: c.EnableLocalDiskEncryption,
		Pin:                       c.Pin,
		WorkloadType:              &workloadType,
	}

	asMin := c.CapacityOverrides.MinWorkers
	asMax := c.CapacityOverrides.MaxWorkers
	if c.SingleNode {
		cluster.IsSingleNode = pointer.BoolPtr(true)
		cluster.Kind = pointer.StringPtr("CLASSIC_PREVIEW")
	} else if asMin == asMax && asMin != 0 {
		cluster.NumWorkers = pointer.IntPtr(asMin)
	} else {
		cluster.AutoScale = &databricksresource.ClusterAutoScale{
			MinWorkers: max(asMin, 1),
			MaxWorkers: max(asMax, c.CapacityOverrides.MaxWorkers, 8),
		}
	}

	regionPrefix := awsregionconsts.RegionPrefix[c.Region]

	// Set up all init scripts. We don't install bigquery-related things
	// in unity catalog clusters, as it's deprecated and we don't want to
	// allow access to it anymore.
	if c.EnableUnityCatalog {
		// Default init scripts.
		cluster.InitScripts = append(cluster.InitScripts,
			volumeInitScript(databricksinstaller.GetSparkRulesInitScript()),
		)
		// Install custom init scripts.
		for _, initScript := range c.InitScripts {
			cluster.InitScripts = append(cluster.InitScripts,
				volumeInitScript(databricksinstaller.GetInitScriptVolumeDestination(initScript)),
			)
		}
		if IsAllowListedTeamWorkspace(c.Owner.TeamName) {
			cluster.InitScripts = append(cluster.InitScripts,
				volumeInitScript(databricksinstaller.GetInitScriptVolumeDestination(databricksinstaller.SymlinkTeamWorkspacesInstaller)),
				volumeInitScript(databricksinstaller.GetInitScriptVolumeDestination(databricksinstaller.UCSymlinkSharedPythonInstaller)),
			)
		}
		if c.Name == "dataplatform" {
			cluster.InitScripts = append(cluster.InitScripts,
				volumeInitScript(databricksinstaller.GetInitScriptVolumeDestination(databricksinstaller.DatadogInstaller)),
			)
		}
	} else {
		// Default init scripts.
		cluster.InitScripts = append(cluster.InitScripts,
			s3InitScript(databricks.BigQueryCredentialsInitScript_OLD_DO_NOT_USE(regionPrefix), c.Region),
			s3InitScript(databricksinstaller.GetSparkRulesInitScript_OLD_DO_NOT_USE(regionPrefix), c.Region),
		)
		// Install custom init scripts.
		for _, initScript := range c.InitScripts {
			cluster.InitScripts = append(cluster.InitScripts,
				s3InitScript(databricks.GetInitScriptS3Destination_OLD_DO_NOT_USE(initScript, regionPrefix), c.Region),
			)
		}
		if IsAllowListedTeamWorkspace(c.Owner.TeamName) {
			cluster.InitScripts = append(cluster.InitScripts,
				s3InitScript(SymLinkTeamWorkspaceInitScript(c.Region), c.Region),
			)
		}
		if c.Name == "dataplatform" {
			cluster.InitScripts = append(cluster.InitScripts,
				s3InitScript(databricks.GetInitScriptS3Destination_OLD_DO_NOT_USE(databricksinstaller.DatadogInstaller, regionPrefix), c.Region),
			)
		}
	}

	// Install default libraries if necessary, and whatever custom libraries are
	// asked for per-cluster.
	var libraries []*databricksresource.Library
	if c.IncludeDefaultLibraries {
		for _, sparkLibrary := range DefaultLibraries(c.EnableUnityCatalog) {
			tfLibrary, err := sparkLibrary.TfResource(c.Region, c.EnableUnityCatalog)
			if err != nil {
				return nil, oops.Wrapf(err, "library resource")
			}
			libraries = append(libraries, tfLibrary)
		}

		gresearchLib := &databricksresource.Library{
			Maven: databricksresource.MavenLibrary{
				Coordinates: string(SparkMavenGResearch),
			},
		}
		libraries = append(libraries, gresearchLib)
	}
	for _, pkg := range c.PythonLibraries {
		tfLibrary, err := PyPIName(pkg).TfResource(c.Region, c.EnableUnityCatalog)
		if err != nil {
			return nil, oops.Wrapf(err, "python library resource")
		}
		libraries = append(libraries, tfLibrary)
	}
	for _, lib := range c.MavenLibraries {
		libraries = append(libraries, &databricksresource.Library{
			Maven: databricksresource.MavenLibrary{
				Coordinates: lib,
			},
		})
	}

	if libraries != nil && len(libraries) > 0 {
		librariesResource := &databricksresource.ClusterLibraries{
			ResourceName: cluster.ClusterName,
			ClusterId:    cluster.ResourceId().Reference(),
			Libraries:    libraries,
		}
		resources = append(resources, librariesResource)
	}

	if c.EnableUnityCatalog {
		// If OwnerUser is defined, it means that the cluster is a single-user cluster hence we set
		// the data security mode to be single user. Otherwise, we set the data security mode to be user isolation(shared).
		if !reflect.DeepEqual(c.OwnerUser, components.MemberInfo{}) {
			cluster.DataSecurityMode = databricksresource.DataSecurityModeSingleUser
			cluster.SingleUserName = c.OwnerUser.Email
		} else {
			cluster.DataSecurityMode = databricksresource.DataSecurityModeUserIsolation
		}
	} else {
		cluster.DataSecurityMode = databricksresource.DataSecurityModeNone
	}

	// Token in appconfig belongs to the machine user metrics account
	// databricksclustersupdater service uses this token to update all interactive clusters
	// with a docker URL. More information here: https://paper.dropbox.com/doc/RFC-Custom-Docker-Images-for-Python-in-Databricks-xrEXeA15yMZmki0dNAB53
	// Thus, this user needs manage permissions on all clusters
	metricsEmail := ""
	switch c.Region {
	case infraconsts.SamsaraAWSDefaultRegion:
		metricsEmail = "dev-databricks-metrics@samsara.com"
	case infraconsts.SamsaraAWSEURegion:
		metricsEmail = "dev-databricks-eu-metrics@samsara.com"
	case infraconsts.SamsaraAWSCARegion:
		metricsEmail = "dev-databricks-ca-metrics@samsara.com"
	default:
		return nil, oops.Errorf("unsupported region: %s", c.Region)
	}

	ownerTeam := c.Owner

	// If OwnerUser is defined, it means that the cluster is a single-user cluster hence we set
	// the clusterOwner to be the user. Otherwise, we set the clusterOwner to be the team.
	var clusterOwner DatabricksPrincipal
	if !reflect.DeepEqual(c.OwnerUser, components.MemberInfo{}) {
		clusterOwner = UserPrincipal{Email: c.OwnerUser.Email}
	} else {
		clusterOwner = TeamPrincipal{Team: ownerTeam}
	}

	permissionsResource, err := Permissions(PermissionsConfig{
		ResourceName: c.Name,
		ObjectType:   databricks.ObjectTypeCluster,
		ObjectId:     databricksresource.ClusterResourceId(c.Name).Reference(),
		Owner:        clusterOwner,
		Admins:       append([]DatabricksPrincipal{UserPrincipal{Email: metricsEmail}}, c.Admins...),
		Users:        c.Users,
		Readonly:     c.ReadOnlyUsers,
		Region:       c.Region,
	})
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, cluster, permissionsResource)
	return resources, nil
}

func s3InitScript(destination string, region string) *databricksresource.InitScript {
	return &databricksresource.InitScript{
		S3: &databricksresource.S3StorageInfo{
			Destination: destination,
			Region:      region,
		},
	}
}

func volumeInitScript(destination string) *databricksresource.InitScript {
	return &databricksresource.InitScript{
		Volumes: &databricksresource.VolumesStorageInfo{
			Destination: destination,
		},
	}
}
