package dataplatformprojects

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var legacyPolicies = map[string]bool{ // lint:sorted
	"accounting":                        true,
	"accountspayable":                   true,
	"biztech-dbt-dev":                   true,
	"biztech-dbt-prod":                  true,
	"biztech-dbt-sw-dev":                true,
	"biztech-dbt-sw-sensitive-dev":      true,
	"biztech-dbt-uat":                   true,
	"biztech-prod":                      true,
	"biztech-sensitive-dbt-prod":        true,
	"biztechenterprisedata":             true,
	"biztechenterprisedata-admin":       true,
	"biztechenterprisedata-silver":      true,
	"biztechenterprisedataadmin":        true,
	"biztechenterprisedataapplication":  true,
	"biztechenterprisedatacontractors":  true,
	"biztechenterprisedatasensitive":    true,
	"businesssystems":                   true,
	"businesssystemsrecruiting":         true,
	"contentmarketing":                  true,
	"customersuccessoperations":         true,
	"epofinance":                        true,
	"financialoperations":               true,
	"finssensitive":                     true,
	"fivetran-greenhouse-recruiting":    true,
	"growth":                            true,
	"gtms":                              true,
	"marketing-data-analytics-prod":     true,
	"marketingdataanalytics":            true,
	"marketingdataanalyticscontractors": true,
	"mda-production":                    true,
	"mda-sandbox":                       true,
	"mda-stage":                         true,
	"netsuite-finance":                  true,
	"salesdataanalytics":                true,
	"salesengineering":                  true,
	"seops":                             true,
	"supplychain":                       true,
	"support":                           true,
	// TODO: Remove the generic "UC" policies as we want to keep teams on per-team cluster policies for
	// tagging purposes (and also so they can customize what libraries and such are installed via the policy)
	// For more context: https://samsaradev.atlassian.net/l/cp/K8g1N7UA
	"unity-catalog":     true,
	"unity-catalog-dlt": true,
}

type ClusterPolicyConfig struct {
	Name string
	Team components.TeamInfo
}

// Cluster policies to create automated job clusters for the teams listed below
// The policy name will always have "automated-job-cluster" appended.
var customJobClusterPolicies = []ClusterPolicyConfig{ // lint: +sorted
	{Name: "biztech-dbt-dev", Team: team.BizTechEnterpriseData},
	{Name: "biztech-dbt-prod", Team: team.BizTechEnterpriseDataApplication},
	{Name: "biztech-dbt-sw-dev", Team: team.BizTechEnterpriseData},
	{Name: "biztech-dbt-sw-sensitive-dev", Team: team.BizTechEnterpriseDataSensitive},
	{Name: "biztech-dbt-uat", Team: team.BizTechEnterpriseDataApplication},
	{Name: "biztech-prod", Team: team.BizTechEnterpriseData},
	{Name: "biztech-sensitive-dbt-prod", Team: team.BizTechEnterpriseDataSensitive},
	{Name: "biztechenterprisedata-admin", Team: team.BizTechEnterpriseData},
	{Name: "biztechenterprisedata-silver", Team: team.BizTechEnterpriseData},
	{Name: "dataplatform-dagster", Team: team.DataPlatform},
	{Name: "datascience-admin", Team: team.DataScience},
	{Name: "fivetran-greenhouse-recruiting", Team: team.BusinessSystemsRecruiting},
	{Name: "marketing-data-analytics-prod", Team: team.MarketingDataAnalytics},
	{Name: "mda-production", Team: team.MarketingDataAnalytics},
	{Name: "mda-sandbox", Team: team.MarketingDataAnalytics},
	{Name: "mda-stage", Team: team.MarketingDataAnalytics},
	{Name: "netsuite-finance", Team: team.BusinessSystems},
}

// Cluster policies to create delta live table clusters for the teams listed below
// The policy name will always have "automated-dlt-cluster" appended.
var customDLTClusterPolicies = []ClusterPolicyConfig{ // lint: +sorted
	{Name: "biztechenterprisedata", Team: team.BizTechEnterpriseData},
}

// Cluster policiy configs for teams that have Databricks users and clusters
func makeTeamPolicyConfigs() []ClusterPolicyConfig {
	var configs []ClusterPolicyConfig
	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		configs = append(configs, ClusterPolicyConfig{
			Name: t.TeamName,
			Team: t,
		})
	}
	return configs
}

func ClusterPolicyTfProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	clusterPolicies, policyPermissions, err := makeClusterPolicies(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create cluster policies")
	}

	dbxProvider := dataplatformresource.DatabricksOauthProvider(config.Hostname)
	policyProject := &project.Project{
		RootTeam: team.DataPlatform.TeamName,
		Provider: providerGroup,
		Class:    "clusterpolicies",
		Name:     "all_cluster_policies",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": dbxProvider,
			"cluster_policies":    clusterPolicies,
			"policy_permissions":  policyPermissions,
		},
	}

	policyProject.ResourceGroups = project.MergeResourceGroups(policyProject.ResourceGroups,
		map[string][]tf.Resource{
			"tf_backend": resource.ProjectTerraformBackend(policyProject),
		})

	return policyProject, nil
}

func ucEnabledPolicy(policyName string) bool {
	// If the policy is marked as legacy then dont enable UC for it.
	if legacyPolicies[policyName] == true {
		return false
	}

	// Non legacy and anything new will have UC enabled by default.
	return true
}

func isSafetyPlatformPolicy(policyName string) bool {
	return strings.HasPrefix(policyName, "safetyplatform")
}

func makeClusterPolicies(providerGroup string) ([]tf.Resource, []tf.Resource, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, nil, err
	}

	var policies []tf.Resource
	var permissions []tf.Resource

	jobPoliciesToGenerate := makeTeamPolicyConfigs()
	jobPoliciesToGenerate = append(jobPoliciesToGenerate, customJobClusterPolicies...)

	for _, c := range jobPoliciesToGenerate {
		name := strings.ToLower(c.Name)
		definition, err := defaultJobPolicyDefinition(providerGroup, name, c.Team)
		if err != nil {
			return nil, nil, err
		}
		policyName := fmt.Sprintf("%s-automated-job-cluster", name)

		policies = append(policies, &databricksresource.ClusterPolicy{
			Name:       policyName,
			Definition: definition,
		})

		perms, err := dataplatformresource.Permissions(dataplatformresource.PermissionsConfig{
			ResourceName: policyName,
			ObjectType:   databricks.ObjectTypeClusterPolicy,
			ObjectId:     databricksresource.ClusterPolicyResourceId(policyName).Reference(),
			Owner:        dataplatformresource.TeamPrincipal{Team: c.Team},
			Region:       config.Region,
		})
		if err != nil {
			return nil, nil, oops.Wrapf(err, "failed to create perms for %s", policyName)
		}
		permissions = append(permissions, perms)
	}

	for _, c := range customDLTClusterPolicies {
		name := strings.ToLower(c.Name)
		definition, err := deltaLiveTablesPolicyDefinition(providerGroup, name, c.Team)
		if err != nil {
			return nil, nil, err
		}
		policyName := fmt.Sprintf("%s-automated-dlt-cluster", name)

		policies = append(policies, &databricksresource.ClusterPolicy{
			Name:       policyName,
			Definition: definition,
		})

		perms, err := dataplatformresource.Permissions(dataplatformresource.PermissionsConfig{
			ResourceName: policyName,
			ObjectType:   databricks.ObjectTypeClusterPolicy,
			ObjectId:     databricksresource.ClusterPolicyResourceId(policyName).Reference(),
			Owner:        dataplatformresource.TeamPrincipal{Team: c.Team},
			Region:       config.Region,
		})
		if err != nil {
			return nil, nil, oops.Wrapf(err, "failed to create perms for %s", policyName)
		}
		permissions = append(permissions, perms)
	}

	// Create a unity catalog job cluster policy for UI scheduled jobs.
	ucJobClusterPolicy, ucJobClusterPerms, err := ucClusterPolicyAndPerms(providerGroup, "unity-catalog", "job")
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to uc job cluster policy")
	}
	policies = append(policies, ucJobClusterPolicy)
	permissions = append(permissions, ucJobClusterPerms)

	// Create a unity catalog delta live tables cluster policy.
	ucDltClusterPolicy, ucDltClusterPerms, err := ucClusterPolicyAndPerms(providerGroup, "unity-catalog-dlt", "dlt")
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to uc job cluster policy")
	}
	policies = append(policies, ucDltClusterPolicy)
	permissions = append(permissions, ucDltClusterPerms)

	return policies, permissions, nil
}

func defaultJobPolicyDefinition(providerGroup string, name string, t components.TeamInfo) (string, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return "", err
	}
	policyName := strings.ToLower(name)
	region := config.Region
	// TODO: replace the default with unity-catalog-cluster once biztech migration is complete.
	arn, err := dataplatformresource.InstanceProfileArn(providerGroup, strings.ToLower(policyName)+"-cluster")
	if err != nil {
		return "", err
	}

	// For Unity Catalog enabled policies, use the unity-catalog-cluster instance
	// profile, which has minimal permissions. SafetyPlatform team is excluded
	// from this since they have special requirements to work with airflow.
	if ucEnabledPolicy(policyName) && !isSafetyPlatformPolicy(policyName) {
		arn, err = dataplatformresource.InstanceProfileArn(providerGroup, "unity-catalog-cluster")
		if err != nil {
			return "", err
		}
	}
	definition := defaultPolicyDefinition(false, "job", policyName, arn, region, t)
	return strings.TrimSpace(definition), nil
}

func deltaLiveTablesPolicyDefinition(providerGroup string, name string, t components.TeamInfo) (string, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return "", err
	}

	teamName := strings.ToLower(t.TeamName)
	policyName := strings.ToLower(name)
	region := config.Region

	arn, err := dataplatformresource.InstanceProfileArn(providerGroup, teamName+"-cluster")
	if err != nil {
		return "", err
	}
	definition := defaultPolicyDefinition(false, "dlt", policyName, arn, region, t)
	return strings.TrimSpace(definition), nil
}

// Add a unity catalog cluster policy to be shared amongst all users for scheduling UI jobs.
func ucClusterPolicyAndPerms(providerGroup string, name string, clusterType string) (*databricksresource.ClusterPolicy, *databricksresource.Permissions, error) {
	policyName := fmt.Sprintf("%s-automated-job-cluster", name)

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, nil, err
	}
	arn, err := dataplatformresource.InstanceProfileArn(providerGroup, "unity-catalog-cluster")
	if err != nil {
		return nil, nil, err
	}

	definition := strings.TrimSpace(defaultPolicyDefinition(true, clusterType, policyName, arn, config.Region, team.DataPlatform))

	// Improvement: The cluster policies API now allows us to specify libraries to install on the job compute.
	// For unity catalog, we should update our custom provider to include this field so we can automatically install the service credentials
	// library to support boto3 usage (so users don't have to do this manually when they create a job).
	// https://docs.databricks.com/api/workspace/clusterpolicies/create
	policy := &databricksresource.ClusterPolicy{
		Name:       policyName,
		Definition: definition,
	}

	var teamsWithClusters []dataplatformresource.DatabricksPrincipal
	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		teamsWithClusters = append(teamsWithClusters, dataplatformresource.TeamPrincipal{
			Team: t,
		})
	}
	perms, err := dataplatformresource.Permissions(dataplatformresource.PermissionsConfig{
		ResourceName: policyName,
		ObjectType:   databricks.ObjectTypeClusterPolicy,
		ObjectId:     databricksresource.ClusterPolicyResourceId(policyName).Reference(),
		Owner:        dataplatformresource.TeamPrincipal{Team: team.DataPlatform},
		// Block RnD user access to this profile, as their team profile now migrated to UC.
		Users: []dataplatformresource.DatabricksPrincipal{
			dataplatformresource.GroupPrincipal{GroupName: dataplatformterraformconsts.SamsaraNonRnDGroup},
		},
		Region: config.Region,
	})
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to create perms for %s", policyName)
	}

	return policy, perms, nil
}

func getNodeTypePattern() string {
	regex := "("

	for i, instanceType := range dataplatformconsts.DatabricksNitroInstanceTypePrefixes {
		regex = regex + instanceType
		if i != len(dataplatformconsts.DatabricksNitroInstanceTypePrefixes)-1 {
			regex = regex + "|"
		}
	}
	regex = regex + ")" + ".[0-8]{0,1}x?large"

	// Final regex will look something like:
	// (m-fleet|md-fleet|m5dn|m5n|m5zn|m6i|m7i|m6id|m6in|m6idn|m6a|m7a|c5a|c5ad|c5n|c6i|c6id|c7i|c6in|c6a|c7a|r-fleet|rd-fleet|r6i|r7i|r7iz|r6id|r6in|r6idn|r6a|r7a|d3|d3en|p3dn|r5dn|r5n|i4i|i3en|g4dn|g5|p4d|p4de|p5).[0-8]{0,1}x?large.
	return regex
}

func getDefaultDriverNodeType() string {
	return "rd-fleet.xlarge"
}

func getDefaultWorkerNodeType() string {
	return "md-fleet.xlarge"
}

// Helper function to be used during the phased rollout of the cloud credentials for the cluster policies.
func isPolicyCloudCredentialMigrated(policyName string) bool {
	nonTeamUcClusterPolicies := map[string]bool{
		"unity-catalog-automated-job-cluster":     true,
		"unity-catalog-dlt-automated-job-cluster": true,
	}
	if _, exists := nonTeamUcClusterPolicies[policyName]; exists {
		return false
	}

	return true
}

func defaultPolicyDefinition(ucPolicy bool, clusterType, policyName, instanceProfileArn, region string, t components.TeamInfo) string {
	teamName := strings.ToLower(t.TeamName)
	productGroup, ok := team.TeamProductGroup[t.TeamName]
	// If we can't find a product group, use the team name.
	if ok {
		productGroup = strings.ToLower(productGroup)
	} else {
		productGroup = teamName
	}
	bucketPrefix := awsregionconsts.RegionPrefix[region]

	clusterLogPathS3 := "databricks-cluster-logs"
	// Output Delta Live Table cluster logs to their own path in the cluster logs bucket, to separate them from the job cluster logs.
	if clusterType == "dlt" {
		clusterLogPathS3 += "/deltalivetables"
	}

	nodeTypePattern := getNodeTypePattern()
	driverNodeType := getDefaultDriverNodeType()
	workerNodeType := getDefaultWorkerNodeType()

	sparkVersion := ""
	defaultCloudCredentialSparkEnv := ""

	teamTag := fmt.Sprintf(`"custom_tags.samsara:team": {
    "type": "fixed",
    "value": "%s"
  },
  `, teamName)

	productGroupTag := fmt.Sprintf(`"custom_tags.samsara:product-group": {
    "type": "fixed",
    "value": "%s"
  },
  `, productGroup)

	allowedLanguages := ""

	bigQueryInitScript := fmt.Sprintf(`"init_scripts.0.s3.destination": {
    "type": "fixed",
    "value": "s3://%sdataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  "init_scripts.0.s3.region": {
    "type": "fixed",
    "value": "%s"
  },
  `, bucketPrefix, region)

	gluePartitionSegments := `"spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "regex",
    "defaultValue": "1",
    "pattern": "[1-5]"
  },
  `

	glueMaxRetries := `"spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "fixed",
    "value": "10"
  },
  `

	hiveMetastoreGlueCatalog := `"spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "fixed",
    "value": "true"
  },
  `

	hiveMetastorePoolSize := `"spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "regex",
    "defaultValue": "3",
    "pattern": "([1-9]|1[0])"
  },
  `

	runtimeEngine := `"runtime_engine": {
    "type": "unlimited",
    "defaultValue": "STANDARD"
  },
  `
	dataSecurityMode := ""

	if ucEnabledPolicy(policyName) {
		dataSecurityMode = `"data_security_mode": {
    "type": "fixed",
    "value": "SINGLE_USER"
  },
  `
	}
	catalogOverride := "hive_metastore"
	lokifileStatusCache := ""
	hadoopDatabricksLokifileStatusCache := ""
	modTimeCheck := ""

	if ucPolicy {
		// Do not set a team tag or a product group tag. The UC job cluster policy is shared amongst all users/teams,
		// so there's no sense of a team/product group owner.
		teamTag = ""
		productGroupTag = ""
	}

	if ucPolicy || ucEnabledPolicy(policyName) {
		// Unmanaged jobs should have the default cloud credential spark env var set to the team's cloud credential.
		// This is so that the team's cloud credential is used for all unmanaged jobs with this cluster policy.
		teamCredentialName := fmt.Sprintf("team-%s", strings.ToLower(t.TeamName))
		if isPolicyCloudCredentialMigrated(policyName) {
			defaultCloudCredentialSparkEnv = fmt.Sprintf(`"spark_env_vars.DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": {
    "type": "unlimited",
    "defaultValue": "%s"
  },
  `, teamCredentialName)
		}

		sparkVersion = fmt.Sprintf(`"spark_version": {
    "type": "fixed",
    "value": "%s"
  },
  `, sparkversion.DefaultClusterPolicyDbrVersion)

		if t.TeamName == team.SafetyPlatform.TeamName {
			sparkVersion = fmt.Sprintf(`"spark_version": {
    "type": "allowlist",
    "defaultValue": "%s",
    "values": ["%s", "%s"]
  },
  `, sparkversion.DefaultClusterPolicyDbrVersion, sparkversion.DefaultClusterPolicyDbrVersion, sparkversion.SparkVersion133xScala212)
		}

		allowedLanguages = `"spark_conf.spark.databricks.repl.allowedLanguages": {
    "type": "forbidden"
  },
  `

		lokifileStatusCache = `"spark_conf.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  `
		hadoopDatabricksLokifileStatusCache = `"spark_conf.spark.hadoop.databricks.loki.fileStatusCache.enabled": {
    "type": "fixed",
    "value": "false"
  },
  `
		modTimeCheck = `"spark_conf.spark.databricks.scan.modTimeCheck.enabled": {
    "type": "fixed",
    "value": "false"
  },
  `
		// Install init scripts via volumes.
		bigQueryInitScript = `"init_scripts.0.volumes.destination": {
    "type": "fixed",
    "value": "/Volumes/s3/dataplatform-deployed-artifacts/init_scripts/load-bigquery-credentials.sh"
  },
  `

		gluePartitionSegments = `"spark_conf.spark.hadoop.aws.glue.partition.num.segments": {
    "type": "forbidden"
  },
  `
		glueMaxRetries = `"spark_conf.spark.hadoop.aws.glue.max-error-retries": {
    "type": "forbidden"
  },
  `
		hiveMetastoreGlueCatalog = `"spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled": {
    "type": "forbidden"
  },
  `
		hiveMetastorePoolSize = `"spark_conf.spark.databricks.hive.metastore.client.pool.size": {
    "type": "forbidden"
  },
  `
		// Override the default catalog to "default" for UC clusters.
		catalogOverride = "default"
	}

	variableMap := map[string]interface{}{
		"ClusterType":                         clusterType,
		"SparkVersion":                        sparkVersion,
		"NodeTypePattern":                     nodeTypePattern,
		"WorkerNodeType":                      workerNodeType,
		"DriverNodeType":                      driverNodeType,
		"TeamTag":                             teamTag,
		"PolicyName":                          policyName,
		"ProductGroupTag":                     productGroupTag,
		"GlueMaxRetries":                      glueMaxRetries,
		"HiveMetastoreGlueCatalog":            hiveMetastoreGlueCatalog,
		"AllowedLanguages":                    allowedLanguages,
		"BucketPrefix":                        bucketPrefix,
		"GluePartitionSegments":               gluePartitionSegments,
		"HiveMetastorePoolSize":               hiveMetastorePoolSize,
		"InstanceProfileArn":                  instanceProfileArn,
		"ClusterLogPathS3":                    clusterLogPathS3,
		"Region":                              region,
		"LokifileStatusCache":                 lokifileStatusCache,
		"HadoopDatabricksLokifileStatusCache": hadoopDatabricksLokifileStatusCache,
		"ModTimeCheck":                        modTimeCheck,
		"DataSecurityMode":                    dataSecurityMode,
		"RuntimeEngine":                       runtimeEngine,
		"BigQueryInitScript":                  bigQueryInitScript,
		"CatalogOverride":                     catalogOverride,
		"DefaultCloudCredentialSparkEnv":      defaultCloudCredentialSparkEnv,
	}

	stringTemplate := `
{
  "cluster_type": {
    "type": "fixed",
    "value": "{{.ClusterType}}"
  },
  {{.SparkVersion}}"node_type_id": {
    "type": "regex",
    "pattern": "{{.NodeTypePattern}}",
    "defaultValue": "{{.WorkerNodeType}}"
  },
  "driver_node_type_id": {
    "type": "regex",
    "pattern": "{{.NodeTypePattern}}",
    "defaultValue": "{{.DriverNodeType}}"
  },
  {{.TeamTag}}"custom_tags.samsara:service": {
    "type": "fixed",
    "value": "databricks{{.ClusterType}}cluster-{{.PolicyName}}"
  },
  {{.ProductGroupTag}}"custom_tags.samsara:rnd-allocation": {
    "type": "unlimited",
    "defaultValue": "1"
  },
  "spark_conf.viewMaterializationDataset": {
    "type": "fixed",
    "value": "spark_view_materialization"
  },
  "spark_conf.spark.databricks.delta.stalenessLimit": {
    "type": "unlimited",
    "defaultValue": "1h"
  },
  "spark_conf.spark.sql.extensions": {
    "type": "fixed",
    "value": "com.samsara.dataplatform.sparkrules.Extension"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.logRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.spark.hadoop.fs.s3a.acl.default": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  {{.GlueMaxRetries}}{{.HiveMetastoreGlueCatalog}}"spark_conf.spark.hadoop.fs.gs.project.id": {
    "type": "fixed",
    "value": "samsara-data"
  },
  "spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": {
    "type": "unlimited",
    "defaultValue": "256MB"
  },
  "spark_conf.spark.driver.maxResultSize": {
    "type": "unlimited",
    "defaultValue": "8000000000"
  },
  "spark_conf.spark.hadoop.google.cloud.auth.service.account.enable": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.hadoop.fs.s3a.canned.acl": {
    "type": "fixed",
    "value": "BucketOwnerFullControl"
  },
  "spark_conf.spark.databricks.delta.history.metricsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.delta.autoCompact.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.temporaryGcsBucket": {
    "type": "fixed",
    "value": "samsara-bigquery-spark-connector"
  },
  "spark_conf.spark.databricks.service.server.enabled": {
    "type": "fixed",
    "value": "true"
  },
  {{.AllowedLanguages}}"spark_conf.spark.sql.warehouse.dir": {
    "type": "fixed",
    "value": "s3://{{.BucketPrefix}}databricks-playground/warehouse/"
  },
  "spark_conf.spark.databricks.delta.properties.defaults.checkpointRetentionDuration": {
    "type": "fixed",
    "value": "INTERVAL 30 DAYS"
  },
  "spark_conf.viewsEnabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.credentialsFile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.sources.default": {
    "type": "fixed",
    "value": "delta"
  },
  {{.GluePartitionSegments}}"spark_conf.spark.sql.adaptive.skewJoin.skewedPartitionFactor": {
    "type": "unlimited",
    "defaultValue": "2"
  },
  "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  {{.HiveMetastorePoolSize}}"spark_conf.spark.hadoop.google.cloud.auth.service.account.json.keyfile": {
    "type": "fixed",
    "value": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "unlimited",
    "defaultValue": "auto"
  },
  "spark_conf.spark.default.parallelism": {
    "type": "unlimited",
    "defaultValue": "4096"
  },
  "spark_conf.spark.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.rddBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "spark_conf.spark.storage.decommission.shuffleBlocks.enabled": {
    "type": "allowlist",
    "values": ["true", "false"],
    "defaultValue": "true"
  },
  "aws_attributes.instance_profile_arn": {
    "type": "fixed",
    "value": "{{.InstanceProfileArn}}"
  },
  "cluster_log_conf.type": {
    "type": "fixed",
    "value": "S3"
  },
  "cluster_log_conf.path": {
    "type": "fixed",
    "value": "s3://{{.BucketPrefix}}{{.ClusterLogPathS3}}/{{.PolicyName}}"
  },
  "cluster_log_conf.region": {
    "type": "fixed",
    "value": "{{.Region}}"
  },
  "autotermination_minutes": {
    "type": "unlimited",
    "defaultValue": 1440
  },
  "autoscale.min_workers": {
    "type": "unlimited",
    "defaultValue": 1
  },
  "autoscale.max_workers": {
    "type": "unlimited",
    "defaultValue": 8
  },
  {{.DefaultCloudCredentialSparkEnv}}"spark_env_vars.AWS_REGION": {
    "type": "unlimited",
    "defaultValue": "{{.Region}}"
  },
  "spark_env_vars.AWS_DEFAULT_REGION": {
    "type": "unlimited",
    "defaultValue": "{{.Region}}"
  },
  {{.LokifileStatusCache}}{{.HadoopDatabricksLokifileStatusCache}}{{.ModTimeCheck}}"spark_env_vars.GOOGLE_CLOUD_PROJECT": {
    "type": "unlimited",
    "defaultValue": "samsara-data"
  },
  {{.DataSecurityMode}}"spark_env_vars.GOOGLE_APPLICATION_CREDENTIALS": {
    "type": "unlimited",
    "defaultValue": "/databricks/samsara-data-5142c7cd3ba2.json"
  },
  {{.RuntimeEngine}}{{.BigQueryInitScript}}"spark_conf.spark.databricks.sql.initial.catalog.name": {
    "type": "fixed",
    "value": "{{.CatalogOverride}}"
  }
}`

	// Parse and execute template
	tmplParsed, err := template.New("config").Parse(stringTemplate)
	if err != nil {
		panic(err)
	}

	var result bytes.Buffer
	err = tmplParsed.Execute(&result, variableMap)
	if err != nil {
		panic(err)
	}

	return result.String()
}
