package unitycatalog

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const UCSyncJobMaxWorkers = 4
const UCAdminInstanceProfile = "admin-cluster"

func UcSyncJobProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	syncJobArn, err := dataplatformresource.InstanceProfileArn(providerGroup, UCAdminInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, syncJobArn),
		},
	}

	var projects []*project.Project

	jobResources, err := ucSyncJob(config)
	if err != nil {
		return nil, err
	}
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "uc_sync_job",
		ResourceGroups: map[string][]tf.Resource{
			"job":                 jobResources,
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
	})

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
			"infra_remote": instanceProfileResources,
		})
	}
	return projects, nil
}

type syncDirection string

const (
	// Specifies the mode of the sync job to sync from HMS to Default Catalog.
	HmsToDefault syncDirection = "forward"
	// Specifies the mode of the sync job to sync from Default Catalog to HMS.
	DefaultToHms syncDirection = "backward"
)

func getJobParameters(direction syncDirection) []string {
	if direction == HmsToDefault {
		// Create Forward Sync Job Parameters.
		return []string{
			"--from-catalog-name", "hive_metastore",
			"--to-catalog-name", "default",
			"--resync-all",
		}
	} else {
		// Create Reverse Sync Job Parameters.
		return []string{
			"--from-catalog-name", "default",
			"--to-catalog-name", "hive_metastore",
			"--resync-all",
		}
	}
}

func ucSyncJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var tfResources []tf.Resource

	// Load the UC Sync script
	ucSyncScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/unity_catalog/unity_catalog_sync.py")
	if err != nil {
		return nil, oops.Wrapf(err, "loading UC sync script")
	}
	tfResources = append(tfResources, ucSyncScript)

	// Define job name with its respective list of DBs to sync
	jobDefinitions := []struct {
		name          string
		dbNames       []string
		syncDirection syncDirection
		AdminTeams    []components.TeamInfo
	}{
		{"unity_catalog_sync_job_dataplat", []string{"dojo"}, HmsToDefault, []components.TeamInfo{team.MLInfra, team.MLScience}}, // Sync the dojo DB from HMS to UC for AI/ML team's superset/ray use cases.
		{"unity_catalog_reverse_sync_job_dataplat", getSyncDBs(), DefaultToHms, []components.TeamInfo{team.MLInfra}},
		{"unity_catalog_reverse_sync_job_misc", getMiscSyncDBs(config.Region), DefaultToHms, []components.TeamInfo{team.MLInfra}},
		{"unity_catalog_reverse_sync_job_rds", extractDBNames(databaseregistries.GetRdsDeltaLakeDbsInRegion(config.Region)), DefaultToHms, nil},
	}

	for _, job := range jobDefinitions {
		syncJobResources, err := getSyncJobResources(config, job.name, job.dbNames, job.syncDirection, job.AdminTeams, ucSyncScript)
		if err != nil {
			return nil, oops.Wrapf(err, "creating job spec for %s", job.name)
		}
		tfResources = append(tfResources, syncJobResources...)
	}

	return tfResources, nil
}

// getMiscSyncDBs returns the list of miscellaneous DBs to sync, filtered by region.
// Some databases are only available in specific regions.
func getMiscSyncDBs(region string) []string {
	// DBs available in all regions
	dbNames := []string{
		"dojo",
		"datamodel_core",
		"datamodel_dev",
		"dataprep",
		"signalpromotiondb",
		"dataengineering_dev",
		"dataengineering",
		"safety_map_data",
		"product_analytics_staging",
		"product_analytics",
		"definitions",
		"messagesdb_hashed",
		"dataanalytics_dev",
		"connectedequipment_dev",
		"firmware_dev",
	}

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		// US has mixpanel_samsara and datamodel_launchdarkly_bronze
		dbNames = append(dbNames, "mixpanel_samsara", "datamodel_launchdarkly_bronze")
	case infraconsts.SamsaraAWSEURegion:
		// EU has mixpanel_samsara only
		dbNames = append(dbNames, "mixpanel_samsara")
	case infraconsts.SamsaraAWSCARegion:
		// CA has no additional region-specific DBs
	}

	sort.Strings(dbNames)
	return dbNames
}

// Get a list of dataplatform owned DBs to sync from UC to HMS for AI/ML team's superset/ray use cases.
func getSyncDBs() []string {
	backwardsDBs := []string{
		"kinesisstats_history",
		"kinesisstats",

		// DBs with UC migration complete
		"s3bigstats",
		"s3bigstats_history",
		"dynamodb",
		"datastreams",
		"datastreams_history",
		"report_staging",
	}

	datapipelinesDBs, err := getDataPipelinesDBs()
	if err != nil {
		// If we can't get the list of DBs, we should still continue with creating the sync job for the remaining DBs.
		datapipelinesDBs = []string{}
	}
	return append(datapipelinesDBs, backwardsDBs...)
}

func getDataPipelinesDBs() ([]string, error) {
	TransformationDir := filepath.Join(dataplatformterraformconsts.DataPlatformRoot, "tables/transformations")

	// Use a map to deduplicate database names
	dbNamesSet := make(map[string]struct{})

	walkErr := filepath.Walk(TransformationDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %v", path, err)
		}
		if info.IsDir() || strings.HasSuffix(info.Name(), ".test.json") || info.Name() == "CODEREVIEW" {
			return nil
		}

		if filepath.Ext(info.Name()) == ".json" {
			var config struct {
				Output struct {
					DBName string `json:"dbname"`
				} `json:"output"`
			}

			// Read the file
			fileBytes, err := os.ReadFile(path)
			if err != nil {
				log.Printf("error reading config file %s: %v", path, err)
				return nil // Skip this file but continue
			}

			// Unmarshal the JSON into the struct
			err = json.Unmarshal(fileBytes, &config)
			if err != nil {
				log.Printf("error unmarshaling JSON in file %s: %v", path, err)
				return nil // Skip this file but continue
			}

			dbNamesSet[config.Output.DBName] = struct{}{}
		}
		return nil
	})

	if walkErr != nil {
		return nil, oops.Wrapf(walkErr, "Error walking transformations directory to fetch DB names")
	}

	// Convert the map keys to a slice for deduplicated database names
	var dbNames []string
	for dbName := range dbNamesSet {
		dbNames = append(dbNames, dbName)
	}

	sort.Strings(dbNames)

	return dbNames, nil
}

func getSyncJobResources(config dataplatformconfig.DatabricksConfig, jobName string, dbNames []string, direction syncDirection, adminTeams []components.TeamInfo, ucSyncScript *awsresource.S3BucketObject) ([]tf.Resource, error) {
	emailAlerts := []string{team.DataPlatform.SlackAlertsChannelEmail.Email}

	// sync job requires the following glue-related spark conf settings despite being UC-enabled
	sparkConf := map[string]string{
		// Use Glue Catalog to enforce table access via IAM.
		"spark.databricks.hive.metastore.glueCatalog.enabled": "true",
		"spark.hadoop.aws.glue.max-error-retries":             "10", // Default without override is 5
		// Max concurrent API calls per cluster = num.segments * pool.size
		"spark.hadoop.aws.glue.partition.num.segments":     "1", // Default without override is 5
		"spark.databricks.hive.metastore.client.pool.size": "3", // Default without override is 5
		// Describe Extended output is truncated by default. This increases the limit from 25 to 2000
		// so that we can compare the full schema of the tables including large proto columns.
		"spark.sql.debug.maxToStringFields": "2000",
	}

	cron := "0 0 */6 * * ?"
	if jobName == "unity_catalog_sync_job_dataplat" {
		// We only want to run this job manually when new tables are added to the dojo DB.
		cron = ""
	}

	// Get job owner for region
	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no job owner for region %s", config.Region)
	}

	// Define Unity Catalog settings
	ucSettings := dataplatformresource.UnityCatalogSetting{
		DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
		SingleUserName:   ciServicePrincipalAppId,
	}

	jobParams := getJobParameters(direction)
	jobParams = append(jobParams, "--job-name", jobName)

	spec := dataplatformresource.JobSpec{
		Name:                         jobName,
		Region:                       config.Region,
		Owner:                        team.DataPlatform,
		Script:                       ucSyncScript,
		SparkVersion:                 sparkversion.UcSyncJobDbrVersion,
		Parameters:                   append(jobParams, "--databases", strings.Join(dbNames, ",")),
		Profile:                      tf.LocalId("instance_profile").Reference(),
		MaxWorkers:                   UCSyncJobMaxWorkers,
		Cron:                         cron,
		DriverNodeType:               "rd-fleet.xlarge",
		WorkerNodeType:               "rd-fleet.xlarge",
		EmailNotifications:           emailAlerts,
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		AdminTeams:                   adminTeams,
		Tags:                         map[string]string{},
		RnDCostAllocation:            1,
		IsProduction:                 true,
		JobType:                      dataplatformconsts.UnityCatalogSync,
		Format:                       databricks.MultiTaskKey,
		JobTags:                      map[string]string{"format": databricks.MultiTaskKey},
		UnityCatalogSetting:          ucSettings,
		SparkConf:                    sparkConf,
		SloConfig: &dataplatformconsts.JobSlo{
			SloTargetHours:              24,
			Urgency:                     dataplatformconsts.JobSloBusinessHoursHigh,
			BusinessHoursThresholdHours: 24,
			HighUrgencyThresholdHours:   96,
		},
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := spec.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource for %s", jobName)
	}
	jobPermissionsResource, err := spec.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job permissions resource for %s", jobName)
	}

	return append([]tf.Resource{}, jobResource, jobPermissionsResource), nil
}

// extractDBNames extracts names from a list of Database objects
func extractDBNames(databases []databaseregistries.SamsaraDB) []string {
	names := make([]string, len(databases))
	for i, db := range databases {
		names[i] = db.Name
	}
	sort.Strings(names)
	return names
}
