package rdslakeprojects

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

type combineShardTable struct {
	TableName         string `json:"table_name"`
	CustomSelectQuery string `json:"custom_select_query,omitempty"`
}

type combineShardDb struct {
	DbName             string              `json:"db_name"`
	ShardNames         []string            `json:"shard_names"`
	Tables             []combineShardTable `json:"tables"`
	HasProductionTable bool                `json:"has_production_table"`
}

func combineShardsProject(config dataplatformconfig.DatabricksConfig, remotes remoteResources) (*project.Project, error) {

	resources_uc, err := combineShardsUCAllResources(config)

	if err != nil {
		return nil, oops.Wrapf(err, "error generated combined shard uc resources")
	}
	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "rdscombineshards",
		Name:     "combine_shards_all",
		ResourceGroups: project.MergeResourceGroups(
			map[string][]tf.Resource{
				"databricks_provider":         dataplatformresource.DatabricksOauthProvider(config.Hostname),
				"infra_remote":                remotes.infra,
				"pool_remote":                 remotes.instancePool,
				"combine_shards_uc_resources": resources_uc,
			},
		),
	}, nil
}

// Create a combined shards job for sharded dbs migrated to UC.
func combineShardsUCAllResources(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource
	var dbSpecs []combineShardDb

	for _, db := range rdsdeltalake.AllDatabases() {
		// Skip empty or unsharded dbs
		if !db.IsInRegion(config.Region) || len(db.Tables) == 0 || !db.Sharded {
			continue
		}

		// Get the spark friendly shard names
		var shards []string
		for _, shard := range db.RegionToShards[config.Region] {
			shards = append(shards, rdsdeltalake.GetSparkFriendlyRdsDBName(shard, db.Sharded, true))
		}
		sort.Strings(shards)

		spec := combineShardDb{
			DbName:             db.Name,
			ShardNames:         shards,
			HasProductionTable: db.HasProductionTable(),
		}

		// Read in custom selects if necessary, and produce the final spec.
		for _, table := range db.TablesInRegion(config.Region) {
			specTable := combineShardTable{
				TableName: table.TableName,
			}
			if table.CustomCombineShardsSelect {
				path := fmt.Sprintf("python3/samsaradev/infra/dataplatform/rdsdeltalake/custom_combine_shards_selects/%s.%s.sql", db.Name, table.TableName)
				sourceFile := filepath.Join(filepathhelpers.BackendRoot, path)
				bytes, err := os.ReadFile(sourceFile)
				if err != nil {
					if os.IsNotExist(err) {
						return nil, oops.Wrapf(err, "custom select does not exist at %s", sourceFile)
					}
					return nil, oops.Wrapf(err, "")
				}

				specTable.CustomSelectQuery = string(bytes)
			}
			spec.Tables = append(spec.Tables, specTable)
		}

		dbSpecs = append(dbSpecs, spec)
	}

	sort.Slice(dbSpecs, func(i, j int) bool {
		return dbSpecs[i].DbName < dbSpecs[j].DbName
	})

	bytes, err := json.MarshalIndent(dbSpecs, "", "  ")
	if err != nil {
		return nil, oops.Wrapf(err, "json marshaling of combine shard spec")
	}

	// we need to escape any quotes
	contents := string(bytes)
	contents = strings.ReplaceAll(contents, `"`, `\"`)

	s3filespec, err := dataplatformresource.DeployedArtifactByContents(config.Region, contents, "generated_artifacts/rdsdeltalake/generated_combined_uc_shards.json")
	if err != nil {
		return nil, err
	}
	combineShardsScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/rdsdeltalake/combine_shards_all.py")
	if err != nil {
		return nil, err
	}

	// During the migration, we will have two combined shards running.  This will avoid the duplicate terraform
	// resource error for combined shards script.
	combineShardsScript.ResourceName = combineShardsScript.ResourceName + "_uc"

	driverPoolIdent := getPoolIdentifiers(config.Region, false)[infraconsts.GetDatabricksAvailabilityZones(config.Region)[0]].DriverPoolIdentifier

	cronSchedule := "0 0 */3 * * ?" // run every 3 hours.

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no ci service principal app id for region %s", config.Region)
	}

	combineShardsJob := dataplatformresource.JobSpec{
		Name:         "rds_combine_shards_uc_all",
		Region:       config.Region,
		Owner:        team.DataPlatform,
		Script:       combineShardsScript,
		SparkVersion: sparkversion.RdsCombineShardsDbrVersion,
		Parameters: []string{
			"--s3file", s3filespec.URL(),
		},
		SingleNodeJob: true,
		SparkConf: map[string]string{
			"spark.databricks.sql.initial.catalog.name":            "default",
			"databricks.loki.fileStatusCache.enabled":              "false",
			"spark.hadoop.databricks.loki.fileStatusCache.enabled": "false",
			"spark.databricks.scan.modTimeCheck.enabled":           "false",
		},
		SparkEnvVars: map[string]string{
			"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "rds-replication",
		},
		Pool:                         tf.LocalId(driverPoolIdent).ReferenceKey("id"),
		Profile:                      tf.LocalId("uc_instance_profile").Reference(),
		EmailNotifications:           []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
		TimeoutSeconds:               int((90 * time.Minute).Seconds()),
		JobOwnerServicePrincipalName: ciServicePrincipalAppId,
		Cron:                         cronSchedule,
		Tags: map[string]string{
			"pool-name": tf.LocalId(driverPoolIdent).ReferenceKey("name"),
		},
		RnDCostAllocation: 1,
		IsProduction:      true,
		JobType:           dataplatformconsts.RdsDeltaLakeIngestionCombineShards,
		SloConfig: &dataplatformconsts.JobSlo{
			SloTargetHours:            7,
			Urgency:                   dataplatformconsts.JobSloHigh,
			HighUrgencyThresholdHours: 7,
		},
		UnityCatalogSetting: dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   ciServicePrincipalAppId,
		},
		RunAs: &databricks.RunAsSetting{
			ServicePrincipalName: ciServicePrincipalAppId,
		},
	}

	jobResource, err := combineShardsJob.TfResource()
	if err != nil {
		return nil, oops.Wrapf(err, "building job resource")
	}
	permissionsResource, err := combineShardsJob.PermissionsResource()
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	resources = append(resources, s3filespec, combineShardsScript, jobResource, permissionsResource)
	return resources, nil
}
