package dataplatformprojects

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const VacuumMaxWorkers = 4
const VacuumInstanceProfile = "admin-cluster"

type table struct {
	Name string
}

func DbVacuumProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	vacuumArn, err := dataplatformresource.InstanceProfileArn(providerGroup, VacuumInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, vacuumArn),
		},
	}

	var projects []*project.Project

	jobResources, err := dbVacuumJob(config)
	if err != nil {
		return nil, err
	}
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "vacuum",
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

// List of tables which cannot be vacuumed for various reasons
// which, if we do not skip, either hang the job indefinitely
// or result in a failed job. The goal is to resolve the issues
// with these tables and the job to eventually have no manual list
// of tables to skip.
func getSkipTables(config dataplatformconfig.DatabricksConfig) []string {
	skipTableList := []string{}
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		skipTableList = []string{
			// hangs indefinitely
			"dataprep_safety.cm_vg_intervals",
			"mldatasets.sites_videos",
			"mobile_logs.mobile_app_log_events",
			"mobile_logs.mobile_troy_device_info",

			// large partitions of count over 50000
			// vacuum frequently hangs indefinitely or takes an extremely
			// long time to perform on highly partitioned tables so skipping
			// them for the sake of a reliable and reasonable run duration.
			"mldatasets.sites_videos_v2",             // 220,350
			"dataprep.osdaccelerometer_event_traces", // 64,000

			// cannot be vacuumed for various miscellaneous reasons
			// TODO: investigate the reason for the failure to vacuum
			// for each table and either fix the issue or annotate
			// the reason why it cannot be fixed here.
			"connectedworker.unhealthy_device_pipelines_v1",
			"gtms.test_table",
			"netsuite_finance.acv_snapshot",
			"netsuite_finance.analytics_acv",
			"netsuite_finance.analytics_acv_base",
			"netsuite_finance.analytics_acv_base_v1",
			"netsuite_finance.analytics_acv_conform_lvl1",
			"netsuite_finance.analytics_acv_snapshot_current",
			"netsuite_finance.analytics_arr",
			"netsuite_finance.analytics_arr_exp",
			"netsuite_finance.analytics_arr_nrr_grr",
			"netsuite_finance.analytics_arr_nrr_grr_2",
			"netsuite_finance.analytics_balance_sheet",
			"netsuite_finance.analytics_customers_masterlist",
			"netsuite_finance.analytics_customers_masterlist_exp",
			"netsuite_finance.analytics_grr",
			"netsuite_finance.analytics_grr_types",
			"netsuite_finance.analytics_nrr",
			"netsuite_finance.analytics_nrr_grr",
			"netsuite_finance.analytics_nrr_grr_base",
			"netsuite_finance.analytics_nrr_grr_base_test",
			"netsuite_finance.analytics_nrr_grr_pre_base",
			"netsuite_finance.analytics_nrr_grr_pre_base_test",
			"netsuite_finance.analytics_nrr_grr_test",
			"netsuite_finance.analytics_nrr_metrics",
			"netsuite_finance.analytics_wapt",
			"netsuite_finance.arr",
			"netsuite_finance.arr_base",
			"netsuite_finance.arr_base_exp",
			"netsuite_finance.arr_base_test",
			"netsuite_finance.arr_snapshot",
			"netsuite_finance.billing_schedules_temp",
			"netsuite_finance.cal_dim",
			"netsuite_finance.cdc_change_log",
			"netsuite_finance.class_dim",
			"netsuite_finance.contract_dim",
			"netsuite_finance.currrency_dim",
			"netsuite_finance.customer_dim",
			"netsuite_finance.customer_processing",
			"netsuite_finance.customer_silver",
			"netsuite_finance.customers_bronze_test",
			"netsuite_finance.customers_silver_test_pre",
			"netsuite_finance.date_test",
			"netsuite_finance.item_dim",
			"netsuite_finance.items_dim",
			"netsuite_finance.licenses",
			"netsuite_finance.licenses_v1",
			"netsuite_finance.licenses_v2",
			"netsuite_finance.opportunity_dim",
			"netsuite_finance.product_group_acv_analytics",
			"netsuite_finance.product_group_acv_analytics_base",
			"netsuite_finance.product_group_analytics",
			"netsuite_finance.product_group_analytics_base",
			"netsuite_finance.product_group_arr_analytics",
			"netsuite_finance.product_group_arr_analytics_base",
			"netsuite_finance.product_group_dim",
			"netsuite_finance.return_subtype_dim",
			"netsuite_finance.return_type_dim",
			"netsuite_finance.rev_leakage_customers",
			"netsuite_finance.rev_rec_rule_dim",
			"netsuite_finance.revenue",
			"netsuite_finance.revenue_elements_dim",
			"netsuite_finance.sales_opportunity_dim",
			"netsuite_finance.sales_rep_dim",
			"netsuite_finance.sfdc_account_naics",
			"netsuite_finance.sfdc_opportunities",
			"netsuite_finance.sfdc_users",
			"netsuite_finance.transaction_fact",
			"netsuite_finance.transaction_line_fact",
			"netsuite_finance.upfront_avg_term_analytics",
			"netsuite_finance.vendor_dim",
			"netsuite_finance.wapt_analytics",
			"netsuite_finance.wapt_segment_analytics",
			"netsuite_finance.wapt_upfront_avg_term_analytics",
			"playground.geo_state",
			"samsara_zendesk.address",
			"samsara_zendesk.agent_attribute",
			"samsara_zendesk.alert_recipient",
			"samsara_zendesk.article",
			"samsara_zendesk.brand",
			"samsara_zendesk.call_leg",
			"samsara_zendesk.call_leg_quality_issue",
			"samsara_zendesk.call_metric",
			"samsara_zendesk.custom_role",
			"samsara_zendesk.daylight_time",
			"samsara_zendesk.domain_name",
			"samsara_zendesk.fivetran_audit",
			"samsara_zendesk.forum_topic",
			"samsara_zendesk.greeting",
			"samsara_zendesk.greeting_category",
			"samsara_zendesk.greeting_ivr",
			"samsara_zendesk.group",
			"samsara_zendesk.group_member",
			"samsara_zendesk.ivr",
			"samsara_zendesk.ivr_menu",
			"samsara_zendesk.ivr_menu_route",
			"samsara_zendesk.ivr_menu_route_option",
			"samsara_zendesk.line",
			"samsara_zendesk.line_categorised_greeting",
			"samsara_zendesk.line_greeting",
			"samsara_zendesk.line_group",
			"samsara_zendesk.organization",
			"samsara_zendesk.organization_tag",
			"samsara_zendesk.post",
			"samsara_zendesk.post_comment",
			"samsara_zendesk.satisfaction_rating",
			"samsara_zendesk.schedule",
			"samsara_zendesk.schedule_holiday",
			"samsara_zendesk.skip_ticket_history",
			"samsara_zendesk.sla_policy_history",
			"samsara_zendesk.sla_policy_metric_history",
			"samsara_zendesk.ticket",
			"samsara_zendesk.ticket_alert",
			"samsara_zendesk.ticket_attribute",
			"samsara_zendesk.ticket_comment",
			"samsara_zendesk.ticket_custom_field",
			"samsara_zendesk.ticket_field_area_causing_escalation",
			"samsara_zendesk.ticket_field_customer_issue_type",
			"samsara_zendesk.ticket_field_customer_request_type",
			"samsara_zendesk.ticket_field_dashboard_categories_ind_",
			"samsara_zendesk.ticket_field_escalation_type",
			"samsara_zendesk.ticket_field_functional_area",
			"samsara_zendesk.ticket_field_hardware_categories_ind_",
			"samsara_zendesk.ticket_field_history",
			"samsara_zendesk.ticket_field_option",
			"samsara_zendesk.ticket_form_history",
			"samsara_zendesk.ticket_link",
			"samsara_zendesk.ticket_macro_reference",
			"samsara_zendesk.ticket_schedule",
			"samsara_zendesk.ticket_sla_policy",
			"samsara_zendesk.ticket_tag",
			"samsara_zendesk.ticket_tag_history",
			"samsara_zendesk.time_zone",
			"samsara_zendesk.user",
			"samsara_zendesk.user_tag",
			"samsara_zendesk.user_vote",
		}
	}
	return skipTableList
}

// List of databases that need to be skipped for various reasons
// such as too many small tables or permission issues, etc.
func getSkipDBs(config dataplatformconfig.DatabricksConfig) []string {
	skipDBList := []string{
		// Redundant with ks/s3bigstats/datastreams vacuum job
		"kinesisstats",
		"kinesisstats_history",
		"s3bigstats",
		"s3bigstats_history",
		"datastreams",
		"datastreams_errors",
		"datastreams_history",
		"datastreams_schema",
	}
	if config.Region == infraconsts.SamsaraAWSDefaultRegion {
		additionalDBsToSkip := []string{
			"biztech_edw_dev",               // AccessDeniedException
			"biztech_edw_docebo_bronze",     // AccessDeniedException
			"biztech_edw_salesforce_bronze", // AccessDeniedException
			"biztech_edw_uat",               // AccessDeniedException
			"dojo_tmp",                      // ML team plans to separately retention this DB
			"fivetran_log",                  // AccessDeniedException
			"people_analytics",              // AccessDeniedException
			"playground",                    // Deprecated. Writes no longer allowed
			"safety_map_data",               // AccessDeniedException
			"stopsigns",                     // Too many tables
			"supplychain",                   // AccessDeniedException
		}
		skipDBList = append(skipDBList, additionalDBsToSkip...)
	} else if config.Region == infraconsts.SamsaraAWSEURegion || config.Region == infraconsts.SamsaraAWSCARegion {
		additionalDBsToSkip := []string{
			"safety_map_data", // AccessDeniedException
			"clouddb",         // For some reason it's also in the db registry, but its handled by rds vacuum jobs.
		}
		skipDBList = append(skipDBList, additionalDBsToSkip...)
	}
	return skipDBList
}

func dbVacuumJob(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	vacuumScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/db_vacuum.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, vacuumScript)

	emailAlerts := []string{team.DataPlatform.SlackAlertsChannelEmail.Email}

	sparkConf := map[string]string{
		// enables logging of vacuum in the delta table history
		"spark.databricks.delta.vacuum.logging.enabled": "true",
		// configures the spark to delete files in parallel based on number of shuffle partitions
		"spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
	}

	// Iterables for parameters/DB/Tables for the vacuum job
	databases := make(map[string]bool)
	var skipTables []table

	// Get all DB names, exclude those that need to be skipped, and add to parameters
	for _, dbName := range databaseregistries.GetAllDBNamesSorted(config, databaseregistries.LegacyOnlyDatabases) {
		databases[dbName] = true
	}

	for _, skipDB := range getSkipDBs(config) {
		databases[skipDB] = false
	}

	var dbNames []string
	for dbName, include := range databases {
		if include {
			dbNames = append(dbNames, dbName)
		}
	}
	sort.Strings(dbNames)

	// There are too many dbs to do in 1 job, let's split them up into
	// a few categories to improve performance.
	var datamodelDbs []string
	var biztechDbs []string
	var otherDbs []string

	for _, dbName := range dbNames {
		if strings.HasPrefix(dbName, "datamodel_") {
			datamodelDbs = append(datamodelDbs, dbName)
		} else if strings.HasPrefix(dbName, "biztech_") {
			biztechDbs = append(biztechDbs, dbName)
		} else {
			otherDbs = append(otherDbs, dbName)
		}
	}

	// Iterate through the list of tables to be skipped and marshall it into a
	// JSON file to be uploaded as an artifact on S3. This will then be read
	// by the vacuum python script and unmarshalled into a list that will be
	// used to skip the list of tables.
	for _, skipTbl := range getSkipTables(config) {
		skipTables = append(skipTables, table{Name: skipTbl})
	}

	skipTablesJSON, err := json.Marshal(skipTables)
	if err != nil {
		fmt.Println(err)
	}

	// we need to escape any quotes
	contents := string(skipTablesJSON)
	contents = strings.ReplaceAll(contents, `"`, `\"`)
	s3filespec, err := dataplatformresource.DeployedArtifactByContents(config.Region, contents, "generated_artifacts/skip_tables.json")
	if err != nil {
		return nil, err
	}
	resources = append(resources, s3filespec)

	type vacuumJobSpec struct {
		name string
		dbs  []string
	}

	for _, spec := range []vacuumJobSpec{
		{
			"vacuum-datamodel-dbs",
			datamodelDbs,
		},
		{
			"vacuum-biztech-dbs",
			biztechDbs,
		}, {
			"vacuum-other-dbs",
			otherDbs,
		},
	} {
		// Provide the db names and the skip table path.
		var parameters []string
		for _, dbName := range spec.dbs {
			parameters = append(parameters, "--db-name", dbName)
		}

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
		}
		ucSettings := dataplatformresource.UnityCatalogSetting{
			DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
			SingleUserName:   ciServicePrincipalAppId,
		}

		sparkConf["spark.databricks.sql.initial.catalog.name"] = "default"
		sparkConf["databricks.loki.fileStatusCache.enabled"] = "false"
		sparkConf["spark.hadoop.databricks.loki.fileStatusCache.enabled"] = "false"
		sparkConf["spark.databricks.scan.modTimeCheck.enabled"] = "false"

		parameters = append(parameters, "--skip-table-s3path", s3filespec.URL())
		job := dataplatformresource.JobSpec{
			Name:           spec.name,
			Region:         config.Region,
			Owner:          team.DataPlatform,
			Script:         vacuumScript,
			SparkVersion:   sparkversion.DbVacuumJobDbrVersion,
			Parameters:     parameters,
			Profile:        tf.LocalId("instance_profile").Reference(),
			Cron:           "0 0 12 ? * MON",
			TimeoutSeconds: 518400, // 6 days
			MaxWorkers:     VacuumMaxWorkers,
			SparkConf:      sparkConf,
			SparkEnvVars: map[string]string{
				"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "dataplatform-deployed-artifacts-read",
			},
			DriverNodeType:               "rd-fleet.4xlarge",
			WorkerNodeType:               "rd-fleet.4xlarge",
			EmailNotifications:           emailAlerts,
			JobOwnerServicePrincipalName: ciServicePrincipalAppId,
			AdminTeams:                   []components.TeamInfo{},
			Tags:                         map[string]string{},
			RnDCostAllocation:            1,
			IsProduction:                 true,
			JobType:                      dataplatformconsts.GeneralDbVacuum,
			Format:                       databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
			UnityCatalogSetting: ucSettings,
			RunAs: &databricks.RunAsSetting{
				ServicePrincipalName: ciServicePrincipalAppId,
			},
		}

		jobResource, err := job.TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job resource")
		}
		permissionsResource, err := job.PermissionsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		resources = append(resources, jobResource, permissionsResource)
	}

	return resources, nil
}
