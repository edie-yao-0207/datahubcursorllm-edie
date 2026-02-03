package dataplatformprojects

import (
	"fmt"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/deploy/helpers"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/reports/sparkreportregistry"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

const ReportAggregatorMaxWorkers = 16
const ReportAggregationInstanceProfile = "report-aggregation-cluster"

func ReportAggregatorProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	reportArn, err := dataplatformresource.InstanceProfileArn(providerGroup, ReportAggregationInstanceProfile)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	instanceProfileResources := []tf.Resource{
		&genericresource.StringLocal{
			Name:  "instance_profile",
			Value: fmt.Sprintf(`"%s"`, reportArn),
		},
	}

	var projects []*project.Project

	jobResources, err := reportAggregatorJobs(config)
	if err != nil {
		return nil, err
	}
	projects = append(projects, &project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "job",
		Name:     "reports",
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

func reportAggregatorJobs(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	var resources []tf.Resource

	uncellifiedScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/reports/report_aggregator.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, uncellifiedScript)

	cellifiedScript, err := dataplatformresource.DeployedArtifactObject(config.Region, "python3/samsaradev/infra/dataplatform/reports/cellified_report_aggregator.py")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}
	resources = append(resources, cellifiedScript)

	for _, report := range sparkreportregistry.AllReportsInRegion(config.Region) {
		const uncellifiedCell = "magic-string-all-cells"

		var cells []string
		if report.Cellified {
			// We don't use infraconsts.GetAllCellsForRegion() because it returns
			// cluster names including ks*, not just cells.
			switch config.Region {
			case infraconsts.SamsaraAWSDefaultRegion:
				cells = infraconsts.USProdCells
			case infraconsts.SamsaraAWSEURegion:
				cells = infraconsts.EUProdCells
			case infraconsts.SamsaraAWSCARegion:
				cells = infraconsts.CAProdCells
			default:
				return nil, oops.Errorf("no region")
			}
		} else {
			// HACK: uncellifiedCell is a special identifier for uncellified jobs.
			cells = []string{uncellifiedCell}
		}

		queryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/report.sql", report.Name)
		query, err := dataplatformresource.DeployedArtifactObject(config.Region, queryPath)
		if err != nil {
			return nil, oops.Wrapf(err, "query script object")
		}
		resources = append(resources, query)

		var rollup *awsresource.S3BucketObject
		if report.CustomRollupFile {
			rollupPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/rollup.sql", report.Name)
			rollup, err = dataplatformresource.DeployedArtifactObject(config.Region, rollupPath)
			if err != nil {
				return nil, oops.Wrapf(err, "query script object")
			}
			resources = append(resources, rollup)
		}

		var eventIntervalsQuery, detailedReportQuery *awsresource.S3BucketObject
		if report.HasDetailedReport {
			if report.HasEventIntervals {
				eventIntervalsQueryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/event_intervals.sql", report.Name)
				eventIntervalsQuery, err = dataplatformresource.DeployedArtifactObject(config.Region, eventIntervalsQueryPath)
				if err != nil {
					return nil, oops.Wrapf(err, "detailed query script object")
				}
				resources = append(resources, eventIntervalsQuery)
			}

			detailedReportQueryPath := fmt.Sprintf("python3/samsaradev/infra/dataplatform/reports/%s/detailed_report.sql", report.Name)
			detailedReportQuery, err = dataplatformresource.DeployedArtifactObject(config.Region, detailedReportQueryPath)
			if err != nil {
				return nil, oops.Wrapf(err, "detailed rollup query script object")
			}
			resources = append(resources, detailedReportQuery)
		}

		for _, cell := range cells {
			for _, spec := range report.Jobs {
				var aggColumns []string
				for _, col := range report.SelectColumns {
					aggColumns = append(aggColumns, strings.ToLower(col.String()))
				}

				startDate := spec.StartDate
				endDate := spec.EndDate

				if endDate == "" {
					endDate = `DATE_ADD(CURRENT_DATE(), 1)`
				}

				entityKeyStrings := make([]string, 0, len(report.EntityKeys))
				for _, entityKey := range report.EntityKeys {
					entityKeyStrings = append(entityKeyStrings, string(entityKey))
				}

				s3Prefix := helpers.GetS3PrefixByRegion(config.Region)

				jobOutputLocation := spec.Name
				if spec.QueueNameOverride != "" {
					jobOutputLocation = spec.QueueNameOverride
				}

				stagingTableLocation := fmt.Sprintf("s3://%sreport-staging-tables/report_aggregator/%s/", s3Prefix, report.Name)
				if cell != uncellifiedCell {
					stagingTableLocation = fmt.Sprintf("s3://%sreport-staging-tables/report_aggregator/%s_%s/", s3Prefix, report.Name, cell)
				}

				tempTableLocation := fmt.Sprintf("s3://%sreport-staging-tables/temp/%s/%s/", s3Prefix, report.Name, spec.Name)
				if cell != uncellifiedCell {
					tempTableLocation += cell + "/"
				}

				parameters := []string{
					"--report-name", report.Name,
					"--s3sqlfile", query.URL(),
					"--start-date", startDate,
					"--end-date", endDate,
					"--temp-table-location", tempTableLocation,
					"--staging-table-location", stagingTableLocation,
					"--sink-path", fmt.Sprintf("s3://%spartial-report-aggregation/csv/%s/job=%s/", s3Prefix, report.Name, jobOutputLocation),
					"--entity-keys", strings.Join(entityKeyStrings, ","),
					"--aggregation-columns", strings.Join(aggColumns, ","),
				}
				if !report.EnableSlidingWindows {
					parameters = append(parameters, "--tumbling-windows", "true")
				}
				if report.HasTumblingWindowsSunday {
					parameters = append(parameters, "--tumbling-windows-sunday", "true")
				}

				if report.GeoSparkRequired {
					parameters = append(parameters, "--geospark", "True")
				}
				if report.CustomRollupFile {
					parameters = append(parameters,
						"--custom-rollup-sql-file", rollup.URL(),
					)
				}
				if !report.DontSoftDeleteFromStagingTable {
					parameters = append(parameters, "--soft-delete-staging-table", "true")
				}
				if report.SoftDeleteFromMySQLTable {
					parameters = append(parameters, "--soft-delete-rollup", "true")
				}

				if len(report.OutputSortKeys) != 0 {
					parameters = append(parameters, "--output-sort-keys", strings.Join(report.OutputSortKeys, ","))
				}

				if report.SQLiteOnly {
					parameters = append(parameters, "--sqlite-only", "true")
				}

				jobtype := dataplatformconsts.ReportAggregator

				var sqlite_parameters []string

				if spec.SQLiteWriter {
					jobtype = dataplatformconsts.ReportSqliteAggregator

					sqlite_parameters = append(sqlite_parameters,
						"--write-sqlite-output", "true",
						"--sqlite-output-bucket", fmt.Sprintf("%spartial-report-aggregation", s3Prefix),
						"--sqlite-output-prefix", fmt.Sprintf("sqlite/%s/", report.Name),
						"--sqlite-table-name", report.Name,
						"--drop-empty", "true",
					)

					if report.HasDetailedReport {
						sqlite_parameters = append(sqlite_parameters,
							"--sqlite-detailed-output-prefix", fmt.Sprintf("sqlite/%s/", report.DetailedTableName),
							"--sqlite-detailed-table-name", report.DetailedTableName,
							"--detailed-time-column-name", report.DetailedTimeColumnName,
							"--detailed-primary-keys", strings.Join(report.DetailedPrimaryKeys, ","),
						)
					}
					parameters = append(parameters, "--aggregate-and-sqlite", "true")
					parameters = append(parameters, sqlite_parameters...)
				}

				detailedTempTableLocation := fmt.Sprintf("s3://%sreport-staging-tables/temp/%s/%s/", s3Prefix, report.DetailedTableName, spec.Name)
				if cell != uncellifiedCell {
					detailedTempTableLocation += cell + "/"
				}

				if report.HasDetailedReport {
					detailedParameters := []string{
						"--has-detailed-report", "true",
						"--s3-detailed-report-sql-file", detailedReportQuery.URL(),
						"--detailed-temp-table-location", detailedTempTableLocation,
						"--detailed-sink-path", fmt.Sprintf("s3://%spartial-report-aggregation/csv/%s/job=%s/", s3Prefix, report.DetailedTableName, jobOutputLocation),
					}

					if len(report.DetailedOutputSortKeys) != 0 {
						detailedParameters = append(detailedParameters,
							"--detailed-output-sort-keys", strings.Join(report.DetailedOutputSortKeys, ","),
						)
					}

					eventStagingTableLocation := fmt.Sprintf("s3://%sreport-staging-tables/report_aggregator/event_intervals_%s/", s3Prefix, report.Name)
					if cell != uncellifiedCell {
						eventStagingTableLocation += cell + "/"
					}

					if report.HasEventIntervals {
						detailedParameters = append(detailedParameters,
							"--has-event-intervals", "true",
							"--s3-event-intervals-sql-file", eventIntervalsQuery.URL(),
							"--event-staging-table-location", eventStagingTableLocation,
							"--detailed-event-keys", strings.Join(report.DetailedEventKeys, ","),
						)
					}

					parameters = append(parameters, detailedParameters...)
				}

				if report.OnlyComputeDetailedReport {
					parameters = append(parameters, "--only-compute-detailed-report", "true")
				}

				maxWorkers := spec.MaxWorkers
				if maxWorkers == 0 {
					maxWorkers = ReportAggregatorMaxWorkers
				}

				sparkConf := map[string]string{
					// Optimize range join on millisecond timestamp in BIGINT.
					// This potentially makes range join on TIMESTAMP type less performant.
					// https://docs.databricks.com/delta/join-performance/range-join.html
					"spark.databricks.optimizer.rangeJoin.binSize": "3600000",
					// The adas report is running into error `Size of broadcasted table far exceeds
					// estimates and exceeds limit of spark.driver.maxResultSize`
					// Setting the maxResultSize to 0 to remove this limit. This is safe to add
					// since reports are not running in a multi tenant envrionment
					"spark.driver.maxResultSize": "0",
				}

				for i, conf := range report.SparkConf {
					sparkConf[i] = conf
				}

				emailAlertNotifications := append([]string{team.DataPlatform.SlackAlertsChannelEmail.Email}, report.AlertEmails...)

				sparkVersion := sparkversion.SparkReportsDbrVersion

				var jars []dataplatformresource.JarName
				if report.GeoSparkRequired {
					jars = dataplatformresource.GeoSparkJars(sparkVersion)
				}

				var script *awsresource.S3BucketObject
				if cell == uncellifiedCell {
					script = uncellifiedScript
				} else {
					script = cellifiedScript
				}

				if cell != uncellifiedCell {
					parameters = append(parameters, "--cell-id", cell)
				}

				name := fmt.Sprintf("report-%s-%s-%s", report.TeamName, report.Name, spec.Name)
				if cell != uncellifiedCell {
					name += "-" + cell
				}

				driverNodeType := "rd-fleet.xlarge"
				if spec.DriverNodeType != "" {
					driverNodeType = spec.DriverNodeType
				}

				schedule := "unknown"
				switch spec.Name {
				case "every-3hr":
					schedule = "3hr"
				case "daily":
					schedule = "daily"
				case "backfill":
					schedule = "backfill"
				}

				owningTeam, ok := team.TeamByName[report.TeamName]
				if !ok {
					return nil, oops.Errorf("could not find team with name: %s", report.TeamName)
				}

				var minRetryIntervalSeconds int
				// We limit the number or retries to 1 as a cost guard rail. Manual intervention
				// is likely required if the job is failing continuously.
				if spec.MaxRetries > 1 {
					return nil, oops.Errorf("MaxRetries for job cannot exceed 1: [%s]", name)
				} else if spec.MaxRetries > 0 {
					minRetryIntervalSeconds = 300 // wait 5 minutes before any retries
				}

				disableFileModificationCheck := false
				ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
				if err != nil {
					return nil, oops.Wrapf(err, "no ci service principal app id for region %s", config.Region)
				}
				ucSetting := dataplatformresource.UnityCatalogSetting{
					DataSecurityMode: databricksresource.DataSecurityModeSingleUser,
					SingleUserName:   ciServicePrincipalAppId,
				}
				disableFileModificationCheck = true

				job := dataplatformresource.JobSpec{
					Name:         name,
					Region:       config.Region,
					Owner:        owningTeam,
					Script:       script,
					SparkVersion: sparkVersion,
					Parameters:   parameters,
					Profile:      tf.LocalId("instance_profile").Reference(),
					MaxWorkers:   maxWorkers,
					SparkConf:    sparkConf,
					SparkEnvVars: map[string]string{
						"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME": "report-aggregator-s3",
					},
					DriverNodeType:               driverNodeType,
					WorkerNodeType:               "rd-fleet.2xlarge",
					EmailNotifications:           emailAlertNotifications,
					JobOwnerServicePrincipalName: ciServicePrincipalAppId,
					AdminTeams:                   []components.TeamInfo{team.PlatformReports},
					Tags: map[string]string{
						"job:database": "reports",
						"job:table":    report.Name,
						"job:schedule": schedule,
					},
					RnDCostAllocation: 0,
					IsProduction:      true,
					JobType:           jobtype,
					MaxRetries:        spec.MaxRetries,
					MinRetryInterval:  time.Duration(minRetryIntervalSeconds) * time.Second,
					Format:            databricks.MultiTaskKey,
					JobTags: map[string]string{
						"format": databricks.MultiTaskKey,
					},
					UnityCatalogSetting:          ucSetting,
					DisableFileModificationCheck: disableFileModificationCheck,
					RunAs: &databricks.RunAsSetting{
						ServicePrincipalName: ciServicePrincipalAppId,
					},
					UseOnDemandCluster: spec.UseOnDemandCluster,
				}

				libraries := dataplatformresource.JobLibraryConfig{
					Jars: jars,
				}
				if spec.SQLiteWriter {
					libraries.PyPIs = []dataplatformresource.PyPIName{
						dataplatformresource.SparkPyPISnappy,
					}
				}
				job.Libraries = libraries

				// If the report is set to queue, we will queue the job instead of skipping it due to a job already in flight.
				if report.Queue {
					job.Queue = &databricks.JobQueue{
						Enabled: true,
					}
				}

				if !report.Disable {
					job.Cron = spec.Cron
				}

				jobResource, err := job.TfResource()
				if err != nil {
					return nil, oops.Wrapf(err, "building job resource")
				}
				permissionsResource, err := job.PermissionsResource()
				if err != nil {
					return nil, oops.Wrapf(err, "")
				}

				// TODO: This is a temporary fix to provide the team owning the speeding_report_v3
				// https://samsara.atlassian-us-gov-mod.net/browse/DAT-434 to remove it.
				// Provide team owning the speeding_report_v3 report access to manage the job.
				if report.Name == "speeding_report_v3" {
					// Update the permissions resource to provide the team owning the report
					// access to manage the job. We can only have one permission level per user/group.
					// So we need to iterate over the existing access controls and update the
					// permission level for the team owning the report.
					for _, ac := range permissionsResource.AccessControls {
						if ac.GroupName == owningTeam.DatabricksAccountGroupName() {
							ac.PermissionLevel = databricks.PermissionLevelCanManage
						}
					}
				}

				resources = append(resources, jobResource, permissionsResource)
			}
		}
	}

	return resources, nil
}
