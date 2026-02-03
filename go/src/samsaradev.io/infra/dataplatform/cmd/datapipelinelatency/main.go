package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineapi"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/models"
	"samsaradev.io/reports/sparkreportregistry"
	"samsaradev.io/system"
)

func main() {
	// Set up the environment, including clients to databricks / datapipelines (stepfunctions)
	var appConfig *config.AppConfig
	var appModels models.ReplicaAppModelsInput
	var pipelineApi *datapipelineapi.Client
	app := system.NewFx(&config.ConfigParams{}, fx.Populate(&appModels, &appConfig, &pipelineApi))
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		log.Fatalln(oops.Wrapf(err, ""))
	}
	defer app.Stop(ctx)

	dbxApi, err := dataplatformhelpers.GetDatabricksE2Client("us-west-2")
	if err != nil {
		log.Fatalln(err)
	}

	// Set up a map from each report name to the pipeline it references.
	// This is basically a copy of the PIPELINED_REPORTS variable in spark_report_registry_test.py.
	type pipelinedReport struct {
		pipelineName  string
		FinalNodeName string
	}
	pipelinedReports := make(map[string]pipelinedReport)
	pipelinedReports["ecodriving_report"] = pipelinedReport{pipelineName: "ecodriving_report"}
	pipelinedReports["activity_report_v4"] = pipelinedReport{pipelineName: "activity_report"}
	pipelinedReports["parth_test_report"] = pipelinedReport{pipelineName: "parth_test_report"}
	pipelinedReports["ifta_mileage"] = pipelinedReport{pipelineName: "ifta_report", FinalNodeName: "combined_distance"}
	pipelinedReports["material_usage_report"] = pipelinedReport{pipelineName: "material_usage_report", FinalNodeName: "aggr_spreader_events_by_hour"}
	pipelinedReports["material_usage_report_v2"] = pipelinedReport{pipelineName: "material_usage_report", FinalNodeName: "aggr_material_usage_by_hour"}
	pipelinedReports["cm_health_report_v2"] = pipelinedReport{pipelineName: "cm_health_report", FinalNodeName: "cm_health_output_v2"}
	pipelinedReports["cm_health_report_v4"] = pipelinedReport{pipelineName: "cm_health_report", FinalNodeName: "cm_health_output_v4"}
	pipelinedReports["driver_app_engagement_report"] = pipelinedReport{pipelineName: "driver_app_engagement_report", FinalNodeName: "app_opens"}

	// Get all the deployed pipelines.
	deployed, err := pipelineApi.ListDeployedPipelines(ctx, "")
	if err != nil {
		log.Fatalf("err %v\n", err)
	}

	deployedByName := make(map[string]*datapipelineapi.DeployedPipeline)
	for _, d := range deployed {
		deployedByName[d.Name] = d
	}

	type completeReport struct {
		pipeline  pipelinedReport
		report    sparkreportregistry.SparkReport
		mainJob   *databricks.Job
		sqliteJob *databricks.Job
	}

	// For every report, figure out
	// - which pipeline it depends on
	// - which staging table job / sqlite jobs it has
	var completeReports []completeReport
	fmt.Printf("Looking through reports....")
	fmt.Printf("The following reports do no have pipelines associated with them and so we won't compute statistics. If they are pipelined, please update the pipelinedReports map in this script.\n")
	for _, report := range sparkreportregistry.AllReportsInRegion(endpoints.UsWest2RegionID) {
		pr, ok := pipelinedReports[report.Name]
		if !ok {
			fmt.Printf("%s\n", report.Name)
			continue
		}

		cr := completeReport{report: report, pipeline: pr}
		mainJob := "report-" + report.TeamName + "-" + report.Name + "-every-3hr-us"
		sqliteJob := "report-" + report.TeamName + "-sqlite-" + report.Name + "-every-3hr-us"
		output, err := dbxApi.ListJobs(ctx, &databricks.ListJobsInput{
			Name: mainJob,
		})
		if err != nil {
			log.Fatalf("fml")
		}
		if output.Jobs != nil && len(output.Jobs) == 1 {
			j := output.Jobs[0]
			cr.mainJob = &j
		}

		if report.SQLiteOnly {
			output, err := dbxApi.ListJobs(ctx, &databricks.ListJobsInput{
				Name: sqliteJob,
			})
			if err != nil {
				log.Fatalf("fml")
			}
			if output.Jobs != nil && len(output.Jobs) == 1 {
				j := output.Jobs[0]
				cr.sqliteJob = &j
			}
		}
		completeReports = append(completeReports, cr)
	}
	fmt.Printf("Done looking through reports!\n\n")

	// All RDS and KS merges start and finish within the same hour. So we'll use pick one (location) as a reference.
	var ingestionJob *databricks.Job
	output, err := dbxApi.ListJobs(ctx, &databricks.ListJobsInput{
		Name: "kinesisstats-merge-location-every-3hr-us",
	})
	if err != nil || output.Jobs == nil || len(output.Jobs) == 0 {
		log.Fatalf("couldnt find a ks merge job check the code")
	}
	j := output.Jobs[0]
	ingestionJob = &j

	outputRuns, err := dbxApi.ListRuns(ctx, &databricks.ListRunsInput{
		JobId:         ingestionJob.JobId,
		CompletedOnly: true,
		Offset:        0,
		Limit:         150,
	})
	if err != nil {
		log.Fatalf("couldn't fetch runs for ks location merge %v\n", err)
	}
	ingestionRuns := outputRuns.Runs

	// Now, for every report, we're going to figure out the end to end latency.
	// In this loop specifically, we're going to try to compute it for a run X runs ago and then a very recent one,
	// to help us see the latency improvements (if any).
	for _, report := range completeReports {

		// 1. Get all the pipeline executions. we are going to fetch 100 because theres 8 a day,
		// so the first one is definitely before we migrated.
		pipelineName := report.pipeline.pipelineName + "-" + report.pipeline.FinalNodeName
		if report.pipeline.FinalNodeName == "" {
			pipelineName += "report"
		}
		deployedPipeline, ok := deployedByName[pipelineName]
		if !ok {
			fmt.Printf("Could not find deployed pipeline %s\n", pipelineName)
			continue
		}

		// Get only completed data pipeline executions, since those are the only ones we care about.
		rawExecutions, err := pipelineApi.ListNodeExecutions(ctx, deployedPipeline.StateMachineArn, 100)
		var executions []*datapipelineapi.NodeExecution
		for _, e := range rawExecutions {
			if e.EndTime == nil {
				continue
			}
			executions = append(executions, e)
		}
		if err != nil {
			fmt.Printf("Error getting executions %v\n", err)
		}

		// 2. Get the job executions for the main job, if it exists.
		// Job runs are returned in descending order.
		var jobRuns []*databricks.Run
		if report.mainJob != nil {
			output, err := dbxApi.ListRuns(ctx, &databricks.ListRunsInput{
				JobId:         report.mainJob.JobId,
				CompletedOnly: true,
				Offset:        0,
				Limit:         150,
			})
			if err != nil {
				fmt.Printf("Error getting runs for main jobs %v\n", err)
				continue
			}
			jobRuns = output.Runs
		}

		// 3. Get the job executions for the sqlite job, if it exists.
		var sqliteJobRuns []*databricks.Run
		if report.sqliteJob != nil {
			output, err := dbxApi.ListRuns(ctx, &databricks.ListRunsInput{
				JobId:         report.sqliteJob.JobId,
				CompletedOnly: true,
				Offset:        0,
				Limit:         150,
			})
			if err != nil {
				fmt.Printf("Error getting runs for sqlite jobs %v\n", err)
				continue
			}
			sqliteJobRuns = output.Runs
		}
		// Now, pick a couple of executions of the pipeline; one recent and one historical
		// So we can compare.
		// We don't use the absolute most recent execution since its report jobs may not have finished,
		// which is why we do executions[3].
		fmt.Printf(">> Recent Job Runs for %s\n", report.report.Name)
		printEndToEndLatency(executions[3], ingestionRuns, jobRuns, sqliteJobRuns)
		fmt.Printf("-------------------------------------------------\n")
		fmt.Printf(">> Historical Job Run for %s\n", report.report.Name)
		printEndToEndLatency(executions[len(executions)-1], ingestionRuns, jobRuns, sqliteJobRuns)
		fmt.Printf("\n\n")
	}
}

func printEndToEndLatency(execution *datapipelineapi.NodeExecution, ingestionRuns []*databricks.Run, mainJobRuns []*databricks.Run, sqliteJobRuns []*databricks.Run) {
	// Given a pipeline execution and all the available ingestion runs, main job runs, and sqlite runs
	// figure out which ingestion job came before this pipeline run and which report runs came after,
	// giving us a full picture of the pipeline's execution.
	var ingestionRun *databricks.Run
	var mainJobRun *databricks.Run
	var sqliteJobRun *databricks.Run

	// Find the *latest* ingestion merge that happens before the pipeline run.
	for i := 0; i < len(ingestionRuns); i++ {
		jobRun := ingestionRuns[i]
		if jobRun.EndTime < samtime.TimeToMs(*execution.StartTime) {
			ingestionRun = jobRun
			break
		}
	}

	// Find the *earliest* job run after the pipeline.
	for i := len(mainJobRuns) - 1; i >= 0; i-- {
		jobRun := mainJobRuns[i]
		if jobRun.StartTime > samtime.TimeToMs(*execution.EndTime) {
			mainJobRun = jobRun
			break
		}
	}

	// Find the *earliest* sqlite job after the main job.
	if mainJobRun != nil {
		for i := len(sqliteJobRuns) - 1; i >= 0; i-- {
			jobRun := sqliteJobRuns[i]
			if jobRun.StartTime > mainJobRun.EndTime {
				sqliteJobRun = jobRun
				break
			}
		}
	}

	ingestionStart := samtime.TruncateHour(ingestionRun.StartTime)
	dateFormat := "2006-01-02 15:04:05"

	fmt.Printf("[KS/RDS Merges] %s\n", samtime.MsToTime(ingestionStart).Format(dateFormat))
	fmt.Printf("[Pipeline] %s to %s\n", execution.StartTime.Format(dateFormat), execution.EndTime.Format(dateFormat))
	totalLatency := samtime.TimeToMs(*execution.EndTime) - ingestionStart
	if mainJobRun != nil {
		fmt.Printf("[Aggregation Job] %s to %s\n", samtime.MsToTime(mainJobRun.StartTime).Format(dateFormat), samtime.MsToTime(mainJobRun.EndTime).Format(dateFormat))
		totalLatency += mainJobRun.EndTime - samtime.TimeToMs(*execution.EndTime)
		if sqliteJobRun != nil {
			fmt.Printf("[Sqlite Job] %s and ended at %s\n", samtime.MsToTime(sqliteJobRun.StartTime).Format(dateFormat), samtime.MsToTime(sqliteJobRun.StartTime+int64(sqliteJobRun.RunDuration)).Format(dateFormat))
			totalLatency += sqliteJobRun.EndTime - mainJobRun.EndTime
		}
	}
	fmt.Printf("== TOTAL E2E Latency: %s\n", samtime.MsToFormattedDuration(totalLatency))
}
