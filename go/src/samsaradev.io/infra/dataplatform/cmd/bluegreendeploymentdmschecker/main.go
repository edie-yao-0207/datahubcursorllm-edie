package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_session "github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/databasemigrationservice"

	"github.com/samsarahq/go/oops"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/rdslakeprojects"
)

var UNKNOWN = "❓"
var SUCCESS = "✅"
var ERROR = "❌"

// This structs holds info to display per database shard.
type Result struct {
	DatabaseName   string
	Shard          string
	ReplicationID  string
	IsRunning      string
	IssuedRecovery string
	IsHealthy      string
	LastShard      bool
}

// Returns the DMS replication Task ID as setup by our RDS replication infra.
func getReplicationTaskID(shard string) string {
	return rdslakeprojects.ParquetCdcTaskName(shard)
}

// Given a region and databases names, fetches all the associated db shards.
func getShardInfo(region string, databaseName string, allDbs []rdsdeltalake.RegistryDatabase) []string {

	var allShards []string
	for _, candidate := range allDbs {

		if candidate.RegistryName != databaseName {
			continue
		}

		for _, shard := range candidate.RegionToShards[region] {
			allShards = append(allShards, shard)
		}
		break
	}

	return allShards
}

// Determines which AWS profile to use for which region.
func getAwsProfile(region string) string {
	switch region {
	case "us-west-2":
		return "dataplatformreadonly"
	case "eu-west-1":
		return "eu-dataplatformreadonly"
	case "ca-central-1":
		return "ca-dataplatformreadonly"
	default:
		panic("Unsupported region")
	}
}

// Find the DMS task, if exists returns it.  Otherwise returns nil.
func getDmsTask(ctx context.Context, replicationTaskId string, dmsClient *databasemigrationservice.DatabaseMigrationService) (*databasemigrationservice.ReplicationTask, error) {
	filter := &databasemigrationservice.Filter{
		Name:   aws.String("replication-task-id"),
		Values: []*string{aws.String(replicationTaskId)},
	}
	output, err := dmsClient.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{filter},
	})

	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundFault") {
			// DMS task doesnt exist
			// this means this databases is not replicated to the datalake.
			return nil, nil
		}
		return nil, oops.Wrapf(err, "couldn't describe task %s", replicationTaskId)
	}

	if len(output.ReplicationTasks) != 1 {
		return nil, oops.Errorf("expected 1, but got %d results back for replication task id %s", len(output.ReplicationTasks), replicationTaskId)
	}

	return output.ReplicationTasks[0], nil

}

// Converts the given string date to time.Time.
func convertStringtoTime(dateStr string) (time.Time, error) {
	layout := "2006-01-02T15:04:05" // This is the format string based on the reference time.

	// Parse the string to time.Time.
	t, err := time.Parse(layout, dateStr)
	if err != nil {
		return time.Time{}, oops.Errorf("Invalid input time, please input the time in YYYY-MM-DDTHH:MM:SS in UTC")
	}

	t = t.UTC()
	return t, nil
}

// Depending upon the region, creates a AWS session with right profile to make API calls.
func getSession(region string) *aws_session.Session {
	session, _ := aws_session.NewSessionWithOptions(aws_session.Options{
		Config: aws.Config{Region: aws.String(region),
			CredentialsChainVerboseErrors: aws.Bool(true)},
		Profile: getAwsProfile(region),
	})

	return session
}

// Generates a direct AWS console link for a given DMS task.
func consoleUrl(region string, dmsTaskId string) string {
	return fmt.Sprintf("https://%s.console.aws.amazon.com/dms/v2/home?region=%s#taskDetails/%s", region, region, dmsTaskId)
}

// Given a log-group-name, fetches latest cloudwatch events upto a given limit.
func fetchCloudWatchLogs(svc *cloudwatchlogs.CloudWatchLogs, logGroupName string, limit int64) ([]*cloudwatchlogs.OutputLogEvent, error) {

	streamsOutput, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(logGroupName),
		Descending:   aws.Bool(true),
		OrderBy:      aws.String("LastEventTime"),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error describing log streams")
	}

	if len(streamsOutput.LogStreams) == 0 {
		return nil, oops.Errorf("no log streams found for %v", logGroupName)
	}

	eventsOutput, err := svc.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: streamsOutput.LogStreams[0].LogStreamName,
		Limit:         aws.Int64(limit),
		StartFromHead: aws.Bool(false),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "error getting log events")
	}

	return eventsOutput.Events, nil
}

// Analyzes the cloudwatch log events, checks for errors or progress.
// This also handles case where task is not seeing any new data,
// but has not errored out. Returns a string emoji SUCCESS or ERROR.
func analyzeLogEventsForHealth(events []*cloudwatchlogs.OutputLogEvent) string {
	var hasErrors bool
	var appliedRecords bool
	var noRecordsReceived bool

	for _, event := range events {
		if event == nil {
			continue
		}

		message := *event.Message

		if strings.Contains(strings.ToLower(message), "error") || strings.Contains(strings.ToLower(message), "fatal") {
			hasErrors = true
			break
		} else if strings.Contains(message, "Applied record") {
			appliedRecords = true
		} else if strings.Contains(message, "No records received to load or apply on target , waiting for data from upstream") {
			noRecordsReceived = true
		}
	}

	if hasErrors {
		return ERROR
	} else if appliedRecords || noRecordsReceived {
		return SUCCESS
	}

	return UNKNOWN
}

// Checks if the DMS task is running and returns a string emoji SUCCESS or ERROR.
func isDmsRunning(task *databasemigrationservice.ReplicationTask) string {

	if task == nil {
		return UNKNOWN
	}
	status := aws.String(*task.Status)
	if *status == "running" {
		return SUCCESS
	}
	return ERROR

}

// Checks if the DMS recovery was issued in the maintenance window.  We verify if the CDC Start time
// and the DMS task start date is after the maintenance window start time.
func isDmsRecoveryDone(task *databasemigrationservice.ReplicationTask, t time.Time) (string, error) {

	if task == nil || task.ReplicationTaskStats == nil {
		return UNKNOWN, oops.Errorf("Replication Task or ReplicationTaskStats are empty")
	}

	cdcStart := aws.StringValue(task.CdcStartPosition)

	// While the buffer window is 30 mins, adding 100 seconds if someone got started bit early.
	cdcBufferWindow := 1800*time.Second + 100*time.Second
	expectedCdcStartBefore := t.Add(-cdcBufferWindow)

	// if cdc start time is not set, then we havent issued the recovery.
	if cdcStart == "" {
		return ERROR, oops.Errorf("%s", fmt.Sprintf("%v cdcstart is empty", aws.StringValue(task.ReplicationTaskIdentifier)))
	}

	cdcStartTime, err := convertStringtoTime(cdcStart)

	if err != nil {
		return UNKNOWN, err
	}

	if cdcStartTime.After(expectedCdcStartBefore) && task.ReplicationTaskStats.StartDate.After(t) {
		return SUCCESS, nil
	}

	fmt.Println(fmt.Sprintf("Taskname: %v, cdcStartTime.After:%v,  task.ReplicationTaskStats.StartDate: %v, cdcstart %v, start date: %v and T: %v, CdcStartBefore:%v", aws.StringValue(task.ReplicationTaskIdentifier), cdcStartTime.After(t), task.ReplicationTaskStats.StartDate.After(t), cdcStartTime, task.ReplicationTaskStats.StartDate, t, expectedCdcStartBefore))

	// if cdc start time is set before the window or the dms start time is before the window, then we havent issued the recovery.
	return ERROR, oops.Errorf("%s", fmt.Sprintf("Recovery Not issued for :%s", aws.StringValue(task.ReplicationTaskIdentifier)))
}

func main() {
	var databaseNames string
	var startDate string
	var region string

	flag.StringVar(&databaseNames, "database-names", "", "The database names, separated by commas. The names here are without the db suffix eg: trips2,workflows")
	flag.StringVar(&startDate, "start-date", "", "The start date of the maintenance window in UTC. (format: 2006-01-02T15:04:05Z)")
	flag.StringVar(&region, "region", "", "The region of the DMS tasks.")
	flag.Parse()

	if databaseNames == "" || startDate == "" || region == "" {
		panic("Please provide all of database-names, start-date and region.")
	}

	windowDateTime, err := convertStringtoTime(startDate)

	if err != nil {
		panic(err.Error())
	}

	session := getSession(region)
	dmsClient := databasemigrationservice.New(session)
	cloudwatchLogsClient := cloudwatchlogs.New(session)

	databaseNames = strings.ToLower(databaseNames)
	dbNames := strings.Split(databaseNames, ",")

	results := []Result{}

	allDbs := rdsdeltalake.AllDatabases()

	databasesWithoutDMS := []string{}

	for _, dbName := range dbNames {

		dbName = strings.TrimSpace(dbName)
		shards := getShardInfo(region, dbName, allDbs)

		// If no shards are replicated, then this databases is marked without DMS.
		if len(shards) == 0 {
			databasesWithoutDMS = append(databasesWithoutDMS, dbName)
		}

		for i, shard := range shards {
			replicationID := getReplicationTaskID(shard)
			dmsTask, err := getDmsTask(context.Background(), replicationID, dmsClient)

			if err != nil {
				panic("Failed to get DMS task with error: " + err.Error())
			}

			logGroup := "dms-tasks-parquet-" + shard
			logsEvents, err := fetchCloudWatchLogs(cloudwatchLogsClient, logGroup, 50)

			if err != nil {
				fmt.Println(fmt.Sprintf("Failed to Fetch logs for group %s with error: %s ", logGroup, err.Error()))
			}

			isHealthy := analyzeLogEventsForHealth(logsEvents)

			// Determine if this the last shard of a database.
			lastShard := false
			if i == len(shards)-1 {
				lastShard = true
			}

			issuedRecovery, err := isDmsRecoveryDone(dmsTask, windowDateTime)

			if err != nil {
				fmt.Println(fmt.Sprintf("Failed to Fetch recovery status for %s with error: %s ", shard, err.Error()))
			}

			results = append(results, Result{
				DatabaseName:   dbName,
				Shard:          shard,
				ReplicationID:  replicationID,
				IsRunning:      isDmsRunning(dmsTask),
				IssuedRecovery: issuedRecovery,
				IsHealthy:      isHealthy,
				LastShard:      lastShard,
			})
		}
	}

	// Now print the information on the console.

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "")

	var notRunningCount, issuedRecoveryCount, unhealthyCount, healthyCount, pendingRecoveryCount, runningCount, total int

	headers := "Shard\t|\tRunning\t|\tRecovery\t|\tHealthy\t|\tConsole Link"
	lines := "-----\t|\t-------\t|\t--------\t|\t-------\t|\t------------"

	fmt.Fprintln(writer, headers)
	fmt.Fprintln(writer, lines)

	for i, result := range results {

		fmt.Fprintf(writer, "%s\t|\t%s\t|\t%s\t|\t%s\t|\t%s\n",
			result.Shard, result.IsRunning, result.IssuedRecovery, result.IsHealthy, consoleUrl(region, result.ReplicationID))

		// if its a last shard of the current database, print the headers for the next database.
		if result.LastShard {
			fmt.Fprintln(writer, "")
			fmt.Fprintln(writer, "")

			if i != len(results)-1 {
				fmt.Fprintln(writer, headers)
				fmt.Fprintln(writer, lines)
			}

		}

		// if recovery was issued, count it.
		if result.IssuedRecovery == SUCCESS {
			issuedRecoveryCount++
		} else {
			pendingRecoveryCount++
		}

		// if the dms task is running, count it.
		if result.IsRunning == SUCCESS {
			runningCount++
		} else {
			notRunningCount++
		}

		// if the dms task is healthy, count it.
		if result.IsHealthy == SUCCESS {
			healthyCount++
		} else {
			unhealthyCount++
		}

		// count the total.
		total++

	}

	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "Summary:")
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "Metric\t|\tCount")
	fmt.Fprintln(writer, "------\t|\t-----")
	fmt.Fprintf(writer, "Not Running DMS\t|\t%d/%d\n", notRunningCount, total)
	fmt.Fprintf(writer, "DMS Shards Pending Recovery\t|\t%d/%d\n", pendingRecoveryCount, total)
	fmt.Fprintf(writer, "Unhealthy DMS\t|\t%d/%d\n", unhealthyCount, total)

	dbWithoutDMSCount := len(databasesWithoutDMS)

	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "")
	fmt.Fprintf(writer, "Databases without DMS: %d\n", dbWithoutDMSCount)
	fmt.Fprintln(writer, "")
	fmt.Fprintln(writer, "No.\t|\tDatabase Name")
	fmt.Fprintln(writer, "---\t|\t-------------")

	for i, database := range databasesWithoutDMS {
		fmt.Fprintf(writer, "%d\t|\t%s\n", i+1, database)
	}

	writer.Flush()
}
