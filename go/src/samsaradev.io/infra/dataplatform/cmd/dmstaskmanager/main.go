package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/parallelism"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
)

const (
	Stop   = "stop"
	Resume = "resume"
	List   = "list"
)

/**
 * HOW TO USE
 * Please run the script with the required arguments.
 * E.g.
 *   AWS_DEFAULT_PROFILE=dataplatformadmin go run infra/dataplatform/cmd/dmstaskmanager/main.go -region us-west-2 -dbs mdm,internal -action stop
 */
func main() {
	var region string
	var databases string
	var action string

	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2, eu-west-1, or ca-central-1)")
	flag.StringVar(&databases, "dbs", "", "comma separated list of dbs")
	flag.StringVar(&action, "action", "", "action to perform. valid values are stop, resume, or list")
	flag.Parse()

	// Parse the arguments and ensure they are correct
	if region != infraconsts.SamsaraAWSDefaultRegion && region != infraconsts.SamsaraAWSEURegion && region != infraconsts.SamsaraAWSCARegion {
		log.Fatalf("Invalid region. Please specify either us-west-2, eu-west-1, or ca-central-1.")
	}

	if action != Stop && action != Resume && action != List {
		log.Fatalf("Invalid action type. Please specify stop, resume, or list.")
	}

	dbList := strings.Split(databases, ",")
	dbs := make(map[string]struct{})
	var invalidDbs []string
	for _, db := range dbList {
		if _, ok := dbregistry.DbToTeam[db]; !ok {
			invalidDbs = append(invalidDbs, db)
		} else {
			dbs[db] = struct{}{}
		}
	}
	if len(invalidDbs) != 0 {
		log.Fatalf("Please enter db names exactly as they appear in dbregistry (i.e. mdm not mdmdb). Invalid dbs: %v\n", invalidDbs)
	}

	fmt.Printf("Script Settings:\n")
	fmt.Printf("Region %s\n", region)
	fmt.Printf("Dbs:\n")
	for db := range dbs {
		fmt.Printf("  %s\n", db)
	}
	fmt.Printf("Action %v\n", action)
	fmt.Printf("\n")

	ctx := context.Background()
	allDbs := rdsdeltalake.AllDatabases()

	// Begin DMS Admin AWS Session
	dmsAwsSession := dataplatformhelpers.GetAWSSession(region)
	dms := databasemigrationservice.New(dmsAwsSession)

	// Collect all shard names
	var allShards []string
	for _, candidate := range allDbs {
		if _, ok := dbs[candidate.RegistryName]; !ok {
			continue
		}

		for _, shard := range candidate.RegionToShards[region] {
			allShards = append(allShards, shard)
		}
	}

	// Collect a list of replication tasks. We do this up front so the user of the script
	// can easily get a picture of how the tasks look now.
	var dmsTasks []*databasemigrationservice.ReplicationTask
	for _, shard := range allShards {
		task, err := getTask(ctx, dms, shard)
		if err != nil {
			log.Fatalf("failed to get task status %v\n", err)
		}
		if task.ReplicationTaskIdentifier == nil || task.Status == nil {
			log.Fatalf("received null task id or task status for %s. %v\n", shard, task)
		}
		dmsTasks = append(dmsTasks, task)
	}
	sort.Slice(dmsTasks, func(i, j int) bool {
		return *dmsTasks[i].ReplicationTaskIdentifier < *dmsTasks[j].ReplicationTaskIdentifier
	})

	if action == List {
		fmt.Printf("=== Listing status for the dms tasks this would affect. === \n")
	} else {
		fmt.Printf("=== Going to ** %s ** dms tasks for the following shards ===\n", action)
	}
	for _, task := range dmsTasks {
		fmt.Printf("[%s]: in status %v.\n", *task.ReplicationTaskIdentifier, *task.Status)
	}
	fmt.Printf("===\n\n")

	if action == List {
		return
	}

	confirm("Are you sure that you want to proceed?")

	if action == Stop {
		fmt.Printf("\n=======Beginning to stop tasks.... ======\n")
		err := parallelism.For(ctx, dmsTasks, func(ctx context.Context, task *databasemigrationservice.ReplicationTask) error {
			return ensureTaskStopped(ctx, dms, task)
		}, parallelism.WithMaxConcurrency(10), parallelism.WithoutCancelOnError())
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		fmt.Printf("\n=======Finished Stopping Tasks.... ======\n")
	} else if action == Resume {
		fmt.Printf("\n=======Beginning to resume tasks.... ======\n")
		err := parallelism.For(ctx, dmsTasks, func(ctx context.Context, task *databasemigrationservice.ReplicationTask) error {
			return ensureTaskResumed(ctx, dms, task)
		}, parallelism.WithMaxConcurrency(10), parallelism.WithoutCancelOnError())
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		fmt.Printf("\n=======Finished Resuming Tasks.... ======\n")
	}
}

func confirm(text string) {
	var response string
	fmt.Print(text + " Confirm by typing 'yes': ")
	_, err := fmt.Scanln(&response)
	if err != nil {
		log.Fatal(err)
	}

	if strings.ToLower(response) != "yes" {
		log.Println("Failed to confirm, exiting.")
		os.Exit(0)
	}
}

func getTask(ctx context.Context, dms *databasemigrationservice.DatabaseMigrationService, rdsDbName string) (*databasemigrationservice.ReplicationTask, error) {
	// Replication tasks have a well known ID based on the individual db (shard) name.
	replicationTaskId := fmt.Sprintf("rds-%s-s3-%s", rdsDbName, rdsDbName)

	// Describe the replication task associated with this task id.
	// DescribeReplicationTasks: https://docs.aws.amazon.com/sdk-for-go/api/service/databasemigrationservice/#DatabaseMigrationService.DescribeReplicationTasks
	filter := &databasemigrationservice.Filter{
		Name:   aws.String("replication-task-id"),
		Values: []*string{aws.String(replicationTaskId)},
	}
	output, err := dms.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{filter},
	})
	if err != nil {
		return nil, oops.Wrapf(err, "couldn't describe task %s", replicationTaskId)
	}
	if len(output.ReplicationTasks) != 1 {
		return nil, oops.Errorf("expected 1, but got %d results back for replication task id %s", len(output.ReplicationTasks), replicationTaskId)
	}

	return output.ReplicationTasks[0], nil
}

// StopReplicationTask: https://docs.aws.amazon.com/sdk-for-go/api/service/databasemigrationservice/#DatabaseMigrationService.StopReplicationTask
func ensureTaskStopped(ctx context.Context, dms *databasemigrationservice.DatabaseMigrationService, task *databasemigrationservice.ReplicationTask) error {
	if task.ReplicationTaskIdentifier == nil || task.ReplicationTaskArn == nil {
		return oops.Errorf("Cannot stop task with nil task id or arn (should never happen)")
	}
	taskId := *task.ReplicationTaskIdentifier
	taskStatus := aws.StringValue(task.Status)

	// DMS will get upset if you attempt to stop an already stopped task, so we have to check the status before we
	// attempt to stop it.
	switch taskStatus {
	case "running", "starting":
		// Do nothing
	case "stopped", "ready":
		fmt.Printf("[%s] Task is in a stopped state.\n", taskId)
		return nil
	case "failed":
		fmt.Printf("[%s] WARNING: Task is failed; this is technically stopped, but should investigate.\n", taskId)
		return nil
	default:
		return oops.Errorf("Unknown replication task status: %s", aws.StringValue(task.Status))
	}

	fmt.Printf("[%s] Stopping Task (current state: %s)\n", taskId, taskStatus)
	filter := &databasemigrationservice.Filter{
		Name:   aws.String("replication-task-arn"),
		Values: []*string{task.ReplicationTaskArn},
	}
	_, err := dms.StopReplicationTaskWithContext(ctx, &databasemigrationservice.StopReplicationTaskInput{
		ReplicationTaskArn: task.ReplicationTaskArn,
	})
	if err != nil {
		return oops.Wrapf(err, "Error stopping DMS Replication Task with ID %s", taskId)
	}

	fmt.Printf("[%s] Stop task command sent successfully. Waiting until task is stopped...\n", taskId)
	err = dms.WaitUntilReplicationTaskStoppedWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{filter},
	})
	if err != nil {
		return oops.Wrapf(err, "Error wait for DMS Replication Task to Stop with ID %s", taskId)
	}

	fmt.Printf("[%s] Task sucessfully stopped!\n", taskId)
	return nil
}

func ensureTaskResumed(ctx context.Context, dms *databasemigrationservice.DatabaseMigrationService, task *databasemigrationservice.ReplicationTask) error {
	if task.ReplicationTaskIdentifier == nil || task.ReplicationTaskArn == nil {
		return oops.Errorf("Cannot stop task with nil task id or arn (should never happen)")
	}
	taskId := *task.ReplicationTaskIdentifier
	taskStatus := aws.StringValue(task.Status)

	// DMS will get upset if you attempt to resume a task thats already running, so we have to check the status
	// before we attempt to stop it.
	switch taskStatus {
	case "starting":
		fmt.Printf("[%s] WARNING: Task is already in a starting state! This may work out but you should check that this is resumed.\n", taskId)
		return nil
	case "running":
		fmt.Printf("[%s] Task is in a running state.\n", taskId)
		// Do nothing
		return nil
	case "stopped", "ready":
		// Do nothing
	case "failed":
		return oops.Errorf("[%s] WARNING: Task is failed; this is technically stopped, but should investigate.\n", taskId)
	default:
		return oops.Errorf("Unknown replication task status: %s", aws.StringValue(task.Status))
	}

	fmt.Printf("[%s] Resuming Task (current state: %s)\n", taskId, taskStatus)
	filter := &databasemigrationservice.Filter{
		Name:   aws.String("replication-task-arn"),
		Values: []*string{task.ReplicationTaskArn},
	}

	output, err := dms.StartReplicationTask(&databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn:       task.ReplicationTaskArn,
		StartReplicationTaskType: aws.String("resume-processing"),
	})

	if err != nil {
		return oops.Wrapf(err, "error resuming replication task")
	}

	fmt.Printf("[%s] Resume task command sent successfully. Waiting until task is resumed...\n", taskId)
	err = dms.WaitUntilReplicationTaskRunningWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{filter},
	})
	if err != nil {
		return oops.Wrapf(err, "Error waiting for replication task to be running with ARN %s", aws.StringValue(output.ReplicationTask.ReplicationTaskArn))
	}
	fmt.Printf("[%s] Task sucessfully resumed!\n", taskId)
	return nil
}
