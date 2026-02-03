package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/slicehelpers"
)

var validRegions = slicehelpers.Map(infraconsts.SamsaraProdCloudsAsSlice, func(cloud infraconsts.SamsaraCloud) string {
	return cloud.AWSRegion
})

func main() {
	var rdsDbName string
	var region string
	var tablesString string
	flag.StringVar(&rdsDbName, "rdsdb", "", "<name> or <name>-shard-<num>, like dispatch or dispatch-shard-1")
	flag.StringVar(&region, "region", "", strings.Join(validRegions, " or "))
	flag.StringVar(&tablesString, "tables", "", "comma-separated list of tables to delete rules for")
	flag.Parse()

	if rdsDbName == "" {
		log.Fatalln("-rdsdb not set")
	}

	if tablesString == "" {
		log.Fatalln("-tables not set")
	}
	tablesToDelete := make(map[string]struct{})
	for _, table := range strings.Split(tablesString, ",") {
		tablesToDelete[table] = struct{}{}
	}

	if !slices.Contains(validRegions, region) {
		log.Fatalf("invalid region: %q", region)
	}

	replicationTaskId := fmt.Sprintf("rds-prod-%sdb-s3-prod-%sdb", rdsDbName, rdsDbName)
	replicationTaskIdentifierFilter := &databasemigrationservice.Filter{
		Name:   aws.String("replication-task-id"),
		Values: []*string{aws.String(replicationTaskId)},
	}

	ctx := context.Background()
	dms := databasemigrationservice.New(dataplatformhelpers.GetAWSSession(region))

	log.Println("describing...")
	describeOutput, err := dms.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{replicationTaskIdentifierFilter},
	})
	if err != nil {
		log.Fatalf("failed to describe: %s", oops.Wrapf(err, ""))
	}
	replicationTask := describeOutput.ReplicationTasks[0]

	var tableMappings awsresource.DMSTableMapping
	if err := json.Unmarshal([]byte(aws.StringValue(replicationTask.TableMappings)), &tableMappings); err != nil {
		log.Fatalln(oops.Wrapf(err, ""))
	}

	var newTableMapping awsresource.DMSTableMapping
	foundRules := false
	for _, rule := range tableMappings.Rules {
		if rule.ObjectLocator == nil {
			log.Fatalln("no ObjectLocator")
		}

		ruleTable := aws.StringValue(rule.ObjectLocator.TableName)
		if _, ok := tablesToDelete[ruleTable]; ok {
			foundRules = true
		} else {
			newTableMapping.Rules = append(newTableMapping.Rules, rule)
		}
	}
	if !foundRules {
		fmt.Println(aws.StringValue(replicationTask.TableMappings))
		log.Fatalf("tables %v not found!", strings.Split(tablesString, ","))
	}

	newTableMappingJson, err := json.Marshal(newTableMapping)
	if err != nil {
		log.Fatalln(oops.Wrapf(err, ""))
	}

	switch strings.ToLower(aws.StringValue(replicationTask.Status)) {
	case "running", "starting":
		log.Println("stopping...")
		stopOutput, err := dms.StopReplicationTaskWithContext(ctx, &databasemigrationservice.StopReplicationTaskInput{
			ReplicationTaskArn: replicationTask.ReplicationTaskArn,
		})
		if err != nil {
			log.Fatalf("stop task: %s", oops.Wrapf(err, ""))
		}
		replicationTask = stopOutput.ReplicationTask
	case "stopped", "ready", "failed", "stopping":
	default:
		log.Fatalf("bad replication task status: %s", aws.StringValue(replicationTask.Status))
	}

	for aws.StringValue(replicationTask.Status) == "stopping" {
		output, err := dms.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
			Filters: []*databasemigrationservice.Filter{replicationTaskIdentifierFilter},
		})
		if err != nil {
			log.Fatalln(oops.Wrapf(err, ""))
		}
		replicationTask = output.ReplicationTasks[0]
		time.Sleep(15 * time.Second)
	}

	log.Println("modifying...")
	modifyOutput, err := dms.ModifyReplicationTaskWithContext(ctx, &databasemigrationservice.ModifyReplicationTaskInput{
		ReplicationTaskArn: replicationTask.ReplicationTaskArn,
		TableMappings:      aws.String(string(newTableMappingJson)),
	})
	if err != nil {
		log.Fatalln(oops.Wrapf(err, ""))
	}

	replicationTask = modifyOutput.ReplicationTask
	for aws.StringValue(replicationTask.Status) == "modifying" {
		output, err := dms.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
			Filters: []*databasemigrationservice.Filter{replicationTaskIdentifierFilter},
		})
		if err != nil {
			log.Fatalln(oops.Wrapf(err, "modify task"))
		}
		replicationTask = output.ReplicationTasks[0]
		time.Sleep(15 * time.Second)
	}

	log.Println("starting...")
	startOutput, err := dms.StartReplicationTaskWithContext(ctx, &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn:       replicationTask.ReplicationTaskArn,
		StartReplicationTaskType: aws.String(databasemigrationservice.StartReplicationTaskTypeValueResumeProcessing),
	})
	if err != nil {
		log.Fatalf("start task: %s", oops.Wrapf(err, ""))
	}
	replicationTask = startOutput.ReplicationTask
	for aws.StringValue(replicationTask.Status) == "starting" {
		output, err := dms.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
			Filters: []*databasemigrationservice.Filter{replicationTaskIdentifierFilter},
		})
		if err != nil {
			log.Fatalln(oops.Wrapf(err, "start task"))
		}
		replicationTask = output.ReplicationTasks[0]
		time.Sleep(15 * time.Second)
	}

	if aws.StringValue(replicationTask.Status) != "running" {
		log.Fatalf("status is %s, not running", aws.StringValue(replicationTask.Status))
	}

	log.Println("done!")
}
