package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/service/databasemigrationservice"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

type tableStatistic struct {
	replicationInstance    string
	replicationTask        string
	tableName              string
	insertions             int64
	deletions              int64
	updates                int64
	fullLoadRows           int64
	fullLoadTime           int64
	estimatedRowsToday     int64
	estimatedFullLoadToday int64
	state                  string
}

func (t tableStatistic) csv() string {
	return fmt.Sprintf("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%s", t.replicationInstance, t.replicationTask, t.tableName, t.insertions, t.deletions, t.updates, t.fullLoadRows, t.fullLoadTime, t.estimatedRowsToday, t.estimatedFullLoadToday, t.state)
}

func main() {
	// Get input flags from command line
	regionFlag := flag.String("region", "us-west-2", "The region to execute command in (us-west-2 for US or eu-west-2 for EU)")
	flag.Parse()

	var region string
	switch *regionFlag {
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
		region = *regionFlag
	default:
		log.Panicf("Invalid region %s. Please specifiy %s or %s", region, infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion)
	}

	ctx := context.Background()

	// Begin DMS Session
	dmsAwsSession := dataplatformhelpers.GetAWSSession(region)
	dms := databasemigrationservice.New(dmsAwsSession)

	// Get all replication tasks
	var tasks []*databasemigrationservice.ReplicationTask
	err := dms.DescribeReplicationTasksPagesWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{},
		func(page *databasemigrationservice.DescribeReplicationTasksOutput, lastPage bool) bool {
			tasks = append(tasks, page.ReplicationTasks...)
			return true
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	// For all replication tasks, fetch table statistics and collect them
	var stats []tableStatistic
	for _, task := range tasks {
		dms.DescribeTableStatisticsPagesWithContext(ctx, &databasemigrationservice.DescribeTableStatisticsInput{
			ReplicationTaskArn: task.ReplicationTaskArn,
		},
			func(page *databasemigrationservice.DescribeTableStatisticsOutput, lastPage bool) bool {
				for _, elem := range page.TableStatistics {
					rowsToday := *elem.FullLoadRows + *elem.Inserts - *elem.Deletes
					fullLoadTime := (*elem.FullLoadEndTime).Sub(*elem.FullLoadStartTime).Milliseconds()
					stats = append(stats, tableStatistic{
						replicationInstance:    *task.ReplicationInstanceArn,
						replicationTask:        *task.ReplicationTaskIdentifier,
						tableName:              *elem.TableName,
						insertions:             *elem.Inserts,
						deletions:              *elem.Deletes,
						updates:                *elem.Updates,
						fullLoadRows:           *elem.FullLoadRows,
						fullLoadTime:           fullLoadTime,
						estimatedRowsToday:     rowsToday,
						estimatedFullLoadToday: int64((float64(rowsToday) / float64(*elem.FullLoadRows)) * float64(fullLoadTime)),
						state:                  *elem.TableState,
					})
				}
				return true
			})
	}

	fmt.Printf("replication instance, replication task, table, insertions, deletions, updates, full load rows, full load time (ms), est rows today, est full load time today (ms), table status\n")
	for _, stat := range stats {
		fmt.Printf("%s", stat.csv()+"\n")
	}

}
