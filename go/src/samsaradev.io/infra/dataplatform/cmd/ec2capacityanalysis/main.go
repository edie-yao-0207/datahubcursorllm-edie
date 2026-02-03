package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/errgroup"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
)

type LockedWriter struct {
	m      sync.Mutex
	Writer io.Writer
}

func (lw *LockedWriter) Write(b []byte) (n int, err error) {
	lw.m.Lock()
	defer lw.m.Unlock()
	return lw.Writer.Write(b)
}

type RunDetail struct {
	JobName                string                             `json:"job_name"`
	RunId                  int64                              `json:"run_id"`
	JobId                  int64                              `json:"job_id"`
	JobRunId               int64                              `json:"job_run_id"`
	StartTime              int64                              `json:"start_time"`
	RunPageUrl             string                             `json:"run_page_url"`
	LifeCycleState         databricks.RunLifeCycleState       `json:"life_cycle_state"`
	ResultState            databricks.RunResultState          `json:"result_state"`
	OutputError            string                             `json:"output_error"`
	ClassifiedError        datapipelineerrors.ErrorDefinition `json:"classified_error"`
	DriverInstanceType     string                             `json:"driver_instance_type"`
	DriverOnSpot           bool                               `json:"driver_on_spot"`
	DriverAvailabilityZone string                             `json:"driver_availability_zone"`
	WorkerInstanceType     string                             `json:"worker_instance_type"`
	WorkerOnSpot           bool                               `json:"worker_on_spot"`
	WorkerAvailabilityZone string                             `json:"worker_availability_zone"`
	RunDetail              *databricks.Run                    `json:"run_detail"`
	DriverPool             databricks.InstancePoolSpec        `json:"driver_pool"`
	WorkerPool             databricks.InstancePoolSpec        `json:"worker_pool"`
}

func analyze(w io.Writer) error {
	client, err := dataplatformhelpers.GetDatabricksE2Client("us-west-2")
	if err != nil {
		return oops.Wrapf(err, "")
	}

	ctx := context.Background()

	instancePoolsOutput, err := client.ListInstancePools(ctx)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	instancePools := make(map[string]databricks.InstancePoolAndStats)
	for _, pool := range instancePoolsOutput.InstancePools {
		instancePools[pool.InstancePoolId] = pool
	}

	jobsOutput, err := client.ListJobs(ctx, &databricks.ListJobsInput{})
	if err != nil {
		return oops.Wrapf(err, "")
	}

	semaphore := make(chan struct{}, 10)
	lockedWriter := &LockedWriter{Writer: w}
	eg, ctx := errgroup.WithContext(ctx)
	for i := range jobsOutput.Jobs {
		job := jobsOutput.Jobs[i]
		jobName := job.JobSettings.Name
		if !strings.HasPrefix(jobName, "kinesisstats-merge-") && !strings.HasPrefix(jobName, "rds-merge-") {
			continue
		}

		eg.Go(func() error {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore
			}()

			runsOutput, err := client.ListRuns(ctx, &databricks.ListRunsInput{
				JobId:         job.JobId,
				CompletedOnly: true,
				ExpandTasks:   true,
				Limit:         100,
				Offset:        0,
			})
			if err != nil {
				return oops.Wrapf(err, "")
			}

			for _, run := range runsOutput.Runs {
				var driverPoolId string
				var workerPoolId string

				if run.JobClusters != nil && len(run.JobClusters) > 0 {
					driverPoolId = run.JobClusters[0].NewCluster.DriverInstancePoolId
					workerPoolId = run.JobClusters[0].NewCluster.InstancePoolId
				}
				if run.Tasks[0].NewCluster != nil && run.Tasks[0].JobClusterKey == "" {
					driverPoolId = run.Tasks[0].NewCluster.DriverInstancePoolId
					workerPoolId = run.Tasks[0].NewCluster.InstancePoolId
				}
				workerPool, ok := instancePools[workerPoolId]
				if !ok {
					continue
				}

				workerInstanceType := workerPool.NodeTypeId

				workerAvailabilityZone := workerPool.AwsAttributes.ZoneId
				workerOnSpot := workerPool.AwsAttributes.Availability == databricks.AwsAvailabilitySpot

				driverPool, ok := instancePools[driverPoolId]
				if !ok {
					continue
				}

				driverInstanceType := driverPool.NodeTypeId

				driverAvailabilityZone := driverPool.AwsAttributes.ZoneId
				driverOnSpot := driverPool.AwsAttributes.Availability == databricks.AwsAvailabilitySpot

				var outputError string
				if run.State.LifeCycleState == databricks.RunLifeCycleStateTerminated && run.State.ResultState != nil && *run.State.ResultState == databricks.RunResultStateSuccess {
					// No need to get output for successful runs.
				} else {
					runOutput, err := client.GetRunOutput(ctx, &databricks.GetRunOutputInput{
						RunId: int64(run.Tasks[0].RunId),
					})
					if err != nil {
						return oops.Wrapf(err, "")
					}
					outputError = runOutput.Error
				}

				var resultState databricks.RunResultState
				if run.State.ResultState != nil {
					resultState = *run.State.ResultState
				}

				classifiedError := datapipelineerrors.GetErrorClassification(outputError)
				detail := RunDetail{
					JobName:                jobName,
					RunId:                  run.RunId,
					JobId:                  job.JobId,
					JobRunId:               run.NumberInJob,
					StartTime:              run.StartTime,
					RunPageUrl:             run.RunPageUrl,
					LifeCycleState:         run.State.LifeCycleState,
					ResultState:            resultState,
					OutputError:            outputError,
					ClassifiedError:        classifiedError,
					WorkerInstanceType:     workerInstanceType,
					WorkerOnSpot:           workerOnSpot,
					WorkerAvailabilityZone: workerAvailabilityZone,
					DriverInstanceType:     driverInstanceType,
					DriverOnSpot:           driverOnSpot,
					DriverAvailabilityZone: driverAvailabilityZone,
					RunDetail:              run,
					DriverPool:             driverPool.InstancePoolSpec,
					WorkerPool:             workerPool.InstancePoolSpec,
				}

				b, err := json.Marshal(detail)
				if err != nil {
					return oops.Wrapf(err, "")
				}
				fmt.Fprintln(lockedWriter, string(b))
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := analyze(os.Stdout); err != nil {
		log.Fatalln(err)
	}
}
