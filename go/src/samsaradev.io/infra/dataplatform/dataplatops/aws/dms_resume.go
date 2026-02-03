package aws

import (
	"context"
	"fmt"
	"strings"

	awsV1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

// =============================================================================
// DMS Resume
// =============================================================================

type DMSResumeResult struct {
	Region                  string `json:"region"`
	ReplicationTaskID       string `json:"replicationTaskId"`
	ReplicationTaskArn      string `json:"replicationTaskArn"`
	PreviousStatus          string `json:"previousStatus"`
	AttemptedResume         bool   `json:"attemptedResume"`
	StartReplicationTaskArn string `json:"startReplicationTaskArn,omitempty"`
}

// DMSResumeOp resumes a DMS replication task by its replication task identifier.
type DMSResumeOp struct {
	// Inputs
	Region            string
	ReplicationTaskID string

	// Clients
	dmsClient *databasemigrationservice.DatabaseMigrationService

	// Planned state
	taskArn string
	status  string
}

func NewDMSResumeOp(region, replicationTaskID string) *DMSResumeOp {
	return &DMSResumeOp{
		Region:            region,
		ReplicationTaskID: replicationTaskID,
	}
}

func (o *DMSResumeOp) Name() string {
	return "dms-resume"
}

func (o *DMSResumeOp) Description() string {
	return "Resume an AWS DMS replication task"
}

func (o *DMSResumeOp) Validate(ctx context.Context) error {
	if strings.TrimSpace(o.Region) == "" {
		return oops.Errorf("--region is required")
	}
	if strings.TrimSpace(o.ReplicationTaskID) == "" {
		return oops.Errorf("--task is required")
	}

	sess := awssessions.NewInstrumentedAWSSessionWithConfigs(&awsV1.Config{
		Region: awsV1.String(o.Region),
	})
	o.dmsClient = databasemigrationservice.New(sess)
	return nil
}

func (o *DMSResumeOp) Plan(ctx context.Context) error {
	if o.dmsClient == nil {
		return oops.Errorf("Validate() must be called before Plan()")
	}

	// DMS supports filtering by replication-task-id.
	out, err := o.dmsClient.DescribeReplicationTasksWithContext(ctx, &databasemigrationservice.DescribeReplicationTasksInput{
		Filters: []*databasemigrationservice.Filter{
			{
				Name:   awsV1.String("replication-task-id"),
				Values: []*string{awsV1.String(o.ReplicationTaskID)},
			},
		},
		WithoutSettings: awsV1.Bool(true),
	})
	if err != nil {
		return oops.Wrapf(err, "describe replication task: id=%s", o.ReplicationTaskID)
	}
	if len(out.ReplicationTasks) != 1 {
		return oops.Errorf("expected 1 replication task for id=%s, got %d", o.ReplicationTaskID, len(out.ReplicationTasks))
	}

	task := out.ReplicationTasks[0]
	o.taskArn = awsV1.StringValue(task.ReplicationTaskArn)
	o.status = awsV1.StringValue(task.Status)

	// Print a minimal plan (runner will also show dry-run messaging).
	slog.Infow(ctx, "📋 Plan: resume DMS replication task",
		"region", o.Region,
		"replicationTaskId", o.ReplicationTaskID,
		"replicationTaskArn", o.taskArn,
		"currentStatus", o.status,
	)

	if o.taskArn == "" {
		return oops.Errorf("replication task ARN is empty for id=%s", o.ReplicationTaskID)
	}
	return nil
}

func (o *DMSResumeOp) Execute(ctx context.Context) (any, error) {
	if o.dmsClient == nil || o.taskArn == "" {
		return nil, oops.Errorf("Plan() must be called before Execute()")
	}

	// Avoid failing the op if the task is already running.
	switch strings.ToLower(o.status) {
	case "running", "starting":
		slog.Infow(ctx, "task already running; skipping resume",
			"replicationTaskId", o.ReplicationTaskID,
			"replicationTaskArn", o.taskArn,
			"status", o.status,
		)
		return &DMSResumeResult{
			Region:             o.Region,
			ReplicationTaskID:  o.ReplicationTaskID,
			ReplicationTaskArn: o.taskArn,
			PreviousStatus:     o.status,
			AttemptedResume:    false,
		}, nil
	}

	resp, err := o.dmsClient.StartReplicationTaskWithContext(ctx, &databasemigrationservice.StartReplicationTaskInput{
		ReplicationTaskArn:       awsV1.String(o.taskArn),
		StartReplicationTaskType: awsV1.String(databasemigrationservice.StartReplicationTaskTypeValueResumeProcessing),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "start replication task (resume-processing): arn=%s", o.taskArn)
	}

	startArn := ""
	if resp.ReplicationTask != nil {
		startArn = awsV1.StringValue(resp.ReplicationTask.ReplicationTaskArn)
	}
	slog.Infow(ctx, fmt.Sprintf("✅ Started DMS resume-processing for task %s", o.ReplicationTaskID),
		"replicationTaskArn", o.taskArn,
		"region", o.Region,
	)

	return &DMSResumeResult{
		Region:                  o.Region,
		ReplicationTaskID:       o.ReplicationTaskID,
		ReplicationTaskArn:      o.taskArn,
		PreviousStatus:          o.status,
		AttemptedResume:         true,
		StartReplicationTaskArn: startArn,
	}, nil
}
