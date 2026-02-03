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
// DMS Upsize
// =============================================================================

type DMSUpsizeResult struct {
	Region                     string `json:"region"`
	ReplicationInstanceIDOrArn string `json:"replicationInstanceIdOrArn"`
	ReplicationInstanceArn     string `json:"replicationInstanceArn"`
	PreviousClass              string `json:"previousClass"`
	TargetClass                string `json:"targetClass"`
	ApplyImmediately           bool   `json:"applyImmediately"`
}

// DMSUpsizeOp increases a DMS replication instance class.
// If TargetClass is empty, it will pick the next size up from the current class.
type DMSUpsizeOp struct {
	// Inputs
	Region                     string
	ReplicationInstanceIDOrArn string
	TargetClass                string

	// Clients
	dmsClient *databasemigrationservice.DatabaseMigrationService

	// Planned state
	instanceArn  string
	currentClass string
	targetClass  string
}

func NewDMSUpsizeOp(region, replicationInstanceIDOrArn, targetClass string) *DMSUpsizeOp {
	return &DMSUpsizeOp{
		Region:                     region,
		ReplicationInstanceIDOrArn: replicationInstanceIDOrArn,
		TargetClass:                targetClass,
	}
}

func (o *DMSUpsizeOp) Name() string { return "dms-upsize" }

func (o *DMSUpsizeOp) Description() string {
	return "Increase a DMS replication instance class by one step (or to a specified target)"
}

func (o *DMSUpsizeOp) Validate(ctx context.Context) error {
	if strings.TrimSpace(o.Region) == "" {
		return oops.Errorf("--region is required")
	}
	if strings.TrimSpace(o.ReplicationInstanceIDOrArn) == "" {
		return oops.Errorf("--instance is required")
	}

	sess := awssessions.NewInstrumentedAWSSessionWithConfigs(&awsV1.Config{
		Region: awsV1.String(o.Region),
	})
	o.dmsClient = databasemigrationservice.New(sess)
	return nil
}

func (o *DMSUpsizeOp) Plan(ctx context.Context) error {
	if o.dmsClient == nil {
		return oops.Errorf("Validate() must be called before Plan()")
	}

	// Find replication instance.
	var filter *databasemigrationservice.Filter
	if strings.HasPrefix(o.ReplicationInstanceIDOrArn, "arn:") {
		filter = &databasemigrationservice.Filter{
			Name:   awsV1.String("replication-instance-arn"),
			Values: []*string{awsV1.String(o.ReplicationInstanceIDOrArn)},
		}
	} else {
		filter = &databasemigrationservice.Filter{
			Name:   awsV1.String("replication-instance-id"),
			Values: []*string{awsV1.String(o.ReplicationInstanceIDOrArn)},
		}
	}

	out, err := o.dmsClient.DescribeReplicationInstancesWithContext(ctx, &databasemigrationservice.DescribeReplicationInstancesInput{
		Filters: []*databasemigrationservice.Filter{filter},
	})
	if err != nil {
		return oops.Wrapf(err, "describe replication instance: %s", o.ReplicationInstanceIDOrArn)
	}
	if len(out.ReplicationInstances) != 1 {
		return oops.Errorf("expected 1 replication instance for %s, got %d", o.ReplicationInstanceIDOrArn, len(out.ReplicationInstances))
	}

	ri := out.ReplicationInstances[0]
	o.instanceArn = awsV1.StringValue(ri.ReplicationInstanceArn)
	o.currentClass = normalizeDmsInstanceClass(awsV1.StringValue(ri.ReplicationInstanceClass))
	if o.instanceArn == "" || o.currentClass == "" {
		return oops.Errorf("replication instance missing arn or class: arn=%q class=%q", o.instanceArn, o.currentClass)
	}

	if strings.TrimSpace(o.TargetClass) != "" {
		o.targetClass = normalizeDmsInstanceClass(o.TargetClass)
	} else {
		next := nextDmsInstanceClass(o.currentClass)
		if next == "" {
			return oops.Errorf("cannot determine next class for %q; please provide --target", o.currentClass)
		}
		o.targetClass = next
	}

	if o.targetClass == o.currentClass {
		return oops.Errorf("target class equals current class (%s); nothing to do", o.currentClass)
	}

	slog.Infow(ctx, "📋 Plan: upsize DMS replication instance",
		"region", o.Region,
		"replicationInstanceIdOrArn", o.ReplicationInstanceIDOrArn,
		"replicationInstanceArn", o.instanceArn,
		"currentClass", o.currentClass,
		"targetClass", o.targetClass,
		"applyImmediately", true,
	)
	return nil
}

func (o *DMSUpsizeOp) Execute(ctx context.Context) (any, error) {
	if o.dmsClient == nil || o.instanceArn == "" || o.targetClass == "" {
		return nil, oops.Errorf("Plan() must be called before Execute()")
	}

	_, err := o.dmsClient.ModifyReplicationInstanceWithContext(ctx, &databasemigrationservice.ModifyReplicationInstanceInput{
		ReplicationInstanceArn:   awsV1.String(o.instanceArn),
		ReplicationInstanceClass: awsV1.String(o.targetClass),
		ApplyImmediately:         awsV1.Bool(true),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "modify replication instance: arn=%s targetClass=%s", o.instanceArn, o.targetClass)
	}

	slog.Infow(ctx, fmt.Sprintf("✅ Requested DMS instance class change %s → %s", o.currentClass, o.targetClass),
		"region", o.Region,
		"replicationInstanceArn", o.instanceArn,
	)

	return &DMSUpsizeResult{
		Region:                     o.Region,
		ReplicationInstanceIDOrArn: o.ReplicationInstanceIDOrArn,
		ReplicationInstanceArn:     o.instanceArn,
		PreviousClass:              o.currentClass,
		TargetClass:                o.targetClass,
		ApplyImmediately:           true,
	}, nil
}

func nextDmsInstanceClass(current string) string {
	c := strings.TrimSpace(strings.ToLower(current))
	if c == "" {
		return ""
	}

	// Common ladders. If you need another family, pass --target.
	ladders := [][]string{
		{"dms.t3.micro", "dms.t3.small", "dms.t3.medium", "dms.t3.large", "dms.t3.xlarge", "dms.t3.2xlarge"},
		{"dms.r5.large", "dms.r5.xlarge", "dms.r5.2xlarge", "dms.r5.4xlarge", "dms.r5.8xlarge", "dms.r5.12xlarge", "dms.r5.16xlarge", "dms.r5.24xlarge"},
		{"dms.r6i.large", "dms.r6i.xlarge", "dms.r6i.2xlarge", "dms.r6i.4xlarge", "dms.r6i.8xlarge", "dms.r6i.12xlarge", "dms.r6i.16xlarge", "dms.r6i.24xlarge", "dms.r6i.32xlarge"},
	}

	for _, ladder := range ladders {
		for i := 0; i < len(ladder); i++ {
			if ladder[i] != c {
				continue
			}
			if i == len(ladder)-1 {
				return ""
			}
			return ladder[i+1]
		}
	}

	return ""
}

func normalizeDmsInstanceClass(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
