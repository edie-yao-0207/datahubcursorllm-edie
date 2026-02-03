package aws

import (
	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	"samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func dmsResumeCmd() *cobra.Command {
	var (
		region string
		taskID string
	)

	cmd := &cobra.Command{
		Use:   "dms-resume",
		Short: "Resume an AWS DMS replication task",
		Long: `Resume an AWS DMS replication task given its replication task identifier.

This command will:
  1. Look up the replication task by id (DescribeReplicationTasks filter)
  2. Show the current status and ARN (dry-run)
  3. Call StartReplicationTask with resume-processing (after confirmation)`,
		Example: `  # Dry run
  dataplatops aws dms-resume --region=us-west-2 --task=rds-mydatabase-s3-mydatabase --dry-run

  # Execute
  dataplatops aws dms-resume --region=us-west-2 --task=rds-mydatabase-s3-mydatabase --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			skipConfirm, _ := cmd.Flags().GetBool("yes")

			op := aws.NewDMSResumeOp(region, taskID)
			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: skipConfirm,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "", "AWS region (required)")
	cmd.Flags().StringVar(&taskID, "task", "", "DMS replication task identifier (required)")
	_ = cmd.MarkFlagRequired("region")
	_ = cmd.MarkFlagRequired("task")

	return cmd
}
