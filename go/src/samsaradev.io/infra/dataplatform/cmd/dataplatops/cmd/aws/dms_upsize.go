package aws

import (
	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	"samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func dmsUpsizeCmd() *cobra.Command {
	var (
		region      string
		instance    string
		targetClass string
	)

	cmd := &cobra.Command{
		Use:   "dms-upsize",
		Short: "Increase a DMS replication instance class",
		Long: `Increase a DMS replication instance class.

By default, this will upsize the instance by one step from the current class.
You may optionally specify an explicit target class via --target.

The command will:
  1. Look up the replication instance by id or arn
  2. Show current class and chosen target (dry-run)
  3. Call ModifyReplicationInstance with ApplyImmediately=true (after confirmation)`,
		Example: `  # Upsize by one step (auto)
  dataplatops aws dms-upsize --region=us-west-2 --instance=my-repl-instance

  # Upsize to an explicit target
  dataplatops aws dms-upsize --region=us-west-2 --instance=my-repl-instance --target=dms.r5.4xlarge

  # Dry run
  dataplatops aws dms-upsize --region=us-west-2 --instance=my-repl-instance --dry-run`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			skipConfirm, _ := cmd.Flags().GetBool("yes")

			op := aws.NewDMSUpsizeOp(region, instance, targetClass)
			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: skipConfirm,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "", "AWS region (required)")
	cmd.Flags().StringVar(&instance, "instance", "", "DMS replication instance identifier or ARN (required)")
	cmd.Flags().StringVar(&targetClass, "target", "", "Target replication instance class (optional; if omitted, upsizes one step)")
	_ = cmd.MarkFlagRequired("region")
	_ = cmd.MarkFlagRequired("instance")

	return cmd
}
