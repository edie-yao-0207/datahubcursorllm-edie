package aws

import (
	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	"samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func sqsShowCmd() *cobra.Command {
	var (
		region    string
		queueName string
	)

	cmd := &cobra.Command{
		Use:   "sqs-show",
		Short: "Show SQS queue information",
		Long: `Display detailed information about an SQS queue.

This command shows:
  • Queue URL and ARN
  • Message counts (visible, in-flight, delayed)
  • Whether the queue is configured as a DLQ

Useful for checking queue health and monitoring redrive progress.`,
		Example: `  # Show queue details
  dataplatops aws sqs-show --region=us-west-2 --queue=my-service-dlq`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			op := aws.NewSQSShowOp(region, queueName)

			// Read-only operation - always skip confirm
			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: true,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "", "AWS region (required)")
	cmd.Flags().StringVar(&queueName, "queue", "", "Queue name or URL (required)")

	cmd.MarkFlagRequired("region")
	cmd.MarkFlagRequired("queue")

	return cmd
}
