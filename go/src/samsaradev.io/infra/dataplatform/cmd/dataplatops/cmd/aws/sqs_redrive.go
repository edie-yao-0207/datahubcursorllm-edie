package aws

import (
	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	"samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func sqsRedriveCmd() *cobra.Command {
	var (
		region      string
		queueName   string
		maxMessages int
	)

	cmd := &cobra.Command{
		Use:   "sqs-redrive",
		Short: "Redrive messages from a Dead Letter Queue",
		Long: `Redrive messages from a Dead Letter Queue (DLQ) back to its source queue.

This is commonly needed during incident recovery when messages have been
moved to a DLQ due to processing failures.

The command will:
  1. Resolve the DLQ URL from the queue name
  2. Show the number of messages to be redriven
  3. Start an SQS message move task (with confirmation)

Note: Large queues may take several minutes to fully redrive.`,
		Example: `  # Redrive all messages from a DLQ
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq

  # Dry run to see message count
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --dry-run

  # Redrive with auto-confirmation (for scripts)
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --yes

  # Limit redrive rate
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --max=100`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			skipConfirm, _ := cmd.Flags().GetBool("yes")

			op := aws.NewSQSRedriveOp(region, queueName, maxMessages)

			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: skipConfirm,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "", "AWS region (required)")
	cmd.Flags().StringVar(&queueName, "queue", "", "DLQ name or URL (required)")
	cmd.Flags().IntVar(&maxMessages, "max", 0, "Maximum messages per second to redrive (0 = unlimited)")

	cmd.MarkFlagRequired("region")
	cmd.MarkFlagRequired("queue")

	return cmd
}
