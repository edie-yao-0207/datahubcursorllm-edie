// Package aws provides AWS-related CLI commands for dataplatops.
package aws

import "github.com/spf13/cobra"

// Command returns the aws parent command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "aws",
		Short: "AWS operations",
		Long: `Commands for AWS infrastructure operations.

Available operations:
  • sqs-redrive  - Redrive messages from Dead Letter Queues
  • sqs-show     - Show SQS queue information and status
  • dms-resume   - Resume a DMS replication task
  • dms-upsize   - Increase a DMS replication instance class
  • dms-update-password - Update a DMS source endpoint password for task(s)

Coming soon:
  • dms-restart  - Restart DMS replication tasks
  • ec2-terminate - Terminate EC2 instances by filter`,
		Example: `  # Redrive all messages from a DLQ
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq

  # Show queue details
  dataplatops aws sqs-show --region=us-west-2 --queue=my-service-dlq

  # Resume a DMS task
  dataplatops aws dms-resume --region=us-west-2 --task=rds-mydatabase-s3-mydatabase

  # Upsize a DMS replication instance
  dataplatops aws dms-upsize --region=us-west-2 --instance=my-repl-instance

  # Update DMS source endpoint password for task(s)
  dataplatops aws dms-update-password --region=us-west-2 --database=mydb --task-type=parquet --shards=prod,1,2,3

  # Dry run before redrive
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --dry-run`,
	}

	// Register subcommands
	cmd.AddCommand(sqsRedriveCmd())
	cmd.AddCommand(sqsShowCmd())
	cmd.AddCommand(dmsResumeCmd())
	cmd.AddCommand(dmsUpsizeCmd())
	cmd.AddCommand(dmsUpdatePasswordCmd())

	return cmd
}
