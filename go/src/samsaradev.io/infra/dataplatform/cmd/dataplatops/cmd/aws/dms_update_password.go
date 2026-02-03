package aws

import (
	"time"

	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	"samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func dmsUpdatePasswordCmd() *cobra.Command {
	var (
		region       string
		database     string
		shards       string
		taskType     string
		resumeTasks  bool
		pollInterval time.Duration
		connTimeout  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "dms-update-password",
		Short: "Update DMS source endpoint password(s) for a database",
		Long: `Update the password on the *source* endpoint(s) used by AWS DMS replication tasks for a database.

This command will:
  1. Resolve the DB shards for the region
  2. Resolve DMS replication tasks by task_type (kinesis/parquet)
  3. Fetch the DB password from the prod secrets file (S3-backed .secrets.json)
  3. Modify the DMS source endpoint password
  4. Test connections and wait for success
  5. (Optional) Resume the replication task(s)

Notes:
  - --database may be provided with or without the trailing 'db' suffix (e.g. 'products' or 'productsdb').
  - Secret key is derived from the database name, except for clouddb which uses 'database_password'.`,
		Example: `  # Dry run (recommended first)
  dataplatops aws dms-update-password --region=us-west-2 --database=productsdb --dry-run

  # Execute (all shards, all task types) + resume
  dataplatops aws dms-update-password --region=eu-west-1 --database=productsdb --resume

  # Sharded DB: target prod + shards 1-3, parquet tasks only
  dataplatops aws dms-update-password --region=us-west-2 --database=eurofleetdb --shards=prod,1,2,3 --task-type=parquet`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			skipConfirm, _ := cmd.Flags().GetBool("yes")

			op := aws.NewDMSUpdatePasswordOp(region, database, shards, taskType, resumeTasks)
			op.PollInterval = pollInterval
			op.ConnTimeout = connTimeout

			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: skipConfirm,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "", "AWS region (required)")
	cmd.Flags().StringVar(&database, "database", "", "Database name (required); accepts with/without trailing 'db' suffix (e.g. products or productsdb)")
	cmd.Flags().StringVar(&shards, "shards", "all", "Shard selector: 'all' (default) or comma-separated tokens like 'prod,1,2,3'")
	cmd.Flags().StringVar(&taskType, "task-type", "all", "DMS task_type to target: kinesis, parquet, or all (default)")
	cmd.Flags().BoolVar(&resumeTasks, "resume", false, "After connection success, resume the replication task(s) (resume-processing)")
	cmd.Flags().DurationVar(&pollInterval, "poll-interval", 10*time.Second, "Polling interval when waiting for DMS TestConnection results")
	cmd.Flags().DurationVar(&connTimeout, "conn-timeout", 3*time.Minute, "Timeout per (replication instance, endpoint) connection test")

	_ = cmd.MarkFlagRequired("region")
	_ = cmd.MarkFlagRequired("database")

	return cmd
}
