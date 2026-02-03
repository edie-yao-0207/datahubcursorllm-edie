// Package cmd provides the CLI commands for dataplatops.
package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/samsarahq/go/oops"
	"github.com/spf13/cobra"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/cmd/dataplatops/cmd/aws"
	"samsaradev.io/infra/dataplatform/cmd/dataplatops/cmd/kinesisstats"
)

var rootCmd = &cobra.Command{
	Use:   "dataplatops",
	Short: "Data Platform operational automation tool",
	Long: `dataplatops provides standardized, safe automation for common
Data Platform operational tasks.

All commands support --dry-run by default and require explicit
confirmation before making changes (use --yes to skip).

Safety Features:
  • Dry-run mode shows planned actions without execution
  • Interactive confirmation before destructive operations
  • Detailed logging for audit trails
  • Guardrails to prevent accidental damage

Tool Groups:
  • aws        - AWS operations (SQS, RDS, DMS, EC2)
  • databricks - Databricks workspace operations (coming soon)
  • dagster    - Dagster pipeline operations (coming soon)`,
	Example: `  # Redrive SQS dead letter queue
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq

  # Show SQS queue information
  dataplatops aws sqs-show --region=us-west-2 --queue=my-service-dlq

  # Dry run to see what would happen
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --dry-run

  # Auto-confirm for scripting (use with caution)
  dataplatops aws sqs-redrive --region=us-west-2 --queue=my-service-dlq --yes`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

// Root exports the root command for doc generation and testing.
func Root() *cobra.Command {
	return rootCmd
}

// Execute runs the root command.
func Execute() {
	slog.SetUpDefaultCLILogger()
	if err := rootCmd.Execute(); err != nil {
		printError(err)
		os.Exit(1)
	}
}

// printError formats and prints an error in a user-friendly way.
func printError(err error) {
	fmt.Println()
	fmt.Println("❌ Error:", getRootCause(err))
	fmt.Println()
	fmt.Println("Stack trace:")
	fmt.Println(err.Error())
}

// getRootCause extracts the root cause message from an oops error chain.
func getRootCause(err error) string {
	if err == nil {
		return ""
	}

	// Get the full error string
	fullErr := err.Error()

	// oops errors have the root cause first, then stack trace after newlines
	// Extract just the first line (the actual error message)
	lines := strings.Split(fullErr, "\n")
	if len(lines) > 0 {
		// The first line contains "wrapper: wrapper: root cause"
		// We want to show the full context but clean
		msg := lines[0]

		// If it's an oops wrapped error, it shows as "outer: inner: cause"
		// This is actually good context, so keep it
		return msg
	}

	return oops.Cause(err).Error()
}

func init() {
	// Global flags available to all subcommands
	rootCmd.PersistentFlags().Bool("dry-run", false, "Show what would happen without making changes")
	rootCmd.PersistentFlags().Bool("yes", false, "Skip confirmation prompts (use with caution)")
	rootCmd.PersistentFlags().Bool("verbose", false, "Enable verbose output")

	// Register tool groups
	rootCmd.AddCommand(aws.Command())
	rootCmd.AddCommand(kinesisstats.Command())

	// TODO: Add these as they are implemented
	// rootCmd.AddCommand(databricks.Command())
	// rootCmd.AddCommand(dagster.Command())
}
