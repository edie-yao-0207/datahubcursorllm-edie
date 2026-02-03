package kinesisstats

import (
	"github.com/spf13/cobra"

	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/runner"
	lib "samsaradev.io/infra/dataplatform/dataplatops/kinesisstats"
)

func showRegistryCmd() *cobra.Command {
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "show-registry",
		Short: "Show KinesisStats registry entries and schedules",
		Long: `Show KinesisStats and S3BigStats registry entries and their schedules.

By default, output is JSON. Use --output-format=csv to export as CSV.`,
		Example: `  # Show registry as JSON (default)
  dataplatops kinesisstats show-registry

  # Export as CSV
  dataplatops kinesisstats show-registry --output-format=csv`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			op := lib.NewShowRegistryOp(outputFormat)

			// Read-only operation - always skip confirm.
			return runner.Run(ctx, op, runner.Options{
				DryRun:      dryRun,
				SkipConfirm: true,
			})
		},
	}

	cmd.Flags().StringVar(&outputFormat, "output-format", lib.OutputFormatJSON, "Output format: json or csv")

	return cmd
}
