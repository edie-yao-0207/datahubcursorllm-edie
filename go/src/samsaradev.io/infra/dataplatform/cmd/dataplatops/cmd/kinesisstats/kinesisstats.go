package kinesisstats

import "github.com/spf13/cobra"

// Command returns the kinesisstats command group.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kinesisstats",
		Short: "KinesisStats operational commands",
	}
	cmd.AddCommand(showRegistryCmd())
	return cmd
}
