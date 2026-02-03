// Package runner provides a standardized execution flow for dataplatops CLI commands.
package runner

import (
	"context"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/cmd/dataplatops/internal/confirm"
	"samsaradev.io/infra/dataplatform/dataplatops"
)

// Options configures how an operation is executed.
type Options struct {
	// DryRun shows what would happen without making changes.
	DryRun bool

	// SkipConfirm skips interactive confirmation prompts.
	SkipConfirm bool
}

// Run executes a dataplatops.Operation with the standard flow:
//
//  1. Validate() - Check inputs
//  2. Plan() - Show what will happen (dry-run output)
//  3. Confirm - Ask user to proceed (unless --yes or --dry-run)
//  4. Execute() - Perform the action
//
// This ensures all CLI commands behave consistently.
// Operations handle their own console output in each method.
func Run(ctx context.Context, op dataplatops.Operation, opts Options) error {
	// Step 1: Validate
	slog.Debugw(ctx, "validating operation", "operation", op.Name())
	if err := op.Validate(ctx); err != nil {
		return oops.Wrapf(err, "validation failed")
	}

	// Step 2: Plan (always runs - shows what will happen)
	slog.Debugw(ctx, "planning operation", "operation", op.Name())
	if err := op.Plan(ctx); err != nil {
		return oops.Wrapf(err, "planning failed")
	}

	// Step 3: If dry-run, stop here
	if opts.DryRun {
		return nil
	}

	// Step 4: Confirm before execution (unless --yes)
	if !opts.SkipConfirm {
		if err := confirm.Prompt("Proceed with execution?"); err != nil {
			return err
		}
	}

	// Step 5: Execute
	slog.Debugw(ctx, "executing operation", "operation", op.Name())
	if _, err := op.Execute(ctx); err != nil {
		return oops.Wrapf(err, "execution failed")
	}

	return nil
}
