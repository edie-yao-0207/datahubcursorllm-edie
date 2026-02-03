// Package dataplatops provides reusable operational automation for the Data Platform.
package dataplatops

import "context"

// Operation defines the contract that all dataplatops operations must implement.
//
// The framework automatically calls methods in order: Validate -> Plan -> Execute.
// Developers only implement business logic; the framework handles:
//   - Calling Validate() first to check inputs
//   - Calling Plan() to show what will happen (dry-run)
//   - Prompting for confirmation (unless --yes flag)
//   - Calling Execute() to perform the action
//
// Each operation defines its own result struct. Callers use type assertion:
//
//	result, err := dataplatops.Run(ctx, op)
//	sqsResult := result.(*aws.SQSRedriveResult)
//
// Example implementation:
//
//	// Result struct for this operation
//	type SQSRedriveResult struct {
//	    TaskArn          string   `json:"taskArn"`
//	    MessagesRedriven int64    `json:"messagesRedriven"`
//	    SourceQueues     []string `json:"sourceQueues"`
//	}
//
//	type SQSRedriveOp struct { ... }
//
//	func (o *SQSRedriveOp) Execute(ctx context.Context) (any, error) {
//	    // Do the actual redrive
//	    return &SQSRedriveResult{
//	        TaskArn:          "arn:aws:sqs:...",
//	        MessagesRedriven: 42,
//	    }, nil
//	}
type Operation interface {
	// Name returns the operation identifier (e.g., "sqs-redrive").
	Name() string

	// Description returns a short description of what this operation does.
	Description() string

	// Validate checks that all required inputs are provided and valid.
	// Called first before any other method.
	Validate(ctx context.Context) error

	// Plan shows what would happen without making changes (dry-run).
	// Should print the planned actions to console.
	// Called after Validate(), before Execute().
	Plan(ctx context.Context) error

	// Execute performs the actual operation.
	// Returns the operation's typed result struct (use type assertion to access).
	// Only called after Plan() and user confirmation.
	Execute(ctx context.Context) (any, error)
}

// Run executes an operation with the standard flow: Validate -> Plan -> Execute.
// This is for programmatic use from other services (no confirmation prompts).
// For dry-run, call Validate() and Plan() directly without Execute().
//
// The returned value is the operation's typed result struct. Use type assertion:
//
//	result, err := dataplatops.Run(ctx, op)
//	sqsResult := result.(*aws.SQSRedriveResult)
func Run(ctx context.Context, op Operation) (any, error) {
	if err := op.Validate(ctx); err != nil {
		return nil, err
	}
	if err := op.Plan(ctx); err != nil {
		return nil, err
	}
	return op.Execute(ctx)
}
