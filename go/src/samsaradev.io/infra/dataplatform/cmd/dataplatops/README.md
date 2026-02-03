# dataplatops

Data Platform operational automation CLI tool.

## Index

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Execution Flow](#execution-flow)
- [Operation Interface](#operation-interface)
  - [Interface Methods Explained](#interface-methods-explained)
- [File Structure](#file-structure)
- [Adding a New Operation](#adding-a-new-operation)
- [Programmatic Usage](#programmatic-usage)
- [Global Flags](#global-flags)
- [Best Practices](#best-practices)

## Overview

`dataplatops` provides standardized, safe automation for common Data Platform operational tasks. It features:

- **Dry-run by default** - See what will happen before execution
- **Interactive confirmation** - Prevents accidental destructive actions
- **Typed results** - Structured responses for programmatic use
- **Reusable library** - Core logic can be imported by other services

## Quick Start
First of all, cd into the folder
```bash
cd $BACKEND_ROOT/go/src/samsaradev.io/infra/dataplatform/cmd/dataplatops
```

then run the tool:

### Example Commands

```bash
# Dry run (show plan without executing)
go run main.go aws sqs-redrive --region=us-west-2 --queue=my-dlq --dry-run

# Execute with confirmation prompt
go run main.go aws sqs-redrive --region=us-west-2 --queue=my-dlq

# Execute without confirmation (for scripts)
go run main.go aws sqs-redrive --region=us-west-2 --queue=my-dlq --yes

# Show queue info
go run main.go aws sqs-show --region=us-west-2 --queue=my-dlq
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         dataplatops                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  CLI Tool (cmd/dataplatops/)           Core Library (dataplatops/)  │
│  ┌─────────────────────────┐          ┌─────────────────────────┐   │
│  │                         │          │                         │   │
│  │  cmd/root.go            │          │  operation.go           │   │
│  │  - Global flags         │  uses    │  - Operation interface  │   │
│  │  - Error handling       │ ───────▶ │  - Run() helper         │   │
│  │                         │          │                         │   │
│  │  cmd/aws/               │          │  aws/                   │   │
│  │  - sqs_redrive.go       │          │  - sqs_redrive.go       │   │
│  │  - sqs_show.go          │          │  - sqs_show.go          │   │
│  │                         │          │  - helper.go            │   │
│  │  internal/runner/       │          │                         │   │
│  │  - Validate→Plan→Exec   │          │  (importable by other   │   │
│  │  - Confirmation prompts │          │   services)             │   │
│  └─────────────────────────┘          └─────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Execution Flow

Every operation follows the same flow:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Validate   │────▶│     Plan     │────▶│   Confirm    │────▶│   Execute    │
│   () error   │     │   () error   │     │  (CLI only)  │     │ (any, error) │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  Check inputs         Show what           --dry-run:            Perform the
  Init clients         will happen           STOP               operation
                       Print plan          --yes: SKIP          Return typed
                                                                result struct
```

## Operation Interface

All operations implement this interface:

```go
type Operation interface {
    Name() string                           // e.g., "sqs-redrive"
    Description() string                    // Human-readable description
    Validate(ctx context.Context) error     // Check inputs, init clients
    Plan(ctx context.Context) error         // Show what will happen (dry-run)
    Execute(ctx context.Context) (any, error) // Perform action, return typed result
}
```

### Interface Methods Explained

#### `Name() string`

Returns the operation identifier used in logs and error messages.

**What to implement:**
- Return a short, kebab-case identifier (e.g., `"sqs-redrive"`, `"rds-reload"`)

---

#### `Description() string`

Returns a human-readable description of what the operation does.

**What to implement:**
- Return a one-line description (e.g., `"Redrive messages from a Dead Letter Queue"`)

---

#### `Validate(ctx context.Context) error`

Called first. Validates all inputs and initializes any clients needed for later steps.

**What to implement:**
- Check all required inputs are provided (return error if missing)
- Validate input formats/values are correct
- Initialize API clients (AWS, Databricks, etc.) and store on the struct
- Do NOT make any state-changing API calls

**Example checks:**
- `--region` flag is not empty
- Queue name matches expected pattern
- AWS credentials are valid (client creation succeeds)

---

#### `Plan(ctx context.Context) error`

Called after `Validate()`. Fetches current state and shows what will happen.

**What to implement:**
- Make read-only API calls to gather current state
- Validate the operation is possible (e.g., queue exists, is a DLQ)
- Print a formatted plan to console showing:
  - Input parameters
  - Current state (e.g., message counts)
  - What will change
  - Any warnings (large operations, empty queues, etc.)
- Store any state needed for `Execute()` on the struct

**Console output guidelines:**
- Use emoji prefixes for visual clarity (📋, ⚠️, etc.)
- Use box-drawing characters for structure
- Show all relevant details the operator needs to confirm

---

#### `Execute(ctx context.Context) (any, error)`

Called after user confirmation. Performs the actual operation.

**What to implement:**
- Perform the state-changing operation
- Print progress/success messages to console
- Return your operation's typed result struct (e.g., `*SQSRedriveResult`)

**Result struct guidelines:**
- Define a typed struct (e.g., `SQSRedriveResult`) for programmatic consumers
- Include all relevant output data (task ARNs, counts, etc.)
- Callers use type assertion: `result.(*aws.SQSRedriveResult)`

## File Structure

```
infra/dataplatform/
├── dataplatops/                      # Core library (reusable)
│   ├── operation.go                  # Operation interface + Run()
│   └── aws/
│       ├── helper.go                 # Shared AWS helpers
│       ├── sqs_redrive.go            # SQSRedriveOp + SQSRedriveResult
│       └── sqs_show.go               # SQSShowOp + SQSShowResult
│
└── cmd/dataplatops/                  # CLI tool (this directory)
    ├── main.go                       # Entry point
    ├── README.md                     # This file
    └── cmd/
        ├── root.go                   # Global flags, error handling
        └── aws/
            ├── aws.go                # `dataplatops aws` command
            ├── sqs_redrive.go        # CLI wrapper for SQSRedriveOp
            └── sqs_show.go           # CLI wrapper for SQSShowOp
```

## Adding a New Operation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Adding a New Operation                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Define result struct in dataplatops/aws/rds_reload.go                   │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │ type RDSReloadResult struct {                           │             │
│     │     Database  string   `json:"database"`                │             │
│     │     Shards    []int    `json:"shards"`                  │             │
│     │     Duration  string   `json:"duration"`                │             │
│     │ }                                                       │             │
│     └─────────────────────────────────────────────────────────┘             │
│                              │                                              │
│                              ▼                                              │
│  2. Implement Operation interface                                           │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │ type RDSReloadOp struct { ... }                         │             │
│     │                                                         │             │
│     │ func (o *RDSReloadOp) Name() string                     │             │
│     │ func (o *RDSReloadOp) Description() string              │             │
│     │ func (o *RDSReloadOp) Validate(ctx) error               │             │
│     │ func (o *RDSReloadOp) Plan(ctx) error                   │             │
│     │ func (o *RDSReloadOp) Execute(ctx) (any, error) {       │             │
│     │     return &RDSReloadResult{...}, nil                   │             │
│     │ }                                                       │             │
│     └─────────────────────────────────────────────────────────┘             │
│                              │                                              │
│                              ▼                                              │
│  3. Create CLI command in cmd/dataplatops/cmd/aws/rds_reload.go             │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │ func rdsReloadCmd() *cobra.Command {                    │             │
│     │     op := aws.NewRDSReloadOp(region, name, shards)      │             │
│     │     return runner.Run(ctx, op, opts)                    │             │
│     │ }                                                       │             │
│     └─────────────────────────────────────────────────────────┘             │
│                              │                                              │
│                              ▼                                              │
│  4. Register in cmd/dataplatops/cmd/aws/aws.go                              │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │ cmd.AddCommand(rdsReloadCmd())                          │             │
│     └─────────────────────────────────────────────────────────┘             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Reference implementations:**
- `dataplatops/aws/sqs_redrive.go` - Destructive operation with confirmation
- `dataplatops/aws/sqs_show.go` - Read-only operation

## Programmatic Usage

The core library can be imported by other services (e.g., watchdogs):

```go
import (
    "samsaradev.io/infra/dataplatform/dataplatops"
    "samsaradev.io/infra/dataplatform/dataplatops/aws"
)

func handleAlert(ctx context.Context, queueName string) error {
    op := aws.NewSQSRedriveOp("us-west-2", queueName, 0)
    
    result, err := dataplatops.Run(ctx, op)
    if err != nil {
        return err
    }
    
    // Type assert to access operation-specific fields
    sqsResult := result.(*aws.SQSRedriveResult)
    slog.Infow(ctx, "redrive completed",
        "taskArn", sqsResult.TaskArn,
        "messages", sqsResult.MessagesRedriven,
    )
    
    return nil
}
```

Or call methods directly for more control:

```go
op := aws.NewSQSRedriveOp("us-west-2", queueName, 0)

if err := op.Validate(ctx); err != nil {
    return err
}

if err := op.Plan(ctx); err != nil {  // Dry-run only
    return err
}

// Optionally execute
result, err := op.Execute(ctx)
if err != nil {
    return err
}

// Type assert to access fields
sqsResult := result.(*aws.SQSRedriveResult)
fmt.Printf("Task ARN: %s\n", sqsResult.TaskArn)
```

## Global Flags

All commands support these flags:

| Flag | Description |
|------|-------------|
| `--dry-run` | Show what would happen without executing |
| `--yes` | Skip confirmation prompt |
| `--verbose` | Enable verbose output |

## Best Practices

1. **Always implement dry-run** - The `Plan()` method should show exactly what will happen
2. **Print progress** - Use `fmt.Println` in `Plan()` and `Execute()` for CLI feedback
3. **Return typed results** - Define a result struct for programmatic consumers
4. **Validate early** - Check all inputs in `Validate()` before any work
5. **Use oops for errors** - Wrap errors with context using `oops.Wrapf()`

