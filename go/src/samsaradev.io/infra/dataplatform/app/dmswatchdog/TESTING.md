# DMS Watchdog Local Testing Guide

This guide provides instructions for testing the DMS Watchdog locally with AWS admin access.

## Prerequisites

1. **AWS Admin Access**: You need AWS credentials with permissions to:
   - Read DMS replication tasks and instances (`dms:Describe*`)
   - Modify DMS replication instances (`dms:ModifyReplicationInstance`)
   - Start DMS replication tasks (`dms:StartReplicationTask`)
   - Read CloudWatch metrics (`cloudwatch:GetMetricStatistics`)

2. **AWS Profile Configuration**: Set up your AWS profile with credentials
   ```bash
   # Configure AWS credentials if not already done
   aws configure --profile your-profile-name
   ```

3. **Go Environment**: Ensure Go 1.24.2+ is installed

## Building the Binary

From the repository root:

```bash
cd go/src/samsaradev.io/infra/dataplatform/app/dmswatchdog
go build -o /tmp/dmswatchdog
```

## Available Flags

The watchdog supports the following flags for testing:

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--run-once` | bool | false | Run once and exit (required for local testing) |
| `--dry-run` | bool | false | Log actions but don't execute them |
| `--skip-resume` | bool | false | Skip failed/stopped task resume remediation |
| `--skip-latency` | bool | false | Skip latency monitoring and upsizing |
| `--region` | string | "" | AWS region override (uses AWS_REGION or session default) |
| `--latency-threshold` | int | 20 | Latency threshold in minutes for triggering upsize |

## Testing Scenarios

### 1. Test Resume Failed/Stopped Tasks (Dry Run)

This tests the first remediation feature: resuming failed or stopped DMS tasks.

```bash
# Set AWS profile and region
export AWS_PROFILE=your-profile-name
export AWS_REGION=us-west-2  # or your region

# Run in dry-run mode to see what would happen without making changes
/tmp/dmswatchdog \
  --run-once \
  --dry-run \
  --skip-latency

# Expected output:
# - Lists all DMS replication tasks
# - Shows which tasks are failed/stopped
# - Logs "[DRY RUN] Would resume replication task" for actionable tasks
# - No actual changes made
```

**What to look for:**
- Console output showing scanned tasks
- Any tasks with status "failed" or "stopped"
- Dry-run messages indicating what would be resumed
- No errors about AWS permissions

### 2. Test Latency Monitoring (Dry Run)

This tests the second remediation feature: detecting high latency and upsizing instances.

```bash
export AWS_PROFILE=your-profile-name
export AWS_REGION=us-west-2

# Test with custom latency threshold (e.g., 5 minutes to trigger more easily)
/tmp/dmswatchdog \
  --run-once \
  --dry-run \
  --skip-resume \
  --latency-threshold 5

# Expected output:
# - Queries CloudWatch metrics for CDCLatencySource and CDCLatencyTarget
# - Shows latency values for running tasks
# - Logs "[DRY RUN] Would upsize DMS instance" for high-latency instances
# - No actual changes made
```

**What to look for:**
- CloudWatch metric queries completing successfully
- Latency values in seconds for each running task
- Any tasks exceeding the threshold
- Instance class information (current → target)
- No errors about CloudWatch permissions

### 3. Test Both Features Together (Dry Run)

```bash
export AWS_PROFILE=your-profile-name
export AWS_REGION=us-west-2

# Run both remediations in dry-run mode
/tmp/dmswatchdog \
  --run-once \
  --dry-run \
  --latency-threshold 10

# Expected output:
# - Resume scan for failed/stopped tasks
# - Latency check for high-latency tasks
# - Complete summary of both operations
```

### 4. Test with Actual Remediation (Use with Caution!)

⚠️ **WARNING**: This will make actual changes to your AWS DMS environment!

```bash
export AWS_PROFILE=your-profile-name
export AWS_REGION=us-west-2

# Remove --dry-run to actually resume tasks and upsize instances
/tmp/dmswatchdog \
  --run-once \
  --latency-threshold 20

# This will:
# - Actually resume failed/stopped tasks
# - Actually upsize instances with high latency
# - Make real AWS API calls that affect your infrastructure
```

**Before running without `--dry-run`:**
1. Verify you have appropriate AWS permissions
2. Ensure you understand the cost implications of upsizing instances
3. Consider testing in a non-production environment first
4. Review the dry-run output thoroughly

### 5. Test Specific Region

```bash
# Test against a specific AWS region
/tmp/dmswatchdog \
  --run-once \
  --dry-run \
  --region us-east-1

# Or use environment variable
AWS_REGION=eu-west-1 /tmp/dmswatchdog --run-once --dry-run
```

### 6. Test with Lower Latency Threshold

To trigger the upsize logic more easily during testing:

```bash
export AWS_PROFILE=your-profile-name
export AWS_REGION=us-west-2

# Use a very low threshold (e.g., 1 minute) to test the upsize logic
/tmp/dmswatchdog \
  --run-once \
  --dry-run \
  --skip-resume \
  --latency-threshold 1

# This makes it more likely to detect "high latency" tasks for testing
```

## Understanding the Output

### Successful Run Example

```
🚀 Running DMS Watchdog in run-once mode
   Region: us-west-2
   Dry Run: true
   Skip Resume: false
   Skip Latency: false
   Latency Threshold: 20 minutes

{"level":"info","msg":"starting dms watchdog scan","dryRun":true,"skipResume":false,"skipLatency":false}
{"level":"info","msg":"attempting to resume replication task","replicationTaskArn":"arn:aws:dms:...","status":"failed","dryRun":true}
{"level":"info","msg":"[DRY RUN] Would resume replication task","replicationTaskArn":"arn:aws:dms:...","status":"failed"}
{"level":"info","msg":"completed dms watchdog task resume scan","tasksScanned":15,"tasksActionable":2,"tasksResumed":2,"tasksFailed":0}
{"level":"info","msg":"starting dms latency check"}
{"level":"info","msg":"detected high latency","replicationTaskArn":"arn:aws:dms:...","latencySeconds":1500,"thresholdSeconds":1200}
{"level":"info","msg":"[DRY RUN] Would upsize DMS instance","currentClass":"dms.r5.large","targetClass":"dms.r5.xlarge"}
{"level":"info","msg":"completed dms latency check","tasksChecked":10,"highLatencyTasks":1,"instancesUpsized":1}

✅ Completed successfully
```

### Common Issues

1. **No tasks found**: Normal if there are no DMS tasks in the region
2. **CloudWatch permission errors**: Ensure your AWS profile has `cloudwatch:GetMetricStatistics`
3. **DMS permission errors**: Ensure you have `dms:Describe*`, `dms:ModifyReplicationInstance`, `dms:StartReplicationTask`
4. **No metrics returned**: Normal for tasks that haven't been running long enough to generate CloudWatch metrics

## Monitoring Real Execution

If you run without `--dry-run`, monitor the actual changes:

```bash
# Watch DMS tasks
aws dms describe-replication-tasks --region us-west-2 | jq '.ReplicationTasks[] | {Identifier: .ReplicationTaskIdentifier, Status: .Status}'

# Watch DMS instances
aws dms describe-replication-instances --region us-west-2 | jq '.ReplicationInstances[] | {Identifier: .ReplicationInstanceIdentifier, Class: .ReplicationInstanceClass, Status: .ReplicationInstanceStatus}'

# Check CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/DMS \
  --metric-name CDCLatencySource \
  --dimensions Name=ReplicationTaskIdentifier,Value=YOUR_TASK_ID \
  --start-time $(date -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Maximum \
  --region us-west-2
```

## Production Deployment Notes

When deploying to production:
- Remove all testing flags (`--run-once`, `--dry-run`, etc.)
- The service will run continuously with leader election
- It will poll every 30 minutes
- All actions will be real (no dry-run)
- Metrics will be emitted to Datadog

## Support

If you encounter issues:
1. Check AWS permissions first
2. Verify the region has DMS tasks
3. Review CloudWatch metrics exist for the tasks
4. Check the logs for specific error messages

