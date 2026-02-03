// Package aws provides AWS operational automation for the Data Platform.
package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/slog"
)

// =============================================================================
// SQS Redrive Operation
// =============================================================================

// SQSRedriveResult is the typed result returned by SQSRedriveOp.Execute().
// Use type assertion to access: result.(*aws.SQSRedriveResult)
type SQSRedriveResult struct {
	// TaskArn is the ARN of the SQS message move task.
	TaskArn string `json:"taskArn"`

	// MessagesRedriven is the approximate number of messages being redriven.
	MessagesRedriven int64 `json:"messagesRedriven"`

	// DLQ contains information about the Dead Letter Queue.
	DLQ QueueInfo `json:"dlq"`

	// SourceQueues lists the queue URLs messages will be redriven to.
	SourceQueues []string `json:"sourceQueues"`
}

// QueueInfo contains basic information about an SQS queue.
type QueueInfo struct {
	URL string `json:"url"`
	ARN string `json:"arn"`
}

// SQSRedriveOp redrives messages from a Dead Letter Queue back to its source queue.
type SQSRedriveOp struct {
	// Input fields
	Region      string
	QueueName   string
	MaxMessages int

	// Internal state (populated during Validate/Plan)
	client       *sqs.Client
	dlqURL       string
	dlqARN       string
	sourceQueues []string
	msgCount     int64
}

// NewSQSRedriveOp creates a new SQS redrive operation.
func NewSQSRedriveOp(region, queueName string, maxMessages int) *SQSRedriveOp {
	return &SQSRedriveOp{
		Region:      region,
		QueueName:   queueName,
		MaxMessages: maxMessages,
	}
}

// Name implements dataplatops.Operation.
func (o *SQSRedriveOp) Name() string {
	return "sqs-redrive"
}

// Description implements dataplatops.Operation.
func (o *SQSRedriveOp) Description() string {
	return "Redrive messages from a Dead Letter Queue back to its source queue"
}

// Validate implements dataplatops.Operation.
func (o *SQSRedriveOp) Validate(ctx context.Context) error {
	if o.Region == "" {
		return oops.Errorf("--region is required")
	}
	if o.QueueName == "" {
		return oops.Errorf("--queue is required")
	}

	client, err := newSQSClient(ctx, o.Region)
	if err != nil {
		return oops.Wrapf(err, "failed to create SQS client")
	}

	o.client = client

	return nil
}

// Plan implements dataplatops.Operation.
func (o *SQSRedriveOp) Plan(ctx context.Context) error {
	// Resolve queue URL
	dlqURL, err := resolveQueueURL(ctx, o.client, o.QueueName)
	if err != nil {
		return oops.Wrapf(err, "failed to resolve queue URL for %s", o.QueueName)
	}

	o.dlqURL = dlqURL

	// Get queue attributes (including ARN for the redrive API)
	attrsOutput, err := o.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameQueueArn,
		},
	})
	if err != nil {
		return oops.Wrapf(err, "failed to get queue attributes")
	}

	// Get the ARN (required for StartMessageMoveTask)
	o.dlqARN = attrsOutput.Attributes[string(types.QueueAttributeNameQueueArn)]
	if o.dlqARN == "" {
		return oops.Errorf("failed to get queue ARN for %s", dlqURL)
	}

	// Check if this queue is actually a DLQ by listing its source queues
	sourceQueuesOutput, err := o.client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: aws.String(dlqURL),
	})
	if err != nil {
		return oops.Wrapf(err, "failed to check if queue is a DLQ")
	}
	if len(sourceQueuesOutput.QueueUrls) == 0 {
		return oops.Errorf("queue %s is not configured as a DLQ for any source queue", o.QueueName)
	}
	o.sourceQueues = sourceQueuesOutput.QueueUrls

	// Parse message count
	msgCountStr := attrsOutput.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	if msgCountStr != "" {
		fmt.Sscanf(msgCountStr, "%d", &o.msgCount)
	}

	// Print the plan
	fmt.Println()
	fmt.Println("📋 SQS Redrive Plan")
	fmt.Println("───────────────────────────────────────")
	fmt.Printf("   Region:       %s\n", o.Region)
	fmt.Printf("   DLQ:          %s\n", o.dlqURL)
	fmt.Printf("   ARN:          %s\n", o.dlqARN)
	fmt.Printf("   Messages:     %d\n", o.msgCount)
	if o.MaxMessages > 0 {
		fmt.Printf("   Rate Limit:   %d msg/sec\n", o.MaxMessages)
	}
	fmt.Println()
	fmt.Println("   Source queue(s) messages will be redriven to:")
	for _, sq := range o.sourceQueues {
		fmt.Printf("     → %s\n", sq)
	}
	fmt.Println()

	if o.msgCount == 0 {
		fmt.Println("⚠️  Warning: DLQ has no messages to redrive")
	}
	if o.msgCount > 10000 {
		fmt.Printf("⚠️  Warning: Large queue (%d messages) - redrive may take several minutes\n", o.msgCount)
	}

	return nil
}

// Execute implements dataplatops.Operation.
// Returns *SQSRedriveResult.
func (o *SQSRedriveOp) Execute(ctx context.Context) (any, error) {
	if o.dlqARN == "" {
		return nil, oops.Errorf("Plan() must be called before Execute()")
	}

	fmt.Println("🚀 Starting redrive...")

	// Start the message move task (requires ARN, not URL)
	moveInput := &sqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(o.dlqARN),
	}
	if o.MaxMessages > 0 {
		moveInput.MaxNumberOfMessagesPerSecond = aws.Int32(int32(o.MaxMessages))
	}

	moveOutput, err := o.client.StartMessageMoveTask(ctx, moveInput)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to start message move task")
	}

	taskArn := aws.ToString(moveOutput.TaskHandle)

	fmt.Println()
	fmt.Println("✅ Redrive initiated successfully!")
	fmt.Printf("   Task ARN:  %s\n", taskArn)
	fmt.Printf("   Messages:  %d (approximate)\n", o.msgCount)
	fmt.Println()
	fmt.Println("Monitor progress:")
	fmt.Printf("   dataplatops aws sqs-show --region=%s --queue=%s\n", o.Region, o.QueueName)

	slog.Infow(ctx, "sqs redrive completed",
		"region", o.Region,
		"queue", o.QueueName,
		"messagesRedriven", o.msgCount,
		"taskArn", taskArn,
	)

	return &SQSRedriveResult{
		TaskArn:          taskArn,
		MessagesRedriven: o.msgCount,
		DLQ: QueueInfo{
			URL: o.dlqURL,
			ARN: o.dlqARN,
		},
		SourceQueues: o.sourceQueues,
	}, nil
}
