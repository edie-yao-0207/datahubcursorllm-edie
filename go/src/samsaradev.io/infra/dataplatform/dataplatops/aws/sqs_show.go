// Package aws provides AWS operational automation for the Data Platform.
package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/samsarahq/go/oops"
)

// =============================================================================
// SQS Show Operation
// =============================================================================

// SQSShowResult is the typed result returned by SQSShowOp.Execute().
// Use type assertion to access: result.(*aws.SQSShowResult)
type SQSShowResult struct {
	// Queue contains the queue URL and ARN.
	Queue QueueInfo `json:"queue"`

	// MessageCounts contains the message count information.
	MessageCounts MessageCounts `json:"messageCounts"`
}

// MessageCounts contains message count information for a queue.
type MessageCounts struct {
	// Visible is the approximate number of visible messages.
	Visible int64 `json:"visible"`

	// NotVisible is the approximate number of in-flight messages.
	NotVisible int64 `json:"notVisible"`

	// Delayed is the approximate number of delayed messages.
	Delayed int64 `json:"delayed"`
}

// SQSShowOp displays detailed information about an SQS queue.
type SQSShowOp struct {
	// Input fields
	Region    string
	QueueName string

	// Internal state
	client *sqs.Client

	// Result data (populated during Plan)
	queueURL       string
	queueARN       string
	msgCount       int64
	msgsNotVisible int64
	msgsDelayed    int64
}

// NewSQSShowOp creates a new SQS show operation.
func NewSQSShowOp(region, queueName string) *SQSShowOp {
	return &SQSShowOp{
		Region:    region,
		QueueName: queueName,
	}
}

// Name implements dataplatops.Operation.
func (o *SQSShowOp) Name() string {
	return "sqs-show"
}

// Description implements dataplatops.Operation.
func (o *SQSShowOp) Description() string {
	return "Display detailed information about an SQS queue"
}

// Validate implements dataplatops.Operation.
func (o *SQSShowOp) Validate(ctx context.Context) error {
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
// For read-only operations, Plan does all the work since there's no side effect.
func (o *SQSShowOp) Plan(ctx context.Context) error {
	queueURL, err := resolveQueueURL(ctx, o.client, o.QueueName)
	if err != nil {
		return oops.Wrapf(err, "failed to resolve queue URL")
	}
	o.queueURL = queueURL

	attrsOutput, err := o.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return oops.Wrapf(err, "failed to get queue attributes")
	}

	// Parse and store values
	fmt.Sscanf(attrsOutput.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], "%d", &o.msgCount)
	fmt.Sscanf(attrsOutput.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], "%d", &o.msgsNotVisible)
	fmt.Sscanf(attrsOutput.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)], "%d", &o.msgsDelayed)
	o.queueARN = attrsOutput.Attributes[string(types.QueueAttributeNameQueueArn)]

	// Print the info
	fmt.Println()
	fmt.Println("📊 SQS Queue Information")
	fmt.Println("───────────────────────────────────────")
	fmt.Printf("   URL:          %s\n", o.queueURL)
	fmt.Printf("   ARN:          %s\n", o.queueARN)
	fmt.Println()
	fmt.Println("📬 Message Counts")
	fmt.Println("───────────────────────────────────────")
	fmt.Printf("   Visible:      %d\n", o.msgCount)
	fmt.Printf("   In-Flight:    %d\n", o.msgsNotVisible)
	fmt.Printf("   Delayed:      %d\n", o.msgsDelayed)

	return nil
}

// Execute implements dataplatops.Operation.
// Returns *SQSShowResult with the data collected during Plan.
func (o *SQSShowOp) Execute(ctx context.Context) (any, error) {
	return &SQSShowResult{
		Queue: QueueInfo{
			URL: o.queueURL,
			ARN: o.queueARN,
		},
		MessageCounts: MessageCounts{
			Visible:    o.msgCount,
			NotVisible: o.msgsNotVisible,
			Delayed:    o.msgsDelayed,
		},
	}, nil
}
