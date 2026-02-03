package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/samsarahq/go/oops"
)

// =============================================================================
// Helper Functions
// =============================================================================

// resolveQueueURL converts a queue name to a full URL if needed.
func resolveQueueURL(ctx context.Context, client *sqs.Client, queueNameOrURL string) (string, error) {
	// If it looks like a URL, return as-is
	if len(queueNameOrURL) > 8 && queueNameOrURL[:8] == "https://" {
		return queueNameOrURL, nil
	}

	// Otherwise, resolve the name to a URL
	output, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueNameOrURL),
	})
	if err != nil {
		return "", oops.Wrapf(err, "failed to get queue URL for %s", queueNameOrURL)
	}

	return aws.ToString(output.QueueUrl), nil
}

// awsConfig loads the AWS config for a given region.
// Supports multiple credential sources via the default provider chain:
// - AWS_PROFILE environment variable for specific profiles
// - EC2 instance profiles
// - Environment variables (AWS_ACCESS_KEY_ID, etc.)
// - Shared credentials/config files
func awsConfig(ctx context.Context, region string) (*aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, oops.Wrapf(err, "failed to load AWS config for region %s", region)
	}
	return &cfg, nil
}

func newSQSClient(ctx context.Context, region string) (*sqs.Client, error) {
	cfg, err := awsConfig(ctx, region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create AWS config for region %s", region)
	}
	return sqs.NewFromConfig(*cfg), nil
}
