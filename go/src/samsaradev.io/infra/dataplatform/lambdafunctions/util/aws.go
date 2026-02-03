package util

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
)

var RetryerConfig = &aws.Config{
	Retryer: &client.DefaultRetryer{
		NumMaxRetries:    8,
		MinRetryDelay:    50 * time.Millisecond,
		MinThrottleDelay: time.Second,
		MaxRetryDelay:    10 * time.Second,
		MaxThrottleDelay: 10 * time.Second,
	},
}
