package dataplatformresource_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

// Finds the S3LifecycleRule in the set of awsresource
// using the ID, return the rule if found, otherwise
// returns nil.
func findS3LifecycleRule(id string, resources []awsresource.S3LifecycleRule) *awsresource.S3LifecycleRule {
	for _, awsresource := range resources {
		if awsresource.ID == id {
			// Found the required S3LifecycleRule id
			return &awsresource
		}
	}
	// Could not Find the required S3LifecycleRule id
	return nil
}

func TestGetBucketS3GlacierIRDaysLifecycle(t *testing.T) {
	type testCase struct {
		description  string
		input_bucket *dataplatformresource.Bucket
		exists       bool
		expectedKey  string
	}

	testCases := []testCase{
		{
			description: "Test bucket when S3GlacierIRDays is not set",
			input_bucket: &dataplatformresource.Bucket{
				Name: "test-without-s3-glacier-days",
			},
			// When not set, it should not contain the expectedKey
			exists:      false,
			expectedKey: "transition-to-glacier-ir-0-days",
		},

		{
			description: "Test bucket when S3GlacierIRDays is explicitly set to 0",
			input_bucket: &dataplatformresource.Bucket{
				Name:            "test-without-s3-glacier-days",
				S3GlacierIRDays: 0,
			},
			// When set to 0, it should not contain the expectedKey i.e as good as not setting it.
			exists:      false,
			expectedKey: "transition-to-glacier-ir-0-days",
		},

		{
			description: "Test bucket when S3GlacierIRDays is set to 7",
			input_bucket: &dataplatformresource.Bucket{
				Name:            "test-without-s3-glacier-days",
				S3GlacierIRDays: 7,
			},
			// When set, should not contain the expectedKey
			exists:      true,
			expectedKey: "transition-to-glacier-ir-7-days",
		},
	}

	for _, ts := range testCases {
		t.Run(
			fmt.Sprintf("TestCase:: %s", ts.description), func(t *testing.T) {
				actual := ts.input_bucket.Bucket()
				// Check for presence of lifecycle ID using findS3LifecycleRule.
				actual_rule := findS3LifecycleRule(ts.expectedKey, actual.LifecycleRules)
				if ts.exists {
					assert.Equal(t, ts.expectedKey, actual_rule.ID)
					assert.Equal(t, awsresource.S3StorageTierGlacierIR, actual_rule.Transition.StorageClass)
					assert.Equal(t, ts.input_bucket.S3GlacierIRDays, actual_rule.Transition.Days)
				} else {
					assert.Empty(t, actual_rule)
				}

			})
	}

}
