package util_test

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util"
)

func TestAccountIdForContext(t *testing.T) {
	testCases := []struct {
		description string
		ctx         context.Context
		expected    int
		hasErr      bool
	}{
		{
			description: "happy path",
			ctx: lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
				InvokedFunctionArn: arn.ARN{
					AccountID: "12345",
				}.String(),
			}),
			expected: 12345,
			hasErr:   false,
		},
		{
			description: "bad id string",
			ctx: lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
				InvokedFunctionArn: arn.ARN{
					AccountID: "abc",
				}.String(),
			}),
			hasErr: true,
		},
		{
			description: "bad arn",
			ctx: lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
				InvokedFunctionArn: "bad:arn",
			}),
			hasErr: true,
		},
		{
			description: "bad context",
			ctx:         context.Background(),
			hasErr:      true,
		},
	}

	for _, tc := range testCases {
		tc := tc // https://golang.org/doc/faq#closures_and_goroutines
		t.Run(tc.description, func(t *testing.T) {
			id, err := util.AccountIdForContext(tc.ctx)
			if tc.hasErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, id)
			}
		})
	}
}
