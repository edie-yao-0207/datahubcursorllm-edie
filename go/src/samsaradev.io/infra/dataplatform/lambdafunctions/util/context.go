package util

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/arn"
)

func AccountIdForContext(ctx context.Context) (int, error) {
	lc, ok := lambdacontext.FromContext(ctx)
	if !ok {
		return -1, errors.New("could not determine lambda context")
	}
	lambdaArn, err := arn.Parse(lc.InvokedFunctionArn)
	if err != nil {
		return -1, err
	}
	accountId, err := strconv.Atoi(lambdaArn.AccountID)
	if err != nil {
		return -1, err
	}
	return accountId, nil
}
