package main

import (
	"context"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

type metricsLoggerInput struct {
	NESFRawOutput string `json:"Output"`
}

func LambdaFunction(ctx context.Context, input metricsLoggerInput) error {
	return nil
}

func main() {
	middleware.StartWrapped(LambdaFunction, middleware.LogInputOutput)
}
