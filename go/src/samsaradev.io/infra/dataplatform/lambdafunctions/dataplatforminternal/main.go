package main

import (
	"context"
	"fmt"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

func LambdaFunction(_ context.Context) error {
	// dataplatforminternal is a no-op internal test lambda function that logs
	// something to cloudwatch.
	fmt.Println("dataplatform internal test lambda function v1")
	return nil
}

func main() {
	middleware.StartWrapped(LambdaFunction, middleware.LogInputOutput)
}
