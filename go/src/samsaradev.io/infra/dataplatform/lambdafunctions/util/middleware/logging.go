package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambdacontext"
)

func jsonOrPercentVString(item interface{}) string {
	itemJson, err := json.Marshal(item)
	if err != nil {
		return fmt.Sprintf("%+v", item)
	}
	return string(itemJson)
}

func logLambdaInputs(ctx context.Context, input []byte) (context.Context, []byte, error) {
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		log.Printf("LAMBDA CONTEXT: %s", jsonOrPercentVString(lc))
	} else {
		log.Printf("CONTEXT IS NOT LAMBDA CONTEXT")
	}
	envMap := make(map[string]string)
	for _, envvar := range os.Environ() {
		parts := strings.SplitN(envvar, "=", 2)
		switch len(parts) {
		case 2:
			envMap[parts[0]] = parts[1]
		case 1:
			envMap[parts[0]] = ""
		}
	}
	log.Printf("ENVIRONMENT: %s", jsonOrPercentVString(envMap))
	log.Printf("INPUT: %s", string(input))

	return ctx, input, nil
}

func logLambdaOutputs(ctx context.Context, output []byte, outErr error) ([]byte, error) {
	log.Printf("OUTPUT: %s", string(output))
	if outErr != nil {
		log.Printf("OUTPUT ERROR: %s", outErr.Error())
	}
	return output, outErr
}

// LogInputOutput is a LambdaMiddleware that logs input and output of a wrapped handler.
/*
	Input logging:
		For input, we log the lambda context (if present), the os environment, and
		the input bytes (request payload).
	Output logging:
		For output, we log the returned bytes (response payload) and any returned error.
*/
var LogInputOutput = LambdaMiddleware{
	Enter: logLambdaInputs,
	Exit:  logLambdaOutputs,
}
