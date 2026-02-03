package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/samsarahq/go/oops"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/dataplatform/lambdafunctions/util/middleware"
)

type testInput struct {
	InValue string `json:"in_value"`
}

type testOutput struct {
	OutValue string `json:"out_value"`
}

type traceKey string

type tracer struct {
	entries []string
}

func addTrace(ctx context.Context, trace string) (context.Context, *tracer) {
	ctxTracer := &tracer{}
	if existingTracer := ctx.Value(traceKey("trace")); existingTracer != nil {
		ctxTracer = existingTracer.(*tracer)
	}
	ctxTracer.entries = append(ctxTracer.entries, trace)
	return context.WithValue(ctx, traceKey("trace"), ctxTracer), ctxTracer
}

func handlerFunc(ctx context.Context, input *testInput) (*testOutput, error) {
	_, _ = addTrace(ctx, "handler func")
	if input.InValue == "badinput" {
		return nil, errors.New("receivedbadinput")
	}
	return &testOutput{OutValue: fmt.Sprintf("out:%s", input.InValue)}, nil
}

func TestWrapNewWithoutMiddleware(t *testing.T) {
	ctx, tracer := addTrace(context.Background(), "tracing")
	payload := []byte(`{"in_value":"stuff"}`)
	handler := middleware.WrapNewHandler(handlerFunc)
	output, err := handler.Invoke(ctx, payload)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"out_value":"out:stuff"}`), output)
	assert.Equal(t, []string{"tracing", "handler func"}, tracer.entries)
}

func TestNothingMiddleware(t *testing.T) {
	ctx, tracer := addTrace(context.Background(), "tracing")
	payload := []byte(`{"in_value":"stuff"}`)
	handler := middleware.WrapNewHandler(handlerFunc, middleware.LambdaMiddleware{})
	output, err := handler.Invoke(ctx, payload)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"out_value":"out:stuff"}`), output)
	assert.Equal(t, []string{"tracing", "handler func"}, tracer.entries)
}

var testMiddleware = middleware.LambdaMiddleware{
	Enter: func(ctx context.Context, input []byte) (context.Context, []byte, error) {
		_, _ = addTrace(ctx, "enter middleware")
		if string(input) == "replaceme" {
			_, _ = addTrace(ctx, "replace input")
			return ctx, []byte(`{"in_value":"surprise"}`), nil
		}
		if string(input) == "middlebad" {
			_, _ = addTrace(ctx, "enter middleware error")
			return ctx, []byte("receivedbadmiddle"), errors.New("bad middleware")
		}
		return ctx, input, nil
	},
	Exit: func(ctx context.Context, innerOutput []byte, innerErr error) ([]byte, error) {
		_, _ = addTrace(ctx, "exit middleware")
		if innerErr != nil {
			_, _ = addTrace(ctx, "saw inner err")
			return innerOutput, innerErr
		}
		return append([]byte("!"), innerOutput...), nil
	},
}

func TestMiddlewareTracing(t *testing.T) {
	handler := middleware.WrapNewHandler(handlerFunc, testMiddleware)
	testCases := []struct {
		input          string
		expectedOutput string
		expectedError  string
		expectedTrace  []string
	}{
		{
			input:          `{"in_value":"stuff"}`,
			expectedOutput: `!{"out_value":"out:stuff"}`,
			expectedTrace:  []string{"tracing", "enter middleware", "handler func", "exit middleware"},
		},
		{
			input:          "replaceme",
			expectedOutput: `!{"out_value":"out:surprise"}`,
			expectedTrace:  []string{"tracing", "enter middleware", "replace input", "handler func", "exit middleware"},
		},
		{
			input:          "middlebad",
			expectedOutput: "receivedbadmiddle",
			expectedError:  "bad middleware",
			expectedTrace:  []string{"tracing", "enter middleware", "enter middleware error"},
		},
		{
			input:          `{"in_value":"badinput"}`,
			expectedOutput: "",
			expectedError:  "receivedbadinput",
			expectedTrace:  []string{"tracing", "enter middleware", "handler func", "exit middleware", "saw inner err"},
		},
	}
	for _, tc := range testCases {
		tc := tc // capture variable: https://golang.org/doc/faq#closures_and_goroutines
		t.Run(tc.input, func(t *testing.T) {
			ctx, tracer := addTrace(context.Background(), "tracing")
			output, err := handler.Invoke(ctx, []byte(tc.input))
			if tc.expectedError != "" {
				assert.EqualError(t, oops.Cause(err), tc.expectedError)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectedOutput, string(output))
			assert.Equal(t, tc.expectedTrace, tracer.entries)
		})
	}
}
