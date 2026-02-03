package middleware

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/samsarahq/go/oops"
)

// LambdaMiddleware defines a construct for onion wrapping lambda handlers
/*
	Before calling a handler, the middleware Enter function will be called. If
	the middleware returns an error, processing will skip the handler and the
	error will be returned. If Enter is successful, the output context and
	byte array will replace the input for passing to the wrapped handler.

	After returning from a handler, the Exit function will be called with the
	post-Enter context and the output byte array + error from the inner lambda
	function.
*/
type LambdaMiddleware struct {
	Enter func(context.Context, []byte) (context.Context, []byte, error)
	Exit  func(context.Context, []byte, error) ([]byte, error)
}

type wrappedHandler struct {
	handler    lambda.Handler
	middleware LambdaMiddleware
}

var _ lambda.Handler = (*wrappedHandler)(nil)

func (w *wrappedHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	if w.middleware.Enter != nil {
		wrappedContext, wrappedPayload, err := w.middleware.Enter(ctx, payload)
		if err != nil {
			return wrappedPayload, oops.Wrapf(err, "middleware error")
		}
		ctx, payload = wrappedContext, wrappedPayload
	}

	respPayload, respErr := w.handler.Invoke(ctx, payload)
	if w.middleware.Exit == nil {
		// skiplint: +nilerror
		return respPayload, respErr
	}
	return w.middleware.Exit(ctx, respPayload, respErr)

}

func wrapHandler(handler lambda.Handler, middleware ...LambdaMiddleware) lambda.Handler {
	h := handler
	for _, mw := range middleware {
		h = &wrappedHandler{
			handler:    h,
			middleware: mw,
		}
	}
	return h
}

// WrapNewHandler takes a handler function and optional middleware and returns a lambda.Handler
func WrapNewHandler(handlerFunc interface{}, middleware ...LambdaMiddleware) lambda.Handler {
	return wrapHandler(lambda.NewHandler(handlerFunc), middleware...)
}

// StartWrapped is a replacement for lambda.StartHandler that allows wrapping with middleware
func StartWrapped(handlerFunc interface{}, middleware ...LambdaMiddleware) {
	handler := WrapNewHandler(handlerFunc, middleware...)
	lambda.StartHandler(handler)
}
