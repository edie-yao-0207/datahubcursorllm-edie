package databricksdb

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"samsaradev.io/infra/dataplatform/samsara_thrift/thrift"
)

type ErrorLoggingHttpRoundTripper struct {
	inner http.RoundTripper
}

func (rt *ErrorLoggingHttpRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	if resp.StatusCode != http.StatusOK {
		// If status is not OK, read the body and throw a TTransportException that
		// includes the error message in the body. thrift.THttpClient will propagate
		// this error as is because the error is a TTransportException.
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("HTTP Response code: %d, Body: %s", resp.StatusCode, string(body)))
	}

	return resp, nil
}
