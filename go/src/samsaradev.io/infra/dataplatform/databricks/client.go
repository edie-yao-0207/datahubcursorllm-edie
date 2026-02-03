package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jpillora/backoff"
	"github.com/samsarahq/go/oops"
	"golang.org/x/time/rate"
)

type Client struct {
	httpClient  http.Client
	urlBase     url.URL
	token       string
	rateLimiter *rate.Limiter

	// oauth token for the databricks api.
	// If this is nil, this client will use the PAT token.
	oauthToken *DatabricksOauthToken
}

type API interface { // lint: +sorted
	CloudCredentialAPI
	CloudCredentialPermissionsAPI
	ClustersAPI
	ClusterPoliciesAPI
	ExecutionContextAPI
	GlobalSqlEndpointsAPI
	GroupsAPI
	InstancePoolsAPI
	InstanceProfilesAPI
	IPAccessListAPI
	JobsAPI
	LibrariesAPI
	PermissionsAPI
	SqlDashboardsAPI
	SQLDashboardScheduleAPI
	SqlEndpointsAPI
	SqlStatementExecutionAPI
	SqlQueryAPI
	SqlQueryHistoryAPI
	SqlVisualizationsAPI
	SqlWidgetsAPI
	TokensAPI
	UsersAPI
	WorkspaceAPI
	WorkspaceConfAPI
	BaseURL() string
}

// convertJsonToGetParams recursively converts an arbitrary GET request payload
// in JSON decoded format to URL parameters accepted by Databricks.
func convertJsonToGetParams(src interface{}) (url.Values, error) {
	dest := make(url.Values)

	var convert func(k string, v interface{}) error
	convert = func(k string, v interface{}) error {
		switch v := v.(type) {
		case float64:
			dest.Add(k, strconv.FormatFloat(v, 'f', -1, 64))
		case string:
			dest.Add(k, v)
		case bool:
			dest.Add(k, strconv.FormatBool(v))
		case []interface{}:
			for _, elem := range v {
				if err := convert(k, elem); err != nil {
					return oops.Wrapf(err, "")
				}
			}
		case map[string]interface{}:
			for field, elem := range v {
				nestedKey := field
				if k != "" {
					nestedKey = k + "." + field
				}
				if err := convert(nestedKey, elem); err != nil {
					return oops.Wrapf(err, "")
				}
			}
		case nil: // An empty slice could be serialized and deserialized as nil.
			return nil
		default:
			return oops.Errorf("type %T in argument %s not supported in GET", v, k)
		}
		return nil
	}

	if err := convert("", src); err != nil {
		return nil, err
	}
	return dest, nil
}

type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e HTTPStatusError) Error() string {
	return fmt.Sprintf("HTTPStatusError(%d %s): %s", e.StatusCode, e.Status, e.Body)
}

const (
	ErrorCodeResourceDoesNotExist = "RESOURCE_DOES_NOT_EXIST"
	ErrorCodeDirectoryNotEmpty    = "DIRECTORY_NOT_EMPTY"
)

type APIError struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

func (e APIError) Error() string {
	return fmt.Sprintf("APIError(%s): %s", e.ErrorCode, e.Message)
}

func New(urlBase, token string) (*Client, error) {
	if urlBase == "" {
		return nil, oops.Errorf("URL for Databricks Host is empty. Please use a valid URL when setting up Databricks client.")
	}

	u, err := url.Parse(urlBase)
	if err != nil {
		return nil, oops.Wrapf(err, "url parse: %s", urlBase)
	}

	// Default limit is 30 req/s per workspace. Use up to half of that.
	rateLimiter := rate.NewLimiter(15, 1)

	return &Client{
		urlBase:     *u,
		token:       token,
		rateLimiter: rateLimiter,
	}, nil
}

// NewV2 creates a new Databricks client using the clientID and clientSecret for oauth.
func NewV2(urlBase, clientID, clientSecret string) (*Client, error) {
	if urlBase == "" {
		return nil, oops.Errorf("URL for Databricks Host is empty. Please use a valid URL when setting up Databricks client.")
	}

	u, err := url.Parse(urlBase)
	if err != nil {
		return nil, oops.Wrapf(err, "url parse: %s", urlBase)
	}

	if clientID == "" || clientSecret == "" {
		return nil, oops.Errorf("clientID and clientSecret must be set when using oauth")
	}

	// Default limit is 30 req/s per workspace. Use up to half of that.
	rateLimiter := rate.NewLimiter(15, 1)

	// The stale duration is 20 minutes. Which means that the token will be refreshed in the last 20 minutes of its life.
	// This is to ensure that we are not refreshing the token too often.
	// The oauth token is valid for 1 hour.
	oauthToken, err := NewDatabricksOauthToken(u.String(), clientID, clientSecret, 20*time.Minute)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating oauth token client")
	}

	client := &Client{
		urlBase:     *u,
		rateLimiter: rateLimiter,
		oauthToken:  oauthToken,
	}

	return client, nil
}

func (c *Client) BaseURL() string {
	return c.urlBase.String()
}

// getToken returns the token for the client.
// If the client is using oauth, it will return the oauth token.
// Otherwise, it will return the PAT token.
func (c *Client) getToken() (string, error) {
	if c.oauthToken != nil {
		if c.oauthToken.ClientID == "" || c.oauthToken.ClientSecret == "" {
			return "", oops.Errorf("clientID and clientSecret must be set when using oauth")
		}
		return c.oauthToken.getOauthToken()
	}
	return c.token, nil
}

func (c *Client) url(endpoint string) string {
	url := c.urlBase
	url.Path = path.Join(url.Path, endpoint)
	return url.String()
}

func (c *Client) do(ctx context.Context, method, endpoint string, payload interface{}, resp interface{}) error {
	const maxRetries = 10
	backoff := backoff.Backoff{
		Min:    2 * time.Second,
		Max:    15 * time.Second,
		Factor: 2,
		Jitter: true,
	}
	for attempt := 0; ; attempt++ {
		err := c.doWithoutRetry(ctx, method, endpoint, payload, resp)
		if err == nil {
			return nil
		}

		if attempt >= maxRetries {
			// skiplint: +nilerror
			return oops.Wrapf(err, "gave up after %d tries", maxRetries)
		}

		inner := oops.Cause(err)
		retryable := false

		// If we make a lot of concurrent requests to the databricks API, we are often getting
		// "unexpected EOF" errors. While we are not sure exactly why this is happening and whether
		// the problem is on our end or on databricks's end, we should consider these as retryable.
		// (This happens a lot from terraform where many requests are made in parallel).
		if strings.Contains(inner.Error(), "unexpected EOF") {
			retryable = true
		} else if httpErr, ok := inner.(*HTTPStatusError); ok && httpErr.StatusCode == http.StatusTooManyRequests {
			retryable = true
		} else if apiErr, ok := inner.(*APIError); ok && (apiErr.ErrorCode == "REQUEST_LIMIT_EXCEEDED" || apiErr.ErrorCode == "TEMPORARILY_UNAVAILABLE" || apiErr.ErrorCode == "RESOURCE_EXHAUSTED") {
			// Consider rate limiting and "temporarily unavailable/resource exhausted" as retryable errors.
			retryable = true
		}

		if !retryable {
			// skiplint: +nilerror
			return oops.Wrapf(err, "not retryable")
		}

		select {
		case <-time.After(backoff.Duration()):
		case <-ctx.Done():
			return oops.Wrapf(err, "canceled after %d tries", attempt+1)
		}
	}
}

func (c *Client) doWithoutRetry(ctx context.Context, method, endpoint string, payload interface{}, resp interface{}) error {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return oops.Errorf("bad method: %s", method)
	}

	scim := strings.Contains(endpoint, "/scim/")

	var body []byte
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return oops.Wrapf(err, "json marshal")
		}
	}

	var params string
	if len(body) != 0 && method == http.MethodGet {
		// Turn JSON payload to URL query parameters.
		var args map[string]interface{}
		if err := json.Unmarshal(body, &args); err != nil {
			return oops.Wrapf(err, "")
		}

		values, err := convertJsonToGetParams(args)
		if err != nil {
			return oops.Wrapf(err, "")
		}
		params = values.Encode()
	}

	var bodyReader io.Reader
	if len(body) != 0 {
		bodyReader = bytes.NewReader(body)
	}

	url := c.url(endpoint)
	if params != "" {
		url += "?" + params
	}
	httpReq, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return oops.Wrapf(err, "url: %s", url)
	}
	httpReq = httpReq.WithContext(ctx)
	token, err := c.getToken()

	if err != nil {
		return oops.Wrapf(err, "failed to get token for url: %s", url)
	}

	httpReq.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	if scim {
		httpReq.Header.Add("Accept", "application/scim+json")
		if bodyReader != nil {
			httpReq.Header.Add("Content-Type", "application/scim+json")
		}
	}

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return oops.Wrapf(err, "url: %s", url)
	}
	defer httpResp.Body.Close()

	body, err = ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return oops.Wrapf(err, "")
	}

	switch httpResp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
	default:
		var apiError APIError
		if err := json.Unmarshal(body, &apiError); err != nil || apiError.ErrorCode == "" {
			err := &HTTPStatusError{
				Status:     httpResp.Status,
				StatusCode: httpResp.StatusCode,
				Body:       string(body),
			}

			if httpResp.StatusCode == http.StatusUnauthorized && token == "" {
				return oops.Wrapf(err, "token not set")
			}
			return oops.Wrapf(err, "url: %s", url)
		}
		return &apiError
	}

	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, resp); err != nil {
		return oops.Wrapf(err, "parse body from url %s: %s", url, string(body))
	}
	return nil
}

var _ API = &Client{}
