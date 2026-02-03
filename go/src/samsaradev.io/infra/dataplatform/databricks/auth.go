package databricks

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"

	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/samsarahq/go/oops"
)

type databricksTokenState int

// databricksTokenState represents the state of the token. Each token can be in one of
// the following three states:
//   - fresh: The token is valid.
//   - stale: The token is valid but will expire soon.
//   - expired: The token has expired and cannot be used.
const (
	tokenFresh   databricksTokenState = iota // #0 The token is valid.
	tokenStale                               // #1 The token is valid but will expire soon.
	tokenExpired                             // #2 The token has expired and cannot be used.
)

const (
	expiryBuffer = 3 * time.Minute // We add a buffer of 3 mins from actual expiry.
)

// OauthResponse represents the response from the oauth endpoint.
type OauthResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// DatabricksOauthToken represents the oauth token for the databricks api.
type DatabricksOauthToken struct {
	// base url for the databricks api.
	AuthBaseEndpoint string
	// client id for the databricks api.
	ClientID string
	// client secret for the databricks api.
	ClientSecret string
	// Duration during which a token is considered stale, see tokenState.
	StaleDuration time.Duration

	httpClient *http.Client
	// A mutex to synchronize access to the token and expiry time.
	mu sync.Mutex

	// The token.
	cachedToken string

	// The time at which the token will expire.
	expiryTime time.Time
}

// NewDatabricksOauthToken creates a new DatabricksOauthToken.
// It initializes the token and expiry time.
// It also loads the token first time during client creation.
func NewDatabricksOauthToken(authBaseEndpoint string, clientID string, clientSecret string, staleDuration time.Duration) (*DatabricksOauthToken, error) {
	dot := &DatabricksOauthToken{
		AuthBaseEndpoint: authBaseEndpoint,
		ClientID:         clientID,
		ClientSecret:     clientSecret,
		StaleDuration:    staleDuration,
		httpClient:       &http.Client{},
	}

	// Load the token first time during client creation.
	err := dot.blockingTokenRefresh()
	if err != nil {
		return nil, oops.Wrapf(err, "error loading token during client creation")
	}
	return dot, nil
}

// tokenState represents the state of the token. Each token can be in one of
// the following three states:
//   - fresh: The token is valid.
//   - stale: The token is valid but will expire soon.
//   - expired: The token has expired and cannot be used.
//
// Token state through time:
//
//	issue time     expiry time
//	    v               v
//	    | fresh | stale | expired -> time
//	    |     valid     |
func (dot *DatabricksOauthToken) tokenState() databricksTokenState {
	if dot.getToken() == "" {
		log.Printf("Token is empty, returning expired")
		return tokenExpired
	}

	expiryTime := dot.getExpiryTime()
	now := time.Now()

	log.Printf("Token Details: currentTime: %v, expiryTime: %v", now, expiryTime)
	lifeSpan := expiryTime.Sub(now)
	log.Printf("Token life span: %v", lifeSpan)

	switch {
	case lifeSpan.Seconds() <= 0:
		return tokenExpired
	case lifeSpan.Seconds() <= dot.StaleDuration.Seconds():
		return tokenStale
	default:
		return tokenFresh
	}
}

// getToken returns the cached token.
func (dot *DatabricksOauthToken) getToken() string {
	dot.mu.Lock()
	defer dot.mu.Unlock()
	return dot.cachedToken
}

// getExpiryTime returns the expiry time of the token.
func (dot *DatabricksOauthToken) getExpiryTime() time.Time {
	dot.mu.Lock()
	defer dot.mu.Unlock()
	return dot.expiryTime
}

// setToken sets the token and expiry time.
func (dot *DatabricksOauthToken) setToken(token string, expiryTime time.Time) {
	dot.mu.Lock()
	defer dot.mu.Unlock()
	dot.cachedToken = token
	dot.expiryTime = expiryTime
}

// getOauthToken returns the token for the client.
// It checks the token state and returns the cached token if it is fresh.
// Otherwise, it triggers a background refresh of the token asynchronously.
func (dot *DatabricksOauthToken) getOauthToken() (string, error) {

	ts := dot.tokenState()
	t := dot.getToken()

	switch ts {
	case tokenFresh:
		log.Printf("Returning cached token")
		return t, nil
	case tokenStale:
		log.Printf("Token is stale(still valid but will expire soon), triggering async refresh and returning cached token")
		dot.triggerAsyncTokenRefresh()
		return t, nil
	default:
		// expired.
		log.Printf("Token is expired, refreshing token synchronously")
		err := dot.blockingTokenRefresh()
		if err != nil {
			log.Printf("error refreshing token synchronously: %v", err)
			return "", oops.Wrapf(err, "error refreshing token synchronously")
		}
		return dot.getToken(), nil
	}
}

// calculateExpiryTime calculates the expiry time of the token based on the response from the oauth endpoint.
// It adds a buffer of 3 minutes to the expiry time.
func (dot *DatabricksOauthToken) calculateExpiryTime(t *OauthResponse) time.Time {
	return time.Now().Add(time.Duration(t.ExpiresIn)*time.Second - time.Duration(expiryBuffer))
}

// triggerAsyncTokenRefresh triggers a background refresh of the token asynchronously.
// It recovers from any panics and logs them as errors.
// It is called when the token is stale and a new token is needed for further requests.
func (dot *DatabricksOauthToken) triggerAsyncTokenRefresh() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Panic hit in async token refresh: %v", r)
			}
		}()

		err := dot.blockingTokenRefresh()
		if err != nil {
			log.Printf("Error refreshing token asynchronously: %v", err)
			return
		}
		log.Printf("Token successfully refreshed asynchronously")
	}()
}

// blockingTokenRefresh refreshes the token synchronously.
// It is called when the token is expired fully and needs to be refreshed.
func (dot *DatabricksOauthToken) blockingTokenRefresh() error {

	tokenEndpoint := dot.AuthBaseEndpoint + "/oidc/v1/token"

	// We are logging the token endpoint here, which contains no sensitive data.
	// skiplint: +slogsensitivedata
	log.Printf("Making request to fetch new oauth token: %v", tokenEndpoint)

	auth := base64.StdEncoding.EncodeToString([]byte(dot.ClientID + ":" + dot.ClientSecret))

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("scope", "all-apis")

	req, err := http.NewRequest("POST", tokenEndpoint, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return oops.Wrapf(err, "error creating request while refreshing token")
	}

	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := dot.httpClient.Do(req)
	if err != nil {
		return oops.Wrapf(err, "error sending request while refreshing token")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return oops.Wrapf(err, "error reading response while refreshing token")
	}

	var oauthResp OauthResponse
	err = json.Unmarshal(body, &oauthResp)
	if err != nil {
		return oops.Wrapf(err, "error unmarshaling response while refreshing token")
	}

	dot.setToken(oauthResp.AccessToken, dot.calculateExpiryTime(&oauthResp))

	return nil
}
