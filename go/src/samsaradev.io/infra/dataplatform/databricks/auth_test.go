package databricks

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDatabricksOauthToken_tokenState(t *testing.T) {
	type fields struct {
		staleDuration time.Duration
		cachedToken   string
		expiryTime    time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   databricksTokenState
	}{
		{
			name: "fresh token",
			fields: fields{
				staleDuration: 30 * time.Minute,
				cachedToken:   "test_token",
				expiryTime:    time.Now().Add(time.Hour), // Expiry is1 hour from now and staleDuration is 30 minutes so should be fresh.
			},
			want: tokenFresh,
		},
		{
			name: "stale token",
			fields: fields{
				staleDuration: 20 * time.Minute,
				cachedToken:   "test_token",
				expiryTime:    time.Now().Add(19 * time.Minute), // Expiry is 19 minutes from now and staleDuration is 20 minutes so should be stale.
			},
			want: tokenStale,
		},
		{
			name: "expired token",
			fields: fields{
				staleDuration: 30 * time.Minute,
				cachedToken:   "test_token",
				expiryTime:    time.Now().Add(-time.Hour), // Expiry is 1 hour ago so should be expired.
			},
			want: tokenExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dot := &DatabricksOauthToken{
				StaleDuration: tt.fields.staleDuration,
				cachedToken:   tt.fields.cachedToken,
				expiryTime:    tt.fields.expiryTime,
			}

			if got := dot.tokenState(); got != tt.want {
				t.Errorf("DatabricksOauthToken.tokenState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetOauthToken(t *testing.T) {
	tests := []struct {
		name          string
		initialToken  string
		expiryTime    time.Time
		staleDuration time.Duration
		expectedToken string
	}{
		{
			name:          "no initial token returns new token",
			initialToken:  "",
			expiryTime:    time.Now().Add(30 * time.Minute),
			staleDuration: 5 * time.Minute,
			expectedToken: "new-token",
		},
		{
			name:          "A fresh token returns from cache",
			initialToken:  "fresh-token",
			expiryTime:    time.Now().Add(30 * time.Minute),
			staleDuration: 5 * time.Minute,
			expectedToken: "fresh-token",
		},
		{
			name:          "stale token triggers async refresh but returns cached stale-token",
			initialToken:  "stale-token",
			expiryTime:    time.Now().Add(4 * time.Minute),
			staleDuration: 5 * time.Minute,
			expectedToken: "stale-token",
		},
		{
			name:          "expired token performs sync refresh",
			initialToken:  "expired-token",
			expiryTime:    time.Now().Add(-1 * time.Minute),
			staleDuration: 5 * time.Minute,
			expectedToken: "new-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/oidc/v1/token", r.URL.Path)
				require.Equal(t, http.MethodPost, r.Method)
				fmt.Fprintln(w, `{"access_token":"new-token","token_type":"Bearer","expires_in":3600}`)

			}))
			defer ts.Close()

			dot := &DatabricksOauthToken{
				StaleDuration:    tt.staleDuration,
				cachedToken:      tt.initialToken,
				expiryTime:       tt.expiryTime,
				httpClient:       &http.Client{},
				AuthBaseEndpoint: ts.URL,
				ClientID:         "test_client_id",
				ClientSecret:     "test_client_secret",
			}

			token, err := dot.getOauthToken()
			if err != nil {
				t.Errorf("getOauthToken() error = %v", err)
				return
			}

			if token != tt.expectedToken {
				t.Errorf("getOauthToken() got = %v, want %v", token, tt.expectedToken)
			}

		})
	}
}
