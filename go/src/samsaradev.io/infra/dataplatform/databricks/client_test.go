package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestClientDo(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/foo", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "Bearer foo", r.Header.Get("Authorization"))
		body, err := ioutil.ReadAll(r.Body)

		require.NoError(t, err)
		require.JSONEq(t, `{"foo": "bar"}`, string(body))
		fmt.Fprintln(w, `{"foo": "bar"}`)
	}))
	defer ts.Close()

	c, err := New(ts.URL, "foo")
	require.NoError(t, err)

	var resp interface{}
	require.NoError(t, c.do(context.Background(), http.MethodPost, "/foo", map[string]string{"foo": "bar"}, &resp))
	require.Equal(t, map[string]interface{}(map[string]interface{}{"foo": "bar"}), resp)
}

func TestConvertParams(t *testing.T) {
	type testCase struct {
		input  interface{}
		params string
	}

	testCases := []testCase{
		{
			input: struct {
				Int64 int64 `json:"int64"`
			}{
				Int64: 1,
			},
			params: "int64=1",
		},
		{
			input: struct {
				Int64   int64   `json:"int64"`
				String  string  `json:"string"`
				Float64 float64 `json:"float64"`
				Array   []int64 `json:"array"`
				Bool    bool    `json:"bool"`
			}{
				Int64:   1,
				String:  "foo",
				Float64: 3.33333,
				Array:   []int64{1, 2, 3, 4},
				Bool:    true,
			},
			params: "array=1&array=2&array=3&array=4&bool=true&float64=3.33333&int64=1&string=foo",
		},
		{
			input: struct {
				Int64 int64 `json:"int64"`
				Inner struct {
					String string  `json:"string"`
					Array  []int64 `json:"array"`
				} `json:"inner"`
			}{
				Int64: 1,
				Inner: struct {
					String string  `json:"string"`
					Array  []int64 `json:"array"`
				}{
					String: "foo",
					Array:  []int64{1, 2},
				},
			},
			params: "inner.array=1&inner.array=2&inner.string=foo&int64=1",
		},
		{
			input: struct {
				Array []int64 `json:"array"`
			}{},
			params: "",
		},
	}

	for _, testCase := range testCases {
		b, err := json.Marshal(testCase.input)
		require.NoError(t, err)

		var args map[string]interface{}
		require.NoError(t, json.Unmarshal(b, &args))

		values, err := convertJsonToGetParams(args)
		require.NoError(t, err)

		require.Equal(t, testCase.params, values.Encode())
	}

}

func TestClientRetry(t *testing.T) {
	count := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		t.Logf("try #%d at %s", count, time.Now())
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	c, err := New(ts.URL, "foo")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	var resp interface{}
	require.Error(t, c.do(ctx, http.MethodPost, "/foo", map[string]string{"foo": "bar"}, &resp))
	require.True(t, count >= 5)
}

func TestClient_getToken(t *testing.T) {
	type fields struct {
		httpClient  http.Client
		urlBase     url.URL
		token       string
		rateLimiter *rate.Limiter
		oauthToken  *DatabricksOauthToken
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "PAT token",
			fields: fields{
				token: "foo-pat",
			},
			want: "foo-pat",
		},
		{
			name: "oauth token",
			fields: fields{
				oauthToken: &DatabricksOauthToken{
					cachedToken:  "foo-oauth",
					expiryTime:   time.Now().Add(time.Hour),
					ClientID:     "foo-client-id",
					ClientSecret: "foo-client-secret",
				},
			},
			want: "foo-oauth",
		},
		{
			name: "both token set then oauth token is used",
			fields: fields{
				token: "foo-pat",
				oauthToken: &DatabricksOauthToken{
					cachedToken:  "foo-oauth",
					expiryTime:   time.Now().Add(time.Hour),
					ClientID:     "foo-client-id",
					ClientSecret: "foo-client-secret",
				},
			},
			want: "foo-oauth",
		},
		{
			name: "oauth token but clientID and clientSecret not set",
			fields: fields{
				oauthToken: &DatabricksOauthToken{
					cachedToken:  "foo-oauth",
					expiryTime:   time.Now().Add(time.Hour),
					ClientID:     "",
					ClientSecret: "",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				httpClient:  tt.fields.httpClient,
				urlBase:     tt.fields.urlBase,
				token:       tt.fields.token,
				rateLimiter: tt.fields.rateLimiter,
				oauthToken:  tt.fields.oauthToken,
			}
			got, err := c.getToken()
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.getToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Client.getToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewV2(t *testing.T) {
	type args struct {
		clientID     string
		clientSecret string
	}

	tests := []struct {
		name       string
		args       args
		wantToken  string
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "valid",
			args: args{
				clientID:     "foo-client-id",
				clientSecret: "foo-client-secret",
			},
			wantToken: "new-token",
		},
		{
			name: "No clientID and clientSecret should throw error",
			args: args{
				clientID:     "",
				clientSecret: "",
			},
			wantErr:    true,
			wantErrMsg: "clientID and clientSecret must be set when using oauth",
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

			got, err := NewV2(ts.URL, tt.args.clientID, tt.args.clientSecret)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewV2() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				require.Equal(t, tt.wantToken, got.oauthToken.cachedToken)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrMsg)
			}
		})
	}
}
