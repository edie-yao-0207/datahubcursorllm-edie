package godatalake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/appenv"
	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/security/secrets"
	"samsaradev.io/libs/ni/infraconsts"
)

// authType identifies the authentication method to use with Databricks. This
// type is used to determine how to construct the connection string and which
// credentials to use.
type authType string

const (
	// authTypePAT uses a Personal Access Token for authentication. This is
	// typically used for individual user access.
	authTypePAT authType = "pat"

	// authTypeOAuthU2M uses OAuth User-to-Machine authentication. This is used
	// for automated processes that need to act on behalf of a user.
	authTypeOAuthU2M authType = "oauth-u2m"

	// authTypeOAuthM2M uses OAuth Machine-to-Machine authentication. This is used
	// for service-to-service communication where no user context is needed.
	authTypeOAuthM2M authType = "oauth-m2m"
)

// DatabricksConfig implements ComputeConfig for Databricks. It contains
// the configuration needed to connect to a Databricks SQL warehouse.
type DatabricksConfig struct {
	// WarehouseID uniquely identifies the Databricks SQL warehouse to connect to.
	// This ID can be found in the Databricks workspace UI or via the API.
	WarehouseID string

	// AuthType determines how the client will authenticate with Databricks. Valid
	// values are defined by the authType constants.
	AuthType authType
}

// Type returns the compute type for this configuration. Always returns
// ComputeDatabricks as this is specific to Databricks connections.
func (c DatabricksConfig) Type() ComputeType {
	return ComputeDatabricks
}

// validateDatabricksConfig performs validation specific to Databricks
// configuration.
func (c *Config) validateDatabricksConfig() error {
	dbConfig, ok := c.ComputeConfig.(DatabricksConfig)
	if !ok {
		return oops.Errorf("invalid compute config type: expected DatabricksConfig")
	}
	if dbConfig.WarehouseID == "" {
		return oops.Errorf("warehouse ID is required for Databricks connection")
	}
	if dbConfig.AuthType == "" {
		return oops.Errorf("auth type is required for Databricks connection")
	}

	// Validate that AuthType is one of the supported types.
	switch dbConfig.AuthType {
	case authTypePAT, authTypeOAuthU2M, authTypeOAuthM2M:
		// Valid auth type.
	default:
		return oops.Errorf("unsupported auth type: %s. Must be one of: %s, %s, or %s",
			dbConfig.AuthType,
			authTypePAT,
			authTypeOAuthU2M,
			authTypeOAuthM2M,
		)
	}

	// TODO: we should validate that the sql warehouse ID is valid:
	// https://github.com/samsara-dev/backend/pull/234883/commits/1b03b33ef78a399e0ea147d0ec278d410c6e93c3#r2060845775

	return nil
}

// getDatabricksHostForRegion returns the appropriate Databricks workspace URL
// for the current AWS region.
//
// The URL format is: samsara-dev-us-west-2.cloud.databricks.com Note: No
// https:// prefix or trailing slash.
//
// The function determines the appropriate workspace URL based on the AWS region
// where the code is running. It supports the default, EU, and CA regions.
func getDatabricksHostForRegion() (string, error) {
	region := samsaraaws.GetAWSRegion()
	// Set the region to the default region if we're running in local dev, eg
	// godatalakeaccessor.
	if samsaraaws.IsRunningInLocalDev() {
		region = infraconsts.SamsaraAWSDefaultRegion
	}
	if region == "" {
		return "", errors.New("failed to determine AWS region")
	}

	var baseURL string

	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		baseURL = infraconsts.SamsaraDevDatabricksWorkspaceURLBase
	case infraconsts.SamsaraAWSEURegion:
		baseURL = infraconsts.SamsaraEUDevDatabricksWorkspaceURLBase
	case infraconsts.SamsaraAWSCARegion:
		baseURL = infraconsts.SamsaraCADevDatabricksWorkspaceURLBase
	default:
		return "", fmt.Errorf("unsupported AWS region for Databricks: %s", region)
	}

	// Remove https:// prefix and trailing slash if present.
	baseURL = strings.TrimPrefix(baseURL, "https://")
	baseURL = strings.TrimSuffix(baseURL, "/")

	return baseURL, nil
}

// getDatabricksPatToken retrieves the personal access token for Databricks
// authentication. In dev mode, it checks for the dbx_personal_access_token
// environment variable. Otherwise, the token is fetched from the secrets service.
func (c *Config) getDatabricksPatToken() (string, error) {
	clientSecretResp, err := c.SecretService.GetSecretValueFromKey(secrets.DatabricksE2ApiToken)
	if err != nil {
		return "", oops.Wrapf(err, "Could not get %s", secrets.DatabricksE2ApiToken)
	}

	return clientSecretResp.StringValue, nil
}

// getDatabricksServicePrincipalIdAndSecret retrieves the service principal ID
// and secret for Databricks OAuth M2M authentication. Both values are fetched
// from the secrets service using predefined keys.
func (c *Config) getDatabricksServicePrincipalIdAndSecret() (string, string, error) {
	clientSecretIdResp, err := c.SecretService.GetSecretValueFromKey(secrets.DatabricksGoDataLakeClientId)
	if err != nil {
		return "", "", oops.Wrapf(err, "failed to get client id from secrets")
	}
	clientSecretResp, err := c.SecretService.GetSecretValueFromKey(secrets.DatabricksGoDataLakeClientSecret)
	if err != nil {
		return "", "", oops.Wrapf(err, "failed to get client secret from secrets")
	}

	return clientSecretIdResp.StringValue, clientSecretResp.StringValue, nil
}

// DatabricksClientParams contains all parameters needed to create a new
// Databricks client. This struct ensures all necessary configuration is
// provided when creating a new client.
type DatabricksClientParams struct {
	Config        DatabricksConfig
	SecretService secrets.SecretService
}

// databricksClient implements the Client interface for Databricks. It maintains
// a connection to a Databricks SQL warehouse and stores the configuration used
// to establish that connection.
type databricksClient struct {
	db     *sql.DB // Connection to Databricks SQL warehouse
	config Config  // Store the full config for access to SecretService etc.
}

// convertArgsToTypedParams converts int64 arguments to explicitly typed Databricks
// parameters with SqlBigInt type. This works around a driver limitation where int64
// values passed via interface{} are incorrectly inferred as INT (32-bit) instead of
// BIGINT (64-bit), causing overflow errors for large values like org IDs in EU.
// Note: The Databricks driver expects Parameter.Value to be a string representation
// when using explicit type hints.
// See: https://pkg.go.dev/github.com/databricks/databricks-sql-go#Parameter
func convertArgsToTypedParams(args []interface{}) []interface{} {
	result := make([]interface{}, len(args))
	for i, arg := range args {
		switch v := arg.(type) {
		case int64:
			result[i] = dbsql.Parameter{
				Type:  dbsql.SqlBigInt,
				Value: strconv.FormatInt(v, 10),
			}
		case int:
			result[i] = dbsql.Parameter{
				Type:  dbsql.SqlBigInt,
				Value: strconv.FormatInt(int64(v), 10),
			}
		default:
			result[i] = arg
		}
	}
	return result
}

// newDatabricksClient creates a new client connected to a Databricks SQL
// warehouse. It handles the authentication flow based on the configured auth
// type and establishes a connection to the appropriate warehouse. The
// connection is tested before being returned to ensure it's valid.
func (c *Config) newDatabricksClient() (Client, error) {
	// Cast ComputeConfig to DatabricksConfig to access AuthType.
	dbComputeConfig, ok := c.ComputeConfig.(DatabricksConfig)
	if !ok {
		return nil, oops.Errorf("invalid compute config type")
	}

	// Get the appropriate Databricks host for the region.
	host, err := getDatabricksHostForRegion()
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get Databricks host")
	}

	// Get the auth prefix based on the auth type. Follow this doc for the
	// formatting:
	// https://docs.databricks.com/aws/en/dev-tools/go-sql-driver#authentication
	var dsn string
	switch dbComputeConfig.AuthType {
	case authTypePAT:
		// Get auth configuration internally.
		token, err := c.getDatabricksPatToken()
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get Databricks authentication")
		}
		// Format: token:<personal-access-token>@<server-hostname>:<port-number>/<http-path>
		dsn = fmt.Sprintf("token:%s@%s:443/sql/1.0/warehouses/%s",
			token,
			host,
			dbComputeConfig.WarehouseID,
		)
	case authTypeOAuthU2M:
		// Format: <server-hostname>:<port-number>/<http-path>?authType=OauthU2M
		dsn = fmt.Sprintf("%s:443/sql/1.0/warehouses?authType=OauthU2M",
			host,
		)
	case authTypeOAuthM2M:
		// Get auth configuration internally.
		clientId, clientSecret, err := c.getDatabricksServicePrincipalIdAndSecret()
		if err != nil {
			return nil, oops.Wrapf(err, "failed to get Databricks service principal ID and secret")
		}
		// Format: <server-hostname>:<port-number>/<http-path>?authType=OAuthM2M&clientID=<client-id>&clientSecret=<client-secret>
		dsn = fmt.Sprintf("%s:443/sql/1.0/warehouses/%s?authType=OAuthM2M&clientID=%s&clientSecret=%s",
			host,
			dbComputeConfig.WarehouseID,
			clientId,
			clientSecret,
		)
	default:
		return nil, oops.Errorf("unsupported auth type: %s", dbComputeConfig.AuthType)
	}

	// Connect to Databricks SQL warehouse.
	db, err := sql.Open("databricks", dsn)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to connect to Databricks")
	}

	// Test the connection.
	// TODO: re-enable this once we have a way to test the connection safely or decouple from fx dependency injection.
	// if err := db.PingContext(context.Background()); err != nil {
	// 	db.Close()
	// 	return nil, oops.Wrapf(err, "failed to ping Databricks")
	// }

	return &databricksClient{
		db:     db,
		config: *c,
	}, nil
}

// QueryWithOpts implements the Client interface for Databricks. It executes the
// provided SQL query against the Databricks warehouse. If a warehouse override
// is provided in the options, it will create a new temporary connection to that
// warehouse for this query only.
func (c *databricksClient) QueryWithOpts(ctx context.Context, query string, args []interface{}, opts *QueryOptions) (Result, error) {
	// If we have query options with a warehouse override, create a new connection.
	if opts != nil && opts.WarehouseID != "" {
		// Create a new config with the overridden warehouse ID.
		newConfig := c.config
		dbConfig, ok := newConfig.ComputeConfig.(DatabricksConfig)
		if !ok {
			return nil, oops.Errorf("invalid compute config type")
		}
		dbConfig.WarehouseID = opts.WarehouseID
		newConfig.ComputeConfig = dbConfig

		// Create a new client with the overridden config.
		tempClient, err := newConfig.newDatabricksClient()
		if err != nil {
			return nil, oops.Wrapf(err, "failed to create temporary client with warehouse override")
		}
		defer tempClient.Close()

		// Execute the query using the temporary client, preserving queryIdentifier
		// but removing warehouse override to avoid recursion.
		tempOpts := &QueryOptions{
			QueryIdentifier: opts.QueryIdentifier,
		}
		return tempClient.QueryWithOpts(ctx, query, args, tempOpts)
	}

	typedArgs := convertArgsToTypedParams(args)

	// Execute the query using the default warehouse connection.
	queryStartTime := time.Now()
	rows, err := c.db.QueryContext(ctx, query, typedArgs...)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to execute query")
	}
	if rows.Err() != nil {
		return nil, oops.Wrapf(rows.Err(), "failed to execute query")
	}

	// Get warehouse ID and auth type for metrics
	dbConfig, _ := c.config.ComputeConfig.(DatabricksConfig)

	// Get query identifier from opts if provided
	var queryIdentifier QueryIdentifier
	if opts != nil && string(opts.QueryIdentifier) != "" {
		queryIdentifier = opts.QueryIdentifier
	}

	// Wrap the sql.Rows in our monitored Result interface.
	return &databricksResult{
		rows:                   rows,
		cumulativeBytes:        0,
		cumulativeScanDuration: 0,
		cumulativeNextDuration: 0,
		queryStartTime:         queryStartTime,
		firstRowTime:           nil,
		warehouseID:            dbConfig.WarehouseID,
		authType:               string(dbConfig.AuthType),
		queryIdentifier:        queryIdentifier,
	}, nil
}

// Query implements the Client interface for Databricks. It executes the
// provided SQL query against the default godatalake SQL warehouse.
func (c *databricksClient) Query(ctx context.Context, query string, args ...interface{}) (Result, error) {
	typedArgs := convertArgsToTypedParams(args)

	// Execute the query using the default warehouse connection.
	queryStartTime := time.Now()
	rows, err := c.db.QueryContext(ctx, query, typedArgs...)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to execute query")
	}
	if rows.Err() != nil {
		return nil, oops.Wrapf(rows.Err(), "failed to execute query")
	}

	// Get warehouse ID and auth type for metrics
	dbConfig, _ := c.config.ComputeConfig.(DatabricksConfig)

	// Wrap the sql.Rows in our monitored Result interface.
	return &databricksResult{
		rows:                   rows,
		cumulativeBytes:        0,
		cumulativeScanDuration: 0,
		cumulativeNextDuration: 0,
		queryStartTime:         queryStartTime,
		firstRowTime:           nil,
		warehouseID:            dbConfig.WarehouseID,
		authType:               string(dbConfig.AuthType),
		queryIdentifier:        QueryIdentifier(""), // No query identifier available in Query method
	}, nil
}

// Close implements the Client interface for Databricks. It closes the
// underlying database connection and releases any associated resources.
func (c *databricksClient) Close() error {
	return c.db.Close()
}

// databricksResult wraps sql.Rows to implement the Result interface and track response size metrics.
// It measures the cumulative byte size of data scanned from query results and
// emits metrics when the result is closed.
type databricksResult struct {
	rows                   *sql.Rows
	cumulativeBytes        int64
	cumulativeScanDuration time.Duration
	cumulativeNextDuration time.Duration
	queryStartTime         time.Time
	firstRowTime           *time.Time
	warehouseID            string
	authType               string
	queryIdentifier        QueryIdentifier
	totalQueryDuration     time.Duration
}

// Next implements the Result interface. It advances the cursor to the next row
// in the result set.
func (r *databricksResult) Next() bool {
	// Measure time spent in the Next() call (includes data fetching and parsing)
	nextStart := time.Now()
	hasNext := r.rows.Next()
	nextDuration := time.Since(nextStart)

	// Add to cumulative Next duration
	r.cumulativeNextDuration += nextDuration

	// Track when the first row becomes available
	if hasNext && r.firstRowTime == nil {
		firstRow := time.Now()
		r.firstRowTime = &firstRow
	}

	return hasNext
}

// Scan implements the Result interface and tracks the byte size of scanned data.
// It estimates the memory size of the data being scanned to track response size.
func (r *databricksResult) Scan(dest ...interface{}) error {
	// Measure time spent in the actual scan operation
	scanStart := time.Now()
	err := r.rows.Scan(dest...)
	scanDuration := time.Since(scanStart)

	// Add to cumulative scan duration
	r.cumulativeScanDuration += scanDuration

	if err != nil {
		return err
	}

	// Estimate the byte size of the scanned data
	for _, d := range dest {
		r.cumulativeBytes += estimateDataSize(d)
	}

	return nil
}

// emitDatadogMetrics emits datadog metrics for the given result.
// If you add a new metric, update the go/src/samsaradev.io/infra/dataplatform/godatalake/README.md file to document it.
func emitDatadogMetrics(r *databricksResult) {
	// Build base tags for metrics
	tags := []string{
		fmt.Sprintf("warehouse_id:%s", r.warehouseID),
		fmt.Sprintf("auth_type:%s", r.authType),
		"compute_type:databricks",
		fmt.Sprintf("app:%s", appenv.AppName()),
	}
	// Add service name tag if available
	if appenv.ServiceName != "" {
		tags = append(tags, fmt.Sprintf("service:%s", appenv.ServiceName))
	}

	// Add query identifier tag if provided
	if string(r.queryIdentifier) != "" {
		tags = append(tags, fmt.Sprintf("query_identifier:%s", string(r.queryIdentifier)))
	}

	// Emit response size metric
	monitoring.AggregatedDatadog.Gauge(
		float64(r.cumulativeBytes),
		"godatalake.query.response_size_bytes",
		tags...,
	)

	// Emit total query duration (what the user experiences)
	monitoring.AggregatedDatadog.Gauge(
		float64(r.totalQueryDuration.Milliseconds()),
		"godatalake.query.total_duration_ms",
		tags...,
	)

	// Emit time to first row (query execution + first batch)
	if r.firstRowTime != nil {
		timeToFirstRow := r.firstRowTime.Sub(r.queryStartTime)
		monitoring.AggregatedDatadog.Gauge(
			float64(timeToFirstRow.Milliseconds()),
			"godatalake.query.time_to_first_row_ms",
			tags...,
		)
	}

	// Emit cumulative Next duration (data fetching/parsing from database)
	monitoring.AggregatedDatadog.Gauge(
		float64(r.cumulativeNextDuration.Milliseconds()),
		"godatalake.query.next_duration_ms",
		tags...,
	)

	// Emit cumulative Scan duration (Go type conversion overhead)
	monitoring.AggregatedDatadog.Gauge(
		float64(r.cumulativeScanDuration.Milliseconds()),
		"godatalake.query.scan_duration_ms",
		tags...,
	)

	// Calculate and emit implied database execution time
	// This is roughly: total_time - next_time - scan_time = database_execution + network_overhead
	impliedDbTime := r.totalQueryDuration - r.cumulativeNextDuration - r.cumulativeScanDuration
	if impliedDbTime > 0 {
		monitoring.AggregatedDatadog.Gauge(
			float64(impliedDbTime.Milliseconds()),
			"godatalake.query.implied_db_execution_ms",
			tags...,
		)
	}

	// Calculate and emit implied overhead duration (all non-database time)
	// This is: next_time + scan_time = network_io + parsing + type_conversion
	impliedOverheadTime := r.cumulativeNextDuration + r.cumulativeScanDuration
	monitoring.AggregatedDatadog.Gauge(
		float64(impliedOverheadTime.Milliseconds()),
		"godatalake.query.implied_overhead_duration_ms",
		tags...,
	)
}

// Close implements the Result interface and emits datadog metrics before closing.
func (r *databricksResult) Close() error {
	queryEndTime := time.Now()
	r.totalQueryDuration = queryEndTime.Sub(r.queryStartTime)

	emitDatadogMetrics(r)

	return r.rows.Close()
}

// estimateDataSize estimates the memory footprint of a value in bytes.
// This gives us an approximation of the data size for response tracking.
func estimateDataSize(v interface{}) int64 {
	if v == nil {
		return 0
	}

	val := reflect.ValueOf(v)

	// Handle pointers by dereferencing
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return 0
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.String:
		return int64(len(val.String()))
	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 { // []byte
			return int64(val.Len())
		}
		// For other slice types, estimate based on element size
		return int64(val.Len()) * int64(val.Type().Elem().Size())
	case reflect.Array:
		return int64(val.Len()) * int64(val.Type().Elem().Size())
	default:
		// For basic types (int, float, bool, etc.), use their memory size
		return int64(val.Type().Size())
	}
}

// Columns implements the Result interface. It returns the names of the columns
// in the result set.
func (r *databricksResult) Columns() ([]string, error) {
	return r.rows.Columns()
}
