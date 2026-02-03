// Package godatalake provides a client for interacting with data lake compute
// platforms.
//
// AS OF 2025-06-05, THIS PACKAGE DOES NOT WORK ON SERVICES RUNNING ON EC2
// INSTANCES.
//
// This is because Databricks requires us to add the IP of the
// service to an allowlist, and EC2 instances are auto-assigned IPs so we can't
// reliably add them to the allowlist.
package godatalake

import (
	"context"
	"os"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/security/secrets"
)

// QueryOptions contains optional parameters for query execution.
type QueryOptions struct {
	// WarehouseID optionally overrides the default warehouse ID for this query.
	// If not specified, the warehouse ID from the config will be used.
	WarehouseID string

	// QueryIdentifier optionally provides a custom identifier for this query
	// that will be included as a tag in Datadog metrics. This helps identify
	// specific queries in monitoring dashboards.
	QueryIdentifier QueryIdentifier
}

// Client interface defines methods for interacting with data lake compute
// platforms.
type Client interface {
	// Query executes a query using the configured compute platform and returns
	// the results. Parameters can be provided to prevent SQL injection attacks.
	// Parameters are represented as ? placeholders in the query string.
	Query(ctx context.Context, query string, args ...interface{}) (Result, error)
	// QueryWithOpts executes a query using the configured compute platform and returns
	// the results. Parameters can be provided to prevent SQL injection attacks.
	// Parameters are represented as ? placeholders in the query string.
	// The opts parameter is optional and can be nil to use default configurations.
	QueryWithOpts(ctx context.Context, query string, args []interface{}, opts *QueryOptions) (Result, error)
	// Close releases all resources associated with this client.
	Close() error
}

// ComputeType identifies which compute platform to use.
type ComputeType string

const (
	ComputeDatabricks ComputeType = "databricks"
	// Future compute platforms can be added here.
	// ComputeSparkK8s  ComputeType = "spark-k8s"
	// ComputeTrino     ComputeType = "trino"
)

// Result provides methods to iterate over and access query results.
type Result interface {
	// Scan copies the columns in the current row into the provided destination
	// variables. The number and types of destination variables must match the
	// query's result columns.
	Scan(dest ...interface{}) error
	// Next advances to the next row in the result set, returning false when no
	// more rows exist.
	Next() bool
	// Close releases the resources associated with this result set.
	Close() error
	// Columns returns the names of the columns in the result set.
	Columns() ([]string, error)
}

// ComputeConfig defines how to connect to and configure a specific compute
// platform. Each platform (Databricks, Spark, Trino) implements this interface
// with its own connection and authentication details.
type ComputeConfig interface {
	// Type returns which compute platform this config is for.
	Type() ComputeType
}

// Config specifies how to connect to and interact with a data lake.
type Config struct {
	// Compute specifies which platform to use for query execution and data
	// processing (e.g., "databricks", "spark-k8s", "trino"). If not specified,
	// defaults to ComputeDatabricks.
	Compute ComputeType

	// ComputeConfig contains platform-specific connection and authentication
	// details. If not specified and Compute is ComputeDatabricks (or empty),
	// defaults to DatabricksComputeConfig with warehouse ID from appConfig.
	ComputeConfig ComputeConfig

	// AppConfig provides application configuration values. This is used to get
	// default values like warehouse ID if not explicitly specified.
	AppConfig *config.AppConfig

	// SecretService is used to access secrets for authentication
	SecretService secrets.SecretService
}

// Validate performs validation of the entire configuration, including compute-specific validation.
func (c *Config) Validate() error {
	if c.ComputeConfig == nil {
		return oops.Errorf("compute config is required")
	}
	if c.ComputeConfig.Type() != c.Compute {
		return oops.Errorf("compute config type %q does not match specified type %q",
			c.ComputeConfig.Type(), c.Compute)
	}
	if c.AppConfig == nil {
		return oops.Errorf("app config is required")
	}
	if c.SecretService == nil {
		return oops.Errorf("secret service is required")
	}

	// Perform compute-specific validation
	switch c.Compute {
	case ComputeDatabricks:
		if err := c.validateDatabricksConfig(); err != nil {
			return oops.Wrapf(err, "invalid Databricks configuration")
		}
	default:
		return oops.Errorf("unsupported compute type: %s", c.Compute)
	}

	return nil
}

// ConfigOverrides allows overriding specific configuration values.
type ConfigOverrides struct {
	// Compute optionally overrides which platform to use for query execution.
	Compute ComputeType `optional:"true"`
	// WarehouseID optionally overrides the default warehouse ID.
	WarehouseID string `optional:"true"`
	// AuthType optionally overrides the default authentication type.
	AuthType authType `optional:"true"`
}

func buildGoDataLakeConfig(c *Config) *Config {
	// If the compute type is not specified, default to Databricks.
	if c.Compute == "" {
		c.Compute = ComputeDatabricks
	}

	// If the compute type is Databricks, ensure we have proper compute config.
	if c.Compute == ComputeDatabricks {
		warehouseId := c.AppConfig.DatabricksSqlWarehouseId
		authType := authTypeOAuthM2M

		// If ComputeConfig exists, try to get existing values.
		if c.ComputeConfig != nil {
			if dbConfig, ok := c.ComputeConfig.(DatabricksConfig); ok {
				if dbConfig.WarehouseID != "" {
					warehouseId = dbConfig.WarehouseID
				}
				if dbConfig.AuthType != "" {
					authType = dbConfig.AuthType
				}
			}
		}

		c.ComputeConfig = DatabricksConfig{
			WarehouseID: warehouseId,
			AuthType:    authType,
		}
	}

	return c
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
	// the following 2 registrations are required for the following reasons:
	// * Godatalake is a struct not an interface but its member Client is an interface so for
	//   Godatalake to be auto-initialized for testing, test constructor for Client is also required
	//   for mocking. "NewForTest" allows for dependency injection by taking in an instance of Client.
	// * There are 2 testing scenarios that makes these 2 constructors necessary:
	//     * Tests that tests an instance with a dependency on Godatalake and the code path directly
	//       uses Godatalake. In this case, only NewForTest constructor is required because Client
	//       will be auto-mocked with testloader.
	//     * Tests that auto-initializes an instance that depends on Godatalake but the code path does
	//       not actually use Godatalake. In this case, the test constructor Client comes in handy
	//       and does not force test writers to include Client mock in their test env variables.
	fxregistry.MustRegisterDefaultTestConstructor(NewForTest)
	fxregistry.MustRegisterDefaultTestConstructor(func() Client { return nil })
}

// Params contains the dependencies needed to create a godatalake client.
type Params struct {
	fx.In

	AppConfig     *config.AppConfig
	SecretService secrets.SecretService
	// Overrides allows overriding specific configuration values. This is optional
	// and if not provided, defaults will be used.
	Overrides *ConfigOverrides `optional:"true"`
}

// GoDataLake provides the godatalake client.
type GoDataLake struct {
	Client Client
}

func newClient(config *Config) (Client, error) {
	// Create the appropriate client based on the compute type.
	var client Client
	var err error
	switch config.Compute {
	case ComputeDatabricks:
		client, err = config.newDatabricksClient()
	default:
		return nil, oops.Errorf("unsupported compute type: %s", config.Compute)
	}
	if err != nil {
		return nil, err
	}

	// Validate the complete configuration after client creation
	if err := config.Validate(); err != nil {
		client.Close() // Clean up the client if validation fails
		return nil, oops.Wrapf(err, "invalid configuration")
	}

	return client, nil
}

func New(p Params) (*GoDataLake, error) {
	compute := ComputeDatabricks
	warehouseID := p.AppConfig.DatabricksSqlWarehouseId
	authType := authTypeOAuthM2M

	// shouldRunInDev indicates whether we should generate a client even if we're
	// not running in a production environment. This is useful for running Go
	// apps, like godatalakeaccessor, or testing changes locally.
	shouldRunInDev := os.Getenv("RUN_IN_DEV") == "true"
	if shouldRunInDev {
		// Set auth type to PAT in dev mode.
		authType = authTypePAT
	} else if !p.AppConfig.IsProductionEnv() {
		// Hack to avoid breaking tens of tests.
		return &GoDataLake{Client: nil}, nil
	}

	// Apply any overrides if provided
	if p.Overrides != nil {
		if p.Overrides.Compute != "" {
			compute = p.Overrides.Compute
		}
		if p.Overrides.WarehouseID != "" {
			warehouseID = p.Overrides.WarehouseID
		}
		if p.Overrides.AuthType != "" {
			authType = p.Overrides.AuthType
		}
	}

	var computeConfig ComputeConfig
	switch compute {
	case ComputeDatabricks:
		computeConfig = DatabricksConfig{
			WarehouseID: warehouseID,
			AuthType:    authType,
		}
	default:
		return nil, oops.Errorf("unsupported compute type: %s", compute)
	}

	config := &Config{
		Compute:       compute,
		ComputeConfig: computeConfig,
		AppConfig:     p.AppConfig,
		SecretService: p.SecretService,
	}

	client, err := newClient(config)
	if err != nil {
		return nil, err
	}

	return &GoDataLake{Client: client}, nil
}

func NewForTest(c Client) *GoDataLake {
	return &GoDataLake{c}
}
