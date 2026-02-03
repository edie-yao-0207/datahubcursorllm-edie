package databricksoauthfx

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/security/secrets"
)

func NewV2FromConfigE2(appConfig *config.AppConfig) (*databricks.Client, error) {
	// if we're in a dev environment, we need to get the databricks e2 api token from the secrets manager.
	// Users can use their own PATs for local development.
	if appConfig.IsDevEnv() {
		databricksE2ApiTokenResp, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksE2ApiToken)
		if err != nil {
			return nil, oops.Wrapf(err, "Could not get %s", secrets.DatabricksE2ApiToken)
		}
		return databricks.New(appConfig.DatabricksE2UrlBase, databricksE2ApiTokenResp.StringValue)
	}

	// if we're in a non-dev environment, we need to get the databricks e2 client id and client secret from the secrets manager.
	// This will use oauth to get a token.
	clientID, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksE2ClientID)
	if err != nil {
		return nil, oops.Wrapf(err, "Could not get %s", secrets.DatabricksE2ClientID)
	}

	clientSecret, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksE2ClientSecret)
	if err != nil {
		return nil, oops.Wrapf(err, "Could not get %s", secrets.DatabricksE2ClientSecret)
	}

	// Initialize the databricks oauth client.
	client, err := databricks.NewV2(appConfig.DatabricksE2UrlBase, clientID.StringValue, clientSecret.StringValue)
	if err != nil {
		return nil, oops.Wrapf(err, "Could not create databricks oauth client in fx")
	}

	return client, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewV2FromConfigE2)
	fxregistry.MustRegisterDefaultConstructor(func(c *databricks.Client) databricks.API { return c })
}
