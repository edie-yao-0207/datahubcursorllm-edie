package databricksfx

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/security/secrets"
)

func NewFromConfigE2(appConfig *config.AppConfig) (*databricks.Client, error) {
	databricksE2ApiTokenResp, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksE2ApiToken)
	if err != nil {
		return nil, oops.Wrapf(err, "Could not get %s", secrets.DatabricksE2ApiToken)
	}

	return databricks.New(appConfig.DatabricksE2UrlBase, databricksE2ApiTokenResp.StringValue)
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewFromConfigE2)
	fxregistry.MustRegisterDefaultConstructor(func(c *databricks.Client) databricks.API { return c })
}
