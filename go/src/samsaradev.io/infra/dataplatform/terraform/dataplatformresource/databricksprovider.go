package dataplatformresource

import (
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/libs/ni/pointer"
)

func DatabricksProvider(databricksHostname string) []tf.Resource {
	tokenVar := &genericresource.StringVariable{
		Name:    "databricks_token",
		Default: `""`,
	}

	provider := &genericresource.Provider{
		Name:  "databricks",
		Host:  pointer.StringPtr(databricksHostname),
		Token: pointer.StringPtr(tokenVar.ResourceId().Reference()),
	}

	return []tf.Resource{
		tokenVar,
		provider,
	}
}

// DatabricksOauthProvider creates a Databricks provider with OAuth credentials.
func DatabricksOauthProvider(databricksHostname string) []tf.Resource {

	tokenVar := &genericresource.StringVariable{
		Name:    "databricks_token",
		Default: `""`,
	}

	clientIDVar := &genericresource.StringVariable{
		Name:    "databricks_ci_client_id",
		Default: `""`,
	}

	clientSecretVar := &genericresource.StringVariable{
		Name:    "databricks_ci_client_secret",
		Default: `""`,
	}

	provider := &genericresource.Provider{
		Name:         "databricks",
		Host:         pointer.StringPtr(databricksHostname),
		Token:        pointer.StringPtr(tokenVar.ResourceId().Reference()),
		ClientId:     pointer.StringPtr(clientIDVar.ResourceId().Reference()),
		ClientSecret: pointer.StringPtr(clientSecretVar.ResourceId().Reference()),
	}

	return []tf.Resource{
		provider,
		tokenVar,
		clientIDVar,
		clientSecretVar,
	}
}
