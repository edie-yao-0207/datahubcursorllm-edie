package databricksofficialprovider

import (
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/libs/ni/pointer"
)

func DatabricksWorkspaceProvider(databricksHostname string) []tf.Resource {
	tokenVar := &genericresource.StringVariable{
		Name:    "databricks_token",
		Default: `""`,
	}

	provider := &genericresource.Provider{
		Name:     "databricks",
		Host:     pointer.StringPtr(databricksHostname),
		Token:    pointer.StringPtr(tokenVar.ResourceId().Reference()),
		AuthType: pointer.StringPtr(dataplatformconfig.DatabricksWorkspaceProviderAuthTypeToken),
	}

	return []tf.Resource{
		tokenVar,
		provider,
	}
}

func DatabricksOauthWorkspaceProvider(databricksHostname string) []tf.Resource {

	clientIdVar := &genericresource.StringVariable{
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
		ClientId:     pointer.StringPtr(clientIdVar.ResourceId().Reference()),
		ClientSecret: pointer.StringPtr(clientSecretVar.ResourceId().Reference()),
		AuthType:     pointer.StringPtr(dataplatformconfig.DatabricksAccountProviderAuthTypeOAuth),
	}

	return []tf.Resource{
		clientIdVar,
		clientSecretVar,
		provider,
	}
}
