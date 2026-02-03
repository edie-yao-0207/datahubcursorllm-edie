package databricksofficialprovider

import (
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/libs/ni/pointer"
)

const DatabricksAccountHost = "https://accounts.cloud.databricks.com"
const DatabricksProviderName = "databricks"

func DatabricksAccountProvider() []tf.Resource {
	dbxClientIdVar := &genericresource.StringVariable{
		Name:    "databricks_client_id",
		Default: `""`,
	}

	dbxClientSecretVar := &genericresource.StringVariable{
		Name:    "databricks_client_secret",
		Default: `""`,
	}

	provider := &genericresource.Provider{
		Name:         DatabricksProviderName,
		Host:         pointer.StringPtr(DatabricksAccountHost),
		AccountId:    pointer.StringPtr(dataplatformconfig.DatabricksAccountId),
		ClientId:     pointer.StringPtr(dbxClientIdVar.ResourceId().Reference()),
		ClientSecret: pointer.StringPtr(dbxClientSecretVar.ResourceId().Reference()),
		AuthType:     pointer.StringPtr(dataplatformconfig.DatabricksAccountProviderAuthTypeOAuth),
	}

	return []tf.Resource{
		dbxClientIdVar,
		dbxClientSecretVar,
		provider,
	}
}
