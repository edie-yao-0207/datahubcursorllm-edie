package unitycatalog

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
)

func unityCatalogRecipientResources(providerGroup string) (map[string][]tf.Resource, error) {
	resourceGroups := map[string][]tf.Resource{}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Exit early in case we're not in the dev workspaces. Catalogs exist at a metastore level, but need to be created
	// from a workspace (a requirement of the terraform provider).
	if !dataplatformresource.IsE2ProviderGroup(config.DatabricksProviderGroup) {
		return nil, nil
	}

	var recipientResources []tf.Resource

	for _, recipient := range unitycatalog.RecipientRegistry {
		if !recipient.InRegion(config.Region) {
			continue
		}

		// Create the recipient
		recipientResource := &databricksresource_official.Recipient{
			Name:               recipient.Name,
			Owner:              recipient.Owner.DatabricksAccountGroupName(),
			AuthenticationType: recipient.AuthenticationType,
		}
		// set metastore ID if it exists
		if recipient.DataRecipientGlobalMetastoreId != "" {
			recipientResource.DataRecipientGlobalMetastoreId = recipient.DataRecipientGlobalMetastoreId
		}
		recipientResources = append(recipientResources, recipientResource)
	}
	// surface shares and share grants
	if len(recipientResources) > 0 {
		resourceGroups["recipients"] = recipientResources
	}

	return resourceGroups, nil
}
