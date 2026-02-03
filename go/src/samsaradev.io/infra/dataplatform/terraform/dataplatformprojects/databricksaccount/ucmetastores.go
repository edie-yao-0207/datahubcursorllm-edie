package databricksaccount

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func unityCatalogMetastoreResources() (map[string][]tf.Resource, error) {
	resourceGroups := map[string][]tf.Resource{}

	// Create a resource for each region.
	// We use the "providerGroup" here since some downstream methods rely on it,
	// and we also want to set up workspace mappings, for which we need the workspace id.
	// But mainly its helping us iterate over all the regions.
	for _, providerGroup := range []string{
		dataplatformconfig.DatabricksDevUsProviderGroup,
		dataplatformconfig.DatabricksDevEuProviderGroup,
		dataplatformconfig.DatabricksDevCaProviderGroup,
	} {
		config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
		if err != nil {
			return nil, oops.Wrapf(err, "retrieving provider config")
		}
		prefix := awsregionconsts.RegionPrefix[config.Region]
		bucketname := prefix + dataplatformprojects.MetastoreBucketName
		lifetimeInSeconds := 0
		metastore := &databricksresource_official.Metastore{
			Name:              bucketname,
			StorageRoot:       fmt.Sprintf("s3://%s", bucketname),
			Region:            config.Region,
			DeltaSharingScope: "INTERNAL",
			DeltaSharingRecipientTokenLifetimeInSeconds: &lifetimeInSeconds,
			// Leaving owner blank as for now it's just databricks admins
			// Owner:             "",
		}

		role, err := dataplatformresource.IAMRoleArn(providerGroup, dataplatformprojects.MetastoreBucketRole)
		if err != nil {
			return nil, oops.Wrapf(err, "creating instance profile arn")
		}

		isDefault := true

		dataAccess := &databricksresource_official.MetastoreDataAccess{
			MetastoreId: metastore.ResourceId().ReferenceAttr("id"),
			Name:        bucketname + "-data-access",
			AwsIamRole:  databricksresource_official.AwsIamRole{RoleArn: role},
			IsDefault:   isDefault,
		}

		// is_default & read_only is deprecated and returns false for CA.
		// so just setting it to false to prevent resource recreation failures.
		if providerGroup == dataplatformconfig.DatabricksDevCaProviderGroup {
			dataAccess.BaseResource = tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					Lifecycle: tf.Lifecycle{
						IgnoreChanges: []string{"is_default", "read_only"},
					},
				},
			}
		}

		resourceGroups["metastores"] = append(resourceGroups["metastores"], metastore, dataAccess)

		// Assign metastores to workspaces.
		// This is only necessary for the US and EU workspaces; all other workspaces are created via terraform
		// and its automatically done in databricks_multi_workspaces.go
		if providerGroup == dataplatformconfig.DatabricksDevUsProviderGroup || providerGroup == dataplatformconfig.DatabricksDevEuProviderGroup {
			workspaceId := dataplatformconsts.GetDevDatabricksWorkspaceIdForRegion(config.Region)
			metastoreAssignment := &databricksresource_official.DatabricksMetastoreAssignment{
				ResourceName: fmt.Sprintf("%d_metastore_assignment", workspaceId),
				MetastoreID:  metastore.ResourceId().ReferenceAttr("id"),
				WorkspaceID:  fmt.Sprintf("%d", workspaceId),
			}
			resourceGroups["metastores"] = append(resourceGroups["metastores"], metastoreAssignment)
		}
	}

	return resourceGroups, nil
}
