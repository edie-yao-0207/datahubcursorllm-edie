package databricksaccount

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

type InternalOverride struct {
}

// Workspace Resources created at: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/terraform/dataplatformresource/workspace.go
// Metastore created at: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/unitycatalog/ucmetastores.go
// Expects you to input the resources created by above two links.
type MultiWorkspace struct {
	Name           string
	DeploymentName string //Part of URL as in https://<prefix>-<deployment-name>.cloud.databricks.com, note prefix is already defined at account level.
	Region         string

	DatabricksAccountId string
	CredentialRoleArn   string // cross acount role.
	StorageBucket       string
	VpcID               string
	SubnetIDs           []string // private subnetids.
	SecurityGroupIDs    []string

	CmkKeyAlias             string
	CmkKeyArn               string
	CmkUseCases             []string
	UnityCatalogMetastoreID string // fetch from https://accounts.cloud.databricks.com/data
	InternalOverrides       InternalOverride
}

var workspaceRegistry = map[string]MultiWorkspace{
	"canada_central_1": {
		Name:                "samsara-dev-ca-central-1",
		DeploymentName:      "dev-ca-central-1", // URL will be: https://samsara-dev-ca-central-1.cloud.databricks.com
		Region:              "ca-central-1",
		DatabricksAccountId: dataplatformconfig.DatabricksAccountId,
		CredentialRoleArn:   "arn:aws:iam::211125295193:role/databricks-cross-account-dev-ca-central-1-role",
		StorageBucket:       "samsara-ca-databricks-dev-ca-central-1-root",
		VpcID:               "vpc-01dada81f5c7efb54",
		SubnetIDs: []string{
			"subnet-03887a02bea59b93e",
			"subnet-059ed56dd645df112",
			"subnet-0598b5049a2c74987",
		},
		SecurityGroupIDs: []string{
			"sg-0f218bec5d6279476",
		},
		CmkKeyAlias:             "databricks-dev-ca-central-1",
		CmkKeyArn:               "arn:aws:kms:ca-central-1:211125295193:key/21f05f0e-b82d-4122-aba4-14225e4f2474",
		CmkUseCases:             []string{"MANAGED_SERVICES"},
		UnityCatalogMetastoreID: "175be941-32cd-46ef-9229-1f986aefe4b1", // https://accounts.cloud.databricks.com/data/175be941-32cd-46ef-9229-1f986aefe4b1/configurations
	},

	"biztech_us_west_2": {
		Name:                "samsara-biztech-us-west-2",
		DeploymentName:      "biztech-us-west-2", // URL will be: https://samsara-biztech-us-west-2.cloud.databricks.com
		Region:              "us-west-2",
		DatabricksAccountId: dataplatformconfig.DatabricksAccountId,
		CredentialRoleArn:   "arn:aws:iam::849340316486:role/bted-dbx-crossaccount-role",
		StorageBucket:       "bted-databricks-workspace-root",
		VpcID:               "vpc-00c9927190c2c1eb5",
		SubnetIDs: []string{
			"subnet-0d86ad885ca882c7f",
			"subnet-0a1685b9a2c3fb43d",
			"subnet-09992553b8118f1f5",
		},
		SecurityGroupIDs: []string{
			"sg-0223138c60c976e29",
		},
		CmkKeyAlias:             "databricks-biztech-us-west-2",
		CmkKeyArn:               "arn:aws:kms:us-west-2:849340316486:key/b431af66-6c4f-430d-9b18-ef5c83b5bee8",
		CmkUseCases:             []string{"MANAGED_SERVICES"},
		UnityCatalogMetastoreID: "a4ada860-2122-418f-ae38-e374e756bc04", // https://accounts.cloud.databricks.com/data/a4ada860-2122-418f-ae38-e374e756bc04/configurations
	},

	"dev_staging_us_west_2": {
		Name:                "samsara-dev-staging-us-west-2",
		DeploymentName:      "dev-staging-us-west-2", // URL will be: https://samsara-dev-staging-us-west-2.cloud.databricks.com
		Region:              "us-west-2",
		DatabricksAccountId: dataplatformconfig.DatabricksAccountId,
		CredentialRoleArn:   "arn:aws:iam::492164655156:role/databricks-cross-account-dev-staging-us-west-2-role",
		StorageBucket:       "samsara-databricks-dev-staging-us-west-2-root",
		VpcID:               "vpc-025ef738e0eebc670",
		SubnetIDs: []string{
			"subnet-0c7a8b231aa8b35c0",
			"subnet-03842c6ee70952043",
			"subnet-0c62d1c8a090164ab",
		},
		SecurityGroupIDs: []string{
			"sg-0bba02ded91dbcc6e",
		},
		CmkKeyAlias:             "databricks-dev-staging-us-west-2",
		CmkKeyArn:               "arn:aws:kms:us-west-2:492164655156:key/b8619822-9ed1-407c-bcb2-02cbb8b8c8b0",
		CmkUseCases:             []string{"MANAGED_SERVICES"},
		UnityCatalogMetastoreID: "a4ada860-2122-418f-ae38-e374e756bc04", // https://accounts.cloud.databricks.com/data/a4ada860-2122-418f-ae38-e374e756bc04/configurations
	},
}

// Create a Databricks Workspace by setting up
// 1. Databricks Credentials to be used by clusters.
// 2. Databricks Storage to be used.
// 3. Datbricks Network to use to spin up compute.
// 4. Assigns a Customer managed Key
// 5. Creates a Workspace
// 6. Attaches a UC metastore
// Since resources can exists in any AWS account, this assumes
// the resources in registry are created before hand.
func (mws *MultiWorkspace) Create() ([]tf.Resource, error) {

	// Create Credentials Configuration.
	credentials := &databricksresource_official.DatabricksMwsCredentials{
		CredentialsName: fmt.Sprintf("databricks-cross-account-%s-role", mws.Name),
		AccountID:       mws.DatabricksAccountId,
		RoleArn:         mws.CredentialRoleArn,
	}
	resources := []tf.Resource{}

	// Create Storage Configuration.
	storage := &databricksresource_official.DatabricksMwsStorageConfigurations{
		StorageConfigurationName: fmt.Sprintf("samsara-databricks-%s-root-storage", mws.Name),
		AccountID:                mws.DatabricksAccountId,
		BucketName:               mws.StorageBucket,
	}

	// Create Network Configurations.
	network := &databricksresource_official.DatabricksMwsNetworks{
		NetworkName:      fmt.Sprintf("%s-vpc", mws.Name),
		AccountID:        mws.DatabricksAccountId,
		VpcID:            mws.VpcID,
		SubnetIDs:        mws.SubnetIDs,
		SecurityGroupIDs: mws.SecurityGroupIDs,
	}

	// Create Managed Services CMK.
	awsKeyInfo := databricksresource_official.AwsKeyInfo{
		KeyAlias:  mws.CmkKeyAlias,
		KeyRegion: mws.Region,
		KeyArn:    mws.CmkKeyArn,
	}

	managedServicesCmk := &databricksresource_official.DatabricksMwsCustomerManagedKeys{
		ResourceName: mws.CmkKeyAlias,
		AccountID:    mws.DatabricksAccountId,
		UseCases:     mws.CmkUseCases,
		AwsKeyInfo:   &awsKeyInfo,
	}

	// Create workspace.
	workspace := &databricksresource_official.DatabricksMwsWorkspaces{
		ResourceName: mws.Name,
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					credentials.ResourceId().String(),
					storage.ResourceId().String(),
					network.ResourceId().String(),
					managedServicesCmk.ResourceId().String(),
				},
			},
		},
		AccountID:                           dataplatformconfig.DatabricksAccountId,
		WorkSpaceName:                       mws.Name,
		DeploymentName:                      mws.DeploymentName,
		AwsRegion:                           mws.Region,
		CredentialsID:                       credentials.ResourceId().ReferenceAttr("credentials_id"),
		StorageConfigurationID:              storage.ResourceId().ReferenceAttr("storage_configuration_id"),
		NetworkID:                           network.ResourceId().ReferenceAttr("network_id"),
		ManagedServicesCustomerManagedKeyID: managedServicesCmk.ResourceId().ReferenceAttr("customer_managed_key_id"),
	}

	// Enable UC in this workspace.
	metastoreAssignment := &databricksresource_official.DatabricksMetastoreAssignment{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: []string{
					workspace.ResourceId().String(),
				},
			},
		},
		ResourceName: mws.Name + "-metastore-assignment",
		WorkspaceID:  workspace.ResourceId().ReferenceAttr("workspace_id"),
		MetastoreID:  mws.UnityCatalogMetastoreID,
	}

	resources = append(resources,
		credentials,
		storage,
		network,
		managedServicesCmk,
		workspace,
		metastoreAssignment,
	)
	return resources, nil
}

func MultiWorkSpacesProject() (map[string][]tf.Resource, error) {

	resources := map[string][]tf.Resource{}

	for name, workspaceInput := range workspaceRegistry {
		ws, err := workspaceInput.Create()

		if err != nil {
			return nil, err
		}
		resources[name+"_workspace"] = ws
	}

	return resources, nil
}
