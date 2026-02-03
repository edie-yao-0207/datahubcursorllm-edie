package unitycatalog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/team"
	teamComponents "samsaradev.io/team/components"
)

func unityCatalogShareResources(providerGroup string) (map[string][]tf.Resource, error) {
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

	var shareResources []tf.Resource
	var shareGrantResources []tf.Resource

	for _, share := range unitycatalog.ShareRegistry {
		if !share.InSourceRegion(config.Region) {
			continue
		}

		// Create the share
		var shareObjects []*databricksresource_official.ShareObject
		shareResource := &databricksresource_official.Share{
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					DependsOn: []string{
						"databricks_recipient." + share.RecipientName,
					},
				},
			},
			Name:  share.Name,
			Owner: share.Owner.DatabricksAccountGroupName(),
		}

		keys := make([]string, 0, len(share.ShareObjects))
		for key := range share.ShareObjects {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		// iterating over share objects
		for _, key := range keys {
			object := share.ShareObjects[key]
			shareObject := &databricksresource_official.ShareObject{
				Name:           object.Name,
				DataObjectType: object.DataObjectType,
			}

			// if object is a table/view, assign SharedAs for unique naming
			if object.DataObjectType == unitycatalog.ObjectTypeTable {
				nameParts := strings.Split(object.Name, ".")
				shareObject.SharedAs = nameParts[1] + "." + nameParts[2]
			}

			if object.HistoryDataSharingStatus != "" {
				shareObject.HistoryDataSharingStatus = object.HistoryDataSharingStatus
			}

			// iterating over partitions
			if len(object.Partitions) > 0 {
				var shareObjectPartitions []*databricksresource_official.ShareObjectPartition
				for _, partition := range object.Partitions {
					var shareObjectPartitionValues []*databricksresource_official.ShareObjectPartitionValue
					for _, partitionValue := range partition.Values {
						shareObjectPartitionValue := &databricksresource_official.ShareObjectPartitionValue{
							Name: partitionValue.Name,
							Op:   partitionValue.Op,
						}

						// only one of recipient property key or value will be set
						if partitionValue.RecipientPropertyKey != "" {
							shareObjectPartitionValue.RecipientPropertyKey = partitionValue.RecipientPropertyKey
						}
						if partitionValue.Value != "" {
							shareObjectPartitionValue.Value = partitionValue.Value
						}
						shareObjectPartitionValues = append(shareObjectPartitionValues, shareObjectPartitionValue)
					}
					shareObjectPartition := &databricksresource_official.ShareObjectPartition{
						Values: shareObjectPartitionValues,
					}
					shareObjectPartitions = append(shareObjectPartitions, shareObjectPartition)
				}

				shareObject.Partitions = shareObjectPartitions
			}
			shareObjects = append(shareObjects, shareObject)
		}
		shareResource.Objects = shareObjects

		// add grants for accessing share
		var selectShareGrantSpecs []databricksresource_official.GrantSpec
		// only select access is needed
		selectGrantSpec := databricksresource_official.GrantSpec{
			Principal: share.RecipientName,
			Privileges: []databricksresource_official.GrantPrivilege{
				databricksresource_official.GrantTableSelect,
			},
		}
		selectShareGrantSpecs = append(selectShareGrantSpecs, selectGrantSpec)

		selectShareGrant := &databricksresource_official.DatabricksGrants{
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					DependsOn: []string{
						"databricks_recipient." + share.RecipientName,
						shareResource.ResourceId().String(),
					},
				},
			},
			ResourceName: fmt.Sprintf("share_use_%s", share.Name),
			Share:        share.Name,
			Grants:       selectShareGrantSpecs,
		}
		shareGrantResources = append(shareGrantResources, selectShareGrant)
		shareResources = append(shareResources, shareResource)
	}
	// surface shares and share grants
	if len(shareResources) > 0 {
		resourceGroups["shares"] = shareResources
		resourceGroups["share_grants"] = shareGrantResources
	}

	return resourceGroups, nil
}

func unityCatalogShareCatalogResources(providerGroup string) (map[string][]tf.Resource, error) {
	resourceGroups := map[string][]tf.Resource{}
	schemasByTeam := make(map[string][]tf.Resource)

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Exit early in case we're not in the dev workspaces. Catalogs exist at a metastore level, but need to be created
	// from a workspace (a requirement of the terraform provider).
	if !dataplatformresource.IsE2ProviderGroup(config.DatabricksProviderGroup) {
		return nil, nil
	}

	var catalogResources []tf.Resource

	for _, share := range unitycatalog.ShareRegistry {
		if !share.InCatalogRegion(config.Region) {
			continue
		}

		// create grant for machine user to access share

		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
		}

		// Create the catalog
		catalogResource := &databricksresource_official.DatabricksCatalog{
			Name:         share.Catalog.Name,
			Owner:        share.Catalog.Owner.DatabricksAccountGroupName(),
			ProviderName: share.Catalog.ProviderName,
			ShareName:    share.Name,
		}
		catalogResources = append(catalogResources, catalogResource)

		uniqueSchemas := make(map[string]bool)

		// iterate over each schema that the share catalog creates and assign proper perms
		for _, schema := range share.ShareObjects {
			// split by . to get just schema name
			parts := strings.Split(schema.Name, ".")
			val, ok := uniqueSchemas[share.Catalog.Name+"."+parts[1]]
			// Schema doesn't exist yet, so create grants
			if !ok || val != true {
				owner := team.DataPlatform.DatabricksAccountGroupName()
				resourceGroup := fmt.Sprintf("%s_schemas", owner)
				if _, ok := schemasByTeam[resourceGroup]; !ok {
					schemasByTeam[resourceGroup] = []tf.Resource{}
				}

				// assign perms to schema
				schemaGrantResources, err := schemaGrantsResources(share.Catalog.Name, parts[1], share.Catalog.CanReadGroups[parts[1]])
				if err != nil {
					return nil, oops.Wrapf(err, "Error making schema and grant resources for catalog %s and schema %s", share.Catalog.Name, parts[1])
				}
				schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], schemaGrantResources...)
			}
			uniqueSchemas[share.Catalog.Name+"."+parts[1]] = true
		}

		catalogGrantSpecs := []databricksresource_official.GrantSpec{
			{
				Principal: ciServicePrincipalAppId,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			},
			{
				// Grant the DataPlat DBX group all privileges, as they are admins.
				Principal: team.DataPlatform.DatabricksAccountGroupName(),
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			},
		}

		// Create a map to track unique teams
		uniqueTeams := make(map[string]bool)

		// Collect unique teams from all schema permissions
		for _, teams := range share.Catalog.CanReadGroups {
			for _, team := range teams {
				uniqueTeams[team.DatabricksAccountGroupName()] = true
			}
		}

		// Convert map keys to sorted slice
		teamNames := make([]string, 0, len(uniqueTeams))
		for teamName := range uniqueTeams {
			teamNames = append(teamNames, teamName)
		}
		sort.Strings(teamNames)

		// add perms for all teams who can access catalog
		for _, teamName := range teamNames {
			// this was granted above, so skip
			if teamName == team.DataPlatform.DatabricksAccountGroupName() {
				continue
			}
			catalogGrantSpecs = append(catalogGrantSpecs, databricksresource_official.GrantSpec{
				Principal: teamName,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			})
		}

		catalogResources = append(catalogResources, &databricksresource_official.DatabricksGrants{
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					DependsOn: []string{
						catalogResource.ResourceId().String(),
					},
				},
			},
			ResourceName: fmt.Sprintf("catalog_%s", share.Catalog.Name),
			Catalog:      catalogResource.ResourceId().Reference(),
			Grants:       catalogGrantSpecs,
		})
	}

	// surface catalog
	if len(catalogResources) > 0 {
		sortCatalogResources(catalogResources)
		resourceGroups["catalogs"] = catalogResources
	}
	resourceGroups = project.MergeResourceGroups(resourceGroups, schemasByTeam)

	return resourceGroups, nil
}

func readSchemaGrantShare(dbxAccountGroupName string) databricksresource_official.GrantSpec {
	return databricksresource_official.GrantSpec{
		Principal: dbxAccountGroupName,
		Privileges: []databricksresource_official.GrantPrivilege{
			databricksresource_official.GrantSchemaUseSchema,
			databricksresource_official.GrantTableSelect,
			databricksresource_official.GrantFunctionExecute,
		},
	}
}

func schemaGrantsResources(catalogName string, schemaName string, canReadGroups []teamComponents.TeamInfo) ([]tf.Resource, error) {
	var resources []tf.Resource

	// Add in read grants.
	var dbGrantSpecs []databricksresource_official.GrantSpec
	for _, readGroup := range canReadGroups {
		dbGrantSpecs = append(dbGrantSpecs, readSchemaGrantShare(readGroup.DatabricksAccountGroupName()))
	}

	if len(dbGrantSpecs) > 0 {
		sort.Slice(dbGrantSpecs, func(i, j int) bool {
			return dbGrantSpecs[i].Principal < dbGrantSpecs[j].Principal
		})
		resources = append(resources, &databricksresource_official.DatabricksGrants{
			BaseResource: tf.BaseResource{
				MetaParameters: tf.MetaParameters{
					DependsOn: []string{
						"databricks_catalog." + catalogName,
					},
				},
			},
			ResourceName: fmt.Sprintf("schema_%s_%s", catalogName, schemaName),
			Schema:       catalogName + "." + schemaName,
			Grants:       dbGrantSpecs,
		})
	}

	return resources, nil
}
