package unitycatalog

import (
	"fmt"
	"sort"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/dataplatform/unitycatalog"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var (
	// IDs of service principals to be granted access to system tables.
	// These are temporary and will be replaced once we manage service principals in the registry.
	// The service principal for cloudzero is temporary and will be replaced once we manage service principals in the registry.
	cloudZeroServicePrincipalID  = "30ac2c6c-a4d1-46bd-94d4-def2a38700b3"
	BiztechServicePrincipalGroup = "bt-bted-service-principals"
)

// TODO: Shailey and Miriam will need temporary access to all of default to
// perform an audit of the current state of the data. Once they are done, we can
// remove this list.
var defaultCatalogReadUsersUS = []string{"shailey.jain@samsara.com", "miriam.gribin@samsara.com"}

type systemTable struct {
	schema string

	// Leave nil to enable all regions
	regions []string

	// Grant specific databricks access to system tables.
	// For machine users, add it to allowedMachineUsersPrincipals.
	groupPrincipals []string
}

func restrictTeamAccessToDefaultCatalog(t components.TeamInfo, region string, catalogName string) bool {

	// If the catalog is not the default catalog, we dont worry about its restrictions.
	if catalogName != "default" {
		return false
	}

	// For US, due to govramp compliance reasons, we have restricted access to default catalog for non-R&D teams.
	// - Their access to product data is granted via non_govramp_customer_data catalog or approved exceptions
	//   Through the #ask-product-data-access channel.
	// For Canada, while there is no govramp compliance to worry about, we still want to follow least privilege principle
	//   and restrict access to default catalog for non-R&D teams.
	// For EU, we have given access to default catalog to all teams, so at this point in time, we've decided not
	//   to restrict access to default catalog for non-R&D teams which may change in the future.
	switch region {
	case infraconsts.SamsaraAWSEURegion:
		return false
	// Make regionconditionalchecker happy :)
	case infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSCARegion:
	}

	// If the team is a development team, we want to grant access to default catalog, so don't restrict it.
	if t.IsDevelopmentTeam() {
		return false
	}

	// If the team is not a development team, we want to grant access to default catalog only if NonDevelopmentTeamHasAccessToDefaultCatalog is true.
	// These are RnD teams requiring access to default catalog but have NonDevelopment set to true.
	// But for Canada, we want to restrict access entirely even for non-development teams in R&D and give access
	// explicitly in the DB registry.
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return !t.NonDevelopmentTeamHasAccessToDefaultCatalog
	// Even though EU will never be hit here per the logic above, add it so that regionconditionalchecker is happy :)
	case infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
	}

	// Based on the above conditions, we are restricting access to teams
	// who are not development teams in Canada region.
	return true
}

// Machine users have region specific emails, add them here if you need access to system tables
// from these machine users.
func allowedMachineUsersPrincipals(config dataplatformconfig.DatabricksConfig) []string {

	return []string{
		config.MachineUsers.CI.Email,
	}
}

func (s systemTable) inRegion(region string) bool {
	if len(s.regions) == 0 {
		return true
	}
	for _, allowedRegion := range s.regions {
		if region == allowedRegion {
			return true
		}
	}
	return false
}

// Get the list of principals for the DS&S (Data Science & Services) teams
// These teams get access to all system tables.
func getDSSOrgTeamPrincipals() []string {
	return []string{
		team.DataPlatform.DatabricksAccountGroupName(),
		team.DataTools.DatabricksAccountGroupName(),
		team.DataAnalytics.DatabricksAccountGroupName(),
		team.DataEngineering.DatabricksAccountGroupName(),
		team.DecisionScience.DatabricksAccountGroupName(),
	}
}

// https://docs.databricks.com/en/administration-guide/system-tables/index.html#enable
var schemasToEnable = []systemTable{ // lint: +sorted
	{
		schema:          "access",
		groupPrincipals: getDSSOrgTeamPrincipals(),
	},
	{
		schema: "billing",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
			BiztechServicePrincipalGroup, //Group for biztech service principals
			cloudZeroServicePrincipalID,  // temporary id of service principal for cloudzero
		),
	},
	{
		schema: "compute",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
			cloudZeroServicePrincipalID, // temporary id of service principal for cloudzero
		),
	},
	// new catalog for jobs information
	{
		schema: "lakeflow",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseData.DatabricksAccountGroupName(),
			team.MarketingDataAnalytics.DatabricksAccountGroupName(),
			team.SalesDataAnalytics.DatabricksAccountGroupName(),
			team.CustomerSuccessOperations.DatabricksAccountGroupName(),
			team.SafetyFirmware.DatabricksAccountGroupName(),
		),
	},
	{
		schema: "query",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		),
	},
	{
		schema: "serving",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		),
	},
	{
		schema: "storage",
		groupPrincipals: append(getDSSOrgTeamPrincipals(),
			team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		),
	},
}

func unityCatalogCatalogResources(providerGroup string) (map[string][]tf.Resource, error) {
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

	// We'll organize the output resources into:
	// - catalogs&grants - 1 resourcegroup for all catalogs & top-level grants
	// - schemasByTeam - 1 resourcegroup per team , and all permissions / volumes / etc. inside of it
	//   this is symmetric with how the registry looks and keeps a reasonable number of output tf files.
	var catalogResources []tf.Resource
	schemasByTeam := make(map[string][]tf.Resource)

	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	for _, catalog := range unitycatalog.GetUnityCatalogRegistry(config) {
		if !catalog.InRegion(config.Region) {
			continue
		}

		// Create the catalog
		catalogResource := &databricksresource_official.DatabricksCatalog{
			Name:  catalog.Name,
			Owner: catalog.Owner.DatabricksAccountGroupName(),
		}
		catalogResources = append(catalogResources, catalogResource)

		// Create the schemas in the catalog registry.
		// TODO: change ownership to be the CI user and only give necessary privileges to individual teams.
		// Ownership gives ALL_PRIVILEGES which is too high of a permission on the schema.
		// Remember to grant the owner team read/write privileges when we remove them as the technical owner of the schema.
		for _, schema := range catalog.Schemas {
			if _, ok := schema.GetRegionsMap()[config.Region]; !ok {
				continue
			}

			owner := team.DataPlatform.DatabricksAccountGroupName()
			if schema.OwnerTeamName != "" {
				owner = schema.OwnerTeamName
			} else if schema.OwnerTeam.TeamName != "" {
				owner = schema.OwnerTeam.DatabricksAccountGroupName()
			}

			resourceGroup := fmt.Sprintf("%s_schemas", owner)
			if _, ok := schemasByTeam[resourceGroup]; !ok {
				schemasByTeam[resourceGroup] = []tf.Resource{}
			}

			catalogRefId := catalogResource.ResourceId().Reference()
			schemaAndGrantResources, err := schemaAndGrantsResources(schema, catalogRefId, catalog.Name, config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "Error making schema and grant resources for catalog %s and schema %s", catalog.Name, schema.Name)
			}
			schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], schemaAndGrantResources...)

			// Note: query_agents.logs table, masking function, and column mask are
			// all created via a Dagster op (setup_query_agents_logs_column_mask) that
			// runs as part of the data model pipeline. No Terraform resources needed.

			// For sharded RDS databases, the current schema is for the combined shards view. We also want to grant permissions on the
			// individual db shards that are referenced by the combined shards view. If the database is unsharded, then there the schema/grants
			// have already been created above and there are no additional resources to create.
			if schema.DatabaseGroup == databaseregistries.RdsDatabaseGroup {
				rdsRegistryName := rdsdeltalake.SparkDbNameToRdsDbRegistryName(schema.Name)
				rdsRegistryDb, err := rdsdeltalake.GetDatabaseByName(rdsRegistryName)
				if err != nil {
					return nil, oops.Wrapf(err, "Error fetching RDS RegistryDatabase by name %s", rdsRegistryName)
				}
				if rdsRegistryDb.Sharded {
					cloud := infraconsts.GetProdCloudByRegion(config.Region)
					awsShardNames, err := dbregistry.GetShardedDBNamesPerCloud(rdsRegistryName, cloud)
					if err != nil {
						return nil, oops.Wrapf(err, "Error getting shard names for db %s in region %s", rdsRegistryName, config.Region)
					}

					for _, awsShardName := range awsShardNames {
						// Duplicate the fields from the combined shards schema, but change the Name to be shard specific.
						shardSchema := schema
						shardSchema.Name = rdsdeltalake.GetSparkFriendlyRdsDBName(awsShardName, true, true)
						shardSchema.DatabaseGroup = databaseregistries.RdsShardsDatabaseGroup

						shardSchemaAndGrantResources, err := schemaAndGrantsResources(shardSchema, catalogRefId, catalog.Name, config.Region)
						if err != nil {
							return nil, oops.Wrapf(err, "Error making schema and grant resources for catalog %s and schema %s", catalog.Name, shardSchema.Name)
						}
						schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], shardSchemaAndGrantResources...)
					}
				}
			}
		}

		for _, schema := range catalog.VolumeSchemas {

			if !unitycatalog.BucketExistsInRegion(schema.UnprefixedBucketLocation, config) {
				// If the bucket does not exist in the region, we can ignore the volume schema and volumes.
				continue
			}
			// TODO: fix team ownership for volumes
			resourceGroup := fmt.Sprintf("%s_schemas", team.DataPlatform.DatabricksAccountGroupName())

			ciUser, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to get ci service principal app id for region %s", config.Region)
			}

			volumeSchemaResource := &databricksresource_official.DatabricksSchema{
				ResourceName: fmt.Sprintf("%s_%s", catalog.Name, schema.UnprefixedBucketLocation),
				CatalogName:  catalogResource.ResourceId().Reference(),
				Name:         schema.UnprefixedBucketLocation,
				Owner:        ciUser,
			}
			schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], volumeSchemaResource)

			teamsWithAccess := []components.TeamInfo{}

			for _, volume := range schema.VolumePrefixes {

				if volume.VolumeExcludedInRegion(config.Region) {
					// If volume is excluded for a given region, we skip creating resource for it.
					continue
				}

				storageLocation := fmt.Sprintf("s3://%s%s/", awsregionconsts.RegionPrefix[config.Region], schema.UnprefixedBucketLocation)
				if volume.Prefix != "" {
					storageLocation += volume.Prefix
				}

				volumeResource := &databricksresource_official.Volume{
					ResourceName:    fmt.Sprintf("volume_%s_%s_%s", catalog.Name, schema.UnprefixedBucketLocation, volume.Name),
					Owner:           team.DataPlatform.DatabricksAccountGroupName(),
					Name:            volume.Name,
					CatalogName:     catalogResource.ResourceId().ReferenceAttr("name"),
					SchemaName:      volumeSchemaResource.ResourceId().ReferenceAttr("name"),
					VolumeType:      databricksresource_official.VolumeTypeExternal,
					StorageLocation: storageLocation,
				}
				schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], volumeResource)

				var readGrantSpecs []databricksresource_official.GrantSpec
				var writeGrantSpecs []databricksresource_official.GrantSpec
				var readWriteGrantSpecs []databricksresource_official.GrantSpec
				for _, team := range volume.ReadTeams {
					readGrantSpecs = append(readGrantSpecs, databricksresource_official.GrantSpec{
						Principal: team.DatabricksAccountGroupName(),
						Privileges: []databricksresource_official.GrantPrivilege{
							databricksresource_official.GrantVolumeReadVolume,
						},
					})
				}
				for _, team := range volume.WriteTeams {
					writeGrantSpecs = append(writeGrantSpecs, databricksresource_official.GrantSpec{
						Principal: team.DatabricksAccountGroupName(),
						Privileges: []databricksresource_official.GrantPrivilege{
							databricksresource_official.GrantVolumeWriteVolume,
						},
					})
				}
				for _, team := range volume.ReadWriteTeams {
					readWriteGrantSpecs = append(readWriteGrantSpecs, databricksresource_official.GrantSpec{
						Principal: team.DatabricksAccountGroupName(),
						Privileges: []databricksresource_official.GrantPrivilege{
							databricksresource_official.GrantVolumeReadVolume,
							databricksresource_official.GrantVolumeWriteVolume,
						},
					})
				}
				if len(readGrantSpecs) > 0 || len(writeGrantSpecs) > 0 || len(readWriteGrantSpecs) > 0 {
					merged := consolidateGrantSpecs(append(append(readGrantSpecs, writeGrantSpecs...), readWriteGrantSpecs...))
					sort.Slice(merged, func(i, j int) bool { return merged[i].Principal < merged[j].Principal })
					schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], &databricksresource_official.DatabricksGrants{
						ResourceName: fmt.Sprintf("volume_%s_%s_%s", catalog.Name, schema.UnprefixedBucketLocation, volume.Name),
						Volume:       volumeResource.ResourceId().Reference(),
						Grants:       merged,
					})
				}
				teamsWithAccess = append(teamsWithAccess, append(append(volume.ReadTeams, volume.WriteTeams...), volume.ReadWriteTeams...)...)
			}

			// Grant USE_SCHEMA access to all teams that have access to the volumes.
			// This is necessary because the volumes are stored in the schema.
			var useSchemaGrantSpecs []databricksresource_official.GrantSpec
			for _, team := range teamsWithAccess {
				useSchemaGrantSpecs = append(useSchemaGrantSpecs, databricksresource_official.GrantSpec{
					Principal: team.DatabricksAccountGroupName(),
					Privileges: []databricksresource_official.GrantPrivilege{
						databricksresource_official.GrantSchemaUseSchema,
					},
				})
			}

			// Create grant schema resource only there atleast 1 team to grant it to.
			if len(useSchemaGrantSpecs) > 0 {
				useSchemaGrantSpecs = consolidateGrantSpecs(useSchemaGrantSpecs)
				sort.Slice(useSchemaGrantSpecs, func(i, j int) bool { return useSchemaGrantSpecs[i].Principal < useSchemaGrantSpecs[j].Principal })
				grant := &databricksresource_official.DatabricksGrants{
					ResourceName: fmt.Sprintf("volume_schema_%s", schema.UnprefixedBucketLocation),
					Schema:       volumeSchemaResource.ResourceId().Reference(),
					Grants:       useSchemaGrantSpecs,
				}
				schemasByTeam[resourceGroup] = append(schemasByTeam[resourceGroup], grant)
			}
		}

		// For catalogs marked as unmanaged, we don't want to manage any permissions beyond the ownership of the catalog in terraform
		// within our backend repo for the catalog. This allows the owning team to manage it independently.
		if catalog.UnmanagedPermissions {
			continue
		}

		catalogGrantSpecs := []databricksresource_official.GrantSpec{
			{
				Principal: ciServicePrincipalAppId,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogAllPrivileges,
				},
			},
			{
				// Grant the DataPlat DBX group all privileges, as they are admins.
				Principal: team.DataPlatform.DatabricksAccountGroupName(),
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogAllPrivileges,
				},
			},
		}

		for _, group := range catalog.UseCatalogGroupNames {
			catalogGrantSpecs = append(catalogGrantSpecs, databricksresource_official.GrantSpec{
				Principal: group,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			})
		}

		if catalog.Name == "default" && config.Region == infraconsts.SamsaraAWSDefaultRegion {
			for _, user := range defaultCatalogReadUsersUS {
				catalogGrantSpecs = append(catalogGrantSpecs, databricksresource_official.GrantSpec{
					Principal: user,
					Privileges: []databricksresource_official.GrantPrivilege{
						databricksresource_official.GrantCatalogUseCatalog,
					},
				})
			}
		}

		catalogResources = append(catalogResources, &databricksresource_official.DatabricksGrants{
			ResourceName: fmt.Sprintf("catalog_%s", catalog.Name),
			Catalog:      catalogResource.ResourceId().Reference(),
			Grants:       catalogGrantSpecs,
		})
	}

	// System schemas are confusingly enabled on a workspace level, but apply to the metastore in that workspace.
	// So, we only want to enable them from our main workspaces, databricks-dev-* workspaces.
	if dataplatformresource.IsE2ProviderGroup(config.DatabricksProviderGroup) {
		var systemSchemas []tf.Resource

		// to capture unique principals for grant use catalog.
		uniqueGroupPrincipals := []string{}

		readTablePrivileges := []databricksresource_official.GrantPrivilege{
			databricksresource_official.GrantSchemaUseSchema,
			databricksresource_official.GrantTableSelect,
		}

		for _, spec := range schemasToEnable {
			if spec.inRegion(config.Region) {
				systemSchemas = append(systemSchemas, &databricksresource_official.SystemSchema{
					Schema: spec.schema,
				})

				// List of principals to grant access to the system schema.
				readSytemTableGrants := []databricksresource_official.GrantSpec{}

				// Add specific Machine users allowed for system tables reads.
				for _, machineUser := range allowedMachineUsersPrincipals(config) {
					readSytemTableGrants = append(readSytemTableGrants, databricksresource_official.GrantSpec{
						Principal:  machineUser,
						Privileges: readTablePrivileges,
					})
				}

				for _, principal := range spec.groupPrincipals {
					// skip granting access to cloudzero outside of US region.
					if principal == cloudZeroServicePrincipalID && config.Region != infraconsts.SamsaraAWSDefaultRegion {
						continue
					}

					// grant use schema and select for system table on listed principals.
					readSytemTableGrants = append(readSytemTableGrants, databricksresource_official.GrantSpec{
						Principal:  principal,
						Privileges: readTablePrivileges,
					})

					// check if principal is already added to uniqueGroupPrincipals and if not add it so that
					// we can grant use-catalog to all the unique group principals across the schemas.
					if !contains(uniqueGroupPrincipals, principal) {
						uniqueGroupPrincipals = append(uniqueGroupPrincipals, principal)
					}

				}

				catalogResources = append(catalogResources, &databricksresource_official.DatabricksGrants{
					ResourceName: fmt.Sprintf("system_catalog_%s", spec.schema),
					Schema:       fmt.Sprintf("system.%s", spec.schema),
					Grants:       readSytemTableGrants,
				})

			}
		}

		// For all principals needing access to system catalog, grant use-catalog.
		// Grant access to catalog for machine users listed.
		useCatalogGrants := []databricksresource_official.GrantSpec{}
		for _, machineUser := range allowedMachineUsersPrincipals(config) {
			useCatalogGrants = append(useCatalogGrants, databricksresource_official.GrantSpec{
				Principal: machineUser,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			})

		}

		// always preserve the order.
		sort.Strings(uniqueGroupPrincipals)
		// Grant use catalog to unique group principals.
		for _, principal := range uniqueGroupPrincipals {
			useCatalogGrants = append(useCatalogGrants, databricksresource_official.GrantSpec{
				Principal: principal,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantCatalogUseCatalog,
				},
			})
		}

		catalogResources = append(catalogResources, &databricksresource_official.DatabricksGrants{
			ResourceName: "system_catalog_use_catalog",
			Catalog:      "system",
			Grants:       useCatalogGrants,
		})

		resourceGroups["system_schemas"] = systemSchemas

	}

	if len(catalogResources) > 0 {
		sortCatalogResources(catalogResources)
		resourceGroups["catalogs"] = catalogResources
	}

	resourceGroups = project.MergeResourceGroups(resourceGroups, schemasByTeam)

	return resourceGroups, nil
}

// sortCatalogResources sorts catalog resources for deterministic terraform generation.
// First, it sorts the principals within each DatabricksGrants resource.
// Then, it sorts the resources by: resource name, then catalog name, then first principal.
func sortCatalogResources(resources []tf.Resource) {
	// First, sort principals within each grants resource
	for _, res := range resources {
		if grants, ok := res.(*databricksresource_official.DatabricksGrants); ok {
			sort.Slice(grants.Grants, func(i, j int) bool {
				return grants.Grants[i].Principal < grants.Grants[j].Principal
			})
		}
	}

	// Then, sort the resources themselves
	sort.Slice(resources, func(i, j int) bool {
		resI := resources[i]
		resJ := resources[j]

		// First: compare by resource name
		nameI := resI.ResourceId().Name
		nameJ := resJ.ResourceId().Name
		return nameI < nameJ
	})
}

type SchemaAccessLevel string

const (
	READ      SchemaAccessLevel = "READ"
	READWRITE SchemaAccessLevel = "READWRITE"
	MANAGE    SchemaAccessLevel = "MANAGE"
	// Only use schema is granted. This is granted to teams that we dont to give direct access to RAW data in
	// default catalog but access to filtered version in a different catalog by views. Example NonRnD teams are given access to
	// filtered version in non_govramp_customer_data catalog.
	RESTRICTED SchemaAccessLevel = "RESTRICTED_READ"
)

func getSchemaPrivileges(team components.TeamInfo, access SchemaAccessLevel) databricksresource_official.GrantSpec {
	return getSchemaPrivilegesForGroupName(team.DatabricksAccountGroupName(), access)
}

func getSchemaPrivilegesForGroupName(groupName string, access SchemaAccessLevel) databricksresource_official.GrantSpec {
	if access == RESTRICTED {
		return databricksresource_official.GrantSpec{
			Principal: groupName,
			Privileges: []databricksresource_official.GrantPrivilege{
				databricksresource_official.GrantSchemaUseSchema,
			},
		}
	}

	var privileges []databricksresource_official.GrantPrivilege

	// All access levels get read privileges
	privileges = append(privileges,
		databricksresource_official.GrantSchemaUseSchema,
		databricksresource_official.GrantTableSelect,
		databricksresource_official.GrantFunctionExecute,
	)

	// READWRITE and MANAGE get write privileges
	if access == READWRITE || access == MANAGE {
		privileges = append(privileges,
			databricksresource_official.GrantSchemaCreateTable,
			databricksresource_official.GrantSchemaCreateFunction,
			databricksresource_official.GrantTableModify,
		)
	}

	// Only MANAGE gets manage privileges
	if access == MANAGE {
		privileges = append(privileges,
			databricksresource_official.GrantSchemaManage,
		)
	}

	return databricksresource_official.GrantSpec{
		Principal:  groupName,
		Privileges: privileges,
	}
}

func schemaAndGrantsResources(schema databaseregistries.SamsaraDB, catalogRefId string, catalogName string, region string) ([]tf.Resource, error) {
	var resources []tf.Resource

	ciUser, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(region)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get ci service principal app id for region %s", region)
	}

	schemaResource := &databricksresource_official.DatabricksSchema{
		ResourceName: fmt.Sprintf("%s_%s", catalogName, schema.Name),
		CatalogName:  catalogRefId,
		Name:         schema.Name,
		Owner:        ciUser,
	}
	if schema.Bucket != "" {
		s3Location, err := schema.GetS3Location(region, databaseregistries.UnityCatalogBucket)
		if err != nil {
			return nil, oops.Wrapf(err, "Error constructing S3Location for schema %s", schema.Name)
		}
		schemaResource.StorageRoot = s3Location
	}
	resources = append(resources, schemaResource)

	var dbGrantSpecs []databricksresource_official.GrantSpec

	// Add read-only grants
	for _, readGroup := range schema.CanReadGroups {
		accessLevel := READ
		// If the team isnt restricted access to the default catalog, grant read access.
		if restrictTeamAccessToDefaultCatalog(readGroup, region, catalogName) {
			accessLevel = RESTRICTED
		}

		dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivileges(readGroup, accessLevel))
	}

	if catalogName == "default" && region == infraconsts.SamsaraAWSDefaultRegion {
		for _, user := range defaultCatalogReadUsersUS {
			dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivilegesForGroupName(user, READ))
		}
	}

	// Add read-only grants for biztech groups - these groups are "unmanaged" in
	// the sense that they are managed outside of the R&D org.
	if len(schema.CanReadBiztechGroups) > 0 {
		for readBiztechGroup, regions := range schema.CanReadBiztechGroups {
			// Biztech teams begin with "bt-"
			if !databaseregistries.IsValidBiztechGroupName(readBiztechGroup) {
				return nil, oops.Errorf("Error on '%s': CanReadBiztechGroup value '%s' doesn't look like a BizTech group", schema.Name, readBiztechGroup)
			}

			// If the region is not in the list of regions, skip adding the grant.
			if !contains(regions, region) {
				continue
			}

			dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivilegesForGroupName(readBiztechGroup, READ))
		}
	}

	// Add read-write grants for biztech groups - these groups are "unmanaged" in
	// the sense that they are managed outside of the R&D org.
	if len(schema.CanReadWriteBiztechGroups) > 0 {
		for readBiztechGroup, regions := range schema.CanReadWriteBiztechGroups {
			// Biztech teams begin with "bt-"
			if !databaseregistries.IsValidBiztechGroupName(readBiztechGroup) {
				return nil, oops.Errorf("Error on '%s': CanReadWriteBiztechGroup value '%s' doesn't look like a BizTech group", schema.Name, readBiztechGroup)
			}

			// If the region is not in the list of regions, skip adding the grant.
			if !contains(regions, region) {
				continue
			}

			dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivilegesForGroupName(readBiztechGroup, READWRITE))
		}
	}

	// Add read-write grants
	for _, writeGroup := range schema.CanReadWriteGroups {
		accessLevel := READWRITE
		// If the team is restricted access to the default catalog, grant restricted access.
		// Exception: query_agents database needs READWRITE for all teams to allow INSERT operations,
		// even for non-development teams. Column masking protects sensitive data instead.
		if schema.Name != "query_agents" && restrictTeamAccessToDefaultCatalog(writeGroup, region, catalogName) {
			accessLevel = RESTRICTED
		}
		dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivileges(writeGroup, accessLevel))
	}

	// Add owner grant with all privileges
	ownerTeamName := schema.OwnerTeamName
	if ownerTeamName == "" {
		ownerTeamName = schema.OwnerTeam.DatabricksAccountGroupName()
	}
	dbGrantSpecs = append(dbGrantSpecs, getSchemaPrivilegesForGroupName(ownerTeamName, MANAGE))

	// Consolidate duplicate principals by merging privileges, then sort deterministically
	dbGrantSpecs = consolidateGrantSpecs(dbGrantSpecs)
	sort.Slice(dbGrantSpecs, func(i, j int) bool {
		return dbGrantSpecs[i].Principal < dbGrantSpecs[j].Principal
	})

	resources = append(resources, &databricksresource_official.DatabricksGrants{
		ResourceName: fmt.Sprintf("schema_%s_%s", catalogName, schema.Name),
		Schema:       schemaResource.ResourceId().Reference(),
		Grants:       dbGrantSpecs,
	})

	return resources, nil
}

// consolidateGrantSpecs merges multiple GrantSpec entries for the same principal by
// taking the union of their privileges and sorting the privileges deterministically.
func consolidateGrantSpecs(grants []databricksresource_official.GrantSpec) []databricksresource_official.GrantSpec {
	if len(grants) == 0 {
		return grants
	}

	principalToPrivileges := make(map[string]map[databricksresource_official.GrantPrivilege]struct{})
	for _, g := range grants {
		if _, ok := principalToPrivileges[g.Principal]; !ok {
			principalToPrivileges[g.Principal] = make(map[databricksresource_official.GrantPrivilege]struct{})
		}
		for _, p := range g.Privileges {
			principalToPrivileges[g.Principal][p] = struct{}{}
		}
	}

	consolidated := make([]databricksresource_official.GrantSpec, 0, len(principalToPrivileges))
	for principal, privSet := range principalToPrivileges {
		privs := make([]databricksresource_official.GrantPrivilege, 0, len(privSet))
		for p := range privSet {
			privs = append(privs, p)
		}
		// Sort privileges for deterministic output
		sort.Slice(privs, func(i, j int) bool { return string(privs[i]) < string(privs[j]) })

		consolidated = append(consolidated, databricksresource_official.GrantSpec{
			Principal:  principal,
			Privileges: privs,
		})
	}

	return consolidated
}

// Returns whether the passed in slice contains the team in question.
func containsTeam(teamList []components.TeamInfo, teamName string) bool {
	for _, team := range teamList {
		if team.TeamName == teamName {
			return true
		}
	}
	return false
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
