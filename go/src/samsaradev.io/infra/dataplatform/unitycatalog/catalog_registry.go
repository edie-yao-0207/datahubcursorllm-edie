package unitycatalog

import (
	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	teamComponents "samsaradev.io/team/components"
)

type Catalog struct {
	Name    string
	Regions []string

	Owner teamComponents.TeamInfo

	UseCatalogGroupNames []string

	// In most cases, we'll manage the permissions for catalogs via terraform.
	// However, some catalogs will be owned by another team (e.g. biztech data)
	// and so we will explicitly not manage any permissioning beyond the ownership
	// of the catalog in terraform, allowing them to manage it independently.
	UnmanagedPermissions bool

	Schemas       []databaseregistries.SamsaraDB
	VolumeSchemas []VolumeSchema
}

type Schema struct {
	Name string

	Owner teamComponents.TeamInfo
}

type VolumeSchema struct {
	UnprefixedBucketLocation string
	VolumePrefixes           []Volume
}

type Volume struct {
	Name           string
	Prefix         string
	ReadTeams      []teamComponents.TeamInfo
	WriteTeams     []teamComponents.TeamInfo
	ReadWriteTeams []teamComponents.TeamInfo
	ExcludeRegions []string // by default creates in all regions, override only excludes for listed regions.
}

func (c Catalog) InRegion(region string) bool {
	for _, candidate := range c.Regions {
		if candidate == region {
			return true
		}
	}
	return false
}

func (v Volume) VolumeExcludedInRegion(region string) bool {
	for _, candidate := range v.ExcludeRegions {
		if candidate == region {
			return true
		}
	}
	return false
}

// GetUnityCatalogRegistry consumes a DatabricksConfig and returns a list of Catalog objects.
// The returned catalogs are filtered based on the Region specified in the provided config.
func GetUnityCatalogRegistry(config dataplatformconfig.DatabricksConfig) []Catalog {
	return []Catalog{
		{
			Name:    "dataplatform_test",
			Regions: []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion},
			Owner:   team.DataPlatform,
			Schemas: []databaseregistries.SamsaraDB{
				{
					Name:          "test-schema",
					OwnerTeam:     team.DataPlatform,
					AWSAccountIDs: []string{databaseregistries.MainAWSAccountEU},
				},
			},
		},
		{
			Name:                 "edw_dev",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "edw_uat",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "edw_bronze",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "edw",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "edw_migration",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "businessdbs",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.BizTechEnterpriseDataAdmin,
			UnmanagedPermissions: true,
		},
		{
			Name:                 "default",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			Owner:                team.DataPlatform,
			UseCatalogGroupNames: append(databaseregistries.GetAllBiztechGroupsInDefaultCatalog(config), dataplatformterraformconsts.AllSamsaraUsersGroup),
			Schemas:              databaseregistries.GetAllDatabases(config, databaseregistries.AllUnityCatalogDatabases),
		},
		{
			// This catalog is to handle volumes, which will abstract over s3 storage.
			Name:                 "s3",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			Owner:                team.DataPlatform,
			VolumeSchemas:        GetAllVolumeSchemas(config),
			UseCatalogGroupNames: []string{dataplatformterraformconsts.AllSamsaraUsersGroup},
		},
		{
			Name:                 "non_govramp_customer_data",
			Regions:              []string{infraconsts.SamsaraAWSDefaultRegion},
			Owner:                team.DataPlatform,
			UseCatalogGroupNames: append(GetBiztechGroupsInGovrampCatalog(), dataplatformterraformconsts.AllSamsaraUsersGroup),
		},
	}
}

// GetBiztechGroupsInGovrampCatalog returns unique biztech groups extracted from CanReadBiztechGroups.
// These are non-R&D teams managed by biztech (not dataplatform) for catalog access.
func GetBiztechGroupsInGovrampCatalog() []string {
	// Get all non-govramp tables from the registry
	tables := govramp.GetAllNonGovrampTables()

	// Use a map to collect unique biztech groups
	uniqueGroups := make(map[string]bool)

	// Iterate through all tables and collect CanReadBiztechGroups
	for _, table := range tables {
		for _, group := range table.CanReadBiztechGroups {
			uniqueGroups[group] = true
		}
	}

	// Convert map keys to slice
	result := make([]string, 0, len(uniqueGroups))
	for group := range uniqueGroups {
		result = append(result, group)
	}

	return result
}
