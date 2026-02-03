package govramp

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

// createGovrampSchemaResources creates schema resources and returns both the resource groups
// and a map of schema resources that other resources can reference.
// This enables type-safe dependencies within the same Terraform project.
func createGovrampSchemaResources(providerGroup string) (map[string][]tf.Resource, map[string]*databricksresource_official.DatabricksSchema, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Get CI service principal for schema ownership
	ciUser, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to get ci service principal app id for region %s", config.Region)
	}

	// Get schema names from the registry
	schemaNames, err := getSchemaNamesFromRegistry(govramp.GetAllNonGovrampTables())
	if err != nil {
		return nil, nil, err
	}

	// Build resources from names and return them
	return buildSchemaResources(schemaNames, ciUser)
}

// getSchemaNamesFromRegistry walks the registry and returns a de-duplicated
// list of database (schema) names to create in the target catalog. It expands
// sharded logical schemas (e.g. workforcevideodb_shards) into physical shard
// schemas (e.g. workforcevideo_shard_0db).
func getSchemaNamesFromRegistry(tables []govramp.Table) ([]string, error) {
	schemaSet := make(map[string]struct{})

	for _, table := range tables {
		// Base schema for any table
		schemaSet[table.SourceSchema] = struct{}{}

		if table.TableCategory == govramp.SourceTableCategoryRdsSharded {
			rdsDbName := strings.TrimSuffix(table.SourceSchema, "db_shards")
			shardNames, err := getShardNames(rdsDbName)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to get shard names for %s", rdsDbName)
			}
			for _, shardName := range shardNames {
				shardSchemaName := fmt.Sprintf("%sdb", shardName)
				schemaSet[shardSchemaName] = struct{}{}
			}
		}
	}

	// Convert to stable slice
	result := make([]string, 0, len(schemaSet))
	for name := range schemaSet {
		result = append(result, name)
	}

	// sort the result for deterministic ordering
	sort.Strings(result)

	return result, nil
}

// buildSchemaResources turns a list of schema names into Terraform resources and a map
// for reference resolution.
func buildSchemaResources(schemaNames []string, ciUser string) (map[string][]tf.Resource, map[string]*databricksresource_official.DatabricksSchema, error) {
	schemaResources := []tf.Resource{}
	schemaResourceMap := make(map[string]*databricksresource_official.DatabricksSchema)

	for _, schemaName := range schemaNames {
		schema := &databricksresource_official.DatabricksSchema{
			ResourceName: fmt.Sprintf("%s_%s", govrampCatalog, schemaName),
			CatalogName:  govrampCatalog,
			Name:         schemaName,
			Owner:        ciUser,
			Comment:      fmt.Sprintf("Schema for non-govramp customer data tables from %s", schemaName),
		}
		schemaResourceMap[schemaName] = schema
		schemaResources = append(schemaResources, schema)
	}

	resourceGroups := make(map[string][]tf.Resource)
	resourceGroups["schemas"] = schemaResources
	return resourceGroups, schemaResourceMap, nil
}
