package dataplatformprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
)

func Databases(c dataplatformconfig.DatabricksConfig) map[string][]tf.Resource {
	// Legacy databases created without a ".db" suffix.
	legacyDbNames := map[string]bool{
		"safety_map_data":  true,
		"dojo":             true,
		"labelbox":         true,
		"samsara_zendesk":  true,
		"netsuite_finance": true,
	}

	bucketPrefix := awsregionconsts.RegionPrefix[c.Region]
	resourcesOutput := map[string][]tf.Resource{}
	for _, db := range databaseregistries.GetAllDatabases(c, databaseregistries.LegacyOnlyDatabases) {
		if !db.GetRegionsMap()[c.Region] {
			continue
		}

		// Any new dbs should have a `.db` suffix so its consistent with other
		// dbs, and also doesn't cause confusion within the bucket.
		// Certain dbs were made before this so we exclude them.
		location := fmt.Sprintf(`s3://%s%s/%s.db`, bucketPrefix, db.GetGlueBucket(), db.Name)
		if _, ok := legacyDbNames[db.Name]; ok {
			location = fmt.Sprintf(`s3://%s%s/%s`, bucketPrefix, db.GetGlueBucket(), db.Name)
		}
		// Default is the custom databases group.
		resourceGroup := "bucket_dbs"
		if db.DatabaseGroup == databaseregistries.FivetranDatabaseGroup {
			resourceGroup = "fivetran_dbs"
		} else if db.DatabaseGroup == databaseregistries.ProductionDatabaseGroup {
			resourceGroup = "prod_dbs"
			location = fmt.Sprintf(`s3://%s/%s.db`, tf.LocalId("databricks_warehouse").Reference(), db.Name)
		} else if db.DatabaseGroup == databaseregistries.PlaygroundDatabaseGroup_LEGACY {
			resourceGroup = "dev_dbs"
			location = fmt.Sprintf(`s3://%s/warehouse/%s.db`, tf.LocalId("playground_bucket").Reference(), db.Name)
		} else if db.DatabaseGroup == databaseregistries.TeamDevDatabaseGroup {
			resourceGroup = "team_dev_dbs"
			location = fmt.Sprintf(`s3://%s%s/team_dbs/%s.db`, bucketPrefix, db.GetGlueBucket(), db.Name)
		}
		resource := &awsresource.GlueCatalogDatabase{
			Name:        db.Name,
			LocationUri: location,
		}
		resourcesOutput[resourceGroup] = append(resourcesOutput[resourceGroup], resource)
	}
	return resourcesOutput
}
