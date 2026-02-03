package govramp

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/dbregistry"
)

const (
	// viewDefinitionDir is the directory where the view definitions are stored.
	viewDefinitionDir = "go/src/samsaradev.io/infra/dataplatform/govramp/viewdefinitions"
)

// createGovrampViewResources creates view resources that depend on schema resources.
// It takes a map of schema resources to ensure type-safe dependencies and guaranteed ordering.
// The views will reference the schema resources directly, ensuring schemas are created first.
func createGovrampViewResources(schemaResourceMap map[string]*databricksresource_official.DatabricksSchema) (map[string][]tf.Resource, map[string]*databricksresource_official.SqlTable, error) {
	if len(schemaResourceMap) == 0 {
		return nil, nil, oops.Errorf("schema resource map is required for view project - schemas must be created first")
	}

	resourceGroups := make(map[string][]tf.Resource)

	// Create SQL warehouse for views
	warehouse := &genericresource.Data{
		Type: "databricks_sql_warehouse",
		Name: "aianddata",
		Args: databricksresource_official.SqlWarehouseArgs{
			Name: "aianddata",
		},
	}
	resourceGroups["warehouse"] = []tf.Resource{warehouse}

	viewResourceMap := make(map[string]*databricksresource_official.SqlTable)
	registryTables := govramp.GetAllNonGovrampTables()
	dependsOnByTableKey := buildGovrampViewDependencies(registryTables)
	// Create views that reference the schema resources directly
	for _, table := range registryTables {
		tableKey := govrampTableKey(table.SourceSchema, table.SourceTable)
		dependsOn := dependsOnByTableKey[tableKey]
		var shardViewDependsOn []string
		// If the table is sharded, create a view for each shard
		if table.TableCategory == govramp.SourceTableCategoryRdsSharded {
			rdsDbName := strings.TrimSuffix(table.SourceSchema, "db_shards")
			shardNames, err := getShardNames(rdsDbName)
			if err != nil {
				return nil, nil, oops.Wrapf(err, "failed to get shard names for %s", rdsDbName)
			}

			for _, shardName := range shardNames {
				sourceSchema := fmt.Sprintf("%sdb", shardName)
				shardTable := govramp.Table{
					SourceCatalog:       "default",
					SourceSchema:        sourceSchema,
					SourceTable:         table.SourceTable,
					TableType:           govramp.SourceTableTypeTable,
					TableCategory:       govramp.SourceTableCategoryRds,
					CustomSelectColumns: table.CustomSelectColumns,
				}

				shardedViewResourceGroups, view, err := createGovrampView(shardTable, schemaResourceMap, nil)

				if err != nil {
					return nil, nil, oops.Wrapf(err, "failed to create govramp view for table %s", shardTable.SourceTable)
				}
				resourceGroups = project.MergeResourceGroups(resourceGroups, shardedViewResourceGroups)
				viewResourceMap[fmt.Sprintf("%s-%s", sourceSchema, table.SourceTable)] = view
				shardViewDependsOn = append(shardViewDependsOn, view.ResourceId().String())
			}
		}

		combinedDependsOn := dependsOn
		if table.TableCategory == govramp.SourceTableCategoryRdsSharded && table.TableType == govramp.SourceTableTypeView {
			combinedDependsOn = mergeDependsOn(dependsOn, shardViewDependsOn)
		}
		viewResourceGroups, view, err := createGovrampView(table, schemaResourceMap, combinedDependsOn)
		if err != nil {
			return nil, nil, oops.Wrapf(err, "failed to create govramp view for table %s", table.SourceTable)
		}

		resourceGroups = project.MergeResourceGroups(resourceGroups, viewResourceGroups)
		viewResourceMap[fmt.Sprintf("%s-%s", table.SourceSchema, table.SourceTable)] = view
	}

	return resourceGroups, viewResourceMap, nil
}

func createGovrampView(table govramp.Table, schemaResourceMap map[string]*databricksresource_official.DatabricksSchema, dependsOn []string) (map[string][]tf.Resource, *databricksresource_official.SqlTable, error) {
	resourceGroups := make(map[string][]tf.Resource)

	sqlString, err := getSqlString(table)
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to get sql string for table %s", table.SourceTable)
	}

	// Get the schema resource for this table - this enforces the dependency
	schemaResource, exists := schemaResourceMap[table.SourceSchema]
	if !exists {
		return nil, nil, oops.Errorf("schema resource not found for schema %s - ensure schema project includes this schema", table.SourceSchema)
	}

	view := &databricksresource_official.SqlTable{
		BaseResource: tf.BaseResource{
			MetaParameters: tf.MetaParameters{
				DependsOn: dependsOn,
			},
		},
		ResourceName:   govrampViewResourceName(table.SourceSchema, table.SourceTable),
		Name:           strings.ToLower(table.SourceTable),
		CatalogName:    govrampCatalog,
		SchemaName:     schemaResource.ResourceId().ReferenceAttr("name"), // Direct reference to schema resource
		TableType:      "VIEW",
		ViewDefinition: sqlString,
		WarehouseId:    genericresource.DataResourceId("databricks_sql_warehouse", "aianddata").ReferenceAttr("id"),
	}

	groupName := fmt.Sprintf("views_%s", table.SourceSchema)
	resourceGroups[groupName] = []tf.Resource{view}

	return resourceGroups, view, nil
}

func buildGovrampViewDependencies(registryTables []govramp.Table) map[string][]string {
	dependsOnByTableKey := make(map[string][]string)
	resourceIdByTableKey := make(map[string]string, len(registryTables))

	for _, table := range registryTables {
		tableKey := govrampTableKey(table.SourceSchema, table.SourceTable)
		resourceIdByTableKey[tableKey] = databricksresource_official.SqlTableResourceId(govrampViewResourceName(table.SourceSchema, table.SourceTable)).String()
	}

	for _, table := range registryTables {
		if len(table.TableReferences) == 0 {
			continue
		}
		tableKey := govrampTableKey(table.SourceSchema, table.SourceTable)
		dependsOnSet := make(map[string]struct{})
		for _, reference := range table.TableReferences {
			refKey, ok := govrampReferenceKey(reference)
			if !ok {
				continue
			}
			if refKey == tableKey {
				continue
			}
			if resourceId, exists := resourceIdByTableKey[refKey]; exists {
				dependsOnSet[resourceId] = struct{}{}
			}
		}
		if len(dependsOnSet) == 0 {
			continue
		}
		dependsOn := make([]string, 0, len(dependsOnSet))
		for resourceId := range dependsOnSet {
			dependsOn = append(dependsOn, resourceId)
		}
		slices.Sort(dependsOn)
		dependsOnByTableKey[tableKey] = dependsOn
	}

	return dependsOnByTableKey
}

func govrampViewResourceName(schemaName, tableName string) string {
	return fmt.Sprintf("%s_%s", schemaName, tableName)
}

func govrampTableKey(schemaName, tableName string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s", schemaName, tableName))
}

func govrampReferenceKey(reference string) (string, bool) {
	parts := strings.Split(reference, ".")
	if len(parts) < 2 {
		return "", false
	}
	schemaName := parts[len(parts)-2]
	tableName := parts[len(parts)-1]
	return govrampTableKey(schemaName, tableName), true
}

func mergeDependsOn(primary []string, secondary []string) []string {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}
	mergedSet := make(map[string]struct{}, len(primary)+len(secondary))
	for _, dep := range primary {
		if dep != "" {
			mergedSet[dep] = struct{}{}
		}
	}
	for _, dep := range secondary {
		if dep != "" {
			mergedSet[dep] = struct{}{}
		}
	}
	merged := make([]string, 0, len(mergedSet))
	for dep := range mergedSet {
		merged = append(merged, dep)
	}
	slices.Sort(merged)
	return merged
}

// getShardNames takes a rdsDbName like "workforcevideo" and returns:
// - shardNames: physical shard names (e.g., workforcevideo_shard_0, workforcevideo_shard_1, ...)
func getShardNames(rdsDbName string) ([]string, error) {
	shardNames, err := dbregistry.GetShardedDBNamesPerCloud(rdsDbName, infraconsts.SamsaraClouds.USProd)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to get sharded db names for %s", rdsDbName)
	}

	// Build parallel slices of physical schema and display name
	shardDisplayNames := make([]string, 0, len(shardNames))
	for i := 0; i < len(shardNames); i++ {
		shardDisplayNames = append(shardDisplayNames, fmt.Sprintf("%s_shard_%d", rdsDbName, i))
	}
	return shardDisplayNames, nil
}

func getSelectColumns(table govramp.Table) string {
	selectColumns := "t.*"
	if len(table.CustomSelectColumns) > 0 {
		selectColumns = "t." + strings.Join(table.CustomSelectColumns, ", t.")
	}
	return selectColumns
}

func getSqlString(table govramp.Table) (string, error) {

	// If a custom view definition is provided, use it instead of the default view definition created by the registry.
	if table.SqlFile != nil && table.SqlFile.CustomViewDefinition {
		fileName := fmt.Sprintf("%s.%s.sql", table.SourceSchema, table.SourceTable)
		sqlFilePath := filepath.Join(viewDefinitionDir, fileName)
		fullSqlFilePath := filepath.Join(filepathhelpers.BackendRoot, sqlFilePath)
		sqlBytes, err := os.ReadFile(fullSqlFilePath)
		if err != nil {
			return "", oops.Wrapf(err, "failed to read custom view definition for table %s", table.SourceTable)
		}
		return string(sqlBytes), nil
	}

	var sqlString string

	selectColumns := getSelectColumns(table)

	tableJoinColumnName := "org_id"
	staterampJoinColumnName := "org_id"
	if table.OrgIdColumnOverride != "" {
		tableJoinColumnName = table.OrgIdColumnOverride
		staterampJoinColumnName = "org_id"
	} else if table.SamNumberColumn != "" {
		tableJoinColumnName = table.SamNumberColumn
		staterampJoinColumnName = "sam_number"
	}

	if table.TableType == govramp.SourceTableTypeTable {
		sourceTable := fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		orgFilterStatement := ""
		if !table.NoOrgSpecificColumn {
			// Determine the correct table alias for the LEFT ANTI JOIN
			// If there's a join with groups table, extract the alias that follows "AS"
			// up to the next whitespace character.
			joinTableAlias := "t"
			if table.JoinClause != "" {
				afterAS := strings.Split(table.JoinClause, "AS ")[1]
				// Extract just the alias (up to next whitespace)
				aliasFields := strings.Fields(afterAS)
				if len(aliasFields) > 0 {
					joinTableAlias = aliasFields[0]
				}
			}

			// Conditionally add newline to join clause if it exists
			joinClauseWithNewline := ""
			if table.JoinClause != "" {
				joinClauseWithNewline = table.JoinClause + "\n"
			}

			if table.OrgIdInArray {
				// If the org_id column is an array, we need to filter out rows that have the stateramp org_id in the array via a array_overlap function
				orgFilterStatement = fmt.Sprintf("\nWHERE NOT arrays_overlap(t.%s, (SELECT collect_set(org_id) FROM default.stateramp.stateramp_orgs))", tableJoinColumnName)
			} else {
				// If the org_id column is not an array, we need to filter out rows that have the stateramp org_id in the column via a LEFT ANTI JOIN
				orgFilterStatement = fmt.Sprintf("\n%sLEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON %s.%s = s.%s", joinClauseWithNewline, joinTableAlias, tableJoinColumnName, staterampJoinColumnName)
			}
		}

		// skiplint: +banselectstar
		sqlString = fmt.Sprintf(`SELECT %s
FROM %s AS t%s`,
			selectColumns, sourceTable, orgFilterStatement)
	} else if table.TableType == govramp.SourceTableTypeView {
		if table.TableCategory == govramp.SourceTableCategoryRdsSharded {
			// For sharded RDS sources, build a UNION ALL across shard_0..5 databases,
			// prefix all references with the target catalog, and join to org_shards_v2
			// to filter only the relevant shard_type/data_type.

			// extract rdsDbName from the sharded DB name by removing the suffix `db_shards`
			rdsDbName := strings.TrimSuffix(table.SourceSchema, "db_shards")

			// Resolve physical shard schemas and human-friendly shard display names
			shardDisplayNames, err := getShardNames(rdsDbName)
			if err != nil {
				return "", err
			}

			// Build per-shard SELECTs
			var shardSelects []string
			for _, shardName := range shardDisplayNames {
				shardDb := fmt.Sprintf("%sdb", shardName)
				// skiplint: +banselectstar
				shardSelect := fmt.Sprintf(`          SELECT
            '%s' AS shard_name,
            *
          FROM
            %s.%s.%s`,
					shardName, govrampCatalog, shardDb, table.SourceTable,
				)
				shardSelects = append(shardSelects, shardSelect)
			}

			// Join all shard selects with UNION ALL
			shardsUnion := strings.Join(shardSelects, "\n          UNION ALL\n")

			// Final SQL assembly
			sqlString = fmt.Sprintf(`
(
  SELECT
    *
  FROM
    (
      SELECT
        /*+ BROADCAST(os) */
        shards.*
      FROM
        (
%s
        ) shards
        LEFT OUTER JOIN %s.appconfigs.org_shards_v2 os ON shards.org_id = os.org_id
      WHERE
        os.shard_type = 1
        AND os.data_type = '%s'
    )
)`, shardsUnion, govrampCatalog, rdsDbName)
		} else if table.TableCategory == govramp.SourceTableCategoryKinesisstats {
			// skiplint: +banselectstar
			sqlString = fmt.Sprintf(`SELECT %s
FROM default.kinesisstats_history.%s AS t
LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.%s = s.%s
WHERE date > date_sub(current_date(), 365)`, selectColumns, table.SourceTable, tableJoinColumnName, staterampJoinColumnName)
		} else if table.TableCategory == govramp.SourceTableCategoryRdsClouddbSplitting {
			if !isCloudDBSplittingTable(table) {
				return "", oops.Errorf("table %s is not a clouddb splitting table", table.SourceTable)
			}

			joinTableAlias := "t"
			if table.JoinClause != "" {
				afterAS := strings.Split(table.JoinClause, "AS ")[1]
				// Extract just the alias (up to next whitespace)
				aliasFields := strings.Fields(afterAS)
				if len(aliasFields) > 0 {
					joinTableAlias = aliasFields[0]
				}
			}

			joinClauseWithNewline := ""
			if table.JoinClause != "" {
				joinClauseWithNewline = table.JoinClause + "\n"
			}

			// skiplint: +banselectstar
			sqlString = fmt.Sprintf(`SELECT %s
FROM default.productsdb.%s AS t
%sLEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON %s.%s = s.%s`, selectColumns, table.SourceTable, joinClauseWithNewline, joinTableAlias, tableJoinColumnName, staterampJoinColumnName)
		} else if table.TableCategory == govramp.SourceTableCategoryDataStreams {
			orgFilterStatement := ""
			if !table.NoOrgSpecificColumn {
				orgFilterStatement = fmt.Sprintf("LEFT ANTI JOIN default.stateramp.stateramp_orgs AS s ON t.%s = s.%s\n", tableJoinColumnName, staterampJoinColumnName)
			}
			// skiplint: +banselectstar
			sqlString = fmt.Sprintf(`SELECT %s
FROM default.datastreams_history.%s AS t
%sWHERE date > date_sub(current_date(), 365)`, selectColumns, table.SourceTable, orgFilterStatement)
		} else {
			if table.SqlFile == nil || table.SqlFile.SqlFilePath == "" {
				return "", oops.Errorf("sql file path is required for table %s", table.SourceTable)
			}
			sqlFilePath := table.SqlFile.SqlFilePath
			fullSqlFilePath := filepath.Join(filepathhelpers.BackendRoot, sqlFilePath)

			sqlBytes, err := os.ReadFile(fullSqlFilePath)
			if err != nil {
				return "", oops.Wrapf(err, "failed to read view definition for table %s", table.SourceTable)
			}
			sqlString = string(sqlBytes)

			// Replace variables in the sql string based on the variable map
			if table.SqlFile.VariableMap != nil {
				for key, value := range table.SqlFile.VariableMap {
					sqlString = strings.ReplaceAll(sqlString, "{{"+key+"}}", value)
				}
			}

			for _, tableReference := range table.TableReferences {
				// Use regex to replace only complete table references (bounded by word boundaries)
				pattern := `\b` + regexp.QuoteMeta(tableReference) + `\b`
				re := regexp.MustCompile(pattern)

				// If tableReference uses three-dot notation (catalog.schema.table),
				// drop the catalog and use schema.table
				parts := strings.Split(tableReference, ".")
				var replacement string
				if len(parts) == 3 {
					// Drop catalog, keep schema.table
					replacement = fmt.Sprintf("non_govramp_customer_data.%s.%s", parts[1], parts[2])
				} else {
					// Use current behavior for two-dot notation
					replacement = fmt.Sprintf("non_govramp_customer_data.%s", tableReference)
				}
				sqlString = re.ReplaceAllString(sqlString, replacement)
			}
			sqlString = strings.ReplaceAll(sqlString, `"`, `\"`)
		}
	}

	return sqlString, nil
}

// isCloudDBSplittingTable validates that a table is in fact a clouddb splitting table
// This is a list of tables in clouddb that was migrated to productsdb but
// their reference is still intact in clouddb as a view on top of the productsdb table.
// This is done to avoid breaking changes to the existing code that references these tables.
func isCloudDBSplittingTable(table govramp.Table) bool {
	clouddb_productsdb_migrated_tables := []string{
		"core_external_keys",
		"core_external_values",
		"devices",
		"dvirs_templates_devices",
		"gateway_device_history",
		"gateways",
		"id_cards",
		"monitors",
		"product_oui_batches",
		"tag_devices",
		"tag_widgets",
		"widgets",
	}

	if table.SourceSchema == "clouddb" && slices.Contains(clouddb_productsdb_migrated_tables, table.SourceTable) {
		return true
	}
	return false
}
