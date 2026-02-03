package govramp

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/ni/filepathhelpers"
)

// TestNonGovrampRegistryIntegrity validates that all table references in the registry
// point to tables that actually exist in the registry. This prevents runtime errors
// when views try to reference non-existent tables.
func TestNonGovrampRegistryIntegrity(t *testing.T) {
	allTables := GetAllNonGovrampTables()
	require.NotEmpty(t, allTables, "registry should not be empty")

	// Build a map of all valid table references
	// For default catalog, use schema.table format (old behavior)
	// For non-default catalogs, use catalog.schema.table format
	validTableReferences := make(map[string]bool)
	for _, table := range allTables {
		var key string
		if table.SourceCatalog == "default" {
			key = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			key = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}
		validTableReferences[key] = true
	}

	// Track all missing references for comprehensive reporting
	var allMissingReferences []string
	var tablesWithMissingRefs []string

	// Check each table's references
	for _, table := range allTables {
		var tableKey string
		if table.SourceCatalog == "default" {
			tableKey = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			tableKey = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}

		if len(table.TableReferences) == 0 {
			continue // No references to validate
		}

		var missingRefsForThisTable []string
		for _, ref := range table.TableReferences {
			if !validTableReferences[ref] {
				missingRefsForThisTable = append(missingRefsForThisTable, ref)
				allMissingReferences = append(allMissingReferences, ref)
			}
		}

		if len(missingRefsForThisTable) > 0 {
			tablesWithMissingRefs = append(tablesWithMissingRefs, tableKey)
			t.Errorf("Table %s references non-existent tables: %v", tableKey, missingRefsForThisTable)
		}
	}

	// Provide comprehensive error summary
	if len(allMissingReferences) > 0 {
		t.Logf("Summary: Found %d tables with missing references", len(tablesWithMissingRefs))
		t.Logf("Tables with issues: %v", tablesWithMissingRefs)
		t.Logf("All missing references: %v", allMissingReferences)

		// Create a helpful error message for developers
		assert.Empty(t, allMissingReferences,
			"Registry integrity check failed. Some tables reference non-existent tables. "+
				"Please ensure all referenced tables are added to the non_govramp_registry or "+
				"remove invalid references from the TableReferences field.")
	}
}

// TestSqlFileRequiresViewType ensures that the SqlFile field is only set for views
// and not for tables.
func TestSqlFileRequiresViewType(t *testing.T) {
	allTables := GetAllNonGovrampTables()
	require.NotEmpty(t, allTables, "registry should not be empty")

	for _, table := range allTables {
		if table.TableType == SourceTableTypeTable && table.SqlFile != nil && !table.SqlFile.CustomViewDefinition {
			t.Errorf("Table %s has a SqlFile field set but is a table", table.SourceTable)
		}
	}
}

// TestNonGovrampRegistryNoDuplicates ensures there are no duplicate table entries
// in the registry (same SourceSchema and SourceTable combination).
func TestNonGovrampRegistryNoDuplicates(t *testing.T) {
	allTables := GetAllNonGovrampTables()
	require.NotEmpty(t, allTables, "registry should not be empty")

	seenTables := make(map[string]bool)
	var duplicates []string

	for _, table := range allTables {
		key := fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)

		if seenTables[key] {
			duplicates = append(duplicates, key)
			t.Errorf("Duplicate table found: %s", key)
		} else {
			seenTables[key] = true
		}
	}

	assert.Empty(t, duplicates, "Registry should not contain duplicate table entries")
}

// TestNonGovrampRegistryBasicStructure performs basic validation on registry entries
// to ensure they have required fields populated.
// This test focuses on critical fields and warns about optional ones.
func TestNonGovrampRegistryBasicStructure(t *testing.T) {
	allTables := GetAllNonGovrampTables()
	require.NotEmpty(t, allTables, "registry should not be empty")

	var invalidTables []string

	for i, table := range allTables {
		tableKey := fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)

		// Check required fields
		if table.SourceCatalog == "" {
			t.Errorf("Table %s (index %d): SourceCatalog is empty", tableKey, i)
			invalidTables = append(invalidTables, tableKey)
		}

		if table.SourceSchema == "" {
			t.Errorf("Table %s (index %d): SourceSchema is empty", tableKey, i)
			invalidTables = append(invalidTables, tableKey)
		}

		if table.SourceTable == "" {
			t.Errorf("Table %s (index %d): SourceTable is empty", tableKey, i)
			invalidTables = append(invalidTables, tableKey)
		}

		if table.TableType == "" {
			t.Errorf("Table %s (index %d): TableType is empty", tableKey, i)
			invalidTables = append(invalidTables, tableKey)
		}

		// For tables with a join clause, ensure they have table references
		if table.JoinClause != "" && len(table.TableReferences) == 0 {
			t.Errorf("Table %s (index %d): Has JoinClause but no TableReferences", tableKey, i)
			invalidTables = append(invalidTables, tableKey)
		}

		// For tables with table references, ensure they are views or have a join clause
		if len(table.TableReferences) > 0 && table.TableType != SourceTableTypeView && table.JoinClause == "" {
			t.Errorf("Table %s (index %d): Has TableReferences but TableType is %s (should be %s)",
				tableKey, i, table.TableType, SourceTableTypeView)
			invalidTables = append(invalidTables, tableKey)
		}
	}

	assert.Empty(t, invalidTables, "All registry entries should have valid basic structure")
}

// extractTableReferencesFromSQL parses SQL and extracts table references in schema.table format.
// It looks for patterns like "FROM schema.table", "JOIN schema.table", etc.
// It ignores column references like "alias.column".
func extractTableReferencesFromSQL(sql string) []string {
	var references []string

	// Remove SQL comments first
	sql = removeComments(sql)

	// Split SQL into lines to process FROM/JOIN clauses
	lines := strings.Split(sql, "\n")

	for i, line := range lines {
		line = strings.TrimSpace(line)

		// Look for FROM/JOIN clauses that contain schema.table patterns
		if isFromOrJoinClause(line) {
			// First check if table reference is on the same line
			refs := extractTableReferencesFromClause(line)
			for _, ref := range refs {
				if ref != "" && !slices.Contains(references, ref) {
					references = append(references, ref)
				}
			}

			// If no references found on this line, check the next line
			// (common case where FROM is on one line and table is on the next)
			if len(refs) == 0 && i+1 < len(lines) {
				nextLine := strings.TrimSpace(lines[i+1])
				// For the next line, just look for schema.table pattern directly
				if strings.Contains(nextLine, ".") {
					tokens := strings.Fields(nextLine)
					if len(tokens) > 0 && strings.Contains(tokens[0], ".") {
						if !slices.Contains(references, tokens[0]) {
							references = append(references, tokens[0])
						}
					}
				}
			}
		}
	}

	// Sort for consistent output
	sort.Strings(references)
	return references
}

// removeComments removes SQL comments from the text
func removeComments(sql string) string {
	// Remove single-line comments (-- comment) - handle each line
	lines := strings.Split(sql, "\n")
	var cleanedLines []string
	for _, line := range lines {
		cleanedLine := regexp.MustCompile(`--.*$`).ReplaceAllString(line, "")
		cleanedLines = append(cleanedLines, cleanedLine)
	}
	sql = strings.Join(cleanedLines, "\n")

	// Remove multi-line comments (/* comment */)
	sql = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(sql, "")

	return sql
}

// isFromOrJoinClause checks if a line contains a FROM or JOIN clause (case insensitive)
func isFromOrJoinClause(line string) bool {
	upperLine := strings.ToUpper(strings.TrimSpace(line))

	// Check for FROM or JOIN keywords (all JOIN variations contain "JOIN")
	keywords := []string{"FROM", "JOIN"}

	for _, keyword := range keywords {
		// Check if line exactly matches keyword or contains it with word boundaries
		if upperLine == keyword ||
			strings.HasPrefix(upperLine, keyword+" ") ||
			strings.Contains(upperLine, " "+keyword+" ") ||
			strings.HasSuffix(upperLine, " "+keyword) {
			return true
		}
	}

	return false
}

// extractTableReferencesFromClause extracts schema.table references from a FROM/JOIN clause
func extractTableReferencesFromClause(clause string) []string {
	var references []string

	upperClause := strings.ToUpper(clause)
	keywords := []string{"FROM", "JOIN"}

	for _, keyword := range keywords {
		// Find the position of the keyword
		keywordPos := strings.Index(upperClause, keyword)
		if keywordPos != -1 {
			// Get the text after the keyword
			afterKeyword := strings.TrimSpace(clause[keywordPos+len(keyword):])

			// Extract the first token (table reference) - split on whitespace
			tokens := strings.Fields(afterKeyword)
			if len(tokens) > 0 {
				tableRef := tokens[0]
				// Strip trailing punctuation (parentheses, commas, semicolons)
				tableRef = strings.TrimRight(tableRef, "(),;")

				// Only include if it looks like schema.table format
				if strings.Contains(tableRef, ".") && !slices.Contains(references, tableRef) {
					references = append(references, tableRef)
				}
			}
		}
	}

	return references
}

// TestNonGovrampRegistrySQLTableReferencesMatch validates that table references declared
// in TableReferences field match the actual table references used in the SQL definition files.
func TestNonGovrampRegistrySQLTableReferencesMatch(t *testing.T) {
	allTables := GetAllNonGovrampTables()
	require.NotEmpty(t, allTables, "registry should not be empty")

	var allErrors []string

	// Only check views (should have SQL files)
	for _, table := range allTables {
		if table.TableType != SourceTableTypeView {
			continue // Skip non-views
		}

		if table.TableCategory == SourceTableCategoryRdsClouddbSplitting ||
			table.TableCategory == SourceTableCategoryRdsSharded ||
			table.TableCategory == SourceTableCategoryKinesisstats ||
			table.TableCategory == SourceTableCategoryDataStreams {
			continue // Skip views that are not of these custom types
		}

		tableKey := fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)

		// Read the SQL file
		if table.SqlFile == nil {
			allErrors = append(allErrors, fmt.Sprintf("Table %s has no SQL file", tableKey))
			continue
		}
		sqlFilePath := table.SqlFile.SqlFilePath

		fullSqlFilePath := filepath.Join(filepathhelpers.BackendRoot, sqlFilePath)

		sqlBytes, err := os.ReadFile(fullSqlFilePath)
		if err != nil {
			allErrors = append(allErrors, fmt.Sprintf("Failed to read SQL file for table %s: %v", tableKey, err))
			continue
		}

		sqlContent := string(sqlBytes)

		// Extract table references from SQL
		extractedRefs := extractTableReferencesFromSQL(sqlContent)

		// Compare declared references with extracted references
		declaredRefs := make([]string, len(table.TableReferences))
		copy(declaredRefs, table.TableReferences)
		sort.Strings(declaredRefs)
		sort.Strings(extractedRefs)

		// Check for specific mismatches and provide clear error messages
		missingFromSQL := findMissing(declaredRefs, extractedRefs)
		extraInSQL := findMissing(extractedRefs, declaredRefs)

		// If there are mismatches, add them to allErrors for this table
		if len(missingFromSQL) > 0 || len(extraInSQL) > 0 {
			errorMsg := fmt.Sprintf("Table %s has table reference mismatches:", tableKey)
			if len(missingFromSQL) > 0 {
				errorMsg += fmt.Sprintf("\n  Tables in TableReferences but NOT used in SQL (%d):", len(missingFromSQL))
				for _, missing := range missingFromSQL {
					errorMsg += fmt.Sprintf("\n    - %s", missing)
				}
			}
			if len(extraInSQL) > 0 {
				errorMsg += fmt.Sprintf("\n  Tables used in SQL but NOT in TableReferences (%d):", len(extraInSQL))
				for _, extra := range extraInSQL {
					errorMsg += fmt.Sprintf("\n    - %s", extra)
				}
			}
			allErrors = append(allErrors, errorMsg)
		}
	}

	// Report all errors at once
	if len(allErrors) > 0 {
		for _, errorMsg := range allErrors {
			t.Error(errorMsg)
		}
	}

	assert.Empty(t, allErrors, "All table references should be consistent between TableReferences field and SQL definition files")
}

// findMissing returns elements in slice a that are not in slice b
func findMissing(a, b []string) []string {
	bMap := make(map[string]bool)
	for _, item := range b {
		bMap[item] = true
	}

	var missing []string
	for _, item := range a {
		if !bMap[item] {
			missing = append(missing, item)
		}
	}
	return missing
}


func TestTableReferencesCanReadGroupsConsistency(t *testing.T) {
	allTables := GetAllNonGovrampTables()

	// Build a map of tableKey -> CanReadGroups for all tables
	// For default catalog, use schema.table format (old behavior)
	// For non-default catalogs, use catalog.schema.table format
	tableCanReadGroups := make(map[string][]string)
	for _, table := range allTables {
		var key string
		if table.SourceCatalog == "default" {
			key = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			key = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}
		var groupNames []string
		for _, group := range table.CanReadGroups {
			groupNames = append(groupNames, group.Name())
		}
		tableCanReadGroups[key] = groupNames
	}

	var errors []string

	for _, table := range allTables {
		if len(table.TableReferences) == 0 {
			continue
		}
		// Only check if this table has CanReadGroups
		if len(table.CanReadGroups) == 0 {
			continue
		}
		var thisKey string
		if table.SourceCatalog == "default" {
			thisKey = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			thisKey = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}
		thisGroups := tableCanReadGroups[thisKey]

		for _, ref := range table.TableReferences {
			refGroups, ok := tableCanReadGroups[ref]
			if !ok {
				errors = append(errors, fmt.Sprintf(
					"Table %s references %s but the referenced table does not exist",
					thisKey, ref,
				))
				continue
			}
			// All groups in thisGroups should be present in refGroups
			var missingGroups []string
			for _, g := range thisGroups {
				found := false
				for _, rg := range refGroups {
					if g == rg {
						found = true
						break
					}
				}
				if !found {
					missingGroups = append(missingGroups, g)
				}
			}
			if len(missingGroups) > 0 {
				errors = append(errors, fmt.Sprintf(
					"Table %s references %s but the following CanReadGroups are missing in the referenced table: %v",
					thisKey, ref, missingGroups,
				))
			}
		}
	}

	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
	}
	assert.Empty(t, errors, "All TableReferences should have CanReadGroups that are present in referenced tables as well")
}

func TestTableReferencesCanReadBiztechGroupsConsistency(t *testing.T) {
	allTables := GetAllNonGovrampTables()

	// Build a map of tableKey -> CanReadBiztechGroups for all tables
	// For default catalog, use schema.table format (old behavior)
	// For non-default catalogs, use catalog.schema.table format
	tableCanReadBiztechGroups := make(map[string][]string)
	for _, table := range allTables {
		var key string
		if table.SourceCatalog == "default" {
			key = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			key = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}
		var groupNames []string
		for _, group := range table.CanReadBiztechGroups {
			groupNames = append(groupNames, group)
		}
		tableCanReadBiztechGroups[key] = groupNames
	}

	var errors []string

	for _, table := range allTables {
		if len(table.TableReferences) == 0 {
			continue
		}
		// Only check if this table has CanReadBiztechGroups
		if len(table.CanReadBiztechGroups) == 0 {
			continue
		}
		var thisKey string
		if table.SourceCatalog == "default" {
			thisKey = fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)
		} else {
			thisKey = fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)
		}
		thisGroups := tableCanReadBiztechGroups[thisKey]

		for _, ref := range table.TableReferences {
			refGroups, ok := tableCanReadBiztechGroups[ref]
			if !ok {
				errors = append(errors, fmt.Sprintf(
					"Table %s references %s but the referenced table does not exist",
					thisKey, ref,
				))
				continue
			}
			// All groups in thisGroups should be present in refGroups
			var missingGroups []string
			for _, g := range thisGroups {
				found := false
				for _, rg := range refGroups {
					if g == rg {
						found = true
						break
					}
				}
				if !found {
					missingGroups = append(missingGroups, g)
				}
			}
			if len(missingGroups) > 0 {
				errors = append(errors, fmt.Sprintf(
					"Table %s references %s but the following CanReadBiztechGroups are missing in the referenced table: %v",
					thisKey, ref, missingGroups,
				))
			}
		}
	}

	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
	}
	assert.Empty(t, errors, "All TableReferences should have CanReadBiztechGroups that are present in referenced tables as well")
}

// A test to ensure that all custom view definitions contain a LEFT ANTI JOIN to the stateramp_orgs table.
// This is required for all custom view definitions to ensure that the view is filtered to only include orgs that are
// not in the stateramp_orgs table for govramp compliance purposes.
func TestCustomViewDefinitionContainsLeftAntiJoin(t *testing.T) {
	allTables := GetAllNonGovrampTables()

	var errors []string
	for _, table := range allTables {
		if table.SqlFile == nil || !table.SqlFile.CustomViewDefinition || table.SqlFile.SqlFilePath == "" {
			continue
		}

		tableKey := fmt.Sprintf("%s.%s.%s", table.SourceCatalog, table.SourceSchema, table.SourceTable)

		fullSqlFilePath := filepath.Join(filepathhelpers.BackendRoot, table.SqlFile.SqlFilePath)
		content, err := os.ReadFile(fullSqlFilePath)
		require.NoError(t, err, "failed to read file: %s", table.SqlFile.SqlFilePath)

		contentStr := string(content)
		hasLeftAntiJoin := strings.Contains(contentStr, "LEFT ANTI JOIN")
		hasStateRampOrgs := strings.Contains(contentStr, "default.stateramp.stateramp_orgs")

		if !hasLeftAntiJoin {
			errors = append(errors, fmt.Sprintf("%s: missing LEFT ANTI JOIN", tableKey))
		}
		if !hasStateRampOrgs {
			errors = append(errors, fmt.Sprintf("%s: missing default.stateramp.stateramp_orgs reference", tableKey))
		}
	}

	assert.Empty(t, errors, "All CustomViewDefinition should contain a LEFT ANTI JOIN to the stateramp_orgs table")
}
