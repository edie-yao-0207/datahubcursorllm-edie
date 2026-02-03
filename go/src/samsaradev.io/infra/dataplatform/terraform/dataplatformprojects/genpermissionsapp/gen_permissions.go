package main

// TODO_CA: This was a script to generate the permissions for the schemas during UC migration
// which is no longer needed. Will remove in a future PR.
// import (
// 	"bytes"
// 	"fmt"
// 	"go/ast"
// 	"go/parser"
// 	"go/printer"
// 	"go/token"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"regexp"
// 	"sort"
// 	"strings"

// 	"github.com/samsarahq/go/oops"
// 	"golang.org/x/tools/go/ast/astutil"

// 	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
// 	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
// 	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
// 	"samsaradev.io/team/components"
// )

// func main() {
// 	dbToTeamPermsRead, dbToTeamPermsWrite, err := getInstanceProfileSchemaToTeamPerms()
// 	if err != nil {
// 		log.Fatalf("getInstanceProfileSchemaToTeamPerms error: %v\n", err)
// 	}

// 	defaultReadSchemas, defaultReadWriteSchemas, err := getDefaultTeamSchemaPerms()
// 	if err != nil {
// 		log.Fatalf("getDefaultTeamSchemaPerms error: %v\n", err)
// 	}

// 	dir := "/home/ubuntu/co/backend/go/src/samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"

// 	// Walk through the directory and process each Go file
// 	filepathErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
// 		if err != nil {
// 			return err
// 		}
// 		if !info.IsDir() && strings.HasSuffix(path, ".go") {
// 			// Skip database_registry.go and database_registry_test.go
// 			if strings.Contains(path, "database_registry.go") || strings.Contains(path, "database_registry_test.go") {
// 				return nil
// 			}

// 			err := processFile(path, dbToTeamPermsRead, dbToTeamPermsWrite, defaultReadSchemas, defaultReadWriteSchemas)
// 			if err != nil {
// 				log.Printf("Failed to process file %s: %v\n", path, err)
// 			}
// 		}
// 		return nil
// 	})

// 	if err != filepathErr {
// 		log.Fatalf("Failed to walk directory: %v\n", err)
// 	}
// }

// func processFile(path string, readPerms map[string][]string, writePerms map[string][]string,
// 	defaultReadSchemas map[string]struct{}, defaultReadWriteSchemas map[string]struct{}) error {

// 	fset := token.NewFileSet() // Create a new FileSet
// 	node, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
// 	if err != nil {
// 		return fmt.Errorf("failed to parse file: %v", err)
// 	}

// 	// Check if the import statement is already present
// 	componentsImportFound := false
// 	clusterHelpersImportFound := false
// 	for _, imp := range node.Imports {
// 		if imp.Path.Value == `"samsaradev.io/team/components"` {
// 			componentsImportFound = true
// 		} else if imp.Path.Value == `"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"` {
// 			clusterHelpersImportFound = true
// 		}
// 	}
// 	// Add the import statement if not found
// 	if !componentsImportFound {
// 		astutil.AddImport(fset, node, "samsaradev.io/team/components")
// 	}
// 	if !clusterHelpersImportFound {
// 		astutil.AddImport(fset, node, "samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers")

// 	}

// 	// Traverse the AST and find variable declarations ending with "Databases"
// 	for _, decl := range node.Decls {
// 		genDecl, ok := decl.(*ast.GenDecl)
// 		if !ok {
// 			continue
// 		}

// 		for _, spec := range genDecl.Specs {
// 			valueSpec, ok := spec.(*ast.ValueSpec)
// 			if !ok {
// 				continue
// 			}

// 			for _, name := range valueSpec.Names {
// 				if strings.HasSuffix(name.Name, "Databases") {
// 					// Process the composite literals in the variable slice
// 					for _, value := range valueSpec.Values {
// 						compLit, ok := value.(*ast.CompositeLit)
// 						if !ok {
// 							continue
// 						}

// 						for _, elt := range compLit.Elts {
// 							subCompLit, ok := elt.(*ast.CompositeLit)
// 							if !ok {
// 								continue
// 							}

// 							curDbName := getSchemaName(subCompLit.Elts)
// 							_, isDefaultReadSchema := defaultReadSchemas[curDbName]
// 							teamReadPerms, hasTeamReadPerms := readPerms[curDbName]

// 							_, isDefaultReadWriteSchema := defaultReadWriteSchemas[curDbName]
// 							teamReadWritePerms, hasTeamReadWritePerms := writePerms[curDbName]

// 							if len(teamReadPerms) == 0 {
// 								hasTeamReadPerms = false
// 							}
// 							if len(teamReadWritePerms) == 0 {
// 								hasTeamReadWritePerms = false
// 							}

// 							if isDefaultReadWriteSchema {
// 								// Read/Write covers the permissions for Read, so skip setting permissions for Read
// 								// if all teams get read/write access to the schema by default.
// 								isDefaultReadSchema = false
// 								hasTeamReadPerms = false
// 							}

// 							// Filter the read perms if individual teams are set, to remove any teams that have read/write access.
// 							if hasTeamReadPerms && !isDefaultReadSchema && hasTeamReadWritePerms {
// 								writeTeamsLookup := make(map[string]struct{})
// 								for _, team := range teamReadWritePerms {
// 									writeTeamsLookup[team] = struct{}{}
// 								}

// 								var filteredReadTeams []string
// 								// Filter out any team read permissions if the team already has read/write.
// 								for _, team := range teamReadPerms {
// 									if _, ok := writeTeamsLookup[team]; !ok {
// 										filteredReadTeams = append(filteredReadTeams, team)
// 									}
// 								}
// 								// Check whether any read permissions are left after filtering.
// 								if len(filteredReadTeams) > 0 {
// 									teamReadPerms = filteredReadTeams
// 								} else {
// 									hasTeamReadPerms = false
// 								}
// 							}

// 							allDbxClusterTeamsExpr := &ast.CallExpr{
// 								Fun: &ast.SelectorExpr{
// 									X:   ast.NewIdent("clusterhelpers"),
// 									Sel: ast.NewIdent("DatabricksClusterTeams"),
// 								},
// 							}

// 							hasCanReadGroups := false
// 							hasCanReadWriteGroups := false

// 							canReadGroupsKey := "CanReadGroups"
// 							canReadWriteGroupsKey := "CanReadWriteGroups"

// 							for _, subElt := range subCompLit.Elts {
// 								kve, ok := subElt.(*ast.KeyValueExpr)
// 								if !ok {
// 									continue
// 								}

// 								key, ok := kve.Key.(*ast.Ident)
// 								if !ok {
// 									continue
// 								}

// 								// Check for CanReadGroups
// 								if key.Name == canReadGroupsKey && isDefaultReadSchema {
// 									hasCanReadGroups = true
// 									// Replace any existing value.
// 									kve.Value = allDbxClusterTeamsExpr
// 								} else if key.Name == canReadGroupsKey && hasTeamReadPerms {
// 									hasCanReadGroups = true
// 									// Append to the existing CanReadGroups slice
// 									subValue, ok := kve.Value.(*ast.CompositeLit)
// 									if !ok {
// 										continue
// 									}
// 									for _, teamName := range teamReadPerms {
// 										if !containsTeamName(subValue.Elts, teamName) {
// 											subValue.Elts = append(subValue.Elts, &ast.SelectorExpr{
// 												X:   ast.NewIdent("team"),
// 												Sel: ast.NewIdent(teamName),
// 											})
// 										}
// 									}
// 								}

// 								// Check for CanReadWriteGroups
// 								if key.Name == canReadWriteGroupsKey && isDefaultReadWriteSchema {
// 									hasCanReadWriteGroups = true
// 									// Replace the existing value
// 									kve.Value = allDbxClusterTeamsExpr
// 								} else if key.Name == canReadWriteGroupsKey && hasTeamReadWritePerms {
// 									hasCanReadWriteGroups = true
// 									// Append to the existing CanReadWriteGroups slice
// 									subValue, ok := kve.Value.(*ast.CompositeLit)
// 									if !ok {
// 										continue
// 									}
// 									for _, teamName := range teamReadWritePerms {
// 										if !containsTeamName(subValue.Elts, teamName) {
// 											subValue.Elts = append(subValue.Elts, &ast.SelectorExpr{
// 												X:   ast.NewIdent("team"),
// 												Sel: ast.NewIdent(teamName),
// 											})
// 										}
// 									}
// 								}
// 							}

// 							// If CanReadGroups is not present, add CanReadGroups
// 							if !hasCanReadGroups {
// 								if isDefaultReadSchema {
// 									canReadGroupsKVE := &ast.KeyValueExpr{
// 										Key:   ast.NewIdent(canReadGroupsKey),
// 										Value: allDbxClusterTeamsExpr,
// 									}
// 									subCompLit.Elts = append(subCompLit.Elts, canReadGroupsKVE)
// 								} else if hasTeamReadPerms {
// 									// Create the new KeyValueExpr for CanReadGroups
// 									canReadGroupsKVE := &ast.KeyValueExpr{
// 										Key: ast.NewIdent(canReadGroupsKey),
// 										Value: &ast.CompositeLit{
// 											Type: &ast.ArrayType{
// 												Elt: &ast.SelectorExpr{
// 													X:   ast.NewIdent("components"),
// 													Sel: ast.NewIdent("TeamInfo"),
// 												},
// 											},
// 											Elts: make([]ast.Expr, len(teamReadPerms)),
// 										},
// 									}
// 									for i, teamName := range teamReadPerms {
// 										canReadGroupsKVE.Value.(*ast.CompositeLit).Elts[i] = &ast.SelectorExpr{
// 											X:   ast.NewIdent("team"),
// 											Sel: ast.NewIdent(teamName),
// 										}
// 									}
// 									// Append the new entry
// 									subCompLit.Elts = append(subCompLit.Elts, canReadGroupsKVE)
// 								}
// 							}
// 							if !hasCanReadWriteGroups {
// 								if isDefaultReadWriteSchema {
// 									canReadWriteGroupsKVE := &ast.KeyValueExpr{
// 										Key:   ast.NewIdent(canReadWriteGroupsKey),
// 										Value: allDbxClusterTeamsExpr,
// 									}
// 									subCompLit.Elts = append(subCompLit.Elts, canReadWriteGroupsKVE)
// 								} else if hasTeamReadWritePerms {
// 									canReadWriteGroupsKVE := &ast.KeyValueExpr{
// 										Key: ast.NewIdent(canReadWriteGroupsKey),
// 										Value: &ast.CompositeLit{
// 											Type: &ast.ArrayType{
// 												Elt: &ast.SelectorExpr{
// 													X:   ast.NewIdent("components"),
// 													Sel: ast.NewIdent("TeamInfo"),
// 												},
// 											},
// 											Elts: make([]ast.Expr, len(teamReadWritePerms)),
// 										},
// 									}
// 									for i, teamName := range teamReadWritePerms {
// 										canReadWriteGroupsKVE.Value.(*ast.CompositeLit).Elts[i] = &ast.SelectorExpr{
// 											X:   ast.NewIdent("team"),
// 											Sel: ast.NewIdent(teamName),
// 										}
// 									}
// 									// Append the new entry
// 									subCompLit.Elts = append(subCompLit.Elts, canReadWriteGroupsKVE)
// 								}

// 							}
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}

// 	// Write the updated AST back to the file
// 	var buf bytes.Buffer
// 	cfg := &printer.Config{Mode: printer.RawFormat, Tabwidth: 8}
// 	err = cfg.Fprint(&buf, fset, node)
// 	if err != nil {
// 		return fmt.Errorf("failed to print file: %v", err)
// 	}

// 	// Add newline for CanReadGroups entries
// 	output := buf.String()
// 	output = strings.ReplaceAll(output, `, CanReadGroups:`, `,`+"\n"+`		CanReadGroups:`)
// 	output = strings.ReplaceAll(output, `, CanReadWriteGroups:`, `,`+"\n"+`		CanReadWriteGroups:`)
// 	output = strings.ReplaceAll(output, `[]components.TeamInfo{team`, `[]components.TeamInfo{`+"\n			team")
// 	output = strings.ReplaceAll(output, `, team.`, `,`+"\n"+`			team.`)

// 	closeBraceRe := regexp.MustCompile(`([a-zA-Z0-9]+)},`)
// 	output = closeBraceRe.ReplaceAllString(output, "$1,\n		},")

// 	err = os.WriteFile(path, []byte(output), 0644)
// 	if err != nil {
// 		return fmt.Errorf("failed to write file: %v", err)
// 	}

// 	fmt.Printf("Finished processing file: %s\n", path)
// 	return nil
// }

// func getSchemaName(elts []ast.Expr) string {
// 	for _, elt := range elts {
// 		if kve, ok := elt.(*ast.KeyValueExpr); ok {
// 			if key, ok := kve.Key.(*ast.Ident); ok && key.Name == "Name" {
// 				if value, ok := kve.Value.(*ast.BasicLit); ok {
// 					// Remove the surrounding quotes
// 					extractedName := strings.Trim(value.Value, `"`)
// 					return extractedName
// 				}
// 			}
// 		}
// 	}
// 	return "" // return an empty string if "Name" key is not found
// }

// func hasNameField(elts []ast.Expr, name string) bool {
// 	for _, elt := range elts {
// 		if kve, ok := elt.(*ast.KeyValueExpr); ok {
// 			if key, ok := kve.Key.(*ast.Ident); ok && key.Name == "Name" {
// 				if value, ok := kve.Value.(*ast.BasicLit); ok && value.Value == fmt.Sprintf(`"%s"`, name) {
// 					return true
// 				}
// 			}
// 		}
// 	}
// 	return false
// }

// func containsTeamName(elts []ast.Expr, teamName string) bool {
// 	for _, elt := range elts {
// 		if sel, ok := elt.(*ast.SelectorExpr); ok {
// 			if sel.X.(*ast.Ident).Name == "team" && sel.Sel.Name == teamName {
// 				return true
// 			}
// 		}
// 	}
// 	return false
// }

// const (
// 	POLICY_ATTACHMENT_READ_DBS      = "read_databases"
// 	POLICY_ATTACHMENT_READWRITE_DBS = "readwrite_databases"

// 	// For any sweeping read bucket permissions, we validate that the team has access to *every* schema registered under that bucket.
// 	// We previously granted glue read on *everything* to every instance profile, so we need to sync permissions on every DB under that bucket
// 	// to ensure no one loses database access.
// 	POLICY_ATTACHMENT_READ_BUCKETS = "read_buckets"
// )

// var defaultReadBuckets = map[string]struct{}{
// 	"spark-playground":                                         {},
// 	databaseregistries.DatabricksWorkspaceBucket:               {},
// 	databaseregistries.DataPipelinesDeltaLakeFromEuWest1Bucket: {},
// }

// var defaultPolicyAttachments = map[string]struct{}{
// 	"dev_team_glue_database_read":            {},
// 	"glue_catalog_readonly":                  {},
// 	"glue_playground_bucket_databases_write": {},
// 	"s3_standard_dev_buckets_read":           {},
// 	"s3_standard_dev_buckets_write":          {},
// 	"datamodel_ga_read":                      {}, // Granted by default to all legacy clusters.
// }

// // Map the policy attachment ResourceName to a map denoting the read and readwrite DBs the policy grants access to.
// // Structure: policyAttachment ResourceName -> {POLICY_ATTACHMENT_READ_DBS: []string{list of read dbs}, POLICY_ATTACHMENT_READWRITE_DBS: []string{list of readwrite dbs}}
// var interactiveClusterPolicyAttachmentToDataSources = map[string]map[string][]string{
// 	// Skip validating this policy. We grant admins all privileges to the catalog, rather than individual schema resources.
// 	"glue_catalog_admin": nil,
// 	"glue_playground_bucket_databases_write": {
// 		POLICY_ATTACHMENT_READWRITE_DBS: databaseregistries.GetDBNames(
// 			databaseregistries.GetDatabasesByGroup(databaseregistries.PlaygroundDatabaseGroup_LEGACY, databaseregistries.LegacyOnlyDatabases),
// 		),
// 	},
// 	"s3_standard_dev_buckets_read": {
// 		POLICY_ATTACHMENT_READ_BUCKETS: dataplatformprojects.StandardDevReadBuckets,
// 	},
// 	"dev_team_glue_database_read": {
// 		POLICY_ATTACHMENT_READ_DBS: dataplatformprojects.GetReadOnlyDevDbNames(),
// 	},
// 	"datamodel_dev_read": {
// 		POLICY_ATTACHMENT_READ_DBS: dataplatformprojects.GetDataModelDatabaseNames(dataplatformprojects.DevelopmentOnly),
// 	},
// 	"datamodel_prod_read": {
// 		POLICY_ATTACHMENT_READ_DBS: dataplatformprojects.GetDataModelDatabaseNames(dataplatformprojects.ProductionOnly),
// 	},
// 	"datamodel_ga_read": {
// 		POLICY_ATTACHMENT_READ_DBS: dataplatformprojects.GetDataModelDatabaseNames(dataplatformprojects.GAOnly),
// 	},
// 	// Skip syncing permissions on glue_catalog_readonly, as we are not granting read on all schemas in UC.
// 	"glue_catalog_readonly": nil,
// 	// Users can request read/write access to these buckets (aka databricks-playground) via UC volumes.
// 	"s3_standard_dev_buckets_write": nil,
// 	// Ignore these biztech policies because we check permissions against the raw cluster profiles,
// 	// before the buckets/dbs in the biztech cluster profiles get replaced by attached policies.
// 	"biztech_s3_bucket_read_write":     nil,
// 	"biztech_glue_database_read_write": nil,
// }

// // There are some databases folks have listed in instance profiles that *do not exist*.
// // Skip syncing permissions for these non-existent schemas. Also skip sycing permissions for
// // test databases we did not bring over to the UC database registry.
// // We can safely get rid of these permissions, since they wouldn't work anyways in the current system.
// var nonExistentSchemas = map[string]struct{}{
// 	"biztech_edw_api_bronze":          {},
// 	"biztech_edw_productapi_main":     {},
// 	"biztech_edw_gold":                {}, // There is dbt_poc_biztech_edw_gold and kpatro_poc_biztech_edw_gold?
// 	"parth_ucx":                       {}, // Test DB we did not bring over to the UC database registry.
// 	"zendesk":                         {}, // This is not a schema. There is a "samsara_zendesk" schema - maybe we should update to grant this?
// 	"fivetran-samsara-source-qa":      {}, // This is a bucket.
// 	"fivetran-samsara-connector-demo": {}, // This is a bucket.
// 	"data-streams-delta-lake":         {}, // This is a bucket.
// 	"epofinance":                      {}, // There is "epofinance_dev" and "epofinance_prod"?
// 	"netsuite_suiteanalytics_for_connection_all_data": {},
// }

// // Map of bucket name, to any schemas that have tables living in the bucket (where the bucket is NOT the schema root storage location).
// // Some buckets contain tables for a schema, but the bucket is not listed as the storage root location for the schema.
// // If widespread read access is granted on these buckets, the current system grants table/schema access to the data in those buckets.
// // We should ensure users maintain their permissions on these schemas.
// var bucketWithTableDataInSchemas = map[string][]string{
// 	"databricks-workspace":                   {"definitions"},
// 	"data-eng-mapping-tables":                {"definitions", "datamodel_core_silver"},
// 	"databricks-billing":                     {"billing"},
// 	"prod-app-configs":                       {"appconfigs"},
// 	"data-pipelines-delta-lake":              {"dataprep_ml", "dataprep", "data_analytics", "gql_query_performance_report", "idle_locations_report", "mixpanel", "samsara_zendesk"},
// 	"netsuite-delta-tables":                  {"fivetran_log", "netsuite_suiteanalytics"},
// 	"fivetran-netsuite-finance-delta-tables": {"fivetran_netsuite_finance"},
// 	"databricks-playground":                  {"fuel_maintenance"},
// 	"mixpanel":                               {"mixpanel"},
// 	"report-staging-tables":                  {"report_staging"},
// 	"databricks-s3-inventory":                {"s3inventory"},
// }

// type GrantType string

// const (
// 	ReadGrant      GrantType = "read"
// 	ReadWriteGrant GrantType = "read_write"
// )

// func teamHasDatabaseGrant(team components.TeamInfo, dbName string, dbPerms GrantType, defaultReadSchemas map[string]struct{}, defaultReadWriteSchemas map[string]struct{}) (bool, error) {
// 	// Check if all teams automatically get access to the schema by default.
// 	_, hasDefaultRead := defaultReadSchemas[dbName]
// 	_, hasDefaultReadWrite := defaultReadWriteSchemas[dbName]
// 	if dbPerms == ReadWriteGrant && hasDefaultReadWrite {
// 		return true, nil
// 	}
// 	if dbPerms == ReadGrant && (hasDefaultRead || hasDefaultReadWrite) {
// 		return true, nil
// 	}

// 	// There are some databases folks have listed in instance profiles that *do not exist*.
// 	// Skip syncing permissions for these non-existent schemas. Also skip sycing permissions for
// 	// test databases we did not bring over to the UC database registry.
// 	if _, ok := nonExistentSchemas[dbName]; ok {
// 		return true, nil
// 	}

// 	db, err := databaseregistries.GetDatabaseByName(dbName, databaseregistries.AllUnityCatalogDatabases)
// 	if err != nil {
// 		return false, err
// 	}
// 	allowedGroups := db.CanReadWriteGroups
// 	// When checking read permissions, either read or read/write permissions meet the read requirement.
// 	if dbPerms == ReadGrant {
// 		allowedGroups = append(allowedGroups, db.CanReadGroups...)
// 	}
// 	for _, group := range allowedGroups {
// 		if group.TeamName == team.TeamName {
// 			return true, nil
// 		}
// 	}
// 	return false, nil
// }

// // Returns a slice of the schemas the team is missing permissions on in the UC database registry.
// func findMissingSchemaGrantsForTeam(team components.TeamInfo, clusterProfile dataplatformprojects.ClusterProfile) (missingRead []string, missingReadWrite []string, err error) {
// 	defaultReadSchemas, defaultReadWriteSchemas, err := getDefaultTeamSchemaPerms()
// 	if err != nil {
// 		return nil, nil, oops.Wrapf(err, "")
// 	}

// 	// Validate permissions in attached managed policies.
// 	var missingSchemaPermsRead []string
// 	var missingSchemaPermsReadWrite []string
// 	for _, policy := range dataplatformprojects.GetPolicyAttachments(clusterProfile) {
// 		// Skip the default policy attachments, as we are not adding individual team names for these. We're using DatabricksClusterTeams()
// 		if _, ok := defaultPolicyAttachments[policy]; ok {
// 			continue
// 		}
// 		// For each policy attachment, lookup the underlying list and validate that the team has the appropriate permissions.
// 		dataSources, ok := interactiveClusterPolicyAttachmentToDataSources[policy]
// 		// Validate we have a mapping to all policy attachments used in interactive cluster instance profiles.
// 		if !ok {
// 			return nil, nil, oops.Errorf("missing mapping for policy attachment %s to associated buckets/dbs", policy)
// 		} else if dataSources == nil {
// 			// Some policy attachment entry values are nil because we can skip validating those permissions.
// 			continue
// 		}

// 		for dataType, dataList := range dataSources {
// 			// When validating bucket permissions, we need to loop through all schemas that are defined on the bucket and
// 			// expect permissions on them.
// 			// We need to duplicate bucket permissions in this way because in the instance profile system, everyone was granted
// 			// glue read permission on everything. This meant if we ever granted bucket read permissions, we were granting read on
// 			// every database in that bucket.
// 			if dataType == POLICY_ATTACHMENT_READ_BUCKETS {
// 				// If any of the buckets contain tables for a schema, but is not the main storage root location,
// 				// track the schemas so we know to duplicate permissions for them.
// 				readSchemasMap := make(map[string]struct{})
// 				// Make a lookup map of readBuckets.
// 				readBucketsMap := make(map[string]struct{})
// 				for _, b := range dataList {
// 					readBucketsMap[b] = struct{}{}
// 					// If the bucket contains tables for a schema, but is not the main storage root location,
// 					// track the schemas so we know to duplicate permissions for them.
// 					if schemas, ok := bucketWithTableDataInSchemas[b]; ok {
// 						for _, schema := range schemas {
// 							readSchemasMap[schema] = struct{}{}
// 						}
// 					}
// 				}

// 				// Loop through all schemas in database_registry.
// 				for _, db := range databaseregistries.GetAllDatabases(databaseregistries.AllUnityCatalogDatabases) {
// 					_, schemaMatch := readSchemasMap[db.Name]
// 					_, bucketMatch := readBucketsMap[db.Bucket]
// 					if !schemaMatch && !bucketMatch {
// 						continue
// 					}

// 					// If the schema bucket is one of the readBuckets, validate the team has permissions on the schema.
// 					if hasPermission, err := teamHasDatabaseGrant(team, db.Name, ReadGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 						return nil, nil, oops.Wrapf(err, "error checking read grant statement for team %s on db %s", team.TeamName, db.Name)
// 					} else if !hasPermission {
// 						missingSchemaPermsRead = append(missingSchemaPermsRead, db.Name)
// 					}
// 				}
// 			} else if dataType == POLICY_ATTACHMENT_READ_DBS {
// 				for _, dbName := range dataList {
// 					if hasPermission, err := teamHasDatabaseGrant(team, dbName, ReadGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 						return nil, nil, oops.Wrapf(err, "error checking read grant statement for team %s on db %s", team.TeamName, dbName)
// 					} else if !hasPermission {
// 						missingSchemaPermsRead = append(missingSchemaPermsRead, dbName)
// 					}
// 				}
// 			} else if dataType == POLICY_ATTACHMENT_READWRITE_DBS {
// 				for _, dbName := range dataList {
// 					if hasPermission, err := teamHasDatabaseGrant(team, dbName, ReadWriteGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 						return nil, nil, oops.Wrapf(err, "error checking read/write grant statement for team %s on db %s", team.TeamName, dbName)
// 					} else if !hasPermission {
// 						missingSchemaPermsReadWrite = append(missingSchemaPermsReadWrite, dbName)
// 					}
// 				}
// 			} else {
// 				return nil, nil, oops.Errorf("Unepxected policy attachment type")
// 			}
// 		}
// 	}

// 	// Validate read permissions in inline policies.
// 	for _, dbName := range dataplatformprojects.GetReadDatabases(clusterProfile) {
// 		if hasPermission, err := teamHasDatabaseGrant(team, dbName, ReadGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 			return nil, nil, oops.Wrapf(err, "error checking read grant statement for team %s on db %s", team.TeamName, dbName)
// 		} else if !hasPermission {
// 			missingSchemaPermsRead = append(missingSchemaPermsRead, dbName)
// 		}
// 	}

// 	// Validate read/write permissions in inline policies.
// 	for _, dbName := range dataplatformprojects.GetReadWriteDatabases(clusterProfile) {
// 		if hasPermission, err := teamHasDatabaseGrant(team, dbName, ReadWriteGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 			return nil, nil, oops.Wrapf(err, "error checking read/write grant statement for team %s on db %s", team.TeamName, dbName)
// 		} else if !hasPermission {
// 			missingSchemaPermsReadWrite = append(missingSchemaPermsReadWrite, dbName)
// 		}
// 	}

// 	// Handle read buckets, and region specific read buckets.
// 	readBucketsNoPrefix := make(map[string]struct{})
// 	for _, bucket := range dataplatformprojects.GetReadBuckets(clusterProfile) {
// 		readBucketsNoPrefix[bucket] = struct{}{}
// 	}
// 	for _, buckets := range dataplatformprojects.GetRegionSpecificReadBuckets(clusterProfile) {
// 		for _, bucket := range buckets {
// 			bucketNoPrefix := strings.ReplaceAll(bucket, "samsara-eu-", "")
// 			bucketNoPrefix = strings.ReplaceAll(bucketNoPrefix, "samsara-", "")
// 			readBucketsNoPrefix[bucketNoPrefix] = struct{}{}
// 		}

// 	}
// 	// If any of the buckets contain tables for a schema, but is not the main storage root location,
// 	// track the schemas so we know to duplicate permissions for them.
// 	readSchemasMap := make(map[string]struct{})
// 	for b := range readBucketsNoPrefix {
// 		if schemas, ok := bucketWithTableDataInSchemas[b]; ok {
// 			for _, schema := range schemas {
// 				readSchemasMap[schema] = struct{}{}
// 			}
// 		}
// 	}
// 	for _, db := range databaseregistries.GetAllDatabases(databaseregistries.AllUnityCatalogDatabases) {
// 		_, schemaMatch := readSchemasMap[db.Name]
// 		_, bucketMatch := readBucketsNoPrefix[db.Bucket]
// 		if !schemaMatch && !bucketMatch {
// 			continue
// 		}
// 		if hasPermission, err := teamHasDatabaseGrant(team, db.Name, ReadGrant, defaultReadSchemas, defaultReadWriteSchemas); err != nil {
// 			return nil, nil, oops.Wrapf(err, "error checking read grant statement for team %s on db %s", team.TeamName, db.Name)
// 		} else if !hasPermission {
// 			missingSchemaPermsRead = append(missingSchemaPermsRead, db.Name)
// 		}
// 	}

// 	return missingSchemaPermsRead, missingSchemaPermsReadWrite, nil
// }

// // Return maps of all the schemas teams automatically get access to
// func getDefaultTeamSchemaPerms() (defaultReadSchema map[string]struct{}, defaultReadWriteSchema map[string]struct{}, err error) {
// 	defaultReadSchema = make(map[string]struct{})
// 	defaultReadWriteSchema = make(map[string]struct{})

// 	// Default policy attachments.
// 	for policy, _ := range defaultPolicyAttachments {
// 		dataSources, ok := interactiveClusterPolicyAttachmentToDataSources[policy]
// 		// Validate we have a mapping to all policy attachments used in interactive cluster instance profiles.
// 		if !ok {
// 			return nil, nil, oops.Errorf("missing mapping for policy attachment %s to associated buckets/dbs", policy)
// 		} else if dataSources == nil {
// 			// Some policy attachment entry values are nil because we can skip adding those permissions.
// 			continue
// 		}

// 		for dataType, dataList := range dataSources {
// 			if dataType == POLICY_ATTACHMENT_READ_BUCKETS {
// 				// If any of the buckets contain tables for a schema, but is not the main storage root location,
// 				// track the schemas so we know to duplicate permissions for them.
// 				readSchemasMap := make(map[string]struct{})
// 				// Make a lookup map of readBuckets.
// 				readBucketsMap := make(map[string]struct{})
// 				for _, b := range dataList {
// 					readBucketsMap[b] = struct{}{}
// 					// If the bucket contains tables for a schema, but is not the main storage root location,
// 					// track the schemas so we know to duplicate permissions for them.
// 					if schemas, ok := bucketWithTableDataInSchemas[b]; ok {
// 						for _, schema := range schemas {
// 							readSchemasMap[schema] = struct{}{}
// 						}
// 					}
// 				}

// 				// Loop through all schemas in database_registry.
// 				for _, db := range databaseregistries.GetAllDatabases(databaseregistries.AllUnityCatalogDatabases) {
// 					_, schemaMatch := readSchemasMap[db.Name]
// 					_, bucketMatch := readBucketsMap[db.Bucket]
// 					if !schemaMatch && !bucketMatch {
// 						continue
// 					}
// 					defaultReadSchema[db.Name] = struct{}{}
// 				}
// 			} else if dataType == POLICY_ATTACHMENT_READ_DBS {
// 				for _, dbName := range dataList {
// 					defaultReadSchema[dbName] = struct{}{}
// 				}
// 			} else if dataType == POLICY_ATTACHMENT_READWRITE_DBS {
// 				for _, dbName := range dataList {
// 					defaultReadWriteSchema[dbName] = struct{}{}
// 				}
// 			} else {
// 				return nil, nil, oops.Errorf("Unepxected policy attachment type")
// 			}
// 		}

// 		// Handle default read buckets.
// 		// If any of the buckets contain tables for a schema, but is not the main storage root location,
// 		// track the schemas so we know to duplicate permissions for them.
// 		readSchemasMap := make(map[string]struct{})
// 		// If the bucket contains tables for a schema, but is not the main storage root location,
// 		// track the schemas so we know to duplicate permissions for them.
// 		for b := range defaultReadBuckets {
// 			if schemas, ok := bucketWithTableDataInSchemas[b]; ok {
// 				for _, schema := range schemas {
// 					readSchemasMap[schema] = struct{}{}
// 				}
// 			}
// 		}
// 		for _, db := range databaseregistries.GetAllDatabases(databaseregistries.AllUnityCatalogDatabases) {
// 			_, schemaMatch := readSchemasMap[db.Name]
// 			_, bucketMatch := defaultReadBuckets[db.Bucket]
// 			if !schemaMatch && !bucketMatch {
// 				continue
// 			}
// 			defaultReadSchema[db.Name] = struct{}{}
// 		}
// 	}

// 	return defaultReadSchema, defaultReadWriteSchema, nil
// }

// // Deprecate this test once we fully move off of managing permissions on instance profiles.
// // Note: This test lives in dataplatformprojects package because there are a lot of private structs and helper functions
// // we want to use in this test.
// func getInstanceProfileSchemaToTeamPerms() (schemaToReadPerms map[string][]string, schemaToReadWritePerms map[string][]string, err error) {
// 	// Lookup map of all instance profile names to the raw instance profile.
// 	// We don't need to get the updated cluster profiles, because all that does is add buckets for the databases
// 	// and we don't need to grant separate bucket permissions for databases in UC.
// 	nameToRawClusterProfileMap := make(map[string]dataplatformprojects.ClusterProfile)
// 	for _, profile := range dataplatformprojects.AllRawClusterProfiles() {
// 		nameToRawClusterProfileMap[dataplatformprojects.GetInstanceProfileName(profile)] = profile
// 	}

// 	schemaToTeamPermsRead := make(map[string][]string)
// 	schemaToTeamPermsReadWrite := make(map[string][]string)
// 	for _, team := range clusterhelpers.DatabricksClusterTeams() {
// 		// Get the default team interactive cluster
// 		// defaultClusterName := teamNameClusterFormatted(team)
// 		defaultClusterInstanceProfileName, err := dataplatformprojects.InteractiveClusterInstanceProfileName(team, team.DatabricksInteractiveClusterOverrides.ClusterProfileOverride)
// 		if err != nil {
// 			return nil, nil, oops.Wrapf(err, "error getting default cluster instance profile name")
// 		}
// 		defaultClusterProfile, err := instanceProfileByName(defaultClusterInstanceProfileName, nameToRawClusterProfileMap)
// 		if err != nil {
// 			return nil, nil, oops.Wrapf(err, "error getting default cluster instance profile")
// 		}

// 		// TODO: VALIDATE clusters SPECIALLY CREATED (dbx support, admin, dagster dev, sql-endpoint)

// 		defaultClusterMissingRead, defaultClusterMissingReadWrite, err := findMissingSchemaGrantsForTeam(team, defaultClusterProfile)
// 		addPermsToSchemaMap(schemaToTeamPermsRead, team.TeamName, defaultClusterMissingRead)
// 		addPermsToSchemaMap(schemaToTeamPermsReadWrite, team.TeamName, defaultClusterMissingReadWrite)

// 		// Loop through all of the custom interactive clusters.
// 		for _, clusterConfig := range team.DatabricksInteractiveClusters {
// 			customClusterInstanceProfileName, err := dataplatformprojects.InteractiveClusterInstanceProfileName(team, clusterConfig.ClusterProfileOverride)
// 			if err != nil {
// 				return nil, nil, oops.Wrapf(err, "error getting custom cluster instance profile name")
// 			}
// 			customClusterProfile, err := instanceProfileByName(customClusterInstanceProfileName, nameToRawClusterProfileMap)
// 			if err != nil {
// 				return nil, nil, oops.Wrapf(err, "error getting custom cluster instance profile")
// 			}

// 			customClusterMissingRead, customClusterMissingReadWrite, err := findMissingSchemaGrantsForTeam(team, customClusterProfile)
// 			addPermsToSchemaMap(schemaToTeamPermsRead, team.TeamName, customClusterMissingRead)
// 			addPermsToSchemaMap(schemaToTeamPermsReadWrite, team.TeamName, customClusterMissingReadWrite)
// 		}
// 	}
// 	return schemaToTeamPermsRead, schemaToTeamPermsReadWrite, nil
// }

// func addPermsToSchemaMap(m map[string][]string, teamName string, schemas []string) {
// 	// Loop through all the schemas the team should have permissions to, to add them to the map.
// 	for _, schema := range schemas {
// 		// If the schema already exists in the map, update the existing slice.
// 		if teamPerms, ok := m[schema]; ok {
// 			// Check if the team already has permissions on the schema.
// 			teamAlreadyHasPerms := false
// 			for _, existingTeam := range teamPerms {
// 				if existingTeam == teamName {
// 					teamAlreadyHasPerms = true
// 					break
// 				}
// 			}
// 			// Do not add the team again, if they are already listed as having permissions on the schema.
// 			if !teamAlreadyHasPerms {
// 				m[schema] = append(m[schema], teamName)
// 			}
// 		} else {
// 			m[schema] = []string{teamName}
// 		}
// 	}
// }

// func instanceProfileByName(name string, nameToProfileMap map[string]dataplatformprojects.ClusterProfile) (dataplatformprojects.ClusterProfile, error) {
// 	// Special case databricks-support for now, as the instance profiles is defined elsewhere.
// 	// TODO: Handle these.
// 	if name == "databricks-support-cluster" {
// 		return dataplatformprojects.ClusterProfile{}, nil
// 	}

// 	// Lookup the instance profile.
// 	clusterProfile, ok := nameToProfileMap[name]
// 	if !ok {
// 		return dataplatformprojects.ClusterProfile{}, oops.Errorf("Could not find cluster profile with name %s", name)
// 	}
// 	return clusterProfile, nil
// }

// func mapSliceKeysToSortedSlice(m map[string][]string) []string {
// 	var keys []string
// 	for k, _ := range m {
// 		keys = append(keys, k)
// 	}
// 	sort.Strings(keys)
// 	return keys
// }

// func mapStructKeysToSortedSlice(m map[string]struct{}) []string {
// 	var keys []string
// 	for k, _ := range m {
// 		keys = append(keys, k)
// 	}
// 	sort.Strings(keys)
// 	return keys
// }
