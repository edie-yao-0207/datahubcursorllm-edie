package sqlschemaparser

import (
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/sparktypes"
)

type Db struct {
	Tables map[string]*Table
}

type Table struct {
	Columns          []Column
	ConversionParams sparktypes.ConversionParams
	ExcludeColumns   []string
}

type Column struct {
	ColName string
	ColType string
}

// trimTickMarks `test_table` --> test_table
func trimTickMarks(str string) string {
	return strings.Trim(str, "`")
}

func trimCommas(str string) string {
	return strings.Trim(str, ",")
}

// isColumnDefinitionString returns true if the input string defines a table
// column by process of elimination. This function assumes that the input string
// does not contain newlines and is part of a SQL table creation expression.
func isColumnDefinitionString(str string) bool {
	return len(str) > 0 &&
		!strings.Contains(str, "CREATE TABLE") &&
		!strings.Contains(str, "KEY") &&
		!strings.Contains(str, "ENGINE=") &&
		!strings.Contains(str, "CONSTRAINT")
}

type SchemaType int

const (
	SchemaTypeMySQL SchemaType = iota
	SchemaTypePostgres
)

// ParseDBSchemaFile reads the .sql file and parses it as either MySQL or Postgres schema based on the schemaType parameter.
// schemaType is an enum: SchemaTypeMySQL (default) or SchemaTypePostgres.
func ParseDBSchemaFile(schemaPath string, schemaType ...SchemaType) (*Db, error) {
	stype := SchemaTypeMySQL
	if len(schemaType) > 0 {
		stype = schemaType[0]
	}

	switch stype {
	case SchemaTypeMySQL:
		return ParseMySQLSchemaFile(schemaPath)
	case SchemaTypePostgres:
		return ParsePostgreSQLSchemaFile(schemaPath)
	default:
		return nil, oops.Errorf("Unknown schema type: %v", stype)
	}
}

// ParseDBSchemaFile reads the .sql file, searcching for for create table statements and filter out tables and columns
func ParseMySQLSchemaFile(schemaPath string) (*Db, error) {
	file, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to read db_schema file at %s", schemaPath)
	}

	sqlStr := string(file)
	parts := strings.Split(sqlStr, ";")
	var tableDefinitions []string
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if strings.Contains(trimmedPart, "CREATE TABLE") {
			tableDefinitions = append(tableDefinitions, trimmedPart)
		}
	}

	dbSchema := &Db{
		Tables: make(map[string]*Table, len(tableDefinitions)),
	}
	tableNameRegexp := regexp.MustCompile(`CREATE TABLE '(?P<name>[a-zA-Z_\d]+)' `)
	for _, tableDef := range tableDefinitions {
		tableNameMatches := tableNameRegexp.FindStringSubmatch(strings.ReplaceAll(tableDef, "`", "'"))
		if len(tableNameMatches) < 2 {
			return nil, oops.Errorf("Unable to parse a table name from db_schema file at %s", schemaPath)
		}
		tableName := tableNameMatches[1]
		tableDefParts := strings.Split(tableDef, "\n")
		var columns []Column
		for _, columnSchemaStr := range tableDefParts {
			columnSchemaStr = strings.TrimSpace(columnSchemaStr)
			// Filter out lines that do not define columns (e.g., table name, primary
			// key, or index definitions).
			if isColumnDefinitionString(columnSchemaStr) {
				columnSchemaParts := strings.Split(columnSchemaStr, " ")
				columns = append(columns, Column{
					ColName: trimTickMarks(columnSchemaParts[0]),
					ColType: trimCommas(columnSchemaParts[1]),
				})
			}
		}

		dbSchema.Tables[tableName] = &Table{
			Columns: columns,
		}
	}

	return dbSchema, nil
}

// isPostgreSQLColumnDefinitionString returns true if the input string defines a table
// column in PostgreSQL format by process of elimination. This function assumes that the input string
// does not contain newlines and is part of a SQL table creation expression.
func isPostgreSQLColumnDefinitionString(str string) bool {
	return len(str) > 0 &&
		!strings.Contains(str, "CREATE TABLE") &&
		!strings.Contains(str, "CONSTRAINT") &&
		!strings.Contains(str, "PRIMARY KEY") &&
		!strings.Contains(str, "FOREIGN KEY") &&
		!strings.Contains(str, "INDEX") &&
		!strings.Contains(str, "ALTER TABLE") &&
		!strings.Contains(str, "ADD CONSTRAINT") &&
		!strings.Contains(str, "REFERENCES") &&
		!strings.Contains(str, "ON DELETE") &&
		!strings.Contains(str, "ON UPDATE") &&
		!strings.Contains(str, "USING btree") &&
		!strings.Contains(str, "CREATE INDEX") &&
		!strings.Contains(str, "DROP TABLE") &&
		!strings.Contains(str, "--") &&
		strings.Contains(str, " ") // Must have at least one space (column name and type)
}

// trimDoubleQuotes "test_table" --> test_table
func trimDoubleQuotes(str string) string {
	return strings.Trim(str, "\"")
}

// ParsePostgreSQLSchemaFile reads a PostgreSQL .sql file, searching for CREATE TABLE statements
// and filtering out tables and columns. It handles PostgreSQL-specific syntax including
// schema prefixes (e.g., public.table_name) and double-quoted identifiers.
func ParsePostgreSQLSchemaFile(schemaPath string) (*Db, error) {
	file, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to read PostgreSQL schema file at %s", schemaPath)
	}

	sqlStr := string(file)
	parts := strings.Split(sqlStr, ";")
	var tableDefinitions []string
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if strings.Contains(trimmedPart, "CREATE TABLE") {
			tableDefinitions = append(tableDefinitions, trimmedPart)
		}
	}

	dbSchema := &Db{
		Tables: make(map[string]*Table, len(tableDefinitions)),
	}

	// PostgreSQL table name regex that handles schema prefixes and double quotes
	// Matches: CREATE TABLE public.place ( or CREATE TABLE "public"."place" ( or CREATE TABLE place ( or CREATE TABLE "place" (
	// Pattern: optional schema (with optional quotes) followed by table name (with optional quotes)
	// Group 1: schema name (may be empty if no schema prefix)
	// Group 2: table name
	tableNameRegexp := regexp.MustCompile(`CREATE TABLE\s+(?:"?([a-zA-Z_][a-zA-Z0-9_]*)"?\.)?(?:"?([a-zA-Z_][a-zA-Z0-9_]*)"?)\s*\(`)

	for _, tableDef := range tableDefinitions {

		tableNameMatches := tableNameRegexp.FindStringSubmatch(tableDef)
		if len(tableNameMatches) < 3 {
			return nil, oops.Errorf("Unable to parse a table name from PostgreSQL schema file at %s", schemaPath)
		}

		// Extract table name, handling both schema.table and just table formats
		// FindStringSubmatch returns matches in order of capturing groups:
		// [0] = full match
		// [1] = schema name (or empty if no schema prefix)
		// [2] = table name
		// If no schema is specified, default to "public"
		var tableName string
		if len(tableNameMatches) > 2 && tableNameMatches[1] != "" {
			// Has schema prefix: schema.table
			tableName = tableNameMatches[1] + "__" + tableNameMatches[2] // __ is used to separate the schema name from the table name
		} else if len(tableNameMatches) > 2 && tableNameMatches[2] != "" {
			// No schema prefix: default to public.table
			tableName = "public__" + tableNameMatches[2] // __ is used to separate the schema name from the table name
		} else {
			return nil, oops.Errorf("Unable to parse table name from PostgreSQL schema file at %s: insufficient matches", schemaPath)
		}

		tableDefParts := strings.Split(tableDef, "\n")
		var columns []Column
		for _, columnSchemaStr := range tableDefParts {
			columnSchemaStr = strings.TrimSpace(columnSchemaStr)
			// Filter out lines that do not define columns
			if isPostgreSQLColumnDefinitionString(columnSchemaStr) {
				// Parse column name and type, handling multi-word types like "character varying"
				// Pattern: column_name [TYPE] [constraints,]
				// Find the first space after the column name, then extract everything up to
				// the first comma (if any) or end of line, excluding constraint keywords
				firstSpaceIdx := strings.Index(columnSchemaStr, " ")
				if firstSpaceIdx <= 0 {
					continue
				}
				colName := trimDoubleQuotes(columnSchemaStr[:firstSpaceIdx])

				// Extract the type part (everything after column name up to comma or constraint keywords)
				typePart := strings.TrimSpace(columnSchemaStr[firstSpaceIdx+1:])
				// Remove trailing comma if present
				typePart = strings.TrimRight(typePart, ",")
				// Remove any constraint keywords that might appear (e.g., NOT NULL, DEFAULT, etc.)
				// Common PostgreSQL constraints/attributes that might follow the type
				constraintKeywords := []string{" NOT NULL", " NULL", " DEFAULT ", " PRIMARY KEY", " UNIQUE", " CHECK", " REFERENCES", " now()"}
				for _, keyword := range constraintKeywords {
					if idx := strings.Index(typePart, keyword); idx > 0 {
						typePart = typePart[:idx]
						break
					}
				}
				typePart = strings.TrimSpace(typePart)

				if colName != "" && typePart != "" {
					columns = append(columns, Column{
						ColName: colName,
						ColType: typePart,
					})
				}
			}
		}

		dbSchema.Tables[tableName] = &Table{
			Columns: columns,
		}
	}
	return dbSchema, nil
}
