// Package sparktypes ports a subset of APIs from pyspark.sql.types package.
// https://spark.apache.org/docs/2.4.3/api/python/pyspark.sql.html#module-pyspark.sql.types
// The DDL representation is follows Hive specification
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL.
package sparktypes

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/libs/ni/filehelpers"
)

type TypeEnum string

const (
	TypeEnumString    = TypeEnum("string")
	TypeEnumFloat     = TypeEnum("float")
	TypeEnumDouble    = TypeEnum("double")
	TypeEnumDecimal   = TypeEnum("decimal")
	TypeEnumByte      = TypeEnum("byte")
	TypeEnumShort     = TypeEnum("short")
	TypeEnumInteger   = TypeEnum("integer")
	TypeEnumLong      = TypeEnum("long")
	TypeEnumBoolean   = TypeEnum("boolean")
	TypeEnumStruct    = TypeEnum("struct")
	TypeEnumArray     = TypeEnum("array")
	TypeEnumDate      = TypeEnum("date")
	TypeEnumBinary    = TypeEnum("binary")
	TypeEnumTimestamp = TypeEnum("timestamp")
	TypeEnumMap       = TypeEnum("map")
)

var mysqlTypeReturnRegex = regexp.MustCompile(`([a-zA-Z]+)(?:\x28(.*)\x29)?`)

// postgresTypeReturnRegex matches PostgreSQL column types, including those with quoted identifiers and parameters.
// Examples it should match: "character varying"(255), integer, "timestamp without time zone"
var postgresTypeReturnRegex = regexp.MustCompile(`(?i)"?([a-zA-Z_][a-zA-Z0-9_ ]*(?:\.[a-zA-Z_][a-zA-Z0-9_ ]+)*)"?(?:\(([^)]*)\))?`)

func (e TypeEnum) SQLType() string {
	switch e {
	case TypeEnumLong:
		return "BIGINT"
	case TypeEnumInteger:
		return "INT"
	default:
		return strings.ToUpper(string(e))
	}
}

func (e TypeEnum) IsBasicType() bool {
	switch e {
	case TypeEnumStruct, TypeEnumArray, TypeEnumMap:
		return false
	default:
		return true
	}
}

type Type struct {
	Type           TypeEnum `json:"type"`
	Nullable       bool     `json:"nullable"`
	Metadata       struct{} `json:"metadata"`
	ContainsNull   bool     `json:"containsNull"`
	KeyType        *Type    `json:"keyType,omitempty"`        // Map only
	ElementType    *Type    `json:"elementType,omitempty"`    // Map / Array only.
	PrecisionScale [2]int   `json:"precisionScale,omitempty"` // Decimal only.
	Fields         []*Field `json:"fields,omitempty"`         // Struct only.
}

func (t Type) SQLIndent(prefix, indent string) string {
	childIndent := ""
	var fields []string
	var arguments []string
	switch t.Type {
	case TypeEnumStruct:
		if len(t.Fields) > 1 || len(t.Fields) > 0 && !t.Fields[0].Type.Type.IsBasicType() {
			childIndent = prefix + indent
		}
		for _, field := range t.Fields {
			fields = append(fields, field.SQLFieldIndent(childIndent, indent))
		}
	case TypeEnumArray:
		if !t.ElementType.Type.IsBasicType() {
			childIndent = prefix + indent
		}
		fields = []string{childIndent + t.ElementType.SQLIndent(childIndent, indent)}
	case TypeEnumMap:
		if !t.KeyType.Type.IsBasicType() || !t.ElementType.Type.IsBasicType() {
			childIndent = prefix + indent
		}
		fields = []string{
			childIndent + t.KeyType.SQLIndent(childIndent, indent),
			childIndent + t.ElementType.SQLIndent(childIndent, indent),
		}
	case TypeEnumDecimal:
		arguments = []string{strconv.Itoa(t.PrecisionScale[0]), strconv.Itoa(t.PrecisionScale[1])}
	}
	typeString := t.Type.SQLType()
	if len(fields) > 0 {
		if childIndent != "" { // One field per line
			typeString = typeString + "<\n" + strings.Join(fields, ",\n") + "\n" + prefix + ">"
		} else {
			typeString = typeString + "<" + strings.Join(fields, ", ") + ">"
		}
	} else if len(fields) == 0 && t.Type == TypeEnumStruct {
		typeString = typeString + "<>"
	}
	if len(arguments) > 0 {
		typeString = typeString + "(" + strings.Join(arguments, ", ") + ")"
	}
	return typeString
}

func (t Type) SQLColumnsIndent(prefix, indent string) string {
	var lines []string
	for _, field := range t.Fields {
		lines = append(lines, field.SQLColumnIndent(prefix, indent))
	}
	return strings.Join(lines, ",\n")
}

func (t Type) String() string {
	return t.SQLIndent("", "  ")
}

type Field struct {
	Name string `json:"name"`
	Type *Type  `json:"type"`
}

// SQLFieldIndent returns Spark SQL specification for a struct field.
func (f Field) SQLFieldIndent(prefix, indent string) string {
	return prefix + "`" + f.Name + "`" + ": " + f.Type.SQLIndent(prefix, indent)
}

// SQLColumnIndent returns Spark SQL specification for a table column.
func (f Field) SQLColumnIndent(prefix, indent string) string {
	return prefix + "`" + f.Name + "`" + " " + f.Type.SQLIndent(prefix, indent)
}

func (f Field) String() string {
	return f.SQLFieldIndent("", "  ")
}

func StructType(fields ...*Field) *Type {
	return &Type{
		Type:     TypeEnumStruct,
		Nullable: true,
		Fields:   fields,
	}
}

func StructField(name string, typ *Type) *Field {
	return &Field{
		Name: name,
		Type: typ,
	}
}

func ArrayType(elementType *Type) *Type {
	return &Type{
		Type:        TypeEnumArray,
		Nullable:    true,
		ElementType: elementType,
	}
}

func MapType(keyType *Type, elementType *Type) *Type {
	return &Type{
		Type:        TypeEnumMap,
		Nullable:    true,
		KeyType:     keyType,
		ElementType: elementType,
	}
}

func ByteType() *Type {
	return &Type{Type: TypeEnumByte, Nullable: true}
}

func ShortType() *Type {
	return &Type{Type: TypeEnumShort, Nullable: true}
}

func IntegerType() *Type {
	return &Type{Type: TypeEnumInteger, Nullable: true}
}

func LongType() *Type {
	return &Type{Type: TypeEnumLong, Nullable: true}
}

func StringType() *Type {
	return &Type{Type: TypeEnumString, Nullable: true}
}

func BooleanType() *Type {
	return &Type{Type: TypeEnumBoolean, Nullable: true}
}

func DoubleType() *Type {
	return &Type{Type: TypeEnumDouble, Nullable: true}
}

func DecimalType(precision, scale int) *Type {
	return &Type{
		Type:           TypeEnumDecimal,
		Nullable:       true,
		PrecisionScale: [2]int{precision, scale},
	}
}

func FloatType() *Type {
	return &Type{Type: TypeEnumFloat, Nullable: true}
}

func DateType() *Type {
	return &Type{Type: TypeEnumDate, Nullable: true}
}

func BinaryType() *Type {
	return &Type{Type: TypeEnumBinary, Nullable: true}
}

func TimestampType() *Type {
	return &Type{Type: TypeEnumTimestamp, Nullable: true}
}

func (t *Type) SaveToFile(path string) error {
	if err := filehelpers.WriteIfChanged(
		path,
		[]byte(t.SQLColumnsIndent("", "  ")+"\n"),
		0644,
	); err != nil {
		return oops.Wrapf(err, "Error saving Spark type to file at path %s", path)
	}

	return nil
}

// getSparkTypeFromMySQLType takes in a MySQL type from a DESCRIBE <table> statement and returns the corresponding
// SparkType type from Type
func GetSparkTypeFromMySQLType(mysqlType string) (*Type, error) {
	match := mysqlTypeReturnRegex.FindStringSubmatch(mysqlType)
	if len(match) == 0 {
		return nil, oops.Errorf("Unknown MySQL type: '%s'", mysqlType)
	}

	sqlType := strings.ToUpper(match[1])
	var arguments []int
	if len(match) > 2 {
		for i, argumentStr := range strings.Split(match[2], ",") {
			argumentStr = strings.TrimSpace(argumentStr)
			if argumentStr == "" {
				continue
			}
			argument, err := strconv.Atoi(argumentStr)
			if err != nil {
				return nil, oops.Wrapf(err, "error parsing argument %d: %s", i, argumentStr)
			}
			arguments = append(arguments, argument)
		}
	}

	switch sqlType {
	case "BLOB", "BINARY", "VARBINARY", "MEDIUMBLOB", "CHAR":
		// Binary data types (including blob) are exported from AWS MySQL as BYTES(), which encodes as strings
		// see: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html
		return StringType(), nil // StringType because AWS DMS exports BLOB type as Hex-Encoded string
	case "DATE":
		return DateType(), nil
	case "DATETIME", "TIMESTAMP":
		return TimestampType(), nil
	case "BIGINT":
		return LongType(), nil
	case "VARCHAR", "MEDIUMTEXT", "TEXT":
		return StringType(), nil
	case "TINYINT":
		return IntegerType(), nil
	case "SMALLINT", "INT":
		return IntegerType(), nil
	case "BIT":
		return BooleanType(), nil
	case "FLOAT":
		return FloatType(), nil
	case "DOUBLE":
		return DoubleType(), nil
	case "DECIMAL":
		// In MySQL, decimal types default to precision=10, scale=0
		var precision, scale = 10, 0
		if len(arguments) > 0 {
			precision = arguments[0]
		}
		if len(arguments) > 1 {
			scale = arguments[1]
		}
		return DecimalType(precision, scale), nil
	default:
		return nil, oops.Errorf("Unable to match MySQL type %s", mysqlType)
	}
}

// GetSparkTypeFromMySQLTypeV2 takes in a MySQL type from a DESCRIBE <table> statement and returns the corresponding
// SparkType type from Type.
// The V2 version is slightly different from the original because of differences between dms parquet output and
// dms csv output.
func GetSparkTypeFromMySQLTypeV2(mysqlType string) (*Type, error) {
	match := mysqlTypeReturnRegex.FindStringSubmatch(mysqlType)
	if len(match) == 0 {
		return nil, oops.Errorf("Unknown MySQL type: '%s'", mysqlType)
	}

	sqlType := strings.ToUpper(match[1])
	var arguments []int
	if len(match) > 2 {
		for i, argumentStr := range strings.Split(match[2], ",") {
			argumentStr = strings.TrimSpace(argumentStr)
			if argumentStr == "" {
				continue
			}
			argument, err := strconv.Atoi(argumentStr)
			if err != nil {
				return nil, oops.Wrapf(err, "error parsing argument %d: %s", i, argumentStr)
			}
			arguments = append(arguments, argument)
		}
	}

	switch sqlType {
	case "BLOB", "BINARY", "VARBINARY", "MEDIUMBLOB", "CHAR", "JSON", "GEOMETRY":
		// Binary data types (including blob) are exported from AWS MySQL as BYTES(), which encodes as strings
		// see: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html
		return StringType(), nil // StringType because AWS DMS exports BLOB type as Hex-Encoded string
	case "DATE":
		return DateType(), nil
	case "DATETIME", "TIMESTAMP":
		return TimestampType(), nil
	case "BIGINT":
		return LongType(), nil
	case "VARCHAR", "MEDIUMTEXT", "TEXT":
		return StringType(), nil
	case "TINYINT":
		return ByteType(), nil
	case "SMALLINT":
		return ShortType(), nil
	case "MEDIUMINT":
		return IntegerType(), nil
	case "INT":
		return IntegerType(), nil
	case "BIT":
		return BooleanType(), nil
	case "FLOAT":
		return FloatType(), nil
	case "DOUBLE":
		return DoubleType(), nil
	case "DECIMAL":
		// In MySQL, decimal types default to precision=10, scale=0
		var precision, scale = 10, 0
		if len(arguments) > 0 {
			precision = arguments[0]
		}
		if len(arguments) > 1 {
			scale = arguments[1]
		}
		return DecimalType(precision, scale), nil
	default:
		return nil, oops.Errorf("Unable to match MySQL type %s", mysqlType)
	}
}

// GetSparkTypeFromPostgreSQLTypeV2 converts a PostgreSQL type to a Spark type.
// This function provides comprehensive mapping of PostgreSQL data types to Spark SQL types
// based on the pg_mapping.csv specification.
// It handles column type strings that may include constraint keywords and default values
// such as "timestamptz DEFAULT now() NULL" or "integer NOT NULL".
func GetSparkTypeFromPostgreSQLTypeV2(columnType string) (*Type, error) {
	// Strip constraint keywords and default values from the column type string
	// These can appear after the type definition in various orders
	// Examples: "timestamptz DEFAULT now() NULL", "integer NOT NULL", "text DEFAULT ''"
	cleanedType := strings.TrimSpace(columnType)

	// Handle NOT NULL constraint first (must come before NULL to avoid partial matches)
	notNullRegex := regexp.MustCompile(`(?i)\s+NOT\s+NULL\b`)
	cleanedType = notNullRegex.ReplaceAllString(cleanedType, "")

	// Handle DEFAULT clauses - match DEFAULT followed by its value
	// PostgreSQL DEFAULT values can be:
	//   - Function calls: now(), current_timestamp(), some_func(arg1, arg2)
	//   - Quoted strings: '', 'text', "text"
	//   - Numbers: 0, 1, 3.14
	//   - NULL or other identifiers
	// Since Go's regexp doesn't support lookahead, we use a multi-step approach:
	// 1. Match DEFAULT followed by common patterns (function calls, identifiers, numbers)
	// 2. The pattern stops at spaces, commas, or constraint keywords
	// 3. We'll strip any remaining constraint keywords separately
	// Pattern: DEFAULT followed by value (function call, identifier, number, or NULL)
	// Then strip any trailing constraint keywords separately
	// First handle function calls (most specific pattern)
	defaultFuncRegex := regexp.MustCompile(`(?i)\s+DEFAULT\s+[a-zA-Z_][a-zA-Z0-9_]*\([^)]*\)`)
	cleanedType = defaultFuncRegex.ReplaceAllString(cleanedType, "")

	// Then handle simple values (quoted strings, numbers, NULL, identifiers)
	// For quoted strings: '' (empty), 'text', or "text" - match one or more chars or empty
	defaultValueRegex := regexp.MustCompile(`(?i)\s+DEFAULT\s+(?:NULL|[0-9]+(?:\.[0-9]+)?|[a-zA-Z_][a-zA-Z0-9_]*|''|'[^']*'|"[^"]*")`)
	cleanedType = defaultValueRegex.ReplaceAllString(cleanedType, "")

	// Handle NULL constraint (using word boundary to avoid matching NULL inside DEFAULT NULL or other contexts)
	nullRegex := regexp.MustCompile(`(?i)\s+NULL\b`)
	cleanedType = nullRegex.ReplaceAllString(cleanedType, "")

	// Trim any remaining whitespace
	cleanedType = strings.TrimSpace(cleanedType)

	match := postgresTypeReturnRegex.FindStringSubmatch(cleanedType)
	if len(match) == 0 {
		return nil, oops.Errorf("Unknown PostgreSQL type: '%s'", columnType)
	}

	sqlType := strings.ToUpper(match[1])

	var arguments []int
	if len(match) > 2 {
		for i, argumentStr := range strings.Split(match[2], ",") {
			argumentStr = strings.TrimSpace(argumentStr)
			if argumentStr == "" {
				continue
			}
			argument, err := strconv.Atoi(argumentStr)
			if err != nil {
				return nil, oops.Wrapf(err, "error parsing argument %d: %s", i, argumentStr)
			}
			arguments = append(arguments, argument)
		}
	}

	// Handle PostgreSQL column datatypes with schema prefix (e.g., "public.geography" or "myschema.mytype")
	// If a type has a dot, strip the prefix and use only the type name for mapping.
	// Only strip if the dot is not at the end (to avoid empty sqlType).
	if dotIdx := strings.Index(sqlType, "."); dotIdx != -1 && dotIdx+1 < len(sqlType) {
		sqlType = sqlType[dotIdx+1:]
	}

	// If sqlType is empty after stripping, return an error.
	if len(sqlType) == 0 {
		return nil, oops.Errorf("Unable to match PostgreSQL type %s (empty type after schema prefix removal)", columnType)
	}

	switch sqlType {
	// Boolean types
	case "BOOLEAN":
		return BooleanType(), nil

	// Integer types
	case "SMALLINT", "INT2":
		return ShortType(), nil
	case "INTEGER", "INT", "INT4":
		return IntegerType(), nil
	case "BIGINT", "INT8":
		return LongType(), nil
	case "SERIAL", "SERIAL4":
		return IntegerType(), nil
	case "BIGSERIAL", "SERIAL8":
		return LongType(), nil

	// Floating point types
	case "REAL", "FLOAT4", "FLOAT":
		return FloatType(), nil
	case "DOUBLE PRECISION", "FLOAT8", "DOUBLE":
		return DoubleType(), nil

	// Numeric/Decimal types
	case "NUMERIC", "DECIMAL":
		// PostgreSQL decimal types default to precision=10, scale=0
		var precision, scale = 10, 0
		if len(arguments) > 0 {
			precision = arguments[0]
		}
		if len(arguments) > 1 {
			scale = arguments[1]
		}
		return DecimalType(precision, scale), nil
	case "MONEY":
		// Money is commonly treated as decimal with precision 19, scale 2
		return DecimalType(19, 2), nil

	// String types
	case "CHAR", "CHARACTER":
		return StringType(), nil
	case "VARCHAR", "CHARACTER VARYING":
		return StringType(), nil
	case "TEXT":
		return StringType(), nil
	case "CITEXT":
		return StringType(), nil
	case "UUID":
		return StringType(), nil

	// Binary types
	case "BYTEA":
		return BinaryType(), nil

	// Date/Time types
	case "DATE":
		return DateType(), nil
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return TimestampType(), nil
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return TimestampType(), nil
	case "TIME", "TIME WITHOUT TIME ZONE":
		// Spark lacks a native TimeType, so we use StringType
		return StringType(), nil
	case "TIMETZ", "TIME WITH TIME ZONE":
		// Stored as string
		return StringType(), nil
	case "INTERVAL":
		// No direct equivalent in Spark, use StringType
		return StringType(), nil

	// JSON and XML types
	case "JSON", "JSONB":
		// Can be treated as StringType or parsed manually if needed
		return StringType(), nil
	case "XML":
		// Treated as text
		return StringType(), nil

	// Geometric types (point, line, circle, polygon, box)
	case "POINT", "LINE", "CIRCLE", "POLYGON", "BOX":
		// Use text or geometry parser
		return StringType(), nil

	// PostGIS spatial types
	case "GEOMETRY":
		// PostGIS geometry type - stored as string for Spark compatibility
		return StringType(), nil
	case "GEOGRAPHY":
		// PostGIS geography type - stored as string for Spark compatibility
		return StringType(), nil

	// Network address types
	case "INET", "CIDR", "MACADDR":
		// Network address as text
		return StringType(), nil

	// Enum types
	case "ENUM":
		// Stores enum label as string
		return StringType(), nil

	// Array types - handle basic element types
	case "ARRAY":
		// For array types, we would need to parse the element type
		// For now, return StringType as a safe fallback
		// TODO: Implement proper array type parsing
		return StringType(), nil

	// Hstore type
	case "HSTORE":
		// Requires hstore → JSON cast, treat as MapType(StringType, StringType)
		// For simplicity, return StringType for now
		// TODO: Implement proper hstore mapping
		return StringType(), nil

	default:
		return nil, oops.Errorf("Unable to match PostgreSQL type %s", columnType)
	}
}

// Removes a field within the struct given the full path to the field.
// Returns an error if it cannot find the field.
func (t *Type) RemoveFieldAtPath(path string) error {
	if len(path) == 0 {
		return oops.Errorf("cannot remove item at empty path")
	}

	parts := strings.Split(path, ".")
	err := t.removeFieldRecurse(parts)
	if err != nil {
		return oops.Wrapf(err, "could not find path %s\n", path)
	}
	return nil
}

func (t *Type) removeFieldRecurse(parts []string) error {
	if len(parts) == 0 || len(t.Fields) == 0 {
		return nil
	}

	currPart := parts[0]

	// Try to find the current part within the struct's `field`s
	foundIdx := -1
	for idx, field := range t.Fields {
		if field.Name == currPart {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		return oops.Errorf("")
	}

	// Remove the field from the fields list if we are at the end of the path.
	if len(parts) == 1 {
		t.Fields = append(t.Fields[:foundIdx], t.Fields[foundIdx+1:]...)
		return nil
	}

	// Otherwise, go a level down in the path and recurse.
	return t.Fields[foundIdx].Type.removeFieldRecurse(parts[1:])
}

// It's possible to construct a Spark type that is not safe for json.
// Many of our pipelines have intermediate values that are json, and so its sometimes
// important to validate that the type is safe for json before using the type.
// Currently, we check that
// - Any maps have only string keys, as non-string keys are not supported.
func (t *Type) ValidateSafeForJson() error {
	errorStrings := t.validateRecursivePath("$")
	if len(errorStrings) > 0 {
		return oops.Errorf("Following paths failed validation. %s\n. Please exclude these fields, or contact #ask-data-platform if you need any assistance.", strings.Join(errorStrings, "\n"))
	}
	return nil
}

func (t *Type) validateRecursivePath(path string) []string {
	var errorStrings []string
	switch t.Type {
	case TypeEnumStruct:
		for _, field := range t.Fields {
			childErrors := field.Type.validateRecursivePath(path + "." + field.Name)
			errorStrings = append(errorStrings, childErrors...)
		}
	case TypeEnumArray:
		errorStrings = t.ElementType.validateRecursivePath(path)
	case TypeEnumMap:
		// We only allow string keys in maps, because json cannot support other types.
		if t.KeyType.Type != TypeEnumString {
			errorStrings = []string{fmt.Sprintf("Invalid type at %s. Map fields cannot contain non-string keys.", path)}
		}
	default:
		// all other types are safe
	}

	return errorStrings
}
