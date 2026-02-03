package sparktypes

import (
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLTypes(t *testing.T) {
	snap := snapshotter.New(t)
	defer snap.Verify()

	testCases := []struct {
		name    string
		sqlType *Type
	}{
		{
			name:    "decimal precision scale",
			sqlType: DecimalType(10, 3),
		},
		{
			name: "struct indents",
			sqlType: StructType(
				StructField("a", IntegerType()),
				StructField("b", IntegerType()),
				StructField("nested", StructType(
					StructField("c", IntegerType()),
				)),
			),
		},
		{
			name:    "array indents",
			sqlType: ArrayType(ArrayType(IntegerType())),
		},
		{
			name:    "map indents",
			sqlType: MapType(StringType(), MapType(StringType(), IntegerType())),
		},
		{
			name: "complex struct",
			sqlType: StructType(
				StructField("int", IntegerType()),
				StructField("bigint", LongType()),
				StructField("string", StringType()),
				StructField("date", DateType()),
				StructField("int_array", ArrayType(IntegerType())),
				StructField("struct_array", ArrayType(StructType(
					StructField("int", IntegerType()),
					StructField("bigint", LongType()),
					StructField("string", StringType()),
					StructField("date", DateType()),
					StructField("int_array", ArrayType(IntegerType())),
				))),
				StructField("basic_map", MapType(
					StringType(),
					StringType())),
				StructField("complex_map", MapType(
					BinaryType(),
					ArrayType(IntegerType()))),
			),
		},
	}

	for _, tc := range testCases {
		snap.Snapshot(tc.name, strings.Split(tc.sqlType.SQLIndent("", "  "), "\n"))
	}
}

func TestGetSparkTypeFromMySQLType(t *testing.T) {
	testCases := []struct {
		testName       string
		mysqlTypeInput string
		errExpected    bool
		sparkType      *Type
	}{
		{
			testName:       "BLOB",
			mysqlTypeInput: "blob",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "BINARY",
			mysqlTypeInput: "binary",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "VARBINARY",
			mysqlTypeInput: "varbinary",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "MEDIUMBLOB",
			mysqlTypeInput: "mediumblob",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "CHAR",
			mysqlTypeInput: "char",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "DATETIME",
			mysqlTypeInput: "datetime(6)",
			errExpected:    false,
			sparkType:      TimestampType(),
		},
		{
			testName:       "BIGINT",
			mysqlTypeInput: "bigint(20)",
			errExpected:    false,
			sparkType:      LongType(),
		},
		{
			testName:       "VARCHAR",
			mysqlTypeInput: "varchar(20)",
			errExpected:    false,
			sparkType:      StringType(),
		},
		{
			testName:       "SMALLINT",
			mysqlTypeInput: "smallint(6)",
			errExpected:    false,
			sparkType:      IntegerType(),
		},
		{
			testName:       "TINYINT",
			mysqlTypeInput: "tinyint(4)",
			errExpected:    false,
			sparkType:      IntegerType(),
		},
		{
			testName:       "INT",
			mysqlTypeInput: "int(11)",
			errExpected:    false,
			sparkType:      IntegerType(),
		},
		{
			testName:       "BIT",
			mysqlTypeInput: "bit",
			errExpected:    false,
			sparkType:      BooleanType(),
		},
		{
			testName:       "DOUBLE",
			mysqlTypeInput: "double(10)",
			errExpected:    false,
			sparkType:      DoubleType(),
		},
		{
			testName:       "DECIMAL",
			mysqlTypeInput: "decimal",
			errExpected:    false,
			sparkType:      DecimalType(10, 0),
		},
		{
			testName:       "DECIMAL()",
			mysqlTypeInput: "decimal()",
			errExpected:    false,
			sparkType:      DecimalType(10, 0),
		},
		{
			testName:       "DECIMAL(7)",
			mysqlTypeInput: "decimal(7)",
			errExpected:    false,
			sparkType:      DecimalType(7, 0),
		},
		{
			testName:       "DECIMAL(8,3)",
			mysqlTypeInput: "decimal(8,3)",
			errExpected:    false,
			sparkType:      DecimalType(8, 3),
		},
		{
			testName:       "NoRegex Match",
			mysqlTypeInput: "",
			errExpected:    true,
		},
		{
			testName:       "Bad Arguments",
			mysqlTypeInput: "decimal(wat!)",
			errExpected:    true,
		},
		{
			testName:       "Invalid MySQL type",
			mysqlTypeInput: "WeirdMySQLType(6)",
			errExpected:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			sparkType, err := GetSparkTypeFromMySQLType(tc.mysqlTypeInput)
			if tc.errExpected {
				assert.Nil(t, sparkType)
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Equal(t, tc.sparkType, sparkType)
			}
		})
	}
}

func TestRemoveFieldAtPath(t *testing.T) {
	testCases := []struct {
		name        string
		path        string
		input       *Type
		expected    *Type
		expectError bool
	}{
		{
			name: "simple removal",
			path: "a",
			input: StructType(
				StructField("a", IntegerType()),
				StructField("b", IntegerType()),
			),
			expected: StructType(
				StructField("b", IntegerType()),
			),
		},
		{
			name: "simple nested removal",
			path: "value",
			input: StructType(
				StructField("date", DateType()),
				StructField("time", IntegerType()),
				StructField("value", StructType(
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("field_a", IntegerType()),
							StructField("field_b", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
			expected: StructType(
				StructField("date", DateType()),
				StructField("time", IntegerType()),
			),
		},
		{
			name: "nested removal",
			path: "value.proto_value.something.field_b",
			input: StructType(
				StructField("date", DateType()),
				StructField("time", IntegerType()),
				StructField("value", StructType(
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("field_a", IntegerType()),
							StructField("field_b", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
			expected: StructType(
				StructField("date", DateType()),
				StructField("time", IntegerType()),
				StructField("value", StructType(
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("field_a", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
		},
		{
			name: "unknown path",
			path: "value.proto_value.something.field_lol",
			input: StructType(
				StructField("date", DateType()),
				StructField("time", IntegerType()),
				StructField("value", StructType(
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("field_a", IntegerType()),
							StructField("field_b", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
			expectError: true,
		},
		{
			name:        "empty path",
			path:        "",
			input:       StructType(StructField("date", DateType())),
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.input.RemoveFieldAtPath(testCase.path)
			if testCase.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.EqualValues(t, testCase.expected, testCase.input)
		})
	}
}

func TestValidateSafeForJson(t *testing.T) {
	testCases := []struct {
		name       string
		input      *Type
		errorPaths []string
	}{
		{
			name: "basic types",
			input: StructType(
				StructField("a", IntegerType()),
				StructField("b", StringType()),
				StructField("c", ArrayType(IntegerType())),
				StructField("d", LongType()),
			),
		},
		{
			name: "contains valid maps only at various levels",
			input: StructType(
				StructField("date", DateType()),
				StructField("confusing_map", ArrayType(MapType(StringType(), StringType()))),
				StructField("valid_map_toplevel", MapType(StringType(), StringType())),
				StructField("value", StructType(
					StructField("valid_map_inside", MapType(StringType(), StringType())),
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("field_b", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
		},
		{
			name: "contains invalid maps at various levels",
			input: StructType(
				StructField("confusing_map", ArrayType(MapType(IntegerType(), StringType()))),
				StructField("date", DateType()),
				StructField("valid_map_toplevel", MapType(StringType(), StringType())),
				StructField("value", StructType(
					StructField("invalid_map_inside", MapType(IntegerType(), StringType())),
					StructField("proto_value", StructType(
						StructField("something", StructType(
							StructField("confusing_map_inside", ArrayType(MapType(IntegerType(), StringType()))),
							StructField("field_a", IntegerType()),
							StructField("field_b", IntegerType()),
						)),
						StructField("something_else", IntegerType()),
					)),
				)),
			),
			errorPaths: []string{
				"confusing_map",
				"value.invalid_map_inside",
				"value.proto_value.something.confusing_map_inside",
			},
		},
		{
			name:  "empty struct",
			input: StructType(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.input.ValidateSafeForJson()
			if len(testCase.errorPaths) > 0 {
				assert.Error(t, err)
				for _, path := range testCase.errorPaths {
					assert.True(t, strings.Contains(err.Error(), "Invalid type at $."+path))
				}
				return
			}

			require.NoError(t, err)
		})
	}
}
